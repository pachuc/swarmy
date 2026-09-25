#!/usr/bin/env python3
"""Checkpoint, collection, and retained-snapshot boot measurements on a root host.

Invoked by swarmy-chaos, which clones retained manifests after collection.
All JSON samples go to stdout. CLI diagnostics remain on stderr.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from volume import Attached, cli, clear_cache, run, vol


def prepare(image):
    volume = vol("create", image)["volume_id"]
    snapshots = []
    with Attached(volume, background=True) as disk:
        disk.mount_disk()
        digest = write_generations(disk, snapshots)
        # Keep the existing lightweight pause measurement comparable with the
        # previous run; the preceding generations exercise concurrent bulk I/O.
        writer = subprocess.Popen([sys.executable, "-c", '''import os, sys, time
with open(sys.argv[1], 'w', buffering=1) as file:
    while True:
        file.write(str(time.monotonic_ns()) + '\\n')
        file.flush()
        os.fsync(file.fileno())
        time.sleep(0.01)
''', str(disk.mount / "root/timestamps")])
        try:
            time.sleep(1)
            pause_checkpoint = vol("checkpoint", volume, "--mount", str(disk.mount))
            time.sleep(1)
        finally:
            writer.terminate()
            writer.wait(timeout=10)
        stamps = [int(line) for line in (disk.mount / "root/timestamps").read_text().splitlines()]
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        assert len(gaps) > 10
        largest = max(range(len(gaps)), key=gaps.__getitem__)
        pause = {"metric": "snapshot_writer_pause", "max_gap_ns": gaps[largest],
                 "sample_count": len(stamps), "timestamps_around_max_ns": stamps[max(0, largest-2):largest+4],
                 "checkpoint": pause_checkpoint}
        snapshots.append({"manifest_id": pause_checkpoint["manifest_id"], "generation": 12, "sha256": digest})
    shown = vol("show", volume)
    snapshots.append({"manifest_id": shown["record"]["head_manifest"], "generation": 12, "sha256": digest, "source": "detach"})
    retained = shown["manifests"]
    assert len(retained) == 10, retained
    expected = {item["manifest_id"]: item for item in snapshots}
    # No publishers remain. Collection runs on the control plane, so the grace
    # window travels on the run request; client SWARMY_GC_GRACE_SECONDS would
    # be ignored. This short grace is valid only in this isolated run.
    time.sleep(3)
    start = time.monotonic()
    dry = cli("gc", "--dry-run", "--grace-seconds", "1")
    dry_seconds = time.monotonic() - start
    start = time.monotonic()
    real = cli("gc", "--grace-seconds", "1")
    real_seconds = time.monotonic() - start
    assert dry["bytes_freed"] == 0, dry
    assert real["bytes_freed"] > 0, real
    assert real["bytes_freed"] == dry["candidate_bytes"], (dry, real)
    return {"pause": pause, "volume": volume, "checkpoints": snapshots,
            "retained": [{"manifest_id": item["manifest_id"],
                          **{key: expected[item["manifest_id"]][key] for key in ("generation", "sha256")}} for item in retained],
            "collector": {"dry_seconds": dry_seconds, "real_seconds": real_seconds,
                          "dry": dry, "real": real}}


def write_generations(disk, snapshots):
    # Keep a second file changing while every generation checkpoint uploads.
    # Its writes exercise preservation without changing the independently hashed
    # generation marker and payload that each retained boot verifies.
    writer = subprocess.Popen([sys.executable, "-c", """import os, sys
with open(sys.argv[1], 'wb', buffering=0) as file:
    while True:
        file.seek(0)
        file.write(os.urandom(8 * 1024 * 1024))
        os.fsync(file.fileno())
""", str(disk.mount / "root/concurrent")])
    try:
        for index in range(13):
            (disk.mount / "root/generation").write_text(str(index))
            with (disk.mount / "root/changed").open("wb" if index == 0 else "r+b") as file:
                data = os.urandom(8 * 1024 * 1024)
                digest = hashlib.sha256(data).hexdigest()
                file.write(data)
                file.flush()
                os.fsync(file.fileno())
            checkpoint = vol("checkpoint", disk.volume, "--mount", str(disk.mount))
            assert writer.poll() is None, "concurrent writer exited during snapshots"
            snapshots.append({"manifest_id": checkpoint["manifest_id"], "generation": index, "sha256": digest})
        return digest
    finally:
        writer.terminate()
        writer.wait(timeout=10)


def boot(volume, generation, digest):
    clear_cache()
    start = time.monotonic()
    with Attached(volume) as disk:
        disk.mount_disk()
        result = run("chroot", str(disk.mount), "/bin/bash", "-c",
                     'test "$(cat /root/generation)" = "$1" && test $(stat -c %s /root/changed) = 8388608 && test "$(sha256sum /root/changed | cut -d " " -f1)" = "$2" && echo BOOTED',
                     "bash", generation, digest, capture=True)
        assert result.strip() == "BOOTED", result
        seconds = time.monotonic() - start
    return {"metric": "retained_snapshot_boot", "volume": volume,
            "generation": int(generation), "seconds": seconds}


if __name__ == "__main__":
    assert os.geteuid() == 0, "run the prebuilt workload with sudo"
    result = prepare(sys.argv[2]) if sys.argv[1] == "prepare" else boot(sys.argv[2], sys.argv[3], sys.argv[4])
    print(json.dumps(result), flush=True)
