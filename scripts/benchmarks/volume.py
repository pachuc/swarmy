#!/usr/bin/env python3
"""Run as root on a disposable benchmark VM with cloud SWARMY_* settings.

Use a fresh working directory on the cache disk and put the CLI binaries on
PATH. Results are JSON lines on stdout; command diagnostics go to stderr.
"""

import json
import os
from pathlib import Path
import select
import shlex
import shutil
import subprocess
import time


def run(*args, capture=False):
    return subprocess.run(
        args, check=True, text=True,
        stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
        stderr=None, timeout=1800,
    ).stdout


def cli(*args):
    return json.loads(run("swarmy", "--json", *args, capture=True))


def report(metric, start, **extra):
    print(json.dumps({"metric": metric, "seconds": time.monotonic() - start,
                      **extra}), flush=True)


def clear_cache():
    # Call only after the attachment has stopped and released the cache.
    cache = Path.cwd() / ".swarmy/volumes/cache"
    if cache.exists():
        shutil.rmtree(cache)
    run("sync")
    Path("/proc/sys/vm/drop_caches").write_text("3\n")


class Attached:
    def __init__(self, volume):
        self.volume = volume
        self.mount = Path.cwd() / "mounted"
        self.mount.mkdir(exist_ok=True)
        self.process = None
        self.mounted = False

    def __enter__(self):
        self.process = subprocess.Popen(
            ["swarmy", "--json", "vol", "attach", self.volume],
            stdout=subprocess.PIPE, text=True,
        )
        try:
            if not select.select([self.process.stdout], [], [], 60)[0]:
                raise TimeoutError("volume attachment did not become ready")
            ready = json.loads(self.process.stdout.readline())
            self.device = ready["device"]
        except BaseException:
            self.process.terminate()
            self.process.wait(timeout=60)
            raise
        return self

    def mount_disk(self):
        run("mount", self.device, str(self.mount))
        self.mounted = True

    def __exit__(self, *_):
        try:
            if self.mounted:
                run("umount", str(self.mount))
            cli("vol", "detach", self.volume)
            assert self.process.wait(timeout=60) == 0, "attachment failed"
        finally:
            if self.process.poll() is None:
                self.process.terminate()
                self.process.wait(timeout=60)


def shell_trial(kind, trial):
    if kind == "cold":
        clear_cache()
    else:
        run("sync")
        Path("/proc/sys/vm/drop_caches").write_text("3\n")
    start = time.monotonic()
    volume = cli("vol", "create", "bench-base:v1")["volume_id"]
    with Attached(volume) as attached:
        attached.mount_disk()
        # script supplies a controlling terminal; bash is explicitly interactive.
        command = shlex.join(["chroot", str(attached.mount), "/bin/bash",
                              "--noprofile", "--norc", "-ic", "echo SWARMY_READY"])
        output = run("script", "-qec", command, "/dev/null", capture=True)
        assert "SWARMY_READY" in output
        report(f"shell_{kind}", start, trial=trial)


def main():
    assert os.geteuid() == 0, "run as root on a disposable VM"
    assert not (Path.cwd() / ".swarmy").exists(), "use a fresh working directory"
    recipe = Path.cwd() / "bench-base"
    recipe.mkdir()
    # The standard base image already has build-essential. This variant leaves
    # it out so the measured package installation actually changes the disk.
    (recipe / "recipe.toml").write_text('''disk_size = 8589934592
source_date_epoch = 1714003200
[source]
kind = "debootstrap"
suite = "noble"
mirror = "http://archive.ubuntu.com/ubuntu"
packages = ["bash", "coreutils", "curl", "ca-certificates"]
''')
    print(json.dumps({"image": cli("image", "build", str(recipe), "--tag", "v1")}), flush=True)
    cli("image", "show", "bench-base:v1")
    run("swarmy", "image", "ls")
    for trial in range(3):
        shell_trial("cold", trial)
    volume = cli("vol", "create", "bench-base:v1")["volume_id"]
    with Attached(volume) as attached:
        run("dd", f"if={attached.device}", "of=/dev/null", "bs=4M", "iflag=direct", "status=none")
    for trial in range(3):
        shell_trial("warm", trial)
    with Attached(volume) as attached:
        attached.mount_disk()
        shutil.copyfile("/etc/resolv.conf", attached.mount / "etc/resolv.conf")
        assert subprocess.run(["chroot", str(attached.mount), "dpkg-query", "-W", "build-essential"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0
        for name in ("dev", "proc", "sys"):
            run("mount", "--rbind", f"/{name}", str(attached.mount / name))
            run("mount", "--make-rslave", str(attached.mount / name))
        try:
            run("chroot", str(attached.mount), "apt-get", "update")
            run("chroot", str(attached.mount), "env", "DEBIAN_FRONTEND=noninteractive",
                "apt-get", "install", "-y", "build-essential")
        finally:
            for name in ("sys", "proc", "dev"):
                run("umount", "-R", str(attached.mount / name))
        # Include sync and filesystem freeze in the flush measurement.
        start = time.monotonic()
        flushed = cli("vol", "flush", volume, "--mount", str(attached.mount))
        report("flush_build_essential", start, manifest=flushed["manifest_id"])
        run("chroot", str(attached.mount), "gcc", "--version")
        run("dd", "if=/dev/urandom", f"of={attached.mount}/sequential.bin",
            "bs=1M", "count=128", "conv=fsync", "status=none")
        cli("vol", "flush", volume, "--mount", str(attached.mount))
    cli("vol", "snapshot", volume)
    clone = cli("vol", "clone", volume)["volume_id"]
    clear_cache()
    with Attached(clone) as attached:
        attached.mount_disk()
        run("chroot", str(attached.mount), "gcc", "--version")
        for kind in ("cold", "warm"):
            start = time.monotonic()
            run("dd", f"if={attached.mount}/sequential.bin", "of=/dev/null",
                "bs=256K", "iflag=direct", "status=none")
            report(f"sequential_{kind}", start, bytes=128 * 1024 * 1024)
    cli("vol", "show", volume)
    run("swarmy", "vol", "ls")


if __name__ == "__main__":
    main()
