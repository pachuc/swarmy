#!/usr/bin/env python3
"""Run as root on a disposable benchmark VM with cloud SWARMY_* settings.

Build with cargo build --release --workspace --locked, then use a fresh working
directory on the cache disk and select --target-dir. Every sample gets an isolated
object prefix so earlier installations cannot supply deduplicated chunks. Results
are JSON lines on stdout; command diagnostics go to stderr.
"""

import argparse
import json
import os
from pathlib import Path
import select
import shlex
import shutil
import subprocess
import time
import uuid


def run(*args, capture=False):
    return subprocess.run(
        args, check=True, text=True,
        stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
        stderr=None, timeout=7200,
    ).stdout


def cli(*args):
    return json.loads(run("swarmy", "--json", *args, capture=True))


def vol(*args):
    # Volume tools moved out of the client binary; the node daemon serves
    # local devices directly from the store.
    return json.loads(run("swarmyd", "--json", "vol", *args, capture=True))


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
    def __init__(self, volume, background=False):
        self.background = background
        self.volume = volume
        self.mount = Path.cwd() / "mounted"
        self.mount.mkdir(exist_ok=True)
        self.process = None
        self.mounted = False

    def __enter__(self):
        self.process = subprocess.Popen(
            ["swarmyd", "--json", "vol", "attach", self.volume]
            + (["--background"] if self.background else []),
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
            vol("detach", self.volume)
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
    volume = vol("create", "bench-base:v1")["volume_id"]
    with Attached(volume) as attached:
        attached.mount_disk()
        # script supplies a controlling terminal; bash is explicitly interactive.
        command = shlex.join(["chroot", str(attached.mount), "/bin/bash",
                              "--noprofile", "--norc", "-ic", "echo SWARMY_READY"])
        output = run("script", "-qec", command, "/dev/null", capture=True)
        assert "SWARMY_READY" in output
        report(f"shell_{kind}", start, trial=trial)


def seconds(duration):
    return duration["secs"] + duration["nanos"] / 1e9


def install_trial(background, trial, profile):
    volume = vol("create", "bench-base:v1")["volume_id"]
    # Give both modes the same warm base cache before starting the writer.
    with Attached(volume) as attached:
        run("dd", f"if={attached.device}", "of=/dev/null", "bs=4M",
            "iflag=direct", "status=none")
    with Attached(volume, background) as attached:
        attached.mount_disk()
        shutil.copyfile("/etc/resolv.conf", attached.mount / "etc/resolv.conf")
        assert subprocess.run(["chroot", str(attached.mount), "dpkg-query", "-W", "build-essential"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0
        mounted = []
        try:
            for name in ("dev", "proc", "sys"):
                run("mount", "--rbind", f"/{name}", str(attached.mount / name))
                mounted.append(name)
                run("mount", "--make-rslave", str(attached.mount / name))
            update_start = time.monotonic()
            run("chroot", str(attached.mount), "apt-get", "update")
            update_seconds = time.monotonic() - update_start
            install_start = time.monotonic()
            run("chroot", str(attached.mount), "env", "DEBIAN_FRONTEND=noninteractive",
                "apt-get", "install", "-y", "build-essential")
            install_seconds = time.monotonic() - install_start
        finally:
            for name in reversed(mounted):
                run("umount", "-R", str(attached.mount / name))
        start = time.monotonic()
        flushed = vol("flush", volume, "--mount", str(attached.mount))
        report("install_and_flush", start, profile=profile, background=background,
               trial=trial, apt_update_seconds=update_seconds,
               install_seconds=install_seconds, frozen_seconds=seconds(flushed["frozen"]),
               freeze_wait_seconds=seconds(flushed["freeze_wait"]),
               frozen_chunks_uploaded=flushed["frozen_chunks_uploaded"],
               total_seconds=update_seconds + install_seconds + time.monotonic() - start,
               chunk_upload_amplification=(
                   flushed["device_total"]["chunks_uploaded"] * 262144
                   / flushed["uploads"]["referenced_chunk_bytes"]
                   if flushed["uploads"]["referenced_chunk_bytes"] else None),
               flush=flushed)
        run("chroot", str(attached.mount), "gcc", "--version")
    clone = vol("clone", volume)["volume_id"]
    clear_cache()
    with Attached(clone) as attached:
        attached.mount_disk()
        run("chroot", str(attached.mount), "gcc", "--version")
    print(json.dumps({"metric": "compiler_persisted", "profile": profile,
                      "background": background, "trial": trial}), flush=True)
    return volume


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", type=Path,
                        default=Path(__file__).resolve().parents[2] / "target",
                        help="Cargo target directory; selects binaries from PROFILE below it")
    parser.add_argument("--profile", choices=("debug", "release"), default="release")
    parser.add_argument("--background", choices=("on", "off", "both"), default="both")
    parser.add_argument("--install-only", action="store_true",
                        help="skip the older shell and sequential-read measurements")
    parser.add_argument("--trials", type=int, default=1)
    args = parser.parse_args()
    if args.trials < 1:
        parser.error("--trials must be positive")
    binary_dir = args.target_dir.resolve() / args.profile
    assert (binary_dir / "swarmy").is_file(), f"build {binary_dir / 'swarmy'} first"
    os.environ["PATH"] = str(binary_dir) + os.pathsep + os.environ["PATH"]
    return args


def main():
    args = options()
    assert os.geteuid() == 0, "run as root on a disposable VM"
    root = Path.cwd()
    # Older benchmark environments embed their prefix in the bucket. Keep
    # those paths intact while supporting the explicit namespace setting.
    namespace_key = "SWARMY_S3_PREFIX" if os.environ.get("SWARMY_S3_PREFIX") else "SWARMY_S3_BUCKET"
    namespace = os.environ[namespace_key]
    metadata = os.environ.get("SWARMY_STORE_DIRECTORY")
    modes = (False, True) if args.background == "both" else (args.background == "on",)
    for trial in range(args.trials):
        for background in modes:
            directory = root / f"{args.profile}-{'on' if background else 'off'}-{trial}"
            directory.mkdir()
            os.chdir(directory)
            sample = "sample-" + uuid.uuid4().hex
            os.environ[namespace_key] = namespace + "/" + sample
            os.environ["SWARMY_STORE_DIRECTORY"] = (metadata or "swarmy") + "/" + sample
            try:
                measurement(args, background, trial)
            finally:
                os.chdir(root)
                os.environ[namespace_key] = namespace
                if metadata is None:
                    os.environ.pop("SWARMY_STORE_DIRECTORY", None)
                else:
                    os.environ["SWARMY_STORE_DIRECTORY"] = metadata


def measurement(args, background, trial):
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
    if not args.install_only:
        for trial_index in range(3):
            shell_trial("cold", trial_index)
        volume = vol("create", "bench-base:v1")["volume_id"]
        with Attached(volume) as attached:
            run("dd", f"if={attached.device}", "of=/dev/null", "bs=4M",
                "iflag=direct", "status=none")
        for trial_index in range(3):
            shell_trial("warm", trial_index)
    volume = install_trial(background, trial, args.profile)
    if args.install_only:
        return
    with Attached(volume) as attached:
        attached.mount_disk()
        run("dd", "if=/dev/urandom", f"of={attached.mount}/sequential.bin",
            "bs=1M", "count=128", "conv=fsync", "status=none")
        vol("flush", volume, "--mount", str(attached.mount))
    vol("snapshot", volume)
    clone = vol("clone", volume)["volume_id"]
    clear_cache()
    with Attached(clone) as attached:
        attached.mount_disk()
        run("chroot", str(attached.mount), "gcc", "--version")
        for kind in ("cold", "warm"):
            start = time.monotonic()
            run("dd", f"if={attached.mount}/sequential.bin", "of=/dev/null",
                "bs=256K", "iflag=direct", "status=none")
            report(f"sequential_{kind}", start, bytes=128 * 1024 * 1024)
    vol("show", volume)
    run("swarmy", "vol", "ls")


if __name__ == "__main__":
    main()
