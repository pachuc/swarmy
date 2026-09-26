#!/usr/bin/env python3
"""Run the fixed tasks through the fleet driver's isolated benchmark command."""
import argparse
import json
import os
import re
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile

from tasks import PIN, TASKS, load

ROOT = Path(__file__).resolve().parents[1]
DRIVER = ROOT / "scripts/fleet/fleet"


def prompt(task):
    return ("Before cloning or building, check whether both ~/.cargo-target and "
            "~/.cargo/registry have no entries (a missing directory is empty). "
            "Print BENCH_COLD=<true-or-false> as your first output, using true "
            "only when both are empty. Then continue.\n\n"
            f"Clone https://github.com/pachuc/swarmy.git into a fresh directory, "
            f"check out commit {PIN} on a new local benchmark branch, then do this task. "
            f"Keep the clone and report the commands and results.\n\n{task['prompt']}")


def cold_from_events(events):
    for event in events:
        if event.get("event") == "session_event":
            match = re.search(r"BENCH_COLD=(true|false)\b", json.dumps(event.get("value", {})))
            if match:
                return match.group(1) == "true"
    raise ValueError("session did not print BENCH_COLD at the start")


def commands(remote, provider, model, effort, directory, image=None):
    for name in TASKS:
        task = load(name)
        for repeat in (1, 2):
            file = directory / f"{name}-{repeat}.txt"
            file.write_text(prompt(task))
            yield name, repeat, [str(DRIVER), "benchmark", "--remote", remote,
                                 "--provider", provider, "--model", model,
                                 "--effort", effort, "--prompt-file", str(file),
                                 *(["--image", image] if image else [])]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("remote")
    parser.add_argument("label")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    provider = os.environ.get("BENCH_PROVIDER", "chatgpt")
    model = os.environ.get("BENCH_MODEL", "gpt-6-sol")
    effort = os.environ.get("BENCH_EFFORT", "medium")
    # The benchmark tasks build swarmy, so they need an image with the Rust
    # toolchain (the swarmy-dev recipe); the remotes default to base-ubuntu.
    image = os.environ.get("BENCH_IMAGE") or None
    if not args.label.replace("-", "").replace("_", "").isalnum():
        parser.error("label must contain only letters, digits, hyphens, and underscores")
    with tempfile.TemporaryDirectory(prefix="swarmy-benchmark-") as temp:
        runs = list(commands(args.remote, provider, model, effort, Path(temp), image))
        sessions = []
        records = []
        output_dir = ROOT / ".dev" / "benchmarks"
        for name, repeat, command in runs:
            if args.dry_run:
                print(f"{name}-{repeat}: {shlex.join(command[:-1] + [f'<prompt:{name}-{repeat}>'])}")
                continue
            output = subprocess.run(command, text=True, capture_output=True, check=False)
            if output.returncode:
                raise RuntimeError(f"{name}-{repeat}: fleet benchmark exited {output.returncode}: {output.stderr}")
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / f"{args.label}-{name}-{repeat}-swarm.jsonl").write_text(output.stdout)
            events = [json.loads(line) for line in output.stdout.splitlines() if line.strip()]
            if any(event.get("event") == "run_outcome" and event.get("outcome") == "failed" for event in events):
                raise RuntimeError(f"{name}-{repeat}: session reported failure")
            ids = [event["session_id"] for event in events
                   if event.get("event") in ("session_created", "session_opened")]
            if not ids:
                raise RuntimeError(f"{name}-{repeat}: no session id in fleet output")
            cold = cold_from_events(events)
            sessions.append(ids[0])
            records.append({"label": args.label, "environment": args.remote,
                            "task": name, "run": repeat, "session_id": ids[0],
                            "cold": cold})
            (output_dir / f"{args.label}-swarm-runs.json").write_text(
                json.dumps({"label": args.label, "runs": records}, indent=2) + "\n")
            (output_dir / f"{args.label}-swarm-sessions.json").write_text(json.dumps(sessions, indent=2) + "\n")
            print(f"{name}-{repeat}: {ids[0]}", flush=True)
        report = [str(DRIVER), "report", "--label", args.label]
        for session in sessions if not args.dry_run else [f"<{name}-{repeat}-session>" for name, repeat, _ in runs]:
            report += ["--session", session]
        if args.dry_run:
            print(shlex.join(report))
        else:
            subprocess.run(report, check=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"benchmark: {error}", file=sys.stderr)
        sys.exit(1)
