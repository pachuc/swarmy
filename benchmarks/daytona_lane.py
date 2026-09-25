#!/usr/bin/env python3
"""Run one benchmark prompt through codex-daytona and print Codex JSON events.

The launcher creates a disposable remote sandbox, runs Codex there without
publishing a branch, and streams Codex's JSON event lines to stdout. This
adapter prints one `benchmark_cache` event first (from the BENCH_COLD line the
prompt asks Codex to print), then the Codex events, which is the contract
`run-daytona.py` expects from a lane. Codex never runs on the laptop.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

PROVIDERS = ("chatgpt", "openrouter")


def launcher():
    root = Path(os.environ.get("CODEX_DAYTONA_DIR", Path.home() / "code" / "codex-daytona"))
    return ["node", "--env-file-if-exists=" + str(root / ".env"), str(root / "src" / "cli.mjs")]


def command(args, prompt):
    return [*launcher(), "run", "--provider", args.provider, "--model", args.model,
            "--effort", args.effort, "--no-publish", "--config", str(args.config),
            "--prompt", prompt]


def events_from(output):
    """Codex events arrive through a remote terminal; keep only JSON object lines."""
    events = []
    for line in output.replace("\r", "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            events.append(value)
    return events


def cold_from(events):
    text = json.dumps(events)
    match = re.search(r"BENCH_COLD=(true|false)\b", text)
    if not match:
        raise ValueError("Codex did not print BENCH_COLD")
    return match.group(1) == "true"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", required=True, choices=PROVIDERS)
    parser.add_argument("--model", required=True)
    parser.add_argument("--effort", required=True)
    parser.add_argument("--workspace", required=True, type=Path)
    parser.add_argument("--prompt-file", required=True, type=Path)
    parser.add_argument("--json", action="store_true", required=True)
    parser.add_argument("--config", type=Path,
                        default=Path(__file__).resolve().parents[1] / ".codex-daytona.json")
    args = parser.parse_args(argv)
    if not args.workspace.is_dir():
        parser.error("workspace must be an existing directory")
    prompt = args.prompt_file.read_text()
    run = command(args, prompt)
    if os.environ.get("BENCH_DRY_RUN") == "1":
        print(json.dumps({"event": "benchmark_cache", "cold": True}))
        print(shlex.join(run[:-1] + ["<prompt>"]))
        return 0
    result = subprocess.run(run, capture_output=True, text=True, check=False)
    sys.stderr.write(result.stderr)
    if result.returncode:
        raise RuntimeError(f"codex-daytona exited {result.returncode}")
    events = events_from(result.stdout)
    print(json.dumps({"event": "benchmark_cache", "cold": cold_from(events)}))
    for event in events:
        print(json.dumps(event))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(f"daytona lane: {error}", file=sys.stderr)
        sys.exit(1)
