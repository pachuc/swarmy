#!/usr/bin/env python3
"""Run pinned prompts on codex-daytona lanes and normalize Codex JSON events."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time

from tasks import PIN, TASKS, load
from run_swarm import prompt

ROOT = Path(__file__).resolve().parents[1]
METRICS = ("queue_ms", "placement_ms", "inference_ms", "tool_ms", "wait_ms",
           "tokens_per_second", "cost_dollars")


def usage(events):
    """Codex reports cumulative turn usage; prefer the final aggregate if present."""
    total = None
    turns = []
    for event in events:
        if event.get("type") == "turn.completed":
            value = event.get("usage", {})
            turns.append(value)
        if event.get("type") == "thread.completed" and event.get("usage"):
            total = event["usage"]
    source = [total] if total is not None else turns
    if not source:
        return None, None
    return (sum(item.get("input_tokens", 0) for item in source),
            sum(item.get("output_tokens", 0) for item in source))


def tool_count(events):
    calls = [event for event in events if event.get("type") == "item.started" and
             event.get("item", {}).get("type") in ("command_execution", "file_change", "mcp_tool_call", "web_search")]
    return len(calls) if any(event.get("type") == "turn.completed" for event in events) else None


def record(label, task, repeat, wall, events, provider, model, effort):
    input_tokens, output_tokens = usage(events)
    return {"label": label, "environment": "codex-daytona", "task": task,
            "run": repeat, "commit": PIN, "provider": provider, "model": model,
            "effort": effort, "session_id": None, "wall_seconds": wall,
            "input_tokens": input_tokens, "output_tokens": output_tokens,
            "tool_calls": tool_count(events), "passed": None,
            **{name: None for name in METRICS}}


def write_report(output, label, results):
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"label": label, "runs": results}, indent=2) + "\n")


def lane_command(prefix, provider, model, effort, workspace, prompt_file):
    return [*prefix, "--provider", provider, "--model", model, "--effort", effort, "--workspace",
            str(workspace), "--json", "--prompt-file", str(prompt_file)]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("label")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    provider = os.environ.get("BENCH_PROVIDER", "openrouter")
    model = os.environ.get("BENCH_MODEL", "openai/gpt-6-sol")
    effort = os.environ.get("BENCH_EFFORT", "medium")
    prefix = shlex.split(os.environ.get("DAYTONA_LANE_CMD", "codex-daytona run"))
    if not prefix:
        parser.error("DAYTONA_LANE_CMD must name a lane runner")
    if not args.label.replace("-", "").replace("_", "").isalnum():
        parser.error("label must contain only letters, digits, hyphens, and underscores")
    output = args.output or ROOT / ".dev" / "benchmarks" / f"{args.label}-daytona.json"
    results = []
    with tempfile.TemporaryDirectory(prefix="daytona-benchmark-") as temp:
        base = Path(temp)
        for name in TASKS:
            for repeat in (1, 2):
                task = load(name)
                workspace = base / f"{name}-{repeat}"
                prompt_file = base / f"{name}-{repeat}.txt"
                prompt_file.write_text(prompt(task))
                command = lane_command(prefix, provider, model, effort, workspace, prompt_file)
                if args.dry_run:
                    print(f"{name}-{repeat}: {shlex.join(command[:-1] + [f'<prompt:{name}-{repeat}>'])}")
                    continue
                workspace.mkdir()
                start = time.monotonic()
                response = subprocess.run(command, capture_output=True, text=True, check=False)
                wall = time.monotonic() - start
                if response.returncode:
                    raise RuntimeError(f"{name}-{repeat}: lane exited {response.returncode}: {response.stderr}")
                events = [json.loads(line) for line in response.stdout.splitlines() if line.strip()]
                raw = output.parent / f"{args.label}-{name}-{repeat}.jsonl"
                raw.parent.mkdir(parents=True, exist_ok=True)
                raw.write_text(response.stdout)
                results.append(record(args.label, name, repeat, wall, events, provider, model, effort))
    if not args.dry_run:
        write_report(output, args.label, results)
        print(output)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(f"benchmark: {error}", file=sys.stderr)
        sys.exit(1)
