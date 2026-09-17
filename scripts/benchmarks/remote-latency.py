#!/usr/bin/env python3
"""Observe 21 serial bash calls from swarmy run --json; exclude the first call.

Usage: python3 scripts/benchmarks/remote-latency.py OUTPUT.json SWARMY [ARGS...]
The fake provider should emit 21 `printf LATENCY_OK` calls, then end the turn.
Times are client event-arrival intervals, not isolated SSH transport overhead.
"""
import datetime
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time


def observe(output, command):
    starts = {}
    samples = []
    session = None
    with output.with_suffix(".stderr").open("w") as errors, output.with_suffix(".jsonl").open("w") as raw:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=errors,
                                   text=True, bufsize=1)
        try:
            for line in process.stdout:
                observed = time.monotonic_ns()
                raw.write(line)
                value = json.loads(line)
                if value.get("event") == "session_created":
                    session = value["session_id"]
                event = value.get("value", {})
                if "tool_call_requested" in event:
                    call = event["tool_call_requested"]["call"]
                    assert call["tool"] == "bash", call
                    assert call["call_id"] not in starts, call
                    starts[call["call_id"]] = observed
                if "tool_call_completed" in event:
                    done = event["tool_call_completed"]
                    detail = json.loads(done["result"]["completed"]["output"])
                    assert detail["stdout"] == "LATENCY_OK", detail
                    assert detail["exit_code"] == 0 and not detail["timed_out"], detail
                    samples.append({"call_id": done["call_id"],
                                    "requested_ns": starts.pop(done["call_id"]),
                                    "completed_ns": observed})
            assert process.wait() == 0, "swarmy run failed; inspect the stderr file"
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    assert len(samples) == 21 and not starts, f"expected 21 completed calls, got {len(samples)}"
    for sample in samples:
        sample["milliseconds"] = (sample["completed_ns"] - sample["requested_ns"]) / 1e6
    warm = [sample["milliseconds"] for sample in samples[1:]]
    report = {
        "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "command": command,
        "session_id": session,
        "first_excluded_ms": samples[0]["milliseconds"],
        "mean_ms": statistics.mean(warm),
        "median_ms": statistics.median(warm),
        "min_ms": min(warm),
        "max_ms": max(warm),
        "samples": samples,
    }
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "samples"}))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    observe(Path(sys.argv[1]), sys.argv[2:])
