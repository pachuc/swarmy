# Fixed coding benchmark protocol

This protocol compares `dev` (single-node swarm), `dev2` (split control and
sandbox nodes), and codex-daytona lanes without changing the work. It is a
measurement plan, not a claim about performance. All tasks use the same pinned
repository commit `8c670aa1b9926cd23badb8a056d2ac68751ad520`; see
`benchmarks/tasks/{small,medium,large}.md` for the exact prompts and mechanical
pass checks. Never merge benchmark branches. Run every task twice on each
environment in a fresh clone. Warm neither run selectively: record whether
images, dependencies, and cargo targets were cold or warm.

Use the same `BENCH_PROVIDER`, `BENCH_MODEL`, and `BENCH_EFFORT` on every runner;
defaults are `chatgpt`, `gpt-6-sol`, and `medium`, matching the fleet and
codex-daytona subscription lanes. OpenRouter is a fallback only when every
environment can use the same provider and model; never compare different
providers as if they were one baseline. `benchmarks/daytona-lane.sh` is the
default adapter. It accepts `--provider`, `--model`, `--effort`, `--workspace`,
`--json`, and `--prompt-file`, runs `codex exec --json` under the state directory
in `CODEX_LANE_STATE` (default `~/.local/state/cdx-lane2`), and rejects providers
other than `chatgpt`. Set `DAYTONA_LANE_CMD` for another lane or installation.
Do not silently substitute a different provider or effort.

From a laptop connected to one remote at a time:

```sh
benchmarks/run-swarm.sh dev baseline-dev --dry-run
benchmarks/run-swarm.sh dev baseline-dev
benchmarks/run-swarm.sh dev2 baseline-dev2
CODEX_LANE_STATE=~/.local/state/cdx-lane2 \
  python3 benchmarks/run-daytona.py baseline-daytona
python3 -m unittest discover -s benchmarks -p 'test_*.py'
```

The swarm runner calls the fleet driver's `benchmark` action for each isolated
session, waits for completion, and invokes `scripts/fleet/fleet report
--label LABEL --session ID ...` on the six session IDs. The fleet driver reads
its remote from `scripts/fleet/fleet.toml`; configure it to match the runner
remote. The `report` action lands in a sibling task, so do not run the live
script until that driver action is installed. The driver uses ephemeral
sessions rather than tasky assignments, so benchmark branches do not consume
real task IDs or workers.
The swarm runner asks each session to print `BENCH_COLD=true` or `false`
before cloning, based on whether both its cargo target and registry are empty.
It records that Boolean per run, along with session IDs and raw event JSONL, in
`.dev/benchmarks/` so a report can be retried without launching another run.
The lane adapter records the same cache state before starting Codex. The
daytona runner writes `.dev/benchmarks/LABEL-daytona.json` (or `--output`)
with `{ "label": LABEL, "runs": [...] }`. Each run includes task, run (1 or 2),
environment, commit, provider, model, effort, session_id, wall_seconds,
input_tokens, output_tokens, tool_calls, passed, cold, queue_ms, placement_ms,
inference_ms, tool_ms, wait_ms, tokens_per_second, and cost_dollars. Unavailable
metrics are JSON `null`, not zero. Token counts come from Codex `turn.completed`
usage (or the final aggregate `thread.completed` usage); tool calls count
`item.started` tool events. Codex does not expose swarm-specific placement or
queue timing. Store the original JSONL alongside the report when running real
lanes so counts can be audited.

After each run apply the task's pass criterion to the resulting clone. Record
its Boolean outcome, start/end UTC, wall time, token totals, tool calls, cost,
per-turn latency, tokens per second, placement and tool time, waits, errors,
node shape, image, and cache state. Do not interpret a missing observation as
zero. Compare successful runs by task and environment, report both raw samples
and median/range, and separate model time from placement and tool time. Report
failures, retries, cold starts, and provider rate limits rather than discarding
them. Two runs are a baseline, not a statistical significance claim.
