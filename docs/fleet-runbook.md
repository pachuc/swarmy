# Fleet runbook

How swarmy's own development runs on swarmy: a long-lived swarm on one
Hetzner dedicated server, a pool of worker agents, and the driver in
`scripts/fleet/`. This
is the manual version of what the swarm-model and cloud-topology goals will
automate; every step here is a plain command.

For node sizing, provisioning, upgrades, recovery, teardown and costs, use
[REMOTE.md](REMOTE.md). Configure the driver from
`scripts/fleet/fleet.example.toml` into the ignored `scripts/fleet/fleet.toml`;
set the pool size and provider choices and restrict file permissions. Run
`swarmy --swarm NAME doctor` before launching work.

## Running the driver on a control node

`swarm = "NAME"` in `fleet.toml` passes `--swarm NAME` to the client.
Tunnels open on demand, and several swarms can be used at once. On the
swarm's control node, `swarm = "local"` (or no key) exports that node's
`[api]` URL and token from `.swarmy/config.toml`. Override the path with
`node_config` (relative to the checkout) or `FLEET_NODE_CONFIG`. The driver
reserves `local`: a registered swarm literally named `local` cannot be
selected through the fleet.

## Perf baseline runners

`benchmarks/run-swarm.sh SWARM LABEL` runs the three fixed tasks twice each
through `fleet benchmark`, records each invocation's wall time, and writes
`.dev/benchmarks/LABEL-*`; run it on each
swarm's control node with `swarm = "local"` (previous section).
`benchmarks/run-daytona.py LABEL` runs the same prompts through codex-daytona
in disposable remote sandboxes with `--no-publish`; Codex never runs on the
laptop. Both read `BENCH_PROVIDER`, `BENCH_MODEL`, and `BENCH_EFFORT`. The
baseline uses the same OpenRouter model everywhere so it compares
infrastructure, not models: `BENCH_PROVIDER=openrouter
BENCH_MODEL=meta/muse-spark-1.3-contributor BENCH_EFFORT=medium`. The
codex-daytona leg needs `OPENROUTER_API_KEY` in the launcher's `.env` (it is
placed in the sandbox as a private file) and `CODEX_DAYTONA_DIR` when the
launcher is not at `~/code/codex-daytona`. The runner closes by invoking
`scripts/fleet/fleet report --swarm SWARM --label LABEL --session ID
--wall ID=SECONDS ...`, which prints each run's wall time as `wall_s` next to
`duration_s`; `benchmark` and `report` need no `fleet.toml` and can also run
ad hoc against the local API with `--swarm local`.

## Daily operation

- Each task runs in a fresh side conversation on its worker (`run --agent
  NAME --new`): same disk and memory files, empty context. The worker's main
  conversation is unused. On a metered provider this keeps every turn's
  billed context to the current task instead of the worker's whole history.
  `fleet resume` and `fleet kill` address the task's own session.
- Use `fleet resume TASK "text"` to queue a follow-up for the next step boundary
  without cancelling a build or tool call. Use `fleet resume TASK --interrupt
  "text"` only when the current turn must stop before the new instruction.
  After an interrupt, the replacement text is queued rather than waiting for
  idle: pending queued messages can keep the session runnable. They are delivered
  in order before the replacement text. An idle interrupt is harmless.
  Direct CLI callers can use `swarmy run --session ID --queue "text"`.
- `scripts/fleet/fleet status`: one line per worker with task, provider,
  model, state and its age, task elapsed time, and cost. A recent wait can show
  `waiting_inference 2m; waiting for inference: ...`. A session in
  `waiting_inference` or `leased` longer than `stall_minutes` (default 10)
  shows `STALLED` in the state column and makes status exit 2; an operator or
  monitoring loop should investigate. Other statuses exit 0. The age is the
  last durable state transition from `session ls --json`, not the task age.
  Older sessions without that timestamp show `?` until their next transition.
  A provider-limited worker holds no lease and resumes when the limit clears.
- Context compaction starts at the model window minus a 16,384-token reserve.
  The successor links back to its archived predecessor.
- Launch: `scripts/fleet/fleet launch TASK --provider P --model M`. The
  driver prefers an idle worker created with that provider and creates one
  while the pool is below `workers`. Workers keep their disks, so the second
  task on a worker builds in a minute or two instead of ten.
- Collect and release: `fleet collect TASK` checks the pushed branch, links
  its head SHA in tasky, moves the task to testing, and prints the report.
  After a commit on the integration branch names `tasky <id>`,
  `fleet release TASK --into BRANCH` frees the worker (`integration` in
  fleet.toml sets the default). `fleet kill TASK` ends a turn that should
  not continue, including a parked one.
- Review every pull request before merging. Medium-reasoning workers do what
  the task text says; the task text and the review are the quality control.
- Cost: `swarmy agent show worker-N --swarm dev` totals a worker's spend;
  `swarmy cost --agent worker-N --swarm dev` shows the same spend by day.

## Subscription limits

All ChatGPT workers and the codex-daytona lanes share one subscription's
usage limit. When it trips, the gateway opens that entry's breaker, the
affected workers park without a lease, and `fleet status` shows the reason.
They continue when the limit clears; a turn gives up only after
`[inference] max_wait_secs` (default one hour). OpenRouter workers are
unaffected. To stop waiting instead, `fleet kill TASK`.

To move the fleet to another model, for example when a subscription's
weekly limit will not clear for days, run `fleet switch --provider P --model
M [--effort E] [--default] [WORKER...]`. It sets each named worker (every
worker by default) to that provider and model with its failover route
cleared, records the change so `fleet launch` picks matching workers, and
with `--default` makes it the `fleet.toml` default for new workers. Busy
workers are skipped: a session that changes model mid-task carries the old
model's transcript over, which some models handle badly. Kill or finish
their tasks, switch, and relaunch the tasks on fresh sessions.

## Operator handoff

The method an operator session follows day to day (review loop, merge criteria, root suites on the node, steering workers, rebuilding the laptop CLI) is in [fleet-operator-handoff.md](fleet-operator-handoff.md). The node-side suite scripts are kept in `scripts/node-suites/`.
