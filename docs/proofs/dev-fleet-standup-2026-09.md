# Development fleet proof, September 2026: long-lived swarm, runbook, parallel tasks

Date: 2026-09-23 to 2026-09-26. Task 01M33VTRF36SH139438WZKZ4GH (goal
dev-fleet). This document collects the evidence already in the repository
and on GitHub for acceptance items 1 to 4; items 5 and 7 were moved out on
2026-09-27 and are listed under "What was not verified".

## What was built

- A long-lived swarm that swarmy's own development runs on. The first one,
  `dev`, was a single m6id.2xlarge node brought up on 2026-09-23 with the
  control plane on the node (`remote up dev --services node`, commit
  eeffd46). Its successor, `dev2`, is the split layout from 2026-09-24: an
  m6i.xlarge control node with no sandboxes and an m6id.4xlarge sandbox node
  with four sandboxes on local NVMe, chunks in the S3 bucket
  `<swarm-bucket>` (`docs/fleet-runbook.md`, "The split layout in
  practice"; `docs/proofs/perf-baseline-2026-09.md`, "Environments"). `dev`
  was retired on 2026-09-26 before its perf baseline leg ran, because dev2
  answered the question that mattered.
- The runbook, `docs/fleet-runbook.md` (237 lines). Added 2026-09-23
  (4b2ba81) and grown in nine later commits through 2026-09-26 (d06334c),
  including what the first split bring-up taught (3640cfa, 2026-09-24). It
  covers shape and cost, bring-up, running the driver on a control node,
  daily operation, subscription limits, disk, adding a node, in-place
  upgrade, and recovery.
- The driver, `scripts/fleet/fleet` (1166 lines, Python, tests in
  `scripts/fleet/test_fleet.py`), merged as pull request 99 on 2026-09-23:
  `launch`, `status`, `collect`, `release`, `kill`, `resume`, `reset`,
  `benchmark`, and `report`.

## Evidence per acceptance item

**1. Two days up, laptop disconnected overnight.** The two swarms together
span 2026-09-23 to today; dev2 alone has been up since 2026-09-24. The
runbook's design puts the control plane on the node so the laptop can
disconnect ("Control plane: on the node, the laptop can disconnect";
recovery section, "The laptop closed: nothing stops"). The merged
fleet-worker pull requests show work landing across every UTC night of the
run: 112 through 116 opened and merged between 22:58 on 2026-09-23 and
01:41 on 2026-09-24, and 160, 161, 164, and 165 between 23:44 on
2026-09-25 and 09:34 on 2026-09-26. What the repository does not hold is a
saved `swarmy remote ls` transcript from a reconnect; the claim
rests on the timeline and the runbook, not on a captured status line.

**2. Three tasks in parallel to merged pull requests, two providers.**
Between 2026-09-23 and 2026-09-26, 48 pull requests whose title starts with
`tasky ` (the driver's naming) were merged on master: 10 on 09-23, 15 on
09-24, 13 on 09-25, 10 on 09-26 (`gh pr list --state merged`). Every one
passed the `linux` CI job before merge (spot-checked on 105 and 161; 161
also passed `openapi-compat`). Using pull-request open and merge times as a
lower bound on overlap (the work itself started earlier): on 2026-09-23,
103, 104, and 105 were all open from 08:09 to 08:17; on 2026-09-24, 113,
114, 115, and 116 from 00:21 to 00:34; on 2026-09-25, 140, 142, 145, 146,
and 148 from 10:07 to 10:43; on 2026-09-26, 150, 160, 161, and 164 from
02:03 to 04:30. Models: the fleet config shipped with OpenRouter
`openai/gpt-6-sol` at medium effort from 2026-09-23 (eeffd46,
`scripts/fleet/fleet.example.toml`), and `gpt-6-sol` was added to the
ChatGPT catalog the same day (9d117c6) so workers could run it on the
subscription; the runbook's subscription section assumes ChatGPT workers
from its first commit. The perf baseline ran six sessions on OpenRouter
`meta/muse-spark-1.3-contributor`, 183 to 1320 s each, $0.11 for all six
(`docs/proofs/perf-baseline-2026-09.md`, wall time and dev2-muse tables).
Per-task provider is shown live by `fleet status` and `fleet report` but
was not recorded per pull request in the repository, so which of the 48
ran on ChatGPT and which on OpenRouter is not reconstructible from git.

**3. Rate limit shows as a waiting session and the task completes.** No
limit tripped during the recorded runs: the perf baseline reports "no
rate-limit waits or provider failures in any of the twelve runs". The
forced path is covered by tests merged in pull request 152 on 2026-09-25:
`rate_limit_opens_durable_breaker_and_keeps_provider_text`
(`crates/swarmy-e2e/tests/gateway.rs`),
`rate_limit_waits_without_a_worker_lease_then_recovers` and
`all_entries_open_parks_until_earliest_retry`
(`crates/swarmy-e2e/tests/worker.rs`), and the driver test at
`scripts/fleet/test_fleet.py` line 158 asserting `fleet status` prints
`waiting for inference: 429 rate limited`. The runbook documents the
operator view ("Subscription limits": the breaker opens, workers park
without a lease, status shows the reason, they continue when it clears,
`max_wait_secs` defaults to one hour).

**4. Peak memory under the node's RAM with headroom.** The runbook records
three workers building at once at 3 GiB used, load 4, on the node
(`docs/fleet-runbook.md`, "Shape and cost"). Each sandbox is capped at
6144 MiB by `images/swarmy-dev/recipe.toml`; the cleanup baseline measured
under an 8 GiB cgroup cap (`docs/proofs/cleanup-baseline-2026-09.md`,
"Machine"). Four sandboxes at the cap total 24 to 32 GiB on a 64 GiB
m6id.4xlarge, so the node keeps at least half its RAM free. The cost of the
cap is that a multi-job workspace build is killed (16-, 4-, and 2-job
attempts all OOM-killed on `aws-sdk-ec2`; single job succeeds in 1603 s
cold), recorded in the same baseline and in `backlog/build-time.md`.

## What was not verified

- Item 5, a newcomer following the runbook to bring up a second swarm,
  moved out on 2026-09-27 to post-cleanup QA in `backlog/final-qa.md`.
- Item 7, the opt-in API latency benchmark from pull request 123 with a
  baseline on the current node, moved on 2026-09-27 into the cleanup
  goal's final measurement task.
- A reconnect `swarmy remote ls` transcript and a per-pull-request
  provider record (see items 1 and 2 above).

## Known limits

- The 6 GiB sandbox cap (8 GiB on the baseline sandbox) forces
  `CARGO_BUILD_JOBS=1`: a cold workspace build is 1603 s, a cold test
  build 1806 s, and clippy 414 s warm. Options are in
  `backlog/build-time.md`.
- Workers cannot run the full-feature test suite under the cap and leave
  the CLI tests to CI; the `cli_auth` suite also fails under the shared
  target directory (cleanup baseline).
- Root-only suites (`swarmy-volume` nbd and image, `swarmyd` node, chaos
  acceptances) are run by hand on a node before merging, not by workers.
