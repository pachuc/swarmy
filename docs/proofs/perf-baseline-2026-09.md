# Perf baseline, September 2026: dev2 (split, S3) versus codex-daytona

Date: 2026-09-25 to 2026-09-26. Model on every environment: OpenRouter
`meta/muse-spark-1.3-contributor` at medium effort, so the comparison is
about infrastructure and harness, not the model. The single-node `dev`
swarm was retired before its leg ran, at the operator's decision, because
dev2 against codex-daytona answers the question that matters.

## Environments

- **dev2**: split swarm. Control node m6i.xlarge (FoundationDB, NATS,
  scheduler, worker, gateway, API), sandbox node m6id.4xlarge with local
  NVMe scratch, chunks in the S3 bucket `swarmy-pachu-dev2`. Sessions ran
  on image `swarmy-dev:dev2` (Rust toolchain, 6 GiB memory limit, empty
  cargo caches). swarmy at commit d665659. Runs driven by
  `benchmarks/run-swarm.sh local dev2-muse` on the control node.
- **codex-daytona**: Codex CLI 0.156 inside a fresh Daytona sandbox per run
  (snapshot `swarmy-dev-5f0d252174e9`, 8 GB memory cap, toolchain and
  `cargo fetch` at setup, no compiled target), driven by
  `benchmarks/run-daytona.py daytona-muse` with the launcher's
  `--provider openrouter --no-publish`.

## The tasks

| task | the model must | what gets built |
|---|---|---|
| small | add a checklist section to `docs/REMOTE.md`, run `make check` | the whole workspace, from cold |
| medium | tighten `validate_remote_name`, run `cargo test -p swarmy-config` | one small crate |
| large | add `GET /v1/version`, a client method, an integration test on the dev stack | the API and client crates |

"Small" is small for the model and the largest build of the three.

## Wall time per run (seconds)

| task | run | dev2 | codex-daytona | daytona tool calls | daytona input tokens |
|---|---|---|---|---|---|
| small | 1 | 1320 | 3381 | 268 | 20,657,122 |
| small | 2 | 1233 | 481 | 45 | 1,950,927 |
| medium | 1 | 183 | 568 | 48 | 1,767,639 |
| medium | 2 | 202 | 346 | 40 | 1,590,937 |
| large | 1 | 749 | 618 | 70 | 3,571,386 |
| large | 2 | 954 | 619 | 76 | 4,250,733 |
| total | | 4642 | 6014 | | |

dev2 wall time is the session id's ULID timestamp to the idle state
timestamp (the runner recorded no wall time; task M6). codex-daytona wall
time is the launcher's own measurement.

## Per-request metrics on dev2

From `fleet report` over the six sessions (medians of medians):

- append to first token 2.3 s (p95 3.5 s); inference request 5.4 s median
  (p95 7.9 s); tool round trip 155 ms median (p95 4.3 s, builds and tests);
  placement 400 ms; one cold hydration of 1 MiB; no waits, retries, or
  errors in any session; $0.11 for all six runs.

The full table is below. Two figures in it are wrong and explained in the
findings: `duration_s` and `tps_*`.

## dev2-muse

| task | turns | duration_s | a2f_p50_ms | a2f_p95_ms | infer_p50_ms | infer_p95_ms | tps_mean | tps_p95 | tool_calls | tool_p50_ms | tool_p95_ms | place_cold | place_warm | place_p50_ms | chunks_fetched | bytes_fetched | waits | retries | errors | input_tokens | cached_input_tokens | output_tokens | reasoning_tokens | cost_dollars |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 01M3DBT0RBRHS2KVE8E5HFC8FA | 1 | 38.8 | 1879.5 | 1879.5 | 7888.8 | 7888.8 | 6161.6 | 19823.9 | 42 | 861.4 | 4481.4 | 1 | 0 | 475.3 | 4 | 1048576 | 0 | 0 | 0 | 272420 | 135312 | 5407 | 3278 | 0.0151 |
| 01M3DD2ABEZ0AZDCZ3TWP66EMQ | 1 | 37.0 | 2316.9 | 2316.9 | 5438.5 | 5438.5 | 10432.7 | 27846.6 | 26 | 155.4 | 3281.5 | 0 | 1 | 444.6 | 0 | 0 | 0 | 0 | 0 | 263582 | 119184 | 3541 | 1716 | 0.0154 |
| 01M3DE7YF3RJ8MJH9N3DCZB86S | 1 | 28.8 | 2164.0 | 2164.0 | 5469.6 | 5469.6 | 5580.9 | 15290.1 | 16 | 138.7 | 4663.1 | 0 | 1 | 384.9 | 0 | 0 | 0 | 0 | 0 | 276792 | 131600 | 3311 | 1056 | 0.0154 |
| 01M3DEDHQ7RRTHKBAYRVEJK7A2 | 1 | 24.1 | 3490.0 | 3490.0 | 5347.7 | 5347.7 | 5941.9 | 15413.4 | 20 | 124.1 | 3041.7 | 0 | 1 | 380.1 | 0 | 0 | 0 | 0 | 0 | 241058 | 128400 | 4108 | 1758 | 0.0123 |
| 01M3DEKQDAWQB6GBKJ1EV77VXJ | 1 | 198.9 | 2425.7 | 2425.7 | 4033.3 | 4033.3 | 13356.3 | 71574.3 | 52 | 150.1 | 3620.7 | 0 | 1 | 401.2 | 0 | 0 | 0 | 0 | 0 | 533220 | 299408 | 7867 | 4701 | 0.0256 |
| 01M3DFAK6SYZADYYVZ0EZAT03X | 1 | 680.5 | 1943.0 | 1943.0 | 4404.3 | 4404.3 | 10349.0 | 20592.8 | 28 | 174.0 | 4143.6 | 0 | 1 | 383.6 | 0 | 0 | 0 | 0 | 0 | 481712 | 243728 | 6577 | 3596 | 0.0256 |
| total | 6 | 1008.1 | 2316.9 | 3490.0 | 5438.5 | 7888.8 | 8393.6 | 27625.7 | 184 | 174.0 | 4339.2 | 1 | 5 | 401.2 | 4 | 1048576 | 0 | 0 | 0 | 2068784 | 1057632 | 30811 | 16105 | 0.1094 |

## medians

| label | turns | duration_s | a2f_p50_ms | a2f_p95_ms | infer_p50_ms | infer_p95_ms | tps_mean | tps_p95 | tool_calls | tool_p50_ms | tool_p95_ms | place_cold | place_warm | place_p50_ms | chunks_fetched | bytes_fetched | waits | retries | errors | input_tokens | cached_input_tokens | output_tokens | reasoning_tokens | cost_dollars |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| dev2-muse | 1 | 38.8 | 2316.9 | 2316.9 | 5438.5 | 5438.5 | 10349.0 | 20592.8 | 28 | 155.4 | 4143.6 | 0 | 1 | 401.2 | 0 | 0 | 0 | 0 | 0 | 276792 | 135312 | 5407 | 3278 | 0.0154 |

## What differs and why

**Model latency dominates and is the same everywhere.** About 5.4 s per
model request through OpenRouter, 2.3 s of it before the first token. A run
with 43 requests spends four minutes in the model. Nothing in either
environment changes that.

**Swarmy's own overhead is small.** A tool round trip on the split layout
(NATS to the sandbox node and back) costs about 155 ms; forty tool calls
add six seconds to a twenty-minute run. Placement and disk hydration from
S3 cost under a second per session. This is the split-architecture
question, and the answer is that the extra hop is not measurable against
model and build time.

**Cold Rust builds are the real cost, and memory caps multiply it.** Both
environments spent most of the small task in `make check`, whose full
workspace build compiles `aws-sdk-ec2` and `aws-lc-sys`. On dev2 the
6 GiB image limit killed the build repeatedly (34 and 31 kills across the
two runs) before single-job retries passed; on daytona the 8 GB cap did the
same. Daytona's first small run took 56 minutes because the model then
spent 268 tool calls polling the retried build every nine seconds; its
second small run reported done in 8 minutes with `make check` still
failing, so that run did not meet the pass criterion. The medium task,
which builds one crate, took three minutes on dev2 and six to nine on
daytona.

**Harness behaviour shows up as step count.** On medium and large, dev2
finished in 17 to 21 model requests where Codex used 40 to 76 commands.
Fewer steps at the same per-request latency is most of dev2's advantage on
those tasks; it is a harness difference (prompt, tools, waiting on
processes), not a machine difference.

**Provider variance** was low: no rate-limit waits or provider failures in
any of the twelve runs.

## Regressions and surprises, with follow-up tasks

1. The per-turn metrics record caps at 64 stages and 16 inference requests.
   A fleet task is one prompt followed by dozens of tool calls, so every
   dev2 session dropped hundreds of stages (452 on the first small run)
   and lost its idle stage, which is why the report's `duration_s` shows
   39 s for a 22-minute run. Task M5 (01M3DGH2ZV1BF6T3F8T9HRNF9E).
2. `output_tokens_per_second` divides by the streaming window, which is
   18 ms when OpenRouter delivers a response in one chunk, giving figures
   like 19,823 tokens per second. Same task.
3. The swarm runner records no wall time and `fleet report` needs a fleet
   config with a GitHub token on the node. Task M6
   (01M3DGH31XNYDX3GEMGKPSA07T).
4. Cold `make check` under a memory cap is the largest cost in both
   environments. Recorded in `backlog/build-time.md` for discussion (drop
   the AWS SDK from the common path, a shared sccache, warm images,
   cheaper profiles, 8 GiB workers, scoped checks).
5. Pass criteria are not evaluated by the runners; pass/fail above was
   read from the transcripts by hand. Folded into M6.
6. The `cold` flag comes from the model's own `BENCH_COLD` line and
   disagreed with the runner's reading in several runs; treat it as
   advisory until M6 records it from the environment.

## Verdict

The split architecture holds up: against the single-node design it adds a
hop that costs about 150 ms per tool call and under a second per session
for placement and S3 hydration, invisible next to 5 s model requests and
multi-minute builds. Against codex-daytona, dev2 completed all six runs
within their pass criteria, in 77 minutes total to daytona's 100, and did
it in fewer model steps on the two tasks that do not hinge on the cold
workspace build. The thing to fix next is not the infrastructure path but
build time under memory caps, which hurt both environments equally.

## Reproducing

dev2: on the control node, `BENCH_PROVIDER=openrouter
BENCH_MODEL=meta/muse-spark-1.3-contributor BENCH_EFFORT=medium
BENCH_IMAGE=swarmy-dev:dev2 benchmarks/run-swarm.sh local LABEL`, then
`scripts/fleet/fleet report --label LABEL --session ID ...` with the ids in
`.dev/benchmarks/LABEL-swarm-sessions.json`. Session ids for this run:
01M3DBT0RBRHS2KVE8E5HFC8FA, 01M3DD2ABEZ0AZDCZ3TWP66EMQ, 01M3DE7YF3RJ8MJH9N3DCZB86S, 01M3DEDHQ7RRTHKBAYRVEJK7A2, 01M3DEKQDAWQB6GBKJ1EV77VXJ, 01M3DFAK6SYZADYYVZ0EZAT03X.

codex-daytona: `BENCH_PROVIDER=openrouter
BENCH_MODEL=meta/muse-spark-1.3-contributor BENCH_EFFORT=medium
python3 benchmarks/run-daytona.py LABEL` with `OPENROUTER_API_KEY` in the
launcher's `.env`. Receipts and raw Codex events for this run are in
`.dev/benchmarks/daytona-muse-*`.
