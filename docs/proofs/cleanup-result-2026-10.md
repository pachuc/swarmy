# Cleanup result, 2026-10

The historical baseline is [September 2026](cleanup-baseline-2026-09.md) at `b2d944f`. This measurement uses `350ec17` (the task branch also contains an empty WIP commit). No other fleet worker was compiling against this shared target during the measurements. The baseline likewise says nothing else used its shared target.

## Machine and method

- Fleet sandbox: `nproc` = 32; `free -g` = 123 GiB total, 0 GiB swap; cgroup `memory.max` = 25769803776 (24 GiB). Baseline: 16 vCPUs, 61 GiB host RAM, 8 GiB cgroup. **This is not the same machine shape.** All timings use `CARGO_BUILD_JOBS=1` to match the baseline, but the different CPU and memory make elapsed-time comparisons directional, not controlled speedups.
- Pinned Rust toolchain, `CARGO_TARGET_DIR=/home/agent/.cargo-target`. `cargo clean` fails with `Device or resource busy` on the mounted target, so the target contents are deleted and emptiness checked, as in the baseline. `date +%s` measures elapsed seconds. Each timing is run twice; the lower is reported. Per-binary builds run sequentially after a clean. The dev stack is running for tests and clippy.
- The test build uses `--no-run` to separate compilation from execution; the test execution can fail on privileged suites in this sandbox. Results and exit codes are reported below without presenting failed runs as green.

## Repository metrics

Output of `python3 scripts/repo-metrics.py` (`--json` captured for the calculations); the full current per-crate table and dependency edges are in the appendix below. Counts can rise when code moves to a different crate without growing the behavior.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Source lines | 57581 | 62770 | +9.0% |
| Test lines | 46647 | 48348 | +3.6% |
| Synchronous test functions | 325 | 353 | +8.6% |
| Tokio test functions | 434 | 454 | +4.6% |
| Cargo.lock packages | 515 | 513 | -0.4% |
| `aws-*` packages | 29 | 27 | -6.9% |
| Internal dependency edges | 79 | 105 | +32.9% |

### Per-crate counts

| Crate / measure | Before | After | Change |
| --- | ---: | ---: | ---: |
| `swarmy-api` source lines | 3681 | 4296 | +16.7% |
| `swarmy-api` test lines | 8319 | 4753 | -42.9% |
| `swarmy-api` tests | 15 | 17 | +13.3% |
| `swarmy-api` Tokio tests | 69 | 36 | -47.8% |
| `swarmy-api-types` source lines | 1340 | 1425 | +6.3% |
| `swarmy-api-types` test lines | 151 | 163 | +7.9% |
| `swarmy-api-types` tests | 6 | 6 | +0.0% |
| `swarmy-api-types` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-bus` source lines | 885 | 807 | -8.8% |
| `swarmy-bus` test lines | 587 | 586 | -0.2% |
| `swarmy-bus` tests | 2 | 2 | +0.0% |
| `swarmy-bus` Tokio tests | 14 | 14 | +0.0% |
| `swarmy-catalog` source lines | 0 | 307 | n/a (zero baseline) |
| `swarmy-catalog` test lines | 0 | 186 | n/a (zero baseline) |
| `swarmy-catalog` tests | 0 | 6 | n/a (zero baseline) |
| `swarmy-catalog` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-chaos` source lines | 2597 | 2625 | +1.1% |
| `swarmy-chaos` test lines | 371 | 373 | +0.5% |
| `swarmy-chaos` tests | 6 | 6 | +0.0% |
| `swarmy-chaos` Tokio tests | 1 | 1 | +0.0% |
| `swarmy-chat` source lines | 0 | 1524 | n/a (zero baseline) |
| `swarmy-chat` test lines | 0 | 365 | n/a (zero baseline) |
| `swarmy-chat` tests | 0 | 13 | n/a (zero baseline) |
| `swarmy-chat` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-cli` source lines | 8948 | 4496 | -49.8% |
| `swarmy-cli` test lines | 4425 | 1871 | -57.7% |
| `swarmy-cli` tests | 70 | 34 | -51.4% |
| `swarmy-cli` Tokio tests | 24 | 3 | -87.5% |
| `swarmy-client` source lines | 1158 | 1271 | +9.8% |
| `swarmy-client` test lines | 541 | 541 | +0.0% |
| `swarmy-client` tests | 3 | 3 | +0.0% |
| `swarmy-client` Tokio tests | 8 | 8 | +0.0% |
| `swarmy-cloud` source lines | 0 | 4899 | n/a (zero baseline) |
| `swarmy-cloud` test lines | 0 | 578 | n/a (zero baseline) |
| `swarmy-cloud` tests | 0 | 20 | n/a (zero baseline) |
| `swarmy-cloud` Tokio tests | 0 | 23 | n/a (zero baseline) |
| `swarmy-config` source lines | 1780 | 1940 | +9.0% |
| `swarmy-config` test lines | 870 | 939 | +7.9% |
| `swarmy-config` tests | 29 | 31 | +6.9% |
| `swarmy-config` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-core` source lines | 2581 | 2856 | +10.7% |
| `swarmy-core` test lines | 1179 | 1388 | +17.7% |
| `swarmy-core` tests | 34 | 37 | +8.8% |
| `swarmy-core` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-devtools` source lines | 0 | 256 | n/a (zero baseline) |
| `swarmy-devtools` test lines | 0 | 0 | n/a (zero baseline) |
| `swarmy-devtools` tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-devtools` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-e2e` source lines | 0 | 0 | n/a (zero baseline) |
| `swarmy-e2e` test lines | 0 | 9142 | n/a (zero baseline) |
| `swarmy-e2e` tests | 0 | 1 | n/a (zero baseline) |
| `swarmy-e2e` Tokio tests | 0 | 96 | n/a (zero baseline) |
| `swarmy-gateway` source lines | 2059 | 1856 | -9.9% |
| `swarmy-gateway` test lines | 2211 | 871 | -60.6% |
| `swarmy-gateway` tests | 8 | 7 | -12.5% |
| `swarmy-gateway` Tokio tests | 23 | 9 | -60.9% |
| `swarmy-harness` source lines | 373 | 416 | +11.5% |
| `swarmy-harness` test lines | 765 | 824 | +7.7% |
| `swarmy-harness` tests | 20 | 22 | +10.0% |
| `swarmy-harness` Tokio tests | 2 | 2 | +0.0% |
| `swarmy-image` source lines | 479 | 479 | +0.0% |
| `swarmy-image` test lines | 0 | 0 | n/a (zero baseline) |
| `swarmy-image` tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-image` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-llm` source lines | 7047 | 6889 | -2.2% |
| `swarmy-llm` test lines | 5905 | 5901 | -0.1% |
| `swarmy-llm` tests | 73 | 72 | -1.4% |
| `swarmy-llm` Tokio tests | 67 | 68 | +1.5% |
| `swarmy-sandbox` source lines | 1287 | 1298 | +0.9% |
| `swarmy-sandbox` test lines | 119 | 135 | +13.4% |
| `swarmy-sandbox` tests | 2 | 3 | +50.0% |
| `swarmy-sandbox` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-scheduler` source lines | 600 | 514 | -14.3% |
| `swarmy-scheduler` test lines | 726 | 0 | -100.0% |
| `swarmy-scheduler` tests | 1 | 0 | -100.0% |
| `swarmy-scheduler` Tokio tests | 11 | 0 | -100.0% |
| `swarmy-store` source lines | 13096 | 14229 | +8.7% |
| `swarmy-store` test lines | 10260 | 11652 | +13.6% |
| `swarmy-store` tests | 23 | 31 | +34.8% |
| `swarmy-store` Tokio tests | 121 | 131 | +8.3% |
| `swarmy-tools` source lines | 321 | 321 | +0.0% |
| `swarmy-tools` test lines | 105 | 105 | +0.0% |
| `swarmy-tools` tests | 3 | 3 | +0.0% |
| `swarmy-tools` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-version` source lines | 60 | 60 | +0.0% |
| `swarmy-version` test lines | 0 | 0 | n/a (zero baseline) |
| `swarmy-version` tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-version` Tokio tests | 0 | 0 | n/a (zero baseline) |
| `swarmy-volume` source lines | 4364 | 4369 | +0.1% |
| `swarmy-volume` test lines | 1759 | 1759 | +0.0% |
| `swarmy-volume` tests | 7 | 7 | +0.0% |
| `swarmy-volume` Tokio tests | 34 | 34 | +0.0% |
| `swarmy-worker` source lines | 2898 | 3562 | +22.9% |
| `swarmy-worker` test lines | 4759 | 2530 | -46.8% |
| `swarmy-worker` tests | 14 | 22 | +57.1% |
| `swarmy-worker` Tokio tests | 47 | 16 | -66.0% |
| `swarmyd` source lines | 2027 | 2075 | +2.4% |
| `swarmyd` test lines | 3595 | 3686 | +2.5% |
| `swarmyd` tests | 9 | 10 | +11.1% |
| `swarmyd` Tokio tests | 13 | 13 | +0.0% |

New crates have a zero baseline; the percentage is undefined. The `swarmy-e2e` tests were moved out of service crates, primarily by PR #175 and consolidated by PR #193.

### Previously largest source files

This follows the baseline file set; new top-ten files are listed in the current metrics appendix.

| File | Before (lines) | After (lines) | Change |
| --- | ---: | ---: | ---: |
| `crates/swarmy-worker/src/worker.rs` | 2882 | 186 | -93.5% |
| `crates/swarmy-store/src/metrics.rs` | 2721 | 2895 | +6.4% |
| `crates/swarmy-client/src/lib.rs` | 1699 | 1733 | +2.0% |
| `crates/swarmy-gateway/src/main.rs` | 1624 | 1667 | +2.6% |
| `crates/swarmy-llm/src/api/bedrock.rs` | 1581 | 1594 | +0.8% |
| `crates/swarmy-config/src/lib.rs` | 1438 | 1428 | -0.7% |
| `crates/swarmy-store/src/agents.rs` | 1321 | 1128 | -14.6% |
| `crates/swarmy-api-types/src/lib.rs` | 1260 | 1432 | +13.7% |
| `crates/swarmy-store/src/credentials.rs` | 1227 | 1094 | -10.8% |
| `crates/swarmy-sandbox/src/runc.rs` | 1203 | 1230 | +2.2% |

## Timings (seconds)

| Command / condition | Before | After | Change |
| --- | ---: | ---: | ---: |
| `cargo build --workspace --locked`, cold | 1603 | 968 | -39.6% |
| `cargo build --workspace --locked`, unchanged rerun | 0 | 0 | n/a (zero baseline) |
| `cargo build --workspace --locked`, after core touch | 71 | 72 | +1.4% |
| `cargo build --locked -p swarmy-cli`, first after clean | 1290 | 467 | -63.8% |
| `cargo build --locked -p swarmy-gateway`, next | 498 | 531 | +6.6% |
| `cargo build --locked -p swarmyd`, next | 26 | 148 | +469.2% |
| `cargo test --workspace --locked --no-run`, cold | 1806 | 1150 | -36.3% |
| `cargo test --workspace --locked --no-run`, unchanged rerun | 1 | 0 | -100.0% |
| `cargo test --workspace --locked`, execution only | 3 (failed) | 5 (failed, rc 101) | +66.7% |
| `cargo test --workspace --locked`, after core touch | not measured | 228 (failed, rc 101) | n/a |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, cold | not measured | 567 | n/a |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, unchanged rerun | 414 | 0 | -100.0% |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, after core touch | 53 | 57 | +7.5% |
| `cargo test --workspace --locked` all-in, cold | not measured | not measured separately | n/a |
| `cargo test --workspace --locked` all-in, unchanged rerun | not measured | not measured separately | n/a |
| `cargo test --workspace --locked` all-in, after core touch | not measured | not measured separately | n/a |

Two runs per measured row (seconds, exit code):

```text
section	run	seconds	rc
build-cold	1	971	0
build-warm	1	0	0
build-touch	1	73	0
build-cold	2	968	0
build-warm	2	0	0
build-touch	2	72	0
cli	1	467	0
gateway	1	531	0
swarmyd	1	148	0
cli	2	467	0
gateway	2	532	0
swarmyd	2	148	0
test-build-cold	1	1150	0
test-build-warm	1	1	0
test-exec	1	5	101
test-touch	1	228	101
test-build-cold	2	1151	0
test-build-warm	2	0	0
test-exec	2	5	101
test-touch	2	229	101
clippy-cold	1	568	0
clippy-warm	1	0	0
clippy-touch	1	58	0
clippy-cold	2	567	0
clippy-warm	2	0	0
clippy-touch	2	57	0
```

The baseline did not time cold clippy, core-touch tests, or a full all-in test separately. The cold `--no-run` build plus the immediately following execution gives a comparable split. The baseline's 414 s "cached clippy" was actually its *first clippy invocation after a test build*, whereas the 0 s row above is an unchanged rerun of clippy itself. Those are different caches and **the -100% figure must not be interpreted as an improvement**. A separate paired measurement below reproduces the baseline ordering. The execution-only runs still fail, but at a different point: 3 of 5 `swarmy-api` `cli_auth` tests pass and 2 fail because `swarmy-auth` was not installed in the sandbox. The helper was installed after the timing matrix for subsequent validation. The historical run failed all 5 on its target-triple heuristic. Exit codes are kept with the timings rather than implying a successful suite.

### Baseline-order clippy check

Both repetitions started with an empty target, ran `cargo test --workspace --locked --no-run`, then ran clippy, an unchanged clippy rerun, and clippy after touching `crates/swarmy-core/src/lib.rs`. This duplicates the baseline's *first clippy after a test build* definition, which the earlier cold-clippy row does not.

| Measure | Before (s) | After (s) | Change |
| --- | ---: | ---: | ---: |
| First clippy after test build | 414 | 265 (runs: 266, 265) | -36.0% |
| Unchanged clippy rerun | not measured | 0 (runs: 1, 0) | n/a |
| Clippy after core touch | 53 | 56 (runs: 56, 57) | +5.7% |

All six clippy executions exited 0. The preceding test builds took 1153 and 1152 s. These numbers still compare different CPU/cgroup shapes.

### API fake first-token latency

| Measure | Baseline on suite node (`380adde`) | Master on same node (`350ec17`) | Change |
| --- | ---: | ---: | ---: |
| First-token p50 | not measured | not measured | n/a |
| First-token p95 | not measured | not measured | n/a |

The opt-in benchmark (`SWARMY_TEST_IMAGE=base-ubuntu:dev SWARMY_API_FAKE_BENCH=1 cargo test --locked -p swarmy-api --test latency -- --test-threads=1 --nocapture`) needs root, NBD, a registered image, and the fake development stack, so the operator ran it on the suite node with `swarmy dev up`. It produced no numbers at either revision. At `350ec17`, `fake_turn_records_first_token_metrics` passed but the benchmark itself failed with "fake provider did not emit a token" (around `latency.rs` line 369): it never saw a token delta for its turn on the live feed. At `380adde`, both tests failed while loading settings (`latency.rs` line 58). The benchmark has not produced a number since pull request 123 added it. Making it run is tasky task 01M3KY8ER5133GQHNGWC0Q3QWT.

## Master CI history

The historical 1474 s mean used `updatedAt - createdAt`, which **includes queue time**, not first job start to last job end. The table includes that comparable definition as well as the requested actual workflow job span and sum of individual job durations. PR #193 split CI into six parallel jobs; older entries had two jobs.

| Short SHA | Created | Finished | Created-to-updated (s) | First job to last job (s) | Job time sum (s) | Jobs |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `350ec17` | 2026-09-28T11:36:08Z | 2026-09-28T11:41:54Z | 346 | 342 | 984 | 6 |
| `75f6ca3` | 2026-09-28T11:18:25Z | 2026-09-28T11:24:18Z | 353 | 349 | 998 | 6 |
| `ed10cf4` | 2026-09-28T10:39:01Z | 2026-09-28T10:44:44Z | 343 | 339 | 961 | 6 |
| `a6087c2` | 2026-09-28T10:16:33Z | 2026-09-28T10:22:02Z | 329 | 326 | 1045 | 6 |
| `7f17d03` | 2026-09-28T09:37:47Z | 2026-09-28T09:45:03Z | 436 | 433 | 1136 | 6 |
| `03decaf` | 2026-09-28T08:29:44Z | 2026-09-28T08:43:20Z | 816 | 812 | 3296 | 6 |
| `26233c7` | 2026-09-27T22:22:16Z | 2026-09-27T22:45:50Z | 1414 | 1411 | 1416 | 2 |
| `1fa236b` | 2026-09-27T20:21:53Z | 2026-09-27T20:46:10Z | 1457 | 1454 | 1460 | 2 |
| `bb94caf` | 2026-09-27T17:51:17Z | 2026-09-27T18:18:22Z | 1625 | 1622 | 1629 | 2 |
| `31992a0` | 2026-09-27T16:06:37Z | 2026-09-27T16:30:36Z | 1439 | 1436 | 1436 | 2 |

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Mean of last ten successful master runs, created-to-updated (s) | 1474 | 856 | -41.9% |
| Mean job span (s) | not recorded | 852 | n/a |
| Mean sum of job times (s) | not recorded | 1436 | n/a |

## Interpretation

- PR #172 split remote-only dependencies away from ordinary builds; PR #179 extracted cloud provisioning and PR #177 removed the AWS credential chain from the store. The normal CLI build avoids the AWS SDK, while the opt-in remote path still needs it. This is the principal expected cold-build reduction; `aws-*` lockfile entries move only slightly because optional remote dependencies remain in the lockfile.
- PR #173 retired dead paths; PR #174 split the model catalog and feature-gated cloud providers; PR #175 extracted process-spawning tests into e2e. PR #193 removed duplicate tests and parallelized CI. Changes in per-crate test counts mostly reflect relocation and deduplication rather than a broad deletion of coverage.
- PR #193 is the direct cause of the shorter six-job CI wall time. The sum of job durations is the better indicator of total work; mixing pre-split and post-split runs in the last-ten mean hides the full parallelization effect. The six post-split runs average 437 s created-to-updated and 1403 s summed job time; the four earlier runs average 1484 s and 1485 s respectively. Most of the wall-time drop is concurrent scheduling, not a 70% drop in total compute.
- PR #194 changed compaction behavior and PR #196 added worker kill points, neither primarily targets build time. The ongoing size of the store and worker crates and remote-feature AWS dependencies remain. The sequential gateway and `swarmyd` builds got slower (+6.6% and +469.2%); unlike the CLI, they benefit less from the newly slimmer CLI build that precedes them, and the new crate split changes what must compile for each binary. An unchanged cached build is already near zero and cannot fall meaningfully.
- The baseline `cli_auth` execution failed because its helper interpreted the hyphenated target directory as a target triple. That failure is not a compilation regression; execution status is reported explicitly. CPU/memory shape and CI runner changes also prevent attributing all wall-time differences solely to code cleanup.

## Appendix: current metrics output

```text
## Lines and tests per crate

| Crate | Source lines | Test lines (inline + files) | `#[test]` | `#[tokio::test]` |
| --- | ---: | ---: | ---: | ---: |
| swarmy-api | 4296 | 4753 (122 + 4631) | 17 | 36 |
| swarmy-api-types | 1425 | 163 (163 + 0) | 6 | 0 |
| swarmy-bus | 807 | 586 (17 + 569) | 2 | 14 |
| swarmy-catalog | 307 | 186 (186 + 0) | 6 | 0 |
| swarmy-chaos | 2625 | 373 (212 + 161) | 6 | 1 |
| swarmy-chat | 1524 | 365 (365 + 0) | 13 | 0 |
| swarmy-cli | 4496 | 1871 (272 + 1599) | 34 | 3 |
| swarmy-client | 1271 | 541 (541 + 0) | 3 | 8 |
| swarmy-cloud | 4899 | 578 (578 + 0) | 20 | 23 |
| swarmy-config | 1940 | 939 (939 + 0) | 31 | 0 |
| swarmy-core | 2856 | 1388 (1388 + 0) | 37 | 0 |
| swarmy-devtools | 256 | 0 (0 + 0) | 0 | 0 |
| swarmy-e2e | 0 | 9142 (0 + 9142) | 1 | 96 |
| swarmy-gateway | 1856 | 871 (871 + 0) | 7 | 9 |
| swarmy-harness | 416 | 824 (57 + 767) | 22 | 2 |
| swarmy-image | 479 | 0 (0 + 0) | 0 | 0 |
| swarmy-llm | 6889 | 5901 (1910 + 3991) | 72 | 68 |
| swarmy-sandbox | 1298 | 135 (33 + 102) | 3 | 0 |
| swarmy-scheduler | 514 | 0 (0 + 0) | 0 | 0 |
| swarmy-store | 14229 | 11652 (1756 + 9896) | 31 | 131 |
| swarmy-tools | 321 | 105 (105 + 0) | 3 | 0 |
| swarmy-version | 60 | 0 (0 + 0) | 0 | 0 |
| swarmy-volume | 4369 | 1759 (424 + 1335) | 7 | 34 |
| swarmy-worker | 3562 | 2530 (2530 + 0) | 22 | 16 |
| swarmyd | 2075 | 3686 (434 + 3252) | 10 | 13 |
| **Total** | **62770** | **48348** | **353** | **454** |

## Dependencies

Cargo.lock has 513 packages, 27 of them `aws-*`.

## Ten largest source files

| File | Lines |
| --- | ---: |
| `crates/swarmy-store/src/metrics.rs` | 2895 |
| `crates/swarmy-client/src/lib.rs` | 1733 |
| `crates/swarmy-gateway/src/main.rs` | 1667 |
| `crates/swarmy-llm/src/api/bedrock.rs` | 1594 |
| `crates/swarmy-cloud/src/tests.rs` | 1551 |
| `crates/swarmy-cli/src/api_commands.rs` | 1455 |
| `crates/swarmy-api-types/src/lib.rs` | 1432 |
| `crates/swarmy-config/src/lib.rs` | 1428 |
| `crates/swarmy-store/src/lib.rs` | 1408 |
| `crates/swarmy-api/src/lib.rs` | 1398 |

## Internal dependency edges

| From | To |
| --- | --- |
| swarmy-api | swarmy-api-types |
| swarmy-api | swarmy-bus |
| swarmy-api | swarmy-client |
| swarmy-api | swarmy-config |
| swarmy-api | swarmy-core |
| swarmy-api | swarmy-image |
| swarmy-api | swarmy-llm |
| swarmy-api | swarmy-store |
| swarmy-api | swarmy-version |
| swarmy-api | swarmy-volume |
| swarmy-api-types | swarmy-core |
| swarmy-bus | swarmy-core |
| swarmy-catalog | swarmy-core |
| swarmy-chaos | swarmy-bus |
| swarmy-chaos | swarmy-config |
| swarmy-chaos | swarmy-core |
| swarmy-chaos | swarmy-llm |
| swarmy-chaos | swarmy-sandbox |
| swarmy-chaos | swarmy-store |
| swarmy-chaos | swarmy-version |
| swarmy-chaos | swarmy-volume |
| swarmy-chat | swarmy-api-types |
| swarmy-chat | swarmy-client |
| swarmy-chat | swarmy-config |
| swarmy-chat | swarmy-core |
| swarmy-cli | swarmy-api-types |
| swarmy-cli | swarmy-chat |
| swarmy-cli | swarmy-client |
| swarmy-cli | swarmy-cloud |
| swarmy-cli | swarmy-config |
| swarmy-cli | swarmy-core |
| swarmy-cli | swarmy-image |
| swarmy-cli | swarmy-version |
| swarmy-client | swarmy-api-types |
| swarmy-client | swarmy-config |
| swarmy-cloud | swarmy-api-types |
| swarmy-cloud | swarmy-client |
| swarmy-cloud | swarmy-config |
| swarmy-cloud | swarmy-core |
| swarmy-cloud | swarmy-version |
| swarmy-config | swarmy-catalog |
| swarmy-config | swarmy-core |
| swarmy-devtools | swarmy-api-types |
| swarmy-devtools | swarmy-client |
| swarmy-devtools | swarmy-config |
| swarmy-devtools | swarmy-core |
| swarmy-devtools | swarmy-llm |
| swarmy-e2e | swarmy-api |
| swarmy-e2e | swarmy-api-types |
| swarmy-e2e | swarmy-bus |
| swarmy-e2e | swarmy-client |
| swarmy-e2e | swarmy-config |
| swarmy-e2e | swarmy-core |
| swarmy-e2e | swarmy-harness |
| swarmy-e2e | swarmy-llm |
| swarmy-e2e | swarmy-store |
| swarmy-e2e | swarmy-volume |
| swarmy-gateway | swarmy-bus |
| swarmy-gateway | swarmy-config |
| swarmy-gateway | swarmy-core |
| swarmy-gateway | swarmy-harness |
| swarmy-gateway | swarmy-llm |
| swarmy-gateway | swarmy-store |
| swarmy-gateway | swarmy-version |
| swarmy-harness | swarmy-core |
| swarmy-harness | swarmy-llm |
| swarmy-harness | swarmy-tools |
| swarmy-image | swarmy-core |
| swarmy-llm | swarmy-catalog |
| swarmy-llm | swarmy-core |
| swarmy-sandbox | swarmy-core |
| swarmy-sandbox | swarmy-store |
| swarmy-sandbox | swarmy-volume |
| swarmy-scheduler | swarmy-bus |
| swarmy-scheduler | swarmy-config |
| swarmy-scheduler | swarmy-core |
| swarmy-scheduler | swarmy-store |
| swarmy-scheduler | swarmy-version |
| swarmy-scheduler | swarmy-volume |
| swarmy-store | swarmy-config |
| swarmy-store | swarmy-core |
| swarmy-store | swarmy-volume |
| swarmy-tools | swarmy-core |
| swarmy-tools | swarmy-harness |
| swarmy-volume | swarmy-config |
| swarmy-volume | swarmy-core |
| swarmy-volume | swarmy-image |
| swarmy-volume | swarmy-store |
| swarmy-worker | swarmy-api-types |
| swarmy-worker | swarmy-bus |
| swarmy-worker | swarmy-config |
| swarmy-worker | swarmy-core |
| swarmy-worker | swarmy-harness |
| swarmy-worker | swarmy-llm |
| swarmy-worker | swarmy-store |
| swarmy-worker | swarmy-tools |
| swarmy-worker | swarmy-version |
| swarmyd | swarmy-api-types |
| swarmyd | swarmy-bus |
| swarmyd | swarmy-config |
| swarmyd | swarmy-core |
| swarmyd | swarmy-sandbox |
| swarmyd | swarmy-store |
| swarmyd | swarmy-version |
| swarmyd | swarmy-volume |
```
