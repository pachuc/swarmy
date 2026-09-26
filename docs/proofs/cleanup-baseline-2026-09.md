# Cleanup baseline, 2026-09-26

How big and how slow the repository is before the cleanup goal, so the
final task of the goal can show what changed. The metrics table below is
the output of `python3 scripts/repo-metrics.py` at commit `b2d944f`
(times were measured on the same checkout). The same numbers are
available as JSON with `python3 scripts/repo-metrics.py --json`.

## Machine

All timings ran on one machine, back to back, with nothing else using
the shared target directory:

- 16 vCPUs, 61 GiB RAM, no swap (`nproc` = 16, `free -g` = 61 total).
- But this session is capped at 8 GiB by its cgroup (`memory.max` =
  8589934592), so the cap, not the host, binds the build.
- Linux 7.0.0-1013-aws (Ubuntu 24.04), x86_64.
- rustc and cargo 1.98.1 (pinned toolchain in `rust-toolchain.toml`).
- `CARGO_TARGET_DIR=/home/agent/.cargo-target`, a scratch mount shared
  across clones but used only by this checkout during the measurements.
  `cargo clean` cannot remove that mount point (`Device or resource
  busy`), so every "after `cargo clean`" step below instead deletes the
  directory's contents, which leaves the same empty target directory
  (verified with `du` after each clean).
- Every timed command ran with `CARGO_BUILD_JOBS=1`: the default 16
  parallel rustc are OOM-killed on the AWS SDK crates under the 8 GiB
  cap, and so are 4- and 2-job builds (see the failed attempts). A
  single `rustc` on `aws-sdk-ec2` survives.
- No dev stack running: the `cargo test` runs below would be the plain
  command, so tests that need FoundationDB, NATS, or SeaweedFS would
  skip cleanly.

## Timings

Each command ran twice with `date +%s` around it; the table keeps the
lower number (1 s resolution). Uncached means with an empty target
directory. Cached means the same command run again immediately with
nothing changed. Core-touch means after `touch
crates/swarmy-core/src/lib.rs` (incremental compilation is off in the
dev profile, so this rebuilds the core crate and all its dependents).

| Step | Uncached (s) | Cached (s) | After core touch (s) |
| --- | ---: | ---: | ---: |
| `cargo build --workspace --locked` | 1603 (runs: 1619, 1603) | 0 (runs: 1, 0) | 71 (runs: 73, 71) |
| `cargo build --locked -p swarmy-cli` | 1290 (runs: 1291, 1290) | n/a (sequential, see note) | n/a |
| `cargo build --locked -p swarmy-gateway` | 498 (runs: 498, 510) | n/a | n/a |
| `cargo build --locked -p swarmyd` | 26 (runs: 26, 26) | n/a | n/a |
| `cargo test --workspace --locked` (all-in) | not measured | not measured | not measured |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | not measured | not measured | not measured |
| `cargo test --workspace --locked` execution only (after `--no-run`) | n/a | not measured | n/a |

Per-package note: the three `-p` builds ran from one clean target, one
after another in the order cli, gateway, swarmyd, and the whole sequence
ran twice; each cell keeps the lower of its two runs. Gateway rebuilds
much of the tree after cli because the two feature sets differ; swarmyd
then needs almost nothing new.

Failed attempts (all OOM-killed by the 8 GiB cap, `signal: 9, SIGKILL`
on `rustc`, kernel `oom_kill` counter at 6 afterwards):

| Command | Jobs | Wall time to failure |
| --- | ---: | ---: |
| `cargo build --workspace --locked` after clean | 16 (default) | 234 s (`aws-sdk-ssm`, `aws-sdk-ec2`) |
| `cargo build --workspace --locked` after clean | 16 (default) | 488 s (`aws-sdk-ssm`, `aws-sdk-ec2`) |
| `cargo build --workspace --locked` after clean | 4 | killed on `aws-sdk-ec2` |
| `cargo build --workspace --locked` after clean | 2 | killed on `aws-sdk-ec2` |

No multi-job configuration completed a workspace build on this computer,
so there are no successful four-job timings to keep; every number in the
table above used one job.

The single-job battery was abandoned during `test-uncached-1` on
operator direction, so the test, clippy, and execution-split rows were
not measured in this pass. The repeat procedure below covers them for
the final task of the goal.

## CI history

Last ten successful CI runs on master (`gh run list --workflow CI
--status success`, wall time = `updatedAt` minus `createdAt`, which
includes queueing):

| Run (short SHA) | Created | Finished | Wall time |
| --- | --- | --- | --- |
| 71d0ec2 | 2026-09-26T09:34:58Z | 2026-09-26T10:06:17Z | 31m19s (1879 s) |
| 1598084 | 2026-09-26T08:32:48Z | 2026-09-26T09:08:35Z | 35m47s (2147 s) |
| 348fb3e | 2026-09-26T07:36:00Z | 2026-09-26T07:56:57Z | 20m57s (1257 s) |
| f7fce63 | 2026-09-26T06:32:45Z | 2026-09-26T07:02:35Z | 29m50s (1790 s) |
| 8622418 | 2026-09-26T06:22:41Z | 2026-09-26T06:51:06Z | 28m25s (1705 s) |
| 1e49243 | 2026-09-26T05:36:58Z | 2026-09-26T05:56:27Z | 19m29s (1169 s) |
| 91ddf14 | 2026-09-26T04:30:41Z | 2026-09-26T04:50:50Z | 20m09s (1209 s) |
| 6633b2f | 2026-09-26T01:34:41Z | 2026-09-26T01:54:08Z | 19m27s (1167 s) |
| d229a6b | 2026-09-25T18:50:18Z | 2026-09-25T19:10:11Z | 19m53s (1193 s) |
| 9a000e6 | 2026-09-25T14:52:24Z | 2026-09-25T15:12:48Z | 20m24s (1224 s) |

Mean wall time: 1474 s (24.6 min).

## Metrics

## Lines and tests per crate

| Crate | Source lines | Test lines (inline + files) | `#[test]` | `#[tokio::test]` |
| --- | ---: | ---: | ---: | ---: |
| swarmy-api | 3681 | 8319 (32 + 8287) | 15 | 69 |
| swarmy-api-types | 1340 | 151 (151 + 0) | 6 | 0 |
| swarmy-bus | 885 | 587 (17 + 570) | 2 | 14 |
| swarmy-chaos | 2597 | 371 (210 + 161) | 6 | 1 |
| swarmy-cli | 8948 | 4425 (2832 + 1593) | 70 | 24 |
| swarmy-client | 1158 | 541 (541 + 0) | 3 | 8 |
| swarmy-config | 1780 | 870 (870 + 0) | 29 | 0 |
| swarmy-core | 2581 | 1179 (1179 + 0) | 34 | 0 |
| swarmy-gateway | 2059 | 2211 (942 + 1269) | 8 | 23 |
| swarmy-harness | 373 | 765 (0 + 765) | 20 | 2 |
| swarmy-image | 479 | 0 (0 + 0) | 0 | 0 |
| swarmy-llm | 7047 | 5905 (1841 + 4064) | 73 | 67 |
| swarmy-sandbox | 1287 | 119 (17 + 102) | 2 | 0 |
| swarmy-scheduler | 600 | 726 (16 + 710) | 1 | 11 |
| swarmy-store | 13096 | 10260 (1336 + 8924) | 23 | 121 |
| swarmy-tools | 321 | 105 (105 + 0) | 3 | 0 |
| swarmy-version | 60 | 0 (0 + 0) | 0 | 0 |
| swarmy-volume | 4364 | 1759 (424 + 1335) | 7 | 34 |
| swarmy-worker | 2898 | 4759 (2209 + 2550) | 14 | 47 |
| swarmyd | 2027 | 3595 (392 + 3203) | 9 | 13 |
| **Total** | **57581** | **46647** | **325** | **434** |

Test lines are inline `#[cfg(test)]` modules (including whole files
pulled in by `#[cfg(test)] mod name;`) plus files under `tests/`, shown
as `total (inline + files)`.

## Dependencies

Cargo.lock has 515 packages, 29 of them `aws-*`.

## Ten largest source files

| File | Lines |
| --- | ---: |
| `crates/swarmy-worker/src/worker.rs` | 2882 |
| `crates/swarmy-store/src/metrics.rs` | 2721 |
| `crates/swarmy-client/src/lib.rs` | 1699 |
| `crates/swarmy-gateway/src/main.rs` | 1624 |
| `crates/swarmy-llm/src/api/bedrock.rs` | 1581 |
| `crates/swarmy-config/src/lib.rs` | 1438 |
| `crates/swarmy-store/src/agents.rs` | 1321 |
| `crates/swarmy-api-types/src/lib.rs` | 1260 |
| `crates/swarmy-store/src/credentials.rs` | 1227 |
| `crates/swarmy-sandbox/src/runc.rs` | 1203 |

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
| swarmy-bus | swarmy-core |
| swarmy-chaos | swarmy-bus |
| swarmy-chaos | swarmy-config |
| swarmy-chaos | swarmy-core |
| swarmy-chaos | swarmy-llm |
| swarmy-chaos | swarmy-sandbox |
| swarmy-chaos | swarmy-store |
| swarmy-chaos | swarmy-version |
| swarmy-chaos | swarmy-volume |
| swarmy-cli | swarmy-api-types |
| swarmy-cli | swarmy-client |
| swarmy-cli | swarmy-config |
| swarmy-cli | swarmy-core |
| swarmy-cli | swarmy-image |
| swarmy-cli | swarmy-llm |
| swarmy-cli | swarmy-version |
| swarmy-client | swarmy-api-types |
| swarmy-config | swarmy-core |
| swarmy-config | swarmy-llm |
| swarmy-gateway | swarmy-api-types |
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
| swarmy-store | swarmy-api-types |
| swarmy-store | swarmy-config |
| swarmy-store | swarmy-core |
| swarmy-store | swarmy-volume |
| swarmy-tools | swarmy-core |
| swarmy-tools | swarmy-harness |
| swarmy-volume | swarmy-config |
| swarmy-volume | swarmy-core |
| swarmy-volume | swarmy-image |
| swarmy-volume | swarmy-store |
| swarmy-worker | swarmy-api |
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

## How to repeat

From the repository root on a fleet sandbox with the warm shared target
directory (`nproc` and `free -g` go in the report):

```sh
python3 scripts/test_repo_metrics.py
python3 scripts/repo-metrics.py > docs/proofs/cleanup-baseline-<date>.md  # metrics section
cargo clean && time cargo build --workspace --locked        # cold build
time cargo build --workspace --locked                       # cached build
touch crates/swarmy-core/src/lib.rs
time cargo build --workspace --locked                       # rebuild after a core edit
```

Run each command twice and keep the lower number, timing with
`date +%s` around it. On a memory-capped sandbox, export
`CARGO_BUILD_JOBS=1` first (multi-job workspace builds are OOM-killed
there); if `cargo clean` fails on the mount-pointed target directory,
delete the directory's contents instead. Then, from a clean target, `time cargo build
--locked -p swarmy-cli`, `-p swarmy-gateway`, and `-p swarmyd` one after
another, recording each; then the same uncached/cached/core-touch triple
for `cargo test --workspace --locked` and for `cargo clippy --workspace
--all-targets --locked -- -D warnings`; then `cargo test --workspace
--locked --no-run` followed by the test command, so compile time and
test time are reported apart. CI history comes from `gh run list
--workflow CI --status success --limit 40 --json
createdAt,updatedAt,headBranch,headSha`, filtered to `headBranch ==
master`, wall time per run plus the mean.
