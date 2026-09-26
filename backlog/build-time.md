# Build time in workers and benchmarks

## What it is

A fleet task that runs `make check` from a cold cache takes about twenty
minutes on a 4-vCPU sandbox, and most of that is not our code. The first
perf baseline (2026-09-25, dev2 and codex-daytona, task `small`) showed the
run spending its time compiling `aws-sdk-ec2` and `aws-lc-sys`, getting
killed by the sandbox memory cap (6 GiB on the `swarmy-dev` image, 8 GB on
the Daytona sandbox), and retrying the build single-threaded. The task that
only built `swarmy-config` took three minutes on the same machines.

## Why it matters

Every worker task pays this once per fresh clone, every benchmark pays it
per run, and the memory kills make the model flail (one daytona run spent
268 tool calls polling a build). It also shapes what workers can verify:
the 6 GiB cap means they cannot run the full-feature test suite at all and
leave the CLI tests to CI.

## Options, in rough order of payoff per effort

1. Take the AWS SDK out of the common path. The CLI needs a handful of EC2
   calls (run, describe, terminate instances, security groups, subnets).
   A thin SigV4 client over reqwest replaces the SDK crates; failing that,
   `make check` and the worker image build with `--no-default-features`
   and only CI builds the `remote` feature. Use the `ring` backend for
   rustls so `aws-lc-sys` needs no C build.
2. A shared compile cache: `sccache` with the swarm's S3 bucket as backend,
   set in the worker and benchmark images, so object files compiled by any
   worker are reused by every other worker and by ephemeral sessions.
3. A warm image: build `swarmy-dev` with master already compiled in
   `target/`, refreshed on a schedule, so cold sessions do incremental
   builds of the diff.
4. Cheaper dev profile: `debug = "line-tables-only"`, `split-debuginfo`,
   the `mold` linker, and `cargo nextest` for the test run.
5. Right-sized memory: 8 GiB workers (the dev2 plan) so builds never fall
   back to single-job mode.
6. Scoped checks in the fleet prompt: a task that touches one crate runs
   that crate's tests; the full `make check` belongs in CI.

## Why not now

The user wants to discuss the approach first (2026-09-26). Items 5 and 6
are configuration and can happen with the dev2 fleet move; items 1 and 2
are the ones that change the picture and deserve a decision.

## Baseline

The September 2026 cleanup baseline is recorded in
[docs/proofs/cleanup-baseline-2026-09.md](../docs/proofs/cleanup-baseline-2026-09.md):
lines of source and tests per crate, dependency edges, cold and cached
build/test/clippy times on a 16-vCPU sandbox, and recent CI wall times.
The final task of the cleanup goal repeats those measurements there.

## Trigger

The discussion, or the next time a worker or benchmark is blocked on a
build that a smaller sandbox cannot finish.
