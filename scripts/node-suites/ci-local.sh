#!/usr/bin/env bash
# Usage: ci-local.sh BRANCH — runs every CI job's Makefile target (the
# `make check-*` targets .github/workflows/ci.yml calls) on this node against
# BRANCH, so a batch branch can be validated without a GitHub run. The job
# list lives in the Makefile; this script only does what CI's runner setup
# does: a clean checkout, the dev stack for the jobs that need it, and none of
# the stack's variables for the script tests. The advisory clone report is
# left out.
# REV pins the commit to test. Each failing target prints a CI_STEP_FAIL line
# and the run continues, so one pass shows every failure; the last line is
# SUITES_EXIT=0 or SUITES_EXIT=1, and the exit status matches it.
# Needs cargo-deny, cargo-machete and ast-grep on PATH (cargo install --locked
# cargo-deny@0.20.2 cargo-machete@0.9.2 ast-grep@0.45.3, matching CI).
set -uo pipefail
branch=$1
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
# CI sets CI=true, which makes tests that need the dev stack fail instead of skipping.
export CI=true
repo=~/chaos
# A fixed second checkout of the suite repository: its own .dev directory (the
# suite checkout's .dev is a symlink to another disk, which git refuses to
# inspect) and a stable path so incremental builds survive between runs. It
# shares the suite checkout's build directory through CARGO_TARGET_DIR, not a
# symlink, for the same reason.
src=~/ci-src
git -C "$repo" fetch -q origin master "$branch" || { echo "checkout failed"; echo "SUITES_EXIT=1"; exit 2; }
rev=$(git -C "$repo" rev-parse --verify "${REV:-origin/$branch}^{commit}") || { echo "checkout failed"; echo "SUITES_EXIT=1"; exit 2; }
[ -d "$src" ] || git -C "$repo" worktree add -q --detach "$src" "$rev"
cd "$src"
git checkout -q --detach --force "$rev" && git clean -q -fdx -e .dev || { echo "checkout failed"; echo "SUITES_EXIT=1"; exit 2; }
echo "== branch $branch at $(git rev-parse --short HEAD)"
export CARGO_TARGET_DIR="$(readlink -f "$repo/target")"
# This node also runs swarmyd and a tunnel that forwards the fleet's NATS on
# port 4222; stop both so the dev stack owns its ports, as root-suites.sh does.
sudo systemctl stop swarmyd swarmy-tunnel
cleanup() { scripts/dev-stack.sh stop >/dev/null 2>&1; sudo systemctl start swarmy-tunnel swarmyd; }
trap cleanup EXIT
# The chaos harness builds under sudo and leaves root-owned files in target/.
sudo chown -R "$(id -un):$(id -gn)" "$CARGO_TARGET_DIR"
rc=0
run_target() {
  echo "== make $*"
  if ! make AST_GREP=ast-grep "$@"; then echo "CI_STEP_FAIL: $*"; rc=1; fi
}

for job in check-lint check-openapi check-deps check-advisories check-remote; do
  run_target "$job"
done

scripts/dev-stack.sh stop >/dev/null 2>&1 || true
if ! scripts/dev-stack.sh start > ~/ci-local-stack.log 2>&1; then
  tail -5 ~/ci-local-stack.log; echo "CI_STEP_FAIL: dev stack did not start"; echo "SUITES_EXIT=1"; exit 1
fi
tail -2 ~/ci-local-stack.log
set -a; . .dev/env; set +a
for job in check-workspace-tests check-e2e check-cli-session; do
  run_target "$job"
done
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
# Some script tests start their own stack; wait until this one has released
# its ports (FoundationDB, NATS and its monitor, SeaweedFS S3).
for _ in $(seq 60); do
  ss -ltn | grep -qE ':(4500|4222|8222|8333) ' || break
  sleep 1
done
# CI runs the script tests without the dev stack's environment; a leftover
# SWARMY_DEV_FDB_PORT, for example, stops the stack script choosing a free
# port. Unset every variable .dev/env exported.
mapfile -t stack_vars < <(sed -n 's/^export \([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' .dev/env)
echo "== make check-scripts"
if ! env "${stack_vars[@]/#/--unset=}" make AST_GREP=ast-grep check-scripts; then echo "CI_STEP_FAIL: check-scripts"; rc=1; fi

echo "SUITES_EXIT=$rc"
exit "$rc"
