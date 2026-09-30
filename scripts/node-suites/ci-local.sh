#!/usr/bin/env bash
# Usage: ci-local.sh BRANCH — runs the commands of every CI job in
# .github/workflows/ci.yml, in job order, on this node against BRANCH, so a
# batch of changes can be validated without a GitHub run. Keep this list in
# step with ci.yml. The advisory jscpd clone report is left out; the dependency
# advisories check, which CI runs weekly, is included.
# REV pins the commit to test. Each failing command prints a CI_STEP_FAIL line
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
cd ~/chaos
git fetch -q origin master "$branch" && git checkout -q -B suite "${REV:-origin/$branch}" || { echo "checkout failed"; echo "SUITES_EXIT=1"; exit 2; }
echo "== branch $branch at $(git rev-parse --short HEAD)"
# This node also runs swarmyd and a tunnel that forwards the fleet's NATS on
# port 4222; stop both so the dev stack owns its ports, as root-suites.sh does.
sudo systemctl stop swarmyd swarmy-tunnel
src=$(mktemp -u -d)
cleanup() { git worktree remove --force "$src" 2>/dev/null; scripts/dev-stack.sh stop >/dev/null 2>&1; sudo systemctl start swarmy-tunnel swarmyd; }
trap cleanup EXIT
# The chaos harness builds under sudo and leaves root-owned files in target/.
sudo chown -R "$(id -un):$(id -gn)" "$(readlink -f target)"
# The repository checks run in a clean worktree: this checkout's .dev is a
# symlink to another disk, which git refuses to inspect.
git worktree add -q --detach "$src" HEAD
rc=0
step() {
  echo "== $*"
  if ! "$@"; then echo "CI_STEP_FAIL: $*"; rc=1; fi
}

# lint
cd "$src"
step scripts/check-public-ids.sh
step scripts/check-docs-accuracy.py
step scripts/check-anyhow-in-libraries.sh
step ast-grep scan --config ast-grep/sgconfig.yml
cd ~/chaos
step cargo fmt --all --check
step cargo build --locked -p swarmy-cli --no-default-features
step cargo clippy --workspace --all-targets --locked -- -D warnings
step cargo clippy --locked -p swarmy-cloud --features remote --all-targets -- -D warnings
step cargo clippy --locked -p swarmy-cli --features remote --all-targets -- -D warnings
step cargo test --locked -p swarmy-llm --no-default-features
step env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked

# openapi-compat
step scripts/check-openapi-compat.sh origin/master

# dependency-policy, plus the weekly advisories check
step cargo deny check licenses bans sources
step cargo deny check advisories
step cargo machete

# remote-tests
step cargo test --locked -p swarmy-cloud --features remote
step cargo test --locked -p swarmy-cli --features remote -- --skip dev_up_run_recover_reconfigure_and_down

# workspace-tests, e2e-and-chaos and cli-session need the dev stack.
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
if ! scripts/dev-stack.sh start > ~/ci-local-stack.log 2>&1; then
  tail -5 ~/ci-local-stack.log; echo "CI_STEP_FAIL: dev stack did not start"; echo "SUITES_EXIT=1"; exit 1
fi
tail -2 ~/ci-local-stack.log
set -a; . .dev/env; set +a
step cargo build --workspace --locked
step cargo test --workspace --locked --exclude swarmy-e2e
step cargo test --locked -p swarmy-e2e --test gateway --test scheduler --test worker -- --test-threads=1
step scripts/chaos-ci.sh
step cargo test --locked -p swarmy-e2e --test cli_session -- --test-threads=1
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
# Some script tests start their own stack; wait until this one has released
# its ports (FoundationDB, NATS and its monitor, SeaweedFS S3).
for _ in $(seq 60); do
  ss -ltn | grep -qE ':(4500|4222|8222|8333) ' || break
  sleep 1
done

# script-tests. CI runs these without the dev stack's environment; a leftover
# SWARMY_DEV_FDB_PORT, for example, stops the stack script choosing a free
# port. Unset every variable .dev/env exported before running them.
mapfile -t stack_vars < <(sed -n 's/^export \([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' .dev/env)
step env "${stack_vars[@]/#/--unset=}" scripts/test-scripts.sh

echo "SUITES_EXIT=$rc"
exit "$rc"
