#!/usr/bin/env bash
# Usage: root-suites.sh BRANCH  — runs the root-only suites for the worker/store/gateway area on this node.
# SUITES overrides the list (comma-separated), for example
# SUITES="swarmyd --test node" for the node suite alone. Each suite's full output goes to
# ~/suite-logs/<branch suffix>-<package>-<test>.log; the console gets the tail.
# REV pins the commit to test (a branch head moves while a worker is still
# pushing); SKIP_CHAOS_CI=1 leaves out scripts/chaos-ci.sh; TEST_ARGS adds test
# harness arguments, for example --nocapture for a benchmark that prints.
set -uo pipefail
branch=$1
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
cd ~/chaos
git fetch -q origin "$branch" && git checkout -q -B suite "${REV:-origin/$branch}" || { echo "checkout failed"; echo "SUITES_EXIT=1"; exit 2; }
echo "== branch $branch at $(git rev-parse --short HEAD)"
sudo systemctl stop swarmyd swarmy-tunnel
api_pid=""
cleanup() { [ -n "$api_pid" ] && kill "$api_pid" 2>/dev/null; sudo systemctl start swarmy-tunnel swarmyd; }
trap cleanup EXIT
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
scripts/dev-stack.sh start 2>&1 | tail -2
set -a; . .dev/env; set +a
# The chaos harness and chaos-ci.sh build under sudo and leave root-owned
# files in target/, which makes this build fail with permission errors.
sudo chown -R "$(id -un):$(id -gn)" "$(readlink -f target)"
# `--tests` builds a package's executable only when it has integration tests,
# and the scheduler, worker, and gateway have none, while the chaos suites run
# the service executables from target/debug. Build those explicitly, or the
# chaos suites run whatever an earlier branch left there.
CARGO_BUILD_JOBS=8 cargo build --locked --tests -p swarmy-chaos -p swarmyd -p swarmy-cli -p swarmy-gateway -p swarmy-worker -p swarmy-scheduler -p swarmy-api > ~/suite-build.log 2>&1 \
  && CARGO_BUILD_JOBS=8 cargo build --locked -p swarmy-scheduler -p swarmy-worker -p swarmy-gateway -p swarmy-api -p swarmyd >> ~/suite-build.log 2>&1 \
  || { tail -20 ~/suite-build.log; echo "build failed"; echo "SUITES_EXIT=1"; exit 2; }
tail -1 ~/suite-build.log
# Since the client split (pull request 150) `swarmy image build` goes through
# the API, so serve one on a loopback port against the dev stack.
if [ -x ./target/debug/swarmy-api ]; then
  api_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
  export SWARMY_API_TOKEN="suite-$(date +%s)-$RANDOM"
  export SWARMY_API_URL="http://127.0.0.1:$api_port"
  SWARMY_API_LISTEN="127.0.0.1:$api_port" ./target/debug/swarmy-api > ~/root-suites-api.log 2>&1 &
  api_pid=$!
  for _ in $(seq 1 120); do
    (echo > "/dev/tcp/127.0.0.1/$api_port") 2>/dev/null && break
    kill -0 "$api_pid" 2>/dev/null || { echo "swarmy-api exited; see ~/root-suites-api.log"; break; }
    sleep 0.5
  done
  echo "== api on $SWARMY_API_URL (pid $api_pid)"
fi
echo "== image build"
sudo -E ./target/debug/swarmy image build images/base-ubuntu --tag dev 2>&1 | tail -2
rc=0
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mkdir -p ~/suite-logs
IFS=',' read -r -a suites <<< "${SUITES:-swarmyd --test node,swarmy-chaos --test bash,swarmy-chaos --test continuity,swarmy-chaos --test coding}"
for suite in "${suites[@]}"; do
  set -- $suite
  echo "== $suite"
  bash "$here/nbd-orphans.sh"
  full=~/suite-logs/${branch##*/}-$1-$3.log
  if sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev "$(command -v cargo)" test --locked -p "$1" "$2" "$3" -- --test-threads=1 ${TEST_ARGS:-} 2>&1 | tee "$full" | { grep -E "^test |test result|panicked" || true; } | tail -12; then :; else rc=1; fi
done
if [ "${SKIP_CHAOS_CI:-0}" != 1 ]; then
  echo "== scripts/chaos-ci.sh"
  sudo -E env PATH="$PATH" bash scripts/chaos-ci.sh 2>&1 | tail -3 || rc=1
fi
[ -n "$api_pid" ] && { kill "$api_pid" 2>/dev/null; wait "$api_pid" 2>/dev/null; api_pid=""; }
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
echo "SUITES_EXIT=$rc"
exit "$rc"
