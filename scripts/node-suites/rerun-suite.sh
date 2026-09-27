#!/usr/bin/env bash
# Usage: rerun-suite.sh REF PACKAGE TEST — rerun one root suite with full output.
# Run it through suite-queue.sh --only, which holds the node's suite lock.
set -uo pipefail
ref=$1; pkg=$2; suite=$3
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
cd ~/chaos
git fetch -q origin "$ref" && git checkout -q -B suite "origin/$ref"
echo "== $ref at $(git rev-parse --short HEAD)"
sudo systemctl stop swarmyd swarmy-tunnel
trap "sudo systemctl start swarmy-tunnel swarmyd" EXIT
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
scripts/dev-stack.sh start 2>&1 | tail -1
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
sudo -E ./target/debug/swarmy image build images/base-ubuntu --tag dev 2>&1 | tail -1
bash "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/nbd-orphans.sh"
sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev RUST_BACKTRACE=0 "$(command -v cargo)" test --locked -p "$pkg" --test "$suite" -- --test-threads=1 --nocapture 2>&1 | grep -vE "^\s+(Compiling|Finished|Running|Blocking)"
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
echo "RERUN_SUITE_DONE"
