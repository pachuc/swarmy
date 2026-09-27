#!/usr/bin/env bash
# Usage: rerun-suite.sh REF PACKAGE TEST — rerun one root suite with full output.
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
CARGO_BUILD_JOBS=8 cargo build --locked --tests -p swarmy-chaos -p swarmyd -p swarmy-cli -p swarmy-gateway -p swarmy-worker -p swarmy-scheduler -p swarmy-api 2>&1 | tail -1
sudo -E ./target/debug/swarmy image build images/base-ubuntu --tag dev 2>&1 | tail -1
sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev RUST_BACKTRACE=0 "$(command -v cargo)" test --locked -p "$pkg" --test "$suite" -- --test-threads=1 --nocapture 2>&1 | grep -vE "^\s+(Compiling|Finished|Running|Blocking)"
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
echo "RERUN_SUITE_DONE"
