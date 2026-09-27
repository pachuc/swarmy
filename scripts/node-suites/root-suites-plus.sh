#!/usr/bin/env bash
# Usage: root-suites-plus.sh BRANCH — the standard root suites plus the image and vol suites.
set -uo pipefail
branch=$1
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
bash "$here/root-suites.sh" "$branch"
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
cd ~/chaos
sudo systemctl stop swarmyd swarmy-tunnel
trap "sudo systemctl start swarmy-tunnel swarmyd" EXIT
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
scripts/dev-stack.sh start 2>&1 | tail -1
set -a; . .dev/env; set +a
CARGO_BUILD_JOBS=8 cargo build --locked --tests -p swarmy-cli -p swarmyd -p swarmy-volume --no-default-features 2>&1 | tail -1
for suite in "swarmy-cli --test image" "swarmyd --test vol" "swarmy-volume --test image" "swarmy-volume --test nbd"; do
  set -- $suite
  echo "== $suite"
  bash "$here/nbd-orphans.sh"
  sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev "$(command -v cargo)" test --locked --no-default-features -p "$1" "$2" "$3" -- --test-threads=1 2>&1 | tail -4
done
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
echo "PLUS_DONE"
