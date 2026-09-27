#!/usr/bin/env bash
# Usage: root-suites-plus.sh BRANCH — the standard root suites plus the image and vol suites.
set -uo pipefail
branch=$1
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
bash "$here/root-suites.sh" "$branch"
# Exit 2 means the branch could not be checked out or built; do not run the
# image suites against whatever was there before.
[ $? -eq 2 ] && exit 1
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
cd ~/chaos
sudo systemctl stop swarmyd swarmy-tunnel
trap "sudo systemctl start swarmy-tunnel swarmyd" EXIT
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
scripts/dev-stack.sh start 2>&1 | tail -1
set -a; . .dev/env; set +a
# The chaos harness and chaos-ci.sh build under sudo and leave root-owned
# files in target/, which makes this build fail with permission errors.
sudo chown -R "$(id -un):$(id -gn)" "$(readlink -f target)"
CARGO_BUILD_JOBS=8 cargo build --locked --tests -p swarmy-cli -p swarmyd -p swarmy-volume --no-default-features > ~/suite-build-plus.log 2>&1 \
  || { tail -20 ~/suite-build-plus.log; echo "image suite build failed"; exit 1; }
tail -1 ~/suite-build-plus.log
for suite in "swarmy-cli --test image" "swarmyd --test vol" "swarmy-volume --test image" "swarmy-volume --test nbd"; do
  set -- $suite
  echo "== $suite"
  bash "$here/nbd-orphans.sh"
  sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev "$(command -v cargo)" test --locked --no-default-features -p "$1" "$2" "$3" -- --test-threads=1 2>&1 | tail -4
done
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
echo "PLUS_DONE"
