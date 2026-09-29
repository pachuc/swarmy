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
rc=0
CARGO_BUILD_JOBS=8 cargo build --locked --tests -p swarmy-cli -p swarmyd -p swarmy-volume --no-default-features > ~/suite-build-plus.log 2>&1 \
  || { tail -20 ~/suite-build-plus.log; echo "image suite build failed"; exit 1; }
tail -1 ~/suite-build-plus.log
for suite in "swarmy-cli --test image" "swarmyd --test vol" "swarmy-volume --test image" "swarmy-volume --test nbd"; do
  set -- $suite
  echo "== $suite"
  bash "$here/nbd-orphans.sh"
  sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev "$(command -v cargo)" test --locked --no-default-features -p "$1" "$2" "$3" -- --test-threads=1 2>&1 | tail -4 || rc=1
done

# The dev-stack acceptance clones the checkout inside a sandbox and runs the
# dev stack and cargo test there, which needs the Rust toolchain and stack
# tools that only the swarmy-dev image carries; base-ubuntu cannot run it.
echo "== image build swarmy-dev"
sudo -E ./target/debug/swarmy image build images/swarmy-dev --tag dev 2>&1 | tail -2 || rc=1
# The node acceptance and the chat tests refuse headless client binaries, so
# rebuild default features once, before the sections that need them.
CARGO_BUILD_JOBS=8 cargo build --locked -p swarmy-cli -p swarmyd -p swarmy-scheduler -p swarmy-worker -p swarmy-gateway -p swarmy-api >> ~/suite-build-plus.log 2>&1 \
  || { tail -20 ~/suite-build-plus.log; echo "chat suite build failed"; exit 1; }
# Tests that run nowhere without their image variable set. Each skips cleanly
# when its variable is absent, so wire each up here with the images built
# above. None duplicates a test that already runs: the node dev-stack
# acceptance is the only end-to-end check that an agent can build swarmy in a
# sandbox, and the root chats cover default-image execution, shared
# background processes, and capped memory input.
echo "== swarmyd --test node root_dev_stack_uses_sandbox_loopback"
bash "$here/nbd-orphans.sh"
sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev SWARMY_TEST_DEV_IMAGE=swarmy-dev:dev SWARMY_TEST_BRANCH="$branch" "$(command -v cargo)" test --locked -p swarmyd --test node -- root_dev_stack_uses_sandbox_loopback --test-threads=1 2>&1 | tail -4 || rc=1
echo "== swarmy-e2e --test cli_session root chats"
bash "$here/nbd-orphans.sh"
sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev "$(command -v cargo)" test --locked -p swarmy-e2e --test cli_session -- root_chat_default_image_executes_pwd root_named_chats_share_a_background_process_and_delete root_memory_written_by_tools_is_in_the_next_turn_and_capped --test-threads=1 2>&1 | tail -6 || rc=1
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
echo "PLUS_DONE"
echo "PLUS_EXIT=$rc"
exit "$rc"
