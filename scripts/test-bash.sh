#!/usr/bin/env bash
# Build as the invoking user; only image creation and execution require root.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
scripts/dev-stack.sh start
source .dev/env
cargo build --workspace --locked
if ! target/debug/swarmy --json image show base-ubuntu:bash-test >/dev/null 2>&1; then
    sudo -E target/debug/swarmy --json image build images/base-ubuntu --tag bash-test
fi
export SWARMY_TEST_IMAGE=base-ubuntu:bash-test
cargo test -p swarmy-chaos --test bash --locked --no-run
test_binary=$(cargo test -p swarmy-chaos --test bash --locked --no-run --message-format=json 2>/dev/null | jq -r 'select(.executable != null and .target.name == "bash") | .executable')
sudo -E "$test_binary" --nocapture

# Open the terminal client with only its default image and execute pwd.
cargo test -p swarmy-cli --test session --locked --no-run
test_binary=$(cargo test -p swarmy-cli --test session --locked --no-run --message-format=json 2>/dev/null | jq -r 'select(.executable != null and .target.name == "session") | .executable')
sudo -E "$test_binary" root_chat_default_image_executes_pwd --nocapture
