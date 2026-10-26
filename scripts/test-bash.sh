#!/usr/bin/env bash
# Build as the invoking user; only image creation and execution require root.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
scripts/dev-stack.sh start
source .dev/env
cargo build --workspace --locked
# Image commands use the API, not the service configuration's [api] section.
api_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
export SWARMY_API_TOKEN="suite-$(date +%s)-$RANDOM"
export SWARMY_API_URL="http://127.0.0.1:$api_port"
SWARMY_API_LISTEN="127.0.0.1:$api_port" target/debug/swarmy-core api > ~/test-bash-api.log 2>&1 &
api_pid=$!
trap 'kill "$api_pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 120); do
    (echo > "/dev/tcp/127.0.0.1/$api_port") 2>/dev/null && break
    kill -0 "$api_pid" 2>/dev/null || { echo "swarmy-core api exited; see ~/test-bash-api.log"; exit 1; }
    sleep 0.5
done
if ! target/debug/swarmy --json image show base-ubuntu:bash-test >/dev/null 2>&1; then
    sudo -E target/debug/swarmy --json image build images/base-ubuntu --tag bash-test
fi
kill "$api_pid" 2>/dev/null || true
wait "$api_pid" 2>/dev/null || true
trap - EXIT
unset SWARMY_API_URL SWARMY_API_TOKEN
export SWARMY_TEST_IMAGE=base-ubuntu:bash-test
cargo test -p swarmy-chaos --test bash --locked --no-run
test_binary=$(cargo test -p swarmy-chaos --test bash --locked --no-run --message-format=json 2>/dev/null | jq -r 'select(.executable != null and .target.name == "bash") | .executable')
sudo -E "$test_binary" --nocapture

# Open the terminal client with only its default image and execute pwd.
cargo test -p swarmy-e2e --test cli_session --locked --no-run
test_binary=$(cargo test -p swarmy-e2e --test cli_session --locked --no-run --message-format=json 2>/dev/null | jq -r 'select(.executable != null and .target.name == "cli_session") | .executable')
sudo -E "$test_binary" root_chat_default_image_executes_pwd --nocapture
