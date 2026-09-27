#!/usr/bin/env bash
set -euo pipefail
# Compile as the runner user; only execute the selected test binaries as root.
: "${CARGO_TARGET_DIR:?CARGO_TARGET_DIR must be set}"
: "${SWARMY_FDB_CLUSTER_FILE:?start the dev stack first}"
export SWARMY_TEST_IMAGE=base-ubuntu:ci
export SWARMY_DEFAULT_IMAGE="$SWARMY_TEST_IMAGE"
export SWARMY_API_LISTEN=127.0.0.1:0
export SWARMY_API_TOKEN=ci-local-test-token
# Image registration uses a real API; keep it alive for the root tests.
SWARMY_API_LISTEN=127.0.0.1:18391 "$CARGO_TARGET_DIR/debug/swarmy-api" &
api_pid=$!
trap 'kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true' EXIT
export SWARMY_API_URL=http://127.0.0.1:18391
for i in {1..50}; do
    if curl -fsS "$SWARMY_API_URL/v1/health" >/dev/null 2>&1; then break; fi
    kill -0 "$api_pid" || exit 1
    sleep 0.2
done
sudo -E /usr/local/sbin/swarmy-ci-root "$CARGO_TARGET_DIR/debug/swarmy" image build images/base-ubuntu --tag ci
for suite in swarmyd:node swarmy-volume:nbd swarmy-volume:image swarmyd:vol swarmy-cli:image swarmy-chaos:bash swarmy-chaos:continuity swarmy-chaos:coding; do
    package=${suite%%:*}
    test_name=${suite#*:}
    executable=$(cargo test --locked -p "$package" --test "$test_name" --no-run --message-format=json | python3 -c '
import json,sys
matches = [x["executable"] for line in sys.stdin if (x := json.loads(line)).get("reason") == "compiler-artifact" and x["target"]["name"] == sys.argv[1] and x.get("executable")]
if len(matches) != 1: sys.exit(f"Expected one executable for {sys.argv[1]}, got {len(matches)}")
print(matches[0])' "$test_name")
    sudo -E /usr/local/sbin/swarmy-ci-root "$executable" --nocapture --test-threads=1
done
