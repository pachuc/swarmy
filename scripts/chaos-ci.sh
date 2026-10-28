#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_dir"
# CI starts the stack and exports its connection settings before this step.
cargo run --locked -p swarmy-chaos -- --no-start-stack \
    --sessions 5 --steps 3 --kills 4 --seed 1 --bin-dir "${CARGO_TARGET_DIR:-target}/debug" "$@"

# Rate-limit saturation acceptance: two API keys on one HTTP fake with tiny limits behind one route.
cargo run --locked -p swarmy-chaos -- --no-start-stack --scenario saturation \
    --sessions 10 --steps 3 --kills 0 --schedulers 1 --workers 2 --gateways 2 \
    --bin-dir "${CARGO_TARGET_DIR:-target}/debug"
