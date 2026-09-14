#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_dir"
# CI starts the stack and exports its connection settings before this step.
cargo run --locked -p swarmy-chaos -- --no-start-stack \
    --sessions 5 --steps 3 --kills 4 --seed 1 "$@"
