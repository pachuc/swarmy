#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_dir"
# CI starts the stack and exports its connection settings before this step.
cargo run --locked -p swarmy-chaos -- --no-start-stack \
    --sessions 5 --steps 3 --kills 4 --seed 1 "$@"
# The reduced persistent checks exercise production dispatch fencing and durable
# notices without NBD or containers. The process registry test uses real child
# processes. The root bash acceptance adds files, mounts, and node services.
cargo test --locked -p swarmy-worker --bin swarmy-worker \
    node_lost_mid_call_fails_once_and_delayed_retry_has_no_second_notice -- --nocapture
cargo test --locked -p swarmy-worker --bin swarmy-worker \
    expired_lease_moves_next_call_and_eviction_has_distinct_durable_notice -- --nocapture
cargo test --locked -p swarmy-worker --bin swarmy-worker \
    named_agent_node_loss_notifies_every_session_once -- --nocapture
cargo test --locked -p swarmyd --bin swarmyd \
    process_records_outlive_the_launching_exec_and_reject_stale_pids -- --nocapture
