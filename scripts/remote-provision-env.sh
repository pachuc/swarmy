#!/usr/bin/env bash
# Helpers sourced by remote-provision.sh and its no-sudo argument tests.
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-s3-env.sh"
parse_sandbox_count() {
    local count=${1-}
    [[ $count =~ ^(0|[1-9][0-9]*)$ ]] || { echo 'sandboxes must be a non-negative integer' >&2; return 1; }
    [[ ${#count} -le 10 ]] && (( count <= 4294967295 )) || { echo 'sandboxes exceed u32 range' >&2; return 1; }
    printf '%s\n' "$count"
}

node_disk_bytes() {
    local sandboxes=$1 mount=$2
    if (( sandboxes == 0 )); then
        printf '0\n'
    else
        df -B1 --output=size "$mount" | tail -1 | tr -d ' '
    fi
}

node_environment() {
    local repo_dir=$1 sandboxes=$2 mount=$3 bucket=${4:-} bucket_region=${5:-} roles=sandbox,volume
    local s3_env
    s3_env=$(swarmy_remote_s3_env "$bucket" "$bucket_region")
    if (( sandboxes == 0 )); then roles=volume; fi
    cat <<ENV
SWARMY_FDB_CLUSTER_FILE=$repo_dir/.dev/fdb.cluster
SWARMY_NATS_URL=nats://127.0.0.1:4222
$s3_env
SWARMY_NODE_CPU_MILLIS=$(($(nproc) * 1000))
SWARMY_NODE_MEMORY_BYTES=$(awk -v reserve="${SWARMY_NODE_MEMORY_RESERVE_MIB:-3072}" '/MemTotal/ {bytes = ($2 - reserve * 1024) * 1024; printf "%.0f", (bytes > 0 ? bytes : 0)}' /proc/meminfo)
SWARMY_NODE_DISK_BYTES=$(node_disk_bytes "$sandboxes" "$mount")
SWARMY_NODE_SANDBOXES=$sandboxes
SWARMY_NODE_ROLES=$roles
# Long-lived workers rewrite build caches constantly; keep few snapshots and
# reclaim unreferenced chunks quickly so the node's object store stays small.
SWARMY_VOLUME_SNAPSHOT_RETENTION=3
# Fleet workers clone fresh per task, so a half-hour recovery point is
# plenty and cuts object-store writes to a third of the ten-minute default.
SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS=1800
SWARMY_GC_GRACE_SECONDS=1800
SWARMY_GC_INTERVAL_SECONDS=600
LD_LIBRARY_PATH=/home/ubuntu/.local/lib
ENV
}
