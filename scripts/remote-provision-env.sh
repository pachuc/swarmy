#!/usr/bin/env bash
# Helpers sourced by remote-provision.sh, remote-upgrade.sh,
# remote-services.sh, and their no-sudo argument tests.
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-s3-env.sh"
# Read the eleven positional arguments of remote-provision.sh into named
# settings. One definition of the order, shared by the script and its
# argument test; the SSH provisioning command sends them in this order.
parse_provision_args() {
    mode=${1:-stack}
    service_address=${2:-127.0.0.1}
    bucket=${3:-}
    bucket_region=${4:-}
    bucket_endpoint=${5:-}
    bucket_prefix=${6:-}
    bucket_conditional_create=${7:-true}
    bucket_static=${8:-false}
    sandboxes=$(parse_sandbox_count "${9-64}")
    service_user=${10:-swarmy}
    local_storage=${11:-}
}

parse_sandbox_count() {
    local count=${1-}
    [[ $count =~ ^(0|[1-9][0-9]*)$ ]] || { echo 'sandboxes must be a non-negative integer' >&2; return 1; }
    [[ ${#count} -le 10 ]] && (( count <= 4294967295 )) || { echo 'sandboxes exceed u32 range' >&2; return 1; }
    printf '%s\n' "$count"
}

# The login that owns the checkout and runs the node units. Plain servers
# use `swarmy`; cloud-image hosts keep `ubuntu` by setting it.
validate_service_user() {
    local user=${1-}
    [[ $user =~ ^[A-Za-z0-9_-]{1,64}$ ]] || { echo 'service user must contain 1-64 letters, digits, hyphens, or underscores' >&2; return 1; }
}

# One definition of the service home, shared by provisioning, upgrade, and
# the units. Prefers the passwd entry so custom homes keep working.
service_home_for() {
    local user=$1 home
    if [[ $user == root ]]; then
        printf '/root\n'
        return 0
    fi
    if home=$(getent passwd "$user" 2>/dev/null | cut -d: -f6) && [[ -n $home ]]; then
        printf '%s\n' "$home"
    else
        printf '/home/%s\n' "$user"
    fi
}

service_repo_for() {
    printf '%s/swarmy\n' "$(service_home_for "$1")"
}

# cloud-init runs at first boot on cloud images only. Dedicated servers boot
# stock Ubuntu without it, so waiting unconditionally would fail there.
needs_cloud_init_wait() {
    command -v cloud-init >/dev/null 2>&1
}

wait_for_cloud_init() {
    if needs_cloud_init_wait; then
        sudo cloud-init status --wait
    fi
}

# Create the service user with passwordless sudo when it does not exist, so
# a plain server arriving with only a root login can be provisioned.
ensure_service_user() {
    local user=$1
    validate_service_user "$user" || return 1
    if ! id "$user" >/dev/null 2>&1; then
        sudo useradd -m -s /bin/bash "$user"
    fi
    # A bootstrap copy as root can leave an existing home owned by root.
    local home
    home=$(service_home_for "$user")
    if [[ -d $home && $(stat -c %U "$home") != "$user" ]]; then
        sudo chown "$user:$user" "$home"
    fi
    sudo mkdir -p /etc/sudoers.d
    printf '%s ALL=(ALL) NOPASSWD:ALL\n' "$user" | sudo tee "/etc/sudoers.d/90-swarmy-$user" >/dev/null
    sudo chmod 0440 "/etc/sudoers.d/90-swarmy-$user"
    sudo visudo -cf "/etc/sudoers.d/90-swarmy-$user"
}

# Classify the local-storage setting. Prints `none`, `device <path>`, or
# `dir <path>`: a block device is formatted and mounted at
# /mnt/swarmy-local, while a directory is used directly (dedicated servers
# usually have their disks already partitioned, often as software RAID).
# Empty means no local storage; sandbox nodes fail later with a message.
parse_local_storage() {
    local setting=${1-}
    if [[ -z $setting ]]; then
        printf 'none\n'
    elif [[ $setting == dir:* ]]; then
        [[ -n ${setting#dir:} && ${setting#dir:} == /* ]] || { echo 'local storage directory must be an absolute path after dir:' >&2; return 1; }
        printf 'dir %s\n' "${setting#dir:}"
    elif [[ $setting == device:* ]]; then
        [[ -n ${setting#device:} ]] || { echo 'local storage device must not be empty after device:' >&2; return 1; }
        printf 'device %s\n' "${setting#device:}"
    elif [[ $setting == /dev/* ]]; then
        printf 'device %s\n' "$setting"
    elif [[ $setting == /* ]]; then
        printf 'dir %s\n' "$setting"
    else
        echo 'local storage must be a /dev device path, dir:/path, or an absolute directory' >&2
        return 1
    fi
}

# Parent disk of a device path, following partitions to their whole disk.
parent_disk_of() {
    local source=$1 parent
    parent=$(lsblk -nr -o PKNAME "$source" 2>/dev/null | head -1)
    if [[ -n $parent ]]; then
        printf '/dev/%s\n' "$parent"
    else
        printf '%s\n' "$source"
    fi
}

# Point `.swarmy/volumes` at the local-storage volumes directory. One shared
# implementation for device mounts and existing directories.
link_volumes_to() {
    local mount=$1
    sudo mkdir -p "$mount/volumes"
    sudo chown "$service_user:$service_user" "$mount/volumes"
    mkdir -p .swarmy
    if [[ ! -e .swarmy/volumes && ! -L .swarmy/volumes ]]; then
        ln -s "$mount/volumes" .swarmy/volumes
    fi
    [[ $(readlink -f .swarmy/volumes) == "$mount/volumes" ]]
}

# One list of the systemd units and binaries provisioning installs, shared
# by the install paths and the decommission script. Provisioning writes
# swarmy-stack.service or swarmy-tunnel.service (stack or node mode) plus
# always swarmyd.service; node-services mode adds the four control-plane
# units. Decommission stops, disables, and removes every name here, so a
# name missing from this list would be left running while its binary is
# deleted.
swarmy_unit_names() {
    printf '%s\n' swarmy-stack.service swarmy-tunnel.service swarmyd.service swarmy-scheduler.service swarmy-worker.service swarmy-gateway.service swarmy-api.service
}

swarmy_binary_names() {
    printf '%s\n' swarmy swarmyd swarmy-scheduler swarmy-gateway swarmy-worker swarmy-api
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
    local repo_dir=$1 sandboxes=$2 mount=$3 bucket=${4:-} bucket_region=${5:-} bucket_endpoint=${6:-} bucket_prefix=${7:-} bucket_conditional_create=${8:-true} bucket_static=${9:-false} roles=sandbox,volume
    local s3_env
    s3_env=$(swarmy_remote_s3_env "$bucket" "$bucket_region" "$bucket_endpoint" "$bucket_prefix" "$bucket_static")
    if (( sandboxes == 0 )); then roles=volume; fi
    cat <<ENV
SWARMY_FDB_CLUSTER_FILE=$repo_dir/.dev/fdb.cluster
SWARMY_NATS_URL=nats://127.0.0.1:4222
$s3_env
SWARMY_S3_CONDITIONAL_CREATE=$bucket_conditional_create
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
LD_LIBRARY_PATH=$(dirname "$repo_dir")/.local/lib
ENV
}
