#!/usr/bin/env bash
# Helpers sourced by remote-provision.sh, remote-upgrade.sh,
# remote-services.sh, and their no-sudo argument tests.
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-s3-env.sh"
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
    printf '%s ALL=(ALL) NOPASSWD:ALL\n' "$user" | sudo tee "/etc/sudoers.d/90-swarmy-$user" >/dev/null
    sudo chmod 0440 "/etc/sudoers.d/90-swarmy-$user"
}

# Classify the local-storage setting. Prints `auto`, `device <path>`, or
# `dir <path>`: a block device is formatted and mounted at
# /mnt/swarmy-local, while a directory is used directly (dedicated servers
# usually have their disks already partitioned, often as software RAID).
parse_local_storage() {
    local setting=${1-}
    if [[ -z $setting ]]; then
        printf 'auto\n'
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

# Print the largest unused whole disk, or fail. A disk is usable when it
# backs neither the root filesystem nor any mount, and carries no partitions
# or filesystem signature. Selection never probes vendor or model strings:
# a fresh cloud disk is unformatted whatever its source, and a partitioned
# server disk never matches. Reads /sys/block, or $SWARMY_SYS_BLOCK in tests.
discover_unused_disk() {
    local sys_block=${SWARMY_SYS_BLOCK:-/sys/block}
    local root_source root_disk name size best best_size=0 mounts children
    root_source=$(findmnt -n -o SOURCE / 2>/dev/null) || { echo 'cannot determine the root disk' >&2; return 1; }
    root_disk=$(parent_disk_of "$root_source")
    best=''
    for path in "$sys_block"/*; do
        name=${path##*/}
        case $name in loop*|ram*|fd*|sr*|dm-*) continue;; esac
        [[ -e $path/size ]] || continue
        [[ $(parent_disk_of "/dev/$name") != "$root_disk" ]] || continue
        children=$(lsblk -nr -o NAME "/dev/$name" 2>/dev/null | wc -l)
        [[ $children == 1 ]] || continue
        mounts=$(lsblk -nr -o MOUNTPOINTS "/dev/$name" 2>/dev/null | tr -d '[:space:]')
        [[ -z $mounts ]] || continue
        if blkid "/dev/$name" >/dev/null 2>&1; then continue; fi
        size=$(tr -d '[:space:]' < "$path/size")
        [[ $size =~ ^[0-9]+$ ]] || continue
        if (( size > best_size )); then
            best_size=$size
            best="/dev/$name"
        fi
    done
    [[ -n $best ]] || { echo 'no unused disk found; pass a block device or dir:/path as local storage' >&2; return 1; }
    printf '%s\n' "$best"
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
LD_LIBRARY_PATH=$(dirname "$repo_dir")/.local/lib
ENV
}
