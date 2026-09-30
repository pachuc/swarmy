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

# The control-plane services provisioning can install on a node. The one
# seed list: unit and binary names derive from these, so adding a service
# touches one place.
swarmy_service_names() {
    printf '%s\n' scheduler worker gateway api
}

# The CLI binary (no unit runs it) and its cargo package (the only binary
# whose package differs). Each name is written exactly once, here.
swarmy_cli_binary() {
    printf 'swarmy\n'
}

swarmy_cli_package() {
    printf 'swarmy-cli\n'
}

# Every systemd unit provisioning installs: unit, binary, owning modes, and
# role (backing services, node agent, or node service). Units that run
# something else leave the binary empty (the stack unit runs dev-stack.sh,
# the tunnel unit runs ssh). Service rows derive from the seed list above.
# This table is the single source installs, upgrades, and teardown read;
# each unit and binary name is written exactly once, here.
swarmy_unit_table() {
    printf '%s\n' \
        'swarmy-stack.service::stack:backing' \
        'swarmy-tunnel.service::node:backing' \
        'swarmyd.service:swarmyd:stack,node:agent'
    local services_text
    services_text=$(swarmy_service_names) || return 1
    [[ -n $services_text ]] || {
        echo 'empty service list' >&2
        return 1
    }
    local services
    mapfile -t services <<< "$services_text"
    local service
    for service in "${services[@]}"; do
        printf 'swarmy-%s.service:swarmy-%s:stack:service\n' "$service" "$service"
    done
}

# One list of the systemd units provisioning installs, in table order.
swarmy_unit_names() {
    swarmy_unit_table | cut -d: -f1
}

# One list of the release binaries provisioning installs, in table order
# with the CLI first.
swarmy_binary_names() {
    swarmy_cli_binary
    swarmy_unit_table | awk -F: '$2 != "" { print $2 }'
}

# The binary a unit runs, or empty for units running something else.
swarmy_unit_binary() {
    swarmy_unit_table | awk -F: -v unit="${1-}" '$1 == unit { print $2 }'
}

# Node-service units (scheduler, worker, gateway, api), in table order.
swarmy_service_units() {
    swarmy_unit_table | awk -F: '$4 == "service" { print $1 }'
}

# Unit for a control-plane service short name, verified against the table
# (empty when the service is unknown, so callers fail loudly).
swarmy_service_unit() {
    swarmy_unit_table | awk -F: -v unit="swarmy-${1-}.service" '$1 == unit { print $1 }'
}

# The node agent unit.
swarmy_agent_unit() {
    swarmy_unit_table | awk -F: '$4 == "agent" { print $1 }'
}

# Backing-service unit of a mode (stack or tunnel): the role both install
# paths and unit files read instead of spelling the name.
swarmy_mode_backing_unit() {
    swarmy_unit_table | awk -F: -v mode="${1-}" '$4 == "backing" && index(","$3",", ","mode",") { print $1 }'
}

# Units a mode owns, backing-service unit first (table order).
swarmy_mode_units() {
    swarmy_unit_table | awk -F: -v mode="${1-}" 'index(","$3",", ","mode",") { print $1 }'
}

# Binaries a mode installs, in install order: the CLI ships on stack hosts
# and every other binary comes from the mode's table rows.
swarmy_mode_binaries() {
    local mode=${1-}
    if [[ $mode == stack ]]; then
        swarmy_cli_binary
    fi
    swarmy_unit_table | awk -F: -v mode="$mode" 'index(","$3",", ","mode",") && $2 != "" { print $2 }'
}

# Cargo build arguments for a mode's binaries: package names match binary
# names (the CLI builds separately with its own feature flags). Emits one
# -p pair per line for mapfile. Each stage is captured before filtering so
# a failing table propagates instead of building an empty package list.
swarmy_mode_build_args() {
    local mode=${1-}
    local table filtered
    table=$(swarmy_unit_table) || return 1
    [[ -n $table ]] || {
        echo 'empty unit table' >&2
        return 1
    }
    filtered=$(printf '%s\n' "$table" | awk -F: -v mode="$mode" 'index(","$3",", ","mode",") && $2 != "" { print $2 }') || return 1
    [[ -n $filtered ]] || {
        echo "no build packages for mode $mode" >&2
        return 1
    }
    local packages package
    mapfile -t packages <<< "$filtered"
    for package in "${packages[@]}"; do
        printf -- '-p\n%s\n' "$package"
    done
}

# Read a list function into an array, failing loudly when the producer
# errors or prints nothing. A bare `mapfile < <(producer)` would hide a
# producer failure and hand the caller an empty list (in a build script,
# an empty package list becomes a whole-workspace build). The text check
# comes before mapfile because mapfile turns empty input into one empty
# element, which a length check would miss.
read_shared_list() {
    local -n list=${1-}
    shift
    local text
    text=$("$@") || return 1
    [[ -n $text ]] || {
        echo "empty list from $*" >&2
        return 1
    }
    mapfile -t list <<< "$text"
}

# Installed control-plane units (the stack unit plus node-services units:
# everything stack mode owns beyond node mode), one per line from the
# installed unit files. Empty output means none are installed. Each stage
# checks its own status instead of relying on the caller's pipefail: a
# failed query is an error, never mistaken for "none".
list_installed_control_units() {
    local raw units control
    raw=$(systemctl list-unit-files --no-legend --no-pager) || return 1
    units=$(printf '%s\n' "$raw" | awk '{ print $1 }') || return 1
    read_shared_list control stack_only_units || return 1
    printf '%s\n' "$units" | grep -xFf <(printf '%s\n' "${control[@]}") || true
}

# Control-plane units: everything stack mode owns beyond node mode (the
# stack unit plus node-services units; never the tunnel or node agent).
stack_only_units() {
    comm -23 <(swarmy_mode_units stack | sort) <(swarmy_mode_units node | sort)
}

# Stop and disable every shared unit so a re-provisioned host never keeps
# the other mode's units or old node-services units running. The caller
# enables what it installs afterwards. Shared by provisioning (before
# installing) and decommissioning (before removing).
disable_previous_units() {
    local units unit
    read_shared_list units swarmy_unit_names
    for unit in "${units[@]}"; do
        sudo systemctl stop "$unit" 2>/dev/null || true
    done
    for unit in "${units[@]}"; do
        sudo systemctl disable "$unit" 2>/dev/null || true
    done
}

# Remove every installed unit, binary, environment, and checkout named in
# the shared table. Idempotent: every removal tolerates a missing file so
# a failed `down` can retry. Needs no checkout beyond this file itself, so
# both the checkout script and piped use call it.
decommission_remove() {
    local service_user=${1-}
    validate_service_user "$service_user" || return 1
    local units unit binaries binary service_repo
    disable_previous_units
    read_shared_list units swarmy_unit_names
    for unit in "${units[@]}"; do
        sudo rm -f "/etc/systemd/system/$unit"
    done
    sudo systemctl daemon-reload
    read_shared_list binaries swarmy_binary_names
    for binary in "${binaries[@]}"; do
        sudo rm -f "/usr/local/bin/$binary"
    done
    sudo rm -f /etc/modules-load.d/swarmy.conf
    sudo rm -rf /etc/swarmy
    service_repo=$(service_repo_for "$service_user")
    sudo rm -rf "$service_repo"
}

# Decommission entrypoint for piped use (no checkout needed): run the
# shared removal when the checkout script file is present (proving the
# checkout is intact), fail loudly when units remain without a checkout,
# and no-op when nothing remains. SWARMY_SYSTEMD_DIR overrides the unit
# directory (tests point it at a fake root).
decommission_probe() {
    local service_user=${1-}
    validate_service_user "$service_user" || return 1
    local repo
    repo=$(service_repo_for "$service_user")
    if [[ -f $repo/scripts/remote-decommission.sh ]]; then
        decommission_remove "$service_user"
    elif ls "${SWARMY_SYSTEMD_DIR:-/etc/systemd/system}"/swarmy*.service >/dev/null 2>&1; then
        echo "swarmy checkout is missing from $repo but swarmy units are still installed; restore the checkout or remove the units by hand (see docs/REMOTE.md)" >&2
        return 1
    else
        echo 'swarmy checkout already removed; nothing to tear down'
    fi
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
