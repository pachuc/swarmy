#!/usr/bin/env bash
# Helpers sourced by remote-provision.sh, remote-upgrade.sh,
# remote-services.sh, and their no-sudo argument tests.
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-s3-env.sh"
# Read the twelve positional arguments of remote-provision.sh into named
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
    prebuilt=${12:-false}
}

# Where the client uploads release binaries inside the node directory.
release_bundle_dir() { printf '%s/.swarmy/release\n' "${1:?node directory}"; }
# Where a development client copies its checkout for the on-node build.
node_source_dir() { printf '%s/source\n' "${1:?node directory}"; }
# The commit a development client recorded for its copied checkout (which has
# no .git), or nothing when the file is missing or malformed.
source_git_commit() {
    local file=${1:?source directory}/.swarmy-source-commit commit
    [[ -f $file ]] || return 0
    commit=$(<"$file")
    [[ $commit =~ ^[0-9a-f]{7,40}$ ]] && printf '%s\n' "$commit"
    return 0
}

# Validate all binaries for a mode before installing any of them.
check_release_bundle() {
    local dir=$1 mode=$2 binaries binary
    read_shared_list binaries swarmy_mode_binaries "$mode" || return 1
    for binary in "${binaries[@]}"; do
        [[ -f $dir/$binary && -x $dir/$binary ]] || { echo "Missing uploaded release binary: $dir/$binary" >&2; return 1; }
    done
    [[ -f $dir/libfdb_c.so ]] || { echo "Missing uploaded libfdb_c.so in $dir" >&2; return 1; }
}

swarmy_core_library_dir() { printf '/usr/local/lib/swarmy\n'; }

# Files provisioning places in shared system drop-in directories start with
# this line. Down removes such a file only when it still starts with it, and
# provisioning never replaces a file without it, so a file the host's own
# setup wrote is never overwritten or deleted.
swarmy_owned_marker() { printf '# Written by swarmy provisioning; swarmy swarm down removes this file.\n'; }
swarmy_modules_file() { printf '/etc/modules-load.d/swarmy-provisioned.conf\n'; }
swarmy_library_conf() { printf '/etc/ld.so.conf.d/swarmy-provisioned.conf\n'; }
owned_by_swarmy() {
    [[ $(sudo -n head -n 1 "${1:?path}" 2>/dev/null) == "$(swarmy_owned_marker)" ]]
}
# Write stdin to PATH after the marker line. Refuses a file swarmy did not write.
install_owned_file() {
    local path=${1:?path} body
    body=$(cat)
    if sudo -n test -e "$path" && ! owned_by_swarmy "$path"; then
        echo "$path exists and was not written by swarmy provisioning; move it away and rerun" >&2
        return 1
    fi
    { swarmy_owned_marker; printf '%s\n' "$body"; } | sudo -n tee "$path" >/dev/null
}
remove_owned_file() {
    local path=${1:?path}
    if owned_by_swarmy "$path"; then
        sudo -n rm -f "$path"
    fi
}
install_core_library() {
    local dir
    dir=$(swarmy_core_library_dir)
    sudo -n install -d -m 0755 "$dir" /etc/ld.so.conf.d
    sudo -n install -m 0644 "$1" "$dir/libfdb_c.so"
    printf '%s\n' "$dir" | install_owned_file "$(swarmy_library_conf)"
    sudo -n ldconfig
}

parse_sandbox_count() {
    local count=${1-}
    [[ $count =~ ^(0|[1-9][0-9]*)$ ]] || { echo 'sandboxes must be a non-negative integer' >&2; return 1; }
    [[ ${#count} -le 10 ]] && (( count <= 4294967295 )) || { echo 'sandboxes exceed u32 range' >&2; return 1; }
    printf '%s\n' "$count"
}

# The login that owns the node directory and runs the node units. Plain servers
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
# seed list: unit and role names derive from these, so adding a service
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

swarmy_core_binary() { printf 'swarmy-core\n'; }
swarmy_core_package() { printf 'swarmy-core-bin\n'; }
swarmy_provisioner_binary() { printf 'swarmy-provisioner\n'; }
swarmy_provisioner_package() { printf 'swarmy-provisioner\n'; }

# Every systemd unit provisioning installs: unit, core role, owning modes, and
# kind (backing service, node agent, or node service). Units that run
# something else leave the role empty. Service rows derive from the seed list.
# This table is the single source installs, upgrades, and teardown read.
swarmy_unit_table() {
    printf '%s\n' \
        'swarmy-stack.service::stack:backing' \
        'swarmy-tunnel.service::node:backing' \
        'swarmyd.service:node:stack,node:agent'
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
        printf 'swarmy-%s.service:%s:stack:service\n' "$service" "$service"
    done
    printf '%s\n' 'swarmy-provisioner.service::stack:provisioner'
}

# One list of the systemd units provisioning installs, in table order.
swarmy_unit_names() {
    swarmy_unit_table | cut -d: -f1
}

# One list of installed binaries, with the CLI first.
swarmy_binary_names() {
    swarmy_cli_binary
    swarmy_core_binary
    swarmy_provisioner_binary
}

# These service binaries were replaced by `swarmy-core <role>`.
retired_binary_names() {
    printf '%s\n' swarmyd swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api
}

remove_retired_binaries() {
    local binaries binary
    read_shared_list binaries retired_binary_names
    for binary in "${binaries[@]}"; do
        sudo -n rm -f "/usr/local/bin/$binary"
    done
}

# The core role a unit runs, or empty for non-core units.
swarmy_unit_role() {
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

swarmy_provisioner_unit() {
    swarmy_unit_table | awk -F: '$4 == "provisioner" { print $1 }'
}

# The node agent unit.
swarmy_agent_unit() {
    swarmy_unit_table | awk -F: '$4 == "agent" { print $1 }'
}

# Unit inputs are recorded separately from the secret node environment.
swarmy_systemd_dir() { printf '%s\n' "${SWARMY_SYSTEMD_DIR:-/etc/systemd/system}"; }
swarmy_etc_dir() { printf '%s\n' "${SWARMY_ETC_DIR:-/etc/swarmy}"; }
swarmy_unit_inputs_file() { printf '%s/units.env\n' "$(swarmy_etc_dir)"; }

set_unit_user() {
    validate_service_user "${1-}" || return 1
    unit_user=$1
    unit_home=$(service_home_for "$unit_user")
    unit_repo=$(service_repo_for "$unit_user")
}

set_unit_inputs() {
    local mode=${1-} user=${2-} primary=${3-} tunnel_s3=${4-} mount=${5-}
    [[ $mode == stack || $mode == node ]] || { echo "invalid unit mode: $mode" >&2; return 1; }
    set_unit_user "$user" || return 1
    if [[ $mode == node || -n $primary ]]; then
        [[ $primary =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "invalid primary address: $primary" >&2; return 1; }
    fi
    [[ $tunnel_s3 == true || $tunnel_s3 == false ]] || { echo 'tunnel S3 forward must be true or false' >&2; return 1; }
    [[ -z $mount || $mount =~ ^/[A-Za-z0-9._/-]+$ ]] || { echo "invalid mount path: $mount" >&2; return 1; }
    unit_mode=$mode unit_primary=$primary unit_tunnel_s3=$tunnel_s3 unit_mount=$mount
}

unit_inputs_text() {
    printf 'SWARMY_UNIT_MODE=%s\nSWARMY_UNIT_USER=%s\nSWARMY_UNIT_PRIMARY=%s\nSWARMY_UNIT_TUNNEL_S3=%s\nSWARMY_UNIT_MOUNT=%s\n' \
        "$unit_mode" "$unit_user" "$unit_primary" "$unit_tunnel_s3" "$unit_mount"
}

record_unit_inputs() {
    local file
    file=$(swarmy_unit_inputs_file)
    sudo -n install -d -m 0755 "$(dirname "$file")" || return 1
    unit_inputs_text | sudo -n tee "$file" >/dev/null || return 1
    sudo -n chmod 0644 "$file"
}

# Treat the recorded file as data, not shell code.
load_unit_inputs() {
    local file=${1-} key value mode='' user='' primary='' tunnel_s3='' mount=''
    while IFS='=' read -r key value; do
        case $key in
            SWARMY_UNIT_MODE) mode=$value ;;
            SWARMY_UNIT_USER) user=$value ;;
            SWARMY_UNIT_PRIMARY) primary=$value ;;
            SWARMY_UNIT_TUNNEL_S3) tunnel_s3=$value ;;
            SWARMY_UNIT_MOUNT) mount=$value ;;
            '') ;;
            *) echo "unknown setting $key in $file" >&2; return 1 ;;
        esac
    done < "$file" || return 1
    set_unit_inputs "$mode" "$user" "$primary" "$tunnel_s3" "$mount"
}

render_stack_unit() {
    : "${unit_repo:?unit inputs not set}"
    cat <<UNIT
[Unit]
Description=Swarmy backing services (FoundationDB, NATS, optional SeaweedFS)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=$unit_user
WorkingDirectory=$unit_repo
Environment=HOME=$unit_home
EnvironmentFile=/etc/swarmy/node.env
Environment=PATH=$unit_home/.local/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin
# systemd owns these processes, so stale state from an interrupted boot is safe to clear.
ExecStartPre=/usr/bin/rm -f $unit_repo/.dev/fdb.pid $unit_repo/.dev/nats.pid $unit_repo/.dev/seaweed.pid
ExecStartPre=-/usr/bin/rmdir $unit_repo/.dev/lock
ExecStart=/bin/bash $unit_repo/scripts/dev-stack.sh start
ExecStop=/bin/bash $unit_repo/scripts/dev-stack.sh stop
TimeoutStartSec=300
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
UNIT
}

render_tunnel_unit() {
    : "${unit_primary:?unit inputs not set}"
    local s3_forward=''
    if [[ $unit_tunnel_s3 == true ]]; then s3_forward=' -L 127.0.0.1:8333:127.0.0.1:8333'; fi
    cat <<UNIT
[Unit]
Description=Swarmy tunnel to first node backing services
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=$unit_user
ExecStart=/usr/bin/ssh -N -T -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/etc/swarmy/tunnel-known-hosts -o ExitOnForwardFailure=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -i /etc/swarmy/tunnel-key -L 127.0.0.1:4500:127.0.0.1:4500 -L 127.0.0.1:4222:127.0.0.1:4222$s3_forward $unit_user@$unit_primary
ExecStartPost=/bin/bash -c 'for attempt in {1..30}; do if (echo > /dev/tcp/127.0.0.1/4500) 2>/dev/null; then exit 0; fi; sleep 1; done; exit 1'
Restart=always
RestartSec=5
TimeoutStartSec=40

[Install]
WantedBy=multi-user.target
UNIT
}

render_node_unit() {
    : "${unit_mode:?unit inputs not set}"
    local stack_dependency dependency_kind=Wants mount_requirement='' node_unit
    stack_dependency=$(swarmy_mode_backing_unit "$unit_mode") || return 1
    node_unit=$(swarmy_agent_unit) || return 1
    # Keep the node role running across tunnel reconnects; its clients reconnect too.
    if [[ $unit_mode == stack ]]; then dependency_kind=Requires; fi
    if [[ -n $unit_mount ]]; then mount_requirement="RequiresMountsFor=$unit_mount"; fi
    cat <<UNIT
[Unit]
Description=Swarmy node agent (swarmy-core node)
$dependency_kind=$stack_dependency
After=network-online.target $stack_dependency systemd-modules-load.service
Wants=network-online.target
${mount_requirement}

[Service]
WorkingDirectory=$unit_repo
EnvironmentFile=/etc/swarmy/node.env
ExecStart=/usr/local/bin/$(swarmy_core_binary) $(swarmy_unit_role "$node_unit")
Restart=always
RestartSec=5
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
UNIT
}

render_service_unit() {
    local unit=${1:?unit} role short stack_unit
    : "${unit_repo:?unit inputs not set}"
    role=$(swarmy_unit_role "$unit")
    [[ -n $role ]] || { echo "unknown service unit $unit" >&2; return 1; }
    short=${unit#swarmy-}; short=${short%.service}
    stack_unit=$(swarmy_mode_backing_unit stack) || return 1
    cat <<UNIT
[Unit]
Description=Swarmy $short
Requires=$stack_unit
After=network-online.target $stack_unit

[Service]
Type=exec
User=$unit_user
WorkingDirectory=$unit_repo
EnvironmentFile=/etc/swarmy/node.env
Environment=HOME=$unit_home
Environment=TOKIO_WORKER_THREADS=2
ExecStart=/usr/local/bin/$(swarmy_core_binary) $role
Restart=always
RestartSec=2
TimeoutStopSec=40

[Install]
WantedBy=multi-user.target
UNIT
}

render_provisioner_unit() {
    : "${unit_repo:?unit inputs not set}"
    local stack_unit
    stack_unit=$(swarmy_mode_backing_unit stack) || return 1
    cat <<UNIT
[Unit]
Description=Swarmy provisioner (elastic machine groups)
Requires=$stack_unit
After=network-online.target $stack_unit

[Service]
Type=exec
User=$unit_user
WorkingDirectory=$unit_repo
EnvironmentFile=/etc/swarmy/node.env
Environment=HOME=$unit_home
Environment=TOKIO_WORKER_THREADS=2
ExecStart=/usr/local/bin/$(swarmy_provisioner_binary)
Restart=always
RestartSec=10
KillMode=mixed
TimeoutStopSec=900

[Install]
WantedBy=multi-user.target
UNIT
}

render_unit() {
    local unit=${1-} kind
    kind=$(swarmy_unit_table | awk -F: -v unit="$unit" '$1 == unit { print $4 }')
    case $kind in
        backing)
            if [[ $unit == "$(swarmy_mode_backing_unit stack)" ]]; then render_stack_unit
            else render_tunnel_unit; fi ;;
        agent) render_node_unit ;;
        service) render_service_unit "$unit" ;;
        provisioner) render_provisioner_unit ;;
        *) echo "no renderer for unit $unit" >&2; return 1 ;;
    esac
}

# Render all before installing any; only install bytewise differences.
write_units() {
    local -n __written=${1:?result array}
    shift
    local dir stage unit
    dir=$(swarmy_systemd_dir)
    stage=$(mktemp -d) || return 1
    for unit in "$@"; do
        render_unit "$unit" > "$stage/$unit" || { rm -rf "$stage"; return 1; }
    done
    for unit in "$@"; do
        if [[ -f $dir/$unit ]] && cmp -s "$stage/$unit" "$dir/$unit"; then continue; fi
        sudo -n install -m 0644 "$stage/$unit" "$dir/$unit" || { rm -rf "$stage"; return 1; }
        __written+=("$unit")
    done
    rm -rf "$stage"
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

# Binaries a mode installs: the CLI ships on stack hosts and the core on all.
swarmy_mode_binaries() {
    local mode=${1-}
    if [[ $mode == stack ]]; then
        swarmy_cli_binary
    fi
    swarmy_core_binary
    if [[ $mode == stack ]]; then swarmy_provisioner_binary; fi
}

# Cargo build arguments for the core binary (the CLI builds separately).
# Emit one -p pair for mapfile. The table is checked before filtering so
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
    printf -- '-p\n%s\n' "$(swarmy_core_package)"
    if [[ $mode == stack ]]; then printf -- '-p\n%s\n' "$(swarmy_provisioner_package)"; fi
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

# Remove every installed unit, binary, environment, and node directory named in
# the shared table. Idempotent: every removal tolerates a missing file so
# a failed `down` can retry. Needs no node directory beyond this file itself, so
# both the installed script and piped use call it. Drop-in files under
# /etc/modules-load.d and /etc/ld.so.conf.d go only when they carry swarmy's marker line.
decommission_remove() {
    local service_user=${1-}
    validate_service_user "$service_user" || return 1
    local units unit binaries binary service_repo service_home
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
    remove_retired_binaries
    sudo rm -rf "$(swarmy_core_library_dir)"
    remove_owned_file "$(swarmy_library_conf)"
    sudo ldconfig
    remove_owned_file "$(swarmy_modules_file)"
    sudo rm -rf /etc/swarmy
    service_repo=$(service_repo_for "$service_user")
    # The provisioner's working data (each elastic group's OpenTofu directory,
    # empty by now because down waits for every elastic group to reach zero,
    # and the group SSH keys) and its OpenTofu download cache. The unit sets
    # only HOME, so these are the XDG defaults.
    service_home=$(service_home_for "$service_user")
    sudo rm -rf "$service_home/.local/share/swarmy/provisioner" "$service_home/.cache/swarmy/tofu"
    sudo rm -rf "$service_repo"
}

# Agents placed on this node whose sandboxes are busy, as a JSON array ('[]' when idle).
# After the node agent stops, every placement it did not release counts as busy.
swarmy_busy_sandboxes() {
    local repo_dir=${1:?node directory} binary=${2:?core binary} role
    role=$(swarmy_unit_role "$(swarmy_agent_unit)") || return 1
    sudo -n sh -c "cd $repo_dir && set -a && . $(swarmy_etc_dir)/node.env && set +a && exec $binary $role --upgrade-processes"
}

# A clean stop checkpoints and releases every computer before removing a node.
drain_node_agent() {
    local service_user=${1-} timeout=${2:?seconds} mode=${3:?wait or force}
    validate_service_user "$service_user" || return 1
    [[ $mode == wait || $mode == force ]] || return 1
    local repo unit binary deadline busy='[]' after
    repo=$(service_repo_for "$service_user")
    unit=$(swarmy_agent_unit)
    binary=${SWARMY_BIN_DIR:-/usr/local/bin}/$(swarmy_core_binary)
    deadline=$((SECONDS + timeout))
    while sudo -n systemctl is-active --quiet "$unit"; do
        busy=$(swarmy_busy_sandboxes "$repo" "$binary") || return 1
        [[ $busy == '[]' ]] && break
        if (( SECONDS >= deadline )); then
            if [[ $mode == wait ]]; then
                printf 'drain {"stopped":false,"busy":%s,"unreleased":[]}\n' "$busy"
                return 0
            fi
            break
        fi
        echo "Waiting for running sandbox commands: $busy" >&2
        sleep "${SWARMY_DRAIN_POLL_SECONDS:-5}"
    done
    if sudo -n systemctl is-active --quiet "$unit"; then
        sudo -n systemctl disable --now "$unit" || return 1
    else
        busy='[]'
    fi
    after=$(swarmy_busy_sandboxes "$repo" "$binary") || return 1
    printf 'drain {"stopped":true,"busy":%s,"unreleased":%s}\n' "$busy" "$after"
}

# Decommission entrypoint for piped use (no node directory needed): run the
# shared removal when the node script file is present (proving the
# node directory is intact), fail loudly when units remain without a node directory,
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
        echo "swarmy node directory is missing from $repo but swarmy units are still installed; restore it with swarmy swarm upgrade or remove the units by hand (see docs/REMOTE.md)" >&2
        return 1
    else
        echo 'swarmy node directory already removed; nothing to tear down'
    fi
}

# Installed units in dependency order; an empty list is valid.
installed_swarmy_units() {
    local units unit installed
    read_shared_list units swarmy_unit_names || return 1
    installed=$(systemctl list-unit-files --no-legend --no-pager | awk '{ print $1 }') || return 1
    for unit in "${units[@]}"; do
        if grep -qxF -- "$unit" <<< "$installed"; then printf '%s\n' "$unit"; fi
    done
}

# Disable dependents before backing units so they stay stopped across reboot.
stop_installed_units() {
    local units i text
    text=$(installed_swarmy_units) || return 1
    [[ -n $text ]] || { echo 'no swarmy units installed'; return 0; }
    mapfile -t units <<< "$text"
    for (( i = ${#units[@]} - 1; i >= 0; i-- )); do
        sudo systemctl disable --now "${units[i]}"
    done
}

start_installed_units() {
    local units unit text
    text=$(installed_swarmy_units) || return 1
    [[ -n $text ]] || { echo 'no swarmy units installed'; return 0; }
    mapfile -t units <<< "$text"
    for unit in "${units[@]}"; do
        sudo systemctl enable --now "$unit"
    done
}

node_disk_bytes() {
    local sandboxes=$1 mount=$2
    if (( sandboxes == 0 )); then
        printf '0\n'
    else
        df -B1 --output=size "$mount" | tail -1 | tr -d ' '
    fi
}

# Default sandbox memory in MiB. Equal to
# swarmy_core::SandboxRequirements::default().memory_mib; a Rust test in
# crates/swarmy-cloud/src/node_files.rs keeps the two the same.
SWARMY_DEFAULT_SANDBOX_MIB=768

# Bytes this host offers sandboxes: MemTotal less the system reserve, never
# negative. SWARMY_MEMINFO overrides /proc/meminfo for tests.
node_memory_bytes() {
    awk -v reserve="${SWARMY_NODE_MEMORY_RESERVE_MIB:-3072}" \
        '/MemTotal/ {bytes = ($2 - reserve * 1024) * 1024; printf "%.0f", (bytes > 0 ? bytes : 0)}' \
        "${SWARMY_MEMINFO:-/proc/meminfo}"
}

# A host with sandboxes must fit one default sandbox after the reserve.
check_node_memory() {
    local sandboxes=${1:?sandbox count} bytes
    if (( sandboxes == 0 )); then return 0; fi
    bytes=$(node_memory_bytes) || return 1
    if (( bytes < SWARMY_DEFAULT_SANDBOX_MIB * 1024 * 1024 )); then
        printf 'This host offers %d MiB to sandboxes after the %d MiB system reserve, less than one default %d MiB sandbox. A sandbox node needs at least %d MiB of memory as the kernel reports it (MemTotal in /proc/meminfo). A 4 GiB machine falls short; choose a size with 8 GiB or more.\n' \
            "$((bytes / 1024 / 1024))" "${SWARMY_NODE_MEMORY_RESERVE_MIB:-3072}" \
            "$SWARMY_DEFAULT_SANDBOX_MIB" \
            "$(( ${SWARMY_NODE_MEMORY_RESERVE_MIB:-3072} + SWARMY_DEFAULT_SANDBOX_MIB ))" >&2
        return 1
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
SWARMY_NODE_MEMORY_BYTES=$(node_memory_bytes)
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

# NVIDIA's PCI vendor id is 0x10de; display controllers have class 0x03xxxx.
nvidia_card_present() {
    local device
    for device in "${SWARMY_PCI_DEVICES_DIR:-/sys/bus/pci/devices}"/*; do
        [[ -r $device/vendor && -r $device/class ]] || continue
        if [[ $(<"$device/vendor") == 0x10de && $(<"$device/class") == 0x03* ]]; then
            return 0
        fi
    done
    return 1
}
# The full driver package includes the X.Org driver and GLX libraries.
# On Ubuntu 24.04 the 570 server package is a transitional name for 580.
nvidia_driver_package() { printf 'nvidia-driver-580-server\n'; }
# swarmyd's default JSON CDI spec path.
nvidia_cdi_spec() { printf '/etc/cdi/nvidia.json\n'; }
# Regenerate on every run because driver upgrades rename libraries.
provision_nvidia() {
    if ! command -v nvidia-smi >/dev/null; then
        sudo -E apt-get install -y "$(nvidia_driver_package)"
        # The card can take a few seconds to answer after the module loads;
        # only a driver that stays unloaded needs a reboot.
        local tries=0 smi
        sudo modprobe nvidia || true
        until smi=$(nvidia-smi 2>&1); do
            if (( ++tries >= ${SWARMY_NVIDIA_WAIT_SECONDS:-30} )); then
                printf '%s\n' "$smi" >&2
                echo 'The NVIDIA driver is installed but not loaded; reboot the machine and rerun provisioning.' >&2
                return 1
            fi
            sleep 1
        done
    fi
    if ! command -v nvidia-ctk >/dev/null; then
        local keyring=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
        curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --batch --yes --dearmor -o "$keyring"
        curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
            | sed "s#deb https://#deb [signed-by=$keyring] https://#g" \
            | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list >/dev/null
        sudo apt-get update
        sudo -E apt-get install -y nvidia-container-toolkit
    fi
    sudo install -d -m 0755 "$(dirname "$(nvidia_cdi_spec)")"
    sudo nvidia-ctk cdi generate --output="$(nvidia_cdi_spec)" --format=json
}
