#!/usr/bin/env bash
# Provision the checkout copied to the service user's home. Safe to rerun.
# Arguments: MODE SERVICE_ADDRESS [BUCKET] [BUCKET_REGION] [SANDBOXES]
#   [SERVICE_USER] [LOCAL_STORAGE]. SERVICE_USER owns the checkout and the
#   units (default swarmy); LOCAL_STORAGE is a block device to format and
#   mount at /mnt/swarmy-local or dir:/path for an existing directory.
#   Sandbox nodes require it; control-only nodes use the root disk.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
mode=${1:-stack}
service_address=${2:-127.0.0.1}
bucket=${3:-}
bucket_region=${4:-}
sandboxes=$(parse_sandbox_count "${5-64}")
service_user=${6:-swarmy}
local_storage=${7:-}
validate_service_user "$service_user"
ensure_service_user "$service_user"
# Privileged setup runs as any sudoer, but the build and the units belong to
# the service user; re-enter as that user so files land owned correctly.
if [[ $(id -un) != "$service_user" ]]; then
    exec sudo -H -u "$service_user" bash "$0" "$@"
fi
service_home=$(service_home_for "$service_user")
service_repo=$(service_repo_for "$service_user")
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
[[ $repo_dir == "$service_repo" ]] || { echo "Expected checkout at $service_repo" >&2; exit 1; }
cd "$repo_dir"
# A root bootstrap copy leaves the tree owned by root; builds run as the
# service user, so fix ownership once instead of failing halfway.
if [[ $(stat -c %U "$repo_dir") != "$service_user" ]]; then
    sudo chown -R "$service_user:$service_user" "$repo_dir"
fi
if [[ -n $bucket ]] && [[ ! $bucket =~ ^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$ || ! $bucket_region =~ ^[a-z0-9-]+$ ]]; then
    echo 'Invalid bucket name or region: use a 3-63 character lowercase DNS name without dots and a region.' >&2
    exit 1
fi
[[ $mode == stack || $mode == node ]] || { echo 'Expected stack or node mode' >&2; exit 1; }
[[ $service_address =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 1
storage=$(parse_local_storage "$local_storage")
stack_dependency=''
if [[ $mode == stack ]]; then
    stack_dependency='swarmy-stack.service'
    dependency_kind=Requires
else
    stack_dependency='swarmy-tunnel.service'
    # Keep swarmyd running across tunnel reconnects; its clients reconnect too.
    dependency_kind=Wants
fi
export DEBIAN_FRONTEND=noninteractive
# Dedicated servers boot stock Ubuntu without cloud-init; only cloud images wait.
wait_for_cloud_init
sudo apt-get update
sudo -E apt-get install -y build-essential pkg-config libssl-dev clang libclang-dev curl rsync \
    runc passt iproute2 util-linux e2fsprogs debootstrap
printf 'nbd\nublk_drv\n' | sudo tee /etc/modules-load.d/swarmy.conf >/dev/null
# Some Ubuntu kernels ship these drivers outside the base package.
if ! sudo modprobe nbd nbds_max=64 || ! sudo modprobe ublk_drv; then
    sudo -E apt-get install -y "linux-modules-extra-$(uname -r)"
    sudo modprobe nbd nbds_max=64
    sudo modprobe ublk_drv
fi

# Sandbox scratch lives on local storage; control-only nodes keep everything
# on the root disk. A device is formatted once and mounted by label, while an
# existing directory (already partitioned server disks) is used directly.
# The setting is explicit: sandbox nodes fail without one.
local_mount=/mnt/swarmy-local
storage_is_mount=false
if [[ $storage == device\ * ]]; then
    local_device=${storage#device }
    [[ -b $local_device ]] || { echo "Local storage device not found: $local_device" >&2; exit 1; }
    [[ $(parent_disk_of "$local_device") != "$(parent_disk_of "$(findmnt -n -o SOURCE /)")" ]] || { echo "Refusing to use the root disk $local_device for local storage" >&2; exit 1; }
    sudo mkdir -p "$local_mount"
    if ! sudo blkid "$local_device" >/dev/null 2>&1; then
        # Refuse a disk with partitions or mounts even if it has no filesystem signature.
        [[ $(lsblk -nr -o NAME "$local_device" | wc -l) == 1 ]]
        [[ -z $(lsblk -nr -o MOUNTPOINTS "$local_device" | tr -d '[:space:]') ]]
        sudo mkfs.ext4 -L swarmy-local "$local_device"
    fi
    [[ $(sudo blkid -s LABEL -o value "$local_device") == swarmy-local ]] || { echo 'Refusing to reuse an unrecognized local storage filesystem' >&2; exit 1; }
    if ! mountpoint -q "$local_mount"; then
        sudo mount "$local_device" "$local_mount"
    fi
    [[ $(findmnt -n -o SOURCE --target "$local_mount") == "$local_device" ]]
    if ! grep -q '^LABEL=swarmy-local ' /etc/fstab; then
        printf 'LABEL=swarmy-local /mnt/swarmy-local ext4 defaults,nofail 0 2\n' | sudo tee -a /etc/fstab >/dev/null
    fi
    storage_is_mount=true
    link_volumes_to "$local_mount"
elif [[ $storage == dir\ * ]]; then
    local_mount=${storage#dir }
    link_volumes_to "$local_mount"
else
    # With no mount, the volume server and scratch directories use the root disk.
    (( sandboxes == 0 )) || { echo 'Local storage is required for sandbox nodes: pass a block device or dir:/path' >&2; exit 1; }
    mkdir -p .swarmy/volumes
fi
# Keep node identity, backing data, and configuration on the root disk.
[[ -e .swarmy/config.toml ]] || touch .swarmy/config.toml

if ! command -v rustup >/dev/null && [[ ! -x $HOME/.cargo/bin/rustup ]]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/swarmy-rustup.sh
    sh /tmp/swarmy-rustup.sh -y --profile minimal --default-toolchain none
fi
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
rustup show
# A version marker avoids downloading the backing tools again on a rerun.
tools_hash=$(sha256sum scripts/install-dev-tools.sh | cut -d' ' -f1)
if [[ ! -f .swarmy/remote-tools-version || $(<.swarmy/remote-tools-version) != "$tools_hash" ]]; then
    bash scripts/install-dev-tools.sh
    printf '%s\n' "$tools_hash" > .swarmy/remote-tools-version
fi
# Check available memory after installing tools, before the expensive release build.
# The node CLI excludes provisioning SDKs, but the services still need headroom.
mem_available_kib=$(awk '/^MemAvailable:/ { print $2 }' /proc/meminfo)
mem_total_kib=$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo)
if (( mem_available_kib < 6 * 1024 * 1024 )); then
    printf 'Release build needs at least 6 GiB available memory; this machine has %s MiB total and %s MiB available. Use at least an 8 GiB machine, or free memory before provisioning.\n' \
        "$((mem_total_kib / 1024))" "$((mem_available_kib / 1024))" >&2
    exit 1
fi
build_started=$SECONDS
if [[ $mode == stack ]]; then
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked -p swarmy-cli --no-default-features
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked \
        -p swarmyd -p swarmy-scheduler -p swarmy-gateway -p swarmy-worker -p swarmy-api
    sudo install -m 0755 target/release/{swarmy,swarmyd,swarmy-scheduler,swarmy-gateway,swarmy-worker,swarmy-api} /usr/local/bin/
else
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked -p swarmyd
    sudo install -m 0755 target/release/swarmyd /usr/local/bin/
fi
printf 'Release build took %s seconds\n' "$((SECONDS - build_started))"
sudo install -d -m 0755 /etc/swarmy
sudo install -m 0600 /dev/null /etc/swarmy/node.env
node_environment "$repo_dir" "$sandboxes" "$local_mount" "$bucket" "$bucket_region" | sudo tee /etc/swarmy/node.env >/dev/null
if [[ $mode == stack ]]; then
sudo tee /etc/systemd/system/swarmy-stack.service >/dev/null <<UNIT
[Unit]
Description=Swarmy backing services (FoundationDB, NATS, optional SeaweedFS)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=$service_user
WorkingDirectory=$repo_dir
Environment=HOME=$service_home
EnvironmentFile=/etc/swarmy/node.env
Environment=PATH=$service_home/.local/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin
# systemd owns these processes, so stale state from an interrupted boot is safe to clear.
ExecStartPre=/usr/bin/rm -f $repo_dir/.dev/fdb.pid $repo_dir/.dev/nats.pid $repo_dir/.dev/seaweed.pid
ExecStartPre=-/usr/bin/rmdir $repo_dir/.dev/lock
ExecStart=/bin/bash $repo_dir/scripts/dev-stack.sh start
ExecStop=/bin/bash $repo_dir/scripts/dev-stack.sh stop
TimeoutStartSec=300
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
UNIT
else
[[ -s .swarmy/tunnel-key && -s .swarmy/tunnel-known-hosts ]] || { echo 'Missing add-node tunnel identity' >&2; exit 1; }
sudo install -o "$service_user" -g "$service_user" -m 0600 .swarmy/tunnel-key /etc/swarmy/tunnel-key
sudo install -o "$service_user" -g "$service_user" -m 0600 .swarmy/tunnel-known-hosts /etc/swarmy/tunnel-known-hosts
s3_forward=''
if [[ -z $bucket ]]; then s3_forward=' -L 127.0.0.1:8333:127.0.0.1:8333'; fi
sudo tee /etc/systemd/system/swarmy-tunnel.service >/dev/null <<UNIT
[Unit]
Description=Swarmy tunnel to first node backing services
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=$service_user
ExecStart=/usr/bin/ssh -N -T -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/etc/swarmy/tunnel-known-hosts -o ExitOnForwardFailure=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -i /etc/swarmy/tunnel-key -L 127.0.0.1:4500:127.0.0.1:4500 -L 127.0.0.1:4222:127.0.0.1:4222$s3_forward $service_user@$service_address
ExecStartPost=/bin/bash -c 'for attempt in {1..30}; do if (echo > /dev/tcp/127.0.0.1/4500) 2>/dev/null; then exit 0; fi; sleep 1; done; exit 1'
Restart=always
RestartSec=5
TimeoutStartSec=40

[Install]
WantedBy=multi-user.target
UNIT
fi
mount_requirement=''
if [[ $storage_is_mount == true ]]; then mount_requirement="RequiresMountsFor=$local_mount"; fi
sudo tee /etc/systemd/system/swarmyd.service >/dev/null <<UNIT
[Unit]
Description=Swarmy node agent
$dependency_kind=$stack_dependency
After=network-online.target $stack_dependency systemd-modules-load.service
Wants=network-online.target
${mount_requirement}

[Service]
WorkingDirectory=$repo_dir
EnvironmentFile=/etc/swarmy/node.env
ExecStart=/usr/local/bin/swarmyd
Restart=always
RestartSec=5
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
if [[ $mode == stack ]]; then
    sudo systemctl enable --now swarmy-stack.service
else
    [[ -s .dev/fdb.cluster ]] || { echo 'Missing primary cluster file' >&2; exit 1; }
    sudo systemctl enable swarmy-tunnel.service
    sudo systemctl restart swarmy-tunnel.service
fi
sudo systemctl enable swarmyd.service
sudo systemctl restart swarmyd.service
invocation=$(sudo systemctl show -p InvocationID --value swarmyd)
for _ in $(seq 1 60); do
    if sudo test -S "$repo_dir/.swarmy/node/control.sock" && sudo journalctl "_SYSTEMD_INVOCATION_ID=$invocation" --no-pager | grep -q 'node registered and ready'; then
        sudo systemctl is-active swarmyd
        if [[ $mode == stack ]]; then sudo systemctl is-active swarmy-stack; fi
        echo "swarmyd registered and ready; $mode mode enabled at boot"
        exit 0
    fi
    sleep 2
done
sudo journalctl -u swarmyd -n 50 --no-pager
exit 1
