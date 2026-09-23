#!/usr/bin/env bash
# Provision the Ubuntu checkout copied by swarmy remote up. Safe to rerun.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
[[ $repo_dir == /home/ubuntu/swarmy ]] || { echo 'Expected checkout at /home/ubuntu/swarmy' >&2; exit 1; }
cd "$repo_dir"
mode=${1:-stack}
service_address=${2:-127.0.0.1}
[[ $mode == stack || $mode == node ]] || { echo 'Expected stack or node mode' >&2; exit 1; }
[[ $service_address =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 1
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
sudo cloud-init status --wait
sudo apt-get update
sudo -E apt-get install -y build-essential pkg-config libssl-dev clang libclang-dev curl rsync \
    runc passt iproute2 util-linux e2fsprogs debootstrap
printf 'nbd\nublk_drv\n' | sudo tee /etc/modules-load.d/swarmy.conf >/dev/null
# Some Ubuntu AWS kernels include these modules in the base package.
if ! sudo modprobe nbd nbds_max=64 || ! sudo modprobe ublk_drv; then
    sudo -E apt-get install -y "linux-modules-extra-$(uname -r)"
    sudo modprobe nbd nbds_max=64
    sudo modprobe ublk_drv
fi

# Only instance-store NVMe is eligible for formatting. Never choose an EBS disk.
local_device=''
for device in /sys/block/nvme*n1; do
    [[ -r $device/device/model ]] || continue
    if [[ $(<"$device/device/model") == *'Amazon EC2 NVMe Instance Storage'* ]]; then
        local_device="/dev/${device##*/}"
        break
    fi
done
[[ -n $local_device ]] || { echo 'An instance with local NVMe storage is required (default: m6id.xlarge)' >&2; exit 1; }
local_mount=/mnt/swarmy-local
sudo mkdir -p "$local_mount"
if ! sudo blkid "$local_device" >/dev/null 2>&1; then
    # Refuse a disk with partitions or mounts even if it has no filesystem signature.
    [[ $(lsblk -nr -o NAME "$local_device" | wc -l) == 1 ]]
    [[ -z $(lsblk -nr -o MOUNTPOINTS "$local_device" | tr -d '[:space:]') ]]
    sudo mkfs.ext4 -L swarmy-local "$local_device"
fi
[[ $(sudo blkid -s LABEL -o value "$local_device") == swarmy-local ]] || { echo 'Refusing to reuse an unrecognized instance-store filesystem' >&2; exit 1; }
if ! mountpoint -q "$local_mount"; then
    sudo mount "$local_device" "$local_mount"
fi
[[ $(findmnt -n -o SOURCE --target "$local_mount") == "$local_device" ]]
if ! grep -q '^LABEL=swarmy-local ' /etc/fstab; then
    printf 'LABEL=swarmy-local /mnt/swarmy-local ext4 defaults,nofail 0 2\n' | sudo tee -a /etc/fstab >/dev/null
fi
sudo mkdir -p "$local_mount/volumes"
sudo chown ubuntu:ubuntu "$local_mount/volumes"
mkdir -p .swarmy
if [[ ! -e .swarmy/volumes && ! -L .swarmy/volumes ]]; then
    ln -s "$local_mount/volumes" .swarmy/volumes
fi
[[ $(readlink -f .swarmy/volumes) == "$local_mount/volumes" ]]
# Keep node identity, backing data, and configuration on the EBS root disk.
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
build_started=$SECONDS
if [[ $mode == stack ]]; then
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked \
        -p swarmy-cli -p swarmyd -p swarmy-scheduler -p swarmy-gateway -p swarmy-worker
    sudo install -m 0755 target/release/{swarmy,swarmy-session,swarmyd,swarmy-scheduler,swarmy-gateway,swarmy-worker} /usr/local/bin/
else
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked -p swarmyd
    sudo install -m 0755 target/release/swarmyd /usr/local/bin/
fi
printf 'Release build took %s seconds\n' "$((SECONDS - build_started))"
sudo install -d -m 0755 /etc/swarmy
sudo install -m 0600 /dev/null /etc/swarmy/node.env
sudo tee /etc/swarmy/node.env >/dev/null <<ENV
SWARMY_FDB_CLUSTER_FILE=$repo_dir/.dev/fdb.cluster
SWARMY_NATS_URL=nats://127.0.0.1:4222
SWARMY_S3_ENDPOINT=http://127.0.0.1:8333
SWARMY_S3_ACCESS_KEY=swarmy-dev
SWARMY_S3_SECRET_KEY=swarmy-dev-secret
SWARMY_S3_BUCKET=swarmy
SWARMY_S3_REGION=us-east-1
SWARMY_NODE_CPU_MILLIS=$(($(nproc) * 1000))
SWARMY_NODE_MEMORY_BYTES=$(awk '/MemTotal/ {printf "%.0f", $2 * 1024}' /proc/meminfo)
SWARMY_NODE_DISK_BYTES=$(df -B1 --output=size "$local_mount" | tail -1 | tr -d ' ')
SWARMY_NODE_SANDBOXES=4
# Long-lived workers rewrite build caches constantly; keep few snapshots and
# reclaim unreferenced chunks quickly so the node's object store stays small.
SWARMY_VOLUME_SNAPSHOT_RETENTION=3
SWARMY_GC_GRACE_SECONDS=1800
SWARMY_GC_INTERVAL_SECONDS=600
LD_LIBRARY_PATH=/home/ubuntu/.local/lib
ENV
if [[ $mode == stack ]]; then
sudo tee /etc/systemd/system/swarmy-stack.service >/dev/null <<UNIT
[Unit]
Description=Swarmy backing services (FoundationDB, NATS, SeaweedFS)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=ubuntu
WorkingDirectory=$repo_dir
Environment=HOME=/home/ubuntu
Environment=PATH=/home/ubuntu/.local/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin
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
sudo install -o ubuntu -g ubuntu -m 0600 .swarmy/tunnel-key /etc/swarmy/tunnel-key
sudo install -o ubuntu -g ubuntu -m 0600 .swarmy/tunnel-known-hosts /etc/swarmy/tunnel-known-hosts
sudo tee /etc/systemd/system/swarmy-tunnel.service >/dev/null <<UNIT
[Unit]
Description=Swarmy tunnel to first node backing services
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=ubuntu
ExecStart=/usr/bin/ssh -N -T -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/etc/swarmy/tunnel-known-hosts -o ExitOnForwardFailure=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -i /etc/swarmy/tunnel-key -L 127.0.0.1:4500:127.0.0.1:4500 -L 127.0.0.1:4222:127.0.0.1:4222 -L 127.0.0.1:8333:127.0.0.1:8333 ubuntu@$service_address
ExecStartPost=/bin/bash -c 'for attempt in {1..30}; do if (echo > /dev/tcp/127.0.0.1/4500) 2>/dev/null; then exit 0; fi; sleep 1; done; exit 1'
Restart=always
RestartSec=5
TimeoutStartSec=40

[Install]
WantedBy=multi-user.target
UNIT
fi
sudo tee /etc/systemd/system/swarmyd.service >/dev/null <<UNIT
[Unit]
Description=Swarmy node agent
$dependency_kind=$stack_dependency
After=network-online.target $stack_dependency systemd-modules-load.service
Wants=network-online.target
RequiresMountsFor=$local_mount

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
