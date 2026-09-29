#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
[[ $(parse_sandbox_count 0) == 0 ]]
[[ $(parse_sandbox_count 64) == 64 ]]
for invalid in -1 word 1.5 4294967296; do
    if parse_sandbox_count "$invalid" >/dev/null 2>&1; then
        echo "accepted invalid count: $invalid" >&2
        exit 1
    fi
done
zero=$(node_environment /tmp/checkout 0 /this-mount-does-not-exist)
[[ $zero == *'SWARMY_NODE_ROLES=volume'* ]]
[[ $zero == *'SWARMY_NODE_SANDBOXES=0'* ]]
[[ $zero == *'SWARMY_NODE_DISK_BYTES=0'* ]]
[[ $zero =~ SWARMY_NODE_MEMORY_BYTES=[0-9]+ ]]
positive=$(node_environment /tmp/checkout 4 /tmp)
[[ $positive == *'SWARMY_NODE_ROLES=sandbox,volume'* ]]
[[ $positive == *'SWARMY_NODE_SANDBOXES=4'* ]]
[[ $positive == *'SWARMY_NODE_DISK_BYTES='* ]]
[[ $positive != *'SWARMY_NODE_DISK_BYTES=0'* ]]
zero_bucket=$(node_environment /tmp/checkout 0 /this-mount-does-not-exist example-bucket eu-west-1)
[[ $zero_bucket == *'SWARMY_NODE_ROLES=volume'* ]]
[[ $zero_bucket == *'SWARMY_NODE_SANDBOXES=0'* ]]
[[ $zero_bucket == *'SWARMY_NODE_DISK_BYTES=0'* ]]
[[ $zero_bucket == *$'SWARMY_S3_ENDPOINT=\n'* ]]
[[ $zero_bucket == *'SWARMY_S3_BUCKET=example-bucket'* ]]
[[ $zero_bucket == *'SWARMY_S3_REGION=eu-west-1'* ]]
[[ $zero_bucket == *'SWARMY_DEV_SKIP_S3=1'* ]]
positive_bucket=$(node_environment /tmp/checkout 4 /tmp example-bucket eu-west-1)
[[ $positive_bucket == *'SWARMY_NODE_ROLES=sandbox,volume'* ]]
[[ $positive_bucket == *'SWARMY_NODE_SANDBOXES=4'* ]]
[[ $positive_bucket == *'SWARMY_S3_BUCKET=example-bucket'* ]]
# The service library derives from the checkout, never a hardcoded login.
[[ $positive == *'LD_LIBRARY_PATH=/tmp/.local/lib'* ]]
[[ $positive != *ubuntu* ]]
[[ $zero != *ubuntu* ]]

# Service user validation and path derivation.
for user in swarmy ubuntu root deploy-1 ci_bot A9; do
    validate_service_user "$user"
done
long=$(printf 'a%.0s' {1..65})
for invalid in '' 'has space' 'semi;colon' '$(injected)' 'dq"quote' 'sq'"'"'quote' 'back\slash' "$long"; do
    if validate_service_user "$invalid" >/dev/null 2>&1; then
        echo "accepted invalid service user: $invalid" >&2
        exit 1
    fi
done
[[ $(service_home_for root) == /root ]]
[[ $(service_repo_for root) == /root/swarmy ]]
# No passwd entry for this login here, so the conventional home applies.
[[ $(service_home_for swarmy) == /home/swarmy ]]
[[ $(service_repo_for swarmy) == /home/swarmy/swarmy ]]
# A custom passwd entry wins over the convention.
getent() { printf 'swarmy:x:1001:1001::/srv/custom:/bin/bash\n'; }
[[ $(service_home_for swarmy) == /srv/custom ]]
[[ $(service_repo_for swarmy) == /srv/custom/swarmy ]]
unset -f getent

# cloud-init exists on cloud images only; stock servers skip the wait.
if command -v cloud-init >/dev/null 2>&1; then had_cloud_init=true; else had_cloud_init=false; fi
if $had_cloud_init; then needs_cloud_init_wait; else ! needs_cloud_init_wait; fi
cloud-init() { echo 'stub cloud-init present'; }
needs_cloud_init_wait
unset -f cloud-init
if $had_cloud_init; then needs_cloud_init_wait; else ! needs_cloud_init_wait; fi

# Local storage setting classification.
[[ $(parse_local_storage '') == auto ]]
[[ $(parse_local_storage /dev/nvme1n1) == 'device /dev/nvme1n1' ]]
[[ $(parse_local_storage device:/dev/md0) == 'device /dev/md0' ]]
[[ $(parse_local_storage dir:/srv/swarmy-local) == 'dir /srv/swarmy-local' ]]
[[ $(parse_local_storage /srv/swarmy-local) == 'dir /srv/swarmy-local' ]]
[[ $(parse_local_storage 'dir:/srv/with space') == 'dir /srv/with space' ]]
for invalid in relative dir: device: dir:relative; do
    if parse_local_storage "$invalid" >/dev/null 2>&1; then
        echo "accepted invalid local storage: $invalid" >&2
        exit 1
    fi
done

# Unused-disk discovery against stubbed block tools and a fixture /sys/block.
sys_block=$(mktemp -d)
trap 'rm -rf "$sys_block"' EXIT
for disk in nvme0n1:200000 nvme1n1:100000 nvme2n1:300000 sda:500000 nvme3n1:400000 nvme4n1:600000 loop0:700000; do
    mkdir -p "$sys_block/${disk%%:*}"
    printf '%s\n' "${disk##*:}" > "$sys_block/${disk%%:*}/size"
done
findmnt() { printf '/dev/nvme0n1p1\n'; }
lsblk() {
    local device=${@: -1}
    case "$*" in
        *PKNAME*)
            if [[ $device == /dev/nvme0n1p1 ]]; then printf 'nvme0n1\n'; fi
            ;;
        *'NAME'*)
            if [[ $device == /dev/sda ]]; then printf 'sda\nsda1\n'; else printf '%s\n' "${device#/dev/}"; fi
            ;;
        *MOUNTPOINTS*)
            if [[ $device == /dev/nvme4n1 ]]; then printf '[/mnt/data]\n'; fi
            ;;
    esac
}
blkid() { [[ ${@: -1} == /dev/nvme3n1 ]]; }
export SWARMY_SYS_BLOCK="$sys_block"
# Largest usable disk wins: sda is partitioned, nvme3n1 has a filesystem,
# nvme4n1 is mounted, nvme0n1 backs root, loop devices never qualify.
[[ $(discover_unused_disk) == /dev/nvme2n1 ]]
# Nothing usable left: every candidate is excluded.
lsblk() {
    local device=${@: -1}
    case "$*" in
        *PKNAME*)
            if [[ $device == /dev/nvme0n1p1 ]]; then printf 'nvme0n1\n'; fi
            ;;
        *'NAME'*) printf '%s\n%s1\n' "${device#/dev/}" "${device#/dev/}" ;;
    esac
}
if discover_unused_disk >/dev/null 2>&1; then
    echo 'discovered a disk among only partitioned devices' >&2
    exit 1
fi
echo 'remote provision argument and environment tests passed'
