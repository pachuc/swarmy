#!/usr/bin/env bash
# Provision the node directory the client uploaded (scripts/ and images/, plus .swarmy/release/ from a release client or source/ from a development client). Safe to rerun.
# Arguments: MODE SERVICE_ADDRESS [BUCKET] [BUCKET_REGION] [BUCKET_ENDPOINT]
#   [BUCKET_PREFIX] [CONDITIONAL_CREATE] [STATIC] [SANDBOXES] [SERVICE_USER]
#   [LOCAL_STORAGE] [PREBUILT]. SERVICE_USER owns the node directory and the
#   units (default swarmy); LOCAL_STORAGE is a block device to format and
#   mount at /mnt/swarmy-local or dir:/path for an existing directory.
#   Sandbox nodes require it; control-only nodes use the root disk. PREBUILT
#   true installs the uploaded bundle from <node dir>/.swarmy/release; false
#   builds on the node from <node dir>/source.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
parse_provision_args "$@"
validate_service_user "$service_user"
# parse_provision_args assigns sandboxes through the sourced helper.
# shellcheck disable=SC2154
check_node_memory "$sandboxes"
ensure_service_user "$service_user"
# Privileged setup runs as any sudoer, but the build and the units belong to
# the service user; re-enter as that user so files land owned correctly.
if [[ $(id -un) != "$service_user" ]]; then
    exec sudo -H -u "$service_user" bash "$0" "$@"
fi
service_home=$(service_home_for "$service_user")
service_repo=$(service_repo_for "$service_user")
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
[[ $repo_dir == "$service_repo" ]] || { echo "Expected the swarmy node directory at $service_repo" >&2; exit 1; }
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
if [[ -n $bucket_endpoint ]] && [[ -z $bucket ]]; then
    echo 'A bucket endpoint without a bucket is not a remote object store.' >&2
    exit 1
fi
[[ $bucket_conditional_create == true || $bucket_conditional_create == false ]] || { echo 'Expected bucket conditional_create true or false' >&2; exit 1; }
[[ $prebuilt == true || $prebuilt == false ]] || { echo 'Expected prebuilt true or false' >&2; exit 1; }
[[ $bucket_static == true || $bucket_static == false ]] || { echo 'Expected bucket static true or false' >&2; exit 1; }
[[ $mode == stack || $mode == node ]] || { echo 'Expected stack or node mode' >&2; exit 1; }
[[ $service_address =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 1
storage=$(parse_local_storage "$local_storage")
# Units this mode owns from the shared table, so installs and teardown can
# never drift apart. Anything else in the shared list is a leftover from a
# previous installation on this host.
stack_dependency=$(swarmy_mode_backing_unit "$mode")
node_unit=$(swarmy_agent_unit)
# A re-provisioned host (adopted or rebuilt) may carry the other mode's
# units or old node-services units; stop and disable them before installing.
# What this mode owns is enabled again below.
disable_previous_units
export DEBIAN_FRONTEND=noninteractive
# Dedicated servers boot stock Ubuntu without cloud-init; only cloud images wait.
wait_for_cloud_init
sudo apt-get update
packages=(curl rsync runc passt iproute2 util-linux e2fsprogs debootstrap)
if [[ $prebuilt == false ]]; then packages+=(build-essential pkg-config libssl-dev clang libclang-dev); fi
sudo -E apt-get install -y "${packages[@]}"
printf 'nbd\nublk_drv\n' | install_owned_file "$(swarmy_modules_file)"
# Some Ubuntu kernels ship these drivers outside the base package.
if ! sudo modprobe nbd nbds_max=64 || ! sudo modprobe ublk_drv; then
    sudo -E apt-get install -y "linux-modules-extra-$(uname -r)"
    sudo modprobe nbd nbds_max=64
    sudo modprobe ublk_drv
fi

# Prepare the driver, toolkit and CDI spec only on machines with NVIDIA cards.
if nvidia_card_present; then
    provision_nvidia
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

if [[ $prebuilt == false ]]; then
    if ! command -v rustup >/dev/null && [[ ! -x $HOME/.cargo/bin/rustup ]]; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/swarmy-rustup.sh
        sh /tmp/swarmy-rustup.sh -y --profile minimal --default-toolchain none
    fi
    export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
    source_dir=$(node_source_dir "$repo_dir")
    [[ -f $source_dir/Cargo.toml ]] || { echo "Missing $source_dir; a development client copies its checkout there" >&2; exit 1; }
    ( cd "$source_dir" && rustup show )
fi
# A version marker avoids downloading the backing tools again on a rerun.
tools_hash=$(sha256sum scripts/install-dev-tools.sh | cut -d' ' -f1)
if [[ ! -f .swarmy/remote-tools-version || $(<.swarmy/remote-tools-version) != "$tools_hash" ]]; then
    bash scripts/install-dev-tools.sh
    printf '%s\n' "$tools_hash" > .swarmy/remote-tools-version
fi
if [[ $prebuilt == false ]]; then
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
    # Packages from the shared table (the CLI package differs, so it builds
    # separately with its own feature flags). read_shared_list fails loudly:
    # an empty package list would become a whole-workspace build.
    ( cd "$source_dir"
    SWARMY_GIT_COMMIT=$(source_git_commit .)
    export SWARMY_GIT_COMMIT
    if [[ $mode == stack ]]; then
        SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked -p "$(swarmy_cli_package)" --no-default-features
    fi
    read_shared_list build_args swarmy_mode_build_args "$mode"
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked "${build_args[@]}"
    )
    bin_dir=$source_dir/target/release
else
    bin_dir=$(release_bundle_dir "$repo_dir")
    check_release_bundle "$bin_dir" "$mode"
    install_core_library "$bin_dir/libfdb_c.so"
fi
# Install from the shared binary list so the install can never name a binary
# teardown misses. Only built binaries are present, selected per mode above.
read_shared_list install_binaries swarmy_mode_binaries "$mode"
sudo install -m 0755 "${install_binaries[@]/#/$bin_dir/}" /usr/local/bin/
remove_retired_binaries
if [[ $prebuilt == true ]]; then
    printf 'Installed release binaries from %s\n' "$bin_dir"
else
    printf 'Release build took %s seconds\n' "$((SECONDS - build_started))"
fi
sudo install -d -m 0755 /etc/swarmy
sudo install -m 0600 /dev/null /etc/swarmy/node.env
node_environment "$repo_dir" "$sandboxes" "$local_mount" "$bucket" "$bucket_region" "$bucket_endpoint" "$bucket_prefix" "$bucket_conditional_create" "$bucket_static" | sudo tee /etc/swarmy/node.env >/dev/null
# Static S3 keys arrive over SSH stdin in a root-owned staging file the
# provisioning user cannot read. Append it verbatim as root in one step and
# delete it: sourcing the file would expand `$`, backticks and spaces in key
# material. Without static keys, delete any staging file left from an earlier
# static provisioning instead of merging stale keys.
if [[ $bucket_static == true ]]; then
    merge_static_s3_keys /etc/swarmy/s3-keys.env /etc/swarmy/node.env
else
    sudo rm -f /etc/swarmy/s3-keys.env
fi
tunnel_primary=''
tunnel_s3=false
if [[ $mode == node ]]; then
    [[ -s .swarmy/tunnel-key && -s .swarmy/tunnel-known-hosts ]] || { echo 'Missing add-node tunnel identity' >&2; exit 1; }
    sudo install -o "$service_user" -g "$service_user" -m 0600 .swarmy/tunnel-key /etc/swarmy/tunnel-key
    sudo install -o "$service_user" -g "$service_user" -m 0600 .swarmy/tunnel-known-hosts /etc/swarmy/tunnel-known-hosts
    tunnel_primary=$service_address
    if [[ -z $bucket ]]; then tunnel_s3=true; fi
fi
unit_mount=''
if [[ $storage_is_mount == true ]]; then unit_mount=$local_mount; fi
set_unit_inputs "$mode" "$service_user" "$tunnel_primary" "$tunnel_s3" "$unit_mount"
record_unit_inputs
provisioned_units=()
write_units provisioned_units "$stack_dependency" "$node_unit"
sudo systemctl daemon-reload
if [[ $mode == stack ]]; then
    sudo systemctl enable --now "$stack_dependency"
else
    [[ -s .dev/fdb.cluster ]] || { echo 'Missing primary cluster file' >&2; exit 1; }
    sudo systemctl enable "$stack_dependency"
    # A new machine's first tunnel connection can fail while its private
    # network or the primary's backing services are still coming up.
    for attempt in 1 2 3 4 5 6; do
        if sudo systemctl restart "$stack_dependency"; then break; fi
        if ((attempt == 6)); then
            sudo journalctl -u "$stack_dependency" --no-pager -n 20 >&2 || :
            exit 1
        fi
        echo "the tunnel to the primary is not up yet; retrying ($attempt/6)" >&2
        sleep 10
    done
fi
sudo systemctl enable "$node_unit"
sudo systemctl restart "$node_unit"
invocation=$(sudo systemctl show -p InvocationID --value "$node_unit")
for _ in $(seq 1 60); do
    if sudo test -S "$repo_dir/.swarmy/node/control.sock" && sudo journalctl "_SYSTEMD_INVOCATION_ID=$invocation" --no-pager | grep -q 'node registered and ready'; then
        sudo systemctl is-active "$node_unit"
        if [[ $mode == stack ]]; then sudo systemctl is-active "$stack_dependency"; fi
        echo "node role (swarmy-core node) registered and ready; $mode mode enabled at boot"
        exit 0
    fi
    sleep 2
done
sudo journalctl -u "$node_unit" -n 50 --no-pager
exit 1
