#!/usr/bin/env bash
# Run over SSH after the client uploaded the node directory. Never modifies node.env; brings installed units to the provisioning text.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-upgrade-lib.sh"
upgrade_args "$@" || exit 2
mode=$1
services=$2
drain_timeout=$3
source_mode=${4:-build}
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
# Builds run as the node directory owner; a bootstrap login re-enters as that user.
repo_owner=$(stat -c %U "$repo_dir")
if [[ $(id -un) != "$repo_owner" ]]; then
    exec sudo -H -u "$repo_owner" bash "$0" "$@"
fi
[[ $repo_dir == "$(service_repo_for "$repo_owner")" ]] || { echo "Expected the swarmy node directory at $(service_repo_for "$repo_owner")" >&2; exit 1; }
cd "$repo_dir"
started=$SECONDS
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
if [[ $source_mode == prebuilt ]]; then
    bin_dir=$(release_bundle_dir "$repo_dir")
    check_release_bundle "$bin_dir" "$mode"
    install_core_library "$bin_dir/libfdb_c.so"
else
    command -v cargo >/dev/null || { echo 'This node has no Rust toolchain (it was provisioned from release binaries); upgrade it with a release client or SWARMY_RELEASE_BASE_URL' >&2; exit 1; }
    source_dir=$(node_source_dir "$repo_dir")
    [[ -f $source_dir/Cargo.toml ]] || { echo "Missing $source_dir; a development client copies its checkout there" >&2; exit 1; }
    ( cd "$source_dir"
    # rsync preserves timestamps; a different commit can otherwise look older to cargo.
    find crates -type f \( -name '*.rs' -o -name 'build.rs' \) -exec touch {} +
    if [[ $mode == stack ]]; then
        SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked -p "$(swarmy_cli_package)" --no-default-features >&2
    fi
    read_shared_list build_args swarmy_mode_build_args "$mode"
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked "${build_args[@]}" >&2
    ) >&2
    bin_dir=$source_dir/target/release
fi
read_shared_list binaries swarmy_mode_binaries "$mode"
changed=()
restarted=()
for binary in "${binaries[@]}"; do
    if binary_changed "/usr/local/bin/$binary" "$bin_dir/$binary"; then
        sudo -n install -m 0755 "$bin_dir/$binary" "/usr/local/bin/$binary"
        changed+=("$binary")
    else
        result=$?
        (( result == 1 )) || exit "$result"
    fi
done
# Nodes provisioned before API token provisioning carry an empty token while
# serving the API. Fill it once; reruns keep the existing token.
api_unit=$(swarmy_service_unit api)
token_status=unchanged
if systemctl cat "$api_unit" >/dev/null 2>&1; then
    token_status=$(ensure_api_token .swarmy/config.toml)
fi
unit_inputs_for_upgrade "$mode" "$repo_owner"
systemd_dir=$(swarmy_systemd_dir)
backing_unit=$(swarmy_mode_backing_unit "$mode")
node_unit=$(swarmy_agent_unit)
read_shared_list service_units swarmy_service_units
reconcile=()
if [[ $services != services-only ]]; then reconcile+=("$backing_unit" "$node_unit"); fi
for unit in "${service_units[@]}"; do
    if [[ -f $systemd_dir/$unit ]]; then reconcile+=("$unit"); fi
done
changed_units=()
reconcile_units changed_units "${reconcile[@]}"
if unit_listed "$backing_unit" "${changed_units[@]}"; then
    echo "$backing_unit changed; upgrade never restarts backing services, so it takes effect at its next restart or reboot" >&2
fi
# Record text changes before restarting so a failed run retries them.
pending_file=.swarmy/upgrade-pending-units
restart_units=()
for unit in "${changed_units[@]}"; do
    [[ $unit == "$backing_unit" ]] || restart_units+=("$unit")
done
if [[ -f $pending_file ]]; then
    while IFS= read -r unit; do
        if [[ -n $unit ]] && ! unit_listed "$unit" "${restart_units[@]}" && unit_listed "$unit" "$node_unit" "${service_units[@]}"; then
            restart_units+=("$unit")
        fi
    done < "$pending_file"
fi
printf '%s\n' "${restart_units[@]}" > "$pending_file"
restarted_units=()
for unit in "${service_units[@]}"; do
    role=$(swarmy_unit_role "$unit")
    binary_path="/usr/local/bin/$(swarmy_core_binary)"
    if systemctl cat "$unit" >/dev/null 2>&1; then
        if unit_restart_wanted "$unit" "$binary_path" "${restart_units[@]}"; then
            sudo -n systemctl restart "$unit"
            restarted+=("$role")
            restarted_units+=("$unit")
        else
            result=$?
            (( result == 1 )) || exit "$result"
        fi
    fi
done
# Control nodes serve the API, which rejects every request while its token is
# empty. Fill a missing token for nodes provisioned before provisioning
# generated one; never rotate an existing token. A fresh token needs the API
# to reload its configuration even when its binary is unchanged.
if [[ $token_status == generated ]]; then
    already_restarted=false
    api_role=$(swarmy_unit_role "$api_unit")
    for entry in "${restarted[@]}"; do
        if [[ $entry == "$api_role" ]]; then already_restarted=true; fi
    done
    if [[ $already_restarted == false ]]; then
        sudo -n systemctl restart "$api_unit"
        restarted+=("$api_role")
        restarted_units+=("$api_unit")
    fi
fi
node_role=$(swarmy_unit_role "$node_unit")
node_binary_path="/usr/local/bin/$(swarmy_core_binary)"
if unit_restart_wanted "$node_unit" "$node_binary_path" "${restart_units[@]}"; then
    if [[ $services == services-only ]]; then
        echo "the $node_role role is out of date but --services-only skips its restart" >&2
    else
        deadline=$((SECONDS + drain_timeout))
        while true; do
            busy=$(sudo -n sh -c "cd $repo_dir && set -a && . /etc/swarmy/node.env && set +a && exec $bin_dir/$(swarmy_core_binary) $node_role --upgrade-processes")
            [[ $busy == '[]' ]] && break
            echo "Waiting for running sandbox commands: $busy" >&2
            if (( SECONDS >= deadline )); then
                echo "Drain timed out after $drain_timeout seconds; restarting $node_role anyway, interrupting: $busy" >&2
                break
            fi
            sleep 5
        done
        sudo -n systemctl restart "$node_unit"
        restarted+=("$node_role")
        restarted_units+=("$node_unit")
    fi
else
    result=$?
    (( result == 1 )) || exit "$result"
fi
remaining=()
for unit in "${restart_units[@]}"; do
    unit_listed "$unit" "${restarted_units[@]}" || remaining+=("$unit")
done
if (( ${#remaining[@]} )); then printf '%s\n' "${remaining[@]}" > "$pending_file"; else rm -f "$pending_file"; fi
remove_unreferenced_retired_binaries
python3 - "${changed[*]}" "${changed_units[*]}" "${restarted[*]}" "$((SECONDS - started))" <<'PYJSON'
import json, sys
print(json.dumps(dict(node='', changed=sys.argv[1].split(), units=sys.argv[2].split(), restarted=sys.argv[3].split(), elapsed_seconds=float(sys.argv[4]))))
PYJSON
