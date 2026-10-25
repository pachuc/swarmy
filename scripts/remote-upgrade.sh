#!/usr/bin/env bash
# Run over SSH after the checkout has been copied. Never modify node.env or units.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-upgrade-lib.sh"
upgrade_args "$@" || exit 2
mode=$1
services=$2
drain_timeout=$3
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
# Builds run as the checkout owner; a bootstrap login re-enters as that user.
repo_owner=$(stat -c %U "$repo_dir")
if [[ $(id -un) != "$repo_owner" ]]; then
    exec sudo -H -u "$repo_owner" bash "$0" "$@"
fi
[[ $repo_dir == "$(service_repo_for "$repo_owner")" ]] || { echo "Expected checkout at $(service_repo_for "$repo_owner")" >&2; exit 1; }
cd "$repo_dir"
started=$SECONDS
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
# rsync preserves timestamps; a different commit can otherwise look older to cargo.
find crates -type f \( -name '*.rs' -o -name 'build.rs' \) -exec touch {} +
# Binaries and their packages come from the shared table (the CLI package
# differs, so it builds separately with its own feature flags).
# read_shared_list fails loudly: an empty list would become a
# whole-workspace build or a no-op install loop.
if [[ $mode == stack ]]; then
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked -p "$(swarmy_cli_package)" --no-default-features >&2
fi
read_shared_list build_args swarmy_mode_build_args "$mode"
SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked "${build_args[@]}" >&2
read_shared_list binaries swarmy_mode_binaries "$mode"
changed=()
restarted=()
for binary in "${binaries[@]}"; do
    if binary_changed "/usr/local/bin/$binary" "target/release/$binary"; then
        sudo -n install -m 0755 "target/release/$binary" "/usr/local/bin/$binary"
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
read_shared_list service_units swarmy_service_units
for unit in "${service_units[@]}"; do
    role=$(swarmy_unit_role "$unit")
    binary_path="/usr/local/bin/$(swarmy_core_binary)"
    if systemctl cat "$unit" >/dev/null 2>&1; then
        if unit_needs_restart "$unit" "$binary_path"; then
            sudo -n systemctl restart "$unit"
            restarted+=("$role")
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
    fi
fi
node_unit=$(swarmy_agent_unit)
node_role=$(swarmy_unit_role "$node_unit")
node_binary_path="/usr/local/bin/$(swarmy_core_binary)"
if unit_needs_restart "$node_unit" "$node_binary_path"; then
    if [[ $services == services-only ]]; then
        echo "the $node_role role is out of date but --services-only skips its restart" >&2
    else
        deadline=$((SECONDS + drain_timeout))
        while true; do
            busy=$(sudo -n sh -c "cd $repo_dir && set -a && . /etc/swarmy/node.env && set +a && exec $repo_dir/target/release/$(swarmy_core_binary) $node_role --upgrade-processes")
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
    fi
else
    result=$?
    (( result == 1 )) || exit "$result"
fi
python3 - "${changed[*]}" "${restarted[*]}" "$((SECONDS - started))" <<'PY'
import json, sys
print(json.dumps(dict(node='', changed=sys.argv[1].split(), restarted=sys.argv[2].split(), elapsed_seconds=float(sys.argv[3]))))
PY
