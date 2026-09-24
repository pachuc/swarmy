#!/usr/bin/env bash
# Run over SSH after the checkout has been copied. Never modify node.env or units.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-upgrade-lib.sh"
upgrade_args "$@" || exit 2
mode=$1
services=$2
drain_timeout=$3
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
[[ $repo_dir == /home/ubuntu/swarmy ]] || { echo 'Expected checkout at /home/ubuntu/swarmy' >&2; exit 1; }
cd "$repo_dir"
started=$SECONDS
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
# rsync preserves timestamps; a different commit can otherwise look older to cargo.
find crates -type f \( -name '*.rs' -o -name 'build.rs' \) -exec touch {} +
if [[ $mode == stack ]]; then
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked \
        -p swarmy-cli -p swarmyd -p swarmy-scheduler -p swarmy-gateway -p swarmy-worker -p swarmy-api >&2
    binaries=(swarmy swarmy-session swarmyd swarmy-scheduler swarmy-gateway swarmy-worker swarmy-api)
else
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked -p swarmyd >&2
    binaries=(swarmyd)
fi
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
pending="$repo_dir/.swarmy/upgrade-swarmyd-pending"
if [[ " ${changed[*]} " == *' swarmyd '* ]]; then
    mkdir -p "$repo_dir/.swarmy"
    touch "$pending"
fi
if [[ $mode == stack ]]; then
    for service in scheduler worker gateway api; do
        binary="swarmy-$service"
        if [[ " ${changed[*]} " == *" $binary "* ]] && systemctl cat "$binary.service" >/dev/null 2>&1; then
            sudo -n systemctl restart "$binary.service"
            restarted+=("$binary")
        fi
    done
fi
if [[ -f $pending && $services == all ]]; then
    deadline=$((SECONDS + drain_timeout))
    while true; do
        busy=$(sudo -n sh -c 'cd /home/ubuntu/swarmy && set -a && . /etc/swarmy/node.env && set +a && exec /home/ubuntu/swarmy/target/release/swarmyd --upgrade-processes')
        [[ $busy == '[]' ]] && break
        echo "Waiting for running sandbox commands: $busy" >&2
        (( SECONDS < deadline )) || { echo "Drain timed out after $drain_timeout seconds; swarmyd was not restarted" >&2; exit 1; }
        sleep 5
    done
    sudo -n systemctl restart swarmyd.service
    rm -f "$pending"
    restarted+=(swarmyd)
fi
python3 - "${changed[*]}" "${restarted[*]}" "$((SECONDS - started))" <<'PY'
import json, sys
print(json.dumps(dict(node='', changed=sys.argv[1].split(), restarted=sys.argv[2].split(), elapsed_seconds=float(sys.argv[3]))))
PY
