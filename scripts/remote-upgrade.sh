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
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked --no-default-features -p swarmy-cli >&2
    SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --release --locked \
        -p swarmyd -p swarmy-scheduler -p swarmy-gateway -p swarmy-worker -p swarmy-api >&2
    binaries=(swarmy swarmyd swarmy-scheduler swarmy-gateway swarmy-worker swarmy-api)
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
for service in scheduler worker gateway api; do
    binary="swarmy-$service"
    if systemctl cat "$binary.service" >/dev/null 2>&1; then
        if unit_needs_restart "$binary.service" "/usr/local/bin/$binary"; then
            sudo -n systemctl restart "$binary.service"
            restarted+=("$binary")
        else
            result=$?
            (( result == 1 )) || exit "$result"
        fi
    fi
done
if unit_needs_restart swarmyd.service /usr/local/bin/swarmyd; then
    if [[ $services == services-only ]]; then
        echo 'swarmyd is out of date but --services-only skips its restart' >&2
    else
        deadline=$((SECONDS + drain_timeout))
        while true; do
            busy=$(sudo -n sh -c 'cd /home/ubuntu/swarmy && set -a && . /etc/swarmy/node.env && set +a && exec /home/ubuntu/swarmy/target/release/swarmyd --upgrade-processes')
            [[ $busy == '[]' ]] && break
            echo "Waiting for running sandbox commands: $busy" >&2
            if (( SECONDS >= deadline )); then
                echo "Drain timed out after $drain_timeout seconds; restarting swarmyd anyway, interrupting: $busy" >&2
                break
            fi
            sleep 5
        done
        sudo -n systemctl restart swarmyd.service
        restarted+=(swarmyd)
    fi
else
    result=$?
    (( result == 1 )) || exit "$result"
fi
python3 - "${changed[*]}" "${restarted[*]}" "$((SECONDS - started))" <<'PY'
import json, sys
print(json.dumps(dict(node='', changed=sys.argv[1].split(), restarted=sys.argv[2].split(), elapsed_seconds=float(sys.argv[3]))))
PY
