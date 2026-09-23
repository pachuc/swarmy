#!/usr/bin/env bash
# Run on the disposable node created by swarmy remote up.
set -euo pipefail
cd /home/ubuntu/swarmy
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
source .dev/env
export LD_LIBRARY_PATH="$HOME/.local/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export SWARMY_TEST_IMAGE="base-ubuntu:${1:?remote name required}"

failures=()
run_suite() {
    local name=$1
    shift
    echo "::group::$name"
    if "$@"; then
        echo "PASS $name"
    else
        echo "FAIL $name" >&2
        failures+=("$name")
    fi
    echo '::endgroup::'
}

test_binary() {
    local package=$1 target=$2
    cargo test --locked -p "$package" --test "$target" --no-run --message-format=json |
        python3 -c 'import json,sys; matches=[item["executable"] for line in sys.stdin if (item:=json.loads(line)).get("reason")=="compiler-artifact" and item.get("target",{}).get("kind")==["test"] and item.get("executable")]; assert len(matches)==1, matches; print(matches[0])'
}

run_root_test() {
    local binary
    binary=$(test_binary "$1" "$2") || return
    sudo env SWARMY_TEST_IMAGE="$SWARMY_TEST_IMAGE" bash -c '
        set -a
        source /etc/swarmy/node.env
        set +a
        exec "$@"
    ' bash "$binary" --test-threads=1 --nocapture
}

# Prebuild the service binaries needed by chaos and the test executables as ubuntu.
run_suite root-tools sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y fio skopeo umoci
run_suite build cargo test --workspace --locked --no-run
if [[ ${#failures[@]} == 0 ]]; then
    run_suite nbd run_root_test swarmy-volume nbd
    run_suite volume-image run_root_test swarmy-volume image
    run_suite cli-image run_root_test swarmy-cli image
    run_suite cli-volume run_root_test swarmy-cli vol
    run_suite container-node run_root_test swarmyd node
    run_suite cli-session run_root_test swarmy-cli session
    run_suite chaos-bash run_root_test swarmy-chaos bash
    run_suite chaos-continuity run_root_test swarmy-chaos continuity
    run_suite chaos-coding run_root_test swarmy-chaos coding
    run_suite reduced-chaos scripts/chaos-ci.sh
    for scenario in persistent continuity coding kill-node-mid-command; do
        options=(--no-start-stack --bin-dir target/debug --image "$SWARMY_TEST_IMAGE" --session-timeout-secs 240)
        case "$scenario" in
            persistent) options+=(--persistent --sessions 2 --gateways 1 --kills 0) ;;
            continuity) options+=(--continuity --sessions 1 --schedulers 1 --workers 1 --gateways 1 --kills 0) ;;
            coding) options+=(--coding --sessions 1 --schedulers 1 --workers 1 --gateways 1 --kills 0) ;;
            kill-node-mid-command) options+=(--kill-node-mid-command --sessions 1 --steps 3 --kills 0) ;;
        esac
        run_suite "$scenario" sudo env SWARMY_TEST_IMAGE="$SWARMY_TEST_IMAGE" bash -c '
            set -a
            source /etc/swarmy/node.env
            set +a
            exec "$@"
        ' bash target/debug/swarmy-chaos "${options[@]}"
    done
fi
printf 'NIGHTLY_FAILED_SUITES=%s\n' "$(IFS=,; echo "${failures[*]}")"
[[ ${#failures[@]} == 0 ]]
