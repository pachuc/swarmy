#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_dir=$(mktemp -d)
dummy_pid=

cleanup() {
    local result=$?
    if (( result != 0 )); then
        for name in occupied default override; do
            if [[ -f $test_dir/$name/.dev/logs/fdb.log ]]; then
                printf '%s FoundationDB log:\n' "$name" >&2
                tail -20 "$test_dir/$name/.dev/logs/fdb.log" >&2
            fi
        done
    fi
    "$test_dir/occupied/scripts/dev-stack.sh" stop >/dev/null 2>&1 || true
    "$test_dir/default/scripts/dev-stack.sh" stop >/dev/null 2>&1 || true
    "$test_dir/override/scripts/dev-stack.sh" stop >/dev/null 2>&1 || true
    if [[ -n $dummy_pid ]]; then
        kill "$dummy_pid" 2>/dev/null || true
        wait "$dummy_pid" 2>/dev/null || true
    fi
    rm -rf -- "$test_dir"
}
trap cleanup EXIT

for name in occupied default override; do
    mkdir -p -- "$test_dir/$name/scripts"
    cp -- "$repo_dir/scripts/dev-stack.sh" "$test_dir/$name/scripts/dev-stack.sh"
done

# The embedded copy needs an explicit absolute stack directory and never creates
# one for status or stop.
out=$(SWARMY_DEV_DIR="$test_dir/embedded" bash -c "$(cat "$repo_dir/scripts/dev-stack.sh")" 'swarmy dev stack' status)
[[ $out == *'fdb: down'* && $out == *'nats: down'* && $out == *'seaweed: down'* ]]
[[ ! -e $test_dir/embedded ]]
SWARMY_DEV_DIR="$test_dir/embedded" bash -c "$(cat "$repo_dir/scripts/dev-stack.sh")" x stop
[[ ! -e $test_dir/embedded/lock ]]
if bash -c "$(cat "$repo_dir/scripts/dev-stack.sh")" x status 2>"$test_dir/error"; then exit 1; fi
grep -q SWARMY_DEV_DIR "$test_dir/error"
if SWARMY_DEV_DIR=relative bash -c "$(cat "$repo_dir/scripts/dev-stack.sh")" x status 2>"$test_dir/error"; then exit 1; fi
grep -q 'absolute path' "$test_dir/error"

# Binding, not a preceding TCP connect, decides whether this test owns 4500.
# A just-stopped server may still hold the port after a connect says "free".
start_dummy() {
    rm -f -- "$test_dir/dummy-ready"
    python3 -c 'import errno, socket, sys, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind(("127.0.0.1", 4500))
except OSError as error:
    if error.errno == errno.EADDRINUSE:
        sys.exit(98)
    raise
s.listen()
open(sys.argv[1], "w").close()
time.sleep(600)' "$test_dir/dummy-ready" &
    dummy_pid=$!
    for ((attempt = 0; attempt < 50; attempt++)); do
        if [[ -f $test_dir/dummy-ready ]]; then return 0; fi
        if ! kill -0 "$dummy_pid" 2>/dev/null; then
            if wait "$dummy_pid"; then
                printf 'Placeholder exited before binding port 4500.\n' >&2
                return 2
            else
                result=$?
                dummy_pid=
                if (( result == 98 )); then return 1; fi
                return 2
            fi
        fi
        sleep 0.1
    done
    printf 'Placeholder did not bind port 4500 within 5 seconds.\n' >&2
    return 2
}

port_was_free=false
if start_dummy; then
    port_was_free=true
else
    result=$?
    (( result == 1 )) || exit "$result"
fi

occupied_stack="$test_dir/occupied/scripts/dev-stack.sh"
"$occupied_stack" start
cluster_file="$test_dir/occupied/.dev/fdb.cluster"
[[ $(cat "$cluster_file") =~ ^dev:dev@127\.0\.0\.1:([0-9]+)$ ]]
chosen_port=${BASH_REMATCH[1]}
[[ $chosen_port != 4500 ]]
[[ $(cat "$test_dir/occupied/.dev/env") == *"export SWARMY_DEV_FDB_PORT=$chosen_port"* ]]
status_output=$("$occupied_stack" status)
[[ $status_output =~ fdb:\ up\ \(pid\ [0-9]+,\ port\ $chosen_port\) ]]
fdbcli -C "$cluster_file" --timeout 5 --exec status >/dev/null
read -r fdb_pid _ < "$test_dir/occupied/.dev/fdb.pid"
"$occupied_stack" stop
! kill -0 "$fdb_pid" 2>/dev/null
status_output=$("$occupied_stack" status)
[[ $status_output == *"fdb: down (port $chosen_port)"* ]]

if [[ $port_was_free == true ]]; then
    kill "$dummy_pid"
    wait "$dummy_pid" 2>/dev/null || true
    dummy_pid=
    rm -- "$test_dir/dummy-ready"
    default_stack="$test_dir/default/scripts/dev-stack.sh"
    "$default_stack" start
    [[ $(cat "$test_dir/default/.dev/fdb.cluster") == 'dev:dev@127.0.0.1:4500' ]]
    "$default_stack" stop
    # stop waits for the old listener to release its port.
    start_dummy || exit 1
    "$default_stack" start
    [[ $(cat "$test_dir/default/.dev/fdb.cluster") != 'dev:dev@127.0.0.1:4500' ]]
    fdbcli -C "$test_dir/default/.dev/fdb.cluster" --timeout 5 --exec status >/dev/null
    "$default_stack" stop
    kill "$dummy_pid"
    wait "$dummy_pid" 2>/dev/null || true
    dummy_pid=
else
    printf 'Skipped default-port check: port 4500 was occupied before the test.\n'
fi

override_stack="$test_dir/override/scripts/dev-stack.sh"
override_port=4600
while (exec 3<>"/dev/tcp/127.0.0.1/$override_port") 2>/dev/null; do
    ((override_port += 1))
done
SWARMY_DEV_FDB_PORT=$override_port "$override_stack" start
[[ $(cat "$test_dir/override/.dev/fdb.cluster") == "dev:dev@127.0.0.1:$override_port" ]]
"$override_stack" stop

printf 'dev-stack port checks passed.\n'
