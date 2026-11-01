#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/test-fake-host.sh
work=$(mktemp -d)
trap 'if [[ ${root_repo_link:-} ]]; then rm -f "$root_repo_link"; fi; rm -rf "$work"' EXIT
make_fake_host "$work/host" remote-services.sh remote-provision-env.sh remote-s3-env.sh
scenario=$work/host
state=$node_root/state
mkdir -p "$state/active" "$state/invocation"
cat > "$scenario/stubs/systemctl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$TEST_NODE_ROOT/systemctl.log"
state=$TEST_NODE_ROOT/state
case $1 in
    start|restart)
        unit=$2
        if [[ ! -f $SWARMY_SYSTEMD_DIR/$unit ]]; then echo "Unit $unit not found." >&2; exit 5; fi
        touch "$state/active/$unit"
        number=0
        [[ ! -f $state/counter ]] || number=$(cat "$state/counter")
        number=$((number + 1))
        printf '%s\n' "$number" > "$state/counter"
        printf '%s\n' "$number" > "$state/invocation/$unit"
        if [[ $unit == swarmyd.service ]]; then
            socket=$TEST_NODE_HOME/swarmy/.swarmy/node/control.sock
            mkdir -p "$(dirname "$socket")"
            rm -f "$socket"
            python3 -c 'import socket, sys; socket.socket(socket.AF_UNIX).bind(sys.argv[1])' "$socket"
        fi ;;
    stop) rm -f "$state/active/$2" ;;
    show) cat "$state/invocation/${@: -1}" ;;
    is-active)
        shift
        quiet=false
        if [[ $1 == --quiet ]]; then quiet=true; shift; fi
        if [[ -f $state/active/$1 ]]; then
            [[ $quiet == true ]] || echo active
        else
            [[ $quiet == true ]] || echo inactive
            exit 3
        fi ;;
    *) : ;;
esac
STUB
cat > "$scenario/stubs/journalctl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
state=$TEST_NODE_ROOT/state
if [[ $1 == -u ]]; then echo "journal of $2"; exit 0; fi
invocation=${1#_SYSTEMD_INVOCATION_ID=}
for file in "$state/invocation/"*; do
    [[ -f $file && $(cat "$file") == "$invocation" ]] || continue
    unit=${file##*/}
    if [[ $unit == swarmyd.service ]]; then
        if [[ -f $state/active/swarmy-node-api.service ]]; then
            echo 'node registered and ready'
        else
            echo 'node register failed; retrying'
        fi
    else
        short=${unit#swarmy-}
        short=${short%.service}
        echo "$short ready"
        echo "$short started"
    fi
    exit 0
done
STUB
cat > "$scenario/stubs/sleep" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
chmod +x "$scenario/stubs/"*
export PATH=$scenario/stubs:$PATH
source scripts/remote-provision-env.sh
set_unit_inputs stack "$(id -un)" '' false ''
render_unit swarmy-stack.service > "$SWARMY_SYSTEMD_DIR/swarmy-stack.service"
render_unit swarmyd.service > "$SWARMY_SYSTEMD_DIR/swarmyd.service"
touch "$state/active/swarmy-stack.service"

if restart_node_agent "$node_repo" >/dev/null 2>&1; then
    echo 'the fake host let the node register without a node API' >&2
    exit 1
fi
: > "$node_root/systemctl.log"
rm -f "$state/active/swarmyd.service"
printf 'a_node_agent_started_before_its_node_api_never_reports_ready: ok\n'

out=$(provision_node_agent stack "$node_repo")
[[ $out == *'enabled; it starts after the node API'* ]]
grep -qx 'enable swarmyd.service' "$node_root/systemctl.log"
if grep -qx 'restart swarmyd.service' "$node_root/systemctl.log"; then
    echo 'stack provisioning started the node too soon' >&2
    exit 1
fi
bash "$node_repo/scripts/remote-services.sh" "$(id -un)" > "$work/services.out"
api_line=$(grep -n '^restart swarmy-node-api.service$' "$node_root/systemctl.log" | head -1 | cut -d: -f1)
node_line=$(grep -n '^restart swarmyd.service$' "$node_root/systemctl.log" | tail -1 | cut -d: -f1)
[[ $api_line -lt $node_line ]]
grep -q 'node agent ready' "$work/services.out"
[[ -f $SWARMY_SYSTEMD_DIR/swarmy-node-api.service ]]
printf 'fresh_stack_host_starts_the_node_agent_after_the_node_api: ok\n'

out=$(provision_node_agent node "$node_repo")
[[ $out == *'registered and ready'* ]]
grep -qx 'restart swarmyd.service' "$node_root/systemctl.log"
rm -f "$state/active/swarmy-node-api.service"
if provision_node_agent node "$node_repo" >/dev/null 2>&1; then
    echo 'joining node registered without the primary node API' >&2
    exit 1
fi
printf 'joining_node_starts_and_waits_during_provisioning: ok\n'

if provision_node_agent other "$node_repo" >/dev/null 2>&1; then
    echo 'accepted unknown node mode' >&2
    exit 1
fi
printf 'provision_node_agent_rejects_an_unknown_mode: ok\n'
