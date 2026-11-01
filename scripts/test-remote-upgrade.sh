#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-upgrade-lib.sh"
upgrade_args stack all 600
upgrade_args node services-only 1
upgrade_args stack all 600 prebuilt
upgrade_args node all 5 build
for args in 'node all -1' 'node all x' 'bad all 60' 'stack bad 60' 'stack all' 'stack all 600 other' 'stack all 600 prebuilt extra'; do
    read -r -a parts <<< "$args"
    if upgrade_args "${parts[@]}" >/dev/null 2>&1; then
        echo "accepted invalid upgrade arguments: $args" >&2
        exit 1
    fi
done
work=$(mktemp -d)
trap 'if [[ ${root_repo_link:-} ]]; then rm -f "$root_repo_link"; fi; rm -rf "$work"' EXIT
printf 'before' > "$work/old"
printf 'before' > "$work/new"
if binary_changed "$work/old" "$work/new"; then
    echo 'treated matching hashes as changed' >&2
    exit 1
fi
printf 'after' > "$work/new"
binary_changed "$work/old" "$work/new"
binary_changed "$work/absent" "$work/new"
if binary_changed "$work/old" "$work/missing" >/dev/null 2>&1; then
    echo 'accepted missing upgrade candidate' >&2
    exit 1
fi
systemctl() { [[ $1 == show ]] && printf '%s\n' "$PPID"; }
sudo() { [[ $1 == -n ]] && shift; "$@"; }
cp /proc/$PPID/exe "$work/running"
if unit_needs_restart fake.service "$work/running"; then
    echo 'restarted an unchanged process' >&2
    exit 1
fi
printf 'different binary' > "$work/running"
unit_needs_restart fake.service "$work/running"
systemctl() { printf '0\n'; }
unit_needs_restart fake.service "$work/running"
printf 'host upgrade arguments and hashes: ok\n'
token_dir="$work/api-token"
mkdir -p "$token_dir"
# A control node provisioned before token provisioning gets a non-empty token.
[[ $(ensure_api_token "$token_dir/missing.toml") == generated ]]
first=$(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/missing.toml")
[[ -n $first ]]
[[ $(stat -c %a "$token_dir/missing.toml") == 600 ]]
# Rerunning the upgrade path keeps the existing token.
[[ $(ensure_api_token "$token_dir/missing.toml") == unchanged ]]
[[ $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/missing.toml") == "$first" ]]
# Empty and whitespace-only tokens are replaced; other sections are preserved.
printf '[api]\ntoken = ""\n[remote]\nregion = "us-east-1"\n' > "$token_dir/empty.toml"
[[ $(ensure_api_token "$token_dir/empty.toml") == generated ]]
[[ -n $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/empty.toml") ]]
grep -q 'region = "us-east-1"' "$token_dir/empty.toml"
printf '[api]\ntoken = "   "\n' > "$token_dir/blank.toml"
[[ $(ensure_api_token "$token_dir/blank.toml") == generated ]]
[[ -n $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/blank.toml") ]]
# A section without a token gains one; an existing token never rotates.
printf '[api]\nlisten = "127.0.0.1:8742"\n' > "$token_dir/notoken.toml"
[[ $(ensure_api_token "$token_dir/notoken.toml") == generated ]]
[[ -n $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/notoken.toml") ]]
grep -q 'listen = "127.0.0.1:8742"' "$token_dir/notoken.toml"
printf '[api]\ntoken = "already-set"\n' > "$token_dir/kept.toml"
[[ $(ensure_api_token "$token_dir/kept.toml") == unchanged ]]
[[ $(sed -n 's/^token = "\(.*\)"$/\1/p' "$token_dir/kept.toml") == already-set ]]
printf 'api token provisioning: ok\n'

source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
root=$work/root
mkdir -p "$root/etc/systemd/system" "$root/etc/swarmy" "$root/usr/local/bin"
export SWARMY_SYSTEMD_DIR=$root/etc/systemd/system SWARMY_ETC_DIR=$root/etc/swarmy
sudo() { [[ $1 == -n ]] && shift
    case $1 in
      rm) command rm -f "$root${@: -1}" ;;
      systemctl) printf '%s\n' "$*" >> "$root/systemctl.log" ;;
      install) printf '%s\n' "${@: -1}" >> "$root/install.log"; command "$@" ;;
      *) command "$@" ;;
    esac; }
set_unit_inputs stack swarmy '' false ''
render_unit swarmy-stack.service > "$SWARMY_SYSTEMD_DIR/swarmy-stack.service"
cat > "$SWARMY_SYSTEMD_DIR/swarmyd.service" <<'UNIT'
[Unit]
Description=Swarmy node agent
RequiresMountsFor=/mnt/swarmy-local
[Service]
ExecStart=/usr/local/bin/swarmyd
UNIT
printf '[Service]\nExecStart=/usr/local/bin/swarmy-scheduler\n' > "$SWARMY_SYSTEMD_DIR/swarmy-scheduler.service"
printf 'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\n' > "$SWARMY_ETC_DIR/node.env"
for binary in swarmyd swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api swarmy-core; do touch "$root/usr/local/bin/$binary"; done
unset unit_mode unit_user unit_home unit_repo unit_primary unit_tunnel_s3 unit_mount
remove_unreferenced_retired_binaries 2>/dev/null
[[ -f $root/usr/local/bin/swarmyd && -f $root/usr/local/bin/swarmy-scheduler ]]
for binary in swarmy-worker swarmy-gateway swarmy-api; do [[ ! -f $root/usr/local/bin/$binary ]]; done
unit_inputs_for_upgrade stack swarmy
[[ $unit_mount == /mnt/swarmy-local ]]
grep -qx 'SWARMY_UNIT_MODE=stack' "$SWARMY_ETC_DIR/units.env"
stack_hash=$(sha256sum "$SWARMY_SYSTEMD_DIR/swarmy-stack.service")
stack_mtime=$(stat -c %Y "$SWARMY_SYSTEMD_DIR/swarmy-stack.service")
changed=()
reconcile_units changed swarmy-stack.service swarmyd.service swarmy-scheduler.service
[[ ${changed[*]} == 'swarmyd.service swarmy-scheduler.service' ]]
if grep -q swarmy-stack.service "$root/install.log"; then echo 'stack unit was reinstalled' >&2; exit 1; fi
[[ $(sha256sum "$SWARMY_SYSTEMD_DIR/swarmy-stack.service") == "$stack_hash" ]]
[[ $(stat -c %Y "$SWARMY_SYSTEMD_DIR/swarmy-stack.service") == "$stack_mtime" ]]
[[ $(wc -l < "$root/systemctl.log") == 1 ]]
grep -qx 'systemctl daemon-reload' "$root/systemctl.log"
grep -qx 'ExecStart=/usr/local/bin/swarmy-node' "$SWARMY_SYSTEMD_DIR/swarmyd.service"
grep -qx 'RequiresMountsFor=/mnt/swarmy-local' "$SWARMY_SYSTEMD_DIR/swarmyd.service"
changed=()
reconcile_units changed swarmy-stack.service swarmyd.service swarmy-scheduler.service
[[ ${#changed[@]} == 0 && $(wc -l < "$root/systemctl.log") == 1 ]]
remove_unreferenced_retired_binaries
for binary in swarmyd swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api; do [[ ! -f $root/usr/local/bin/$binary ]]; done
[[ -f $root/usr/local/bin/swarmy-core ]]

root=$work/node-root
mkdir -p "$root/etc/systemd/system" "$root/etc/swarmy"
export SWARMY_SYSTEMD_DIR=$root/etc/systemd/system SWARMY_ETC_DIR=$root/etc/swarmy
printf '[Service]\nExecStart=/usr/local/bin/swarmyd\n' > "$SWARMY_SYSTEMD_DIR/swarmyd.service"
printf '[Service]\nExecStart=/usr/bin/ssh -N -L 127.0.0.1:4500:127.0.0.1:4500 ubuntu@10.0.0.9\n' > "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
printf 'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\n' > "$SWARMY_ETC_DIR/node.env"
derive_unit_inputs node ubuntu
[[ $unit_primary == 10.0.0.9 && $unit_tunnel_s3 == true ]]
old_inputs=$(unit_inputs_text)
render_tunnel_unit > "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
[[ $(render_tunnel_unit) == *'127.0.0.1:8743:127.0.0.1:8743'* ]]
[[ $(render_tunnel_unit) != *4500* ]]
derive_unit_inputs node ubuntu
[[ $(unit_inputs_text) == "$old_inputs" ]]
printf 'old_tunnel_units_derive_the_same_inputs: ok\n'
printf 'SWARMY_S3_ENDPOINT=https://objects.example.invalid\n' > "$SWARMY_ETC_DIR/node.env"
derive_unit_inputs node ubuntu
[[ $unit_tunnel_s3 == false ]]
rm "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
if derive_unit_inputs node ubuntu 2>/dev/null; then echo 'accepted missing tunnel unit' >&2; exit 1; fi
set_unit_inputs node ubuntu 10.0.0.9 false ''
unit_inputs_text > "$SWARMY_ETC_DIR/units.env"
if unit_inputs_for_upgrade stack swarmy 2>/dev/null; then echo 'accepted mismatched recorded mode' >&2; exit 1; fi
systemctl() { return 99; }
unit_restart_wanted fake.service /missing/binary fake.service
printf 'host upgrade unit reconciliation: ok\n'
# Node API environment migration preserves operator settings and is idempotent.
export SWARMY_ETC_DIR=$work/env-etc
mkdir -p "$SWARMY_ETC_DIR"
old_env=$'SWARMY_S3_KEY=secret\nSWARMY_DEFAULT_IMAGE=base-ubuntu:hz\nSWARMY_FDB_CLUSTER_FILE=/x\nLD_LIBRARY_PATH=/y\nSWARMY_NODE_API_URL=http://old\n'
node_env=$(printf '%s' "$old_env" | node_env_with_node_api node)
[[ $node_env == $'SWARMY_S3_KEY=secret\nSWARMY_DEFAULT_IMAGE=base-ubuntu:hz\nSWARMY_NODE_API_URL=http://127.0.0.1:8743\nSWARMY_NODE_TOKEN_FILE='"$SWARMY_ETC_DIR/node-token" ]]
[[ $(printf '%s\n' "$node_env" | node_env_with_node_api node) == "$node_env" ]]
stack_env=$(printf '%s' "$old_env" | node_env_with_node_api stack)
[[ $stack_env == *'SWARMY_FDB_CLUSTER_FILE=/x'* && $stack_env == *'LD_LIBRARY_PATH=/y'* ]]
[[ $(printf '%s\n' "$stack_env" | node_env_with_node_api stack) == "$stack_env" ]]
printf 'node_env_with_node_api_keeps_other_lines: ok\n'

# Exercise the SSH entrypoint against a pre-units.env node without touching host units.
scenario=$work/entrypoint
mkdir -p "$scenario/home/swarmy/scripts" "$scenario/home/swarmy/.swarmy/release" \
    "$scenario/root/etc/systemd/system" "$scenario/root/etc/swarmy" \
    "$scenario/root/usr/local/bin" "$scenario/stubs"
cp scripts/remote-{upgrade.sh,upgrade-lib.sh,provision-env.sh,s3-env.sh} "$scenario/home/swarmy/scripts/"
node_repo=$scenario/home/swarmy
if [[ $(id -un) == root ]]; then
    # service_home_for deliberately hardcodes root's home.
    [[ ! -e /root/swarmy && ! -L /root/swarmy ]] || { echo '/root/swarmy already exists; cannot isolate upgrade test' >&2; exit 1; }
    ln -s "$node_repo" /root/swarmy
    root_repo_link=/root/swarmy
    node_repo=/root/swarmy
fi
node_root=$scenario/root
cat > "$scenario/stubs/getent" <<'STUB'
#!/usr/bin/env bash
if [[ $1 == passwd && $2 == "$(id -un)" ]]; then
    printf '%s:x:1000:1000::%s:/bin/bash\n' "$2" "$TEST_NODE_HOME"
else
    command /usr/bin/getent "$@"
fi
STUB
cat > "$scenario/stubs/systemctl" <<'STUB'
#!/usr/bin/env bash
case $1 in
    cat) [[ -f $SWARMY_SYSTEMD_DIR/$2 ]] ;;
    show) printf '%s\n' "${TEST_RUNNING_PID:-0}" ;;
    *) printf '%s\n' "$*" >> "$TEST_NODE_ROOT/systemctl.log" ;;
esac
STUB
cat > "$scenario/stubs/sudo" <<'STUB'
#!/usr/bin/env bash
[[ $1 == -n ]] && shift
case $1 in
    install)
        # The test runs unprivileged, so drop the owner and group a real sudo would set.
        args=()
        while (( $# )); do
            case $1 in
                -o|-g) shift 2 ;;
                *) args+=("$1"); shift ;;
            esac
        done
        last=$(( ${#args[@]} - 1 ))
        dest=${args[last]}
        [[ $dest == /usr/local/* || $dest == /etc/* ]] && args[last]="$TEST_NODE_ROOT$dest"
        if [[ ${args[1]} == -d ]]; then
            for ((i=3; i<=last; i++)); do
                [[ ${args[i]} == /usr/local/* || ${args[i]} == /etc/* ]] && args[i]="$TEST_NODE_ROOT${args[i]}"
            done
        fi
        command "${args[@]}" ;;
    tee) if [[ $2 == "$TEST_NODE_ROOT"/* ]]; then command tee "$2"; else command tee "$TEST_NODE_ROOT$2"; fi ;;
    ldconfig) : ;;
    systemctl) shift; systemctl "$@" ;;
    sh)
        if [[ $* == *'node-credential issue'* ]]; then
            printf '%s\n' "$*" >> "$TEST_NODE_ROOT/issued.log"
            printf 'swn.%s.test-secret\n' "$(cat "$TEST_NODE_HOME/swarmy/.swarmy/node-id")"
        elif [[ $* == *'cat > '*node-token* ]]; then
            cat > "$SWARMY_ETC_DIR/node-token"
        else
            printf '[]\n'
        fi ;;
    rm)
        shift
        for path in "$@"; do
            [[ $path == -* ]] && continue
            [[ $path == /usr/local/* || $path == /etc/* ]] && path=$TEST_NODE_ROOT$path
            command rm -rf -- "$path"
        done ;;
    *) command "$@" ;;
esac
STUB
cat > "$scenario/stubs/sha256sum" <<'STUB'
#!/usr/bin/env bash
file=$1
[[ $file == /usr/local/* ]] && file=$TEST_NODE_ROOT$file
/usr/bin/sha256sum "$file"
STUB
chmod +x "$scenario/stubs/"*
export TEST_NODE_HOME=$scenario/home TEST_NODE_ROOT=$node_root
export SWARMY_SYSTEMD_DIR=$node_root/etc/systemd/system SWARMY_ETC_DIR=$node_root/etc/swarmy
export PATH=$scenario/stubs:$PATH
# Keep the backing unit identical so the upgrade must not reinstall it.
set_unit_inputs stack "$(id -un)" '' false ''
render_unit swarmy-stack.service > "$SWARMY_SYSTEMD_DIR/swarmy-stack.service"
printf '[Service]\nExecStart=/usr/local/bin/swarmyd\n' > "$SWARMY_SYSTEMD_DIR/swarmyd.service"
for role in scheduler worker gateway api; do
    printf '[Service]\nExecStart=/usr/local/bin/swarmy-%s\n' "$role" > "$SWARMY_SYSTEMD_DIR/swarmy-$role.service"
done
printf 'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\n' > "$SWARMY_ETC_DIR/node.env"
printf '[api]\ntoken = "existing"\n' > "$node_repo/.swarmy/config.toml"
printf '%s' '01ARZ3NDEKTSV4RRFFQ69G5FAV' > "$node_repo/.swarmy/node-id"
for binary in swarmy swarmy-core swarmy-node swarmy-provisioner libfdb_c.so; do
    printf '#!/usr/bin/env bash\necho "[]"\n' > "$node_repo/.swarmy/release/$binary"
    chmod +x "$node_repo/.swarmy/release/$binary"
done
for binary in swarmyd swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api; do
    touch "$node_root/usr/local/bin/$binary"
done
bash "$node_repo/scripts/remote-upgrade.sh" stack all 5 prebuilt > "$scenario/first.json"
python3 - "$scenario/first.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding='utf-8') as file:
    data = json.load(file)
assert data['units'] == ['swarmyd.service', 'swarmy-scheduler.service',
                         'swarmy-worker.service', 'swarmy-gateway.service',
                         'swarmy-api.service', 'swarmy-node-api.service',
                         'swarmy-provisioner.service'], data
PY
[[ ! -f $node_repo/.swarmy/upgrade-pending-units ]]
[[ -f $SWARMY_SYSTEMD_DIR/swarmy-node-api.service ]]
grep -q '^enable swarmy-node-api.service$' "$node_root/systemctl.log"
[[ $(wc -l < "$node_root/issued.log") == 1 ]]
grep -q '^swn.01ARZ3NDEKTSV4RRFFQ69G5FAV.' "$SWARMY_ETC_DIR/node-token"
grep -q '^SWARMY_NODE_API_URL=http://127.0.0.1:8743$' "$SWARMY_ETC_DIR/node.env"
grep -q "^SWARMY_NODE_TOKEN_FILE=$SWARMY_ETC_DIR/node-token$" "$SWARMY_ETC_DIR/node.env"
[[ -f $SWARMY_SYSTEMD_DIR/swarmy-provisioner.service ]]
grep -q '^enable swarmy-provisioner.service$' "$node_root/systemctl.log"
grep -q '^restart swarmy-provisioner.service$' "$node_root/systemctl.log"
for binary in swarmyd swarmy-scheduler swarmy-worker swarmy-gateway swarmy-api; do
    [[ ! -f $node_root/usr/local/bin/$binary ]]
done
[[ $(grep -c '^daemon-reload$' "$node_root/systemctl.log") == 1 ]]
for role in scheduler worker gateway api; do
    [[ $(grep -n "^restart swarmy-$role.service$" "$node_root/systemctl.log" | head -1 | cut -d: -f1) -lt $(grep -n '^restart swarmyd.service$' "$node_root/systemctl.log" | head -1 | cut -d: -f1) ]]
done
if grep -q 'restart swarmy-stack.service' "$node_root/systemctl.log"; then echo 'stack unit restarted' >&2; exit 1; fi
bash "$node_repo/scripts/remote-upgrade.sh" stack all 5 prebuilt > "$scenario/second.json"
python3 - "$scenario/second.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding='utf-8') as file:
    data = json.load(file)
assert data['units'] == [], data
PY
[[ $(grep -c '^daemon-reload$' "$node_root/systemctl.log") == 1 ]]
[[ $(wc -l < "$node_root/issued.log") == 1 ]]
printf 'host upgrade entrypoint: ok\n'
# A node-only host must not acquire credentials on a services-only pass.
for unit in "$SWARMY_SYSTEMD_DIR"/swarmy-{stack,scheduler,worker,gateway,api,node-api,provisioner}.service; do rm -f "$unit"; done
set_unit_inputs node "$(id -un)" 10.0.0.9 false ''
unit_inputs_text > "$SWARMY_ETC_DIR/units.env"
render_unit swarmy-tunnel.service > "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
# Simulate the old tunnel with its FoundationDB forward; it must be re-rendered.
sed -i 's/8743/4500/g' "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
printf 'SWARMY_S3_KEY=keep\nSWARMY_FDB_CLUSTER_FILE=/old/fdb.cluster\nLD_LIBRARY_PATH=/old/lib\n' > "$SWARMY_ETC_DIR/node.env"
rm "$SWARMY_ETC_DIR/node-token"
touch "$node_root/usr/local/bin/swarmy-core"
: > "$node_root/systemctl.log"
HOME="$TEST_NODE_HOME" bash "$node_repo/scripts/remote-upgrade.sh" node services-only 0 prebuilt > "$scenario/services-only.json"
[[ $(cat "$SWARMY_ETC_DIR/node.env") == *SWARMY_FDB_CLUSTER_FILE* ]]
[[ ! -s $node_root/systemctl.log ]]
[[ -f $node_root/usr/local/bin/swarmy-core ]]
printf 'services_only_leaves_the_node_alone: ok\n'
if HOME="$TEST_NODE_HOME" bash "$node_repo/scripts/remote-upgrade.sh" node all 0 prebuilt > "$scenario/missing.json" 2> "$scenario/missing.err"; then
    echo 'accepted node upgrade without a token' >&2; exit 1
fi
grep -q 'swarmy swarm upgrade' "$scenario/missing.err"
[[ ! -s $node_root/systemctl.log ]]
printf 'node_upgrade_without_a_token_fails_and_names_the_fix: ok\n'
printf 'swn.01ARZ3NDEKTSV4RRFFQ69G5FAV.test-secret\n' > "$SWARMY_ETC_DIR/node-token"
# The fake running executable equals the installed node after the restart.
cp /proc/$$/exe "$node_repo/.swarmy/release/swarmy-node"
export TEST_RUNNING_PID=$$
touch "$node_root/usr/local/bin/swarmy-core"
mkdir -p "$node_repo/.dev"
touch "$node_repo/.dev/fdb.cluster"
HOME="$TEST_NODE_HOME" bash "$node_repo/scripts/remote-upgrade.sh" node all 0 prebuilt > "$scenario/node.json"
stop_line=$(grep -n '^stop swarmyd.service$' "$node_root/systemctl.log" | tail -1 | cut -d: -f1)
tunnel_line=$(grep -n '^restart swarmy-tunnel.service$' "$node_root/systemctl.log" | tail -1 | cut -d: -f1)
start_line=$(grep -n '^start swarmyd.service$' "$node_root/systemctl.log" | tail -1 | cut -d: -f1)
[[ $stop_line -lt $tunnel_line && $tunnel_line -lt $start_line ]]
! grep -q 'FDB_CLUSTER_FILE\|LD_LIBRARY_PATH' "$SWARMY_ETC_DIR/node.env"
[[ ! -f $node_root/usr/local/bin/swarmy-core && ! -f $node_repo/.dev/fdb.cluster ]]
printf 'node_upgrade_stops_restarts_tunnel_then_starts: ok\n'

# An existing object-store node has one SSH process forwarding all three ports.
set_unit_inputs node "$(id -un)" 10.0.0.9 true ''
unit_inputs_text > "$SWARMY_ETC_DIR/units.env"
render_unit swarmy-tunnel.service > "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
sed -i '/^Wants=swarmy-objects-tunnel.service$/d' "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
# Use the actual service user rather than a fixture-specific username.
sed -i "s/ $(id -un)@10.0.0.9/ -L 127.0.0.1:8333:127.0.0.1:8333 $(id -un)@10.0.0.9/" "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
rm -f "$SWARMY_SYSTEMD_DIR/swarmy-objects-tunnel.service"
set_unit_inputs node "$(id -un)" 10.0.0.9 false ''
render_unit swarmyd.service > "$SWARMY_SYSTEMD_DIR/swarmyd.service"
: > "$node_root/systemctl.log"
HOME="$TEST_NODE_HOME" bash "$node_repo/scripts/remote-upgrade.sh" node all 0 prebuilt > "$scenario/object-tunnel.json"
[[ -f $SWARMY_SYSTEMD_DIR/swarmy-objects-tunnel.service ]]
grep -q '^enable swarmy-objects-tunnel.service$' "$node_root/systemctl.log"
stop_line=$(grep -n '^stop swarmyd.service$' "$node_root/systemctl.log" | tail -1 | cut -d: -f1)
tunnel_line=$(grep -n '^restart swarmy-tunnel.service swarmy-objects-tunnel.service$' "$node_root/systemctl.log" | tail -1 | cut -d: -f1)
start_line=$(grep -n '^start swarmyd.service$' "$node_root/systemctl.log" | tail -1 | cut -d: -f1)
[[ $stop_line -lt $tunnel_line && $tunnel_line -lt $start_line ]]
! grep -q '8333' "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
printf 'object_store_node_upgrade_splits_the_tunnel: ok\n'
