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
grep -qx 'ExecStart=/usr/local/bin/swarmy-core node' "$SWARMY_SYSTEMD_DIR/swarmyd.service"
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
printf '[Service]\nExecStart=/usr/bin/ssh -N ubuntu@10.0.0.9\n' > "$SWARMY_SYSTEMD_DIR/swarmy-tunnel.service"
printf 'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\n' > "$SWARMY_ETC_DIR/node.env"
derive_unit_inputs node ubuntu
[[ $unit_primary == 10.0.0.9 && $unit_tunnel_s3 == true ]]
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
    show) printf '0\n' ;;
    *) printf '%s\n' "$*" >> "$TEST_NODE_ROOT/systemctl.log" ;;
esac
STUB
cat > "$scenario/stubs/sudo" <<'STUB'
#!/usr/bin/env bash
[[ $1 == -n ]] && shift
case $1 in
    install)
        args=("$@")
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
    sh) printf '[]\n' ;;
    rm) shift; command rm -f "$TEST_NODE_ROOT${@: -1}" ;;
    *) command "$@" ;;
esac
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
for binary in swarmy swarmy-core swarmy-provisioner libfdb_c.so; do
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
                         'swarmy-api.service', 'swarmy-provisioner.service'], data
PY
[[ ! -f $node_repo/.swarmy/upgrade-pending-units ]]
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
printf 'host upgrade entrypoint: ok\n'
