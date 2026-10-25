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
trap 'rm -rf "$work"' EXIT
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
printf 'remote upgrade arguments and hashes: ok\n'
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
! grep -q swarmy-stack.service "$root/install.log"
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
printf 'remote upgrade unit reconciliation: ok\n'
