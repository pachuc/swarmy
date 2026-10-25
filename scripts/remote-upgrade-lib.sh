#!/usr/bin/env bash
# Kept independent of sudo so upgrade decisions can be tested locally.
upgrade_args() {
    if [[ ( $# != 3 && $# != 4 ) || ( $1 != stack && $1 != node ) || ( $2 != all && $2 != services-only ) || ! $3 =~ ^[0-9]+$ || ( $# == 4 && $4 != build && $4 != prebuilt ) ]]; then
        echo 'Usage: remote-upgrade.sh {stack|node} {all|services-only} DRAIN_TIMEOUT_SECONDS [build|prebuilt]' >&2
        return 1
    fi
}

# Ensure the node API configuration carries a non-empty token, generating one
# only when missing or empty so reruns never rotate an existing token. Prints
# `generated` or `unchanged`, never the token itself.
ensure_api_token() {
    local config=${1:-.swarmy/config.toml}
    python3 - "$config" <<'PY'
import os
import re
import secrets
import sys
path = sys.argv[1]
try:
    with open(path, encoding='utf-8') as handle:
        text = handle.read()
except FileNotFoundError:
    text = ''
lines = text.splitlines(keepends=True) if text else []
section = None
api_index = None
token_index = None
token_value = None
for index, line in enumerate(lines):
    stripped = line.strip()
    if stripped.startswith('[') and stripped.endswith(']'):
        section = stripped[1:-1].strip()
        if section == 'api' and api_index is None:
            api_index = index
        continue
    if section == 'api':
        match = re.match(r"""\s*token\s*=\s*(?P<quote>['"])(?P<value>.*?)(?P=quote)\s*(?:#.*)?$""", line.rstrip('\n'))
        if match and token_index is None:
            token_index = index
            token_value = match.group('value')
if token_value and token_value.strip():
    print('unchanged')
    sys.exit(0)
fresh = secrets.token_hex(16)
if token_index is not None:
    lines[token_index] = 'token = "%s"\n' % fresh
elif api_index is not None:
    lines.insert(api_index + 1, 'token = "%s"\n' % fresh)
else:
    if lines and not lines[-1].endswith('\n'):
        lines[-1] += '\n'
    lines.append('[api]\ntoken = "%s"\n' % fresh)
with open(path, 'w', encoding='utf-8') as handle:
    handle.writelines(lines)
os.chmod(path, 0o600)
print('generated')
PY
}

binary_changed() {
    local installed=$1 candidate=$2
    [[ -f $candidate ]] || { echo "Missing built binary: $candidate" >&2; return 2; }
    [[ -f $installed ]] || return 0
    [[ $(sha256sum "$installed" | cut -d' ' -f1) != $(sha256sum "$candidate" | cut -d' ' -f1) ]]
}

# Return 0 when the unit is stopped or its running image differs, 1 when current,
# and 2 when systemd or hashing cannot be checked safely.
unit_needs_restart() {
    local unit=$1 installed=$2 pid installed_hash running_hash
    pid=$(systemctl show -p MainPID --value "$unit") || return 2
    [[ $pid =~ ^[0-9]+$ ]] || return 2
    if (( pid == 0 )); then return 0; fi
    if ! sudo -n test -e "/proc/$pid/exe"; then
        [[ -d /proc/$pid ]] || return 0
        echo "Cannot inspect running executable for $unit (pid $pid)" >&2
        return 2
    fi
    installed_hash=$(sha256sum "$installed" | cut -d' ' -f1) || return 2
    running_hash=$(sudo -n sha256sum "/proc/$pid/exe" | cut -d' ' -f1) || return 2
    [[ $installed_hash != "$running_hash" ]]
}

unit_listed() { local needle=$1 item; shift; for item in "$@"; do [[ $item == "$needle" ]] && return 0; done; return 1; }

unit_restart_wanted() {
    local unit=$1 binary=$2
    shift 2
    if unit_listed "$unit" "$@"; then return 0; fi
    unit_needs_restart "$unit" "$binary"
}

# Derive settings once from the installed units of a pre-units.env node.
derive_unit_inputs() {
    local mode=$1 user=$2 dir agent tunnel exec primary='' tunnel_s3=false mount status
    dir=$(swarmy_systemd_dir)
    agent=$dir/$(swarmy_agent_unit)
    [[ -f $agent ]] || { echo "Missing $agent; provision this node again (see docs/REMOTE.md)" >&2; return 1; }
    mount=$(sed -n 's/^RequiresMountsFor=//p' "$agent" | head -n 1)
    if [[ $mode == node ]]; then
        tunnel=$dir/$(swarmy_mode_backing_unit node)
        [[ -f $tunnel ]] || { echo "Missing $tunnel on a joining node; provision it again" >&2; return 1; }
        exec=$(sed -n 's/^ExecStart=//p' "$tunnel" | head -n 1)
        primary=${exec##*@}
        if sudo -n grep -qx 'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333' "$(swarmy_etc_dir)/node.env"; then
            tunnel_s3=true
        else
            status=$?
            (( status == 1 )) || { echo "Cannot read $(swarmy_etc_dir)/node.env" >&2; return 1; }
        fi
    fi
    set_unit_inputs "$mode" "$user" "$primary" "$tunnel_s3" "$mount"
}

unit_inputs_for_upgrade() {
    local mode=$1 user=$2 file
    file=$(swarmy_unit_inputs_file)
    if [[ -f $file ]]; then
        load_unit_inputs "$file" || return 1
        [[ $unit_mode == "$mode" && $unit_user == "$user" ]] || {
            echo "$file records $unit_mode units for $unit_user, but this node runs $mode units as $user; provision it again" >&2
            return 1
        }
    else
        derive_unit_inputs "$mode" "$user" || return 1
        record_unit_inputs || return 1
        echo "Recorded unit settings derived from the installed units in $file" >&2
    fi
}

reconcile_units() {
    local -n __reconciled=${1:?result array}
    shift
    (( $# )) || return 0
    write_units __reconciled "$@" || return 1
    if (( ${#__reconciled[@]} )); then sudo -n systemctl daemon-reload || return 1; fi
}

retired_binary_referenced() {
    local binary=$1 dir units unit
    dir=$(swarmy_systemd_dir)
    read_shared_list units swarmy_unit_names || return 2
    for unit in "${units[@]}"; do
        [[ -f $dir/$unit ]] || continue
        if grep -Eq -- "^ExecStart=/usr/local/bin/${binary}( |\$)" "$dir/$unit"; then return 0; fi
    done
    return 1
}

remove_unreferenced_retired_binaries() {
    local binaries binary status
    read_shared_list binaries retired_binary_names || return 1
    for binary in "${binaries[@]}"; do
        if retired_binary_referenced "$binary"; then
            echo "Keeping /usr/local/bin/$binary: an installed unit still runs it; run a full upgrade without --services-only" >&2
        else
            status=$?
            (( status == 1 )) || return 1
            sudo -n rm -f "/usr/local/bin/$binary"
        fi
    done
}
