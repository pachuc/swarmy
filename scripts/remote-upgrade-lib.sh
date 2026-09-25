#!/usr/bin/env bash
# Kept independent of sudo so upgrade decisions can be tested locally.
upgrade_args() {
    if [[ $# != 3 || ( $1 != stack && $1 != node ) || ( $2 != all && $2 != services-only ) || ! $3 =~ ^[0-9]+$ ]]; then
        echo 'Usage: remote-upgrade.sh {stack|node} {all|services-only} DRAIN_TIMEOUT_SECONDS' >&2
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
