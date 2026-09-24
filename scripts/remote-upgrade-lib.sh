#!/usr/bin/env bash
# Kept independent of sudo so upgrade decisions can be tested locally.
upgrade_args() {
    if [[ $# != 3 || ( $1 != stack && $1 != node ) || ( $2 != all && $2 != services-only ) || ! $3 =~ ^[1-9][0-9]*$ ]]; then
        echo 'Usage: remote-upgrade.sh {stack|node} {all|services-only} DRAIN_TIMEOUT_SECONDS' >&2
        return 1
    fi
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
