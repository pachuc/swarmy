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
