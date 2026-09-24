#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-upgrade-lib.sh"
upgrade_args stack all 600
upgrade_args node services-only 1
for args in 'node all 0' 'bad all 60' 'stack bad 60' 'stack all'; do
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
