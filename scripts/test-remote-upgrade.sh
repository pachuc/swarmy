#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-upgrade-lib.sh"
upgrade_args stack all 600
upgrade_args node services-only 1
! upgrade_args node all 0 >/dev/null 2>&1
! upgrade_args bad all 60 >/dev/null 2>&1
! upgrade_args stack bad 60 >/dev/null 2>&1
! upgrade_args stack all >/dev/null 2>&1
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
printf 'before' > "$work/old"
printf 'before' > "$work/new"
! binary_changed "$work/old" "$work/new"
printf 'after' > "$work/new"
binary_changed "$work/old" "$work/new"
binary_changed "$work/absent" "$work/new"
! binary_changed "$work/old" "$work/missing" >/dev/null 2>&1
printf 'remote upgrade arguments and hashes: ok\n'
