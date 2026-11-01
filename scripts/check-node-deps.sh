#!/usr/bin/env bash
# The node binary and its libraries must not link the store or FoundationDB through normal edges; tests may.
set -euo pipefail
cargo=${CARGO:-cargo}
packages=(swarmyd swarmy-volume swarmy-sandbox)
forbidden='^(foundationdb|foundationdb-sys|swarmy-store) '
status=0
for package in "${packages[@]}"; do
    tree=$("$cargo" tree --locked -e normal -p "$package" --prefix none --format '{p}')
    if found=$(grep -E "$forbidden" <<< "$tree" | sort -u) && [[ -n $found ]]; then
        printf '%s must not depend on these through normal edges:\n%s\n' "$package" "$found" >&2
        status=1
    fi
done
exit "$status"
