#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cat > "$tmp/cargo" <<'FAKE'
#!/usr/bin/env bash
printf '%s\n' 'swarmyd v0.1.0 (/x/crates/swarmyd)' 'swarmy-volume v0.1.0 (/x/crates/swarmy-volume)' 'tokio v1.0.0'
if [[ ${INCLUDE_FDB_VOLUME:-} == 1 && $* == *swarmy-volume* ]]; then
    printf '%s\n' 'foundationdb-sys v0.10.0'
fi
if [[ ${INCLUDE_STORE:-} == 1 ]]; then
    printf '%s\n' 'swarmy-store v0.1.0 (/x/crates/swarmy-store)'
fi
FAKE
chmod +x "$tmp/cargo"
CARGO="$tmp/cargo" scripts/check-node-deps.sh
if INCLUDE_STORE=1 CARGO="$tmp/cargo" scripts/check-node-deps.sh > "$tmp/output" 2>&1; then
    echo 'node dependency check accepted a store dependency' >&2
    exit 1
fi
grep -q 'swarmy-store v0.1.0' "$tmp/output"
if INCLUDE_FDB_VOLUME=1 CARGO="$tmp/cargo" scripts/check-node-deps.sh > "$tmp/output" 2>&1; then
    echo 'node dependency check accepted a volume FoundationDB dependency' >&2
    exit 1
fi
grep -q 'swarmy-volume must not depend' "$tmp/output"
grep -q 'foundationdb-sys v0.10.0' "$tmp/output"
echo 'node dependency check fixtures passed'
