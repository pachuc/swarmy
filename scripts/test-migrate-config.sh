#!/usr/bin/env bash
# Migration acceptance for the 2026-09 grouped config break.
# Migrates a fixture copied from a real master-rendered config and loads the
# result with the new Settings; a Python-only key check is not enough.
set -euo pipefail
cd "$(dirname "$0")/.."

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cp scripts/fixtures/old-config.toml "$work/config.toml"
python3 scripts/migrate-config-2026-09.py "$work/config.toml" > "$work/out.txt" 2> "$work/err.txt"
[[ -f "$work/config.toml.bak" ]] || { echo 'migration did not write .bak' >&2; exit 1; }
grep -q 'fdb_cluster_file' "$work/config.toml.bak" || { echo '.bak lost old keys' >&2; exit 1; }
# Old flat keys are gone; new grouped keys hold the values, including the token.
python3 - "$work/config.toml" <<'PY'
import sys, tomllib
data = tomllib.load(open(sys.argv[1], 'rb'))
olds = ["fdb_cluster_file", "store_directory", "bus_prefix", "reasoning_effort",
        "credential_file", "provider", "node_id", "node_capacity",
        "volume_snapshots.period_seconds", "gc.grace_seconds",
        "inference.max_wait_seconds"]
def get(d, dotted):
    node = d
    for part in dotted.split('.'):
        if not isinstance(node, dict) or part not in node:
            return None, False
        node = node[part]
    return node, True
failed = [o for o in olds if get(data, o)[1]]
if failed:
    print(f"old keys remain: {failed}", file=sys.stderr)
    sys.exit(1)
PY
grep -q 'token = "test-token-keep-me"' "$work/config.toml" || { echo 'api token lost' >&2; exit 1; }
grep -q '^\[store\]' "$work/config.toml" || { echo 'missing [store]' >&2; exit 1; }
grep -q '^\[selection\]' "$work/config.toml" || { echo 'missing [selection]' >&2; exit 1; }
# Load with the new Settings (fails the test when keys or values are wrong).
cargo run --quiet --locked -p swarmy-config --example check-config "$work/config.toml" > "$work/load.txt"
grep -q '^ok ' "$work/load.txt" || { echo 'new Settings refused the migrated file' >&2; exit 1; }
# Idempotent: a second run leaves the file unchanged.
before=$(sha256sum "$work/config.toml")
python3 scripts/migrate-config-2026-09.py "$work/config.toml" > /dev/null
[[ $(sha256sum "$work/config.toml") == "$before" ]] || { echo 'migration is not idempotent' >&2; exit 1; }
# Unknown keys are kept, not dropped, and reported.
printf 'future_opt = "keep-me"\nstore_directory = "swarmy"\n[node]\nroles = ["volume"]\n' > "$work/unknown.toml"
python3 scripts/migrate-config-2026-09.py "$work/unknown.toml" > /dev/null 2> "$work/unknown.err"
grep -q 'future_opt' "$work/unknown.toml" || { echo 'unknown key was dropped' >&2; exit 1; }
grep -q 'future_opt' "$work/unknown.err" || { echo 'unknown key was not reported' >&2; exit 1; }
echo 'migrate-config-2026-09: ok'
