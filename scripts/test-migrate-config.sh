#!/usr/bin/env bash
# Migration acceptance for the 2026-09 grouped config break.
# Migrates a hand-written fixture covering every old flat key and loads the
# result with the new Settings; a Python-only key check is not enough.
set -euo pipefail
cd "$(dirname "$0")/.."

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cp scripts/fixtures/old-config.toml "$work/config.toml"
chmod 0600 "$work/config.toml"
python3 scripts/migrate-config-2026-09.py "$work/config.toml" > "$work/out.txt" 2> "$work/err.txt"
[[ -f "$work/config.toml.bak" ]] || { echo 'migration did not write .bak' >&2; exit 1; }
grep -q 'fdb_cluster_file' "$work/config.toml.bak" || { echo '.bak lost old keys' >&2; exit 1; }
# The config can hold the API token, so the backup and the result keep mode 0600,
# and the write is atomic (no temp files left behind).
[[ $(stat -c %a "$work/config.toml") == 600 ]] || { echo 'migrated file lost mode 0600' >&2; exit 1; }
[[ $(stat -c %a "$work/config.toml.bak") == 600 ]] || { echo 'backup lost mode 0600' >&2; exit 1; }
[[ -z $(ls "$work"/config.toml.*.tmp 2>/dev/null) ]] || { echo 'temp file left behind' >&2; exit 1; }
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
# Dotted provider ids stay one key, control characters survive the round trip,
# and the nested unknown key is kept.
custom = data.get("selection", {}).get("custom_providers", {})
assert "z.ai" in custom, f"z.ai lost: {sorted(custom)}"
assert custom["z.ai"]["base_url"] == "https://z.ai/api"
assert custom["z.ai"]["api"] == "OpenAiCompletions"
assert data["context"]["system_prompt"] == "line one\r\nline two", repr(data["context"]["system_prompt"])
assert data["gc"]["future_gc_opt"] == "keep-me", "nested unknown key was dropped"
PY
grep -q 'token = "test-token-keep-me"' "$work/config.toml" || { echo 'api token lost' >&2; exit 1; }
grep -q '^\[store\]' "$work/config.toml" || { echo 'missing [store]' >&2; exit 1; }
grep -q '^\[selection\]' "$work/config.toml" || { echo 'missing [selection]' >&2; exit 1; }
# Keys that are not bare-safe are quoted, and carriage returns are escaped.
grep -q '"z.ai"' "$work/config.toml" || { echo 'dotted key was not quoted' >&2; exit 1; }
grep -q '\\r' "$work/config.toml" || { echo 'carriage return was not escaped' >&2; exit 1; }
# Nested unknowns are reported, not dropped.
grep -q 'future_gc_opt' "$work/err.txt" || { echo 'nested unknown key was not reported' >&2; exit 1; }
# Strip the unknown key before loading: Settings rejects unknown keys, so the
# load check covers the renames while the checks above cover unknown handling.
python3 - "$work/config.toml" "$work/clean.toml" <<'PY'
import importlib.util, sys, tomllib
data = tomllib.load(open(sys.argv[1], 'rb'))
del data["gc"]["future_gc_opt"]
spec = importlib.util.spec_from_file_location("migrate", "scripts/migrate-config-2026-09.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
open(sys.argv[2], "w").write(module.dump(data))
PY
# Load with the new Settings (fails the test when keys or values are wrong).
cargo run --quiet --locked -p swarmy-config --example check-config "$work/clean.toml" > "$work/load.txt"
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
printf '[gc]\ngrace_secs = 60\nfuture_nested = "keep-me"\n' > "$work/nested.toml"
python3 scripts/migrate-config-2026-09.py --check "$work/nested.toml" > /dev/null 2>&1 || true
printf 'store_directory = "swarmy"\n[gc]\ngrace_secs = 60\nfuture_nested = "keep-me"\n' > "$work/nested.toml"
python3 scripts/migrate-config-2026-09.py "$work/nested.toml" > /dev/null 2> "$work/nested.err"
grep -q 'future_nested' "$work/nested.toml" || { echo 'nested unknown key was dropped' >&2; exit 1; }
grep -q 'future_nested' "$work/nested.err" || { echo 'nested unknown key was not reported' >&2; exit 1; }
# The rename list in docs/api-breaks.txt is generated from --print-breaks.
python3 scripts/migrate-config-2026-09.py --print-breaks > "$work/breaks.txt"
python3 - "$work/breaks.txt" <<'PY'
import re, sys
printed = open(sys.argv[1]).read().splitlines()
documented = []
for line in open("docs/api-breaks.txt"):
    match = re.match(r'# - `(.*?)` moves to `(.*?)`', line.strip())
    if match:
        documented.append(f"{match.group(1)} -> {match.group(2)}")
assert documented, "no rename list in docs/api-breaks.txt"
assert printed == documented, (
    "docs/api-breaks.txt drifted from --print-breaks:\n"
    + "\n".join(f"- {l}" for l in documented if l not in printed)
    + "\n"
    + "\n".join(f"+ {l}" for l in printed if l not in documented)
)
PY
echo 'migrate-config-2026-09: ok'
