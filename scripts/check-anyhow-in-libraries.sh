#!/usr/bin/env bash
set -euo pipefail
# Library crates use `thiserror` for errors other crates match on; `anyhow`
# stays in binaries. Fail when a crate with a library target depends on the
# `anyhow` package in [dependencies] or a target-specific dependencies table
# (dev-dependencies are fine: tests may use anything). The manifests are
# parsed as TOML so a renamed dependency (`errors = { package = "anyhow" }`,
# directly or through [workspace.dependencies]) is caught too.
# A crate whose lib.rs declares no modules and that has a binary target counts
# as a binary (swarmyd keeps shared protocol types there while its code lives
# in the binary). A single-file library with no binary is still a library.
cd "$(dirname "$0")/.."
anyhow_manifests=$(python3 - <<'PY'
import glob
import os
import tomllib


def load(path):
    with open(path, "rb") as handle:
        return tomllib.load(handle)


workspace = {}
if os.path.isfile("Cargo.toml"):
    workspace = load("Cargo.toml").get("workspace", {}).get("dependencies", {})


def package(name, spec):
    if isinstance(spec, dict) and spec.get("workspace") is True:
        spec = workspace.get(name, name)
    if isinstance(spec, dict):
        return spec.get("package", name)
    return name


for manifest in sorted(glob.glob("crates/*/Cargo.toml")):
    data = load(manifest)
    tables = [data.get("dependencies", {})]
    tables += [target.get("dependencies", {}) for target in data.get("target", {}).values()]
    if any(package(name, spec) == "anyhow" for table in tables for name, spec in table.items()):
        print(manifest)
PY
)
fail=0
for manifest in $anyhow_manifests; do
    crate="${manifest%/Cargo.toml}"
    lib="$crate/src/lib.rs"
    [[ -f "$lib" ]] || continue
    if ! grep -qE '^(pub(\(crate\))? )?mod [a-z_]' "$lib" \
        && { [[ -f "$crate/src/main.rs" ]] || [[ -d "$crate/src/bin" ]] || grep -q '^\[\[bin\]\]' "$manifest"; }; then
        continue
    fi
    echo "error: $manifest depends on anyhow in [dependencies] but ships a library; use thiserror" >&2
    fail=1
done
exit "$fail"
