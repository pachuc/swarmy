#!/usr/bin/env bash
set -euo pipefail
# Library crates use `thiserror` for errors other crates match on; `anyhow`
# stays in binaries. Fail when a crate with a library target lists `anyhow`
# in [dependencies] (dev-dependencies are fine: tests may use anything).
# A crate whose lib.rs declares no modules counts as a binary (swarmyd keeps
# shared protocol types there while its code lives in the binary).
cd "$(dirname "$0")/.."
fail=0
for manifest in crates/*/Cargo.toml; do
    if ! awk '
        /^\[(target\..*)?dependencies\]$/ { in_deps = 1; next }
        /^\[/ { in_deps = 0; next }
        in_deps && /^[[:space:]]*anyhow([.[:space:]]|=|$)/ { found = 1; exit 0 }
        END { exit !found }
    ' "$manifest"; then
        continue
    fi
    lib="${manifest%/Cargo.toml}/src/lib.rs"
    if [[ ! -f "$lib" ]] || ! grep -qE '^(pub(\(crate\))? )?mod [a-z_]' "$lib"; then
        continue
    fi
    echo "error: $manifest lists anyhow in [dependencies] but ships a library; use thiserror" >&2
    fail=1
done
exit "$fail"
