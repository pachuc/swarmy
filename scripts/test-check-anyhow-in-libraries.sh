#!/usr/bin/env bash
# Exercise scripts/check-anyhow-in-libraries.sh against a fixture crate tree,
# plus the real workspace, so the assertions run the same flags as CI.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/scripts"
cp "$repo_dir/scripts/check-anyhow-in-libraries.sh" "$work/scripts/"

fixture() {
    rm -rf "$work/crates"
    mkdir -p "$work/crates/mylib/src"
    cat >"$work/crates/mylib/Cargo.toml"
    if [ "$1" != "none" ]; then
        printf '%s\n' "$1" >"$work/crates/mylib/src/lib.rs"
    fi
    if [ "${2:-}" = "bin" ]; then
        printf 'fn main() {}\n' >"$work/crates/mylib/src/main.rs"
    fi
}

check() {
    local name="$1" expected="$2"
    if bash "$work/scripts/check-anyhow-in-libraries.sh" >"$work/out.log" 2>&1; then
        actual=0
    else
        actual=1
    fi
    if [ "$actual" != "$expected" ]; then
        printf 'check-anyhow-in-libraries test failed for %s: want exit %s\n' "$name" "$expected"
        cat "$work/out.log"
        exit 1
    fi
}

# A library target with anyhow in [dependencies] fails.
fixture 'mod worker;' <<'EOF'
[dependencies]
anyhow.workspace = true
EOF
check "library with anyhow fails" 1

# anyhow in [dev-dependencies] is fine: tests may use anything.
fixture 'mod worker;' <<'EOF'
[dependencies]
serde.workspace = true

[dev-dependencies]
anyhow.workspace = true
EOF
check "dev-dependency anyhow passes" 0

# A binary without src/lib.rs passes.
fixture none <<'EOF'
[dependencies]
anyhow.workspace = true
EOF
check "binary without lib passes" 0

# A lib.rs declaring no modules next to a binary counts as a binary (the
# swarmyd shape).
fixture 'pub struct Token(String);' bin <<'EOF'
[dependencies]
anyhow.workspace = true
EOF
check "module-less lib with a binary passes" 0

# A single-file library with no binary is still a library.
fixture 'pub struct Token(String);' <<'EOF'
[dependencies]
anyhow.workspace = true
EOF
check "single-file library with anyhow fails" 1

# The table form of the dependency is caught too.
fixture 'mod worker;' <<'EOF'
[dependencies.anyhow]
workspace = true
EOF
check "table-form anyhow fails" 1

# A clean library passes.
fixture 'mod worker;' <<'EOF'
[dependencies]
thiserror.workspace = true
EOF
check "library without anyhow passes" 0

# Target-specific dependency sections are checked too.
fixture 'mod worker;' <<'EOF'
[target.'cfg(unix)'.dependencies]
anyhow.workspace = true
EOF
check "target-specific anyhow fails" 1

# A renamed dependency still pulls in anyhow: the inline-table form.
fixture 'mod worker;' <<'EOF'
[dependencies]
errors = { package = "anyhow", version = "1" }
EOF
check "inline renamed anyhow fails" 1

# The table form of a renamed dependency.
fixture 'mod worker;' <<'EOF'
[dependencies.errors]
package = "anyhow"
version = "1"
EOF
check "table-form renamed anyhow fails" 1

# A rename declared once in [workspace.dependencies] and inherited.
cat >"$work/Cargo.toml" <<'EOF'
[workspace.dependencies]
errors = { package = "anyhow", version = "1" }
EOF
fixture 'mod worker;' <<'EOF'
[dependencies]
errors.workspace = true
EOF
check "workspace-renamed anyhow fails" 1
rm "$work/Cargo.toml"

# The real workspace passes after the anyhow migrations.
if ! bash "$repo_dir/scripts/check-anyhow-in-libraries.sh" >"$work/out.log" 2>&1; then
    printf 'check-anyhow-in-libraries test failed for the real workspace\n'
    cat "$work/out.log"
    exit 1
fi
printf 'check-anyhow-in-libraries: ok\n'
