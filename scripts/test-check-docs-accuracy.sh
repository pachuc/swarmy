#!/usr/bin/env bash
# Exercise scripts/check-docs-accuracy.sh against a fixture repository tree,
# plus the real workspace, so the assertions run the same flags as CI.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/scripts" "$work/docs" "$work/crates/swarmy-cli/src" \
    "$work/crates/swarmy-cloud/src" "$work/crates/swarmyd/src" \
    "$work/crates/swarmy-fake/tests"
cp "$repo_dir/scripts/check-docs-accuracy.sh" "$work/scripts/"

cat >"$work/crates/swarmy-cli/src/main.rs" <<'EOF'
mod nested;
mod widget_command;

#[derive(Parser)]
#[command(name = "swarmy")]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Widget {
        #[command(subcommand)]
        command: widget_command::Command,
    },
    Nested {
        #[command(subcommand)]
        command: nested::Command,
    },
    Remote {
        #[command(subcommand)]
        command: swarmy_cloud::Command,
    },
    Doctor,
}
EOF

cat >"$work/crates/swarmy-cli/src/widget_command.rs" <<'EOF'
#[derive(Subcommand)]
pub(crate) enum Command {
    /// List widgets
    #[command(alias = "list")]
    Ls,
    /// Show one widget
    Show { name: String },
    /// Add a node, exercising kebab-case spellings
    AddNode { name: String },
}
EOF

cat >"$work/crates/swarmy-cli/src/nested.rs" <<'EOF'
#[derive(Subcommand)]
pub(crate) enum Command {
    Deep {
        #[command(subcommand)]
        command: DeepCommand,
    },
}

#[derive(Subcommand)]
pub(crate) enum DeepCommand {
    Run,
}
EOF

cat >"$work/crates/swarmy-cloud/src/command.rs" <<'EOF'
#[derive(Subcommand)]
pub enum Command {
    Up { name: String },
    Ls,
}
EOF

cat >"$work/crates/swarmyd/src/vol_command.rs" <<'EOF'
pub(crate) enum Command {
    Attach { volume: String },
    Ls,
}
EOF

printf '// fixture target\n' >"$work/crates/swarmy-fake/tests/target.rs"
cat >"$work/crates/swarmy-fake/Cargo.toml" <<'EOF'
[package]
name = "swarmy-fake"
version = "0.0.0"
edition = "2021"
EOF

printf '/.local/\n' >"$work/.gitignore"
printf '# fixture root doc\n' >"$work/README.md"
printf '# fixture other doc\n' >"$work/other.md"

# Always-valid references: real paths and commands, proposals from the
# allowlist, runtime-ignored paths, and prose the extractor must skip.
cat >"$work/docs/good.md" <<'EOF'
# Fixture

Root path `crates/swarmy-cli/src/main.rs`, sibling path `../README.md`,
other doc `../other.md`, and a runtime path `.local/env` that git ignores.

`swarmy widget ls`, `swarmy widget list`, `swarmy widget add-node NAME`,
`swarmy widget show w1 --json`, `swarmy --json widget ls`,
`swarmy nested deep run`, `swarmy remote up NAME`, `swarmy doctor`,
`swarmyd vol attach`, and `swarmyd vol ls` all exist.

Proposals `swarmy agent fork SOURCE NEW`, `swarmy channel x`, and
`swarmy dm AGENT` are allowlisted, not implemented.

Placeholders `PROVIDER/LABEL`, `bucket/key`, and `openai/model-x`, the URL
`https://example.com/a/b`, the glob `crates/*/Cargo.toml`, and the table
row below are not repository paths or commands:

| swarmy | widget |

```sh
sudo -E swarmy widget ls
./target/debug/swarmy widget show w1
swarmy-fake --test target
cargo test -p swarmy-fake --test target
swarmy-chaos --continuity
Hello from swarmy!
swarmyd::{Alpha, Beta}
```
EOF

git -C "$work" init -q
git -C "$work" -c user.email=docs@test -c user.name=docs add -A
git -C "$work" -c user.email=docs@test -c user.name=docs commit -qm base

check() {
    local name="$1" expected="$2"
    if bash "$work/scripts/check-docs-accuracy.sh" >"$work/out.log" 2>&1; then
        actual=0
    else
        actual=1
    fi
    if [ "$actual" != "$expected" ]; then
        printf 'check-docs-accuracy test failed for %s: want exit %s\n' "$name" "$expected"
        cat "$work/out.log"
        exit 1
    fi
}

add_case() {
    printf '%s\n' "$2" >"$work/docs/case.md"
    git -C "$work" -c user.email=docs@test -c user.name=docs add docs/case.md
}

# The untouched fixture passes.
check "valid fixture" 0

add_case x '`swarmy widget bogus` names no subcommand.'
check "unknown subcommand fails" 1

add_case x '`swarmy nested deep bogus` names no nested subcommand.'
check "unknown nested subcommand fails" 1

add_case x 'There is no `swarmy nosuch` command.'
check "unknown top-level command fails" 1

add_case x 'See `crates/swarmy-cli/src/missing.rs` for details.'
check "unknown path fails" 1

add_case x 'Run `swarmy-fake --test missing` as root.'
check "unknown test target fails" 1

add_case x 'Run `cargo test -p swarmy-fake --test missing` as root.'
check "unknown cargo test target fails" 1

add_case x 'Run `swarmyd vol bogus` on the node.'
check "unknown volume subcommand fails" 1

# Back to green after removing the bad case file.
rm "$work/docs/case.md"
git -C "$work" rm -q docs/case.md
check "fixture green again" 0

# The real workspace passes after the documentation fixes.
if ! bash "$repo_dir/scripts/check-docs-accuracy.sh" >"$work/out.log" 2>&1; then
    printf 'check-docs-accuracy test failed for the real workspace\n'
    cat "$work/out.log"
    exit 1
fi
printf 'check-docs-accuracy: ok\n'
