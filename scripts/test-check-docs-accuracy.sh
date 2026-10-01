#!/usr/bin/env bash
# Exercise scripts/check-docs-accuracy.py against a fixture repository tree.
# The fixture covers repository paths, Markdown link targets and anchors, and
# --test targets; command names and flags belong to the clap-tree tests in
# swarmy-cli, swarmyd, and swarmy-chaos (through swarmy-testkit's
# check_docs_commands) against the real command trees. Failing cases
# require exit code 1 with the expected message, so an
# internal crash (exit 2) cannot pass as a detected problem.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/scripts" "$work/docs" "$work/backlog" \
    "$work/crates/swarmy-fake/tests"
cp "$repo_dir/scripts/check-docs-accuracy.py" "$work/scripts/"

printf '// fixture target\n' >"$work/crates/swarmy-fake/tests/target.rs"
cat >"$work/crates/swarmy-fake/Cargo.toml" <<'EOF'
[package]
name = "swarmy-fake"
version = "0.0.0"
edition = "2021"
EOF

printf '/.local/\n' >"$work/.gitignore"
printf '# fixture root doc\n' >"$work/README.md"
cat >"$work/other.md" <<'EOF'
# fixture other doc

## Setup

## Setup

```sh
# not a heading
```
EOF

# Always-valid references: real paths, real test targets, runtime-ignored
# paths, and prose the extractor must skip. Commands are inert text to this
# check; the backlog proposal names files that do not exist yet.
cat >"$work/docs/good.md" <<'EOF'
# Fixture

Root path `crates/swarmy-fake/tests/target.rs`, sibling path `../README.md`,
other doc `../other.md`, and a runtime path `.local/env` that git ignores.

`swarmy-fake --test target` and `cargo test -p swarmy-fake --test target`
name a real test target. `swarmy widget ls` is a command, not a path.

Placeholders `PROVIDER/LABEL`, `bucket/key`, and `openai/model-x`, the URL
`https://example.com/a/b`, the glob `crates/*/Cargo.toml`, and the table
row below are not repository paths or test targets:

| swarmy | widget |

Links: [root](../README.md), [other](../other.md#fixture-other-doc), the
repeated heading [second setup](../other.md#setup-1), this page's
[top](#fixture), a [directory](../crates/swarmy-fake), an
[external page](https://example.com/missing.md#nowhere), and the code span
`[not a link](missing.md)`.
EOF

cat >"$work/backlog/future.md" <<'EOF'
# Proposal

`future/commands.md` does not exist yet and `swarmy agent fork SOURCE NEW`
is unimplemented; proposals are never checked.
EOF

git -C "$work" init -q
git -C "$work" -c user.email=docs@test -c user.name=docs add -A
git -C "$work" -c user.email=docs@test -c user.name=docs commit -qm base

run_check() {
    if python3 "$work/scripts/check-docs-accuracy.py" >"$work/out.log" 2>&1; then
        actual=0
    else
        actual=$?
    fi
}

check_pass() {
    local name="$1"
    run_check
    if [ "$actual" != "0" ]; then
        printf 'check-docs-accuracy test failed for %s: want exit 0, got %s\n' "$name" "$actual"
        cat "$work/out.log"
        exit 1
    fi
}

check_fail() {
    local name="$1" expected_msg="$2"
    run_check
    if [ "$actual" != "1" ]; then
        printf 'check-docs-accuracy test failed for %s: want exit 1, got %s\n' "$name" "$actual"
        cat "$work/out.log"
        exit 1
    fi
    if ! grep -qF "$expected_msg" "$work/out.log"; then
        printf 'check-docs-accuracy test failed for %s: missing %s\n' "$name" "$expected_msg"
        cat "$work/out.log"
        exit 1
    fi
}

add_case() {
    printf '%s\n' "$2" >"$work/docs/case.md"
    git -C "$work" -c user.email=docs@test -c user.name=docs add docs/case.md
}

# The untouched fixture passes, including the unchecked backlog proposal.
check_pass "valid fixture"

add_case x 'See `crates/swarmy-fake/tests/missing.rs` for details.'
check_fail "unknown path fails" "unknown repository path"

add_case x 'Run `swarmy-fake --test missing` as root.'
check_fail "unknown test target fails" "unknown test target"

add_case x 'Run `cargo test -p swarmy-fake --test missing` as root.'
check_fail "unknown cargo test target fails" "unknown test target"

add_case x 'See [the guide](missing.md).'
check_fail "broken link target fails" "broken link target: missing.md"

add_case x 'See [setup](../other.md#no-such-section).'
check_fail "unknown anchor fails" "unknown link anchor: ../other.md#no-such-section"

add_case x 'A shell comment in a fence is no heading: [x](../other.md#not-a-heading).'
check_fail "fenced heading is not an anchor" "unknown link anchor: ../other.md#not-a-heading"

add_case x 'This page has no [such part](#nowhere).'
check_fail "unknown same-page anchor fails" "unknown link anchor: #nowhere"

# Back to green after removing the bad case file.
rm "$work/docs/case.md"
git -C "$work" rm -q docs/case.md
check_pass "fixture green again"

printf 'check-docs-accuracy: ok\n'
