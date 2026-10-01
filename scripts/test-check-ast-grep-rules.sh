#!/usr/bin/env bash
# Exercise scripts/check-ast-grep-rules.sh against a fixture tree: identical
# ignore lists across the four library rules pass, a drifted library copy
# fails, the fifth rule (no-unchained-error-logs) keeps its own narrower
# ignores without failing the drift check, and every ast-grep-ignore comment,
# for any rule, must name one rule and follow a comment line giving the
# reason. Failing cases require exit code 1 with the expected message, so a
# crash cannot pass as a detection.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/scripts" "$work/ast-grep/rules" "$work/crates/lib/src"
cp "$repo_dir/scripts/check-ast-grep-rules.sh" "$work/scripts/"
for rule in no-spawn-in-libraries no-stringified-errors no-unwrap-in-libraries no-print-in-libraries; do
    printf 'id: %s\nignores:\n  # Binaries.\n  - %s\n' "$rule" "'crates/bin/src/**'" \
        >"$work/ast-grep/rules/$rule.yml"
done
printf 'id: no-unchained-error-logs\nignores:\n  # Tests only.\n  - %s\n' "'**/tests/**'" \
    >"$work/ast-grep/rules/no-unchained-error-logs.yml"
printf 'fn quiet() {}\n' >"$work/crates/lib/src/lib.rs"
git -C "$work" init -q
git -C "$work" add -A

run_check() {
    if bash "$work/scripts/check-ast-grep-rules.sh" >"$work/out.log" 2>&1; then
        actual=0
    else
        actual=$?
    fi
}

check_pass() {
    run_check
    if [ "$actual" != "0" ]; then
        printf 'check-ast-grep-rules test failed for %s: want exit 0, got %s\n' "$1" "$actual"
        cat "$work/out.log"
        exit 1
    fi
}

check_fail() {
    run_check
    if [ "$actual" != "1" ] || ! grep -qF "$2" "$work/out.log"; then
        printf 'check-ast-grep-rules test failed for %s: want exit 1 with %s, got %s\n' "$1" "$2" "$actual"
        cat "$work/out.log"
        exit 1
    fi
}

check_pass "identical library ignore lists pass with a narrower fifth rule"

printf '  - %s\n' "'crates/extra/**'" >>"$work/ast-grep/rules/no-print-in-libraries.yml"
check_fail "drifted ignore list fails" "no-print-in-libraries.yml differs from"
printf 'id: no-print-in-libraries\nignores:\n  # Binaries.\n  - %s\n' "'crates/bin/src/**'" >"$work/ast-grep/rules/no-print-in-libraries.yml"

printf 'fn start() {\n    // The handle aborts the task on drop.\n    // ast-grep-ignore: no-spawn-in-libraries\n    tokio::spawn(async {});\n}\n' \
    >"$work/crates/lib/src/lib.rs"
check_pass "explained single-rule suppression passes"

printf 'fn f(error: E) {\n    // ignore_best_effort logs a generic Debug value.\n    // ast-grep-ignore: no-unchained-error-logs\n    tracing::debug!(?error, "failed");\n}\n' \
    >"$work/crates/lib/src/lib.rs"
check_pass "explained fifth-rule suppression passes"

printf 'fn start() {\n    // ast-grep-ignore\n    tokio::spawn(async {});\n}\n' >"$work/crates/lib/src/lib.rs"
check_fail "suppression without a rule fails" "must be its own line naming one rule"

printf 'fn start() {\n    tokio::spawn(async {}); // ast-grep-ignore: no-spawn-in-libraries\n}\n' \
    >"$work/crates/lib/src/lib.rs"
check_fail "trailing suppression fails" "must be its own line naming one rule"

printf 'fn start() {\n    let task = 1;\n    // ast-grep-ignore: no-spawn-in-libraries\n    tokio::spawn(async {});\n}\n' \
    >"$work/crates/lib/src/lib.rs"
check_fail "unexplained suppression fails" "explain the ast-grep-ignore"

printf 'fn f(error: E) {\n    let logged = 1;\n    // ast-grep-ignore: no-unchained-error-logs\n    tracing::debug!(?error, "failed");\n}\n' \
    >"$work/crates/lib/src/lib.rs"
check_fail "unexplained fifth-rule suppression fails" "explain the ast-grep-ignore"

printf 'check-ast-grep-rules: ok\n'
