#!/usr/bin/env bash
set -euo pipefail
# ast-grep can share AST matchers between rules (`utils`), but not file
# globs, so the four library rules repeat the same `ignores:` list of
# binaries, entry points, and test files. This check keeps those four copies
# identical. (no-unchained-error-logs is intentionally different: logging an
# error without its cause chain is wrong in a binary too, so it also covers
# binaries and entry points.) It also keeps each exception visible: an
# `ast-grep-ignore` comment must sit on its own line, name exactly one rule,
# and follow a comment line that says why the exception applies. That
# suppression check covers every rule, including no-unchained-error-logs.
cd "$(dirname "$0")/.."
fail=0
reference=""
reference_rule=""
for rule in \
    ast-grep/rules/no-spawn-in-libraries.yml \
    ast-grep/rules/no-stringified-errors.yml \
    ast-grep/rules/no-unwrap-in-libraries.yml \
    ast-grep/rules/no-print-in-libraries.yml \
; do
    block=$(sed -n '/^ignores:$/,$p' "$rule")
    if [[ -z "$block" ]]; then
        echo "error: $rule has no ignores: list" >&2
        fail=1
    elif [[ -z "$reference" ]]; then
        reference=$block
        reference_rule=$rule
    elif [[ "$block" != "$reference" ]]; then
        echo "error: the ignores: list in $rule differs from $reference_rule; keep every copy identical" >&2
        diff <(printf '%s\n' "$reference") <(printf '%s\n' "$block") >&2 || true
        fail=1
    fi
done
if ! git ls-files -z -- 'crates/*.rs' | xargs -0 --no-run-if-empty awk '
    FNR == 1 { previous = "" }
    /ast-grep-ignore/ {
        if ($0 !~ /^[[:space:]]*\/\/ ast-grep-ignore: [a-z-]+[[:space:]]*$/) {
            print FILENAME ":" FNR ": an ast-grep-ignore comment must be its own line naming one rule" > "/dev/stderr"
            bad = 1
        } else if (previous !~ /^[[:space:]]*\/\/ /) {
            print FILENAME ":" FNR ": explain the ast-grep-ignore in a comment on the line above it" > "/dev/stderr"
            bad = 1
        }
    }
    { previous = $0 }
    END { exit bad }
'; then
    fail=1
fi
exit "$fail"
