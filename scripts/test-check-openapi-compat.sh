#!/usr/bin/env bash
# Exercise scripts/check-openapi-compat.sh against fixture revisions in a
# throwaway git repository, so the assertions run the same flags as CI.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
fixtures="$repo_dir/scripts/fixtures/openapi-compat"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/scripts" "$work/docs"
cp "$repo_dir/scripts/check-openapi-compat.sh" "$work/scripts/"
cp "$fixtures/base.json" "$work/docs/openapi.json"
git -C "$work" init -q
git -C "$work" -c user.email=compat@test -c user.name=compat add docs/openapi.json
git -C "$work" -c user.email=compat@test -c user.name=compat commit -qm base

check() {
    local revision="$1" expected="$2"
    cp "$fixtures/$revision" "$work/docs/openapi.json"
    if bash "$work/scripts/check-openapi-compat.sh" HEAD >"$work/out.log" 2>&1; then
        actual=0
    else
        actual=1
    fi
    if [ "$actual" != "$expected" ]; then
        printf 'check-openapi-compat test failed for %s: want exit %s\n' "$revision" "$expected"
        cat "$work/out.log"
        exit 1
    fi
}

check base.json 0
check removed-field.json 1
check renamed-route.json 1
check added-optional-field.json 0
check added-enum-value.json 0
printf 'check-openapi-compat: ok\n'
