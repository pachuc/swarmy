#!/usr/bin/env bash
# Fail when docs/openapi.json breaks the compatibility policy in docs/api.md.
#
# The working tree document is diffed against the merge base with the given
# base revision (default: origin/master), so a push to master compares master
# with itself and trivially passes; pull requests are where the check bites.
# Breaking means the OpenAPI diff tool reports an error against the base: a
# removed path or operation, a removed request or response field, or a changed
# field type. Additive changes pass: new paths, new optional fields, new
# response enum values, and new event variants, which clients must ignore.
# Deprecations carry an x-sunset date at least sixty days out; removals pass
# only after their sunset date.
# Usage: scripts/check-openapi-compat.sh [base-ref] (default: origin/master).
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_dir"

base_ref="${1:-origin/master}"
new_spec="docs/openapi.json"
oasdiff_version="1.32.1"
oasdiff_sha256="7c8939fc49b75ee11fec66a5b83b37a2fca6aee109fed85013b1ba2ac2a1ee7f"

if [ ! -f "$new_spec" ]; then
    echo "check-openapi-compat: $new_spec is missing; regenerate it from swarmy-api-types" >&2
    exit 1
fi

base_commit="$(git merge-base HEAD "$base_ref" 2>/dev/null || echo "$base_ref")"
if ! base_tree="$(git rev-parse "$base_commit^{commit}" 2>/dev/null)"; then
    echo "check-openapi-compat: base ref $base_ref is unavailable" >&2
    if [ -n "${CI:-}" ]; then
        exit 1
    fi
    echo "check-openapi-compat: skipping"
    exit 0
fi
if ! git cat-file -e "$base_tree:$new_spec" 2>/dev/null; then
    echo "check-openapi-compat: $new_spec is new in $base_tree; nothing to compare"
    exit 0
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
git show "$base_tree:$new_spec" >"$work/base.json"

# The policy treats any removal of an unannounced field as breaking, so
# removals that oasdiff grades below error are escalated to it. Changed types
# and unannounced removals are already errors. New response enum values and
# event variants stay informational so additive growth passes, and removals
# after their x-sunset date pass through --deprecation-days-stable below.
cat >"$work/severity-levels.txt" <<EOF
request-parameter-removed ERR
request-property-removed ERR
response-optional-property-removed ERR
response-property-enum-value-added INFO
response-property-one-of-added INFO
EOF

oasdiff_bin="${OASDIFF_BIN:-}"
if [ -z "$oasdiff_bin" ] && command -v oasdiff >/dev/null 2>&1; then
    oasdiff_bin="oasdiff"
fi
if [ -z "$oasdiff_bin" ]; then
    arch="$(uname -m)"
    case "$arch" in
        x86_64) arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        *) echo "check-openapi-compat: unsupported architecture $arch" >&2; exit 1 ;;
    esac
    tarball="oasdiff_${oasdiff_version}_linux_${arch}.tar.gz"
    url="https://github.com/oasdiff/oasdiff/releases/download/v${oasdiff_version}/${tarball}"
    curl --retry 3 -fsSL "$url" -o "$work/$tarball"
    if [ "$arch" = "amd64" ]; then
        echo "$oasdiff_sha256  $work/$tarball" | sha256sum -c -
    fi
    tar -xzf "$work/$tarball" -C "$work"
    oasdiff_bin="$work/oasdiff"
fi

if "$oasdiff_bin" breaking --severity-levels "$work/severity-levels.txt" \
    --fail-on ERR --deprecation-days-stable 60 "$work/base.json" "$new_spec"; then
    echo "check-openapi-compat: no breaking changes against $base_tree"
else
    echo "check-openapi-compat: breaking API change detected against $base_tree" >&2
    echo "Within /v1 only additive changes are allowed; see docs/api.md." >&2
    exit 1
fi
