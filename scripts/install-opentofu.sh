#!/usr/bin/env bash
# Install the pinned, checksum-verified OpenTofu into PREFIX/bin.
set -euo pipefail
fail() { printf '%s\n' "$*" >&2; exit 1; }
prefix=${HOME:?HOME must be set}/.local
case ${1:-} in
    '') [[ $# == 0 ]] || fail "Usage: $0 [--prefix DIRECTORY]" ;;
    --prefix) [[ $# == 2 && -n $2 ]] || fail "Usage: $0 [--prefix DIRECTORY]"; prefix=$2 ;;
    *) fail "Usage: $0 [--prefix DIRECTORY]" ;;
esac
OPENTOFU_VERSION=1.13.2
case $(uname -s)-$(uname -m) in
    Linux-x86_64) arch=amd64 sha256=ac05d9952662c7db28dd57e93f0a68018837bfdf9342384ec0987977c16a9645 ;;
    Linux-aarch64 | Linux-arm64) arch=arm64 sha256=88e2d9e3f94f97b76ab2faf55ade87578f56aed19c5e45fa139d70e994366d1f ;;
    *) fail "This installer supports Linux x86-64 and arm64. Install OpenTofu $OPENTOFU_VERSION for your platform from https://opentofu.org/docs/intro/install/." ;;
esac
for cmd in curl tar sha256sum install mktemp; do
    command -v "$cmd" >/dev/null || fail "Missing prerequisite: $cmd"
done
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
asset="tofu_${OPENTOFU_VERSION}_linux_${arch}.tar.gz"
curl --retry 3 -fsSL "https://github.com/opentofu/opentofu/releases/download/v${OPENTOFU_VERSION}/${asset}" -o "$work/$asset"
printf '%s  %s\n' "$sha256" "$work/$asset" | sha256sum -c --quiet -
tar -xzf "$work/$asset" -C "$work" tofu
install -d -- "$prefix/bin"
install -m 0755 "$work/tofu" "$prefix/bin/tofu"
"$prefix/bin/tofu" version
