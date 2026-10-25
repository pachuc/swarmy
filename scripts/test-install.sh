#!/usr/bin/env bash
# Fake file:// releases exercise platform mapping, versions, checksums, and piped execution.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
installer="$repo_dir/scripts/install.sh"
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
fail() { printf 'test-install: %s\n' "$*" >&2; exit 1; }
targets=(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-apple-darwin aarch64-apple-darwin)
release() {
    local dir=$1 version=$2 target
    mkdir -p "$dir"
    for target in "${targets[@]}"; do
        rm -rf "$work/stage"
        mkdir -p "$work/stage/swarmy-$target"
        printf '#!/bin/sh\necho "swarmy %s %s"\n' "$version" "$target" >"$work/stage/swarmy-$target/swarmy"
        chmod 0755 "$work/stage/swarmy-$target/swarmy"
        tar -C "$work/stage" -czf "$dir/swarmy-$target.tar.gz" "swarmy-$target"
    done
    (cd "$dir" && sha256sum -- *.tar.gz >SHA256SUMS)
}
release "$work/good/latest/download" 9.9.9
release "$work/good/download/v9.9.8" 9.9.8
release "$work/tampered/latest/download" 9.9.9
printf x >>"$work/tampered/latest/download/swarmy-x86_64-unknown-linux-gnu.tar.gz"
release "$work/missing/latest/download" 9.9.9
sed -i '/x86_64-unknown-linux-gnu/d' "$work/missing/latest/download/SHA256SUMS"
shells=(sh)
command -v dash >/dev/null && shells+=(dash)
command -v busybox >/dev/null && shells+=("busybox sh")
run() {
    # shellcheck disable=SC2086 # shell may be "busybox sh"
    env -i HOME="$work/home" PATH="${path_extra:+$path_extra:}/usr/bin:/bin" \
        SWARMY_INSTALL_BASE_URL="file://$work/${base:-good}" \
        SWARMY_INSTALL_OS="${os:-Linux}" SWARMY_INSTALL_ARCH="${arch:-x86_64}" \
        ${shell:-sh} "$installer" "$@"
}
expect_failure() {
    local message=$1; shift
    if "$@" >"$work/out" 2>"$work/err"; then fail "expected failure: $*"; fi
    grep -qF -- "$message" "$work/err" || fail "missing '$message' in: $(cat "$work/err")"
}
for shell in "${shells[@]}"; do
    rm -rf "${work:?}/home"
    run >"$work/out"
    test -f "$work/home/.local/bin/swarmy" || fail "no binary for $shell"
    test "$(stat -c %a "$work/home/.local/bin/swarmy")" = 755 || fail 'bad mode'
    test "$("$work/home/.local/bin/swarmy")" = 'swarmy 9.9.9 x86_64-unknown-linux-gnu' || fail 'bad binary'
    # shellcheck disable=SC2016 # The PATH in the output is intentionally literal.
    grep -qF 'export PATH="'"$work"'/home/.local/bin:$PATH"' "$work/out" || fail 'missing PATH hint'
done
shell=sh
path_extra="$work/home/.local/bin" run >"$work/out"
! grep -qF 'export PATH' "$work/out" || fail 'unneeded PATH hint'
test "$("$work/home/.local/bin/swarmy")" = 'swarmy 9.9.9 x86_64-unknown-linux-gnu' || fail 'overwritten binary'
rm -rf "${work:?}/home"
os=Darwin arch=arm64 run >"$work/out"
test "$("$work/home/.local/bin/swarmy")" = 'swarmy 9.9.9 aarch64-apple-darwin' || fail 'macOS mapping'
rm -rf "${work:?}/home"
arch=aarch64 run >"$work/out"
test "$("$work/home/.local/bin/swarmy")" = 'swarmy 9.9.9 aarch64-unknown-linux-gnu' || fail 'Linux mapping'
rm -rf "${work:?}/home"
run --version v9.9.8 --prefix "$work/custom" >"$work/out"
test "$("$work/custom/bin/swarmy")" = 'swarmy 9.9.8 x86_64-unknown-linux-gnu' || fail 'pinned binary'
test ! -e "$work/home/.local/bin/swarmy" || fail 'unexpected default install'
run --version=9.9.8 --prefix="$work/custom2" >"$work/out"
test "$("$work/custom2/bin/swarmy")" = 'swarmy 9.9.8 x86_64-unknown-linux-gnu' || fail 'equals options'
rm -rf "${work:?}/home"
base=tampered expect_failure 'checksum mismatch' run
test ! -e "$work/home/.local/bin/swarmy" || fail 'installed tampered binary'
base=missing expect_failure 'no entry for swarmy-x86_64-unknown-linux-gnu.tar.gz' run
os=FreeBSD expect_failure 'unsupported operating system: FreeBSD' run
arch=riscv64 expect_failure 'unsupported architecture: riscv64' run
os=MINGW64_NT expect_failure 'unsupported operating system' run
expect_failure 'unknown argument: --bogus' run --bogus
expect_failure 'invalid version' run --version ../x
expect_failure 'download failed' run --version v0.0.1
rm -rf "${work:?}/home"
env -i HOME="$work/home" PATH=/usr/bin:/bin \
    SWARMY_INSTALL_BASE_URL="file://$work/good" SWARMY_INSTALL_OS=Linux SWARMY_INSTALL_ARCH=x86_64 \
    sh -s -- --prefix "$work/piped" <"$installer" >"$work/out"
test -f "$work/piped/bin/swarmy" || fail 'piped install missing'
printf 'test-install: ok\n'
