#!/bin/sh
# Install the swarmy client from a GitHub release:
# curl -fsSL https://raw.githubusercontent.com/pachuc/swarmy/master/scripts/install.sh | sh
# Linux and macOS on x86_64 and aarch64. Checksums are verified; releases are not signed.
# Test hooks: SWARMY_INSTALL_BASE_URL, SWARMY_INSTALL_OS, SWARMY_INSTALL_ARCH.
set -eu
RELEASES_URL=https://github.com/pachuc/swarmy/releases
fail() { printf 'swarmy install: %s\n' "$*" >&2; exit 1; }
say() { printf 'swarmy install: %s\n' "$*"; }
usage() {
    cat <<'USAGE'
Usage: install.sh [--version vX.Y.Z] [--prefix DIRECTORY]

  --version  release tag to install (default: the latest release)
  --prefix   install root; swarmy goes to PREFIX/bin (default: ~/.local)
USAGE
}
detect_target() {
    os=${SWARMY_INSTALL_OS:-$(uname -s)}
    arch=${SWARMY_INSTALL_ARCH:-$(uname -m)}
    case $os in
        Linux) os_part=unknown-linux-gnu ;;
        Darwin) os_part=apple-darwin ;;
        *) fail "unsupported operating system: $os. Releases cover Linux and macOS; build from source instead (docs/DEV.md)." ;;
    esac
    case $arch in
        x86_64 | amd64) arch_part=x86_64 ;;
        aarch64 | arm64) arch_part=aarch64 ;;
        *) fail "unsupported architecture: $arch. Releases cover x86_64 and aarch64; build from source instead (docs/DEV.md)." ;;
    esac
    printf '%s-%s\n' "$arch_part" "$os_part"
}
download() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --retry 3 -o "$2" "$1"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$2" "$1"
    else
        fail 'curl or wget is required'
    fi
}
sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{ print $1 }'
    else
        shasum -a 256 "$1" | awk '{ print $1 }'
    fi
}
main() {
    version=
    prefix=${HOME:?HOME must be set}/.local
    while [ $# -gt 0 ]; do
        case $1 in
            --version) [ $# -ge 2 ] || fail '--version needs a value'; version=$2; shift 2 ;;
            --version=*) version=${1#--version=}; shift ;;
            --prefix) [ $# -ge 2 ] || fail '--prefix needs a value'; prefix=$2; shift 2 ;;
            --prefix=*) prefix=${1#--prefix=}; shift ;;
            -h | --help) usage; exit 0 ;;
            *) usage >&2; fail "unknown argument: $1" ;;
        esac
    done
    [ -n "$prefix" ] || fail '--prefix cannot be empty'
    case $version in
        '') ;;
        *[!A-Za-z0-9.+-]*) fail "invalid version: $version" ;;
        v[0-9]*) ;;
        [0-9]*) version=v$version ;;
        *) fail "invalid version: $version (expected vX.Y.Z)" ;;
    esac
    target=$(detect_target) || exit 1
    command -v tar >/dev/null 2>&1 || fail 'tar is required'
    command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 \
        || fail 'sha256sum or shasum is required'
    base=${SWARMY_INSTALL_BASE_URL:-$RELEASES_URL}
    base=${base%/}
    if [ -n "$version" ]; then url=$base/download/$version; else url=$base/latest/download; fi
    asset=swarmy-$target.tar.gz
    tmp=$(mktemp -d 2>/dev/null || mktemp -d -t swarmy-install)
    trap 'rm -rf "$tmp"' EXIT
    trap 'exit 1' HUP INT TERM
    say "downloading $asset (${version:-latest})"
    download "$url/$asset" "$tmp/$asset" || fail "download failed: $url/$asset"
    download "$url/SHA256SUMS" "$tmp/SHA256SUMS" || fail "download failed: $url/SHA256SUMS"
    expected=$(awk -v name="$asset" '$2 == name || $2 == "*" name { print $1; exit }' "$tmp/SHA256SUMS")
    [ -n "$expected" ] || fail "SHA256SUMS has no entry for $asset; not installing"
    actual=$(sha256_of "$tmp/$asset")
    [ "$actual" = "$expected" ] || fail "checksum mismatch for $asset (expected $expected, got $actual); not installing"
    tar -xzf "$tmp/$asset" -C "$tmp" || fail "cannot extract $asset"
    [ -f "$tmp/swarmy-$target/swarmy" ] || fail "$asset does not contain swarmy-$target/swarmy"
    mkdir -p "$prefix/bin" || fail "cannot create $prefix/bin"
    bindir=$(cd "$prefix/bin" && pwd)
    staged=$bindir/.swarmy.install.$$
    { cp "$tmp/swarmy-$target/swarmy" "$staged" && chmod 0755 "$staged" && mv -f "$staged" "$bindir/swarmy"; } \
        || { rm -f "$staged"; fail "cannot install into $bindir"; }
    "$bindir/swarmy" --version || fail "installed $bindir/swarmy but it does not run on this machine"
    say "installed $bindir/swarmy"
    case :$PATH: in
        *:"$bindir":*) ;;
        *)
            printf '\n%s is not on your PATH. Add this line to your shell profile:\n\n' "$bindir"
            # shellcheck disable=SC2016 # Print $PATH literally for the user's profile.
            printf '    export PATH="%s:$PATH"\n\n' "$bindir"
            ;;
    esac
}
main "$@"
