#!/usr/bin/env bash
# Build, package, and checksum release archives. The workflow and local dry run
# use this same script through the Makefile.
set -euo pipefail
fail() { printf 'release: %s\n' "$*" >&2; exit 1; }
usage() { fail 'usage: scripts/release.sh version | package client|core|image TARGET DIR | checksums DIR'; }
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo"
CARGO=${CARGO:-cargo}
# Keep aligned with scripts/install-dev-tools.sh and .github/actions/install-dev-services.
FDB_VERSION=7.3.79
work=
trap 'rm -rf -- "${work:-}"' EXIT

version() {
    local value
    value=$(sed -n 's/^version = "\([^"]*\)"$/\1/p' Cargo.toml | head -n 1)
    [ -n "$value" ] || fail 'workspace version not found'
    printf '%s\n' "$value"
}

release_dir() {
    local target_dir=${CARGO_TARGET_DIR:-target}
    mkdir -p "$target_dir"
    target_dir=$(cd "$target_dir" && pwd)
    printf '%s/%s/release\n' "$target_dir" "$1"
}

fetch_fdb() {
    local arch asset
    case "$1" in
        x86_64-unknown-linux-gnu) arch=x86_64 ;;
        aarch64-unknown-linux-gnu) arch=aarch64 ;;
        *) fail 'swarmy-core is Linux only' ;;
    esac
    mkdir -p "$2"
    cd "$2"
    asset="libfdb_c.${arch}.so"
    curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${asset}"
    curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${asset}.sha256"
    sha256sum -c "${asset}.sha256"
    install -m 0755 "$asset" libfdb_c.so
    cd "$repo"
}

package() {
    local component=$1 target=$2 dir=$3 name stage host fdb_dir
    mkdir -p "$dir"
    dir=$(cd "$dir" && pwd)
    work=$(mktemp -d)
    export CARGO_PROFILE_RELEASE_STRIP=symbols
    export SWARMY_RELEASE_BUILD=1
    case "$component" in
        client)
            name=swarmy
            stage="$work/$name-$target"
            mkdir -p "$stage"
            env -u SWARMY_FDB_LIB_DIR "$CARGO" build --release --locked --target "$target" -p swarmy-cli --features remote --bin swarmy
            install -m 0755 "$(release_dir "$target")/swarmy" "$stage/swarmy"
            if [[ $target == *-linux-gnu ]] && ldd "$stage/swarmy" | grep -q libfdb_c; then
                fail 'client must not link libfdb_c'
            fi
            ;;
        core)
            : "${CORE_BIN_PACKAGE:?CORE_BIN_PACKAGE must name the package with the swarmy-core binary}"
            name=swarmy-core
            stage="$work/$name-$target"
            mkdir -p "$stage"
            # foundationdb-sys keeps the first FDB_CLIENT_LIB_PATH in its cached
            # build output and never reruns when it changes, so the library
            # lives at one stable path per target, never a temporary directory.
            fdb_dir="$(release_dir "$target")/fdb"
            fetch_fdb "$target" "$fdb_dir"
            env -u SWARMY_FDB_LIB_DIR FDB_CLIENT_LIB_PATH="$fdb_dir" "$CARGO" build --release --locked --target "$target" -p "$CORE_BIN_PACKAGE" --bin swarmy-core
            install -m 0755 "$(release_dir "$target")/swarmy-core" "$stage/swarmy-core"
            env -u SWARMY_FDB_LIB_DIR FDB_CLIENT_LIB_PATH="$fdb_dir" "$CARGO" build --release --locked --target "$target" -p swarmy-provisioner --bin swarmy-provisioner
            install -m 0755 "$(release_dir "$target")/swarmy-provisioner" "$stage/swarmy-provisioner"
            install -m 0644 "$fdb_dir/libfdb_c.so" "$stage/libfdb_c.so"
            patchelf --set-rpath '$ORIGIN' "$stage/swarmy-core"
            [ "$(patchelf --print-rpath "$stage/swarmy-core")" = '$ORIGIN' ] || fail 'core rpath is not $ORIGIN'
            readelf -d "$stage/swarmy-core" | grep -q 'NEEDED.*\[libfdb_c\.so\]' || fail 'core does not need libfdb_c.so'
            patchelf --set-rpath '$ORIGIN' "$stage/swarmy-provisioner"
            [ "$(patchelf --print-rpath "$stage/swarmy-provisioner")" = '$ORIGIN' ] || fail 'provisioner rpath is not $ORIGIN'
            readelf -d "$stage/swarmy-provisioner" | grep -q 'NEEDED.*\[libfdb_c\.so\]' || fail 'provisioner does not need libfdb_c.so'
            ;;
        image)
            # Build natively with sudo and debootstrap; see images/README.md.
            [ "$target" = x86_64-unknown-linux-gnu ] || fail 'the base image is published for x86_64-unknown-linux-gnu only'
            [ "$target" = "$(rustc -vV | sed -n 's/^host: //p')" ] || fail 'build the base image on a native runner'
            name=swarmy-image-base-ubuntu
            stage="$work/$name-$target"
            mkdir -p "$stage"
            env -u SWARMY_FDB_LIB_DIR "$CARGO" build --locked -p swarmy-cli --bin swarmy
            "$(cd "${CARGO_TARGET_DIR:-target}" && pwd)/debug/swarmy" image build images/base-ubuntu --output "$stage/disk.ext4" --no-register
            install -m 0644 images/base-ubuntu/recipe.toml "$stage/recipe.toml"
            debugfs -R 'stat /usr/local/libexec/swarmy-github' "$stage/disk.ext4" 2>/dev/null | grep -q 'Type: regular' || fail 'image lacks the agent setup'
            [ "$(du -B1 "$stage/disk.ext4" | cut -f1)" -lt "$(stat -c %s "$stage/disk.ext4")" ] || fail 'image is not sparse'
            ;;
        *) usage ;;
    esac
    host=$(rustc -vV | sed -n 's/^host: //p')
    if [ "$target" = "$host" ]; then
        if [ "$component" = client ]; then
            "$stage/swarmy" --version
            [ "$("$stage/swarmy" version --release)" = "v$(version)" ] || fail 'release tag mismatch'
            "$stage/swarmy" --json version --release | grep -q '"release_build":true' || fail 'client is not marked as a release build'
            "$stage/swarmy" version --release
        elif [ "$component" = core ]; then
            ldd "$stage/swarmy-core" | grep -F "libfdb_c.so => $stage/libfdb_c.so" || fail 'bundled libfdb_c.so not loaded'
            "$stage/swarmy-core" api --version
            "$stage/swarmy-provisioner" --version
        fi
    fi
    if [ "$component" = image ]; then
        tar -C "$work" --sparse -czf "$dir/$name-$target.tar.gz" "$name-$target"
        [ "$(stat -c %s "$dir/$name-$target.tar.gz")" -lt 1900000000 ] || fail 'image archive exceeds the GitHub asset limit'
    else
        tar -C "$work" -czf "$dir/$name-$target.tar.gz" "$name-$target"
    fi
    printf '%s\n' "$dir/$name-$target.tar.gz"
}

checksums() {
    cd "$1"
    rm -f SHA256SUMS
    sha256sum -- *.tar.gz > SHA256SUMS
    sha256sum -c SHA256SUMS
}

case "${1:-}" in
    version) [ "$#" = 1 ] || usage; version ;;
    package) [ "$#" = 4 ] || usage; package "$2" "$3" "$4" ;;
    checksums) [ "$#" = 2 ] || usage; checksums "$2" ;;
    *) usage ;;
esac
