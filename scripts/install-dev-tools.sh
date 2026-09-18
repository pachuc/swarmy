#!/usr/bin/env bash
set -euo pipefail

fail() { printf '%s\n' "$*" >&2; exit 1; }
prefix=${HOME:?HOME must be set}/.local
case ${1:-} in
    '') [[ $# == 0 ]] || fail 'Usage: scripts/install-dev-tools.sh [--prefix DIRECTORY]' ;;
    --prefix) [[ $# == 2 && -n $2 ]] || fail 'Usage: scripts/install-dev-tools.sh [--prefix DIRECTORY]'; prefix=$2 ;;
    *) fail 'Usage: scripts/install-dev-tools.sh [--prefix DIRECTORY]' ;;
esac
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || fail 'This installer supports Linux x86-64. Install the backing tools for your platform manually.'
for command in curl tar sha256sum install mktemp; do
    command -v "$command" >/dev/null || fail "Missing prerequisite: $command"
done
mkdir -p -- "$prefix"
prefix=$(cd -- "$prefix" && pwd)
[[ $prefix != *[,:]* && $prefix != *$'\n'* && $prefix != *$'\r'* ]] || fail 'The prefix cannot contain linker separators (comma, colon, or newline).'
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
install_dir=$(mktemp -d)
trap 'rm -rf -- "$install_dir"' EXIT
cd -- "$install_dir"

# Keep these versions aligned with .daytona/Dockerfile and scripts/dev-stack.sh.
FDB_VERSION=7.3.79
NATS_VERSION=2.14.6
SEAWEEDFS_VERSION=4.47
for asset in fdbserver.x86_64 fdbcli.x86_64 libfdb_c.x86_64.so; do
    curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${asset}"
    curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${asset}.sha256"
    sha256sum -c "${asset}.sha256"
done
curl --retry 3 -fsSL "https://github.com/nats-io/nats-server/releases/download/v${NATS_VERSION}/nats-server-v${NATS_VERSION}-linux-amd64.tar.gz" -o nats.tar.gz
tar -xzf nats.tar.gz
curl --retry 3 -fsSL "https://github.com/seaweedfs/seaweedfs/releases/download/${SEAWEEDFS_VERSION}/linux_amd64.tar.gz" -o seaweed.tar.gz
tar -xzf seaweed.tar.gz
install -d -- "$prefix/bin" "$prefix/lib"
install -m 0755 fdbserver.x86_64 "$prefix/bin/fdbserver"
install -m 0755 fdbcli.x86_64 "$prefix/bin/fdbcli"
install -m 0755 libfdb_c.x86_64.so "$prefix/lib/libfdb_c.so"
install -m 0755 "nats-server-v${NATS_VERSION}-linux-amd64/nats-server" "$prefix/bin/nats-server"
install -m 0755 weed "$prefix/bin/weed"
printf '\nInstalled backing tools to %s/bin and the client library to %s/lib.\n' "$prefix" "$prefix"
printf 'Run this from the checkout to install swarmy and its services:\n\n'
if [ "$prefix" = "$HOME/.local" ]; then
    printf '    make install\n'
else
    printf '    make install SWARMY_FDB_LIB_DIR=%q\n' "$prefix/lib"
fi
printf '\nswarmy finds these tools automatically; no PATH or LD_LIBRARY_PATH exports are needed.\n'
printf 'Keep this checkout for swarmy dev up, which uses scripts/dev-stack.sh.\n'
