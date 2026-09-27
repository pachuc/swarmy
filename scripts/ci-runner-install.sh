#!/usr/bin/env bash
set -euo pipefail
# Run from a trusted checkout on a dedicated Ubuntu 24.04 machine, as root.
if (( EUID != 0 )) || (( $# != 2 )) || [[ ! $1 =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || [[ -z $2 ]]; then
    echo 'Usage: sudo scripts/ci-runner-install.sh OWNER/REPO REGISTRATION_TOKEN' >&2
    exit 2
fi
repo=$1
token=$2
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
id ci >/dev/null 2>&1 || useradd --create-home --shell /bin/bash ci
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y curl tar git sudo build-essential pkg-config libssl-dev clang libclang-dev python3 libfuse2t64 rsync runc passt iproute2 util-linux e2fsprogs debootstrap
# NBD device nodes must exist before root acceptance starts after a reboot.
printf 'nbd\n' > /etc/modules-load.d/swarmy-ci.conf
printf 'options nbd nbds_max=32 max_part=8\n' > /etc/modprobe.d/swarmy-ci.conf
if ! modprobe nbd nbds_max=32 max_part=8; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y "linux-modules-extra-$(uname -r)"
    modprobe nbd nbds_max=32 max_part=8
fi
install -d -o ci -g ci /var/lib/swarmy-ci /var/lib/swarmy-ci/target /var/lib/swarmy-ci/runner
# Install dependencies in the service user's home, never in a transient checkout.
runuser -u ci -- env HOME=/home/ci bash "$root/scripts/install-dev-tools.sh"
install -m 0644 /home/ci/.local/lib/libfdb_c.so /usr/local/lib/libfdb_c.so
ldconfig
if [[ ! -x /home/ci/.cargo/bin/rustup ]]; then
    runuser -u ci -- bash -c 'curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal'
fi
install -m 0644 "$root/rust-toolchain.toml" /home/ci/rust-toolchain.toml
runuser -u ci -- env HOME=/home/ci PATH=/home/ci/.cargo/bin:$PATH bash -c 'cd /home/ci && rustup show'
arch=$(uname -m)
[[ $arch == x86_64 ]] || { echo 'Only x86_64 is supported' >&2; exit 1; }
runner=/var/lib/swarmy-ci/runner
if [[ ! -f $runner/.runner ]]; then
    version=2.328.0
    archive="actions-runner-linux-x64-${version}.tar.gz"
    curl -fsSL "https://github.com/actions/runner/releases/download/v${version}/${archive}" -o "/tmp/${archive}"
    tar -xzf "/tmp/${archive}" -C "$runner"
    rm -f "/tmp/${archive}"
    chown -R ci:ci "$runner"
    runuser -u ci -- bash -c 'cd /var/lib/swarmy-ci/runner && ./config.sh --unattended --url "$1" --token "$2" --name "$(hostname)-swarmy-ci" --labels swarmy-ci --work /var/lib/swarmy-ci/work --replace' _ "https://github.com/$repo" "$token"
fi
# This root-owned gate accepts only known test binaries in the runner target.
cat > /usr/local/sbin/swarmy-ci-root <<'GATE'
#!/usr/bin/env bash
set -euo pipefail
(( $# >= 1 )) || exit 2
binary=$(realpath -e -- "$1")
case "$binary" in
    /var/lib/swarmy-ci/target/debug/swarmy)
        [[ ${2:-} == image && ${3:-} == build ]] || exit 2 ;;
    /var/lib/swarmy-ci/target/debug/deps/*)
        name=${binary##*/}
        [[ $name =~ ^(node|nbd|image|vol|bash|continuity|coding)-[a-f0-9]+$ ]] || exit 2 ;;
    *) exit 2 ;;
esac
shift
exec "$binary" "$@"
GATE
chmod 0755 /usr/local/sbin/swarmy-ci-root
printf 'ci ALL=(root) NOPASSWD:SETENV: /usr/local/sbin/swarmy-ci-root *\n' > /etc/sudoers.d/swarmy-ci
chmod 0440 /etc/sudoers.d/swarmy-ci
visudo -cf /etc/sudoers.d/swarmy-ci
cat > /etc/systemd/system/swarmy-ci-runner.service <<'UNIT'
[Unit]
Description=Swarmy GitHub Actions runner
After=network-online.target
Wants=network-online.target
[Service]
User=ci
WorkingDirectory=/var/lib/swarmy-ci/runner
Environment=HOME=/home/ci
Environment=PATH=/home/ci/.cargo/bin:/home/ci/.local/bin:/usr/local/bin:/usr/bin:/bin
Environment=CARGO_TARGET_DIR=/var/lib/swarmy-ci/target
ExecStart=/var/lib/swarmy-ci/runner/run.sh
Restart=always
[Install]
WantedBy=multi-user.target
UNIT
cat > /etc/systemd/system/swarmy-ci-clean.service <<'UNIT'
[Unit]
Description=Clean swarmy CI Rust build cache
ConditionPathExists=/var/lib/swarmy-ci/work/swarmy/swarmy/Cargo.toml
[Service]
Type=oneshot
User=ci
WorkingDirectory=/var/lib/swarmy-ci
Environment=HOME=/home/ci
Environment=PATH=/home/ci/.cargo/bin:/usr/local/bin:/usr/bin:/bin
Environment=CARGO_TARGET_DIR=/var/lib/swarmy-ci/target
ExecStart=/home/ci/.cargo/bin/cargo clean --manifest-path /var/lib/swarmy-ci/work/swarmy/swarmy/Cargo.toml
UNIT
cat > /etc/systemd/system/swarmy-ci-clean.timer <<'UNIT'
[Unit]
Description=Weekly swarmy CI Rust cache cleanup
[Timer]
OnCalendar=Sun *-*-* 03:00:00
Persistent=true
[Install]
WantedBy=timers.target
UNIT
systemctl daemon-reload
systemctl enable --now swarmy-ci-runner.service swarmy-ci-clean.timer
