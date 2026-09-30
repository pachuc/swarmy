#!/usr/bin/env bash
# Behavioral test for scripts/remote-decommission.sh: with stubbed sudo and
# systemctl and a fake root, every unit and binary in the shared lists is
# stopped, disabled, and removed, while the fstab entry and the service
# user's home itself are left alone.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/remote-provision-env.sh

fake=$(mktemp -d)
trap 'rm -rf "$fake"' EXIT
export FAKE_ROOT=$fake
mkdir -p "$fake/bin" "$fake/etc/systemd/system" "$fake/usr/local/bin" \
    "$fake/etc/modules-load.d" "$fake/etc/swarmy" "$fake/home/decomm-swarmy/swarmy"

# Stub sudo: run the command with absolute paths redirected under the fake
# root, so the test never touches the real filesystem.
cat > "$fake/bin/sudo" <<'STUB'
#!/usr/bin/env bash
args=()
for a in "$@"; do
    if [[ $a == /* ]]; then args+=("$FAKE_ROOT$a"); else args+=("$a"); fi
done
exec "${args[@]}"
STUB
# Stub systemctl: record unit actions and succeed.
cat > "$fake/bin/systemctl" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_ROOT/systemctl.log"
exit 0
STUB
chmod +x "$fake/bin/sudo" "$fake/bin/systemctl"
export PATH="$fake/bin:$PATH"
: > "$fake/systemctl.log"

# A fully provisioned host: every shared unit and binary present, including
# the other mode's units and old node-services units.
while IFS= read -r unit; do
    touch "$fake/etc/systemd/system/$unit"
done < <(swarmy_unit_names)
while IFS= read -r binary; do
    touch "$fake/usr/local/bin/$binary"
done < <(swarmy_binary_names)
printf 'nbd\nublk_drv\n' > "$fake/etc/modules-load.d/swarmy.conf"
printf 'secret=fixture\n' > "$fake/etc/swarmy/node.env"
touch "$fake/home/decomm-swarmy/swarmy/checkout-marker"
printf 'LABEL=swarmy-local /mnt/swarmy-local ext4 defaults,nofail 0 2\n' > "$fake/etc/fstab"

# The service user's home resolves through a stubbed passwd entry so the
# checkout removal targets the fake root, never the real filesystem.
getent() { printf 'decomm-swarmy:x:1001:1001::/home/decomm-swarmy:/bin/bash\n'; }
export -f getent

bash scripts/remote-decommission.sh decomm-swarmy

# Every shared unit was stopped and disabled before removal.
while IFS= read -r unit; do
    grep -qxF "stop $unit" "$fake/systemctl.log" || { echo "unit never stopped: $unit" >&2; exit 1; }
    grep -qxF "disable $unit" "$fake/systemctl.log" || { echo "unit never disabled: $unit" >&2; exit 1; }
done < <(swarmy_unit_names)
grep -qxF "daemon-reload" "$fake/systemctl.log" || { echo "units never reloaded" >&2; exit 1; }

# No swarmy unit, binary, module config, node environment, or checkout remains.
[[ -z $(ls -A "$fake/etc/systemd/system") ]] || { echo "unit files remain" >&2; exit 1; }
[[ -z $(ls -A "$fake/usr/local/bin") ]] || { echo "binaries remain" >&2; exit 1; }
[[ ! -e $fake/etc/modules-load.d/swarmy.conf ]] || { echo "modules config remains" >&2; exit 1; }
[[ ! -e $fake/etc/swarmy ]] || { echo "node environment remains" >&2; exit 1; }
[[ ! -e $fake/home/decomm-swarmy/swarmy ]] || { echo "checkout remains" >&2; exit 1; }

# The service user's home, its fstab line, and unrelated files are untouched.
[[ -d $fake/home/decomm-swarmy ]] || { echo "service home removed" >&2; exit 1; }
grep -qxF 'LABEL=swarmy-local /mnt/swarmy-local ext4 defaults,nofail 0 2' "$fake/etc/fstab" || { echo "fstab line changed" >&2; exit 1; }

# Rerunning after a complete teardown still succeeds (a retried down is a no-op).
bash scripts/remote-decommission.sh decomm-swarmy
echo 'remote decommission behavioral tests passed'
