#!/usr/bin/env bash
# Behavioral test for scripts/remote-decommission.sh: with stubbed sudo and
# systemctl and a fake root, every unit and binary in the shared lists is
# stopped, disabled, and removed, while the fstab entry and the service
# user's home itself are left alone. Drop-in files without swarmy's marker
# line, such as the host's own modules list, survive.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/remote-provision-env.sh

fake=$(mktemp -d)
trap 'rm -rf "$fake"' EXIT
export FAKE_ROOT=$fake
mkdir -p "$fake/bin" "$fake/etc/systemd/system" "$fake/usr/local/bin" \
    "$fake/etc/modules-load.d" "$fake/etc/ld.so.conf.d" "$fake/usr/local/lib/swarmy" "$fake/etc/swarmy" "$fake/home/decomm-swarmy/swarmy"

# Stub sudo: run the command with absolute paths redirected under the fake
# root, so the test never touches the real filesystem.
cat > "$fake/bin/sudo" <<'STUB'
#!/usr/bin/env bash
[[ ${1-} != -n ]] || shift
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
cat > "$fake/bin/ldconfig" <<'STUB'
#!/usr/bin/env bash
printf 'ldconfig\n' >> "$FAKE_ROOT/ldconfig.log"
STUB
chmod +x "$fake/bin/sudo" "$fake/bin/systemctl" "$fake/bin/ldconfig"
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
while IFS= read -r binary; do
    touch "$fake/usr/local/bin/$binary"
done < <(retired_binary_names)
# The host's own module list, which provisioning and down must leave alone.
printf 'nbd\nublk_drv\n' > "$fake/etc/modules-load.d/swarmy.conf"
printf 'nbd\nublk_drv\n' | install_owned_file /etc/modules-load.d/swarmy-provisioned.conf
touch "$fake/usr/local/lib/swarmy/libfdb_c.so"
printf '/opt/host-libs\n' > "$fake/etc/ld.so.conf.d/swarmy.conf"
printf '/usr/local/lib/swarmy\n' | install_owned_file /etc/ld.so.conf.d/swarmy-provisioned.conf
# Provisioning refuses to replace a drop-in it did not write.
printf 'host\n' > "$fake/etc/ld.so.conf.d/swarmy-foreign.conf"
if printf 'x\n' | install_owned_file /etc/ld.so.conf.d/swarmy-foreign.conf 2>/dev/null; then
    echo "install_owned_file replaced a host file" >&2; exit 1
fi
[[ $(cat "$fake/etc/ld.so.conf.d/swarmy-foreign.conf") == host ]]
rm "$fake/etc/ld.so.conf.d/swarmy-foreign.conf"
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
[[ ! -e $fake/usr/local/lib/swarmy && ! -e $fake/etc/ld.so.conf.d/swarmy-provisioned.conf ]]
[[ $(cat "$fake/etc/ld.so.conf.d/swarmy.conf") == /opt/host-libs ]] || { echo "host linker config removed" >&2; exit 1; }
grep -qx ldconfig "$fake/ldconfig.log"
[[ ! -e $fake/etc/modules-load.d/swarmy-provisioned.conf ]] || { echo "modules config remains" >&2; exit 1; }
[[ $(cat "$fake/etc/modules-load.d/swarmy.conf") == $'nbd\nublk_drv' ]] || { echo "host modules config removed" >&2; exit 1; }
[[ ! -e $fake/etc/swarmy ]] || { echo "node environment remains" >&2; exit 1; }
[[ ! -e $fake/home/decomm-swarmy/swarmy ]] || { echo "checkout remains" >&2; exit 1; }

# The service user's home, its fstab line, and unrelated files are untouched.
[[ -d $fake/home/decomm-swarmy ]] || { echo "service home removed" >&2; exit 1; }
grep -qxF 'LABEL=swarmy-local /mnt/swarmy-local ext4 defaults,nofail 0 2' "$fake/etc/fstab" || { echo "fstab line changed" >&2; exit 1; }

# Rerunning after a complete teardown still succeeds (a retried down is a no-op).
bash scripts/remote-decommission.sh decomm-swarmy

mkdir -p "$fake/src"
touch "$fake/src/libfdb_c.so"
(cd "$fake/src" && install_core_library libfdb_c.so)
[[ -f $fake/usr/local/lib/swarmy/libfdb_c.so ]]
[[ $(cat "$fake/etc/ld.so.conf.d/swarmy-provisioned.conf") == "$(swarmy_owned_marker)"$'\n'/usr/local/lib/swarmy ]]

# The piped probe with no checkout: a leftover swarmyd.service unit file
# fails with the checkout-is-missing error (the glob must cover swarmyd,
# which a `swarmy-` glob would miss)...
probe_dir=$(mktemp -d)
trap 'rm -rf "$fake" "$probe_dir"' EXIT
export SWARMY_SYSTEMD_DIR=$probe_dir
touch "$probe_dir/swarmyd.service"
if bash -c 'source scripts/remote-provision-env.sh; decommission_probe decomm-swarmy' 2>"$probe_dir/probe_err"; then
    echo "probe should fail with leftover units and no checkout" >&2
    exit 1
fi
grep -q "node directory is missing" "$probe_dir/probe_err" || { echo "probe hides the missing checkout" >&2; exit 1; }
# ...while no units and no checkout succeeds as nothing to tear down.
rm -f "$probe_dir/swarmyd.service"
probe_out=$(bash -c 'source scripts/remote-provision-env.sh; decommission_probe decomm-swarmy')
[[ $probe_out == 'swarmy node directory already removed; nothing to tear down' ]] || { echo "clean probe should no-op: $probe_out" >&2; exit 1; }
echo 'remote decommission behavioral tests passed'
