#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/remote-provision-env.sh
fake=$(mktemp -d)
trap 'rm -rf "$fake"' EXIT
export FAKE_ROOT=$fake
mkdir -p "$fake/bin" "$fake/etc/systemd/system"
cat > "$fake/bin/sudo" <<'STUB'
#!/usr/bin/env bash
exec "$@"
STUB
cat > "$fake/bin/systemctl" <<'STUB'
#!/usr/bin/env bash
if [[ $1 == list-unit-files ]]; then
    for file in "$FAKE_ROOT"/etc/systemd/system/*.service; do
        [[ -e $file ]] && printf '%s enabled\n' "${file##*/}"
    done
    exit 0
else
    printf '%s\n' "$*" >> "$FAKE_ROOT/actions"
fi
STUB
chmod +x "$fake/bin/"*
export PATH="$fake/bin:$PATH"
mapfile -t units < <(swarmy_unit_names)
for unit in "${units[@]}"; do touch "$fake/etc/systemd/system/$unit"; done
: > "$fake/actions"
stop_installed_units
for ((i=0; i<${#units[@]}; i++)); do
    [[ $(sed -n "$((i+1))p" "$fake/actions") == "disable --now ${units[${#units[@]}-1-i]}" ]]
done
: > "$fake/actions"
start_installed_units
for ((i=0; i<${#units[@]}; i++)); do
    [[ $(sed -n "$((i+1))p" "$fake/actions") == "enable --now ${units[i]}" ]]
done
rm -f "$fake/etc/systemd/system/"*
touch "$fake/etc/systemd/system/swarmyd.service" "$fake/etc/systemd/system/swarmy-tunnel.service"
: > "$fake/actions"
stop_installed_units
[[ $(cat "$fake/actions") == $'disable --now swarmyd.service\ndisable --now swarmy-tunnel.service' ]]
: > "$fake/actions"
start_installed_units
[[ $(cat "$fake/actions") == $'enable --now swarmy-tunnel.service\nenable --now swarmyd.service' ]]
rm -f "$fake/etc/systemd/system/"*
: > "$fake/actions"
stop_installed_units
start_installed_units
[[ ! -s $fake/actions ]]
echo 'remote stop/start behavioral tests passed'
