#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir -p "$root/bin" "$root/etc" "$root/home/swarmy/swarmy"
printf 'TEST_NODE=1\n' > "$root/etc/node.env"
cat > "$root/bin/sudo" <<'SH'
#!/usr/bin/env bash
[[ ${1-} != -n ]] || shift
exec "$@"
SH
cat > "$root/bin/systemctl" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_ROOT/systemctl.log"
case $1 in
is-active) [[ $(cat "$FAKE_ROOT/state") == active ]] ;;
disable) echo inactive > "$FAKE_ROOT/state" ;;
esac
SH
cat > "$root/bin/swarmy-core" <<'SH'
#!/usr/bin/env bash
line=$(head -1 "$FAKE_ROOT/busy")
printf '%s\n' "$line"
sed -i '1d' "$FAKE_ROOT/busy"
SH
chmod +x "$root/bin/"*
export FAKE_ROOT=$root SWARMY_ETC_DIR=$root/etc SWARMY_BIN_DIR=$root/bin SWARMY_DRAIN_POLL_SECONDS=0
export PATH="$root/bin:$PATH"
# The service account's home is resolved via getent; never touch real hosts.
getent() { printf 'swarmy:x:1001:1001::%s/home/swarmy:/bin/bash\n' "$FAKE_ROOT"; }
export -f getent
run() {
    local label=$1 state=$2 timeout=$3 mode=$4 lines=$5 expected=$6
    printf '%s\n' "$state" > "$root/state"
    printf '%s\n' "$lines" > "$root/busy"
    : > "$root/systemctl.log"
    local out
    out=$(bash -c 'source scripts/remote-provision-env.sh; drain_node_agent swarmy "$1" "$2"' -- "$timeout" "$mode" 2>"$root/stderr")
    [[ $(tail -1 <<< "$out") == "$expected" ]] || { printf 'unexpected %s: %s\n' "$label" "$out" >&2; exit 1; }
}
run idle active 1 wait $'[]\n[]' 'drain {"stopped":true,"busy":[],"unreleased":[]}'
grep -qx 'disable --now swarmyd.service' "$root/systemctl.log"
run busy-then-idle active 1 wait $'["agent-a"]\n[]\n[]' 'drain {"stopped":true,"busy":[],"unreleased":[]}'
grep -q 'Waiting for running sandbox commands' "$root/stderr"
run busy-timeout active 0 wait '["agent-a"]' 'drain {"stopped":false,"busy":["agent-a"],"unreleased":[]}'
if grep -q '^disable ' "$root/systemctl.log"; then echo 'unexpected disable' >&2; exit 1; fi
run force active 0 force $'["agent-a"]\n[]' 'drain {"stopped":true,"busy":["agent-a"],"unreleased":[]}'
grep -qx 'disable --now swarmyd.service' "$root/systemctl.log"
run inactive inactive 0 wait '["orphan"]' 'drain {"stopped":true,"busy":[],"unreleased":["orphan"]}'
if grep -q '^disable ' "$root/systemctl.log"; then echo 'unexpected disable' >&2; exit 1; fi
run unreleased active 1 wait $'[]\n["agent-b"]' 'drain {"stopped":true,"busy":[],"unreleased":["agent-b"]}'
echo 'remote drain behavioral tests passed'
