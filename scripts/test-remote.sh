#!/usr/bin/env bash
# Maintainer acceptance: a host swarm opens and reuses its on-demand API tunnel.
set -euo pipefail
if [[ -z ${SWARMY_REMOTE_TEST_KEY:-} ]]; then
    echo 'Skipping host-swarm SSH acceptance: SWARMY_REMOTE_TEST_KEY is unset.'
    exit 0
fi
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cli="$repo/target/debug/swarmy"
work=$(mktemp -d /tmp/swarmy-host-test.XXXXXX)
daemon=
cleanup() {
    if [[ -n $daemon ]]; then kill "$daemon" 2>/dev/null || true; wait "$daemon" 2>/dev/null || true; fi
    rm -rf -- "$work"
}
trap cleanup EXIT
key=$SWARMY_REMOTE_TEST_KEY
host=${SWARMY_REMOTE_TEST_HOST:-127.0.0.1}
user=${SWARMY_REMOTE_TEST_USER:-ubuntu}
while IFS= read -r variable; do
    case "$variable" in SWARMY_REMOTE_TEST_*) ;; SWARMY_*) unset "$variable" ;; esac
done < <(compgen -e)
export HOME="$work/home"
export XDG_CONFIG_HOME="$work/config" XDG_DATA_HOME="$work/data"
export XDG_STATE_HOME="$work/state" XDG_CACHE_HOME="$work/cache"
mkdir -p "$HOME" "$XDG_CONFIG_HOME/swarmy/swarms" "$XDG_DATA_HOME/swarmy/swarms/local"
chmod 700 "$XDG_DATA_HOME/swarmy/swarms/local"
# The local dev stack backs a real API; only SSH and the record are fixtures.
# shellcheck disable=SC1091
source "$repo/.dev/env"
SWARMY_API_LISTEN=127.0.0.1:8742 SWARMY_API_TOKEN=remote-acceptance-token \
    "$repo/target/debug/swarmy-core" api >"$work/api.log" 2>&1 &
daemon=$!
python3 - <<'PY'
import socket, time
for _ in range(100):
    try:
        with socket.create_connection(('127.0.0.1', 8742), timeout=.1): break
    except OSError: time.sleep(.1)
else: raise RuntimeError('API did not listen within 10 seconds')
PY
python3 - "$XDG_CONFIG_HOME/swarmy/swarms/local.toml" "$XDG_DATA_HOME/swarmy/swarms/local/record.toml" "$host" "$user" "$key" <<'PY'
import pathlib, sys
spec, record, host, user, key = map(str, sys.argv[1:])
pathlib.Path(spec).write_text(f'''name = "local"
lifecycle = "persistent"
[groups.box]
module = "existing"
[[groups.box.hosts]]
address = "{host}"
ssh_user = "{user}"
ssh_key = "{key}"
[placement]
stack = "box"
services = "box"
nodes = ["box"]
''')
p = pathlib.Path(record)
p.write_text(f'''name = "local"
created_at = "2026-10-26T00:00:00Z"
[api]
token = "remote-acceptance-token"
[[hosts.box]]
name = "local-box-1"
public_address = "{host}"
private_address = "127.0.0.1"
ssh_user = "{user}"
ssh_key = "{key}"
provisioned = true
''')
p.chmod(0o600)
PY
"$cli" --swarm local --json session ls > "$work/sessions.json"
tunnel="$XDG_DATA_HOME/swarmy/swarms/local/tunnel/tunnel.json"
python3 - "$tunnel" "$work/first.json" <<'PY'
import json, pathlib, sys
p = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert len(p['forwards']) == 1 and p['forwards'][0]['service'] == 'api'
assert p['forwards'][0]['remote'] == 8742
assert p['forwards'][0]['local'] not in (0, 8742)
pathlib.Path(sys.argv[2]).write_text(json.dumps(p))
PY
"$cli" --swarm local --json session ls > "$work/sessions-again.json"
python3 - "$tunnel" "$work/first.json" <<'PY'
import json, pathlib, sys
first = json.loads(pathlib.Path(sys.argv[2]).read_text())
again = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert (first['pid'], first['socket']) == (again['pid'], again['socket'])
PY
"$cli" --json swarm status local > "$work/status.json"
python3 - "$work/status.json" <<'PY'
import json, sys
p = json.load(open(sys.argv[1]))
assert p['tunnel'] and p['hosts'][0]['ssh']
PY
"$cli" swarm rm local --force
python3 - "$work/first.json" <<'PY'
import json, pathlib, socket, sys
p = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert not pathlib.Path(p['socket']).exists()
try:
    with socket.create_connection(('127.0.0.1', p['forwards'][0]['local']), timeout=1):
        raise AssertionError('forward still accepts connections')
except OSError: pass
PY
echo 'Host-swarm SSH tunnel acceptance passed.'
