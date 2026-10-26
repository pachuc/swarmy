#!/usr/bin/env bash
# Run after building the workspace with the provisioning client enabled
# (cargo build --workspace --locked --features swarmy-cli/remote) and
# starting scripts/dev-stack.sh.
set -euo pipefail
if [[ -z ${SWARMY_REMOTE_TEST_KEY:-} ]]; then
    echo 'Skipping remote SSH acceptance: SWARMY_REMOTE_TEST_KEY is unset.'
    exit 0
fi
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cli="$repo/target/debug/swarmy"
if ! "$cli" remote --help >/dev/null 2>&1; then
    echo "$cli was built without the remote feature; rebuild with --features swarmy-cli/remote" >&2
    exit 1
fi
work=$(mktemp -d /tmp/swarmy-remote-test.XXXXXX)
# Do not let an existing project profile or endpoints change this fixture.
while IFS= read -r variable; do
    case "$variable" in SWARMY_REMOTE_TEST_*) ;; SWARMY_*) unset "$variable" ;; esac
done < <(compgen -e)
export XDG_CACHE_HOME="$work/cache"
export SWARMY_STATE_DIR="$work/.swarmy"
daemon=
cleanup() {
    result=$?
    if (( result != 0 )); then
        for log in "$work"/.local/state/swarmy/swarms/dev/logs/*.log "$work"/swarmyd.log; do
            if [[ -f $log ]]; then cat "$log"; fi
        done
    fi
    if [[ -n $daemon ]]; then kill "$daemon" 2>/dev/null || true; wait "$daemon" 2>/dev/null || true; fi
    cd "$work"
    "$cli" remote disconnect local >/dev/null 2>&1 || true
    rm -rf -- "$work"
}
trap cleanup EXIT
mkdir -p "$SWARMY_STATE_DIR/remote"
python3 - "$SWARMY_STATE_DIR" "$SWARMY_REMOTE_TEST_KEY" "${SWARMY_REMOTE_TEST_HOST:-127.0.0.1}" <<'PY'
import json, pathlib, sys
state = pathlib.Path(sys.argv[1])
(state / 'remote/local.json').write_text(json.dumps(dict(name='local', region='local',
    instance_id='localhost', public_ip=sys.argv[3], private_ip='127.0.0.1',
    key_path=sys.argv[2], ssh_user='ubuntu', ports=dict(fdb=4500, nats=4222, s3=8333),
    nodes=[], created_at='2026-09-16T00:00:00Z')))
(state / 'config.toml').write_text('[selection]\nprovider = "fake"\n[store]\ndirectory = "remote-acceptance"\n[bus]\nprefix = "remote-acceptance"\n[s3]\nprefix = "remote-acceptance"\n')
PY
cd "$work"
cp "$repo/.dev/fdb.pid" "$work/fdb.pid"
cp "$repo/.dev/nats.pid" "$work/nats.pid"
cp "$repo/.dev/seaweed.pid" "$work/seaweed.pid"
"$cli" remote connect local --json > profile.json
"$cli" remote connect local --json > profile-again.json
python3 - <<'PYTHON'
import json
first = json.load(open('profile.json'))
again = json.load(open('profile-again.json'))
assert not first['timing']['reused']
assert again['timing']['reused']
assert again['timing']['address_probe_seconds'] == 0
assert again['timing']['tunnel_startup_seconds'] == 0
for result in [first, again]:
    timing = result.pop('timing')
    assert timing['elapsed_seconds'] >= timing['address_probe_seconds'] + timing['tunnel_startup_seconds']
assert first == again
PYTHON
"$cli" remote connect local > connect-human.txt
grep -q 'address probing: .*tunnel startup:' connect-human.txt
python3 - <<'PY'
import json, os, socket
p = json.load(open('profile.json'))
for name, default in [('fdb', 4500), ('nats', 4222), ('s3', 8333)]:
    if os.environ.get('SWARMY_REMOTE_TEST_HOST', '127.0.0.1') == '127.0.0.1':
        assert p['ports'][name] != default, (name, 'collision did not choose another port')
    else:
        assert (p['ports'][name] == default if name == 'fdb' else p['ports'][name] != 0), (name, 'isolated client port unavailable')
    with socket.create_connection(('127.0.0.1', p['ports'][name]), timeout=3) as stream:
        if name == 'nats':
            assert stream.recv(1024).startswith(b'INFO ')
        elif name == 's3':
            stream.sendall(b'GET / HTTP/1.0\r\nHost: localhost\r\n\r\n')
            assert stream.recv(1024).startswith(b'HTTP/')
assert str(p['ports']['nats']) in p['nats_url']
assert str(p['ports']['s3']) in p['s3_endpoint']
assert str(p['ports']['fdb']) in open(p['fdb_cluster_file']).read()
PY
# The laptop-services checks return with swarm placement; see the goal plan.
"$cli" remote disconnect local
"$cli" remote disconnect local
python3 - <<'PY'
import json, pathlib, socket, time
p = json.load(open('profile.json'))
time.sleep(.2)
assert not pathlib.Path(p['socket_path']).exists()
for port in p['ports'].values():
    try:
        stream = socket.create_connection(('127.0.0.1', port), timeout=.5)
    except OSError:
        continue
    stream.close()
    raise AssertionError(f'forward still listening: {port}')
PY
echo 'Remote SSH acceptance passed for' "${SWARMY_REMOTE_TEST_HOST:-127.0.0.1}"
