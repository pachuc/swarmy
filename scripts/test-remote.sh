#!/usr/bin/env bash
# Run after building the workspace and starting scripts/dev-stack.sh.
set -euo pipefail
if [[ -z ${SWARMY_REMOTE_TEST_KEY:-} ]]; then
    echo 'Skipping remote SSH acceptance: SWARMY_REMOTE_TEST_KEY is unset.'
    exit 0
fi
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cli="$repo/target/debug/swarmy"
work=$(mktemp -d /tmp/swarmy-remote-test.XXXXXX)
# Do not let an existing project profile or endpoints change this fixture.
while IFS= read -r variable; do
    case "$variable" in SWARMY_REMOTE_TEST_*) ;; SWARMY_*) unset "$variable" ;; esac
done < <(compgen -e)
export SWARMY_STATE_DIR="$work/.swarmy"
daemon=
cleanup() {
    result=$?
    if (( result != 0 )); then
        for log in "$work"/.swarmy/dev/logs/*.log "$work"/swarmyd.log; do
            if [[ -f $log ]]; then cat "$log"; fi
        done
    fi
    if [[ -n $daemon ]]; then kill "$daemon" 2>/dev/null || true; wait "$daemon" 2>/dev/null || true; fi
    cd "$work"
    if [[ -f .swarmy/dev/remote ]]; then "$cli" dev down >/dev/null 2>&1 || true; fi
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
(state / 'config.toml').write_text('provider = "fake"\nstore_directory = "remote-acceptance"\nbus_prefix = "remote-acceptance"\ns3_prefix = "remote-acceptance"\n')
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
        assert p['ports'][name] == default, (name, 'isolated client must have default ports free')
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
if [[ ${SWARMY_REMOTE_TEST_HOST:-127.0.0.1} != 127.0.0.1 ]]; then
"$cli" dev up --remote local
for service in fdb nats seaweed; do cmp "$repo/.dev/$service.pid" "$work/$service.pid"; done
"$cli" doctor --remote local --json > doctor.json
python3 - <<'PY'
import json
result = json.load(open('doctor.json'))
assert result['ok'], result
checks = {c['name']: c for c in result['checks']}
assert checks['remote tunnel']['ok']
for name in ['FoundationDB', 'NATS', 'S3']:
    assert checks['remote ' + name]['ok']
assert 'fdbserver' not in checks
PY
# This fake-provider session never materializes a computer, but still needs a registered image.
mkdir -p "$work/remote-fixture/rootfs"
cat > "$work/remote-fixture/recipe.toml" <<'EOF'
disk_size = 16777216
source_date_epoch = 1714003200
[source]
kind = "directory"
path = "rootfs"
EOF
"$cli" image build --remote local "$work/remote-fixture" --tag test
export SWARMY_DEFAULT_IMAGE=remote-fixture:test
timeout 45 "$cli" run --remote local 'what time is it' > run.log
cat run.log
grep -q 'Hello from swarmy!' run.log
SWARMY_REMOTE=local "$repo/target/debug/swarmyd" > swarmyd.log 2>&1 &
daemon=$!
for attempt in {1..100}; do
    if grep -q 'node registered and ready' swarmyd.log; then break; fi
    kill -0 "$daemon"
    sleep .1
done
grep -q 'node registered and ready' swarmyd.log
"$cli" remote status --json > status.json
python3 - <<'PYTHON'
import json
status = json.load(open('status.json'))[0]
assert status['tunnel'], status
assert status['registrations'], status
assert any(r['heartbeating'] for r in status['registrations']), status
print(status)
PYTHON
kill "$daemon"
wait "$daemon"
daemon=
"$cli" dev down
for service in fdb nats seaweed; do cmp "$repo/.dev/$service.pid" "$work/$service.pid"; done
else
    if "$cli" dev up --remote local > rejected.log 2>&1; then
        echo 'Expected remapped FoundationDB profile to reject service startup.' >&2
        exit 1
    fi
    grep -q 'FoundationDB cannot use a remapped port' rejected.log
    if "$cli" doctor --remote local --json > doctor.json; then exit 1; fi
    python3 - <<'PYTHON'
import json
checks = {c['name']: c for c in json.load(open('doctor.json'))['checks']}
assert checks['remote tunnel']['ok']
assert not checks['remote FoundationDB port']['ok']
PYTHON
fi
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
