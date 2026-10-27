#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/acceptance-hetzner-cloud.sh
fixture=$PWD/scripts/fixtures/acceptance
fake=$(mktemp -d)
trap 'rm -rf "$fake"' EXIT
mkdir -p "$fake/bin" "$fake/data/swarmy"
export XDG_DATA_HOME="$fake/data" CURL_FIXTURE="$fake" PATH="$fake/bin:$PATH"
cat > "$XDG_DATA_HOME/swarmy/cloud-credentials.toml" <<'CREDS'
[hetzner-cloud]
token = "hcloud-secret-5f2b9c"
[hetzner-s3]
access_key = "dry-access"
secret_key = "s3-secret-91d0"
CREDS
cat > "$fake/bin/curl" <<'STUB'
#!/usr/bin/env bash
if [[ $* == '--version' ]]; then echo 'curl 8.0.0'; exit 0; fi
printf '%s\n' "$*" >> "$CURL_FIXTURE/curl-argv"
cat > "$CURL_FIXTURE/curl-stdin"
case $* in
  *'/server_types?'*) cat "$CURL_FIXTURE/server-types.json";;
  *'/pricing'*) cat "$CURL_FIXTURE/pricing.json";;
  *'/servers?'*) cat "$CURL_FIXTURE/servers.json";;
  *'/primary_ips/'*) printf '%s' "${PRIMARY_IP_CODE:-404}";;
  *'/networks?'*) echo '{"networks":[]}' ;;
  *'/firewalls?'*) echo '{"firewalls":[]}' ;;
  *'/ssh_keys?'*) echo '{"ssh_keys":[]}' ;;
  *'your-objectstorage.com/'*) if [[ $* == *' -I '* ]]; then printf '%s' "${BUCKET_CODE:-404}"; else printf '%s' "${LIST_CODE:-200}"; fi;;
  *) echo 'unexpected URL' >&2; exit 1;;
esac
STUB
chmod +x "$fake/bin/curl"
# Fixtures use server_types[].prices[location].price_hourly.gross and
# pricing.primary_ips[type=ipv4].prices[location].price_hourly.gross.
cp "$fixture/server-types.json" "$fixture/pricing.json" "$fake/"
printf '{"servers":[]}\n' > "$fake/servers.json"
: > "$fake/curl-argv"
location=fsn1 name=acc-test bucket=acc-test-bucket
hcloud_get '/server_types?name=cpx31' > "$fake/out" 2> "$fake/err"
s3_status "$bucket" >> "$fake/out" 2>> "$fake/err"
for secret in hcloud-secret-5f2b9c s3-secret-91d0; do
    if grep -q "$secret" "$fake/curl-argv" "$fake/out" "$fake/err"; then exit 1; fi
    if env | grep -q "$secret"; then exit 1; fi
done
grep -q 's3-secret-91d0' "$fake/curl-stdin"
s3_list_ok
LIST_CODE=403; export LIST_CODE
if s3_list_ok; then exit 1; fi
unset LIST_CODE
hcloud_get /pricing >/dev/null
grep -q 'hcloud-secret-5f2b9c' "$fake/curl-stdin"
[[ $(py cost "$fixture/pricing.json" fsn1 0.20 90 cpx31) == *'€1.26'* ]]
for _ in {1..50}; do [[ $(new_name) =~ ^[a-z]([a-z0-9-]{0,18}[a-z0-9])?$ ]]; done
cp "$fixture/server-types.json" "$fake/bad.json"
python3 - "$fake/bad.json" <<'PY'
import json,sys
p=sys.argv[1]; data=json.load(open(p)); data['server_types'][0]['architecture']='arm'; open(p,'w').write(json.dumps(data))
PY
if py server-type "$fake/bad.json" fsn1 cpx31 > "$fake/out" 2> "$fake/err"; then exit 1; fi
py alternatives "$fake/bad.json" fsn1 2> "$fake/err"
grep -q cpx41 "$fake/err"
python3 - "$fake/bad.json" <<'PY'
import json,sys
p=sys.argv[1]; data=json.load(open(p)); data['server_types'][0]['architecture']='x86'; data['server_types'][0]['deprecation']={'announced':'fixture'}; open(p,'w').write(json.dumps(data))
PY
if py server-type "$fake/bad.json" fsn1 cpx31 > "$fake/out" 2> "$fake/err"; then exit 1; fi
py alternatives "$fake/bad.json" fsn1 2> "$fake/err"
grep -q cpx41 "$fake/err"
primary_ids=(1)
verify_clean
printf '{"servers":[{"id":1,"name":"acc-test-control-1","public_net":{"ipv4":{"id":1}}}]}\n' > "$fake/servers.json"
[[ $(py primary-ids "$fake/servers.json") == 1 ]]
printf '{"servers":[{"public_net":{"ipv4":{"id":null}}}]}\n' > "$fake/servers.json"
if py primary-ids "$fake/servers.json" 2> "$fake/err"; then exit 1; fi
printf '{"servers":[]}\n' > "$fake/servers.json"
PRIMARY_IP_CODE=200; export PRIMARY_IP_CODE
if verify_clean 2> "$fake/err"; then exit 1; fi
grep -q "primary IPv4 remains" "$fake/err"
unset PRIMARY_IP_CODE
printf '{"servers":[{"id":1,"name":"acc-test-control-1"}]}\n' > "$fake/servers.json"
if verify_clean 2> "$fake/err"; then exit 1; fi
grep -q 'servers: acc-test-control-1' "$fake/err"
printf '{"servers":[]}\n' > "$fake/servers.json"
BUCKET_CODE=200; export BUCKET_CODE
if verify_clean > "$fake/out" 2> "$fake/err"; then exit 1; fi
grep -q acc-test-bucket "$fake/err"
manual_cleanup 2> "$fake/err"
grep -q 'swarmy-swarm=acc-test' "$fake/err"
grep -q acc-test-bucket "$fake/err"
if grep -Eq 'hcloud-secret-5f2b9c|s3-secret-91d0' "$fake/err"; then exit 1; fi
py tool-output-contains "$fixture/session-show.json" swarmy-acceptance-abcdef123456 0 checkpoint
[[ $(py max-seq "$fixture/session-show.json") == 5 ]]
if py tool-output-contains "$fixture/session-show.json" swarmy-acceptance-abcdef123456 4 2> "$fake/err"; then exit 1; fi
if py tool-output-contains "$fixture/session-show.json" assistant-only-nonce 0 2> "$fake/err"; then exit 1; fi
if parse_args --location ash 2>/dev/null; then exit 1; fi
if parse_args --server-type "bad/type" 2>/dev/null; then exit 1; fi
if parse_args --max-minutes 5 2>/dev/null; then exit 1; fi
[[ $( (parse_args --unknown) 2>/dev/null; echo $? ) == 2 ]]
py doctor "$fixture/doctor.json"
python3 - "$fixture/doctor.json" "$fake/doctor-warn.json" <<'PY'
import json,sys
body=json.load(open(sys.argv[1])); body["checks"][0]["status"]="warn"
open(sys.argv[2],"w").write(json.dumps(body))
PY
if py doctor "$fake/doctor-warn.json" 2> "$fake/err"; then exit 1; fi
yes=0
if confirm_cost > "$fake/out" 2>&1; then exit 1; fi
grep -qx 're-run with --yes to create these machines' "$fake/out"
yes=1
confirm_cost
cat > "$fake/bin/swarmy" <<'STUB'
#!/usr/bin/env bash
[[ $* == '--json cloud ls' ]] || exit 1
echo '[{"provider":"hetzner-cloud","set":true},{"provider":"hetzner-s3","set":true}]'
STUB
chmod +x "$fake/bin/swarmy"
parse_args --provider openai --swarmy "$fake/bin/swarmy"
if local_preconditions 2> "$fake/err"; then exit 1; fi
grep -q -- '--model ID' "$fake/err"
printf 'fixture-key' > "$fake/key"
chmod 600 "$fake/key"
parse_args --provider openai --model fixture --api-key-file "$fake/key" --swarmy "$fake/bin/swarmy"
if local_preconditions 2> "$fake/err"; then exit 1; fi
grep -q 'this swarmy has no swarm scale' "$fake/err"
# Source the script in a separate shell: no cloud path runs. The actual EXIT
# teardown must wait for a hung step to stop before invoking fake swarm down.
cat > "$fake/bin/teardown-swarmy" <<'STUB'
#!/usr/bin/env bash
case $* in
    'swarm down acc-signal')
        if [[ -f $SIGNAL_DIR/child-pid ]] && ! kill -0 "$(cat "$SIGNAL_DIR/child-pid")" 2>/dev/null; then
            touch "$SIGNAL_DIR/teardown-after-stop"
        else
            touch "$SIGNAL_DIR/teardown-before-stop"
        fi;;
    'swarm rm acc-signal') :;;
    *) exit 1;;
esac
STUB
chmod +x "$fake/bin/teardown-swarmy"
cat > "$fake/signal-harness.sh" <<'HARNESS'
#!/usr/bin/env bash
set -euo pipefail
source "$ACCEPTANCE_SCRIPT"
deadline=$((SECONDS + 5))
step_pid=''
created=1 dry_run=1 name=acc-signal bucket='' location=fsn1
work="$SIGNAL_DIR/work"
mkdir -p "$work"
swarmy=$SIGNAL_SWARMY
trap 'interrupted 130' INT TERM
trap 'interrupted 129' HUP
trap on_exit EXIT
# This child installs no signal handler; it inherits the real step's dispositions.
step hung bash -c 'echo "$$" > "$SIGNAL_DIR/child-pid"; echo ready > "$SIGNAL_DIR/ready"; while :; do sleep 1; done'
HARNESS
export ACCEPTANCE_SCRIPT=$PWD/scripts/acceptance-hetzner-cloud.sh SIGNAL_SWARMY=$fake/bin/teardown-swarmy
set -m
for signal in INT TERM HUP; do
    export SIGNAL_DIR="$fake/$signal"
    mkdir -p "$SIGNAL_DIR"
    mkfifo "$SIGNAL_DIR/ready"
    # Signals ignored when a shell starts cannot be trapped, and nohup or a
    # background job in a non-interactive shell (as CI runners do) starts the
    # harness with HUP or INT ignored. Restore the defaults, as a terminal has.
    python3 -c 'import os, signal, sys
for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
    signal.signal(s, signal.SIG_DFL)
os.execvp(sys.argv[1], sys.argv[1:])' bash "$fake/signal-harness.sh" > "$SIGNAL_DIR/out" 2> "$SIGNAL_DIR/err" &
    signal_pid=$!
    # A FIFO handshake ensures the step is running before sending the signal.
    if ! timeout 3 cat "$SIGNAL_DIR/ready" > "$SIGNAL_DIR/started"; then
        kill -KILL -- "-$signal_pid" 2>/dev/null || :
        echo "step never started" >&2
        exit 1
    fi
    grep -qx ready "$SIGNAL_DIR/started"
    kill -s "$signal" "$signal_pid"
    # tail --pid has a deadline and completes only after the harness exits.
    if ! timeout 3 tail --pid="$signal_pid" -f /dev/null; then
        kill -TERM -- "-$signal_pid" 2>/dev/null || :
        kill -KILL -- "-$signal_pid" 2>/dev/null || :
        echo "$signal did not promptly interrupt the step" >&2
        exit 1
    fi
    signal_status=0
    wait "$signal_pid" || signal_status=$?
    if [[ $signal == HUP ]]; then [[ $signal_status == 129 ]]; else [[ $signal_status == 130 ]]; fi
    [[ -f $SIGNAL_DIR/teardown-after-stop && ! -e $SIGNAL_DIR/teardown-before-stop ]]
    if grep -q 'acceptance passed' "$SIGNAL_DIR/out"; then exit 1; fi
done
# The real main path is never invoked here; even negative cases stay isolated.
echo 'acceptance helper tests passed'
