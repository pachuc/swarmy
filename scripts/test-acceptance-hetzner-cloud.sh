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
printf '%s\n' "$*" >> "$CURL_FIXTURE/curl-argv"
cat > "$CURL_FIXTURE/curl-stdin"
case $* in
  *'/server_types?'*) cat "$CURL_FIXTURE/server-types.json";;
  *'/pricing'*) cat "$CURL_FIXTURE/pricing.json";;
  *'/servers?'*) cat "$CURL_FIXTURE/servers.json";;
  *'/networks?'*) echo '{"networks":[]}' ;;
  *'/firewalls?'*) echo '{"firewalls":[]}' ;;
  *'/ssh_keys?'*) echo '{"ssh_keys":[]}' ;;
  *'your-objectstorage.com/'*) if [[ $* == *' -I '* ]]; then printf '%s' "${BUCKET_CODE:-404}"; else echo 'ok'; fi;;
  *) echo 'unexpected URL' >&2; exit 1;;
esac
STUB
chmod +x "$fake/bin/curl"
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
grep -q cpx41 "$fake/err"
verify_clean
printf '{"servers":[{"id":1,"name":"acc-test-control-1"}]}\n' > "$fake/servers.json"
if verify_clean > "$fake/out"; then exit 1; fi
grep -q acc-test-control-1 "$fake/out"
printf '{"servers":[]}\n' > "$fake/servers.json"
BUCKET_CODE=200; export BUCKET_CODE
if verify_clean > "$fake/out" 2> "$fake/err"; then exit 1; fi
grep -q acc-test-bucket "$fake/err"
manual_cleanup 2> "$fake/err"
grep -q 'swarmy-swarm=acc-test' "$fake/err"
grep -q acc-test-bucket "$fake/err"
if grep -Eq 'hcloud-secret-5f2b9c|s3-secret-91d0' "$fake/err"; then exit 1; fi
py tool-output-contains "$fixture/session-show.json" swarmy-acceptance-abcdef123456 checkpoint
if py tool-output-contains "$fixture/session-show.json" assistant-only-nonce; then exit 1; fi
if parse_args --location ash 2>/dev/null; then exit 1; fi
if parse_args --max-minutes 5 2>/dev/null; then exit 1; fi
[[ $( (parse_args --unknown) 2>/dev/null; echo $? ) == 2 ]]
# The real main path fails before curl when required flags or credentials are absent.
# Cost/no --yes is covered by the fixture cost helper; it never creates a machine.
echo 'acceptance helper tests passed'
