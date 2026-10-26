#!/usr/bin/env bash
set -euo pipefail

# All credential parsing and JSON contracts live here; secrets leave only on curl stdin.
read -r -d '' ACCEPTANCE_PY <<'PY' || :
import json, os, sys, tomllib
from decimal import Decimal
from pathlib import Path

cmd, *args = sys.argv[1:]
def load(path):
    return json.loads(Path(path).read_text())
def items(body, key):
    return body[key]
def priced(entry, location):
    return Decimal(next(p['price_hourly']['gross'] for p in entry['prices'] if p['location'] == location))
def types(body, location):
    return sorted(((priced(t, location), t['name']) for t in body['server_types']
                   if t.get('deprecation') is None and t.get('architecture') == 'x86'
                   and any(p['location'] == location for p in t['prices'])), key=lambda pair: pair[0])
if cmd in ('hcloud-config', 's3-config'):
    credentials = tomllib.loads((Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local/share')) /
                                 'swarmy/cloud-credentials.toml').read_text())
    def quote(value):
        return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
    if cmd == 'hcloud-config':
        print('header = ' + quote('Authorization: Bearer ' + credentials['hetzner-cloud']['token']))
    else:
        c = credentials['hetzner-s3']
        print('user = ' + quote(c['access_key'] + ':' + c['secret_key']))
        print('aws-sigv4 = ' + quote('aws:amz:' + args[0] + ':s3'))
elif cmd == 'record-bucket':
    print(tomllib.loads(Path(args[0]).read_text())['tofu_bucket'])
elif cmd == 'cloud-ready':
    rows = load(args[0]); assert all(any(r['provider'] == p and r['set'] is True for r in rows)
                                for p in ('hetzner-cloud', 'hetzner-s3'))
elif cmd == 'server-type':
    body, location, name = load(args[0]), args[1], args[2]
    found = [t for t in body['server_types'] if t['name'] == name]
    if len(found) != 1 or found[0].get('deprecation') is not None or found[0].get('architecture') != 'x86' or not any(p['location'] == location for p in found[0]['prices']):
        print('server type unavailable; pass --server-type with one of the listed alternatives', file=sys.stderr)
        sys.exit(1)
    print(priced(found[0], location))
elif cmd == 'alternatives':
    print('alternatives: ' + ', '.join(n for _, n in types(load(args[0]), args[1])[:10]), file=sys.stderr)
elif cmd == 'cost':
    pricing, location, rate, minutes = load(args[0]), args[1], Decimal(args[2]), int(args[3])
    ip = next(p for p in pricing['pricing']['primary_ips'] if p['type'] == 'ipv4')
    ip_rate = priced(ip, location)
    hours = (minutes + 30 + 59) // 60
    print(f'machines: 3 (1 control + up to 2 workers) x {args[4]} at €{rate:.2f}/h')
    print(f'primary IPv4: 3 x €{ip_rate:.2f}/h')
    print(f'hours billed: at most {hours}')
    print(f'estimated maximum: €{(3 * (rate + ip_rate) * hours):.2f} (plus Hetzner object storage\'s prorated base fee)')
elif cmd == 'count':
    assert len(load(args[0])[args[1]]) == int(args[2]), f'expected {args[2]} {args[1]}'
elif cmd == 'plan':
    groups = load(args[0])['groups']
    assert len(groups) == 2 and {g['group'] for g in groups} == {'control', 'workers'} and all(g['changes'] is True for g in groups)
elif cmd == 'doctor':
    body = load(args[0])
    assert body['ok'] is True, 'doctor reported failures'
    assert any(c['name'] == 'nodes' and c['status'] == 'pass' for c in body['checks']), 'doctor sees no live node'
elif cmd == 'primary-ids':
    for server in load(args[0])['servers']:
        ip_id = server['public_net']['ipv4']['id']
        assert isinstance(ip_id, int), 'server primary IPv4 has no id'
        print(ip_id)
elif cmd == 'session-id':
    print(next(e['session_id'] for e in map(json.loads, Path(args[0]).read_text().splitlines()) if e.get('event') in ('session_created', 'session_opened')))
elif cmd == 'max-seq':
    rows = [json.loads(l) for l in Path(args[0]).read_text().splitlines()[1:]]
    print(max((v.get('seq', 0) for r in rows for v in r.values() if isinstance(v, dict)), default=0))
elif cmd == 'session-node':
    session = json.loads(Path(args[0]).read_text().splitlines()[0])
    node = (session.get('placement') or session.get('scratch') or {}).get('node_id')
    if node: print('agent node after shrink: ' + node)
elif cmd == 'tool-output-contains':
    after = int(args[2])
    records = [json.loads(line) for line in Path(args[0]).read_text().splitlines()[1:]]
    calls = {}
    results = {}
    for row in records:
        event = row.get('tool_call_requested') or row.get('tool_call_completed')
        if not event or event['seq'] <= after: continue
        if 'call' in event: calls[event['call']['call_id']] = event['call']['tool']
        if 'result' in event: results[event['call_id']] = event['result']
    needle = args[1]
    assert any(needle in r.get('completed', {}).get('output', '') and calls.get(cid) == 'bash'
               for cid, r in results.items()), 'nonce absent from bash tool result'
    if len(args) > 3 and args[3] == 'checkpoint':
        assert any(calls.get(cid) == 'checkpoint' and 'completed' in r for cid, r in results.items()), 'checkpoint did not succeed'
elif cmd == 'leftovers':
    body, kind = load(args[0]), args[1]
    for row in body[kind]: print(f'{kind}: {row["name"]}', file=sys.stderr)
    sys.exit(bool(body[kind]))
else:
    raise ValueError('unknown acceptance helper command')
PY

usage() { echo 'usage: acceptance-hetzner-cloud.sh [--dry-run] [--yes] --provider ID --model ID --api-key-file FILE [--server-type TYPE] [--location LOCATION] [--max-minutes N] [--swarmy PATH]'; }
fail() { echo "$*" >&2; return 1; }
py() { python3 -c "$ACCEPTANCE_PY" "$@"; }
new_name() { printf 'acc-%s\n' "$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"; }
hcloud_get() { py hcloud-config | curl -K - --fail-with-body --silent --show-error --max-time 30 --retry 3 "https://api.hetzner.cloud/v1$1"; }
hcloud_status() { py hcloud-config | curl -K - --fail-with-body --silent --show-error --max-time 30 --retry 3 -o /dev/null -w '%{http_code}' "https://api.hetzner.cloud/v1$1"; }
s3_status() { py s3-config "$location" | curl -K - --fail-with-body --silent --show-error --max-time 30 --retry 3 -I -o /dev/null -w '%{http_code}' "https://$location.your-objectstorage.com/$1"; }
s3_list_ok() {
    local code
    code=$(py s3-config "$location" | curl -K - --fail-with-body --silent --show-error --max-time 30 --retry 3 -o /dev/null -w '%{http_code}' "https://$location.your-objectstorage.com/") || return 1
    [[ $code == 200 ]]
}
step() {
    local label=$1 remaining status=0; shift
    echo "== $label" >&2
    remaining=$((deadline - SECONDS))
    ((remaining > 0)) || fail "max runtime reached before $label"
    # Background steps may ignore INT; TERM reaches both swarmy and OpenTofu.
    timeout --signal=TERM --kill-after=60 "$remaining" "$@" &
    step_pid=$!
    wait "$step_pid" || status=$?
    step_pid=''
    return "$status"
}
interrupted() {
    if [[ -n ${step_pid:-} ]]; then kill -TERM "$step_pid" 2>/dev/null || :; wait "$step_pid" 2>/dev/null || :; fi
    exit "$1"
}
manual_cleanup() {
    echo "retry: swarmy swarm down $name --destroy" >&2
    echo "Hetzner console: project > Servers, Networks, Firewalls, Security/SSH keys; filter label swarmy-swarm=$name" >&2
    for kind in server network firewall ssh-key; do echo "hcloud $kind list -l swarmy-swarm=$name" >&2; done
    echo 'hcloud server delete NAME (and remove remaining labelled resources)' >&2
    echo "Bucket ${bucket:-unknown} at $location.your-objectstorage.com: delete it in the console under Object Storage, after emptying it" >&2
    echo "OpenTofu state, if it still exists: ${XDG_DATA_HOME:-$HOME/.local/share}/swarmy/swarms/$name/tofu/" >&2
    echo 'No credential was printed.' >&2
}
verify_clean() {
    local kind file leftovers=0 code ip_id
    for kind in servers networks firewalls ssh_keys; do
        file=$(mktemp)
        if ! hcloud_get "/$kind?label_selector=swarmy-swarm%3D$name" > "$file"; then leftovers=1
        elif ! py leftovers "$file" "$kind"; then leftovers=1; fi
        rm -f "$file"
    done
    for ip_id in "${primary_ids[@]}"; do
        code=$(hcloud_status "/primary_ips/$ip_id") || :
        [[ $code == 404 ]] || { echo "primary IPv4 remains (HTTP $code)" >&2; leftovers=1; }
    done
    if [[ -n $bucket ]]; then
        code=$(s3_status "$bucket") || :
        [[ $code == 404 ]] || { echo "bucket remains: $bucket (HTTP $code)" >&2; leftovers=1; }
    else
        echo 'bucket unknown; verify object storage manually' >&2; leftovers=1
    fi
    ((leftovers == 0))
}
on_exit() {
    local result=$? down_ok=0 clean_ok=0
    trap - EXIT
    if ((created)); then
        trap 'echo "teardown interrupted" >&2; manual_cleanup; exit 130' INT TERM HUP
        if ((dry_run)); then
            timeout 1800 "$swarmy" swarm down "$name" && down_ok=1 || :
            echo 'dry run: Hetzner API checks skipped'
            clean_ok=1
        else
            timeout 1800 "$swarmy" swarm down "$name" --destroy && down_ok=1 || :
            verify_clean && clean_ok=1 || :
        fi
        if ((down_ok && clean_ok)); then
            "$swarmy" swarm rm "$name" || result=1
            if [[ -f ${XDG_STATE_HOME:-$HOME/.local/state}/swarmy/current ]] && grep -qx "$name" "${XDG_STATE_HOME:-$HOME/.local/state}/swarmy/current"; then echo 'note: current still names removed swarm' >&2; fi
        else
            ((result != 0)) || result=1
        fi
        ((result == 0 && down_ok && clean_ok)) || manual_cleanup
    fi
    if ((dry_run)) && [[ -n $work ]]; then
        if [[ -f $work/tofu.log ]] && grep -Eq ' (apply|destroy|output)$' "$work/tofu.log"; then echo 'fake tofu ran a forbidden operation' >&2; result=1; fi
        rm -rf "$work"
        ((result != 0)) || echo 'dry run complete: create and plan work; nothing was created'
    elif ((result == 0 && created)); then
        echo "acceptance passed in $(((SECONDS - started + 59) / 60)) minutes"
    fi
    exit "$result"
}
parse_args() {
    yes=0 dry_run=0 provider='' model='' api_key_file='' server_type=cpx31 location=fsn1 max_minutes=90 swarmy=''
    while (($#)); do
        case $1 in
            --yes) yes=1;; --dry-run) dry_run=1;;
            --provider|--model|--api-key-file|--server-type|--location|--max-minutes|--swarmy)
                (($# >= 2)) || { usage >&2; return 2; }
                case $1 in
                    --provider) provider=$2;; --model) model=$2;; --api-key-file) api_key_file=$2;;
                    --server-type) server_type=$2;; --location) location=$2;; --max-minutes) max_minutes=$2;; --swarmy) swarmy=$2;;
                esac
                shift;;
            -h|--help) usage; exit 0;; *) usage >&2; return 2;;
        esac
        shift
    done
    [[ $server_type =~ ^[a-z0-9-]+$ ]] || { fail 'choose --server-type using lowercase letters, digits and hyphens'; return 1; }
    [[ $location =~ ^(fsn1|nbg1|hel1)$ ]] || { fail 'choose --location fsn1, nbg1, or hel1'; return 1; }
    if ! [[ $max_minutes =~ ^[0-9]+$ ]] || ((max_minutes < 20 || max_minutes > 240)); then fail 'choose --max-minutes between 20 and 240'; return 1; fi
}
local_preconditions() {
    ((BASH_VERSINFO[0] > 4 || BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4)) || fail 'install bash 4.4 or newer'
    for bin in curl python3 ssh-keygen timeout; do command -v "$bin" >/dev/null || fail "install $bin"; done
    python3 -c 'import tomllib,sys; sys.exit(sys.version_info < (3,11))' || fail 'install Python 3.11 or newer'
    local version
    version=$(curl --version | head -1 | awk '{print $2}')
    python3 -c 'import sys; sys.exit(tuple(map(int, sys.argv[1].split(".")[:2])) < (7,75))' "$version" || fail 'install curl 7.75 or newer'
    [[ -x $swarmy ]] || fail 'install swarmy or pass --swarmy PATH to an executable'
    local cloud_file; cloud_file=$(mktemp)
    if ! "$swarmy" --json cloud ls > "$cloud_file" || ! py cloud-ready "$cloud_file"; then rm -f "$cloud_file"; fail "run: printf '%s\\n' \"\$TOKEN\" | swarmy cloud login hetzner-cloud; swarmy cloud login hetzner-s3"; fi
    rm -f "$cloud_file"
    if (( ! dry_run )); then
        [[ -n $provider && -n $model ]] || { fail 'pass --provider ID and --model ID'; return 1; }
        if [[ $provider != chatgpt ]]; then
            [[ -f $api_key_file && ! -L $api_key_file && -s $api_key_file && $(stat -c %a "$api_key_file") == 600 ]] || fail 'pass --api-key-file FILE: nonempty regular file with mode 0600'
        fi
        [[ -z ${SWARMY_TOFU_BIN:-} ]] || echo 'warning: SWARMY_TOFU_BIN set; real run should use pinned OpenTofu download' >&2
        "$swarmy" swarm scale --help >/dev/null 2>&1 || fail 'this swarmy has no swarm scale; pass --swarmy built from the swarms branch'
    fi
}
setup_dry_run() {
    work=$(mktemp -d)
    export HOME="$work/home" XDG_CONFIG_HOME="$work/config" XDG_DATA_HOME="$work/data" XDG_STATE_HOME="$work/state" XDG_CACHE_HOME="$work/cache"
    unset SWARMY_SWARM SWARMY_API_URL SWARMY_API_TOKEN
    mkdir -p "$XDG_CONFIG_HOME/swarmy" "$work/bin"
    printf '[selection]\nprovider = "fake"\n' > "$XDG_CONFIG_HOME/swarmy/config.toml"
    printf 'dry-run-token\n' | "$swarmy" cloud login hetzner-cloud >/dev/null
    printf 'dry-access\ndry-secret\n' | "$swarmy" cloud login hetzner-s3 >/dev/null
    cat > "$work/bin/tofu" <<'TOFU'
#!/usr/bin/env bash
printf '%s %s\n' "$(basename "$PWD")" "$1" >> "$ACCEPTANCE_TOFU_LOG"
case $1 in
    init) exit 0;;
    plan) echo "dry-run plan for $(basename "$PWD")"; exit 2;;
    *) echo "fake tofu: $1 is not allowed in a dry run" >&2; exit 1;;
esac
TOFU
    chmod +x "$work/bin/tofu"
    export ACCEPTANCE_TOFU_LOG="$work/tofu.log" SWARMY_TOFU_BIN="$work/bin/tofu"
}
confirm_cost() {
    ((yes)) || { echo 're-run with --yes to create these machines'; return 1; }
}
main() {
    yes=0 dry_run=0 provider='' model='' api_key_file='' server_type='' location='' max_minutes='' swarmy='' name='' bucket='' work='' created=0 deadline=0 nonce='' plan_file='' file='' session_id='' rate='' after_seq='' step_pid='' started=$SECONDS
    primary_ids=()
    parse_args "$@"
    [[ -n $swarmy ]] || swarmy=$(command -v swarmy || :)
    trap on_exit EXIT
    trap 'interrupted 130' INT TERM
    trap 'interrupted 129' HUP
    ((dry_run == 0)) || setup_dry_run
    local_preconditions
    name=$(new_name)
    [[ ! -e ${XDG_CONFIG_HOME:-$HOME/.config}/swarmy/swarms/$name.toml ]] || fail 'generated name already exists; retry'
    if ((dry_run)); then echo 'estimated cost: not fetched in dry run'
    else
        file=$(mktemp); hcloud_get "/server_types?name=$server_type" > "$file"
        rate=$(py server-type "$file" "$location" "$server_type") || {
            rm -f "$file"
            file=$(mktemp)
            hcloud_get '/server_types?per_page=50' > "$file"
            py alternatives "$file" "$location"
            rm -f "$file"
            return 1
        }; rm -f "$file"
        file=$(mktemp); hcloud_get /pricing > "$file"; py cost "$file" "$location" "$rate" "$max_minutes" "$server_type"; rm -f "$file"
        s3_list_ok || fail "the stored hetzner-s3 keys were rejected by $location.your-objectstorage.com"
        file=$(mktemp); hcloud_get "/servers?label_selector=swarmy-swarm%3D$name" > "$file"
        py count "$file" servers 0 || { rm -f "$file"; fail 'generated swarm label already has servers'; }; rm -f "$file"
        confirm_cost
    fi
    deadline=$((SECONDS + max_minutes * 60))
    created=1
    step create "$swarmy" swarm create "$name" --on hetzner-cloud --size "$server_type" --location "$location" --nodes 1 --lifecycle ephemeral
    plan_file=$(mktemp)
    step plan "$swarmy" --json swarm plan "$name" > "$plan_file"
    cat "$plan_file"
    py plan "$plan_file"; rm -f "$plan_file"
    bucket=$(py record-bucket "${XDG_DATA_HOME:-$HOME/.local/share}/swarmy/swarms/$name/record.toml")
    if ((dry_run)); then return 0; fi
    local up_args=(); [[ $provider != chatgpt ]] || up_args+=(--copy-credential)
    step up "$swarmy" swarm up "$name" "${up_args[@]}"
    if [[ $provider != chatgpt ]]; then step auth "$swarmy" --swarm "$name" auth set "$provider" --file "$api_key_file"; fi
    file=$(mktemp); step doctor "$swarmy" --swarm "$name" --json doctor > "$file"; py doctor "$file"; rm -f "$file"
    step agent "$swarmy" --swarm "$name" agent create acceptance --provider "$provider" --model "$model"
    nonce="swarmy-acceptance-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
    file=$(mktemp)
    step volume-write "$swarmy" --swarm "$name" --json run --agent acceptance "Use the bash tool to run exactly: printf %s $nonce > /home/agent/acceptance.txt && cat /home/agent/acceptance.txt && uname -m. Then call the checkpoint tool. Reply with the command output." > "$file"
    session_id=$(py session-id "$file"); rm -f "$file"
    file=$(mktemp); step show-write "$swarmy" --swarm "$name" session show "$session_id" --json > "$file"; py tool-output-contains "$file" "$nonce" 0 checkpoint; after_seq=$(py max-seq "$file"); rm -f "$file"
    step scale-up "$swarmy" swarm scale "$name" workers 2
    check_hosts 3
    step scale-down "$swarmy" swarm scale "$name" workers 1
    check_hosts 2
    file=$(mktemp)
    step volume-read "$swarmy" --swarm "$name" --json run --agent acceptance 'Use the bash tool to run exactly: cat /home/agent/acceptance.txt' > "$file"
    session_id=$(py session-id "$file"); rm -f "$file"
    file=$(mktemp); step show-read "$swarmy" --swarm "$name" session show "$session_id" --json > "$file"; py tool-output-contains "$file" "$nonce" "$after_seq"; py session-node "$file"; rm -f "$file"
}
check_hosts() {
    local file ids_file; file=$(mktemp)
    step "status-$1" "$swarmy" --json swarm status "$name" > "$file"; py count "$file" hosts "$1"; rm -f "$file"
    file=$(mktemp); hcloud_get "/servers?label_selector=swarmy-swarm%3D$name" > "$file"; py count "$file" servers "$1"
    if [[ $1 == 3 ]]; then
        ids_file=$(mktemp)
        py primary-ids "$file" > "$ids_file"
        mapfile -t primary_ids < "$ids_file"
        rm -f "$ids_file"
        [[ ${#primary_ids[@]} == 3 ]] || fail 'expected three primary IPv4 ids'
    fi
    rm -f "$file"
}
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main "$@"; fi
