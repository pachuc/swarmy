#!/usr/bin/env bash
# Real-model desktop proof: tool events, image bytes, and app record must agree.
set +x
set -euo pipefail
read -r -d '' CHECK_PY <<'PY' || :
import hashlib, json, re, sys
from pathlib import Path

def require(ok, message):
    if not ok:
        sys.exit(message)

mode, *args = sys.argv[1:]
def rows(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines()]
def events(path, after):
    for row in rows(path)[1:]:
        for kind, event in row.items():
            if isinstance(event, dict) and event.get('seq', 0) > after:
                yield kind, event
def tool_events(path, after):
    calls = [e for kind, e in events(path, after) if kind == 'tool_call_requested']
    completed = {e['call_id']: e for kind, e in events(path, after) if kind == 'tool_call_completed'}
    return calls, completed
def bash_result(event):
    completed = event.get('result', {}).get('completed')
    require(completed, 'bash tool did not complete')
    return json.loads(completed['output'])
if mode == 'model-image':
    model = json.loads(Path(args[0]).read_text())
    require('image' in model.get('input_modalities', []),
            'model has no image input; screenshots would be omitted')
elif mode == 'session-id':
    print(next(e['session_id'] for e in rows(args[0]) if e.get('event') in ('session_created', 'session_opened')))
elif mode == 'max-seq':
    print(max((e['seq'] for _, e in events(args[0], 0)), default=0))
elif mode == 'exact-bash':
    path, after, command, *needles = args
    calls, completed = tool_events(path, int(after))
    require(len(calls) == 1, f'exact bash: expected one tool call, got {len(calls)}')
    call = calls[0]['call']
    require(call['tool'] == 'bash' and call['arguments'].get('command') == command, 'exact bash command differs')
    require(call['call_id'] in completed, 'exact bash completion missing')
    result = bash_result(completed[call['call_id']])
    require(result['exit_code'] == 0, f'exact bash exited {result["exit_code"]}: {result.get("stderr", "")}')
    require(all(needle in result['stdout'] for needle in needles), 'exact bash stdout missing expected text')
    print(result['stdout'], end='')
elif mode == 'window-tools':
    calls, completed = tool_events(args[0], int(args[1]))
    allowed = {'browser_navigate', 'browser_snapshot', 'browser_click', 'browser_type',
               'browser_select', 'browser_scroll', 'browser_screenshot', 'screen_screenshot', 'screen_windows'}
    names = [e['call']['tool'] for e in calls]
    for entry in calls:
        if entry['call']['tool'] == 'browser_navigate':
            url = entry['call']['arguments'].get('url', '')
            require(url.startswith('http://127.0.0.1:8765/'), f'task navigated outside the app: {url}')
    require(names and set(names) <= allowed, f'task used forbidden tool: {set(names)-allowed}')
    for needed in ('browser_navigate', 'browser_type'):
        require(needed in names, f'task missing {needed}')
    require('browser_select' in names or 'browser_click' in names, 'task missing setting interaction')
    require('screen_screenshot' in names or 'browser_screenshot' in names, 'task missing screenshot')
    images = []
    for call in calls:
        call_id, name = call['call']['call_id'], call['call']['tool']
        result = completed.get(call_id, {}).get('result', {}).get('completed')
        require(result, f'task tool {name} did not complete')
        metadata = result.get('metadata', {})
        if metadata.get('image_object_key'):
            require(metadata.get('image_media_type') == 'image/png', f'{name} returned non-PNG image')
            images.append((call_id, name))
    require(images, 'task had no completed image call')
    for call_id, name in images:
        print(call_id, name)
elif mode == 'tool-list':
    calls, _ = tool_events(args[0], int(args[1]))
    for entry in calls:
        print(entry['call']['call_id'], entry['call']['tool'])
elif mode == 'reported-code':
    responses = [e['completion'] for kind, e in events(args[0], int(args[1]))
                 if kind == 'inference_completed']
    require(responses, 'task had no inference completion')
    parts = responses[-1]['message']['parts']
    text = ''.join(part.get('text', {}).get('text', '') for part in parts)
    match = re.search(r'CODE:\s*([A-Za-z0-9]{4,12})', text)
    require(match, 'task did not report CODE: <code>')
    print(match.group(1))
elif mode == 'check-record':
    record, user, code = json.loads(Path(args[0]).read_text()), args[1], args[2]
    require(record['login']['ok'] and record['login']['user'] == user, 'app record: login missing')
    require(record['setting']['theme'] == 'dark' and record['setting']['saved'], 'app record: dark theme not saved')
    normalize = lambda value: re.sub(r'[\s-]', '', value).upper()
    require(normalize(record['code']) == normalize(code), f'app record code {record["code"]} != reported {code}')
elif mode == 'summary':
    out, agent, session, provider, model, code = args
    folder = Path(out)
    images = []
    for index, p in enumerate(sorted(folder.glob('*.png')), 1):
        dimensions = json.loads((folder/f'png-{index}.json').read_text())
        images.append({'file': p.name, 'bytes': p.stat().st_size,
                       'sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
                       'width': dimensions['width'], 'height': dimensions['height']})
    summary = {'agent':agent,'session':session,'provider':provider,'model':model,
               'reported_code':code,'record':json.loads((folder/'record.json').read_text()),
               'images':images,'tool_calls':(folder/'tools.txt').read_text().splitlines(), 'verdict':'passed'}
    (folder/'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True)+'\n')
else:
    raise ValueError(f'unknown check helper: {mode}')
PY
py() { python3 -E -c "$CHECK_PY" "$@"; }
usage() { echo 'usage: desktop-agent-check.sh --swarm NAME --provider ID --model ID [--effort LEVEL] [--image base-desktop:dev] [--output DIR] [--app-ref master] [--keep-agent] [--swarmy PATH] [--dry-run]'; }
parse_args() {
    swarm= provider= model= effort= output= app_ref=master image=base-desktop:dev keep_agent=0 dry_run=0 swarmy=swarmy
    while (($#)); do
        case $1 in
            --swarm|--provider|--model|--effort|--output|--app-ref|--image|--swarmy)
                (($# >= 2)) || { usage >&2; return 2; }
                case $1 in
                    --swarm) swarm=$2;; --provider) provider=$2;; --model) model=$2;;
                    --effort) effort=$2;; --output) output=$2;; --app-ref) app_ref=$2;;
                    --image) image=$2;; --swarmy) swarmy=$2;;
                esac
                shift 2;;
            --keep-agent) keep_agent=1; shift;;
            --dry-run) dry_run=1; shift;;
            *) usage >&2; return 2;;
        esac
    done
    if [[ -z $swarm || -z $provider || -z $model ]]; then usage >&2; return 2; fi
    [[ $app_ref =~ ^[A-Za-z0-9._/-]+$ ]] || { echo 'invalid --app-ref' >&2; return 2; }
}
call() { timeout 1200 "$swarmy" --swarm "$swarm" "$@"; }
fail() {
    if (( ! failure_reported )); then
        echo "$1" >&2
        if ((created)); then
            echo "desktop check failed; agent kept. Delete with: swarmy --swarm $swarm agent delete $agent --yes" >&2
        fi
        failure_reported=1
    fi
    return 1
}
main_body() {
    local root password sha setup task read_command sid s1 s2 code out n call_id tool
    root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd) || return 1
    agent="desktop-check-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
    password="pw-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
    sha=$(sha256sum "$root/images/base-desktop/tests/desktop_check_app.py" | awk '{print $1}') || return 1
    setup="mkdir -p /home/agent/desktop-check && cd /home/agent/desktop-check && curl -fsSL https://raw.githubusercontent.com/pachuc/swarmy/$app_ref/images/base-desktop/tests/desktop_check_app.py -o app.py && echo '$sha  app.py' | sha256sum -c - && python3 app.py --background --port 8765 --record record.json --log app.log --username desk --password $password"
    task="A small web app is running in your sandbox at http://127.0.0.1:8765/. Use only the browser and screen tools (the browser_* and screen_* tools); do not use bash, file tools, or browser_evaluate. Log in with username desk and password $password. On the settings page, change Theme to Dark and save it. Then take a screenshot with screen_screenshot. The settings page shows a verification code drawn as an image; read it from your screenshot and finish with exactly one line: CODE: <the code>."
    read_command='cat /home/agent/desktop-check/record.json'
    if ((dry_run)); then
        printf 'planned: image show, agent create, setup run, browser/screen task run, exact readback, image downloads, cleanup\nsetup prompt: Use the bash tool to run exactly this command, then reply with its output: %s\ntask prompt: %s\nreadback prompt: Use the bash tool to run exactly this command, then reply with its output: %s\n' "$setup" "$task" "$read_command"
        return 0
    fi
    ((BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4))) || { echo 'bash 4.4 required' >&2; return 1; }
    command -v python3 >/dev/null && [[ -x $(command -v "$swarmy") ]] || { echo 'python3 and executable swarmy required' >&2; return 1; }
    if ! call --json image show "$image" >/dev/null; then
        echo "build it: swarmy --swarm NAME image build images/base-desktop --tag dev on a node with sudo" >&2
        return 1
    fi
    out=${output:-${XDG_STATE_HOME:-$HOME/.local/state}/swarmy/desktop-check/$agent}
    mkdir -p "$out" || return 1
    call --json models show "$provider/$model" > "$out/model.json" || return 1
    py model-image "$out/model.json" || return 1
    local -a create=(--json agent create "$agent" --image "$image" --provider "$provider" --model "$model")
    if [[ -n $effort ]]; then create+=(--effort "$effort"); fi
    call "${create[@]}" > "$out/agent-create.json" || return 1
    created=1
    call --json run --agent "$agent" "Use the bash tool to run exactly this command, then reply with its output: $setup" > "$out/setup-run.jsonl" || return 1
    sid=$(py session-id "$out/setup-run.jsonl") || return 1
    call session show "$sid" --json > "$out/session-setup.jsonl" || return 1
    if ! py exact-bash "$out/session-setup.jsonl" 0 "$setup" listening > "$out/setup-output.txt"; then
        fail 'setup failed; if checksum differs, checkout and --app-ref differ'
        return 1
    fi
    s1=$(py max-seq "$out/session-setup.jsonl") || return 1
    call --json run --agent "$agent" "$task" > "$out/task-run.jsonl" || return 1
    call session show "$sid" --json > "$out/session-task.jsonl" || return 1
    py window-tools "$out/session-task.jsonl" "$s1" > "$out/images.txt" || return 1
    py tool-list "$out/session-task.jsonl" "$s1" > "$out/tools.txt" || return 1
    code=$(py reported-code "$out/session-task.jsonl" "$s1") || return 1
    s2=$(py max-seq "$out/session-task.jsonl") || return 1
    call --json run --agent "$agent" "Use the bash tool to run exactly this command, then reply with its output: $read_command" > "$out/readback-run.jsonl" || return 1
    call session show "$sid" --json > "$out/session-readback.jsonl" || return 1
    py exact-bash "$out/session-readback.jsonl" "$s2" "$read_command" > "$out/record.json" || return 1
    local expected kind code_visible=0
    expected=$(python3 -E -c 'import json,sys; print(json.load(open(sys.argv[1]))["code"])' "$out/record.json") || return 1
    n=0
    while read -r call_id tool; do
        [[ -n $call_id ]] || continue
        n=$((n+1))
        local file="$out/$(printf '%02d' "$n")-$tool-$call_id.png"
        call session image "$sid" "$call_id" --output "$file" > "$out/image-$n.json" || return 1
        kind=browser
        if [[ $tool == screen_screenshot ]]; then kind=screen; fi
        python3 -E "$root/images/base-desktop/tests/png_check.py" "$file" --kind "$kind" > "$out/png-$n.json" || return 1
        if python3 -E "$root/images/base-desktop/tests/png_check.py" "$file" --kind "$kind" --expect-code "$expected" > "$out/png-$n-code.json" 2> "$out/png-$n-code.err"; then
            code_visible=$((code_visible+1))
        fi
    done < "$out/images.txt"
    ((n > 0)) || { fail 'no screenshot downloaded'; return 1; }
    ((code_visible >= 1)) || { fail 'no screenshot shows the verification code'; return 1; }
    py check-record "$out/record.json" desk "$code" || return 1
    py summary "$out" "$agent" "$sid" "$provider" "$model" "$code" || return 1
    if (( ! keep_agent )); then call agent delete "$agent" --yes > "$out/agent-delete.json" || return 1; fi
    created=0
    echo "desktop agent check passed; evidence in $out"
}
main() {
    parse_args "$@" || return $?
    created=0 failure_reported=0
    main_body "$@" || fail "desktop check failed"
}
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main "$@"; fi
