#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bash -n "$repo/scripts/desktop-agent-check.sh"
source "$repo/scripts/desktop-agent-check.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/fixtures"
cp "$repo/scripts/fixtures/desktop-check/"* "$tmp/fixtures/"
cat > "$tmp/bin/swarmy" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$STUB_ARGS"
args=" $* "
case $args in
  *' image show '*) echo '{}';;
  *' models show '*)
    if [[ ${STUB_TEXT_ONLY:-0} == 1 ]]; then echo '{"input_modalities":["text"]}';
    else echo '{"input_modalities":["text","image"]}'; fi;;
  *' agent create '*) echo '{}';;
  *' run --agent '*)
    stage=$(cat "$STUB_STAGE" 2>/dev/null || echo 0)
    stage=$((stage+1)); echo "$stage" > "$STUB_STAGE"
    if ((stage == 1)); then printf '%s\n' "${*: -1}" | sed 's/^Use the bash tool with yield_seconds: 60 to run exactly this command, then reply with its output: //' > "$STUB_SETUP"; fi
    echo '{"event":"session_opened","session_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}';;
  *' session show '*)
    stage=$(cat "$STUB_STAGE")
    python3 - "$STUB_FIXTURES/show-$stage.jsonl" "$STUB_SETUP" <<'PY'
import pathlib, sys
text=pathlib.Path(sys.argv[1]).read_text()
print(text.replace('__SETUP__',pathlib.Path(sys.argv[2]).read_text().strip()),end='')
PY
    ;;
  *' session image '*)
    while (($#)); do if [[ $1 == --output ]]; then file=$2; break; fi; shift; done
    python3 - "$file" "$args" <<'PY'
import os,struct,sys,zlib
sys.path.insert(0, os.environ['STUB_APP_SOURCE'])
from desktop_check_app import GLYPHS, PAD, CELL
w,h=1280,800
rows=[bytearray(b'\xff\xff\xff'*w) for _ in range(h)]
if os.environ.get('STUB_BLANK'):
    rows=[bytearray(b'\0\0\0'*w) for _ in range(h)]
else:
    for y in range(10,90):
        rows[y][10*3:110*3]=b'\x44\x88\xcc'*100
    if ' screen-image ' in sys.argv[2] and not os.environ.get('STUB_NO_CODE'):
        for i,char in enumerate('K7MWX3'):
            for y,line in enumerate(GLYPHS[char]):
                for x,bit in enumerate(line):
                    if bit == '1':
                        for dy in range(CELL):
                            start=(50+(PAD+i*6+x)*CELL)*3
                            rows[60+(PAD+y)*CELL+dy][start:start+CELL*3]=b'\x11\x11\x11'*CELL
raw=b''.join(b'\0'+row for row in rows)
def chunk(kind,payload): return struct.pack('>I',len(payload))+kind+payload+struct.pack('>I',zlib.crc32(kind+payload))
open(sys.argv[1],'wb').write(b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',w,h,8,2,0,0,0))+chunk(b'IDAT',zlib.compress(raw))+chunk(b'IEND',b''))
PY
    echo '{}';;
  *' agent delete '*) echo '{}';;
  *) echo "unexpected swarmy call: $args" >&2; exit 1;;
esac
EOF
chmod +x "$tmp/bin/swarmy"
export STUB_APP_SOURCE="$repo/images/base-desktop/tests"
export STUB_ARGS="$tmp/argv" STUB_STAGE="$tmp/stage" STUB_SETUP="$tmp/setup" STUB_FIXTURES="$tmp/fixtures"
export SWARMY_API_TOKEN=token-secret-3c9e
run_case() {
    rm -f "$STUB_STAGE" "$STUB_ARGS"
    local name=$1; shift
    if bash "$repo/scripts/desktop-agent-check.sh" --swarm test --provider fake --model fake --swarmy "$tmp/bin/swarmy" --output "$tmp/out-$name" "$@" > "$tmp/$name.stdout" 2> "$tmp/$name.stderr"; then return 0; else return 1; fi
}
run_case positive
grep -q "Use the bash tool with yield_seconds: 60 to run exactly this command" "$STUB_ARGS"
grep -q "chromium about:blank.*curl -fsS --retry 60 --retry-connrefused" "$STUB_SETUP"
[[ -s "$tmp/out-positive/summary.json" && -s "$tmp/out-positive/01-browser_screenshot-browser-image.png" ]]
[[ -s "$tmp/out-positive/png-1-code.err" && -s "$tmp/out-positive/png-2-code.json" ]]
grep -q 'code not visible' "$tmp/out-positive/png-1-code.err"
! grep -r -q 'token-secret-3c9e' "$tmp" --exclude=desktop-agent-check.sh
python3 - "$tmp/fixtures" <<'PY'
import json,pathlib,sys
folder=pathlib.Path(sys.argv[1]); original={p.name:p.read_text() for p in folder.glob('*.jsonl')}
(folder/'original.json').write_text(json.dumps(original))
PY
restore() { python3 - "$tmp/fixtures" <<'PY'
import json,pathlib,sys
folder=pathlib.Path(sys.argv[1]); original=json.loads((folder/'original.json').read_text())
for name,text in original.items(): (folder/name).write_text(text)
PY
}
mutate() { python3 - "$tmp/fixtures/show-$1.jsonl" "$2" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]); change=sys.argv[2]
rows=list(map(json.loads,p.read_text().splitlines()))
if change in ('bash','browser_evaluate'): rows[3]['tool_call_requested']['call']['tool']=change
elif change=='code': rows[-1]['inference_completed']['completion']['message']['parts'][0]['text']['text']='CODE: AAAAAA'
elif change=='theme': rows[-1]['tool_call_completed']['result']['completed']['output']=rows[-1]['tool_call_completed']['result']['completed']['output'].replace('dark','light')
elif change=='setup': rows[1]['tool_call_requested']['call']['arguments']['command']='__SETUP__x'
elif change=='view_source': rows[3]['tool_call_requested']['call']['arguments']['url']='view-source:http://127.0.0.1:8765/settings'
elif change in ('select_recovered', 'select_last_failed'):
    requested = json.loads(json.dumps(rows[7]))
    completed = json.loads(json.dumps(rows[8]))
    requested['tool_call_requested']['call']['call_id'] = 'select-failed'
    completed['tool_call_completed']['call_id'] = 'select-failed'
    completed['tool_call_completed']['result'] = {'failed': {'error': 'selection failed'}}
    if change == 'select_recovered':
        rows[7:7] = [requested, completed]
    else:
        rows[9:9] = [requested, completed]

p.write_text(''.join(json.dumps(row)+'\n' for row in rows))
PY
}
restore
mutate 2 select_recovered
run_case select_recovered
restore
mutate 2 select_last_failed
if run_case select_last_failed; then echo 'last failed select unexpectedly passed' >&2; exit 1; fi
grep -q 'last task tool browser_select did not complete' "$tmp/select_last_failed.stderr"
export PYTHONOPTIMIZE=1
for variant in bash browser_evaluate code theme setup blank no_code view_source; do
    restore
    case $variant in
        bash|browser_evaluate|code|view_source) stage=2;; theme) stage=3;; setup) stage=1;;
    esac
    if [[ $variant != blank && $variant != no_code ]]; then mutate "$stage" "$variant"; fi
    if [[ $variant == blank ]]; then export STUB_BLANK=1; else unset STUB_BLANK || true; fi
    if [[ $variant == no_code ]]; then export STUB_NO_CODE=1; else unset STUB_NO_CODE || true; fi
    if run_case "$variant"; then echo "negative case $variant unexpectedly passed" >&2; exit 1; fi
    case $variant in
        bash|browser_evaluate) grep -q 'forbidden tool' "$tmp/$variant.stderr";;
        code) grep -q 'app record code K7MWX3 != reported AAAAAA' "$tmp/$variant.stderr";;
        no_code) grep -q 'no screenshot shows the verification code' "$tmp/$variant.stderr";;
        theme) grep -q 'dark theme not saved' "$tmp/$variant.stderr";;
        setup) grep -q 'exact bash command differs' "$tmp/$variant.stderr";;
        blank) grep -q 'blank: one color' "$tmp/$variant.stderr";;
        view_source) grep -q 'task navigated outside the app' "$tmp/$variant.stderr";;
    esac
done
unset PYTHONOPTIMIZE
for variant in setup blank; do
    grep -q 'agent kept. Delete with:' "$tmp/$variant.stderr"
done
unset STUB_BLANK STUB_NO_CODE || true
if STUB_TEXT_ONLY=1 run_case text_only; then
    echo 'text-only model accepted' >&2; exit 1
fi
grep -q 'model has no image input; screenshots would be omitted' "$tmp/text_only.stderr"
! grep -q ' agent create ' "$STUB_ARGS"
restore
# Dry run must not call the stub.
rm -f "$STUB_ARGS"
main --swarm test --provider fake --model fake --swarmy "$tmp/bin/swarmy" --dry-run > "$tmp/dry.stdout"
[[ ! -e $STUB_ARGS ]]
if main --swarm test --provider fake --swarmy "$tmp/bin/swarmy" > "$tmp/missing.stdout" 2>&1; then
    echo 'missing model accepted' >&2; exit 1
else
    rc=$?
    [[ $rc -eq 2 ]]
fi
! grep -r -q 'token-secret-3c9e' "$tmp"
echo 'desktop-agent-check: ok (positive, recovered select, nine negative cases, text-only preflight, dry-run, required flags)'
