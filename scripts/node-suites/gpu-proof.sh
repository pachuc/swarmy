#!/usr/bin/env bash
# Prove GPU rendering through a real agent from the operator's machine against a swarm with a GPU group.
# Creates and deletes a throwaway agent. Autoscale may buy a GPU machine and removes it about
# 30 minutes after the agent is deleted.
set +x
set -euo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)/desktop-agent-check.sh"
read -r -d '' GPU_PY <<'PY' || :
import hashlib, json, re, sys
from pathlib import Path

def require(ok, message):
    if not ok:
        sys.exit(message)

mode, *args = sys.argv[1:]
if mode == 'probe':
    lines = Path(args[0]).read_text().splitlines()
    require(lines and lines[0].strip() == 'nvidia', 'renderer file did not say nvidia')
    renderer_lines = [i for i, line in enumerate(lines) if 'OpenGL renderer string:' in line]
    require(renderer_lines, 'glxinfo renderer missing')
    index = renderer_lines[0]
    renderer = lines[index].split('OpenGL renderer string:', 1)[1].strip()
    require(renderer and 'llvmpipe' not in renderer.lower(), 'software or empty OpenGL renderer')
    require(index + 1 < len(lines), 'nvidia-smi card missing')
    card_line = lines[index + 1].strip()
    match = re.fullmatch(r'([^,]+),\s*(\d+)', card_line)
    require(match and match.group(1).strip(), 'nvidia-smi card and memory missing')
    require('GEARS' in lines[index + 2:], 'glxgears marker missing')
    gears = lines.index('GEARS', index + 2)
    require(gears + 1 < len(lines) and re.fullmatch(r'\d+', lines[gears + 1].strip())
            and int(lines[gears + 1]) > 0, 'no GPU memory in use while glxgears runs')
    print(json.dumps({'renderer': renderer, 'card': match.group(1).strip(),
                      'memory_used_mib': int(match.group(2)),
                      'memory_during_gears_mib': int(lines[gears + 1]),
                      'processes_listed': bool(re.search(r'glxgears|Xorg', '\n'.join(lines[gears + 2:]), re.I))}))
elif mode == 'windows':
    # Session JSON events and completion payloads follow the desktop proof's tool_events layout.
    calls, completed = tool_events(args[0], int(args[1]))
    for name in ('screen_windows', 'screen_screenshot'):
        matches = [entry for entry in calls if entry['call']['tool'] == name]
        require(matches, f'missing {name} call')
        results = [completed.get(entry['call']['call_id'], {}).get('result', {}).get('completed')
                   for entry in matches]
        require(results[-1], f'{name} did not complete')
        if name == 'screen_windows':
            output = results[-1].get('output', '')
            require('glxgears' in (output if isinstance(output, str) else json.dumps(output)).lower(),
                    'screen_windows did not list glxgears')
elif mode == 'summary':
    out, agent, sid, provider, model, gpu, memory, started, probed = args
    folder = Path(out)
    probe = json.loads((folder/'probe.json').read_text())
    screen = folder/'screen.png'
    png = json.loads((folder/'png.json').read_text())
    summary = {'agent': agent, 'session': sid, 'provider': provider, 'model': model,
               'gpu': gpu, 'memory_mib': int(memory), **probe,
               'screenshot': {'file': screen.name, 'bytes': screen.stat().st_size,
                              'sha256': hashlib.sha256(screen.read_bytes()).hexdigest(),
                              'width': png['width'], 'height': png['height']},
               'first_command_seconds': int(probed) - int(started), 'verdict': 'passed'}
    (folder/'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True)+'\n')
else:
    raise ValueError(f'unknown GPU check helper: {mode}')
PY
gpu_py() { python3 -E -c "${CHECK_PY%%mode, *}$GPU_PY" "$@"; }
gpu_usage() { echo 'usage: gpu-proof.sh --swarm NAME --provider ID --model ID --image NAME:TAG [--effort LEVEL] [--gpu shared|dedicated] [--memory MIB] [--output DIR] [--keep-agent] [--swarmy PATH] [--dry-run]'; }
gpu_parse_args() {
    swarm= provider= model= effort= image= output= gpu=shared memory=4096 keep_agent=0 dry_run=0 swarmy=swarmy
    while (($#)); do
        case $1 in
            --swarm|--provider|--model|--image|--effort|--gpu|--memory|--output|--swarmy)
                (($# >= 2)) && [[ -n $2 && $2 != --* ]] || { gpu_usage >&2; return 2; }
                case $1 in
                    --swarm) swarm=$2;; --provider) provider=$2;; --model) model=$2;;
                    --image) image=$2;; --effort) effort=$2;; --gpu) gpu=$2;;
                    --memory) memory=$2;; --output) output=$2;; --swarmy) swarmy=$2;;
                esac
                shift 2;;
            --keep-agent) keep_agent=1; shift;;
            --dry-run) dry_run=1; shift;;
            *) gpu_usage >&2; return 2;;
        esac
    done
    if [[ -z $swarm || -z $provider || -z $model || $image != *:* ||
          ( $gpu != shared && $gpu != dedicated ) || ! $memory =~ ^[0-9]+$ ]] ||
          ((10#$memory < 1)); then gpu_usage >&2; return 2; fi
}
gpu_fail() {
    if (( ! failure_reported )); then
        echo "$1" >&2
        if ((created)); then
            echo "gpu proof failed; agent kept. Delete with: swarmy --swarm $swarm agent delete $agent --yes" >&2
        fi
        failure_reported=1
    fi
    return 1
}
gpu_main_body() {
    local root probe shot out sid s1 call_id started probed
    local -a create
    root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd) || return 1
    probe='export DISPLAY=${DISPLAY:-:99}; cat /run/swarmy-display/renderer; glxinfo -B | grep -i "renderer string"; nvidia-smi --query-gpu=name,memory.used --format=csv,noheader,nounits; (setsid glxgears >/tmp/gpu-proof-glxgears.log 2>&1 &); sleep 5; echo GEARS; nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits; nvidia-smi'
    shot='Call the screen_windows tool, then the screen_screenshot tool, each alone in its own response. Then reply with exactly DONE.'
    if ((dry_run)); then
        printf 'planned: agent create with --gpu %s --memory %s, exact bash probe, screen_windows and screen_screenshot, image download, agent delete\nprobe prompt: Use the bash tool with yield_seconds: 60 to run exactly this command, then reply with its output: %s\nscreenshot prompt: %s\n' "$gpu" "$memory" "$probe" "$shot"
        return 0
    fi
    command -v python3 >/dev/null && [[ -x $(command -v "$swarmy") ]] || { echo 'python3 and executable swarmy required' >&2; return 1; }
    if ! call --json image show "$image" >/dev/null; then
        echo "image $image must be registered on the swarm" >&2
        return 1
    fi
    agent="gpu-proof-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
    out=${output:-${XDG_STATE_HOME:-$HOME/.local/state}/swarmy/gpu-proof/$agent}
    mkdir -p "$out" || return 1
    create=(--json agent create "$agent" --image "$image" --provider "$provider" --model "$model" --memory "$memory" --gpu "$gpu" --gpu-reason "gpu proof $agent")
    if [[ -n $effort ]]; then create+=(--effort "$effort"); fi
    call "${create[@]}" > "$out/agent-create.json" || return 1
    created=1
    started=$(date -u +%s)
    call --json run --agent "$agent" "Use the bash tool with yield_seconds: 60 to run exactly this command, then reply with its output: $probe" > "$out/probe-run.jsonl" || return 1
    probed=$(date -u +%s)
    sid=$(py session-id "$out/probe-run.jsonl") || return 1
    call session show "$sid" --json > "$out/session-probe.jsonl" || return 1
    py exact-bash "$out/session-probe.jsonl" 0 "$probe" GEARS > "$out/probe-output.txt" || return 1
    gpu_py probe "$out/probe-output.txt" > "$out/probe.json" || return 1
    s1=$(py max-seq "$out/session-probe.jsonl") || return 1
    call --json run --agent "$agent" "$shot" > "$out/shot-run.jsonl" || return 1
    call session show "$sid" --json > "$out/session-shot.jsonl" || return 1
    py tool-list "$out/session-shot.jsonl" "$s1" > "$out/tools.txt" || return 1
    gpu_py windows "$out/session-shot.jsonl" "$s1" || return 1
    call_id=$(awk '$2 == "screen_screenshot" {id=$1} END {print id}' "$out/tools.txt")
    [[ -n $call_id ]] || return 1
    call session image "$sid" "$call_id" --output "$out/screen.png" > "$out/image.json" || return 1
    python3 -E "$root/images/base-desktop/tests/png_check.py" "$out/screen.png" --kind screen > "$out/png.json" || return 1
    gpu_py summary "$out" "$agent" "$sid" "$provider" "$model" "$gpu" "$memory" "$started" "$probed" || return 1
    if ! python3 -E -c 'import json,sys; sys.exit(not json.load(open(sys.argv[1]))["processes_listed"])' "$out/probe.json"; then
        echo 'note: nvidia-smi inside the sandbox lists no process; check nvidia-smi on the GPU host'
    fi
    if (( ! keep_agent )); then call agent delete "$agent" --yes > "$out/agent-delete.json" || return 1; fi
    created=0
    echo "gpu proof passed; evidence in $out"
}
gpu_main() {
    gpu_parse_args "$@" || return $?
    created=0 failure_reported=0
    gpu_main_body || gpu_fail 'gpu proof failed'
}
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then gpu_main "$@"; fi
