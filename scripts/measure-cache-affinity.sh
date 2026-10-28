#!/usr/bin/env bash
# Operator-only: consumes real provider quota. The two entries must not share
# a prompt cache: use different accounts, workspaces, or providers (for example
# a ChatGPT login and an OpenAI API key). Both must serve --model.
# Recommend --gap 180 --turns 8: one entry stays warm across a 180-second
# gap while alternating entries revisit a cache twice as old, and the second
# entry starts cold.
# For --mode without, set cache_affinity_secs = 0 in each entry's
# [selection.custom_providers.PROVIDER] table, restart workers, then remove it.
set +x
set -euo pipefail
usage() {
    cat <<'EOF'
Usage: measure-cache-affinity.sh --mode with|without --entries PROVIDER/LABEL[=MODEL],PROVIDER/LABEL[=MODEL] --model ID [--turns 20] [--gap 0] [--dry-run]
For --mode without, set cache_affinity_secs = 0 in each entry's
[selection.custom_providers.PROVIDER] table and restart workers; remove the
override after measuring. The session starts on the first entry; --model is
the agent's model, and a step's =MODEL overrides it for that step.
Use --gap 180 --turns 8 for a clear comparison of warm and expired caches.
EOF
}
mode= entries= model= turns=20 gap=0 dry=0
while (($#)); do
    case "$1" in
        --mode|--entries|--model|--turns|--gap)
            (($# >= 2)) || { usage >&2; exit 2; }
            case "$1" in
                --mode) mode=$2;; --entries) entries=$2;; --model) model=$2;; --turns) turns=$2;; --gap) gap=$2;;
            esac
            shift 2;;
        --dry-run) dry=1; shift;;
        --help) usage; exit 0;;
        *) usage >&2; exit 2;;
    esac
done
[[ $mode == with || $mode == without ]] || { usage >&2; exit 2; }
[[ $entries == *,* ]] || { usage >&2; exit 2; }
a=${entries%%,*}; b=${entries#*,}
[[ $a == ?*/?* && $b == ?*/?* && $b != *,* && $a != "$b" && -n $model && $turns =~ ^[1-9][0-9]*$ && $gap =~ ^[0-9]+$ ]] || { usage >&2; exit 2; }
if [[ -n ${CI:-} && $dry == 0 ]]; then
    echo 'Real provider measurements must not run in CI.' >&2; exit 2
fi
bin=${SWARMY_BIN:-swarmy}
run() {
    if ((dry)); then
        { printf '+ '; printf '%q ' "$bin" "$@"; printf '\n'; } >&2
    else
        "$bin" "$@" >&2
    fi
}
capture() {
    if ((dry)); then run "$@"; else "$bin" "$@"; fi
}
wait_gap() {
    if ((gap > 0)); then
        if ((dry)); then printf '+ sleep %s\n' "$gap" >&2; else sleep "$gap"; fi
    fi
}
if ((!dry)); then
    # auth ls exposes labels and status, not key values. Refuse to start a
    # quota-consuming measurement if either named entry is absent.
    listing=$(capture auth ls)
    for step in "$a" "$b"; do
        entry=${step%%=*}
        if ! awk -F '\t' -v provider="${entry%%/*}" -v label="${entry#*/}" '$1 == provider && $3 == label { found = 1 } END { exit !found }' <<< "$listing"; then
            printf 'Missing stored entry: %s\n' "$entry" >&2
            exit 2
        fi
    done
fi
tmp=$(mktemp -d)
route=cache-affinity-probe-$mode
agent=cache-probe-$mode-$(date +%s)
cleanup() {
    run agent delete "$agent" --yes || true
    run auth routes rm "$route" || true
    rm -rf "$tmp"
}
trap cleanup EXIT
python3 - "$tmp/prompt.txt" <<'PY'
import sys
with open(sys.argv[1], 'w') as out:
    for i in range(600):
        out.write(f'Reference {i:04d}: Explain the stable vocabulary and definitions used by this session.\n')
PY
run auth routes set "$route" "$a" "$b"
run agent create "$agent" --provider "${a%%/*}" --model "$model" --route "$route" --system-prompt-file "$tmp/prompt.txt"
old_input=0 old_cached=0 old_a=0 old_b=0 total_input=0 total_cached=0
printf '| Turn | Entry | Input delta | Cached delta | Cached %% |\n|---:|---|---:|---:|---:|\n'
for ((i=1; i<=turns; i++)); do
    if ((i % 2)); then first=$a; second=$b; else first=$b; second=$a; fi
    run auth routes set "$route" "$first" "$second"
    run run --agent "$agent" "Turn $i. Reply with the single word ok."
    if ((dry)); then capture --json agent show "$agent"; wait_gap; continue; fi
    json=$(capture --json agent show "$agent")
    read -r input cached count_a count_b < <(python3 -c '
import json,sys
x=json.load(sys.stdin)
u=x.get("usage") or {}
e={r["entry"]:r.get("completions",0) for r in x.get("entries",[])}
print(u.get("input_tokens",0),u.get("cached_input_tokens",0),e.get(sys.argv[1],0),e.get(sys.argv[2],0))
' "${a%%=*}" "${b%%=*}" <<< "$json")
    di=$((input-old_input)); dc=$((cached-old_cached))
    if ((count_a > old_a)); then used=$a; elif ((count_b > old_b)); then used=$b; else used=unknown; fi
    pct=0; if ((di > 0)); then pct=$((100*dc/di)); fi
    printf '| %d | %s | %d | %d | %d%% |\n' "$i" "$used" "$di" "$dc" "$pct"
    old_input=$input; old_cached=$cached; old_a=$count_a; old_b=$count_b
    total_input=$((total_input+di)); total_cached=$((total_cached+dc))
    wait_gap
done
if ((!dry)); then
    pct=0; if ((total_input > 0)); then pct=$((100*total_cached/total_input)); fi
    printf '| **Total (%s)** | | **%d** | **%d** | **%d%%** |\n' "$mode" "$total_input" "$total_cached" "$pct"
fi
