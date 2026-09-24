#!/bin/sh
# Run Codex under one subscription lane's isolated state directory.
set -eu
provider= model= effort= workspace= prompt_file= json=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --provider|--model|--effort|--workspace|--prompt-file)
            [ "$#" -ge 2 ] || { echo "missing value for $1" >&2; exit 2; }
            case "$1" in
                --provider) provider=$2 ;;
                --model) model=$2 ;;
                --effort) effort=$2 ;;
                --workspace) workspace=$2 ;;
                --prompt-file) prompt_file=$2 ;;
            esac
            shift 2 ;;
        --json) json=true; shift ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done
[ "$provider" = chatgpt ] || { echo "codex-daytona lane requires chatgpt" >&2; exit 2; }
[ -n "$model" ] && [ -n "$effort" ] && [ -d "$workspace" ] && [ -f "$prompt_file" ] && [ "$json" = true ] || {
    echo "model, effort, workspace, --json, and prompt file are required" >&2; exit 2;
}
export XDG_STATE_HOME=${CODEX_LANE_STATE:-"$HOME/.local/state/cdx-lane2"}
target=${CARGO_TARGET_DIR:-"$HOME/.cargo-target"}
registry=${CARGO_HOME:-"$HOME/.cargo"}/registry
cold=true
for dir in "$target" "$registry"; do
    if [ -d "$dir" ] && [ -n "$(find "$dir" -mindepth 1 -print -quit)" ]; then
        cold=false
    fi
done
printf '{"event":"benchmark_cache","cold":%s}\n' "$cold"
if [ "${BENCH_DRY_RUN:-}" = 1 ]; then
    printf 'XDG_STATE_HOME=%s codex exec --json -m %s -c model_reasoning_effort=%s --cd %s <prompt>\n' \
        "$XDG_STATE_HOME" "$model" "$effort" "$workspace"
    exit 0
fi
exec codex exec --json -m "$model" -c "model_reasoning_effort=$effort" --cd "$workspace" "$(cat "$prompt_file")"
