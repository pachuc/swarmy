#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bash -n "$repo/scripts/node-suites/gpu-proof.sh"
source "$repo/scripts/node-suites/gpu-proof.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
base=(--swarm hz --provider p --model m --image swarmy-dev:hz)
gpu_parse_args "${base[@]}"
[[ $gpu == shared && $memory == 4096 ]]
gpu_parse_args "${base[@]}" --gpu dedicated --memory 12288 --effort low --keep-agent --dry-run
[[ $gpu == dedicated && $memory == 12288 && $effort == low && $keep_agent == 1 && $dry_run == 1 ]]
reject() {
    local status=0
    gpu_parse_args "$@" 2> "$tmp/error" || status=$?
    [[ $status == 2 ]] && grep -q '^usage: gpu-proof.sh ' "$tmp/error"
}
reject
reject --provider p --model m --image swarmy-dev:hz
reject --swarm hz --model m --image swarmy-dev:hz
reject --swarm hz --provider p --image swarmy-dev:hz
reject --swarm hz --provider p --model m
reject --swarm hz --provider p --model m --image swarmy-dev
reject "${base[@]}" --gpu none
reject "${base[@]}" --memory 0
reject "${base[@]}" --memory 12x
reject "${base[@]}" --bogus
reject "${base[@]}" --swarm
mkdir -p "$tmp/bin"
printf '#!/bin/sh\nexit 99\n' > "$tmp/bin/swarmy"
chmod +x "$tmp/bin/swarmy"
PATH="$tmp/bin:$PATH" gpu_main "${base[@]}" --dry-run > "$tmp/plan"
grep -q 'glxinfo -B' "$tmp/plan"
grep -q 'nvidia-smi' "$tmp/plan"
grep -q 'screen_screenshot' "$tmp/plan"
printf 'nvidia\nOpenGL renderer string: NVIDIA L4/PCIe/SSE2\nNVIDIA L4, 310\nGEARS\n352\n/usr/lib/xorg/Xorg\n' > "$tmp/probe"
gpu_py probe "$tmp/probe" > "$tmp/result"
grep -q '"processes_listed": true' "$tmp/result"
sed 's@NVIDIA L4/PCIe/SSE2@llvmpipe (LLVM 17.0.6, 256 bits)@' "$tmp/probe" > "$tmp/invalid"
if gpu_py probe "$tmp/invalid" >/dev/null 2>&1; then exit 1; fi
sed '1s/nvidia/software/' "$tmp/probe" > "$tmp/invalid"
if gpu_py probe "$tmp/invalid" >/dev/null 2>&1; then exit 1; fi
sed '5s/352/0/' "$tmp/probe" > "$tmp/invalid"
if gpu_py probe "$tmp/invalid" >/dev/null 2>&1; then exit 1; fi
printf 'gpu-proof: ok\n'
