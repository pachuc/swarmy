#!/usr/bin/env bash
# Exercise real CLI checkpoints and measure a separate collector process.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
source "$repo/.dev/env"
binary="$repo/target/debug/swarmy"
api_binary="$repo/target/debug/swarmy-api"
node_binary="$repo/target/debug/swarmyd"
[[ -x $binary ]] || { echo 'Build swarmy-cli --bins first.' >&2; exit 1; }
[[ -x $api_binary ]] || { echo 'Build swarmy-api first.' >&2; exit 1; }
[[ -x $node_binary ]] || { echo 'Build swarmyd first.' >&2; exit 1; }
# Image builds and collection run through the API; volume tools moved to the
# node daemon. The API needs a token, generated here for this isolated run.
export SWARMY_API_URL="${SWARMY_API_URL:-http://127.0.0.1:18743}"
export SWARMY_API_LISTEN="${SWARMY_API_URL#http://}"
export SWARMY_API_TOKEN="${SWARMY_API_TOKEN:-$(python3 -c 'import secrets; print(secrets.token_hex(32))')}"
work=$(mktemp -d /tmp/swarmy-gc-bench.XXXXXX)
export SWARMY_STORE_DIRECTORY="gc-bench-$(date +%s)-$$"
export SWARMY_S3_BUCKET="$SWARMY_STORE_DIRECTORY"
# The grace window is a server-side collection setting now; client
# SWARMY_GC_GRACE_SECONDS would be ignored, so pass --grace-seconds on the
# run request instead. This short grace is safe only for this isolated
# workload with no pending writes.
export SWARMY_VOLUME_SNAPSHOT_RETENTION=3
# Explicit checkpoints control retention in this workload.
export SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS=86400
volume=''
server=''
api=''
cleanup() {
    if [[ -n $volume ]]; then
        sudo -E "$node_binary" vol detach "$volume" || {
            if [[ -n $server ]]; then sudo kill -TERM "$server" || true; fi
        }
    fi
    if [[ -n $server ]]; then wait "$server" || true; fi
    if [[ -n $api ]]; then kill -TERM "$api" || true; wait "$api" || true; fi
    echo "Evidence and isolated namespace: $work, $SWARMY_STORE_DIRECTORY"
}
trap cleanup EXIT
curl --fail --silent --show-error --aws-sigv4 "aws:amz:${SWARMY_S3_REGION}:s3" \
    --user "$SWARMY_S3_ACCESS_KEY:$SWARMY_S3_SECRET_KEY" \
    -X PUT "$SWARMY_S3_ENDPOINT/$SWARMY_S3_BUCKET" >/dev/null
"$api_binary" > "$work/api.log" 2>&1 &
api=$!
for _ in $(seq 1 100); do
    if curl --fail --silent "$SWARMY_API_URL/v1/health" > /dev/null; then break; fi
    kill -0 "$api" || { echo 'API failed to start; see api.log'; cat "$work/api.log"; exit 1; }
    sleep 0.2
done
mkdir -p "$work/.swarmy" "$work/gc-seed/rootfs"
printf 'store_directory = "%s"\n' "$SWARMY_STORE_DIRECTORY" > "$work/.swarmy/config.toml"
cat > "$work/gc-seed/recipe.toml" <<'RECIPE'
disk_size = 536870912
source_date_epoch = 1714003200
[source]
kind = "directory"
path = "rootfs"
RECIPE
cd "$work"
sudo -E "$binary" --json image build "$work/gc-seed" --tag test > "$work/image.json"
volume=$(sudo -E "$node_binary" --json vol create gc-seed:test | python3 -c 'import json,sys; print(json.load(sys.stdin)["volume_id"])')
# Attach selects an unused device. Read its path from the ready event.
sudo -E "$node_binary" --json vol attach "$volume" > "$work/attach.jsonl" 2> "$work/attach.log" &
server=$!
for _ in $(seq 1 300); do
    if [[ -s $work/attach.jsonl ]]; then break; fi
    sudo kill -0 "$server"
    sleep 0.1
done
device=$(python3 -c 'import json,sys; print(json.loads(open(sys.argv[1]).readline())["device"])' "$work/attach.jsonl")
for pass in $(seq 1 22); do
    sudo dd if=/dev/urandom of="$device" bs=1M count=256 oflag=direct conv=notrunc status=none
    sudo -E "$node_binary" --json vol checkpoint "$volume" >> "$work/checkpoints.jsonl"
    echo "Checkpoint $pass/22"
done
# This short grace is safe only for this isolated workload with no pending writes.
sleep 2
/usr/bin/time -v -o "$work/dry.time" "$binary" --json gc --dry-run --grace-seconds 1 > "$work/dry.json"
/usr/bin/time -v -o "$work/gc.time" "$binary" --json gc --grace-seconds 1 > "$work/gc.json"
cat "$work/dry.json" "$work/gc.json"
cat "$work/gc.time"
"$binary" --json gc --grace-seconds 1 > "$work/second.json"
python3 - "$work" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
dry, run, second = [json.loads((root / name).read_text()) for name in ['dry.json', 'gc.json', 'second.json']]
assert dry['scanned'] >= 22000
assert dry['deleted'] == 0 and dry['candidates'] == run['deleted']
assert run['deleted'] >= 19000 and second['deleted'] == 0
print('Benchmark assertions passed')
PY
