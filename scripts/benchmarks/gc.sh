#!/usr/bin/env bash
# Exercise real CLI checkpoints and measure a separate collector process.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
source "$repo/.dev/env"
binary="$repo/target/debug/swarmy"
[[ -x $binary ]] || { echo 'Build swarmy-cli --bins first.' >&2; exit 1; }
work=$(mktemp -d /tmp/swarmy-gc-bench.XXXXXX)
export SWARMY_STORE_DIRECTORY="gc-bench-$(date +%s)-$$"
export SWARMY_S3_BUCKET="$SWARMY_STORE_DIRECTORY"
export SWARMY_GC_GRACE_SECONDS=1
export SWARMY_VOLUME_SNAPSHOT_RETENTION=3
# Explicit checkpoints control retention in this workload.
export SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS=86400
volume=''
server=''
cleanup() {
    if [[ -n $volume ]]; then
        sudo -E "$binary" vol detach "$volume" || {
            if [[ -n $server ]]; then sudo kill -TERM "$server" || true; fi
        }
    fi
    if [[ -n $server ]]; then wait "$server" || true; fi
    echo "Evidence and isolated namespace: $work, $SWARMY_STORE_DIRECTORY"
}
trap cleanup EXIT
curl --fail --silent --show-error --aws-sigv4 "aws:amz:${SWARMY_S3_REGION}:s3" \
    --user "$SWARMY_S3_ACCESS_KEY:$SWARMY_S3_SECRET_KEY" \
    -X PUT "$SWARMY_S3_ENDPOINT/$SWARMY_S3_BUCKET" >/dev/null
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
volume=$("$binary" --json vol create gc-seed:test | python3 -c 'import json,sys; print(json.load(sys.stdin)["volume_id"])')
# Attach selects an unused device. Read its path from the ready event.
sudo -E "$binary" --json vol attach "$volume" > "$work/attach.jsonl" 2> "$work/attach.log" &
server=$!
for _ in $(seq 1 300); do
    if [[ -s $work/attach.jsonl ]]; then break; fi
    sudo kill -0 "$server"
    sleep 0.1
done
device=$(python3 -c 'import json,sys; print(json.loads(open(sys.argv[1]).readline())["device"])' "$work/attach.jsonl")
for pass in $(seq 1 22); do
    sudo dd if=/dev/urandom of="$device" bs=1M count=256 oflag=direct conv=notrunc status=none
    sudo -E "$binary" --json vol checkpoint "$volume" >> "$work/checkpoints.jsonl"
    echo "Checkpoint $pass/22"
done
# This short grace is safe only for this isolated workload with no pending writes.
sleep 2
/usr/bin/time -v -o "$work/dry.time" "$binary" --json gc --dry-run > "$work/dry.json"
/usr/bin/time -v -o "$work/gc.time" "$binary" --json gc > "$work/gc.json"
cat "$work/dry.json" "$work/gc.json"
cat "$work/gc.time"
"$binary" --json gc > "$work/second.json"
python3 - "$work" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
dry, run, second = [json.loads((root / name).read_text()) for name in ['dry.json', 'gc.json', 'second.json']]
assert dry['scanned'] >= 22000
assert dry['deleted'] == 0 and dry['candidates'] == run['deleted']
assert run['deleted'] >= 19000 and second['deleted'] == 0
print('Benchmark assertions passed')
PY
