#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/remote-s3-env.sh"

# AWS instance-role buckets keep the empty endpoint and empty keys.
bucket=$(swarmy_remote_s3_env example-bucket eu-west-1)
[[ $bucket == *$'SWARMY_S3_ENDPOINT=\n'* ]]
[[ $bucket == *$'SWARMY_S3_ACCESS_KEY=\n'* ]]
[[ $bucket == *$'SWARMY_S3_SECRET_KEY=\n'* ]]
[[ $bucket == *$'SWARMY_S3_BUCKET=example-bucket\n'* ]]
[[ $bucket == *$'SWARMY_S3_PREFIX=\n'* ]]
[[ $bucket == *$'SWARMY_S3_REGION=eu-west-1\n'* ]]
[[ $bucket == *'SWARMY_DEV_SKIP_S3=1'* ]]

# Static-key buckets carry the endpoint and prefix; keys are merged later and
# no empty key lines may precede them.
static=$(swarmy_remote_s3_env example-bucket eu-west-1 https://objects.example.invalid runs/team true)
[[ $static == *'SWARMY_S3_ENDPOINT=https://objects.example.invalid'* ]]
[[ $static != *'SWARMY_S3_ACCESS_KEY='* ]]
[[ $static != *'SWARMY_S3_SECRET_KEY='* ]]
[[ $static == *$'SWARMY_S3_BUCKET=example-bucket\n'* ]]
[[ $static == *$'SWARMY_S3_PREFIX=runs/team\n'* ]]
[[ $static == *'SWARMY_DEV_SKIP_S3=1'* ]]

local_store=$(swarmy_remote_s3_env '' '')
[[ $local_store == *'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333'* ]]
[[ $local_store == *'SWARMY_S3_PREFIX='* ]]
[[ $local_store == *'SWARMY_DEV_SKIP_S3=0'* ]]
# The bucket name and region are checked; the endpoint and prefix were
# validated when the bucket description was saved (Rust checks them).
! swarmy_remote_s3_env 'bad/bucket' eu-west-1 >/dev/null
! swarmy_remote_s3_env 'bad.bucket' eu-west-1 >/dev/null
! swarmy_remote_s3_env example-bucket '' >/dev/null
! swarmy_remote_s3_env '' '' https://objects.example.invalid >/dev/null
swarmy_remote_s3_env example-bucket eu-west-1 https://objects.example.invalid 'runs/nested' >/dev/null

# The root merge runs as a caller who does not own the staging file: the
# staging file is root-owned 0600, one sudo shell appends it verbatim and
# deletes it, and nothing is printed. The secret carries `$`, a backtick, a
# space and a command substitution, which must survive literally and never run.
merge_dir=''
trap 'if [[ -n $merge_dir ]]; then rm -rf "$merge_dir"; fi' EXIT
if sudo -n true 2>/dev/null; then
    merge_dir=$(mktemp -d)
    staging=$merge_dir/staging.env
    node_env=$merge_dir/node.env
    hostile='sek ret$with`backtick$(touch "$merge_dir/pwned")'
    printf 'SWARMY_S3_BUCKET=example-bucket\n' >"$node_env"
    chmod 600 "$node_env"
    printf 'SWARMY_S3_ACCESS_KEY=%s\nSWARMY_S3_SECRET_KEY=%s\n' "$hostile" "$hostile" | sudo tee "$staging" >/dev/null
    sudo chmod 600 "$staging"
    [[ $(stat -c %U "$staging") == root ]]
    if [[ $(id -u) != 0 ]]; then
        ! cat "$staging" >/dev/null 2>&1
    fi
    merged=$(merge_static_s3_keys "$staging" "$node_env")
    [[ -z $merged ]]
    [[ ! -e $staging ]]
    [[ ! -e $merge_dir/pwned ]]
    [[ $merged != *"$hostile"* ]]
    [[ $(grep -c -F "SWARMY_S3_ACCESS_KEY=$hostile" "$node_env") == 1 ]]
    [[ $(grep -c -F "SWARMY_S3_SECRET_KEY=$hostile" "$node_env") == 1 ]]
    # A missing staging file fails instead of writing an empty node env.
    ! merge_static_s3_keys "$staging" "$node_env" >/dev/null
else
    printf 'skipping root merge test (no passwordless sudo)\n' >&2
fi
printf 'remote S3 environment: ok\n'
