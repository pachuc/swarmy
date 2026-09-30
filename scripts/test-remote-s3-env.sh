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
[[ $static != *'example-access'* ]]
[[ $static != *'example-secret'* ]]

local_store=$(swarmy_remote_s3_env '' '')
[[ $local_store == *'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333'* ]]
[[ $local_store == *'SWARMY_S3_PREFIX='* ]]
[[ $local_store == *'SWARMY_DEV_SKIP_S3=0'* ]]
! swarmy_remote_s3_env 'bad/bucket' eu-west-1 >/dev/null
! swarmy_remote_s3_env 'bad.bucket' eu-west-1 >/dev/null
! swarmy_remote_s3_env example-bucket eu-west-1 'not a url' >/dev/null
! swarmy_remote_s3_env example-bucket eu-west-1 https://objects.example.invalid '/lead' >/dev/null
! swarmy_remote_s3_env example-bucket eu-west-1 https://objects.example.invalid 'a//b' >/dev/null
! swarmy_remote_s3_env example-bucket eu-west-1 https://objects.example.invalid 'a/../b' >/dev/null
! swarmy_remote_s3_env '' '' https://objects.example.invalid >/dev/null
swarmy_remote_s3_env example-bucket eu-west-1 https://objects.example.invalid 'runs/nested' >/dev/null

# Static keys append to a 0600 file and never reach stdout.
keys_file=$(mktemp)
trap 'rm -f "$keys_file"' EXIT
output=$(SWARMY_S3_ACCESS_KEY=example-access SWARMY_S3_SECRET_KEY=example-secret swarmy_remote_s3_keys "$keys_file")
[[ -z $output ]]
[[ $(stat -c %a "$keys_file") == 600 ]]
[[ $(grep -c '^SWARMY_S3_ACCESS_KEY=example-access$' "$keys_file") == 1 ]]
[[ $(grep -c '^SWARMY_S3_SECRET_KEY=example-secret$' "$keys_file") == 1 ]]
# Missing or newline-containing keys are refused without touching the file.
before=$(cat "$keys_file")
! SWARMY_S3_ACCESS_KEY=example-access swarmy_remote_s3_keys "$keys_file" >/dev/null
! SWARMY_S3_ACCESS_KEY=$'a\nb' SWARMY_S3_SECRET_KEY=example-secret swarmy_remote_s3_keys "$keys_file" >/dev/null
[[ $(cat "$keys_file") == "$before" ]]

# The root merge runs as a caller who does not own the staging file: the
# staging file is root-owned 0600, one sudo shell merges and deletes it, and
# nothing is printed.
helpers="$(dirname "${BASH_SOURCE[0]}")/remote-s3-env.sh"
merge_dir=''
trap 'rm -f "$keys_file"; [[ -n $merge_dir ]] && rm -rf "$merge_dir"' EXIT
if sudo -n true 2>/dev/null; then
    merge_dir=$(mktemp -d)
    staging=$merge_dir/staging.env
    node_env=$merge_dir/node.env
    printf 'SWARMY_S3_BUCKET=example-bucket\n' >"$node_env"
    chmod 600 "$node_env"
    printf 'SWARMY_S3_ACCESS_KEY=merge-access\nSWARMY_S3_SECRET_KEY=merge-secret\n' | sudo tee "$staging" >/dev/null
    sudo chmod 600 "$staging"
    [[ $(stat -c %U "$staging") == root ]]
    merged=$(merge_static_s3_keys "$staging" "$node_env" "$helpers")
    [[ -z $merged ]]
    [[ ! -e $staging ]]
    [[ $(stat -c %a "$node_env") == 600 ]]
    [[ $(grep -c '^SWARMY_S3_ACCESS_KEY=merge-access$' "$node_env") == 1 ]]
    [[ $(grep -c '^SWARMY_S3_SECRET_KEY=merge-secret$' "$node_env") == 1 ]]
    [[ $merged != *'merge-access'* ]]
    [[ $merged != *'merge-secret'* ]]
    # A missing staging file fails instead of writing an empty node env.
    ! merge_static_s3_keys "$staging" "$node_env" "$helpers" >/dev/null
else
    printf 'skipping root merge test (no passwordless sudo)\n' >&2
fi
printf 'remote S3 environment: ok\n'
