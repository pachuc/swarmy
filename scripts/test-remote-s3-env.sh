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

# Static-key buckets carry the endpoint and prefix; keys never appear here.
static=$(swarmy_remote_s3_env example-bucket eu-west-1 https://objects.example.invalid runs/team)
[[ $static == *'SWARMY_S3_ENDPOINT=https://objects.example.invalid'* ]]
[[ $static == *$'SWARMY_S3_ACCESS_KEY=\n'* ]]
[[ $static == *$'SWARMY_S3_SECRET_KEY=\n'* ]]
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
printf 'remote S3 environment: ok\n'
