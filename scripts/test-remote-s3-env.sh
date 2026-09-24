#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/remote-s3-env.sh"

bucket=$(swarmy_remote_s3_env example-bucket eu-west-1)
[[ $bucket == *$'SWARMY_S3_ENDPOINT=\n'* ]]
[[ $bucket == *$'SWARMY_S3_ACCESS_KEY=\n'* ]]
[[ $bucket == *$'SWARMY_S3_SECRET_KEY=\n'* ]]
[[ $bucket == *$'SWARMY_S3_BUCKET=example-bucket\n'* ]]
[[ $bucket == *$'SWARMY_S3_REGION=eu-west-1\n'* ]]
[[ $bucket == *'SWARMY_DEV_SKIP_S3=1'* ]]
local_store=$(swarmy_remote_s3_env '' '')
[[ $local_store == *'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333'* ]]
[[ $local_store == *'SWARMY_DEV_SKIP_S3=0'* ]]
! swarmy_remote_s3_env 'bad/bucket' eu-west-1 >/dev/null
! swarmy_remote_s3_env 'bad.bucket' eu-west-1 >/dev/null
printf 'remote S3 environment: ok\n'
