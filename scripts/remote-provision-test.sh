#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/remote-provision-env.sh"
[[ $(parse_sandbox_count 0) == 0 ]]
[[ $(parse_sandbox_count 64) == 64 ]]
for invalid in -1 word 1.5 4294967296; do
    if parse_sandbox_count "$invalid" >/dev/null 2>&1; then
        echo "accepted invalid count: $invalid" >&2
        exit 1
    fi
done
zero=$(node_environment /tmp/checkout 0 /this-mount-does-not-exist)
[[ $zero == *'SWARMY_NODE_ROLES=volume'* ]]
[[ $zero == *'SWARMY_NODE_SANDBOXES=0'* ]]
[[ $zero == *'SWARMY_NODE_DISK_BYTES=0'* ]]
[[ $zero =~ SWARMY_NODE_MEMORY_BYTES=[0-9]+ ]]
positive=$(node_environment /tmp/checkout 4 /tmp)
[[ $positive == *'SWARMY_NODE_ROLES=sandbox,volume'* ]]
[[ $positive == *'SWARMY_NODE_SANDBOXES=4'* ]]
[[ $positive == *'SWARMY_NODE_DISK_BYTES='* ]]
[[ $positive != *'SWARMY_NODE_DISK_BYTES=0'* ]]
zero_bucket=$(node_environment /tmp/checkout 0 /this-mount-does-not-exist example-bucket eu-west-1)
[[ $zero_bucket == *'SWARMY_NODE_ROLES=volume'* ]]
[[ $zero_bucket == *'SWARMY_NODE_SANDBOXES=0'* ]]
[[ $zero_bucket == *'SWARMY_NODE_DISK_BYTES=0'* ]]
[[ $zero_bucket == *$'SWARMY_S3_ENDPOINT=\n'* ]]
[[ $zero_bucket == *'SWARMY_S3_BUCKET=example-bucket'* ]]
[[ $zero_bucket == *'SWARMY_S3_REGION=eu-west-1'* ]]
[[ $zero_bucket == *'SWARMY_DEV_SKIP_S3=1'* ]]
positive_bucket=$(node_environment /tmp/checkout 4 /tmp example-bucket eu-west-1)
[[ $positive_bucket == *'SWARMY_NODE_ROLES=sandbox,volume'* ]]
[[ $positive_bucket == *'SWARMY_NODE_SANDBOXES=4'* ]]
[[ $positive_bucket == *'SWARMY_S3_BUCKET=example-bucket'* ]]
echo 'remote provision argument and environment tests passed'
