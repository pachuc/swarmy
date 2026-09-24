#!/usr/bin/env bash
# Emit the S3 settings consumed by the stack and node units.
swarmy_remote_s3_env() {
    local bucket=${1:-} region=${2:-}
    if [[ -n $bucket ]]; then
        [[ $bucket =~ ^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$ && $region =~ ^[a-z0-9-]+$ ]] || return 1
        printf 'SWARMY_S3_ENDPOINT=\nSWARMY_S3_ACCESS_KEY=\nSWARMY_S3_SECRET_KEY=\nSWARMY_S3_BUCKET=%s\nSWARMY_S3_REGION=%s\nSWARMY_DEV_SKIP_S3=1\n' "$bucket" "$region"
    else
        printf 'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\nSWARMY_S3_ACCESS_KEY=swarmy-dev\nSWARMY_S3_SECRET_KEY=swarmy-dev-secret\nSWARMY_S3_BUCKET=swarmy\nSWARMY_S3_REGION=us-east-1\nSWARMY_DEV_SKIP_S3=0\n'
    fi
}
