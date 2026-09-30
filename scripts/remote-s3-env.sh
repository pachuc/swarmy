#!/usr/bin/env bash
# Emit the S3 settings consumed by the stack and node units.
#
# Bucket remotes never carry key material on stdout: this function prints the
# non-secret coordinates only (endpoint, bucket, region, prefix). The bucket
# name and region are checked here because the script reads them directly;
# the endpoint and prefix were validated when the bucket description was
# saved (Rust checks them), so they pass through. Static keys reach
# /etc/swarmy/node.env through merge_static_s3_keys below, which appends the
# staging file verbatim and prints nothing.
swarmy_remote_s3_env() {
    local bucket=${1:-} region=${2:-} endpoint=${3:-} prefix=${4:-} keys_later=${5:-false}
    if [[ -n $bucket ]]; then
        [[ $bucket =~ ^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$ && $region =~ ^[a-z0-9-]+$ ]] || return 1
        printf 'SWARMY_S3_ENDPOINT=%s\n' "$endpoint"
        # Static keys are merged into the node environment later by
        # merge_static_s3_keys; emitting empty lines first would leave
        # stale key entries ahead of the real ones.
        if [[ $keys_later != true ]]; then
            printf 'SWARMY_S3_ACCESS_KEY=\nSWARMY_S3_SECRET_KEY=\n'
        fi
        printf 'SWARMY_S3_BUCKET=%s\nSWARMY_S3_PREFIX=%s\nSWARMY_S3_REGION=%s\nSWARMY_DEV_SKIP_S3=1\n' "$bucket" "$prefix" "$region"
    else
        [[ -z $endpoint && -z $prefix ]] || return 1
        printf 'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\nSWARMY_S3_ACCESS_KEY=swarmy-dev\nSWARMY_S3_SECRET_KEY=swarmy-dev-secret\nSWARMY_S3_BUCKET=swarmy\nSWARMY_S3_PREFIX=\nSWARMY_S3_REGION=us-east-1\nSWARMY_DEV_SKIP_S3=0\n'
    fi
}

# Append a root-owned static-key staging file to the node environment as root
# in one step, then delete the staging file. The bytes move verbatim: the
# file is never sourced, so `$`, backticks and spaces in key material cannot
# expand or split. The provisioning user cannot read the staging file, hence
# a single sudo shell. Prints nothing, so key material never reaches SSH
# output or logs. Fails when the staging file is missing.
merge_static_s3_keys() {
    local staging=${1:-} dest=${2:-}
    [[ -n $staging && -n $dest ]] || return 1
    [[ -f $staging ]] || { echo 'Static S3 keys were not uploaded to the node' >&2; return 1; }
    sudo sh -c 'cat "$0" >> "$1" && rm -f "$0"' "$staging" "$dest"
}
