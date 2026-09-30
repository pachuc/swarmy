#!/usr/bin/env bash
# Emit the S3 settings consumed by the stack and node units.
#
# Bucket remotes never carry key material on stdout: this function prints the
# non-secret coordinates only (endpoint, bucket, region, prefix). Static keys
# reach /etc/swarmy/node.env through swarmy_remote_s3_keys, which reads them
# from the environment and appends them to the file directly, printing nothing.
swarmy_remote_s3_env() {
    local bucket=${1:-} region=${2:-} endpoint=${3:-} prefix=${4:-} keys_later=${5:-false}
    if [[ -n $bucket ]]; then
        [[ $bucket =~ ^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$ && $region =~ ^[a-z0-9-]+$ ]] || return 1
        if [[ -n $endpoint ]]; then
            [[ $endpoint =~ ^https?://[^[:space:]]+$ ]] || return 1
        fi
        if [[ -n $prefix ]]; then
            [[ $prefix != /* && $prefix != */ && $prefix != *'//'*
                && $prefix != *$'\n'* ]] || return 1
            local segment segments
            IFS=/ read -ra segments <<<"$prefix"
            for segment in "${segments[@]}"; do
                [[ -n $segment && $segment != '.' && $segment != '..' ]] || return 1
            done
        fi
        if [[ $keys_later == true ]]; then
            # Static keys are merged into the node environment later by
            # merge_static_s3_keys; emitting empty lines first would leave
            # stale key entries ahead of the real ones.
            printf 'SWARMY_S3_ENDPOINT=%s\nSWARMY_S3_BUCKET=%s\nSWARMY_S3_PREFIX=%s\nSWARMY_S3_REGION=%s\nSWARMY_DEV_SKIP_S3=1\n' "$endpoint" "$bucket" "$prefix" "$region"
        else
            printf 'SWARMY_S3_ENDPOINT=%s\nSWARMY_S3_ACCESS_KEY=\nSWARMY_S3_SECRET_KEY=\nSWARMY_S3_BUCKET=%s\nSWARMY_S3_PREFIX=%s\nSWARMY_S3_REGION=%s\nSWARMY_DEV_SKIP_S3=1\n' "$endpoint" "$bucket" "$prefix" "$region"
        fi
    else
        [[ -z $endpoint && -z $prefix ]] || return 1
        printf 'SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\nSWARMY_S3_ACCESS_KEY=swarmy-dev\nSWARMY_S3_SECRET_KEY=swarmy-dev-secret\nSWARMY_S3_BUCKET=swarmy\nSWARMY_S3_PREFIX=\nSWARMY_S3_REGION=us-east-1\nSWARMY_DEV_SKIP_S3=0\n'
    fi
}

# Append static S3 keys from the environment to a 0600 node environment file.
# Prints nothing, so key material never reaches SSH output or logs. Fails when
# either key is missing; the file is created private when absent.
swarmy_remote_s3_keys() {
    local dest=${1:-}
    [[ -n $dest ]] || return 1
    [[ -n ${SWARMY_S3_ACCESS_KEY:-} && -n ${SWARMY_S3_SECRET_KEY:-} ]] || return 1
    [[ $SWARMY_S3_ACCESS_KEY != *$'\n'* && $SWARMY_S3_SECRET_KEY != *$'\n'* ]] || return 1
    umask 077
    touch "$dest"
    chmod 600 "$dest"
    printf 'SWARMY_S3_ACCESS_KEY=%s\nSWARMY_S3_SECRET_KEY=%s\n' "$SWARMY_S3_ACCESS_KEY" "$SWARMY_S3_SECRET_KEY" >>"$dest"
}

# Merge a root-owned static-key staging file into the node environment as
# root in one step, then delete the staging file. The provisioning user
# cannot read the staging file, so sourcing it directly would fail with
# "Permission denied"; one sudo shell reads it, appends the keys, and
# deletes it. Prints nothing, so key material never reaches SSH output or
# logs. Fails when the staging file is missing.
merge_static_s3_keys() {
    local staging=${1:-} dest=${2:-} helpers=${3:-}
    [[ -n $staging && -n $dest && -n $helpers ]] || return 1
    [[ -f $staging ]] || { echo 'Static S3 keys were not uploaded to the node' >&2; return 1; }
    sudo bash -c 'set -a; . "$0"; set +a; . "$1"; swarmy_remote_s3_keys "$2" && rm -f "$0"' "$staging" "$helpers" "$dest"
}
