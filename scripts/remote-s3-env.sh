#!/usr/bin/env bash
# Emit the S3 settings consumed by the stack and node units.
#
# Bucket remotes never carry key material on stdout: this function prints the
# non-secret coordinates only (endpoint, bucket, region, prefix). Static keys
# reach /etc/swarmy/node.env through swarmy_remote_s3_keys, which reads them
# from the environment and appends them to the file directly, printing nothing.
swarmy_remote_s3_env() {
    local bucket=${1:-} region=${2:-} endpoint=${3:-} prefix=${4:-}
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
        printf 'SWARMY_S3_ENDPOINT=%s\nSWARMY_S3_ACCESS_KEY=\nSWARMY_S3_SECRET_KEY=\nSWARMY_S3_BUCKET=%s\nSWARMY_S3_PREFIX=%s\nSWARMY_S3_REGION=%s\nSWARMY_DEV_SKIP_S3=1\n' "$endpoint" "$bucket" "$prefix" "$region"
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
