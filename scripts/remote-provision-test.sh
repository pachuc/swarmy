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
# The service library derives from the checkout, never a hardcoded login.
[[ $positive == *'LD_LIBRARY_PATH=/tmp/.local/lib'* ]]
[[ $positive != *ubuntu* ]]
[[ $zero != *ubuntu* ]]

# Service user validation and path derivation.
for user in swarmy ubuntu root deploy-1 ci_bot A9; do
    validate_service_user "$user"
done
long=$(printf 'a%.0s' {1..65})
for invalid in '' 'has space' 'semi;colon' '$(injected)' 'dq"quote' 'sq'"'"'quote' 'back\slash' "$long"; do
    if validate_service_user "$invalid" >/dev/null 2>&1; then
        echo "accepted invalid service user: $invalid" >&2
        exit 1
    fi
done
[[ $(service_home_for root) == /root ]]
[[ $(service_repo_for root) == /root/swarmy ]]
# No passwd entry for this login here, so the conventional home applies.
[[ $(service_home_for swarmy) == /home/swarmy ]]
[[ $(service_repo_for swarmy) == /home/swarmy/swarmy ]]
# A custom passwd entry wins over the convention.
getent() { printf 'swarmy:x:1001:1001::/srv/custom:/bin/bash\n'; }
[[ $(service_home_for swarmy) == /srv/custom ]]
[[ $(service_repo_for swarmy) == /srv/custom/swarmy ]]
unset -f getent

# cloud-init exists on cloud images only; stock servers skip the wait.
# Stub `command` so the test does not depend on the machine it runs on.
command() {
    if [[ ${1-} == -v && ${2-} == cloud-init ]]; then
        [[ ${SWARMY_TEST_HAS_CLOUD_INIT:-absent} == present ]] && return 0 || return 1
    fi
    builtin command "$@"
}
SWARMY_TEST_HAS_CLOUD_INIT=present needs_cloud_init_wait
SWARMY_TEST_HAS_CLOUD_INIT=absent; if needs_cloud_init_wait; then echo 'waited without cloud-init' >&2; exit 1; fi
SWARMY_TEST_HAS_CLOUD_INIT=present; needs_cloud_init_wait
unset -f command

# Local storage setting classification: empty means none, never guessing.
[[ $(parse_local_storage '') == none ]]
[[ $(parse_local_storage /dev/nvme1n1) == 'device /dev/nvme1n1' ]]
[[ $(parse_local_storage device:/dev/md0) == 'device /dev/md0' ]]
[[ $(parse_local_storage dir:/srv/swarmy-local) == 'dir /srv/swarmy-local' ]]
[[ $(parse_local_storage /srv/swarmy-local) == 'dir /srv/swarmy-local' ]]
[[ $(parse_local_storage 'dir:/srv/with space') == 'dir /srv/with space' ]]
for invalid in relative dir: device: dir:relative; do
    if parse_local_storage "$invalid" >/dev/null 2>&1; then
        echo "accepted invalid local storage: $invalid" >&2
        exit 1
    fi
done
static_bucket=$(node_environment /tmp/checkout 4 /tmp example-bucket eu-west-1 https://objects.example.invalid runs/team)
[[ $static_bucket == *'SWARMY_S3_ENDPOINT=https://objects.example.invalid'* ]]
[[ $static_bucket == *'SWARMY_S3_PREFIX=runs/team'* ]]
[[ $static_bucket == *'SWARMY_S3_CONDITIONAL_CREATE=true'* ]]
[[ $static_bucket == *$'SWARMY_S3_ACCESS_KEY=\n'* ]]
[[ $static_bucket == *$'SWARMY_S3_SECRET_KEY=\n'* ]]
# Static keys land in a 0600 file with nothing printed, never on a command line.
node_keys=$(mktemp)
trap 'rm -f "$node_keys"' EXIT
printed=$(SWARMY_S3_ACCESS_KEY=fixture-access SWARMY_S3_SECRET_KEY=fixture-secret swarmy_remote_s3_keys "$node_keys")
[[ -z $printed ]]
[[ $(stat -c %a "$node_keys") == 600 ]]
[[ $(grep -c '^SWARMY_S3_ACCESS_KEY=fixture-access$' "$node_keys") == 1 ]]
[[ $(grep -c '^SWARMY_S3_SECRET_KEY=fixture-secret$' "$node_keys") == 1 ]]
[[ $static_bucket != *'fixture-access'* ]]
[[ $static_bucket != *'fixture-secret'* ]]
echo 'remote provision argument and environment tests passed'
