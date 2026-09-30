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
# Every provisioning argument lands in the variable the SSH command sends it
# as; defaults match a bare `remote up` with no bucket.
parse_provision_args node 10.0.0.2 test-bucket eu-west-1 https://objects.example.invalid runs/team false true 4 deploy-user dir:/srv/data
[[ $mode == node ]]
[[ $service_address == 10.0.0.2 ]]
[[ $bucket == test-bucket ]]
[[ $bucket_region == eu-west-1 ]]
[[ $bucket_endpoint == https://objects.example.invalid ]]
[[ $bucket_prefix == runs/team ]]
[[ $bucket_conditional_create == false ]]
[[ $bucket_static == true ]]
[[ $sandboxes == 4 ]]
[[ $service_user == deploy-user ]]
[[ $local_storage == dir:/srv/data ]]
parse_provision_args
[[ $mode == stack ]]
[[ $service_address == 127.0.0.1 ]]
[[ -z $bucket ]]
[[ -z $bucket_region ]]
[[ -z $bucket_endpoint ]]
[[ -z $bucket_prefix ]]
[[ $bucket_conditional_create == true ]]
[[ $bucket_static == false ]]
[[ $sandboxes == 64 ]]
[[ $service_user == swarmy ]]
[[ -z $local_storage ]]
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
[[ $zero_bucket == *'SWARMY_S3_CONDITIONAL_CREATE=true'* ]]
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
# Static buckets carry no key lines at all: the keys are merged later by
# merge_static_s3_keys, and empty lines ahead of the real ones would be stale.
static_bucket=$(node_environment /tmp/checkout 4 /tmp example-bucket eu-west-1 https://objects.example.invalid runs/team false true)
[[ $static_bucket == *'SWARMY_S3_ENDPOINT=https://objects.example.invalid'* ]]
[[ $static_bucket == *'SWARMY_S3_PREFIX=runs/team'* ]]
[[ $static_bucket == *'SWARMY_S3_CONDITIONAL_CREATE=false'* ]]
[[ $static_bucket != *'SWARMY_S3_ACCESS_KEY='* ]]
[[ $static_bucket != *'SWARMY_S3_SECRET_KEY='* ]]
# Installed control-plane units come from the installed unit files through
# the shared table: tunnel-only or agent-only hosts report none, while the
# stack unit or any node service reports it.
systemctl() {
    printf '%s\n' "$SWARMY_TEST_INSTALLED"
}
SWARMY_TEST_INSTALLED='swarmy-tunnel.service enabled
swarmyd.service enabled'
[[ -z $(list_installed_control_units) ]]
SWARMY_TEST_INSTALLED='swarmy-tunnel.service enabled
swarmy-gateway.service enabled'
[[ $(list_installed_control_units) == 'swarmy-gateway.service' ]]
SWARMY_TEST_INSTALLED='swarmy-stack.service enabled'
[[ $(list_installed_control_units) == 'swarmy-stack.service' ]]
SWARMY_TEST_INSTALLED='swarmy-stack.service enabled
swarmyd.service enabled
swarmy-api.service enabled'
[[ $(list_installed_control_units | sort) == $'swarmy-api.service\nswarmy-stack.service' ]]
# A failing systemctl is an error, never mistaken for "no units installed".
systemctl() {
    echo 'cannot list units' >&2
    return 1
}
if list_installed_control_units >/dev/null 2>&1; then
    echo 'failing systemctl looked like no units' >&2
    exit 1
fi
unset -f systemctl
# read_shared_list fails loudly on empty or failing producers instead of
# handing callers an empty list.
empty_producer() { :; }
if read_shared_list probe_result empty_producer >/dev/null 2>&1; then
    echo 'empty producer looked fine' >&2
    exit 1
fi
swarmy_unit_table() { return 1; }
if read_shared_list probe_result swarmy_mode_build_args stack >/dev/null 2>&1; then
    echo 'failing unit table looked fine' >&2
    exit 1
fi
unset -f empty_producer
echo 'remote provision argument and environment tests passed'
