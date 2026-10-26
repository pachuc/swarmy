#!/usr/bin/env bash
# Check the contract, format, lockfile, validation and offline mock-provider tests.
set -euo pipefail
cd "$(dirname "$0")/.."
fail() { printf 'check-infra: %s\n' "$*" >&2; exit 1; }
tofu=${TOFU:-tofu}
command -v "$tofu" >/dev/null || fail "OpenTofu is not installed; run scripts/install-opentofu.sh"
expected=$(sed -n 's/^OPENTOFU_VERSION=//p' scripts/install-opentofu.sh)
actual=$("$tofu" version)
actual=${actual%%$'\n'*}
[[ $actual == "OpenTofu v$expected" ]] || fail "expected OpenTofu v$expected, found: $actual"
"$tofu" fmt -check -diff -recursive infra
data=$(mktemp -d)
trap 'rm -rf -- "$data"' EXIT
modules=0
for module in infra/modules/*/; do
    [[ -d $module ]] || continue
    name=$(basename -- "$module")
    printf '==> %s\n' "$name"
    # Only root configuration files count; tests and generated .terraform files do not.
    actual_inputs=$(sed -nE 's/^[[:space:]]*variable "([^"]+)"[[:space:]]*\{.*/\1/p' "$module"*.tf | LC_ALL=C sort -u)
    expected_inputs=$(cat infra/contract-inputs.txt)
    [[ $actual_inputs == "$expected_inputs" ]] || fail "$name input variables differ from infra/contract-inputs.txt; expected: $expected_inputs; found: $actual_inputs"
    actual_outputs=$(sed -nE 's/^[[:space:]]*output "([^"]+)"[[:space:]]*\{.*/\1/p' "$module"*.tf | LC_ALL=C sort -u)
    [[ $actual_outputs == $'bucket\nmachines\nnetwork_id' ]] || fail "$name outputs must be exactly bucket, machines, network_id; found: $actual_outputs"
    (
        cd -- "$module"
        export TF_DATA_DIR="$data/$name" TF_IN_AUTOMATION=1
        unset HCLOUD_TOKEN MINIO_USER MINIO_PASSWORD MINIO_ACCESS_KEY MINIO_SECRET_KEY AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
        "$tofu" init -input=false -backend=false -lockfile=readonly
        "$tofu" validate
        "$tofu" test
    )
    modules=$((modules + 1))
done
((modules > 0)) || fail "no modules under infra/modules"
printf 'check-infra: ok (%d modules)\n' "$modules"
