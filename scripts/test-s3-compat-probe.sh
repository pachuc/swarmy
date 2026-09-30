#!/usr/bin/env bash
# Run the S3 compatibility probe against the disposable development stack's
# SeaweedFS, covering both the default conditional path and the
# plain-PUT fallback (`conditional_create = false`). Fails when any check
# fails, when cleanup leaves objects behind, or when the output leaks key
# material or endpoint coordinates.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

scripts/dev-stack.sh stop >/dev/null 2>&1 || true
trap 'scripts/dev-stack.sh stop >/dev/null 2>&1 || true' EXIT
scripts/dev-stack.sh start >/dev/null
source .dev/env

probe=(cargo run --quiet --locked -p swarmy-volume --example s3-compat-probe)

output=$("${probe[@]}")
printf '%s\n' "$output"
[[ $output != *'"ok":false'* ]]
[[ $(grep -c '"ok":true' <<<"$output") == 10 ]]

fallback=$("${probe[@]}" -- --conditional-create=false)
printf '%s\n' "$fallback"
[[ $fallback != *'"ok":false'* ]]
[[ $(grep -c '"ok":true' <<<"$fallback") == 10 ]]

# Missing coordinates fail with the argument check, not a stack trace.
if SWARMY_S3_ENDPOINT='' SWARMY_S3_BUCKET='' "${probe[@]}" -- --region us-east-1 >/dev/null 2>&1; then
    printf 'probe accepted missing coordinates\n' >&2
    exit 1
fi

# The report names checks only: no endpoint, bucket, prefix, or key material.
combined=$output$fallback
[[ $combined != *"$SWARMY_S3_SECRET_KEY"* ]]
[[ $combined != *"$SWARMY_S3_ACCESS_KEY"* ]]
[[ $combined != *"127.0.0.1"* ]]
[[ $combined != *"8333"* ]]
[[ $combined != *"$SWARMY_S3_BUCKET"* ]]
printf 'S3 compatibility probe: ok\n'
