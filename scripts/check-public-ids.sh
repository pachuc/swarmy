#!/usr/bin/env bash
set -euo pipefail
# Check tracked text only: build outputs and local dev-stack settings are private.
# EBS volume ids are `vol-` plus exactly 17 hex digits; cargo test binaries
# carry a shorter 16-digit hash that must not match. Benchmark buckets are
# `swarmy-bench-` plus exactly 32 hex digits; redacted `swarmy-bench-<id-N>`
# placeholders carry no identifier and must not match.
if git grep -nE '(^|[^[:alnum:]_-])(i-0[0-9a-f]{8,}|vol-[0-9a-f]{17}([^[:alnum:]_-]|$)|swarmy-bench-[0-9a-f]{32}([^[:alnum:]_-]|$)|ip-([0-9]{1,3}-){3}[0-9]{1,3})' -- ':!scripts/check-public-ids.sh'; then
    echo 'Committed infrastructure identifiers must be redacted' >&2
    exit 1
fi
