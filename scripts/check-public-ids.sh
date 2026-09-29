#!/usr/bin/env bash
set -euo pipefail
# Check tracked text only: build outputs and local dev-stack settings are private.
if git grep -nE '(^|[^[:alnum:]_-])(i-0[0-9a-f]{8,}|vol-[0-9a-f]{8,})' -- ':!scripts/check-public-ids.sh'; then
    echo 'Committed infrastructure identifiers must be redacted' >&2
    exit 1
fi
