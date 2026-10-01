#!/usr/bin/env bash
set -euo pipefail
# Check tracked text only: build outputs and local dev-stack settings are private.
# Every pattern below names one kind of real infrastructure identifier; a
# match fails the check. Use placeholders such as <account-id> instead.
#
# EC2 instance ids are `i-0` plus hex. EBS volume ids are `vol-` plus exactly
# 17 hex digits; cargo test binaries carry a shorter 16-digit hash that must
# not match. Security group, subnet, and snapshot ids are `sg-`, `subnet-`,
# and `snap-` plus 8 or 17 hex digits; readable test names such as
# `sg-test` must not match. Benchmark buckets are `swarmy-bench-` plus
# exactly 32 hex digits; redacted `swarmy-bench-<id-N>` placeholders carry no
# identifier and must not match. EC2 private host names are `ip-A-B-C-D`.
# A 12-digit AWS account id only counts in a context that names it: inside an
# ARN, as an ECR registry host, or after an account or owner key. Bare
# 12-digit numbers are timestamps and byte counts in benchmark records.
ids='(^|[^[:alnum:]_-])(i-0[0-9a-f]{8,}|vol-[0-9a-f]{17}([^[:alnum:]_-]|$)|(sg|subnet|snap)-([0-9a-f]{17}|[0-9a-f]{8})([^[:alnum:]_-]|$)|swarmy-bench-[0-9a-f]{32}([^[:alnum:]_-]|$)|ip-([0-9]{1,3}-){3}[0-9]{1,3})'
accounts='arn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:[0-9]{12}:|(^|[^0-9])[0-9]{12}\.dkr\.ecr\.|([Aa]ccount|ACCOUNT|[Oo]wner|OWNER)[_ -]?([Ii][Dd])?["'\'']?[[:space:]]*[:=][[:space:]]*["'\'']?[0-9]{12}([^0-9]|$)'
status=0
if git grep -nIE -e "$ids" -e "$accounts" -- ':!scripts/check-public-ids.sh'; then
    status=1
fi
# IPv4 addresses: only the documentation ranges (192.0.2.0/24,
# 198.51.100.0/24, 203.0.113.0/24), loopback (127.0.0.0/8), private and
# link-local ranges (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16,
# 169.254.0.0/16), 0.0.0.0/8, and 224.0.0.0 and above (multicast and
# netmasks) may appear. Four dotted numbers inside a longer dotted run are
# version strings, not addresses.
if git grep -nIE '([0-9]{1,3}\.){3}[0-9]{1,3}' -- ':!scripts/check-public-ids.sh' | awk '
    function allowed(a, b, c) {
        return a == 0 || a == 10 || a == 127 || a >= 224 ||
            (a == 172 && b >= 16 && b <= 31) || (a == 192 && b == 168) ||
            (a == 169 && b == 254) || (a == 192 && b == 0 && c == 2) ||
            (a == 198 && b == 51 && c == 100) || (a == 203 && b == 0 && c == 113)
    }
    {
        rest = $0
        sub(/^[^:]*:[0-9]+:/, "", rest)
        while (match(rest, /[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/)) {
            before = RSTART > 1 ? substr(rest, RSTART - 1, 1) : ""
            after = substr(rest, RSTART + RLENGTH, 2)
            quad = substr(rest, RSTART, RLENGTH)
            rest = substr(rest, RSTART + RLENGTH)
            if (before ~ /[[:alnum:]._-]/ || after ~ /^[[:alnum:]_]/ || after ~ /^\.[0-9]/) continue
            split(quad, o, ".")
            if (o[1] > 255 || o[2] > 255 || o[3] > 255 || o[4] > 255) continue
            if (!allowed(o[1] + 0, o[2] + 0, o[3] + 0)) { print; found = 1; break }
        }
    }
    END { exit found ? 0 : 1 }'; then
    status=1
fi
if [ "$status" != 0 ]; then
    echo 'Committed infrastructure identifiers must be redacted' >&2
fi
exit "$status"
