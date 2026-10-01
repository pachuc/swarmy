#!/usr/bin/env bash
# The public-ids check flags real infrastructure identifiers (instance,
# volume, security group, subnet, and snapshot ids, account ids in context,
# benchmark buckets, and public IPv4 addresses) but passes look-alikes: cargo
# test binary names, readable test names, placeholders, private and
# documentation addresses, and version strings.
# Failing cases require exit code 1 with the flagged line, so a crash cannot
# pass as a detection.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/scripts"
cp "$repo_dir/scripts/check-public-ids.sh" "$work/scripts/"

git -C "$work" init -q
git -C "$work" -c user.email=ids@test -c user.name=ids commit -q --allow-empty -m base

run_check() {
    if (cd "$work" && bash scripts/check-public-ids.sh) >"$work/out.log" 2>&1; then
        actual=0
    else
        actual=$?
    fi
}

check_pass() {
    local name="$1"
    run_check
    if [ "$actual" != "0" ]; then
        printf 'check-public-ids test failed for %s: want exit 0, got %s\n' "$name" "$actual"
        cat "$work/out.log"
        exit 1
    fi
}

check_fail() {
    local name="$1" expected_msg="$2"
    run_check
    if [ "$actual" != "1" ]; then
        printf 'check-public-ids test failed for %s: want exit 1, got %s\n' "$name" "$actual"
        cat "$work/out.log"
        exit 1
    fi
    if ! grep -qF "$expected_msg" "$work/out.log"; then
        printf 'check-public-ids test failed for %s: missing %s\n' "$name" "$expected_msg"
        cat "$work/out.log"
        exit 1
    fi
}

add_case() {
    printf '%s\n' "$2" >"$work/$1"
    git -C "$work" -c user.email=ids@test -c user.name=ids add "$1"
}

drop_case() {
    rm "$work/$1"
    git -C "$work" add -u "$1"
}

# A cargo test binary hash is 16 hex digits: not an EBS volume id.
add_case log.txt 'running /tmp/build/deps/vol-ad12838e0bf91175'
check_pass "sixteen-digit cargo name passes"
drop_case log.txt

# A real EBS volume id is vol- plus exactly 17 hex digits. The id is built
# at runtime so this file never contains a flaggable literal.
vol_prefix='vol-'
vol_id="${vol_prefix}0123456789abcdef0"
add_case ids.txt "snapshot ${vol_id} ready"
check_fail "seventeen-digit volume id fails" "${vol_id}"
drop_case ids.txt

# EC2 instance ids keep their existing shape.
inst_prefix='i-'
inst_id="${inst_prefix}0123456789abcdef0"
add_case ids.txt "node ${inst_id} joined"
check_fail "instance id fails" "${inst_id}"
drop_case ids.txt

# Benchmark buckets are swarmy-bench- plus exactly 32 hex digits. The id is
# built at runtime so this file never contains a flaggable literal.
bench_prefix='swarmy-bench-'
bench_id="${bench_prefix}0123456789abcdef0123456789abcdef"
add_case ids.txt "bucket gs://${bench_id} checked"
check_fail "thirty-two-digit benchmark bucket fails" "${bench_id}"
drop_case ids.txt

# The redacted placeholder carries no identifier.
add_case ids.txt 'bucket gs://swarmy-bench-<id-1> checked'
check_pass "redacted benchmark placeholder passes"
drop_case ids.txt

# Addresses in the documentation, loopback, private, and link-local ranges
# pass, as do netmasks, version strings, and numbers that are not addresses.
add_case ips.txt 'listen 127.0.0.1:8080, 10.1.2.3, 172.20.0.5, 192.168.1.10, 169.254.169.254'
check_pass "private and loopback addresses pass"
drop_case ips.txt
add_case ips.txt 'example 192.0.2.10 198.51.100.7 203.0.113.11, bind 0.0.0.0, mask 255.255.255.0'
check_pass "documentation addresses and netmasks pass"
drop_case ips.txt
add_case ips.txt 'version 1.2.3.4.5, tag v8.8.8.8, not an address 300.1.1.1'
check_pass "version strings pass"
drop_case ips.txt

# Public addresses fail. They are built at runtime so this file never
# contains a flaggable literal.
public_ip=$(printf '%s.%s.%s.%s' 8 8 8 8)
add_case ips.txt "resolver ${public_ip}"
check_fail "public IPv4 address fails" "${public_ip}"
drop_case ips.txt
outside_ip=$(printf '%s.%s.%s.%s' 172 32 0 1)
add_case ips.txt "peer ${outside_ip}"
check_fail "address just outside 172.16.0.0/12 fails" "${outside_ip}"
drop_case ips.txt

# Readable test names are not resource ids.
add_case ids.txt 'group sg-test in subnet-only, snapshot snap-in'
check_pass "readable resource names pass"
drop_case ids.txt

# Security group, subnet, and snapshot ids have 8 or 17 hex digits.
for kind in sg subnet snap; do
    for digits in 01234567 0123456789abcdef0; do
        resource_id="${kind}-${digits}"
        add_case ids.txt "resource ${resource_id} ready"
        check_fail "${kind} id with ${#digits} digits fails" "${resource_id}"
        drop_case ids.txt
    done
done

# A 12-digit account id counts only where the context names it.
add_case ids.txt 'role arn:aws:iam::<account-id>:role/x, profile arn:aws:bedrock:eu-west-1:123:inference-profile/m, elapsed 123456789012'
check_pass "placeholders, short test ids, and bare numbers pass"
drop_case ids.txt
account=$(printf '%s%s' 123456 789012)
for line in "role arn:aws:iam::${account}:role/x" \
    "image ${account}.dkr.ecr.us-east-1.amazonaws.com/x" \
    "\"OwnerId\": \"${account}\"," \
    "AWS_ACCOUNT_ID=${account}"; do
    add_case ids.txt "$line"
    check_fail "account id in: ${line}" "${account}"
    drop_case ids.txt
done

check_pass "fixture green again"

printf 'check-public-ids: ok\n'
