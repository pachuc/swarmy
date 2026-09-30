#!/usr/bin/env bash
# The public-ids check flags real infrastructure identifiers but passes cargo
# test binary names, whose 16-digit hashes only look like EBS volume ids.
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

check_pass "fixture green again"

printf 'check-public-ids: ok\n'
