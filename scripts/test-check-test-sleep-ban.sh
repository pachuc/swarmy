#!/usr/bin/env bash
# Exercise scripts/check-test-sleep-ban.py against fixture crate trees, plus
# the real workspace, so the assertions run the same flags as CI. Failing
# cases require exit code 1 with the expected message, so an internal crash
# (exit 2) cannot pass as a detected problem.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/scripts"
cp "$repo_dir/scripts/check-test-sleep-ban.py" "$work/scripts/"

crate="acme"
fixture() {
    rm -rf "$work/crates"
    mkdir -p "$work/crates/$crate/src" "$work/crates/$crate/tests"
}

write_src() {
    printf '%s\n' "$1" >"$work/crates/$crate/src/$2"
}

check() {
    local name="$1" expected="$2"
    local pattern="${3:-}"
    if python3 "$work/scripts/check-test-sleep-ban.py" >"$work/out.log" 2>&1; then
        actual=0
    else
        actual=$?
    fi
    if [ "$actual" != "$expected" ]; then
        printf 'check-test-sleep-ban test failed for %s: want exit %s, got %s\n' "$name" "$expected" "$actual"
        cat "$work/out.log"
        exit 1
    fi
    if [ -n "$pattern" ] && ! grep -qF "$pattern" "$work/out.log"; then
        printf 'check-test-sleep-ban test failed for %s: missing %s\n' "$name" "$pattern"
        cat "$work/out.log"
        exit 1
    fi
}

lib_with() {
    write_src "$1" lib.rs
}

# (a) A test root with the deny passes.
fixture
lib_with 'pub fn f() {}'
printf '#![deny(clippy::disallowed_methods)]\n' >"$work/crates/$crate/tests/x.rs"
check "root with deny passes" 0

# (b) A test root without it fails.
fixture
lib_with 'pub fn f() {}'
printf '#[test]\nfn x() {}\n' >"$work/crates/$crate/tests/x.rs"
check "root without deny fails" 1 "crates/acme/tests/x.rs:1: test module x lacks #![deny(clippy::disallowed_methods)]"

# (c) An inline cfg(test) module with the deny passes.
fixture
lib_with 'pub fn f() {}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    #[test]
    fn x() {}
}'
check "inline module with deny passes" 0

# (d) An inline cfg(test) module without it fails.
fixture
lib_with 'pub fn f() {}
#[cfg(test)]
mod tests {
    #[test]
    fn x() {}
}'
check "inline module without deny fails" 1 "crates/acme/src/lib.rs:3: test module tests lacks #![deny(clippy::disallowed_methods)]"

# (e) A file module whose target lacks the deny fails; with it passes.
fixture
lib_with 'pub fn f() {}
#[cfg(test)]
mod tests;'
write_src '#[test]
fn x() {}' tests.rs
check "file module without deny fails" 1 "crates/acme/src/lib.rs:3: test module tests lacks #![deny(clippy::disallowed_methods)]"
write_src '#![deny(clippy::disallowed_methods)]
#[test]
fn x() {}' tests.rs
check "file module with deny passes" 0

# (f) A path attribute resolves the target.
fixture
lib_with 'pub fn f() {}
#[cfg(test)]
#[path = "t.rs"]
mod tests;'
write_src '#[test]
fn x() {}' t.rs
check "path attribute resolves the target" 1 "crates/acme/src/lib.rs:4: test module tests lacks #![deny(clippy::disallowed_methods)]"

# (g) A module declared in device.rs resolves under device/.
fixture
lib_with 'pub fn f() {}'
write_src 'pub fn g() {}
#[cfg(test)]
mod upload_tests;' device.rs
mkdir -p "$work/crates/$crate/src/device"
printf '#[test]\nfn x() {}\n' >"$work/crates/$crate/src/device/upload_tests.rs"
check "device-relative module resolves" 1 "crates/acme/src/device.rs:3: test module upload_tests lacks #![deny(clippy::disallowed_methods)]"

# (h) A module nested inside a compliant inline module is exempt.
fixture
lib_with 'pub fn f() {}
#[cfg(test)]
mod outer {
    #![deny(clippy::disallowed_methods)]
    #[cfg(test)]
    mod inner {
        #[test]
        fn x() {}
    }
}'
check "nested module in a compliant module passes" 0

# (i) cfg(any(test, ...)) is not test-only and is skipped.
fixture
lib_with 'pub fn f() {}
#[cfg(any(test, feature = "x"))]
mod helpers;'
check "any(test) module is skipped" 0

# (j) A commented-out deny does not count.
fixture
lib_with 'pub fn f() {}'
printf '// #![deny(clippy::disallowed_methods)]\n#[test]\nfn x() {}\n' >"$work/crates/$crate/tests/x.rs"
check "commented-out deny fails" 1 "crates/acme/tests/x.rs:1: test module x lacks #![deny(clippy::disallowed_methods)]"

# (k) The real repository passes.
if ! python3 "$repo_dir/scripts/check-test-sleep-ban.py" >"$work/out.log" 2>&1; then
    printf 'check-test-sleep-ban test failed for the real workspace\n'
    cat "$work/out.log"
    exit 1
fi
printf 'check-test-sleep-ban: ok\n'
