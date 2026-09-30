#!/usr/bin/env bash
# Exit-status contract for scripts/node-suites/*.sh. Those scripts run as
# root on a test node and cannot run in CI, so this exercises their
# exit-status logic with stubbed cargo, git, sudo, and systemctl under a
# fake HOME: no root, no NBD, no dev stack, no network. The stubs only drive
# exit statuses and short output lines; the branching under test is the
# scripts' own. Every script puts $HOME/.cargo/bin first on PATH and does
# `cd ~/chaos`, so pointing HOME at the fixture is enough. Failing cases
# require the exact non-zero exit plus the SUITES_EXIT=/PLUS_EXIT= marker
# the queue script greps for, so a crash or a silent pass cannot score as
# the expected result.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/.cargo/bin" "$tmp/chaos/scripts" "$tmp/chaos/target/debug" \
  "$tmp/chaos/.dev" "$tmp/chaos/images/swarmy-dev" "$tmp/suite-logs"

# Stub git: fetch fails only when FAKE_FETCH_FAIL=1.
cat > "$tmp/.cargo/bin/git" <<'EOF'
#!/usr/bin/env bash
set -u
case "${1:-}" in
  fetch)
    [ "${FAKE_FETCH_FAIL:-0}" = 1 ] && exit 128
    exit 0
    ;;
  rev-parse)
    echo "abc1234"
    ;;
  *)
    exit 0
    ;;
esac
EOF

# Stub cargo. `build` fails only when FAKE_BUILD_FAIL=1. For `test`, the
# suite is the value after `--test`: it fails when it equals FAKE_TEST_FAIL,
# and a compile failure (FAKE_COMPILE_FAIL=1) prints only a compile error,
# which is how a real compile failure looks to the queue script's log grep.
cat > "$tmp/.cargo/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -u
if [ "${1:-}" = build ]; then
  [ "${FAKE_BUILD_FAIL:-0}" = 1 ] && exit 101
  exit 0
fi
name=""
prev=""
for arg in "$@"; do
  [ "$prev" = --test ] && name="$arg"
  prev="$arg"
done
if [ "${FAKE_COMPILE_FAIL:-0}" = 1 ]; then
  echo "error: could not compile stub-suite due to 1 previous error" >&2
  exit 101
fi
if [ -n "${FAKE_TEST_FAIL:-}" ] && [ "$name" = "$FAKE_TEST_FAIL" ]; then
  echo "test result: FAILED. 0 passed; 1 failed"
  exit 101
fi
echo "test result: ok. 1 passed; 0 failed"
exit 0
EOF

# Stub sudo: drop a leading -E, then run the command. umount and python3
# only appear in orphan cleanup with no devices present here.
cat > "$tmp/.cargo/bin/sudo" <<'EOF'
#!/usr/bin/env bash
set -u
[ "${1:-}" = -E ] && shift
case "${1:-}" in
  umount|python3) exit 0 ;;
esac
exec "$@"
EOF

cat > "$tmp/.cargo/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

cat > "$tmp/chaos/scripts/dev-stack.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

cat > "$tmp/chaos/scripts/chaos-ci.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

# Stub swarmy binary for the image-build lines. No swarmy-api executable on
# purpose, so the API blocks are skipped; they are not under test.
cat > "$tmp/chaos/target/debug/swarmy" <<'EOF'
#!/usr/bin/env bash
echo "stub image build ok"
exit 0
EOF

printf 'disk_size = 1\n' > "$tmp/chaos/images/swarmy-dev/recipe.toml"
: > "$tmp/chaos/.dev/env"

chmod +x "$tmp/.cargo/bin/git" "$tmp/.cargo/bin/cargo" "$tmp/.cargo/bin/sudo" \
  "$tmp/.cargo/bin/systemctl" "$tmp/chaos/scripts/dev-stack.sh" \
  "$tmp/chaos/scripts/chaos-ci.sh" "$tmp/chaos/target/debug/swarmy"

run_case() {
  if "$@" >"$tmp/out.log" 2>&1; then
    actual=0
  else
    actual=$?
  fi
}

check_exit_and_marker() {
  local name="$1" want_exit="$2" marker="$3"
  if [ "$actual" != "$want_exit" ]; then
    printf 'node-suites test failed for %s: want exit %s, got %s\n' "$name" "$want_exit" "$actual"
    cat "$tmp/out.log"
    exit 1
  fi
  if ! grep -qF "$marker" "$tmp/out.log"; then
    printf 'node-suites test failed for %s: missing %s\n' "$name" "$marker"
    cat "$tmp/out.log"
    exit 1
  fi
}

node_suites="$repo_dir/scripts/node-suites"

# root-suites.sh: all pass.
run_case env HOME="$tmp" bash "$node_suites/root-suites.sh" b
check_exit_and_marker "root-suites pass" 0 "SUITES_EXIT=0"

# root-suites.sh: a failing suite gives exit 1 and SUITES_EXIT=1.
run_case env HOME="$tmp" FAKE_TEST_FAIL=node bash "$node_suites/root-suites.sh" b
check_exit_and_marker "root-suites suite failure" 1 "SUITES_EXIT=1"

# root-suites.sh: a failed checkout stops the run with exit 2.
run_case env HOME="$tmp" FAKE_FETCH_FAIL=1 bash "$node_suites/root-suites.sh" b
check_exit_and_marker "root-suites checkout failure" 2 "checkout failed"

# root-suites.sh: a failed build stops the run with exit 2.
run_case env HOME="$tmp" FAKE_BUILD_FAIL=1 bash "$node_suites/root-suites.sh" b
check_exit_and_marker "root-suites build failure" 2 "SUITES_EXIT=1"

# root-suites-plus.sh: everything passing gives 0.
run_case env HOME="$tmp" bash "$node_suites/root-suites-plus.sh" b
check_exit_and_marker "plus pass" 0 "PLUS_EXIT=0"

# root-suites-plus.sh: a base-only failure fails the run overall.
run_case env HOME="$tmp" FAKE_TEST_FAIL=coding bash "$node_suites/root-suites-plus.sh" b
check_exit_and_marker "plus base failure propagates" 1 "PLUS_EXIT=1"

# root-suites-plus.sh: a plus-only failure keeps the base marker at 0.
run_case env HOME="$tmp" FAKE_TEST_FAIL=vol bash "$node_suites/root-suites-plus.sh" b
check_exit_and_marker "plus section failure base marker" 1 "SUITES_EXIT=0"
check_exit_and_marker "plus section failure plus marker" 1 "PLUS_EXIT=1"

# root-suites-plus.sh: a base checkout failure prints PLUS_EXIT and exits 2.
run_case env HOME="$tmp" FAKE_FETCH_FAIL=1 bash "$node_suites/root-suites-plus.sh" b
check_exit_and_marker "plus base checkout failure" 2 "PLUS_EXIT=1"

# rerun-suite.sh: a passing suite gives 0.
run_case env HOME="$tmp" bash "$node_suites/rerun-suite.sh" b swarmyd node
check_exit_and_marker "rerun pass" 0 "SUITES_EXIT=0"

# rerun-suite.sh: a target that fails to compile counts as a failure even
# though it prints no "test result: FAILED" line for the queue grep to find.
run_case env HOME="$tmp" FAKE_COMPILE_FAIL=1 bash "$node_suites/rerun-suite.sh" b swarmyd node
check_exit_and_marker "rerun compile failure" 1 "SUITES_EXIT=1"

# rerun-suite.sh: a failed checkout stops the run with exit 2.
run_case env HOME="$tmp" FAKE_FETCH_FAIL=1 bash "$node_suites/rerun-suite.sh" b swarmyd node
check_exit_and_marker "rerun checkout failure" 2 "checkout failed"

printf 'node-suites: ok\n'
