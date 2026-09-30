#!/usr/bin/env bash
# Usage: suite-queue.sh [--at COMMIT] [--ci | --plus | --node | --chaos | --only "PACKAGE TEST"] BRANCH...
#   --ci     ci-local.sh: every CI job's commands, for validating a batch
#            branch without a GitHub run
#   default  root-suites.sh (node suite, three chaos suites, chaos-ci)
#   --plus   root-suites-plus.sh (the default set plus image, vol, and nbd)
#   --node   the node suite alone
#   --chaos  the three chaos suites and chaos-ci, for worker, scheduler, and
#            gateway changes that do not touch the node
#   --only   one suite; chaos suites run through root-suites.sh, which serves
#            the API they need, and others through rerun-suite.sh with full output
#   --at     test this commit instead of the branch head at the time the run
#            starts, so a worker's unfinished pushes are not picked up
# Run only what the change can break: these suites are serial and slow.
# Every run on this node takes ~/suite.lock, so queued runs wait for each other
# however they were started. Each branch appends one line to ~/suite-queue.log:
#   QUEUE_DONE <branch> <mode> SUITES_EXIT=<0|1>
# with the full log at ~/suite-logs/<suffix>-<mode>.log. Start it detached:
#   setsid nohup bash ~/suite-queue.sh --plus swarmy/abc123 >/dev/null 2>&1 </dev/null &
set -u
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mode=default; only=""; rev=""
if [ "${1:-}" = --at ]; then rev=$2; shift 2; fi
case "${1:-}" in
  --ci) mode=ci; shift ;;
  --plus) mode=plus; shift ;;
  --node) mode=node; shift ;;
  --chaos) mode=chaos; shift ;;
  --only) mode=only; only=$2; shift 2 ;;
esac
mkdir -p ~/suite-logs
for branch in "$@"; do
  suffix=${branch##*/}
  log=~/suite-logs/$suffix-$mode${only:+-${only// /-}}.log
  (
    export REV=$rev
    flock 9
    cd ~/chaos && scripts/dev-stack.sh stop >/dev/null 2>&1
    # A fresh stack per branch, except for a single-suite rerun, which reuses
    # the image the previous full run registered.
    [ "$mode" = only ] || sudo rm -rf ~/chaos/.dev/fdb ~/chaos/.dev/nats ~/chaos/.dev/seaweed
    case $mode in
      default) bash "$here/root-suites.sh" "$branch" ;;
      ci) bash "$here/ci-local.sh" "$branch" ;;
      plus) bash "$here/root-suites-plus.sh" "$branch" ;;
      node) SUITES="swarmyd --test node" bash "$here/root-suites.sh" "$branch" ;;
      chaos) SUITES="swarmy-chaos --test bash,swarmy-chaos --test continuity,swarmy-chaos --test coding" bash "$here/root-suites.sh" "$branch" ;;
      only)
        set -- $only
        if [ "$1" = swarmy-chaos ]; then
          SUITES="$1 --test $2" SKIP_CHAOS_CI=1 bash "$here/root-suites.sh" "$branch"
        else
          bash "$here/rerun-suite.sh" "$branch" "$1" "$2"
        fi ;;
    # Close the lock descriptor for the run itself: the dev stack's daemons
    # outlive a failed run and would otherwise hold the lock forever.
    esac > "$log" 2>&1 9>&-
    if grep -qE "SUITES_EXIT=1|PLUS_EXIT=1|CI_STEP_FAIL|test result: FAILED|checkout failed" "$log"; then rc=1; else rc=0; fi
    echo "QUEUE_DONE $branch${rev:+@$rev} $mode${only:+ $only} SUITES_EXIT=$rc" >> ~/suite-queue.log
  ) 9> ~/suite.lock
done
