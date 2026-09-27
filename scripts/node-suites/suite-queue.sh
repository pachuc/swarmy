#!/usr/bin/env bash
# Usage: suite-queue.sh [--plus | --node | --only "PACKAGE TEST"] BRANCH...
#   default  root-suites.sh (node suite, three chaos suites, chaos-ci)
#   --plus   root-suites-plus.sh (the default set plus image, vol, and nbd)
#   --node   the node suite alone
#   --only   one suite, rerun-suite.sh with full output
# Every run on this node takes ~/suite.lock, so queued runs wait for each other
# however they were started. Each branch appends one line to ~/suite-queue.log:
#   QUEUE_DONE <branch> <mode> SUITES_EXIT=<0|1>
# with the full log at ~/suite-logs/<suffix>-<mode>.log. Start it detached:
#   setsid nohup bash ~/suite-queue.sh --plus swarmy/abc123 >/dev/null 2>&1 </dev/null &
set -u
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mode=default; only=""
case "${1:-}" in
  --plus) mode=plus; shift ;;
  --node) mode=node; shift ;;
  --only) mode=only; only=$2; shift 2 ;;
esac
mkdir -p ~/suite-logs
for branch in "$@"; do
  suffix=${branch##*/}
  log=~/suite-logs/$suffix-$mode${only:+-${only// /-}}.log
  (
    flock 9
    cd ~/chaos && scripts/dev-stack.sh stop >/dev/null 2>&1
    # A fresh stack per branch, except for a single-suite rerun, which reuses
    # the image the previous full run registered.
    [ "$mode" = only ] || sudo rm -rf ~/chaos/.dev/fdb ~/chaos/.dev/nats ~/chaos/.dev/seaweed
    case $mode in
      default) bash "$here/root-suites.sh" "$branch" ;;
      plus) bash "$here/root-suites-plus.sh" "$branch" ;;
      node) SUITES="swarmyd --test node" bash "$here/root-suites.sh" "$branch" ;;
      only) set -- $only; bash "$here/rerun-suite.sh" "$branch" "$1" "$2" ;;
    esac > "$log" 2>&1
    if grep -qE "SUITES_EXIT=1|test result: FAILED|checkout failed" "$log"; then rc=1; else rc=0; fi
    echo "QUEUE_DONE $branch $mode${only:+ $only} SUITES_EXIT=$rc" >> ~/suite-queue.log
  ) 9> ~/suite.lock
done
