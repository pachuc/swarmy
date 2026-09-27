#!/usr/bin/env bash
# Usage: suite-queue.sh BRANCH... — waits for any running suite, then runs root-suites.sh per branch with a wiped stack between runs.
set -u
while pgrep -f "^bash /home/ubuntu/root-suites(-plus)?.sh" >/dev/null; do sleep 30; done
for branch in "$@"; do
  suffix=${branch##*/}
  cd ~/chaos && scripts/dev-stack.sh stop >/dev/null 2>&1
  sudo rm -rf ~/chaos/.dev/fdb ~/chaos/.dev/nats ~/chaos/.dev/seaweed
  bash ~/root-suites.sh "$branch" > ~/root-suites-$suffix-q.log 2>&1
  echo "QUEUE_DONE $branch $(grep -o 'SUITES_EXIT=[0-9]*' ~/root-suites-$suffix-q.log)"
done
echo QUEUE_FINISHED
