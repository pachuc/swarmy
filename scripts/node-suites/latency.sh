#!/usr/bin/env bash
# Usage: latency.sh BRANCH. The primary and node live in separate namespaces;
# only the veth ends receive netem delay. LATENCY_RTTS defaults to "175 400".
set -uo pipefail
branch=$1
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
. "$here/suite-api.sh"
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export SWARMY_REQUIRE_ROOT_SUITES=1
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
primary_ns=swl-primary
node_ns=swl-node
primary_ip=10.203.77.1
node_ip=10.203.77.2
user=$(id -un)
cd ~/chaos
git fetch -q origin "$branch" && git checkout -q -B suite "${REV:-origin/$branch}" || { echo "checkout failed"; echo "SUITES_EXIT=1"; exit 2; }
echo "== branch $branch at $(git rev-parse --short HEAD)"
in_primary() { sudo -E ip netns exec "$primary_ns" "$@"; }
as_user_in_primary() { in_primary sudo -u "$user" env "PATH=$PATH" "HOME=$HOME" "$@"; }
wipe_stack_state() { sudo rm -rf .dev/fdb .dev/nats .dev/seaweed .dev/fdb.cluster; }
cleanup() {
  stop_suite_api
  if [ -e "/var/run/netns/$primary_ns" ]; then
    as_user_in_primary scripts/dev-stack.sh stop >/dev/null 2>&1 || true
  fi
  for ns in "$node_ns" "$primary_ns"; do
    sudo ip netns pids "$ns" 2>/dev/null | xargs -r sudo kill -9 2>/dev/null || true
    sudo ip netns del "$ns" 2>/dev/null || true
  done
  bash "$here/nbd-orphans.sh"
  wipe_stack_state
  sudo systemctl start swarmy-tunnel swarmyd
}
sudo systemctl stop swarmyd swarmy-tunnel
trap cleanup EXIT
scripts/dev-stack.sh stop >/dev/null 2>&1 || true
wipe_stack_state
sudo chown -R "$user:$(id -gn)" "$(readlink -f target)"
CARGO_BUILD_JOBS=8 cargo build --locked -p swarmy-chaos -p swarmy-cli -p swarmy-core-bin > ~/suite-build-latency.log 2>&1 \
  || { tail -20 ~/suite-build-latency.log; echo "build failed"; echo "SUITES_EXIT=1"; exit 2; }
tail -1 ~/suite-build-latency.log
for ns in "$primary_ns" "$node_ns"; do sudo ip netns del "$ns" 2>/dev/null || true; done
sudo modprobe sch_netem
if ! { sudo ip netns add "$primary_ns" && sudo ip netns add "$node_ns" \
  && sudo ip link add swl-p netns "$primary_ns" type veth peer name swl-n netns "$node_ns" \
  && sudo ip -n "$primary_ns" addr add "$primary_ip/30" dev swl-p \
  && sudo ip -n "$node_ns" addr add "$node_ip/30" dev swl-n \
  && sudo ip -n "$primary_ns" link set lo up && sudo ip -n "$node_ns" link set lo up \
  && sudo ip -n "$primary_ns" link set swl-p up && sudo ip -n "$node_ns" link set swl-n up; }; then
  echo "namespace setup failed"; echo "SUITES_EXIT=1"; exit 2
fi
as_user_in_primary scripts/dev-stack.sh start "$primary_ip" 2>&1 | tail -2
set -a; . .dev/env; set +a
export SWARMY_NATS_URL="nats://$primary_ip:4222"
export SWARMY_S3_ENDPOINT="http://$primary_ip:8333"
start_suite_api ~/latency-api.log sudo -E ip netns exec "$primary_ns"
echo "== image build"
in_primary ./target/debug/swarmy image build images/base-ubuntu --tag dev 2>&1 | tail -2
stop_suite_api
mkdir -p ~/suite-logs
rc=0
for rtt in ${LATENCY_RTTS:-175 400}; do
  if ! [[ $rtt =~ ^[1-9][0-9]*$ ]]; then echo "LATENCY_RTTS entry '$rtt' is not a positive integer"; rc=1; continue; fi
  sudo ip netns exec "$primary_ns" tc qdisc replace dev swl-p root netem delay "$((rtt * 500))us"
  sudo ip netns exec "$node_ns" tc qdisc replace dev swl-n root netem delay "$((rtt * 500))us"
  echo "== round trip $rtt ms"
  bash "$here/nbd-orphans.sh"
  full=~/suite-logs/${branch##*/}-latency-$rtt.log
  if in_primary ./target/debug/swarmy-chaos --no-start-stack --bin-dir target/debug \
      --image base-ubuntu:dev --sessions 1 --steps 2 --kills 0 --gateways 1 \
      --session-timeout-secs 600 --node-netns "$node_ns" \
      --max-node-transaction-ms "$((rtt * 4 + 150))" > "$full" 2>&1; then
    echo "LATENCY_RTT=$rtt ok"
  else
    rc=1
    echo "LATENCY_RTT=$rtt FAILED; see $full"
    tail -5 "$full"
  fi
done
echo "SUITES_EXIT=$rc"
exit "$rc"
