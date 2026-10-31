#!/usr/bin/env bash
# Usage: latency.sh BRANCH. The primary and node live in separate namespaces;
# only the veth ends receive netem delay. LATENCY_RTTS defaults to "175 400".
# The namespaces have no route out, so the base image is built on the host and
# pulled inside the primary namespace from a static server on its loopback,
# laid out like a release (SHA256SUMS plus the image tarball).
# pasta, which gives each sandbox its network, exits with "No routable
# interface for IPv4" unless the namespace it starts from has a default route
# to copy. Sandboxes start in the node namespace and, for chaos's final disk
# check, in the primary one, so each gets a default route pointing at the
# other end of the veth. Neither has an uplink, so there is still no way out.
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
release_pid=""
work=""
cd ~/chaos
git fetch -q origin "$branch" && git checkout -q -B suite "${REV:-origin/$branch}" || { echo "checkout failed"; echo "SUITES_EXIT=1"; exit 2; }
echo "== branch $branch at $(git rev-parse --short HEAD)"
# nsenter switches only the network namespace. `ip netns exec` also remounts
# /sys in a private mount namespace, which drops /sys/fs/cgroup, and the
# node's sandbox runtime then fails with "no cgroup mount found in mountinfo".
# Background jobs use the primary_cmd array, not a function, so $! is the
# sudo process itself and killing it stops the service.
primary_cmd=(sudo -E nsenter --net="/run/netns/$primary_ns")
in_ns() { local ns=$1; shift; sudo -E nsenter --net="/run/netns/$ns" "$@"; }
in_primary() { "${primary_cmd[@]}" "$@"; }
as_user_in_primary() { in_primary sudo -u "$user" env "PATH=$PATH" "HOME=$HOME" "$@"; }
wipe_stack_state() { sudo rm -rf .dev/fdb .dev/nats .dev/seaweed .dev/fdb.cluster; }
stop_release_server() {
  [ -n "$release_pid" ] || return 0
  sudo kill "$release_pid" 2>/dev/null
  wait "$release_pid" 2>/dev/null
  release_pid=""
}
# Serve every request path by its last component from DIR, so the release tag
# in the URL does not matter. Writes the bound port to PORT_FILE once listening.
release_server='
import http.server, os, sys
root, port_file = sys.argv[1], sys.argv[2]
class Handler(http.server.SimpleHTTPRequestHandler):
    def translate_path(self, path):
        return os.path.join(root, os.path.basename(path.split("?", 1)[0]))
    def log_message(self, *args):
        pass
server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(port_file + ".tmp", "w") as f:
    f.write(str(server.server_address[1]))
os.rename(port_file + ".tmp", port_file)
server.serve_forever()
'
cleanup() {
  stop_release_server
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
  [ -z "$work" ] || sudo rm -rf "$work"
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
echo "== image build on the host"
work=$(mktemp -d ~/latency-image.XXXXXX)
top=swarmy-image-base-ubuntu-$(uname -m)-unknown-linux-gnu
mkdir -p "$work/stage/$top" "$work/www" "$work/cache"
if ! { ./target/debug/swarmy image build images/base-ubuntu --tag dev --output "$work/stage/$top/disk.ext4" --no-register 2>&1 | tail -2 \
  && install -m 0644 images/base-ubuntu/recipe.toml "$work/stage/$top/recipe.toml" \
  && tar -C "$work/stage" --sparse -I 'gzip -1' -cf "$work/www/$top.tar.gz" "$top" \
  && rm -rf "$work/stage" \
  && (cd "$work/www" && sha256sum -- "$top.tar.gz" > SHA256SUMS); }; then
  echo "image build failed"; echo "SUITES_EXIT=1"; exit 2
fi
for ns in "$primary_ns" "$node_ns"; do sudo ip netns del "$ns" 2>/dev/null || true; done
sudo modprobe sch_netem
if ! { sudo ip netns add "$primary_ns" && sudo ip netns add "$node_ns" \
  && sudo ip link add swl-p netns "$primary_ns" type veth peer name swl-n netns "$node_ns" \
  && sudo ip -n "$primary_ns" addr add "$primary_ip/30" dev swl-p \
  && sudo ip -n "$node_ns" addr add "$node_ip/30" dev swl-n \
  && sudo ip -n "$primary_ns" link set lo up && sudo ip -n "$node_ns" link set lo up \
  && sudo ip -n "$primary_ns" link set swl-p up && sudo ip -n "$node_ns" link set swl-n up \
  && sudo ip -n "$node_ns" route add default via "$primary_ip" dev swl-n \
  && sudo ip -n "$primary_ns" route add default via "$node_ip" dev swl-p; }; then
  echo "namespace setup failed"; echo "SUITES_EXIT=1"; exit 2
fi
as_user_in_primary scripts/dev-stack.sh start "$primary_ip" 2>&1 | tail -2
set -a; . .dev/env; set +a
export SWARMY_NATS_URL="nats://$primary_ip:4222"
export SWARMY_S3_ENDPOINT="http://$primary_ip:8333"
start_suite_api ~/latency-api.log "${primary_cmd[@]}"
echo "== image pull inside $primary_ns"
"${primary_cmd[@]}" python3 -c "$release_server" "$work/www" "$work/port" > "$work/server.log" 2>&1 &
release_pid=$!
for _ in $(seq 1 60); do [ -s "$work/port" ] && break; sleep 0.5; done
if ! [ -s "$work/port" ] || ! in_primary env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    SWARMY_RELEASE_BASE_URL="http://127.0.0.1:$(cat "$work/port")" XDG_CACHE_HOME="$work/cache" \
    ./target/debug/swarmy image pull --tag dev 2>&1 | tail -3; then
  cat "$work/server.log"; echo "image pull failed"; echo "SUITES_EXIT=1"; exit 2
fi
stop_release_server
stop_suite_api
sudo rm -rf "$work"
work=""
mkdir -p ~/suite-logs
rc=0
for rtt in ${LATENCY_RTTS:-175 400}; do
  if ! [[ $rtt =~ ^[1-9][0-9]*$ ]]; then echo "LATENCY_RTTS entry '$rtt' is not a positive integer"; rc=1; continue; fi
  in_ns "$primary_ns" tc qdisc replace dev swl-p root netem delay "$((rtt * 500))us"
  in_ns "$node_ns" tc qdisc replace dev swl-n root netem delay "$((rtt * 500))us"
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
