# Sourced by node-suite scripts. Run the API through an optional command prefix.
suite_api_pid=""
suite_api_prefix=()
start_suite_api() {
  local log=$1 port
  shift
  suite_api_prefix=("$@")
  [ -x ./target/debug/swarmy-core ] || return 0
  port=$("$@" python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
  export SWARMY_API_TOKEN="suite-$(date +%s)-$RANDOM"
  export SWARMY_API_URL="http://127.0.0.1:$port"
  "$@" env SWARMY_API_LISTEN="127.0.0.1:$port" ./target/debug/swarmy-core api > "$log" 2>&1 &
  suite_api_pid=$!
  for _ in $(seq 1 120); do
    "$@" bash -c "echo > /dev/tcp/127.0.0.1/$port" 2>/dev/null && break
    "$@" kill -0 "$suite_api_pid" 2>/dev/null || { echo "swarmy-core api exited; see $log"; break; }
    sleep 0.5
  done
  echo "== api on $SWARMY_API_URL (pid $suite_api_pid)"
}
stop_suite_api() {
  [ -n "$suite_api_pid" ] || return 0
  "${suite_api_prefix[@]}" kill "$suite_api_pid" 2>/dev/null
  wait "$suite_api_pid" 2>/dev/null
  suite_api_pid=""
}
