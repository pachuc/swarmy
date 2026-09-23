#!/usr/bin/env bash
set -euo pipefail

# Debian installs fdbserver outside the PATH used by some unprivileged shells.
export PATH="$PATH:${HOME:?HOME must be set}/.local/bin:/usr/sbin"
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
dev_dir="$repo_dir/.dev"
services=(fdb nats seaweed)
started=()
locked=false
start_complete=false
advertise_address=${2:-127.0.0.1}
bind_address=$advertise_address
# Wildcard listening keeps loopback clients working when a private address is advertised.
if [[ $advertise_address != 127.0.0.1 ]]; then bind_address=0.0.0.0; fi

fail() {
    printf '%s\n' "$*" >&2
    exit 1
}

# Linux start times prevent stale PID files from identifying a reused PID.
process_info() {
    local stat
    local -a fields
    [[ $pid =~ ^[1-9][0-9]*$ && -r /proc/$pid/stat ]] || return 1
    read -r stat < "/proc/$pid/stat" || return 1
    read -r -a fields <<< "${stat##*) }"
    [[ ${fields[0]} != Z && ${fields[0]} != X ]] || return 1
    process_start=${fields[19]}
}

running() {
    local saved_start
    [[ -f $dev_dir/$1.pid ]] || return 1
    read -r pid saved_start < "$dev_dir/$1.pid" || return 1
    process_info && [[ $process_start == "$saved_start" ]] && kill -0 "$pid" 2>/dev/null
}

stop_service() {
    local service=$1 deadline
    if running "$service"; then
        kill -TERM "$pid" 2>/dev/null || true
        deadline=$((SECONDS + 30))
        while running "$service"; do
            if (( SECONDS >= deadline )); then
                printf '%s did not stop within 30 seconds; kept its PID file.\n' "$service" >&2
                return 1
            fi
            sleep 0.2
        done
    fi
    rm -f -- "$dev_dir/$service.pid"
    if [[ $service == fdb && -f $dev_dir/fdb.cluster ]]; then
        read_fdb_port
        printf 'fdb: down (port %s)\n' "$fdb_port"
    else
        printf '%s: down\n' "$service"
    fi
}

read_fdb_port() {
    local cluster
    [[ -f $dev_dir/fdb.cluster ]] || fail 'Missing .dev/fdb.cluster.'
    cluster=$(tr -d '\n\r' < "$dev_dir/fdb.cluster")
    [[ $cluster =~ ^dev:dev@([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)$ ]] \
        || fail 'Invalid .dev/fdb.cluster.'
    fdb_address=${BASH_REMATCH[1]}
    fdb_port=${BASH_REMATCH[2]}
}

port_in_use() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

cleanup() {
    local result=$? service
    trap - EXIT INT TERM
    if [[ $start_complete == false ]]; then
        for service in "${started[@]}"; do
            stop_service "$service" || result=1
        done
    fi
    if [[ $locked == true ]]; then
        rmdir -- "$dev_dir/lock"
    fi
    exit "$result"
}

launch() {
    local service=$1
    shift
    if running "$service"; then
        printf '%s: already running\n' "$service"
        return
    fi
    # Ignore hangups and detach standard input so the stack outlives this shell.
    (trap '' HUP; exec "$@") </dev/null >>"$dev_dir/logs/$service.log" 2>&1 &
    pid=$!
    process_info || fail "$service exited during launch; see .dev/logs/$service.log"
    printf '%s %s\n' "$pid" "$process_start" > "$dev_dir/$service.pid"
    started+=("$service")
}

wait_ready() {
    local service=$1 deadline=$((SECONDS + 90))
    shift
    until "$@" >"$dev_dir/logs/$service-ready.log" 2>&1; do
        running "$service" || fail "$service exited; see .dev/logs/$service.log"
        (( SECONDS < deadline )) || fail "$service is not ready; see .dev/logs/$service*.log"
        sleep 1
    done
    running "$service" || fail "$service exited; see .dev/logs/$service.log"
    printf '%s: ready\n' "$service"
}

fdb_ready() {
    local output
    output=$(fdbcli -C "$dev_dir/fdb.cluster" --timeout 3 --exec 'status minimal') || return 1
    printf '%s\n' "$output"
    [[ $output == *'The database is available.'* ]]
}

s3_request() {
    curl --fail --silent --show-error --connect-timeout 1 --max-time 3 \
        --noproxy '*' --aws-sigv4 'aws:amz:us-east-1:s3' \
        --header 'x-amz-content-sha256: UNSIGNED-PAYLOAD' \
        --user 'swarmy-dev:swarmy-dev-secret' "$@"
}

start() {
    local binary service port requested_port
    local -a ports
    for binary in fdbserver fdbcli nats-server weed curl; do
        command -v "$binary" >/dev/null || fail "Missing $binary; see docs/DEV.md for installation."
    done
    [[ $(curl --help all) == *'--aws-sigv4'* ]] || fail 'curl 7.75 or newer is required for S3 authentication.'
    mkdir -p -- "$dev_dir/fdb/data" "$dev_dir/fdb/logs" "$dev_dir/nats" "$dev_dir/seaweed" "$dev_dir/logs"

    if [[ -f $dev_dir/fdb.cluster ]]; then
        read_fdb_port
        [[ $fdb_address == "$advertise_address" ]] \
            || fail 'The cluster file advertises another address; use the original advertise-address.'
    else
        fdb_port=4500
    fi
    requested_port=${SWARMY_DEV_FDB_PORT:-}
    if [[ -n $requested_port ]]; then
        [[ $requested_port =~ ^[1-9][0-9]{0,4}$ ]] && (( requested_port <= 65535 )) \
            || fail 'SWARMY_DEV_FDB_PORT must be a port from 1 to 65535.'
        if running fdb && [[ $fdb_port != "$requested_port" ]]; then
            fail "FoundationDB is already running on port $fdb_port."
        fi
        fdb_port=$requested_port
    elif ! running fdb && port_in_use "$fdb_port"; then
        for ((port = 4500; port <= 65535; port++)); do
            if ! port_in_use "$port"; then
                fdb_port=$port
                break
            fi
        done
        (( port <= 65535 )) || fail 'No free FoundationDB port is available.'
    fi

    # Refuse occupied ports before launching, including SeaweedFS internal APIs.
    for service in "${services[@]}"; do
        running "$service" && continue
        case $service in
            fdb) ports=("$fdb_port") ;;
            nats) ports=(4222 8222) ;;
            seaweed) ports=(8080 8333 8888 9333 18080 18333 18888 19333) ;;
        esac
        for port in "${ports[@]}"; do
            if port_in_use "$port"; then
                fail "Port $port is already in use by an unmanaged process; stop it before starting the stack."
            fi
        done
    done

    printf 'dev:dev@%s:%s\n' "$advertise_address" "$fdb_port" > "$dev_dir/fdb.cluster"
    cat > "$dev_dir/s3.json" <<'JSON'
{
  "identities": [{
    "name": "swarmy-dev",
    "credentials": [{"accessKey": "swarmy-dev", "secretKey": "swarmy-dev-secret"}],
    "actions": ["Admin"]
  }]
}
JSON

    launch fdb fdbserver -p "$advertise_address:$fdb_port" -l "$bind_address:$fdb_port" -C "$dev_dir/fdb.cluster" \
        -d "$dev_dir/fdb/data" -L "$dev_dir/fdb/logs"
    launch nats nats-server -js -sd "$dev_dir/nats" -a "$bind_address" --client_advertise "$advertise_address:4222" -p 4222 -m 8222
    # Unix socket paths are limited to about 100 bytes, so they cannot live
    # under a deep repository path. Key a short directory by the repository.
    socket_dir="${TMPDIR:-/tmp}/swarmy-$(printf '%s' "$dev_dir" | sha256sum | cut -c1-12)"
    mkdir -p -- "$socket_dir"
    # Separate S3 test buckets each need collection volume slots.
    launch seaweed weed server -dir "$dev_dir/seaweed" -ip "$advertise_address" -ip.bind "$bind_address" \
        -volume.max 32 \
        -s3 -s3.port 8333 -s3.config "$dev_dir/s3.json" \
        -filer.localSocket "$socket_dir/filer.sock" -s3.localSocket "$socket_dir/s3.sock" \
        -s3.port.iceberg 0 -s3.port.lance 0 -master.telemetry=false

    if [[ ! -f $dev_dir/fdb/configured ]]; then
        # An interrupted first start may already have configured the database.
        fdbcli -C "$dev_dir/fdb.cluster" --timeout 15 --exec 'configure new single ssd' \
            >"$dev_dir/logs/fdb-configure.log" 2>&1 || true
    fi
    wait_ready fdb fdb_ready
    touch "$dev_dir/fdb/configured"
    wait_ready nats curl --fail --silent --show-error --noproxy '*' --max-time 3 http://127.0.0.1:8222/jsz
    wait_ready seaweed s3_request http://127.0.0.1:8333/
    if ! s3_request --head http://127.0.0.1:8333/swarmy >/dev/null 2>&1; then
        s3_request -X PUT http://127.0.0.1:8333/swarmy >"$dev_dir/logs/s3-bucket.log" 2>&1 \
            || fail 'Could not create the swarmy bucket; see .dev/logs/s3-bucket.log'
    fi
    s3_request --head http://127.0.0.1:8333/swarmy >/dev/null

    {
        printf 'export SWARMY_FDB_CLUSTER_FILE=%q\n' "$dev_dir/fdb.cluster"
        printf 'export SWARMY_DEV_FDB_PORT=%q\n' "$fdb_port"
        printf 'export SWARMY_NATS_URL=nats://127.0.0.1:4222\n'
        printf 'export SWARMY_S3_ENDPOINT=http://127.0.0.1:8333\n'
        printf 'export SWARMY_S3_ACCESS_KEY=swarmy-dev\n'
        printf 'export SWARMY_S3_SECRET_KEY=swarmy-dev-secret\n'
        printf 'export SWARMY_S3_BUCKET=swarmy\n'
        printf 'export SWARMY_S3_PREFIX=%q\n' ''
        printf 'export SWARMY_S3_REGION=us-east-1\n'
    } > "$dev_dir/env"
    start_complete=true
    printf 'Stack ready. Run: source %q\n' "$dev_dir/env"
}

case ${1:-} in
    start) [[ $# == 1 || $# == 2 ]] || fail 'Usage: scripts/dev-stack.sh start [advertise-address]' ;;
    stop|status) [[ $# == 1 ]] || fail 'Usage: scripts/dev-stack.sh {stop|status}' ;;
    *) fail 'Usage: scripts/dev-stack.sh {start|stop|status}' ;;
esac

[[ $advertise_address =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail 'Expected an IPv4 advertise address.'

if [[ $1 == status ]]; then
    for service in "${services[@]}"; do
        if running "$service"; then
            if [[ $service == fdb ]]; then
                read_fdb_port
                printf 'fdb: up (pid %s, port %s)\n' "$pid" "$fdb_port"
            else
                printf '%s: up (pid %s)\n' "$service" "$pid"
            fi
        else
            if [[ $service == fdb && -f $dev_dir/fdb.cluster ]]; then
                read_fdb_port
                printf 'fdb: down (port %s)\n' "$fdb_port"
            else
                printf '%s: down\n' "$service"
            fi
        fi
    done
    exit 0
fi

umask 077
mkdir -p -- "$dev_dir"
mkdir -- "$dev_dir/lock" 2>/dev/null || fail 'Another start/stop holds .dev/lock; see docs/DEV.md if it is stale.'
locked=true
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ $1 == start ]]; then
    start
else
    result=0
    for service in seaweed nats fdb; do
        stop_service "$service" || result=1
    done
    exit "$result"
fi
