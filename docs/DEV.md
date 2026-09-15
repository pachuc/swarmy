# Local development

## Quick start

Install the pinned backing services using the instructions below. With Rust and
those binaries installed, run these commands from a clean checkout. The commands
work in Bash and fish without sourcing an environment file.

```sh
cargo build --workspace --locked
./target/debug/swarmy dev up
./target/debug/swarmy run "hello"
./target/debug/swarmy dev status
./target/debug/swarmy dev down
```

`up` starts FoundationDB, NATS, and SeaweedFS when needed, then starts one
scheduler, worker, and gateway under a background CLI supervisor. It returns
when their logs report readiness and prints process ids. The default fake
provider replies `Hello from swarmy!` without credentials. Builds are separate
from startup; build again after changing Rust code. All four binaries must live
in the same directory. You can add `target/debug` to your shell's executable
search path to use `swarmy` directly. The FoundationDB client library must be
installed in the system library path as shown below.

```sh
./target/debug/swarmy dev logs           # follow all three service logs
./target/debug/swarmy dev logs worker    # follow one service; Ctrl-C ends tailing
./target/debug/swarmy dev logs supervisor
```

`status` shows the backing stack, services, and supervisor with pids and uptime.
`down` stops them all and preserves data. Repeating `up` reports and replaces
recorded processes, including survivors of a supervisor killed with SIGKILL.
It reloads configuration, so edits take effect on the next `up`. An unexpected
service exit stops the other services; inspect the logs and run `up` again.
PID records contain Linux process start times to guard against PID reuse.

## Shared configuration

Every binary searches upward from its current directory for
`.swarmy/config.toml`, then checks `$XDG_CONFIG_HOME/swarmy/config.toml` or
`$HOME/.config/swarmy/config.toml`. The nearest project file wins.
Environment variables override the file, and omitted fields use local defaults.
Relative filesystem paths in a project file are relative to the directory
containing `.swarmy`; paths in the user file are relative to its directory.
Relative environment paths remain relative to the invoking directory.

`dev up` creates the project file if absent, imports connection settings from
`.dev/env` as data, and saves the file with private permissions. It updates the
stack connection fields on each start and preserves other settings. Environment
overrides are passed to children but are not saved. Service logs, pid records,
and the default fake script and call log live in `.swarmy/dev/`.

The generated file contains every setting. This partial example shows the
common choices (store directories are FoundationDB directory names):

```toml
store_directory = "swarmy"
bus_prefix = ""
provider = "fake"
model = "gpt-5"
reasoning_effort = "medium"
credential_file = "/home/me/.swarmy/auth.json"
worker_partitions = "0-255"
scheduler_partitions = "0-255"

[fake]
script = ".swarmy/dev/fake.json"
call_log = ".swarmy/dev/calls.log"
```

The connection keys are `fdb_cluster_file`, `nats_url`, `s3_endpoint`,
`s3_access_key`, `s3_secret_key`, `s3_bucket`, and `s3_region`. Additional settings
are `scheduler_scan_interval_ms`, `scheduler_resend_interval_ms`,
`worker_lease_ms`, `worker_recovery_interval_ms`, `bus_ack_wait_ms`,
`bus_max_deliver`, `gateway_concurrency`, and `system_prompt`. The optional
`worker_kill_point` retains the worker failure-injection setting. Existing
`SWARMY_*` names still work: uppercase the key and add `SWARMY_`. Exceptions
are `credential_file` (`SWARMY_CHATGPT_AUTH`), `[fake].script`
(`SWARMY_FAKE_SCRIPT`), and `[fake].call_log` (`SWARMY_FAKE_CALL_LOG`).

To use ChatGPT, set `provider = "chatgpt"`, choose the model and effort, and set
`credential_file` to a dedicated credential file. `swarmy auth login` and the
gateway read that same path. `swarmy auth --auth-file PATH login` overrides it
for a login. Never share a refresh writer with a running Codex login.

## Backing service installation

Run FoundationDB, NATS with JetStream, and SeaweedFS as background processes on
Linux. The script uses Bash, curl 7.75 or newer (for AWS request signing), standard
Linux utilities, and the service binaries. It does not need a container runtime
or root privileges. Installation requires root privileges.

Use these exact versions, matching `.daytona/Dockerfile` and CI:

| Service | Version | Binaries |
| --- | --- | --- |
| FoundationDB | 7.3.79 | `fdbserver`, `fdbcli`, and the client library |
| NATS | 2.14.6 | `nats-server` |
| SeaweedFS | 4.47 | `weed` |

The installation commands below target x86-64 machines. Release assets come from
[FoundationDB](https://github.com/apple/foundationdb/releases/tag/7.3.79),
[NATS](https://github.com/nats-io/nats-server/releases/tag/v2.14.6), and
[SeaweedFS](https://github.com/seaweedfs/seaweedfs/releases/tag/4.47).

### Ubuntu

Run this in Bash. Extract the verified FoundationDB Debian packages to install
their binaries, headers, and client library without package hooks starting a
system service. This is also how the development image and CI install them.

```bash
set -euo pipefail
sudo apt-get update
sudo apt-get install -y ca-certificates curl tar
install_dir=$(mktemp -d)
cd "$install_dir"
FDB_VERSION=7.3.79
for pkg in clients server; do
  deb="foundationdb-${pkg}_${FDB_VERSION}-1_amd64.deb"
  curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${deb}"
  curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${deb}.sha256"
  sha256sum -c "${deb}.sha256"
  sudo dpkg-deb -x "$deb" /
done
sudo ldconfig
cd -
rm -rf -- "$install_dir"
```

Then install NATS and SeaweedFS using the shared instructions below.

### Arch Linux

Install the pinned upstream FoundationDB binaries and client library instead of
relying on the version currently packaged by Arch or the AUR. Run this in Bash:

```bash
set -euo pipefail
sudo pacman -S --needed ca-certificates curl tar
install_dir=$(mktemp -d)
cd "$install_dir"
FDB_VERSION=7.3.79
for asset in fdbserver.x86_64 fdbcli.x86_64 libfdb_c.x86_64.so; do
  curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${asset}"
  curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${asset}.sha256"
  sha256sum -c "${asset}.sha256"
done
sudo install -Dm755 fdbserver.x86_64 /usr/local/bin/fdbserver
sudo install -Dm755 fdbcli.x86_64 /usr/local/bin/fdbcli
sudo install -Dm755 libfdb_c.x86_64.so /usr/local/lib/libfdb_c.so
printf '/usr/local/lib\n' | sudo tee /etc/ld.so.conf.d/swarmy-fdb.conf >/dev/null
sudo ldconfig
cd -
rm -rf -- "$install_dir"
```

### NATS and SeaweedFS on either distribution

```bash
set -euo pipefail
install_dir=$(mktemp -d)
cd "$install_dir"
NATS_VERSION=2.14.6
SEAWEEDFS_VERSION=4.47
curl --retry 3 -fsSL "https://github.com/nats-io/nats-server/releases/download/v${NATS_VERSION}/nats-server-v${NATS_VERSION}-linux-amd64.tar.gz" | tar -xz
sudo install -m 0755 "nats-server-v${NATS_VERSION}-linux-amd64/nats-server" /usr/local/bin/nats-server
curl --retry 3 -fsSL "https://github.com/seaweedfs/seaweedfs/releases/download/${SEAWEEDFS_VERSION}/linux_amd64.tar.gz" | tar -xz
sudo install -m 0755 weed /usr/local/bin/weed
cd -
rm -rf -- "$install_dir"
export PATH="$PATH:/usr/sbin"
fdbserver --version
fdbcli --version
nats-server --version
weed version
```

## Manual reference

### Run only the backing stack

From the repository root:

```bash
scripts/dev-stack.sh start
source .dev/env
scripts/dev-stack.sh status
```

`start` waits for FoundationDB to become available, for the NATS JetStream
monitoring endpoint, and for an authenticated S3 bucket listing. It configures
a new single-node FoundationDB database with the `ssd` engine on first start,
creates the S3 bucket `swarmy`, and writes `.dev/env` only after readiness checks
pass. Repeating `start` reuses running processes and existing data. You can also
invoke the script by absolute path from another directory.

All data, configuration, PID files, and logs live under the ignored `.dev/`
directory. Logs are in `.dev/logs/` and FoundationDB trace logs are in
`.dev/fdb/logs/`. PID files record both the PID and Linux process start time so
that `stop` does not signal an unrelated process after PID reuse. `status` reports
process liveness; use the requests below to check service health.

Services bind to `127.0.0.1`. Reserve ports 4500 for FoundationDB; 4222 and 8222
for NATS; and 8080, 8333, 8888, 9333, 18080, 18333, 18888, and 19333 for
SeaweedFS HTTP and gRPC APIs. Stop any system-installed service using these ports
first. S3 uses the fixed local development credentials `swarmy-dev` and
`swarmy-dev-secret`, with admin rights. These credentials are for this local stack.

```bash
fdbcli -C "$SWARMY_FDB_CLUSTER_FILE" --exec status
curl --fail http://127.0.0.1:8222/jsz
curl --fail --aws-sigv4 "aws:amz:$SWARMY_S3_REGION:s3" \
  --user "$SWARMY_S3_ACCESS_KEY:$SWARMY_S3_SECRET_KEY" "$SWARMY_S3_ENDPOINT/"
```

The last request lists buckets and should include `swarmy`. An unsigned request
to the S3 endpoint returns an authentication error. When uploading a body with
curl, also pass `--header 'x-amz-content-sha256: UNSIGNED-PAYLOAD'` so older curl
versions and SeaweedFS agree on payload signing.

Stop the processes cleanly, preserving data for the next start:

```bash
scripts/dev-stack.sh stop
scripts/dev-stack.sh status
```

`stop` sends SIGTERM and waits up to 30 seconds per process. If a process does
not exit, it reports a failure and retains its PID file for a retry. Failed or
interrupted startup stops the processes launched by that invocation. A lock
prevents simultaneous `start` and `stop` operations. If the script itself was
killed with SIGKILL, first verify no `start` or `stop` invocation is still running,
then remove the stale lock with `rmdir .dev/lock` and run `stop` before retrying.
To reset all local data, stop successfully and then remove `.dev/`.

### Environment and tests

When using the manual flow without a configuration file, source `.dev/env` in
every Bash shell that runs integration tests. Its
exports are inherited by child processes; starting the stack alone cannot change
the calling shell's environment.

| Variable | Value |
| --- | --- |
| `SWARMY_FDB_CLUSTER_FILE` | Absolute path to `.dev/fdb.cluster` |
| `SWARMY_NATS_URL` | `nats://127.0.0.1:4222` |
| `SWARMY_S3_ENDPOINT` | `http://127.0.0.1:8333` |
| `SWARMY_S3_ACCESS_KEY` | `swarmy-dev` |
| `SWARMY_S3_SECRET_KEY` | `swarmy-dev-secret` |
| `SWARMY_S3_BUCKET` | `swarmy` |
| `SWARMY_S3_REGION` | `us-east-1` |

S3 clients should use path-style bucket addressing with this endpoint. Tests
requiring a backing system must skip cleanly when its environment variables are
absent. Run the same checks as CI:

```bash
source .dev/env
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

The Ubuntu CI job installs these pinned versions, starts the stack, and copies
the exported settings into `GITHUB_ENV` so subsequent test steps inherit them.
Its cleanup step runs even if an earlier step fails.

## Session failure injection

The slice 1 acceptance program builds and launches scheduler, worker, and gateway
binaries, then creates sessions through the same store and scheduler wake APIs
as `swarmy run`. It starts the development stack if needed and sources `.dev/env`
in a child process. No provider credentials are needed.

```sh
cargo run --locked -p swarmy-chaos -- --sessions 20 --steps 5 --kills 15
```

Defaults are twenty concurrent sessions, five inference steps per session,
fifteen SIGKILL/restart pairs, and two processes of each service kind. Each
intermediate response calls `get_time`; the last response is the fixed answer
`chaos session complete`. Each gateway handles one call at a time, making the
allowed cost of each gateway kill at most one additional provider call.

Use `--schedulers`, `--workers`, and `--gateways` to change process counts.
`--min-interval-ms` and `--max-interval-ms` set the inclusive random interval range
(default 100 to 350 ms). `--latency-ms` delays each fake delta (default 100 ms,
two deltas per response). The program fails if sessions finish before all kills
are injected; increase latency or shorten intervals for very small workloads.
Every killed slot is immediately restarted with its original environment.

The program prints its seed, every victim and interval, call totals, and elapsed
time, excluding builds and stack startup. Pass `--seed NUMBER` to replay the random choices; process and database
scheduling still vary. `--session-timeout-secs` defaults to 120 and covers waking
and observing each session. Failures identify the session, last observed state,
and last eight events. Script, call log, and service logs are retained in a
reported temporary directory on failure.

Each run uses a fresh ULID for its FoundationDB directory and NATS prefix. It
stops its service children and removes recorded snapshots, directory, streams, and
temporary files at the end. The development stack stays running for reuse.
Stored events and inference inputs use the libraries' versioned encoding.
Logs must have contiguous sequence numbers from one, unique request ids and
step ids, and one request per inference completion. A second pending request
fails immediately, including when it uses a different id. Completed sessions
must be Idle, have the expected number of requests and clock results, and carry
the expected final answer in their last inference completion. The synced fake
call log must contain between `sessions * steps` and that total plus gateway
kills, inclusive.

For an already running stack, export its settings and use `--no-start-stack`.
The reduced CI command is:

```sh
scripts/dev-stack.sh start
source .dev/env
scripts/chaos-ci.sh
```

This runs five sessions, three steps, and four kills with seed 1. The GitHub
Actions workflow runs it after the lint and test steps, with the stack already
started. The script accepts additional runner arguments, such as `--latency-ms 200`.
`--bin-dir PATH` uses prebuilt binaries without rebuilding, for experiments with
instrumented services. Otherwise the runner builds services beside its own
executable using the active Cargo target directory and debug or release profile.

To add another killable service in a later slice, add its `Kind` variant and
binary name in `crates/swarmy-chaos/src/process.rs`, then register its process
count and environment at startup. Restart and health checks remain shared.
