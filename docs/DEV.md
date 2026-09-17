# Development

## Quick start

Install the pinned backing services using the instructions below. With Rust and
those binaries installed, run these commands from a clean checkout. The commands
work in Bash and fish without sourcing an environment file.

```sh
SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --workspace --locked
./target/debug/swarmy dev up
./target/debug/swarmy doctor
sudo -E ./target/debug/swarmy image build images/base-ubuntu --tag dev
./target/debug/swarmy run --image base-ubuntu:dev "hello"
./target/debug/swarmy dev status
./target/debug/swarmy dev down
```

`up` starts FoundationDB, NATS, and SeaweedFS when needed, then starts one
scheduler, worker, and gateway under a background CLI supervisor. It returns
when their logs report readiness and prints process ids. The default fake
provider replies `Hello from swarmy!` without credentials. Builds are separate
from startup; build again after changing Rust code. The CLI, its `swarmy-session` companion, and all three services must live
in the same directory. You can add `target/debug` to your shell's executable
search path to use `swarmy` directly. When using a system client library, omit `SWARMY_FDB_LIB_DIR`. With a custom
install prefix, set it to that prefix's `lib` directory at build time.

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

To run the backing services and swarmyd on EC2, see [remote node workflow](#remote-node-workflow).

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
default_image = "base-ubuntu:dev"
reasoning_effort = "medium"
credential_file = "/home/me/.swarmy/auth.json"
worker_partitions = "0-255"
scheduler_partitions = "0-255"

[fake]
script = ".swarmy/dev/fake.json"
call_log = ".swarmy/dev/calls.log"
```

Every new session needs a registered image, including sessions that only use
remote tools. Set `default_image = "NAME:TAG"` or `SWARMY_DEFAULT_IMAGE`, or pass
`--image NAME:TAG` to `swarmy run` or `swarmy chat`. The flag overrides the
setting, which has no built-in default. Unknown images fail before a session is
created and the error lists registered images; `swarmy image ls` also lists them.
Image construction requires root. The computer is materialized on first sandbox
tool use, so a fake-provider conversation without sandbox tools needs no node.
Resuming an existing session keeps its pinned image.

The connection keys are `fdb_cluster_file`, `nats_url`, `s3_endpoint`,
`s3_access_key`, `s3_secret_key`, `s3_bucket`, `s3_prefix`, and `s3_region`.
`s3_prefix` defaults to empty. Use it to select a namespace within the bucket;
see the [namespace and migration rules](DESIGN.md#73-snapshot-retention-and-garbage-collection). Additional settings
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

The supported no-root installation path targets Linux x86-64. It needs Bash,
curl, tar, sha256sum, and install. Building Rust also needs a C/C++ toolchain,
pkg-config, and clang/libclang for bindgen. The stack uses curl 7.75 or newer for
AWS request signing. These prerequisites must already be available.

From the checkout:

```sh
scripts/install-dev-tools.sh
```

The script verifies FoundationDB release SHA-256 files, installs the backing
executables into `~/.local/bin` and `libfdb_c.so` into `~/.local/lib`, and prints
a one-line command that installs the CLI and services. Run it in Bash:

```bash
for crate in cli scheduler worker gateway; do SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo install --locked --path "crates/swarmy-$crate"; done
```

Use `scripts/install-dev-tools.sh --prefix /absolute/path` for another location,
then run its printed command. The build embeds that library directory in the
runtime search path and uses it at link time. The shared build script also adds
existing `/usr/lib`, `/usr/local/lib`, and `/usr/lib/x86_64-linux-gnu` directories.
It emits the same rpath option on macOS, where the library is `libfdb_c.dylib`;
the installer and process supervisor currently target Linux.

swarmy searches the caller's PATH first, then the build-time install prefix's
`bin`, `~/.local/bin`, and `/usr/sbin`. It passes this path to the stack script.
No shell profile changes or `LD_LIBRARY_PATH` exports are needed. To call a
backing tool directly, use its full path or add its bin directory to your PATH.

Versions match `.daytona/Dockerfile` and the backing stack:

| Service | Version | Installed files |
| --- | --- | --- |
| FoundationDB | 7.3.79 | `fdbserver`, `fdbcli`, `libfdb_c.so` |
| NATS | 2.14.6 | `nats-server` |
| SeaweedFS | 4.47 | `weed` |

For machines with these dependencies installed system-wide, including CI's
Ubuntu runner, the checkout installation needs no variable:

```bash
cargo install --locked --path crates/swarmy-cli
for crate in scheduler worker gateway; do cargo install --locked --path "crates/swarmy-$crate"; done
```

Cargo puts all executables in `~/.cargo/bin` by default. Use `swarmy` after
rustup's normal shell setup, or `~/.cargo/bin/swarmy` directly. Keep the checkout
because `swarmy dev` uses `scripts/dev-stack.sh` from it.

### Diagnose an installation

```sh
swarmy dev up
swarmy doctor
swarmy doctor --json
```

Doctor checks the effective configuration, the FoundationDB client library and
API version, each backing executable and its version, the installed companion
and services, and ChatGPT credential validity when that provider is selected.
It reads credentials without refreshing them or printing their contents.
Once `.dev` exists it probes the configured FoundationDB coordinator, NATS, and
S3 ports with timeouts. Port connectivity does not verify database or S3
permissions. A stopped stack reports a fix pointing to `swarmy dev up`.
Before initialization, doctor asks for the missing config and tells you that the
stack has not been initialized. Each failure includes a fix and causes exit 1.
JSON output is one object containing `ok` and a `checks` array; each check has
`name`, `ok`, `detail`, and an optional `fix`.

The public CLI loads the client only for its doctor probe. Database commands
run through the installed `swarmy-session` companion, so doctor can still name
a missing `libfdb_c` even when the database commands cannot start.

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
| `SWARMY_S3_PREFIX` | empty |
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

The S3 namespace acceptance test also needs `SWARMY_S3_TEST_BUCKET` naming a
pre-created, dedicated empty bucket. It refuses a non-empty bucket and cleans
up its objects and metadata after each case, including assertion failures.
It tests empty and nested prefixes, more than 1000 objects in one listing,
legacy compatibility, sibling isolation, and dry and real collection. For the
local SeaweedFS stack:

```bash
source .dev/env
export SWARMY_S3_TEST_BUCKET=swarmy-s3-namespace-test
curl --fail --silent --show-error --noproxy '*' \
  --aws-sigv4 'aws:amz:us-east-1:s3' \
  --header 'x-amz-content-sha256: UNSIGNED-PAYLOAD' \
  --user 'swarmy-dev:swarmy-dev-secret' -X PUT \
  "http://127.0.0.1:8333/$SWARMY_S3_TEST_BUCKET"
cargo test -p swarmy-store --test s3_namespace --locked -- --nocapture
```

The same test runs against cloud S3 by setting the S3 endpoint, credentials,
region, and a dedicated empty test bucket, with a reachable FoundationDB.

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

## Remote node workflow

The laptop runs the CLI, scheduler, worker, and inference gateway as your normal
user. EC2 runs FoundationDB, NATS, SeaweedFS, and the privileged `swarmyd` that
hosts agent computers. SSH forwards the coordinator, NATS, and S3 endpoints.
The current FoundationDB client also connects to the node's advertised private
address: this workflow presently needs direct private-network reachability to
TCP 4500 (for example, a client in the same VPC). SSH alone from an external
laptop is not proven to work; see the [measured limitation](volume-benchmarks.md#2026-09-17-remote-node-workflow-without-local-root).
No local root, container runtime, NBD device, or cloud CLI is required by
`swarmy remote`. The current client and supervisor target Linux x86-64; macOS support is not
established by this procedure. Provisioning uses passwordless sudo **on the
Ubuntu EC2 node**, including for packages, kernel modules, and systemd.

### Prerequisites and configuration

Start in a swarmy checkout with the pinned Rust toolchain, C/C++ compiler,
pkg-config, clang/libclang, OpenSSH client (`ssh` and `ssh-keygen`), and rsync.
Install the user-owned tools and build the local binaries:

```bash
scripts/install-dev-tools.sh
rustup show
SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --workspace --locked
export PATH="$PWD/target/debug:$PATH"
```

Only the FoundationDB client library is used locally in remote mode; the
installer also supplies backing executables for local development. It needs
no sudo. Preinstalled compilers and system prerequisites are assumed.

Supply AWS credentials through the standard SDK credential chain, for example
a profile in `~/.aws/credentials` with `export AWS_PROFILE=development`, or
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and, for temporary credentials,
`AWS_SESSION_TOKEN`. Do not put credentials in the checkout. Provisioning needs
EC2 DescribeImages, DescribeInstances, DescribeKeyPairs, RunInstances,
ImportKeyPair, CreateTags, TerminateInstances, DeleteKeyPair, and SSM GetParameter
for the default AMI. Provider cleanup queries below additionally need
DescribeVolumes. No Google Cloud resources or credentials are needed.

Choose a subnet with outbound internet access in the configured region and a
security group in that VPC with inbound TCP 22 from your public address (`/32`)
and **all TCP ports from that same security group**. Both nodes use that group.
Backing services listen on all interfaces so joining nodes can reach them;
do not admit their ports from the internet. A launcher in the same VPC can use
the private address and group membership instead of public SSH ingress.

Create `.swarmy/config.toml` with private permissions, or merge these settings
into the existing file. Replace the subnet, group, and credential path:

```toml
provider = "chatgpt"
model = "gpt-5"
reasoning_effort = "medium"
credential_file = "/home/me/.swarmy/auth.json"

[remote]
region = "us-east-1"
subnet = "subnet-REPLACE"
security_group = "sg-REPLACE"
instance_type = "m6id.xlarge"
disk_gb = 100
managed_by_tag = "swarmy"
# image = "ami-..."  # optional Ubuntu 24.04 amd64 override
```

Set `managed_by_tag = "codex-launcher"` for the tag-restricted benchmark identity.
Instances, root volumes, and key pairs receive that ownership tag at creation.
The default AMI comes from Canonical's Ubuntu 24.04 SSM parameter. The instance
needs local NVMe storage; caches go there and backing databases go on EBS.

### Up, authenticate, and chat

```bash
chmod 600 .swarmy/config.toml
swarmy dev down                 # stop any local stack before reserving port 4500
swarmy remote up demo           # builds on EC2; prints elapsed time and SSH command
swarmy remote connect demo
swarmy auth login              # dedicated ChatGPT login, on the laptop
swarmy dev up --remote demo
swarmy doctor --remote demo
swarmy remote status
```

FoundationDB must bind local `127.0.0.1:4500` exactly. A local stack using that
port prevents a usable remote connection. Preserving the port does not solve
the private-address connection requirement above. Stop a conflicting local
stack, disconnect, and reconnect.
NATS and S3 alone can use automatically selected alternative local ports.

A new remote has no base image. Use the SSH command printed by `up` to log in
and run these commands **on that EC2 node**; then exit back to the laptop:

```bash
cd ~/swarmy
sudo bash -c 'set -a; . /etc/swarmy/node.env; set +a; /usr/local/bin/swarmy image build images/base-ubuntu --tag remote'
exit
```

This registers `base-ubuntu:remote` in the remote stack. Image construction
needs node root; it never needs laptop root. Start a disk-backed session on
the laptop, then resume the session id printed by `run` in the chat UI:

```bash
swarmy run --remote demo --image base-ubuntu:remote 'Say ready and wait for my next instruction.'
swarmy chat --remote demo SESSION_ID
```

Starting `swarmy chat` without that session id creates or selects a session.
Use `swarmy chat --remote demo --image base-ubuntu:remote` for a new chat, or set
`default_image = "base-ubuntu:remote"` in the laptop configuration. In the
session, ask the agent to use `process_start` to run
`python3 -u -m http.server 18765 --bind 127.0.0.1`. In the next turn ask it to
fetch `http://127.0.0.1:18765/` with `bash` and verify the server is still listed
by `process_list`. Then ask it to write a marker file and call `checkpoint`.
An acknowledged checkpoint makes the disk durable; it does not save running
processes. Escape or Ctrl-C closes chat while the session remains stored.

The ChatGPT credential stays in the laptop's configured file. The local gateway
reads it and sends inference directly to the provider. Remote provisioning
copies the checkout while excluding `.swarmy/`, `.dev/`, `.git/`, `target/`,
`.env`, and `.env.*`; it does not copy the laptop's home or credential cache.
Keep any differently named credential file outside the checkout too, since
rsync copies other checkout files. Do not copy auth.json to the node or share
its refresh writer with a running Codex login. Prompts, outputs, and session
history do live in the remote backing services. The fake-provider acceptance
run requires no ChatGPT credential; see the dated benchmark evidence.

### Add capacity and exercise recovery

```bash
swarmy remote add-node demo
swarmy remote status
swarmy remote logs demo         # Ctrl-C stops following the primary's journal
swarmy dev logs worker          # local routing and placement diagnostics
```

`add-node` launches another `swarmyd` using the saved launch settings and the
first node's private backing-service endpoints. It adds compute, not database
replicas. The first node must remain available. For a controlled failure, locate
the hosting node from worker/node logs and the saved JSON in
`.swarmy/remote/demo.json`. SSH to that node with its saved key and address,
then run `sudo systemctl kill --signal=SIGKILL swarmyd` **there**. The unit
restarts automatically after five seconds. Submit another tool turn in chat;
recovery must wait for expired placement and volume-writer leases. Check that
the durable rebuild notice reports the restart, lost processes, and latest
snapshot. An interrupted call may return the notice as an error; retry the
read-only marker check after recovery. The checkpointed marker should survive;
the HTTP server must be started again. A kill does not guarantee migration to the added node.

### Costs, inspection, and teardown

EC2 time, the 100 GiB gp3 root disk on each node, public IPv4 addresses, and
applicable network transfer cost money. Instance storage is part of the instance
allocation. Every `add-node` adds another instance and disk. Real inference also
uses the operator's provider account. This development stack uses SeaweedFS on
the first node; it does not create a managed S3 bucket. Disconnecting, closing
chat, or stopping local services leaves cloud resources running and billable.

`swarmy dev status` shows local processes. `swarmy remote status` shows saved
instances, SSH reachability, and node heartbeat ages; it does **not** query EC2
power state or billing. Use the AWS console or provider queries for that.
Save the instance ids and key names before teardown, since `down` removes the
local record. With the optional AWS CLI installed as your user:

```bash
aws ec2 describe-instances --region us-east-1 \
  --filters Name=tag:Name,Values=demo,demo-2 \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name,Key:KeyName}'
swarmy dev down
swarmy remote disconnect demo
swarmy remote down demo
# Replace the following values with every id/key saved above.
aws ec2 describe-instances --region us-east-1 --instance-ids i-FIRST i-SECOND \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name}'
aws ec2 describe-volumes --region us-east-1 \
  --filters Name=tag:Name,Values=demo,demo-2 --query 'Volumes[].VolumeId'
aws ec2 describe-key-pairs --region us-east-1 \
  --filters Name=key-name,Values=swarmy-FIRST,swarmy-SECOND --query 'KeyPairs[].KeyName'
```

Expect `terminated` for all recorded instances (or eventual absence), and empty
volume and key-pair lists. `down` waits for termination, deletes imported keys,
and removes local state. It permanently deletes this stack's backing data;
checkpoints here do not survive teardown of the first node. Failed provisioning
retains its state for `remote down`; failed cleanup retains state for retry.
Never delete that state to work around an error while resources still exist.
See [remote provisioning details](REMOTE.md) for the state contract and node
service configuration.

### Tunnel and profile reference

`swarmy remote connect NAME` reads `<state_dir>/remote/NAME.json`, starts an
SSH control master, and writes `NAME.profile.json` beside it. `state_dir`
defaults to the discovered project's `.swarmy` directory; `SWARMY_STATE_DIR`
can select another directory. Provisioning and tunnel commands share the
`swarmy_config::RemoteNode` JSON contract. For an existing host, a state file
can be written directly:

```json
{
  "name": "test",
  "region": "us-east-1",
  "instance_id": "i-example",
  "public_ip": "203.0.113.10",
  "private_ip": "10.0.0.10",
  "key_path": "/home/me/.ssh/test",
  "ssh_user": "ubuntu",
  "ports": { "fdb": 4500, "nats": 4222, "s3": 8333 },
  "nodes": [],
  "created_at": "2026-09-16T00:00:00Z"
}
```

```sh
swarmy remote connect test
swarmy dev up --remote test
swarmy doctor --remote test
swarmy run --remote test 'what time is it'
swarmy chat --remote test
swarmy remote status
swarmy remote logs test
swarmy dev down
swarmy remote disconnect test
```

Selection also works with `export SWARMY_REMOTE=test` or `profile = "test"`
in the config's `[remote]` table. The flag takes precedence over the variable,
which takes precedence over the config setting. The selected profile overrides
the FoundationDB cluster file, NATS URL, and S3 endpoint after other environment
overrides. Credentials, bucket, object prefix, and store directory retain their
normal configuration. Scheduler, worker, and gateway binaries honor the same
variable and config setting without additional flags.

Remote `dev up` starts only scheduler, gateway, and worker. It records that
choice so `dev down` leaves the backing services running, even without a remote
flag. Disconnect stops only the SSH control master and removes its profile and
cluster file; instance state and logs remain. Reconnecting a healthy tunnel is
idempotent. SSH uses the configured identity, batch authentication, normal host
key verification with first-use acceptance, and keepalives. SSH diagnostics are
saved in `NAME.ssh.log`. `remote logs` follows the `swarmyd` systemd journal until
interrupted; the SSH user needs permission to read that journal.

Local ports prefer 4500, 4222, and 8333, with free ephemeral ports chosen on
collision. All forwards bind only to `127.0.0.1`. The profile records the actual
ports, PID, and control socket. Port selection and SSH binding cannot be atomic;
if another process claims a selected port, SSH fails startup and no profile is
published. Retry connect after resolving the collision.

FoundationDB needs special care: the local coordinator file preserves the
remote cluster identity and rewrites its address to the local tunnel. New
remotes and joining nodes forward to the first node's loopback address. Its transport verifies that the connected
port matches the server's advertised port. A remapped coordinator port fails
that check even when the destination is localhost. The local `127.0.0.1:4500` endpoint must reach the same remote database.
Free that port before connecting, or use a separate network namespace. Connect
still records and opens the alternative forward for inspection, but warns;
doctor reports the mapping failure, and configuration loading rejects it before
starting the native client. NATS and S3 support alternative ports normally.
See the port assertion in [FoundationDB's transport source](https://github.com/apple/foundationdb/blob/7.3.63/fdbrpc/FlowTransport.actor.cpp).

Doctor verifies a real FoundationDB session read transaction and NATS
publish/subscribe round trip through the selected profile. It fails if server
advertising sends database traffic outside the tunnel, even when the control
master is healthy. Database and NATS probes have eight- and five-second limits;
S3 remains a TCP check. Joining nodes use a systemd SSH tunnel for all three
services; see [remote provisioning](REMOTE.md).

Connect prints total elapsed time, address probing time, and tunnel startup
time. JSON adds these seconds under `timing`, alongside `reused`; an existing
healthy tunnel reports zero for the skipped probe and startup phases.

Status reports saved instance IDs and SSH reachability. An unreachable host has
unknown instance state; this command does not query a cloud API. For each
connected stack it scans all registered swarmyd nodes, including stale records,
and shows heartbeat age (live means at most 30 seconds old). Registrations belong
to the stack; they are not attributed to an instance because node records contain
no instance address. Store failures and disconnected tunnels report unknown
registration rather than claiming the node is absent. `--json` emits the same
information for scripts.

### Maintainer tunnel acceptance tests

These localhost tests are separate from the no-root EC2 workflow above.
To run the SSH acceptance test on the launcher, authorize a temporary SSH key for
`ubuntu@127.0.0.1`, then run:

```sh
cargo build --workspace --locked
scripts/dev-stack.sh start
SWARMY_REMOTE_TEST_KEY=/absolute/path/to/key scripts/test-remote.sh
```

The default run checks collisions against localhost, forwarded NATS traffic,
profile recording, the FoundationDB mapping diagnostic, and disconnect. Full
service acceptance needs a separate client network namespace, with the host's
sshd reachable over a veth pair and port 4500 free in the client. For example, use an unused subnet and namespace name:

```sh
sudo ip netns add swarmy-remote-test
sudo ip link add swarmy-host type veth peer name swarmy-client
sudo ip link set swarmy-client netns swarmy-remote-test
sudo ip addr add 10.253.117.1/30 dev swarmy-host
sudo ip link set swarmy-host up
sudo ip netns exec swarmy-remote-test ip addr add 10.253.117.2/30 dev swarmy-client
sudo ip netns exec swarmy-remote-test ip link set swarmy-client up
sudo ip netns exec swarmy-remote-test ip link set lo up
sudo ip netns exec swarmy-remote-test sudo -u ubuntu env \
  SWARMY_REMOTE_TEST_KEY=/absolute/path/to/key \
  SWARMY_REMOTE_TEST_HOST=10.253.117.1 scripts/test-remote.sh
sudo ip netns delete swarmy-remote-test
```

That run uses an isolated project, starts the three local services, runs the
fake provider end to end, checks doctor and a live swarmyd registration, and
verifies that backing process IDs remain unchanged. The script removes its
services and tunnel on exit. Without `SWARMY_REMOTE_TEST_KEY` it skips cleanly.
Remove the temporary authorized key and network namespace after testing.
