# Development

## Task fleet

The Python driver at `scripts/fleet/fleet` runs tasky work on a pool of
long-lived worker agents in a connected Swarmy remote. It uses the `swarmy`
CLI and needs `tasky` and `gh` on `PATH`. Copy
`scripts/fleet/fleet.example.toml` to `scripts/fleet/fleet.toml`, set the
remote, repository, provider defaults, pool size, and GitHub token, then run
`chmod 600 scripts/fleet/fleet.toml`. The driver refuses a config readable by
other users. The token is piped to `swarmy agent create --github-token-stdin`;
it never appears in command arguments or the session log. The config file is
ignored by Git. Swarmy stores the token privately for each worker.

```sh
scripts/fleet/fleet launch EWR2HD --provider openrouter --model openai/gpt-6-sol
scripts/fleet/fleet status
scripts/fleet/fleet collect EWR2HD
scripts/fleet/fleet resume EWR2HD "Address the review comments"
scripts/fleet/fleet release EWR2HD
scripts/fleet/fleet kill EWR2HD --timeout-seconds 60
scripts/fleet/fleet reset worker-2
```

`launch` picks an idle worker, or creates `worker-N` from the remote's
default image (or the configured `image`) while the pool is below `workers`,
sends the task on the worker's main conversation, and marks the task in
progress in tasky. The prompt carries `AGENTS.md`, the task text, and the
worker rules from `AGENTS.md`: a fresh clone under `~/work/<suffix>`, one
branch `swarmy/<suffix>` from master, a `cargo clean` when the shared target
directory passes 40 GiB, and the dev stack stopped at the end. State and JSON
run output are stored under `.dev/fleet` with owner-only permissions.
`status` prints one line per worker with its task, provider, model, state
(including a breaker's waiting reason), elapsed time, and cost. `collect`
verifies that the URL in the worker's last message is an open PR against
master from that branch before recording it in tasky and moving the task to
testing. `release` frees the worker once that PR is merged (`--force` skips
the check); the worker keeps its disk and warm cache. `reset` deletes an idle
worker and its disk; the next launch recreates it. Codex Daytona remains an
available task launcher during the transition.
`kill` interrupts the worker's main session, waits for Idle, then frees the
worker as `release --force` would. The tasky task stays in progress for an
operator to relaunch or cancel. The wait defaults to `kill_timeout_seconds`
in `fleet.toml`, or 60 seconds if unset; `--timeout-seconds` overrides it.

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

On one fleet worker, a clean `cargo test --workspace --no-run --locked` build
used 28,860,925,523 bytes of target space and took 404 seconds before the
debug-profile change; with line-table debug info and incremental compilation
disabled it used 9,404,695,572 bytes and took 417 seconds. Both runs used the
same machine and a clean target directory. For a local debugging session with
full debug info, run `CARGO_PROFILE_DEV_DEBUG=2 cargo build --workspace`.

`up` starts FoundationDB, NATS, and SeaweedFS when needed, then starts one
scheduler, worker, and gateway under a background CLI supervisor. It returns
when their logs report readiness and prints process ids. The default fake
provider replies `Hello from swarmy!` without credentials. Builds are separate
from startup; build again after changing Rust code. The CLI and the services
(scheduler, worker, gateway, API) must live
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

## Ephemeral sessions and named agents

A new `swarmy run` or `swarmy chat` conversation is ephemeral by default and
gets its own computer. Esc or Ctrl-C closes the chat client; the session can
still be resumed with `swarmy chat SESSION_ID`. Use `swarmy session close ID` to
complete an ephemeral session and delete its computer while retaining its log.
`swarmy session ls` (also spelled `session list`) displays kind, agent name,
state, and whether the computer was deleted.

For conversations that should share files and running processes, create a named
agent after registering an image:

```sh
swarmy agent create tommy --image base-ubuntu:dev --description "Compiler work"
swarmy agent ls
swarmy chat --agent tommy
# In another terminal, open a separate session on the same computer:
swarmy chat --agent tommy --new
swarmy run --agent tommy "Inspect the background processes"
swarmy agent show tommy
swarmy agent delete tommy --yes
```

Agent creation uses `default_image` if `--image` is omitted. Names contain 1-64
ASCII letters, digits, hyphens, or underscores. `--agent` accepts a name or
agent id, resumes its main session (creating it on first use), and rejects
`--image`; it uses the agent's pinned image without requiring a configured
default. It also cannot accompany a chat session id. Both chats use the same
computer and placement epoch. Closing either client keeps that computer. Add
`--new` to `chat --agent` or `run --agent` for a side conversation without
changing the main pointer. `--new` requires `--agent`. `session close` refuses
the main session and points to `agent delete`; closing a side session preserves
the shared computer. Closed sessions cannot become main. Delete asks for
confirmation unless `--yes` is supplied, removes the named identity and
computer, and retains session transcripts.

The chat status bar shows the agent name or `ephemeral`, session id, state, and
provider. Named conversations label system notices with their session id so
notices remain attributable when several chats share a computer. `--agent` skips
the recent-session picker. `chat --json` reads one prompt per stdin line and
emits the run event protocol, waiting for idle between prompts; EOF closes the
client. The other agent and session commands also support `--json`, and all of
these commands accept `--remote NAME` before or after the subcommand.

Each named agent can override the stack's system prompt, model, and reasoning
effort. For example:

```sh
swarmy agent create reviewer --image base-ubuntu:dev --system-prompt-file reviewer.txt --model gpt-5 --effort high
swarmy agent set reviewer --model gpt-5-mini --effort medium
swarmy agent show reviewer --json
```

Use `--system-prompt TEXT` for an inline prompt or `--system-prompt-file PATH`
for a UTF-8 file. The two flags are mutually exclusive and preserve whitespace.
`agent set NAME` changes only supplied fields. Changes apply to the next inference
in every existing or future session of that agent; an already submitted request
keeps its settings. Unset fields use the current stack defaults, as do ephemeral
sessions. `agent show` labels unset fields as `(stack default)` in text and emits
`null` in JSON. Effort accepts `none`, `minimal`, `low`, `medium`, `high`, or `xhigh`;
`none` is an explicit override, distinct from an unset field.

`agent show` reports `main_session` and marks the main session in its text
listing. `session ls` includes a `main` boolean in JSON and `main=true` or
`main=false` in text output. `agent show` also reports placement node and epoch,
every session's state, and last disk snapshot time and age from the committed
manifest's ULID timestamp. Before the computer has a volume, snapshot fields are
empty (null in JSON). The node currently has no sandbox-status query, so sandbox
state is explicitly `unknown` with `node status reporting unavailable`;
placement is not a liveness report. See the [CLI
README](../crates/swarmy-cli/README.md) for output fields and the root
acceptance test that verifies a background process across two named chats.

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

Every new ephemeral session needs a registered image, including sessions that only use
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

The script verifies FoundationDB release SHA-256 files and installs the backing
executables into `~/.local/bin` and `libfdb_c.so` into `~/.local/lib`
(`make dev-tools` runs it). Then install the CLI and the services with one
command:

```sh
make install
```

`make install` is `make install-client` plus `make install-core`. The client
links no database library and needs no `libfdb_c`; the service binaries
(scheduler, worker, gateway, API) still link it. Install only the client with
`make install-client` when the services live elsewhere.

The Makefile looks for the client library in `~/.local/lib`, `/usr/local/lib`,
`/usr/lib`, and `/usr/lib/x86_64-linux-gnu`, in that order, and passes the first
match as `SWARMY_FDB_LIB_DIR`. `make install-node` also installs `swarmyd`,
`make check` runs the three CI commands, and `make uninstall` removes the
binaries. Use `scripts/install-dev-tools.sh --prefix /absolute/path` for another
location, then `make install SWARMY_FDB_LIB_DIR=/absolute/path/lib`. The build embeds that library directory in the
runtime search path and uses it at link time. The shared build script also adds
existing `/usr/lib`, `/usr/local/lib`, and `/usr/lib/x86_64-linux-gnu` directories.
It emits the same rpath option on macOS, where the library is `libfdb_c.dylib`;
the installer and process supervisor currently target Linux.

Local Linux nodes also need `runc`, `passt` (which supplies `pasta`),
`iproute2`, and `util-linux` (which supplies `nsenter`). Install them with
`sudo apt-get install runc passt iproute2 util-linux` before
starting `swarmyd`.

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

The dev stack generates `.dev/nats.conf` with an 8 MB `max_payload` as a
guard for large work messages. Inference requests are stored by reference, so
their conversation history does not consume that bus limit.

For machines with these dependencies installed system-wide, including CI's
Ubuntu runner, the same `make install` needs no variable because the library
is found in a system directory.

Cargo puts all executables in `~/.cargo/bin` by default. Reinstall all of them
together after pulling: `swarmy dev up` refuses to start a scheduler, worker, or
gateway whose version differs from the CLI. Use `swarmy` after
rustup's normal shell setup, or `~/.cargo/bin/swarmy` directly. Keep the checkout
because `swarmy dev` uses `scripts/dev-stack.sh` from it.

### Diagnose an installation

```sh
swarmy dev up
swarmy doctor
swarmy doctor --json
```

Doctor checks the effective configuration, each backing executable and its
version, the installed service binaries, live service health from the
control-plane API, and ChatGPT credential validity when that provider is
selected. The client links no database library, so there is no client-library
check; database access problems surface through the API checks.
It reads credentials without refreshing them or printing their contents.
Once `.dev` exists it probes the configured FoundationDB coordinator, NATS, and
S3 ports with timeouts. Port connectivity does not verify database or S3
permissions. A stopped stack reports a fix pointing to `swarmy dev up`.
Before initialization, doctor asks for the missing config and tells you that the
stack has not been initialized. Each failure includes a fix and causes exit 1.
JSON output is one object containing `ok` and a `checks` array; each check has
`name`, `ok`, `detail`, and an optional `fix`.

The public CLI never opens the database. Conversation commands (`run`,
`chat`, `bench`), management reads, image builds, collection runs, and
`session close` and `session interrupt` all talk to the control-plane API; the
client machine needs no database, bus, or object store credentials. The
developer volume tools live in the node daemon instead: run them as
`swarmyd vol ...` on a machine with the store and devices (see
[volume tools](#volume-tools)). Only `auth login`, `auth import`, and `models
probe` act locally, and they resolve credentials from the login file and the
environment, never from the cluster store.

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

Services bind to `127.0.0.1`. FoundationDB prefers port 4500 and selects the
next free port when it is occupied. Set `SWARMY_DEV_FDB_PORT` to request a
specific port. The chosen port is recorded in `.dev/fdb.cluster` and `.dev/env`;
`status` reports it. A later start reuses that port when it is free. Reserve
4222 and 8222 for NATS; and 8080, 8333, 8888, 9333, 18080, 18333, 18888,
and 19333 for SeaweedFS HTTP and gRPC APIs. Stop any system-installed service
using those ports first. S3 uses the fixed local development credentials `swarmy-dev` and
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
| `SWARMY_DEV_FDB_PORT` | Chosen local FoundationDB port; also selects the port on a later `start` |
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

### Volume tools

Volume attach and snapshot need local block devices and the store, so they
live in the node daemon rather than the client. Run them with `swarmyd vol`
on a machine with root and the NBD module (a node, or a local machine with
the dev stack sourced):

```bash
source .dev/env
sudo -E swarmyd vol create base-ubuntu:dev
sudo -E swarmyd vol ls
sudo -E swarmyd vol attach VOLUME_ID --background
sudo -E swarmyd vol flush VOLUME_ID
sudo -E swarmyd vol snapshot VOLUME_ID
sudo -E swarmyd vol detach VOLUME_ID
```

`swarmyd vol --help` lists every subcommand. Creation, cloning, listing, and
history need only the store; attach, flush, checkpoint, snapshot of an
attached writer, and detach additionally drive the local volume server.

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
FoundationDB advertises loopback port 4500, and all three backing services
are reached through SSH. The [SSH-only proof](volume-benchmarks.md#2026-09-17-ssh-only-remote-stack)
blocks direct private-network service traffic from the client user.
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

The client itself needs no FoundationDB client library in any mode; every
database command runs through the control-plane API. Building the workspace
from source still needs the library for the services, and the installer also
supplies backing executables for local development. It needs
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
swarmy remote up demo           # builds binaries and base-ubuntu:demo; prints build times and SSH command
swarmy remote connect demo
swarmy auth login              # dedicated ChatGPT login, on the laptop
swarmy dev up --remote demo
swarmy doctor --remote demo
swarmy remote status
```

FoundationDB must bind local `127.0.0.1:4500` exactly. A local stack using that
port prevents a usable remote connection. Stop a conflicting local stack, disconnect, and reconnect.
NATS and S3 alone can use automatically selected alternative local ports.

`up` finishes by building `images/base-ubuntu` as root on the node using
`/etc/swarmy/node.env`. It streams the build progress, reports its duration,
and registers `base-ubuntu:demo`. `connect` copies that image into the remote
profile's `default_image`, so new sessions need no image flag or local image
configuration. Image construction needs node root; it never needs laptop root.
`swarmy remote status` lists registered images while the remote is connected.

Start a chat on the laptop and ask the agent to run `pwd` in its sandbox:

```bash
swarmy chat --remote demo
# Alternatively, start a session and resume its printed id:
swarmy run --remote demo 'Run pwd in the sandbox, then wait for my next instruction.'
swarmy chat --remote demo SESSION_ID
```

Use `swarmy remote up demo --image-recipe images/custom` to build another recipe
directory within the checkout. Relative paths are resolved from the checkout
root; absolute paths must also be inside that checkout. The registered name
remains `base-ubuntu:demo`. `--no-image` skips the build and leaves the remote
without a saved default; provide an already registered image through `--image`
or `default_image` before starting a new session. The two options cannot be
combined. An explicit session `--image NAME:TAG` overrides the profile default.

In the session, ask the agent to use `process_start` to run
`python3 -u -m http.server 18765 --bind 127.0.0.1`. In the next turn ask it to
fetch `http://127.0.0.1:18765/` with `bash` and verify the server is still listed
by `process_list`. Then ask it to write a marker file and call `checkpoint`.
An acknowledged checkpoint makes the disk durable; it does not save running
processes. Escape or Ctrl-C closes chat while the session remains stored.

In the default laptop-services mode, the ChatGPT credential stays in the
laptop's configured file. The local gateway
reads it and sends inference directly to the provider. Remote provisioning
copies the checkout while excluding `.swarmy/`, `.dev/`, `.git/`, `target/`,
`.env`, and `.env.*`; it does not copy the laptop's home or credential cache.
The configured credential path is also excluded from the checkout copy.
Do not share its refresh writer with a running Codex login. Node services have
the explicit credential-transfer option described below. Prompts, outputs, and session
history do live in the remote backing services. The fake-provider acceptance
run requires no ChatGPT credential; see the dated benchmark evidence.

### Ephemeral sessions and named agents

Without `--agent`, each new conversation gets its own computer and writable disk:

```bash
swarmy chat --remote demo
# In another terminal, create a separate ephemeral conversation:
swarmy chat --remote demo
swarmy session ls --remote demo
swarmy chat --remote demo SESSION_ID   # resume an existing conversation
swarmy session close SESSION_ID --remote demo
swarmy session show SESSION_ID --remote demo --json
```

Escape, Ctrl-C, or EOF in `chat --json` closes the client only. The session
and computer remain available for resume. `session close` completes an ephemeral
session and deletes its computer while keeping its transcript. The scheduler
also closes Idle ephemeral sessions after 24 hours by default. For a disposable
retention test, set the interval on the scheduler before starting services:

```bash
swarmy dev down
SWARMY_EPHEMERAL_RETENTION_SECONDS=90 swarmy dev up --remote demo
```

This override applies to local services in the default laptop mode. Node services
need the override in their systemd environment and a scheduler restart. The sweep
runs every 60 seconds or every retention interval if shorter. Only idle sessions
strictly older than the cutoff qualify; active sessions and named agents do not.

Create a named agent to keep a computer independently of any one conversation:

```bash
swarmy agent create tommy --remote demo --description 'Shared development computer'
swarmy chat --agent tommy --remote demo
# In a second terminal, open another session on the same computer:
swarmy chat --agent tommy --new --remote demo
swarmy agent ls --remote demo
swarmy agent show tommy --remote demo --json
swarmy run --agent tommy --remote demo 'Read the files created in the other chat'
```

Ask the first chat to write a file and use `process_start` to start
`python3 -u -m http.server 18765 --bind 127.0.0.1`. Ask the second to read that
file, fetch the server with `curl`, and list managed processes. Both sessions
share those files and processes. Simultaneous tool calls queue for the shared
computer, with one call running at a time. Each conversation has its own log.
`agent show` reports the sampled holder session, queued call count, node, epoch,
and observation expiry, plus the last committed disk snapshot time. Missing
or stale samples mean unknown activity. Busy includes computer startup.

Creation pins `default_image` or an explicit `--image NAME:TAG`. New sessions
on the named agent use that pin, so `--agent` cannot be combined with `--image`
or a resume session id. Names and agent ids both work. `chat --agent` and
`run --agent` resume the main session by default, creating it if absent. `--new`
opens a side conversation without changing that pointer. Quitting either client
leaves the agent available, and ephemeral retention never deletes it.

Ask for `checkpoint` before the daemon-kill procedure below. After recovery,
both transcripts receive the same rebuild notice, including idle chats. Read
the checkpointed file from both sessions and restart the server: files survive
at the snapshot boundary, while background processes and memory are lost.
Then delete the shared computer:

```bash
swarmy agent delete tommy --remote demo            # interactive confirmation
# For scripts, use --yes on the same command.
swarmy session show FIRST_SESSION_ID --remote demo --json
swarmy session show SECOND_SESSION_ID --remote demo --json
```

`session close` refuses the main session and points to `agent delete` instead.
Side sessions can be closed while retaining the shared computer.
Deletion removes the identity, placement, and disk references immediately;
physical processes and attachments disappear on the node's next failed renewal.
Transcripts remain readable and further sandbox tools are refused. Recreating
`tommy` makes a new identity and computer.

Chunks are reclaimed separately. On an isolated disposable stack with no
pending writes, wait beyond a short grace window, inspect candidates, collect,
and confirm a subsequent pass has nothing more to delete:

`swarmy gc` starts the run on the control plane and follows its progress,
so the collection policy comes from the API host's configuration, not the
client's environment. For this disposable-stack procedure, set the short grace
in the API service's environment and restart it first:

```bash
swarmy gc --remote demo --dry-run --json
swarmy gc --remote demo --json
swarmy gc --remote demo --dry-run --json
```

Use the normal six-hour grace for ongoing work. Images, other volumes, and
retained snapshots protect shared chunks, so freed bytes need not equal the
logical disk size. These commands report object bytes, not SeaweedFS filesystem
space after compaction. See the dated lifecycle run in
[volume benchmarks](volume-benchmarks.md) for transcript and reclamation evidence.

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

Remote `dev up` starts scheduler, gateway, and worker locally only for a
remote created with laptop services. A remote created with node services starts
none locally, including when local service binaries are not installed. It records that
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

### Running control-plane services on the node

Laptop services are the default. They keep ChatGPT credentials local and make
worker/gateway development convenient, but every store transaction crosses the
tunnel. For lower turn latency, run the control plane under systemd beside the
store:

```sh
# With provider = "fake", no credential is copied.
swarmy remote up demo --services node
swarmy remote connect demo
swarmy dev up --remote demo
swarmy chat --remote demo
```

Alternatively set `services = "node"` in `[remote]` before `remote up`.
`--services laptop` overrides that setting. The selected mode is saved in the
remote's launch settings; later edits to local config do not switch an existing
node. Node mode uses the configured provider, model, reasoning effort, system
prompt, store/bus namespace, and fake script. The fake script is sent over SSH;
if no script exists, the default greeting script is installed.

For a ChatGPT gateway, use `swarmy remote up demo --services node
--copy-credential`. This explicit flag acknowledges that the configured
`credential_file` leaves the laptop. A warning precedes transfer. SSH sends the
contents on stdin, and `/etc/swarmy/auth.json` is owned by ubuntu with mode 0600.
The ordinary checkout copy excludes the configured credential path as well as
`.swarmy`, `.dev`, and environment files. Without the flag a ChatGPT node launch
fails before creating resources. Do not run a laptop gateway that refreshes the
same account concurrently. Stopping the services does not erase the copied file;
`remote down` terminates the node and its root disk.

`dev up --remote demo` stops any recorded local control-plane processes, checks
the tunnel, and starts nothing locally in node mode. The services continue when
the laptop disconnects. `dev down` stops only local processes; use SSH and
`sudo systemctl stop swarmy-{scheduler,worker,gateway}` to pause node services.
Inspect their logs with `sudo journalctl -u swarmy-worker -u swarmy-gateway
-u swarmy-scheduler`. `remote logs` continues to follow the execution node log.

Both configurations use the same port-4500 tunnel requirement, tool placement,
rebuild notices, and teardown commands:

```sh
swarmy dev down
swarmy remote disconnect demo
swarmy remote down demo
```

See the dated measurements in [volume benchmarks](volume-benchmarks.md) for
latency results and the remaining round trips. Node services are a development
mode on one backing-store node, without replicated storage or high availability.

### Conversation summaries and memory

Named agents keep a chain of main sessions. At the end of a turn the worker
compares the latest provider input plus output token count with
`summarize_at_tokens` (`SWARMY_SUMMARIZE_AT_TOKENS`). When omitted, the threshold
is three quarters of `model_context_window_tokens`
(`SWARMY_MODEL_CONTEXT_WINDOW_TOKENS`, default 400000 for the default model).
Set the window when selecting a model with a different context capacity.
Side sessions use the same mechanism with an input-token threshold: an
explicit `SWARMY_SUMMARIZE_AT_TOKENS`, else three quarters of a
`SWARMY_MODEL_CONTEXT_WINDOW_TOKENS` override, else the catalog's per-model
or per-provider `summarize_at`, else 400000 input tokens. Ephemeral sessions are not summarized automatically.
At 75 percent of the side threshold the worker appends a `context_pressure`
system warning once per session; at the threshold it summarizes.

The summary is a normal durable inference job with no tools. Its JSON contains
goals, state of work, open questions, and facts worth keeping. A successful
summary creates an idle main session with that opening context and a reference
to the previous session, or an idle side session with that opening plus the
last few turns. Creation, archival, links, and pointer replacement
commit in one fenced transaction. Provider failure or invalid summary JSON
keeps the existing session. `session list` and `agent show` identify archived
sessions; `session show ID` still reads their full logs. Open chats display a
notice and follow the chain, including after a missed live notification.
`run --session OLD` follows to the successor and prints the same notice.

The worker includes memory files in every named-agent inference, including
turns in side sessions. `memory_dir` (`SWARMY_MEMORY_DIR`) defaults to
`/home/agent/memory`; `memory_max_bytes` (`SWARMY_MEMORY_MAX_BYTES`) defaults to
32768. The node reads regular files directly in that directory in filename
order, includes their names, and adds a note when the byte budget truncates
content. Subdirectories, symbolic links, and special files are skipped. Use an
absolute directory path without symbolic-link components. A computer that has
not been placed contributes empty memory.

A memory read uses one sandbox exec. The node caches the result by placement
epoch, volume head manifest, directory, byte budget, and file metadata. The
metadata check avoids an exec for unchanged files while detecting ordinary
tool and background writes before a checkpoint advances the manifest. Memory
has the same durability as other home-disk files: use `checkpoint` when facts
must survive node failure. The default prompt explains how to save memory and
that conversations may be summarized; `{memory_dir}` in a configured system
prompt expands to the selected directory.
