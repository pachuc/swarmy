# Development

## Task fleet

The Python driver at `scripts/fleet/fleet` runs tasky work on a pool of
long-lived worker agents in a connected Swarmy remote. It uses the `swarmy`
CLI and needs `tasky` and `git` on `PATH`. Copy
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
scripts/fleet/fleet release EWR2HD --into one-binary
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
checks the pushed branch, links its head SHA in tasky, moves the task to
testing, and prints the worker's report. `release` frees the worker once a
commit on the integration branch (`--into` or `integration` in fleet.toml)
names `tasky <id>`; `--force` skips the check. The worker keeps its disk and
warm cache. `reset` deletes an idle
worker and its disk; the next launch recreates it. Codex Daytona remains an
available task launcher during the transition.
`kill` interrupts the worker's main session, waits for Idle, then frees the
worker as `release --force` would. The tasky task stays in progress for an
operator to relaunch or cancel. The wait defaults to `kill_timeout_seconds`
in `fleet.toml`, or 60 seconds if unset; `--timeout-seconds` overrides it.

## Quick start

### Choosing the target swarm

Client commands choose their API in order: `--swarm NAME`, `SWARMY_SWARM`,
the `SWARMY_API_URL` and `SWARMY_API_TOKEN` pair, then the current swarm.
Without any target, the client explains how to list, use, or create a swarm.
Configuration files never choose the client's API: `[api] url` and `token`
configure the services started by `swarmy dev up`. That command registers its
API as swarm `dev` and makes it current when none is selected.

Specs live under `$XDG_CONFIG_HOME/swarmy/swarms`, records under
`$XDG_DATA_HOME/swarmy/swarms`, and the `current` file under
`$XDG_STATE_HOME/swarmy` (with standard HOME-based XDG defaults).

```sh
swarmy swarm ls
swarmy swarm show
swarmy swarm use dev
printf '%s' "$TOKEN" | swarmy swarm attach staging --url https://swarmy.example.test --token-stdin
swarmy --swarm staging session ls
swarmy swarm rm staging
```

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

**Where dev keeps its files.** Inside a checkout, `swarmy dev up` creates
`<checkout>/.swarmy/config.toml`, keeps supervised service state under
`<checkout>/.swarmy/dev/`, and uses `<checkout>/.dev/` for the backing stack.
A directory with its own `.swarmy/config.toml` works the same way, with the
stack in the project state directory's `stack/`. Outside both, swarmy follows the XDG Base Directory
layout: `~/.config/swarmy/config.toml`, the stack in
`~/.local/share/swarmy/stack/`, and service logs, pids, the fake script and
its call log in `~/.local/state/swarmy/dev/`. Absolute `XDG_CONFIG_HOME`,
`XDG_DATA_HOME` and `XDG_STATE_HOME` move them. The keyring and default
`auth.json` live in `~/.local/share/swarmy/` in every layout. Downloaded cores
and images go to `~/.cache/swarmy/` (`XDG_CACHE_HOME`) in every layout.
An existing configuration file wins; `SWARMY_STATE_DIR` changes the service
state root, and `SWARMY_DEV_DIR` overrides the stack directory. The CLI runs a
compiled-in copy of the backing script, so it needs no checkout. The fixed
NATS and SeaweedFS ports allow only one stack per host.

On one fleet worker, a clean `cargo test --workspace --no-run --locked` build
used 28,860,925,523 bytes of target space and took 404 seconds before the
debug-profile change; with line-table debug info and incremental compilation
disabled it used 9,404,695,572 bytes and took 417 seconds. Both runs used the
same machine and a clean target directory. For a local debugging session with
full debug info, run `CARGO_PROFILE_DEV_DEBUG=2 cargo build --workspace`.

`up` starts FoundationDB, NATS, and SeaweedFS when needed, then starts one
scheduler, worker, gateway, and API under a background CLI supervisor. It returns
when their logs report readiness and prints process ids. The default fake
provider replies `Hello from swarmy!` without credentials. Builds are separate
from startup; build again after changing Rust code. The CLI and the services
(scheduler, worker, gateway, API) must live
in the same directory. You can add `target/debug` to your shell's executable
search path to use `swarmy` directly. When using a system client library, omit `SWARMY_FDB_LIB_DIR`. With a custom
install prefix, set it to that prefix's `lib` directory at build time.

```sh
./target/debug/swarmy dev logs           # follow all four service logs
./target/debug/swarmy dev logs worker    # follow one service; Ctrl-C ends tailing
./target/debug/swarmy dev logs supervisor
```

`status` shows the backing stack, services, and supervisor with pids and uptime.
`down` stops them all and preserves data. Repeating `up` reports and replaces
recorded processes, including survivors of a supervisor killed with SIGKILL.
It reloads configuration, so edits take effect on the next `up`. An unexpected
service exit stops the other services; inspect the logs and run `up` again.
PID records contain Linux process start times to guard against PID reuse.

To run the backing services and swarmyd on another machine, see [remote node workflow](#remote-node-workflow).

## Local swarms

A local swarm runs FoundationDB, NATS, SeaweedFS, and the scheduler, worker,
gateway, and API on this machine. Create it once, then use the same name to
manage it. It does not run a sandbox node, so tool-calling turns still need a
node. Ephemeral swarms delete their data on `down`; persistent swarms require
`down --destroy` to delete data. `stop` and `start` preserve data.

```sh
swarmy swarm create dev --local
swarmy swarm up dev
swarmy swarm status dev
swarmy swarm logs dev worker
swarmy swarm stop dev
swarmy swarm start dev
swarmy swarm down dev
```

The files follow XDG directories (`XDG_CONFIG_HOME`, `XDG_DATA_HOME`, and
`XDG_STATE_HOME`, with HOME-based defaults):

| Path under swarmy's XDG root | Contents |
|---|---|
| config/swarms/NAME.toml | Private swarm spec |
| data/swarms/NAME/record.toml | Private API URL and token record |
| data/swarms/NAME/services.toml | Effective service settings, regenerated on each up/start; do not edit |
| data/swarms/NAME/stack/ | Default backing stack data, owned by the swarm |
| state/swarms/NAME/ | Supervisor and service pids, ready marker, logs, fake files, API uploads |
| state/local.lock | Machine-wide local lifecycle lock |
| data/keyring | Shared local cluster keyring |

Inside a checkout, creation finds the first ancestor with
`scripts/dev-stack.sh` and sets `stack_dir` to its `.dev`. This shares the
stack with `scripts/dev-stack.sh` and `make check`. `--stack-dir PATH` overrides
it; relative paths resolve against the current directory. `down` stops but
never deletes an explicit or detected `stack_dir`. Without either, the swarm
owns its stack under its data directory.

A release client downloads its matching `swarmy-core`. A development client
uses a sibling `target/*/swarmy-core` or one on PATH. Use `--core-bin PATH`
to choose explicitly. Service tuning is read on every up/start from the first
project `.swarmy/config.toml` found from the current directory, otherwise
`~/.config/swarmy/config.toml`. Only the default image is saved to this file;
`services.toml` is rebuilt each time. Shell `SWARMY_*` variables do not reach
the local services. Fixed ports allow only one local swarm to run at a time.
`swarmy swarm rm` refuses to forget a running local swarm; stop it first.

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
these commands accept `--swarm NAME` before or after the subcommand.

Each named agent can override the stack's system prompt, model, and reasoning
effort. For example:

```sh
swarmy agent create reviewer --image base-ubuntu:dev --system-prompt-file reviewer.txt --model gpt-5 --effort high
swarmy agent set reviewer --model gpt-5-mini --effort medium
swarmy agent show reviewer --json
```

Use `--system-prompt TEXT` for an inline prompt or `--system-prompt-file PATH`
for a UTF-8 file. The two flags are mutually exclusive and preserve whitespace.
`agent create` and `agent set` also take `--memory MIB` for the sandbox memory
limit and `--gpu none|shared|dedicated` for the placement GPU requirement.
`agent set NAME` changes only supplied fields. Changes apply to the next inference
in every existing or future session of that agent; an already submitted request
keeps its settings. Unset fields use the current stack defaults, as do ephemeral
sessions. `agent show` labels unset fields as `(stack default)` in text and emits
`null` in JSON. Effort accepts `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max`;
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

`dev up` creates a project file in a checkout or a user file outside one,
imports connection settings from the stack's `env` file as data, and saves the
file with private permissions. It updates stack connection fields on each start
and preserves other settings. Environment overrides are passed to children but
are not saved. Service logs and pid records live under `<state_dir>/dev/`,
and so do the default fake script and call log. `state_dir` defaults to
`<project>/.swarmy` for a project file and `~/.local/state/swarmy` otherwise.

The generated file contains every setting. This partial example shows the
common choices (store directories are FoundationDB directory names):

```toml
[store]
directory = "swarmy"
cluster_file = ".dev/fdb.cluster"

[bus]
prefix = ""

[selection]
provider = "fake"
model = "gpt-5"
default_image = "base-ubuntu:dev"
effort = "medium"
credential_file = "/home/me/.local/share/swarmy/auth.json"

[worker]
partitions = "0-255"

[scheduler]
partitions = "0-255"

[fake]
script = ".swarmy/dev/fake.json"
call_log = ".swarmy/dev/calls.log"
```

Every new ephemeral session needs a registered image, including sessions that only use
remote tools. Set `[selection] default_image = "NAME:TAG"` or
`SWARMY_DEFAULT_IMAGE`, or pass `--image NAME:TAG` to `swarmy run` or
`swarmy chat`. The flag overrides the setting, which has no built-in default. Unknown images fail before a session is
created and the error lists registered images; `swarmy image ls` also lists them.
Image construction requires root. The computer is materialized on first sandbox
tool use, so a fake-provider conversation without sandbox tools needs no node.
Resuming an existing session keeps its pinned image.

The connection keys are `[store] cluster_file`, `[bus] nats_url`, `[s3] endpoint`,
`[s3] access_key`, `[s3] secret_key`, `[s3] bucket`, `[s3] prefix`,
`[s3] region`, and `[s3] conditional_create`. `[s3] prefix` defaults to empty. Use it to select a namespace
within the bucket; see the [namespace and migration
rules](../crates/swarmy-store/src/objects.rs). Set `[s3] conditional_create`
to `false` (or `SWARMY_S3_CONDITIONAL_CREATE=false`) for object stores that
reject create-only PUTs; chunks and manifests are content-addressed, so the
plain-PUT fallback is safe. Additional settings are
`[scheduler] scan_interval_ms`, `[scheduler] resend_interval_ms`,
`[scheduler] ephemeral_retention_secs`, `[scheduler] placement_lease_secs`,
`[worker] lease_ms`, `[worker] recovery_interval_ms`, `[bus] ack_wait_ms`,
`[bus] max_deliver`, `[gateway] concurrency`, `[node] heartbeat_interval_ms`,
`[sandbox] idle_secs`, `[gc] grace_secs`, `[gc] interval_secs`,
`[inference] max_wait_secs`, `[inference] max_backoff_secs`,
`[inference] gateway_wait_secs`, `[volume_snapshots] period_secs`, and
`[context] system_prompt`. The optional `[context] summarize_at` and
`[context] context_window` override the compaction threshold and the known
model window. The optional `[worker] kill_point` retains the
worker failure-injection setting. The documented `SWARMY_*` names still work;
TOML keys live in the tables above.
Exceptions are `[selection] credential_file` (`SWARMY_CHATGPT_AUTH`),
`[fake].script` (`SWARMY_FAKE_SCRIPT`), and `[fake].call_log`
(`SWARMY_FAKE_CALL_LOG`).

To use ChatGPT, set `[selection] provider = "chatgpt"`, choose the model and
effort, and set `[selection] credential_file` to a dedicated credential file.
`swarmy auth login` and the gateway read that same path. `swarmy auth
--auth-file PATH login` overrides it for a login. Never share a refresh
writer with a running Codex login.

## Backing service installation

The supported no-root installation path targets Linux x86-64. It needs Bash,
curl, tar, sha256sum, and install. Building Rust also needs a C/C++ toolchain,
pkg-config, and clang/libclang for bindgen. The stack uses curl 7.75 or newer for
AWS request signing. These prerequisites must already be available.

On Ubuntu, the build prerequisites are `build-essential pkg-config clang
libclang-dev ca-certificates curl tar git`. Clone the repository with
`git clone https://github.com/pachuc/swarmy.git`, enter the checkout, and run
`rustup show` to install the pinned toolchain. Cargo installs binaries into
`~/.cargo/bin`; use `~/.cargo/bin/swarmy` if that directory is not on PATH.
When pinned backing tools and the FoundationDB library are already installed
system-wide, `make install` finds them in `/usr/local/lib` or `/usr/lib`.

Without a checkout, `swarmy dev install-tools` runs the same compiled-in script.
It accepts `--prefix DIRECTORY` to select another installation root.

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
links no database library and needs no `libfdb_c`; `swarmy-core` (scheduler, worker, gateway, API, node, and vol) still links it. Install only the client with
`make install-client` when the services live elsewhere. `make install-client`
builds the client with `remote` and `chat`, plus the `swarmy-auth` helper for interactive provider login. `make install-node` builds the headless client with neither feature, and installs `swarmy-core`. The `remote` feature compiles the S3 client behind the `swarmy remote`
provisioning commands. A plain
`cargo build -p swarmy-cli` leaves that feature off for the slimmer node
binary; add `--features remote` to a plain cargo invocation when the
provisioning commands are needed.

The Makefile looks for the client library in `~/.local/lib`, `/usr/local/lib`,
`/usr/lib`, and `/usr/lib/x86_64-linux-gnu`, in that order, and passes the first
match as `SWARMY_FDB_LIB_DIR`. `make install-node` also installs `swarmy-core`,
`make check` runs every pull-request CI job, and `make uninstall` removes the
binaries. Use `scripts/install-dev-tools.sh --prefix /absolute/path` for another
location, then `make install SWARMY_FDB_LIB_DIR=/absolute/path/lib`. The build embeds that library directory in the
runtime search path and uses it at link time. The shared build script also adds
existing `/usr/lib`, `/usr/local/lib`, and `/usr/lib/x86_64-linux-gnu` directories.
It emits the same rpath option on macOS, where the library is `libfdb_c.dylib`;
the installer and process supervisor currently target Linux.

Local Linux nodes also need `runc`, `passt` (which supplies `pasta`),
`iproute2`, and `util-linux` (which supplies `nsenter`). Install them with
`sudo apt-get install runc passt iproute2 util-linux` before
starting `swarmy-core node`.

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

### Service binary

`swarmy-core` runs `scheduler`, `worker`, `gateway`, `api`, `node`, and `vol` roles. Run `swarmy-core --version` to check its identity. The `node` role needs root and NBD.

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
The object-store check is a TCP connection to the configured S3 endpoint;
there is no FoundationDB or NATS port probe. Database and bus liveness for a
running stack come from the API snapshot: reaching the API and reading its
service heartbeats proves the store path. TCP connectivity does not verify
database or S3 permissions. A stopped stack reports a fix pointing to `swarmy dev up`.
Before initialization, doctor asks for the missing config and tells you that the
stack has not been initialized. Each failure includes a fix and causes exit 1.
JSON output is one object containing `ok`, a `checks` array, and a
`providers` array; each check has `name`, `ok`, `status`, `detail`, and an
optional `fix`.

The public CLI never opens the database. Conversation commands (`run`,
`chat`, `bench`), management reads, image builds, collection runs, and
`session close` and `session interrupt` all talk to the control-plane API; the
client machine needs no database, bus, or object store credentials. The
developer volume tools live in the node daemon instead: run them as
`swarmy-core vol ...` on a machine with the store and devices (see
[volume tools](#volume-tools)). `models probe` uses the API host's credential resolver, so a laptop without
provider keys can probe credentials stored in the swarm. Scripted `fake` probes read fixture files on the API host. The separately
installed `swarmy-auth` helper handles interactive `auth login` and `auth import`;
both save credentials through the API. Node installs omit it.

## Release builds

A `v*` tag runs `.github/workflows/release.yml` to build and publish release
archives. Pull requests touching the workflow or `scripts/release.sh`, and
manual workflow runs, build all targets without publishing. Native runners
smoke-test their binaries: Linux x86_64 uses Ubuntu 22.04, Linux aarch64 uses
Ubuntu 22.04 ARM, macOS x86_64 uses macOS 15 Intel, and macOS aarch64 uses
macOS 15.

Each client archive is named `swarmy-<target>.tar.gz` and contains
`swarmy-<target>/swarmy`. The Linux core archives are named
`swarmy-core-<target>.tar.gz` and contain `swarmy-core-<target>/swarmy-core`
and `swarmy-core-<target>/libfdb_c.so`. The core loads its bundled
FoundationDB 7.3.79 client through an `$ORIGIN` rpath; keep the two files
together. Packaging checks the upstream library against its published SHA-256
checksum. Nodes need no FoundationDB installation for this binary.

The image archive `swarmy-image-base-ubuntu-x86_64-unknown-linux-gnu.tar.gz`
contains `disk.ext4` (a sparse 8 GiB ext4 member packed with GNU tar
`--sparse`) and `recipe.toml`. It is built with
`make release-package RELEASE_COMPONENT=image RELEASE_TARGET=x86_64-unknown-linux-gnu`.

Each release includes one `SHA256SUMS` for all seven archives. Download it with
the archives and verify using `sha256sum -c --ignore-missing SHA256SUMS`.
Releases are not signed yet; signing may be added later.

`swarmy version --release` prints the matching tag. The tag must be `v` plus
the workspace version or the workflow fails. A version containing a hyphen
publishes as a prerelease. Bump `[workspace.package] version` and Cargo.lock
before cutting a tag.

`make release-dry-run` builds the Linux x86_64 client by default, packages it,
and generates a checksum file. Override `RELEASE_COMPONENT=core`,
`RELEASE_TARGET=...`, and `RELEASE_DIR=...` to package a different target.
Core builds need patchelf, clang, and libclang-dev.
`RELEASE_COMPONENT=image` builds the base-ubuntu ext4 on Ubuntu 24.04 with
passwordless sudo, debootstrap, and e2fsprogs. `CORE_BIN_PACKAGE` names
the cargo package that builds the `swarmy-core` binary.

### Install a release

`scripts/install.sh` installs the client from a GitHub release with optional
`--version vX.Y.Z` and `--prefix DIRECTORY` flags. The raw URL on master serves
the installer, so changes reach users when merged. `SWARMY_INSTALL_BASE_URL`,
`SWARMY_INSTALL_OS`, and `SWARMY_INSTALL_ARCH` are test hooks for overriding
the release URL and detected platform. `scripts/test-install.sh` exercises
fake releases in `make check-scripts`.

## The base image

`swarmy image pull base-ubuntu --set-default` verifies and uploads the release
image through the sparse upload path, then sets `selection.default_image`.
Restart with `swarmy dev up` to apply it. `dev up` does this automatically when
there is no default image; `--no-image` skips it. The download is temporarily
cached in `~/.cache/swarmy/images` and removed after upload. For local fake releases,
set `SWARMY_RELEASE_BASE_URL` and include the image archive in SHA256SUMS.

## The core binary

`swarmy core ensure [--core-bin PATH] [--target TRIPLE]` locates the matching
Linux core and prints its path. `swarmy dev up --core-bin PATH` or
`SWARMY_CORE_BIN` selects an explicit executable first. Release builds fetch
and verify the matching release next; development builds instead find
`swarmy-core` beside the CLI (or on PATH), so `cargo build && target/debug/swarmy
dev up` still works. Set `SWARMY_RELEASE_BASE_URL` to force release selection
in development; it accepts HTTPS, or HTTP on loopback only.

Archives live in swarmy's cache directory under `core/<tag>/`
(`$XDG_CACHE_HOME/swarmy`, default `~/.cache/swarmy`).
Every ensure checks the release-wide SHA256SUMS and replaces a changed archive;
when the server cannot be reached, a complete verified cache is reused with a
warning. SHA-256 checksums protect integrity, but releases are not signed.

To test locally with a fake release (requires patchelf and clang; the image
line also requires passwordless sudo, debootstrap, e2fsprogs, and several
minutes to build):

```sh
make release-package RELEASE_COMPONENT=core RELEASE_DIR=target/fake-release/v0.1.0
make release-package RELEASE_COMPONENT=client RELEASE_DIR=target/fake-release/v0.1.0
make release-package RELEASE_COMPONENT=image RELEASE_DIR=target/fake-release/v0.1.0
scripts/release.sh checksums target/fake-release/v0.1.0
python3 -m http.server --bind 127.0.0.1 --directory target/fake-release 8765
SWARMY_RELEASE_BASE_URL=http://127.0.0.1:8765 swarmy dev up
```

`swarmy doctor` checks `core version`. `run`, `chat`, and `bench` refuse an API
core that is incompatible: a different package version, or a different major
API version. Upgrade the core (`swarmy dev up` locally or
`swarmy remote upgrade NAME`) or pass the global `--allow-version-mismatch` flag
to proceed with a warning. A core built from another commit of the same version
is only reported (a note from `run` and `chat`, a warning in `doctor`): every
development build shares one version, and most commits change nothing the
client depends on. On-node builds of a development client record the copied
checkout's commit, so the node reports it rather than `unknown`.

## Manual reference

### Run only the backing stack

From the repository root:

```bash
scripts/dev-stack.sh start
source .dev/env
scripts/dev-stack.sh status
```

The script uses `SWARMY_DEV_DIR` when set (an absolute path); otherwise, when
run as a file, it uses `<checkout>/.dev`. Direct script callers retain the
checkout default.

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
| `SWARMY_NATS_MONITOR_URL` | `http://127.0.0.1:8222` |
| `SWARMY_S3_ENDPOINT` | `http://127.0.0.1:8333` |
| `SWARMY_S3_ACCESS_KEY` | `swarmy-dev` |
| `SWARMY_S3_SECRET_KEY` | `swarmy-dev-secret` |
| `SWARMY_S3_BUCKET` | `swarmy` |
| `SWARMY_S3_PREFIX` | empty |
| `SWARMY_S3_REGION` | `us-east-1` |

S3 clients should use path-style bucket addressing with this endpoint. Tests
requiring a backing system must skip cleanly when its environment variables are
absent. Build the workspace first. CI runs these commands in parallel jobs
([workflow](../.github/workflows/ci.yml)); run e2e suites serially:

```bash
source .dev/env
cargo fmt --all --check
cargo build --locked -p swarmy-cli --no-default-features
cargo build --workspace --locked
cargo test --workspace --locked --exclude swarmy-e2e
cargo test --locked -p swarmy-e2e --test cli_session -- --test-threads=1
cargo test --locked -p swarmy-e2e --test gateway --test scheduler --test worker -- --test-threads=1
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --locked -p swarmy-cloud --features remote
cargo test --locked -p swarmy-cli --features remote -- --skip dev_up_run_recover_reconfigure_and_down
cargo clippy --locked -p swarmy-cloud --features remote --all-targets -- -D warnings
cargo clippy --locked -p swarmy-cli --features remote --all-targets -- -D warnings
cargo test --locked -p swarmy-llm --no-default-features
scripts/chaos-ci.sh
scripts/check-openapi-compat.sh origin/master
```

The S3 namespace acceptance test also needs `SWARMY_S3_TEST_BUCKET` naming a
pre-created, dedicated empty bucket. The dev stack creates
`swarmy-s3-namespace-test` and exports the variable, so with the stack
running:

```bash
source .dev/env
cargo test -p swarmy-store --test s3_namespace --locked -- --nocapture
```

It refuses a non-empty bucket and cleans up its objects and metadata after
each case, including assertion failures. It tests empty and nested prefixes,
more than 1000 objects in one listing, sibling isolation, and dry and real
collection.

The same test runs against cloud S3 by setting the S3 endpoint, credentials,
region, and a dedicated empty test bucket, with a reachable FoundationDB.

The Ubuntu CI job installs these pinned versions, starts the stack, and copies
the exported settings into `GITHUB_ENV` so subsequent test steps inherit them.
Its cleanup step runs even if an earlier step fails.

### Volume tools

Volume attach and snapshot need local block devices and the store, so they
live in the node daemon rather than the client. Run them with `swarmy-core vol`
on a machine with root and the NBD module (a node, or a local machine with
the dev stack sourced):

Root-only tests check root, `/dev/nbd0`, and required tools; suite-node scripts set `SWARMY_REQUIRE_ROOT_SUITES=1` so missing prerequisites fail rather than skip.

```bash
source .dev/env
sudo -E swarmy-core vol create base-ubuntu:dev
sudo -E swarmy-core vol ls
sudo -E swarmy-core vol attach VOLUME_ID --background
sudo -E swarmy-core vol flush VOLUME_ID
sudo -E swarmy-core vol snapshot VOLUME_ID
sudo -E swarmy-core vol detach VOLUME_ID
```

`swarmy-core vol --help` lists every subcommand. Creation, cloning, listing, and
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

Cloud API keys for creating machines are stored with `swarmy cloud login`; see [cloud.md](cloud.md).

Use [REMOTE.md](REMOTE.md) for remote bring-up, upgrade, recovery, teardown,
costs and node troubleshooting. For fleet-driver commands see
[fleet-runbook.md](fleet-runbook.md).

### Conversation summaries and memory

Named agents keep a chain of main sessions. At the end of a turn the worker
compares the latest provider input plus output token count with the model
context window minus a 16,384-token reserve (or the configured threshold).
Side sessions check the latest total token usage after tool results; both main and
side sessions keep recent context through compaction. Unknown windows do not
trigger usage compaction, but a provider context overflow may still recover.
Ephemeral sessions do not compact automatically.

The summary is a normal durable inference job with no tools or cache writes.
It asks the model for a Pi-format Markdown checkpoint. A successful summary
archives the old session and creates a successor with a user-role summary
opening and recent context. Creation, archival, links, and main pointer
replacement commit in one fenced transaction. A failed or truncated summary
keeps the existing session. `session list` and `agent show` identify archived
sessions; `session show ID` still reads their full logs. Open chats display a
notice and follow the chain, including after a missed live notification.
`run --session OLD` follows to the successor and prints the same notice.

The worker includes memory files in every named-agent inference, including
turns in side sessions. `[memory] dir` (`SWARMY_MEMORY_DIR`) defaults to
`/home/agent/memory`; `[memory] max_bytes` (`SWARMY_MEMORY_MAX_BYTES`)
defaults to 32768. The node reads regular files directly in that directory in
filename order, includes their names, and adds a note when the byte budget
truncates content. Subdirectories, symbolic links, and special files are skipped. Use an
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
