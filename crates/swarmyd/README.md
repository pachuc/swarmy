# Node agent

`swarmyd` registers a node in FoundationDB, refreshes its heartbeat, and hosts
runc sandboxes and their NBD volume servers. It uses the same shared attachment
service as `swarmy vol attach`, including writer fencing, lease renewal,
background uploads, periodic snapshots, and final checkpoint on detach. Calls
arrive on the node tool queue and share one computer per agent.

Run the binary as root after starting the dev stack:

```sh
scripts/dev-stack.sh start
source .dev/env
cargo build -p swarmyd -p swarmy-cli
sudo -E target/debug/swarmyd
```

Configuration comes from `swarmy-config`. The existing `node_id` setting or
`SWARMY_NODE_ID` selects a stable ULID; otherwise `.swarmy/node-id` persists it.
Set capacity to the resources reserved for swarmy on this machine. The memory
figure is the sandbox budget after reserving RAM for the daemon and caches;
`node_memory_reserve_mib` derives it from host RAM at registration. Defaults
advertise one CPU, 1 GiB sandbox memory, 32 GiB disk, and one sandbox as a
secondary count cap.

| TOML setting | Environment override | Default |
| --- | --- | --- |
| `node_roles` | `SWARMY_NODE_ROLES` (comma-separated) | `["sandbox", "volume"]` |
| `node_capacity.cpu_millis` | `SWARMY_NODE_CPU_MILLIS` | `1000` |
| `node_capacity.memory_bytes` (sandbox budget) | `SWARMY_NODE_MEMORY_BYTES` | `1073741824` |
| `node_memory_reserve_mib` (derive budget from host RAM) | `SWARMY_NODE_MEMORY_RESERVE_MIB` | unset |
| `node_capacity.disk_bytes` | `SWARMY_NODE_DISK_BYTES` | `34359738368` |
| `node_capacity.sandboxes` | `SWARMY_NODE_SANDBOXES` | `1` |
| `node_heartbeat_interval_ms` | `SWARMY_NODE_HEARTBEAT_INTERVAL_MS` | `5000` |
| `sandbox_idle_seconds` | `SWARMY_SANDBOX_IDLE_SECONDS` | `1800` |
| `placement_lease_seconds` | `SWARMY_PLACEMENT_LEASE_SECONDS` | `30` |

`Store::get_node` reads `("node", node_id)`. `scan_live_nodes` takes a minimum
heartbeat timestamp, an exclusive node-id cursor, and a page size. It returns
live records and a cursor over all examined records, including expired nodes.
Continue until the cursor is absent, even if a page has no live records.
Heartbeat updates cannot move timestamps backwards.

## Local control

The owner-only Unix socket is `.swarmy/node/control.sock` under the discovered
configuration root. `swarmyd::{Request, Response}` defines its newline-delimited
JSON protocol. Each connection carries one request, limited to 64 KiB. Create,
exec, pause, resume, destroy, and capabilities are available. Exec sends bounded
stdout/stderr frames as bytes, then an exit result. Callers should keep reading
until the terminal response. This is a trusted local administrative interface;
agent calls use the placement-fenced NATS path.

The runtime permits one execution at a time per sandbox. It uses a writable
ext4 root filesystem, private PID/mount/IPC/UTS/cgroup namespaces, and the host
network for outbound package downloads. The guest socket directory is mounted
at `/run/swarmy`; `/etc/resolv.conf` is bound read-only. The container init is
`/bin/sleep infinity` until the guest-agent slice supplies its own init.

Timeout includes blocked output delivery. A timeout or cancelled exec kills
only its own process group. Managed background processes remain alive. The disk remains attached
until pause or destroy. Resume always cold boots: runc reports
`memory_pause = false` and `kvm = false`.

Destroy stops the container, unmounts, publishes the final volume manifest,
detaches NBD, and releases the writer lease. Read the resulting manifest from
`Store::get_volume`. Pause performs the same sequence and returns a handle for
cold resume. SIGINT and SIGTERM finish in-progress lifecycle operations, cancel
execs, then destroy all local sandboxes with a final flush.

On startup, an exclusive directory lock prevents a second daemon from taking
over local state. Journals identify containers and mounts from a killed daemon;
startup deletes those containers, unmounts their disks, and clears journaled NBD
attachments before registration.
A subsequent create uses the last committed head and a fresh local overlay.
It must wait for the previous writer's 60-second lease to expire. Recovery does
not publish the killed command's uncommitted writes or steal a live writer lease.

## Root acceptance test

Build as the ordinary user; run only the test executable under sudo. The test
skips with a message without root or the dev stack environment. The test builds
`images/base-ubuntu` through the CLI if `base-ubuntu:test` is absent, and reuses
that immutable image on subsequent runs. `SWARMY_TEST_CLI` can override the CLI
binary path when using a non-default target directory.

```sh
scripts/dev-stack.sh start
source .dev/env
cargo build -p swarmy-cli
cargo test -p swarmyd --test node --no-run --message-format=json > /tmp/swarmy-node-build.json
sudo -E "$(jq -r 'select(.executable != null and .target.name == "node") | .executable' /tmp/swarmy-node-build.json)" --nocapture
```

The test checks registration and advancing heartbeats, live-node pagination,
capabilities, installing jq, recreating a volume from the final manifest,
pause/resume, separate output streams and exit codes, descendant timeout, and
SIGKILL during exec followed by re-registration and committed-head recovery.
Cleanup guards stop containers and unmount after a test failure.

## Sandbox tools from sessions

Start a disk-backed session with `swarmy run --image base-ubuntu:TAG "PROMPT"`.
The CLI pins the image manifest before waking the session. The worker records
sandbox tool request events, asks the scheduler for a live sandbox node, and atomically
stores tool jobs while releasing the session into `WaitingTools`. A recovery
scan republishes pending jobs if a process dies before publishing to NATS.

The node places an unplaced agent locally, accepts a live placement assigned to
this node, or takes over an expired epoch. It refuses live placements on other
nodes. A per-agent task records the placement, attached volume, container, and
time since the last completed call. It serializes calls across sessions of that
agent while other agents execute independently.

The home volume uses the agent ULID as its volume id. Until agent records own a
volume reference, the first call initializes this volume from the session's
pinned image (or imports the published head of its existing slice 2 sandbox).
Subsequent sessions use that same volume. No attempt volume is created. Clone
support remains in the volume library for explicit forks.

The container and its processes survive successful calls. Each call has its own
renewable tool lease, and completion checks both that lease and the placement
epoch. Output completion does not freeze, flush, or advance the disk. The result's
manifest id names the latest observed checkpoint; it does not assert durability
of that call's writes. The attachment continuously stages chunks and publishes
snapshots according to the shared volume snapshot settings.

Placement renewal runs every third of the lease duration, including during boot,
commands, and final checkpoints. Renewal failure or expiry cancels execution,
stops guest processes, and discards the attachment without publishing. Writer
acquisition, renewal, and publication also validate the bound placement epoch in
FoundationDB. A restarted daemon cleans local state first and waits for old
placement and writer leases to expire before rebuilding under a new epoch.

After the idle window without a call, the node stops processes, unmounts,
publishes a final checkpoint, detaches the device, and releases placement.
The retained epoch counter makes the next placement report reason `eviction`.
Graceful shutdown does the same for hosted agents, keeping renewal active during
checkpointing. Idle time starts after a call finishes, so an active call cannot
be evicted. Running managed processes prevent idle eviction; unmanaged background
processes alone do not reset this timer.

The root node suite also drives the NATS tool path across multiple sessions of
one agent. It checks process persistence, renewal during a call, no publication
on a trivial call with 64 MiB dirty, idle checkpoint and rehydration, SIGKILL
cleanup, and refusal after another node takes over the epoch.

The registered tools are `bash`, `process_start`, `process_list`, `process_log`,
`process_stop`, and `checkpoint`. Bash returns stdout, stderr, and exit status;
a timeout returns a tool error without stopping other process groups.
Checkpoint freezes the mounted disk, publishes through the attachment control
socket, thaws, and returns the committed manifest id.

Process tools execute a small Python program in the sandbox. Images must include
`/bin/bash`, `/usr/bin/setsid`, and `/usr/bin/python3`, as the base Ubuntu image
does. Each managed process has a ULID directory under `/var/lib/swarmy/processes`
containing `record.json` and `output.log`. Records include the command, PID,
Linux start ticks, start time, and sandbox lifetime. Launch detaches the process
session and redirects all standard streams so the launching exec can finish.
Listing reads disk records and checks `/proc`; it needs no daemon memory and can
be reconstructed whenever the sandbox remains alive. The current node restart
path rebuilds containers, so those processes are lost. Old records report
`restarted` and cannot be used to stop a process in the new sandbox.

Log reads return at most the last 64 KiB of combined stdout and stderr. Stop
signals the managed process group with TERM, waits 250 ms, then sends KILL.
Files persist across failures only up to the latest published snapshot, normally
every ten minutes or when checkpoint is called. Neither snapshots nor idle
eviction preserve process memory.

The root suite checks a managed HTTP server from a later bash call, listing and
logs, a bounded log tail, stop, idle protection, timeout isolation, explicit
checkpoint against the store head, and old process ids after a rebuild.

## Shared computer status

The hosting actor admits tool calls through a bounded FIFO channel per agent.
Calls waiting for channel capacity retain FIFO admission order. A running call
finishes or reaches its tool timeout before the next call starts; later calls
cannot overtake an already queued session. Each call collects its own stdout
and stderr before committing its result.

Each node heartbeat publishes `swarmy_core::AgentCallStatus` in the separate
store row `("agent_call_status", agent_id)`. CLI consumers can read it through
`Store::agent_call_status(agent_id)`. Its fields are `agent_id`, `node_id`,
`epoch`, `holder_session_id`, `queued_calls`, `observed_at`, and `expires_at`.
The holder includes sandbox startup and call execution. The queue count excludes
the holder and includes calls waiting for channel capacity. An idle resident
computer has a null holder and zero queued calls.

Status is sampled at the configured node heartbeat interval and expires after
three intervals. The store rejects writes from a replaced or expired placement
and hides observations after sample expiry, placement expiry, release, deletion,
or takeover. A missing observation means occupancy is unknown. It must not be
displayed as an idle computer. These fields report activity and grant no sandbox
authority. Existing node and placement binary records keep their encoding.
