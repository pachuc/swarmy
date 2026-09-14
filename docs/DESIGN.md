# Swarmy Design

Status: draft v1, 2026-09-14. Decisions recorded here were agreed in planning
and are binding until revisited. Open questions are collected at the end.

## 1. Goal

Run millions of long-lived, personalized, fully capable coding agents on
fungible cloud compute, with no single machine failure able to lose an agent,
a session, or an agent's disk. Inference comes from external provider APIs.
Every service scales horizontally and independently. Everything is Rust and
all orchestration logic is in house.

## 2. Principles

1. **An agent is data, not a process.** An agent is an append-only event log,
   a compact snapshot, and a durable disk. Nothing runs on an agent's behalf
   between steps. Idle agents cost storage rows only.
2. **Every worker is stateless and leased.** Work is claimed under an
   expiring lease. A dead worker is an expired lease, and the work is
   re-dispatched. No worker has identity.
3. **Idempotency everywhere.** Every inference request and tool call carries a
   deterministic key derived from session id and step sequence. Retries never
   double-spend or double-apply.
4. **Three stateful primitives, none written by us.** FoundationDB is truth.
   NATS is motion. Object storage is bulk. Everything we write is stateless or
   rebuildable from those three.
5. **Quota is the scarce resource.** The scheduler is a quota-aware admission
   controller, not a queue. Millions of live sessions is easy. Concurrent
   inference streams are bounded by provider capacity.
6. **Disk is durable, memory is a cache.** Sandbox disk state is globally
   durable. Sandbox memory state is a host-local optimization that may be
   discarded.
7. **Abstract the seams we consume, not the cloud.** Traits for node
   provisioning, sandbox runtime, and blob storage. No cloud-specific code
   outside those trait implementations.
8. **Build in vertical slices.** Every slice works end to end and retires a
   specific risk. No layer is built out ahead of the slice that needs it.

## 3. Concepts

| Concept | Meaning |
|---|---|
| **Agent** | Durable identity. Name, persona and model config, one home volume, one mailbox, one main session, zero or more worker sessions. Lives forever unless deleted. |
| **Session** | One bounded context window. An append-only event log plus periodic snapshots. States: `Idle`, `Runnable`, `Leased`, `WaitingInference`, `WaitingTools`, `Sleeping`, `Completed`. |
| **Main session** | The agent's continuous thread. Physically a chain of sessions linked by compaction summaries and by the memory files on the home volume. All inbox delivery lands here. |
| **Worker session** | Spawned by an agent for a discrete task. Runs in the agent's sandbox by default, or in a clone when isolation is requested. Reports back via the mailbox on completion. |
| **Step** | One unit of leased work on a session: build prompt and submit inference, or parse a completed inference and dispatch tools, or fold tool results back in. Milliseconds of CPU. |
| **Event** | An immutable record in a session log. Messages, parts, inference requests and completions, tool calls and results, volume manifest changes, state transitions. |
| **Volume** | A persistent block device. A chain of manifests over content-addressed fixed-size chunks in object storage. One writer at a time. Clone is copy-on-write. |
| **Base image** | An immutable volume manifest shared by many agents. OS, toolchains, browser. |
| **Sandbox** | An ephemeral execution environment on one node with the agent's home volume attached. One per agent, shared by that agent's sessions. |
| **Channel** | A durable ordered message log with membership. A DM is a two-member channel. |
| **Principal** | Anything that can be a channel member: an agent, a human, or the system. |
| **Node** | A Linux host running `swarmyd`. Advertises roles and capacity. |

## 4. Services

All services are stateless Rust binaries except where noted. Any instance of
a service can handle any request.

### 4.1 Store (library, not a service)

The FoundationDB layer. Owns the key layout and every transaction. Every
other service links it. No service talks to FoundationDB except through it.

### 4.2 Scheduler

Owns the runnable set, leases, timers, wakeups, quota admission, and sandbox
placement. Multiple instances partition sessions by hash range and coordinate
through FoundationDB. Publishes work nudges to NATS. Reaps expired leases and
dead nodes.

### 4.3 Step worker

Claims a step, loads snapshot plus tail, runs the harness logic for that
step, appends events, releases. Never holds an inference future. Never
executes sandbox tools. Remote tools with no filesystem dependency, such as
web fetch, run here.

### 4.4 Inference gateway

The OpenCode port. Provider adapters, auth flows, message normalization,
streaming. On top: key pools, per-key rate accounting, cross-provider
failover, cost metering, partial-stream journaling, and session-to-key
affinity for prompt caching. Consumes inference requests from NATS, writes
completions as session events.

### 4.5 Node agent (`swarmyd`)

Runs on every node. Registers, heartbeats, reports capacity. Hosts the volume
block device server and the sandbox runtime. Executes sandbox-bound tool
calls against local sandboxes. Flushes volumes and records manifests.

### 4.6 Guest agent (`swarmy-guest`)

Runs inside each sandbox. Exposes exec, file operations, filesystem freeze
and sync, display screenshot, and browser devtools proxy over vsock or a unix
socket to `swarmyd`.

### 4.7 Volume service (library plus `swarmyd` component)

Chunk store on object storage, manifests in FoundationDB, block device
materialization on nodes, flush and snapshot, clone.

### 4.8 Channels

Channel logs, membership, per-principal cursors, delivery to agent inboxes
with coalescing, WebSocket delivery to humans.

### 4.9 API and CLI

External HTTP and WebSocket API. Create agents, send messages, join channels,
observe sessions, manage volumes and images. The `swarmy` CLI wraps it.

## 5. Step lifecycle

The whole system is this loop. Every arrow that crosses a process boundary
is a FoundationDB transaction plus a NATS nudge.

1. **Wakeup.** A message arrives, a timer fires, or a child completes.
   Transaction: session `Idle` to `Runnable`, insert into runnable index.
   Nudge on `sched.runnable.{partition}`.
2. **Claim.** A step worker receives the nudge. Transaction: verify
   `Runnable`, write lease with expiry, set `Leased`, read snapshot ref and
   event tail.
3. **Submit inference.** Worker builds the prompt. `request_id` is
   `blake3(session_id, seq)`. Transaction: append `InferenceRequested`,
   record inflight, set `WaitingInference`, clear lease. Publish request on
   `infer.req.{provider_class}`.
4. **Inference.** Gateway consumes the request under a NATS ack deadline.
   Checks the idempotency record. Streams from the provider, journals deltas
   to `infer.live.{session_id}` for observers, writes the completed response
   to object storage if over the value limit. Transaction: append
   `InferenceCompleted`, clear inflight, set `Runnable`. Ack.
   If the gateway dies mid-stream the message is redelivered. The idempotency
   record shows requested but not completed, so the request is re-issued. Cost
   is one wasted partial call, never a lost session.
5. **Parse.** Worker claims again. If the response ends the turn, go to 8.
   Otherwise, transaction: append one `ToolCallRequested` per call, set
   `WaitingTools`. Publish remote tool calls on `tool.remote`. Publish sandbox
   tool calls on `tool.node.{node_id}` if the agent has a placed sandbox,
   otherwise on `sched.place` for the scheduler to place first.
6. **Execute.** A step worker or `swarmyd` claims each tool call under its own
   lease. For sandbox tools: run through the guest agent, then freeze, flush
   dirty chunks, write a new manifest, unfreeze. Transaction: append
   `ToolCallCompleted` including the new manifest id, and when all outstanding
   calls are done set `Runnable`.
7. **Fold.** Worker claims, appends tool result parts, returns to 3.
8. **Turn end.** Main session: set `Idle`. Worker session: append result,
   set `Completed`, post a message to the parent agent's mailbox.

Compaction is a step type: when the tail exceeds a threshold, a step
summarizes, writes a snapshot, and starts a new session in the chain.

**Crash consistency between disk and log.** A volume flush happens at tool
call boundaries. If a node dies mid-call, the disk rolls back to the manifest
recorded before that call, the tool call lease expires, and the call re-runs
from a clean state. The disk and the log are always a consistent pair.

## 6. Storage layout

### 6.1 FoundationDB

Tuples in a `swarmy` directory subspace. Values are protobuf or bincode.
Values over 100KB are stored in object storage with a pointer in the value.

```
("agent", agent_id)                         -> AgentRecord
("agent_by_name", name)                     -> agent_id
("agent_dir", agent_id)                     -> presence, description, tags
("session", session_id)                     -> SessionRecord {agent_id, state, head_seq, snapshot_ref, parent}
("event", session_id, seq)                  -> Event
("snapshot", session_id, seq)               -> SnapshotRef
("runnable", partition, priority, wake_at, session_id) -> ()
("lease", session_id)                       -> {owner, expires_at, seq}
("toolcall", session_id, seq, call_id)      -> {state, lease, result_ref}
("idem", request_id)                        -> {state, result_ref}
("inflight", request_id)                    -> {session_id, seq, provider, key_id}
("quota", provider, key_id, model, window)  -> counters
("affinity", session_id)                    -> {provider, key_id}
("volume", volume_id)                       -> {head_manifest, writer_lease, parent}
("manifest", manifest_id)                   -> ManifestHeader {size, chunk_size, root_ref}
("image", name, tag)                        -> manifest_id
("sandbox", agent_id)                       -> {node_id, state, memory_snapshot_ref}
("node", node_id)                           -> {roles, capacity, last_heartbeat, cached_images}
("channel", channel_id)                     -> ChannelRecord
("channel_msg", channel_id, seq)            -> Message
("member", channel_id, principal_id)        -> MemberRecord
("cursor", principal_id, channel_id)        -> seq
("inbox", agent_id, seq)                    -> {channel_id, msg_seq}
("timer", wake_at, session_id)              -> ()
```

Partition count for `runnable` is fixed at creation, for example 256, and
sessions hash to a partition. Scheduler instances own partition ranges.

### 6.2 NATS subjects

```
sched.runnable.{partition}     nudge: work available
sched.place                    sandbox placement requests
infer.req.{provider_class}     inference requests (JetStream, ack wait = lease)
infer.live.{session_id}        streamed deltas for observers (core NATS)
tool.remote                    remote tool calls (JetStream)
tool.node.{node_id}            sandbox tool calls for one node (JetStream)
node.heartbeat                 node liveness
session.events.{session_id}    event fan-out for UIs
channel.msg.{channel_id}       live channel delivery
```

JetStream is used where at-least-once delivery matters. Correctness never
depends on it because every consumer re-validates against FoundationDB.

### 6.3 Object storage

Accessed through the `object_store` crate so S3, GCS, and Azure Blob are
interchangeable.

```
chunks/{hash[0:2]}/{hash}      immutable fixed-size volume chunks, blake3 keyed
manifests/{manifest_id}        chunk hash arrays for large volumes
blobs/{hash}                   large event payloads, tool outputs, responses
memsnap/{agent_id}/{ts}        optional uploaded memory snapshots (later)
```

## 7. Volumes

This is the largest piece of novel engineering and the second slice, because
a blocker here would reshape everything above it.

### 7.1 Model

A volume is a sparse block device of fixed size, for example 32 GiB, split
into fixed-size chunks, initially 256 KiB. A manifest is the array of chunk
hashes by block index, with a reserved hash meaning all zeros. A manifest is
immutable. A volume record points at its head manifest and carries a writer
lease. Base images are manifests. Creating an agent volume from a base image
is writing a volume record that points at the image manifest. Cloning is the
same operation against any manifest. Both are free.

Fixed-size chunking is correct for block devices because offsets are stable,
so content-defined chunking buys nothing. Large manifests are stored as a
two-level tree so a snapshot after a small write rewrites only the affected
leaf and the root.

### 7.2 Node materialization

`swarmyd` serves each attached volume as a kernel block device. First
implementation is an in-process NBD server, because the protocol is simple
and the kernel module is available on every distribution. Second
implementation is `ublk` via the `libublk` crate for throughput. For
Firecracker, a later implementation serves vhost-user-blk directly with no
kernel device.

Read path: local NVMe chunk cache, then object storage, with readahead. Base
image chunks are shared by every agent on a node so they stay hot. Per-agent
diffs are small and cold reads are rare after warmup.

Write path: writes land in a local dirty block store on NVMe. Flush uploads
dirty chunks by hash, skipping ones the store already has, writes a new
manifest, and transactionally advances the volume head. Flush is triggered at
tool call boundaries and on sandbox pause.

Consistency: before a flush the guest agent runs sync and a filesystem
freeze, so every manifest is a clean ext4 state. Even without that, a
manifest is equivalent to a power-loss snapshot and ext4 journal replay
handles it.

Single writer: the volume writer lease is held by the node hosting the
sandbox. Attaching elsewhere requires the lease to expire or be released.

### 7.3 Risks to retire in slice 2

- Kernel module availability on stock cloud images. Verify on GCP, AWS, and
  Azure Ubuntu LTS images. Both `nbd` and `ublk_drv` are present locally.
- Cold read latency from object storage per chunk. Measure and tune chunk
  size and readahead. Target: interactive shell usable within seconds of cold
  boot on a warm-base-image node.
- Flush time after a heavy tool step such as a full dependency install.
  Measure. Consider a background flush that overlaps with the next step.
- Root privileges on nodes. `swarmyd` runs as root. Acceptable on our own VMs
  and in privileged pods.

## 8. Sandboxes

### 8.1 Runtime

`SandboxRuntime` trait with three implementations in this order:

1. **OCI container via runc.** Works on every host, no KVM. First slice.
   Rootfs is the mounted volume block device. Memory pause is not supported,
   so pause equals stop, and resume equals cold boot from disk.
2. **gVisor.** Works on every host, has native checkpoint and restore for
   memory pause. Some syscall gaps and I/O overhead. Candidate for the
   default on hosts without KVM.
3. **Firecracker.** Requires `/dev/kvm`. Strong fault containment, fast
   memory snapshot and restore, virtio-blk from our block device. Default
   on hosts with KVM.

Cloud KVM availability is a hard constraint. GCP standard VMs support nested
virtualization. Azure Dv3 and later do. AWS exposes KVM only on `.metal`
instances. So on AWS the default runtime is gVisor unless metal node pools
are used. This is why the container path is built first and never removed.

### 8.2 Lifecycle

States: `Absent`, `Placing`, `Booting`, `Running`, `Paused`, `Dead`.

Policy, tunable per agent class:

- Sandbox stays `Running` while tool calls are in flight and for a short
  hysteresis window after, since coding turns cluster tool calls.
- After the window with no tool calls, pause: freeze and flush disk, snapshot
  memory to host disk if the runtime supports it, free CPU and RAM.
- After a longer idle period, evict: discard the memory snapshot and detach
  the volume. The agent is now `Absent` and costs nothing.
- On the next sandbox tool call: resume from memory snapshot if it exists on
  a live node, otherwise place and cold boot from the volume head.
- Placement prefers the node holding the memory snapshot, then nodes with
  the agent's base image hot in cache, then any node with capacity.
- Node death is detected by heartbeat loss. All sandboxes on it are marked
  `Dead`, their volume writer leases are broken, and in-flight tool calls
  are re-dispatched after re-placement. The agent is informed in its next
  turn that its environment restarted and running processes were lost.

One sandbox per agent. All of an agent's sessions share it. A worker session
that requests isolation gets a cloned volume and its own sandbox.

### 8.3 Guest agent

Static Rust binary baked into every base image, started as PID 1 or by init.
RPC over vsock in Firecracker, a unix socket bind mount in containers.
Operations: exec with streaming stdio and timeouts, read, write, stat, list,
glob, grep, sync and freeze, display screenshot, and a devtools proxy to the
in-sandbox browser.

### 8.4 Browser and screen

Base image variant with a headless display server, a window manager, and
Chromium. Tools expose devtools navigation, clicking, and DOM extraction,
and a raw screenshot of the display for anything that is not a browser. No
GPU. Memory budget per resident sandbox with a browser is roughly 2 to 4 GiB,
which is a further reason memory pause matters.

## 9. Inference gateway

The port from OpenCode covers the provider catalog sourced from models.dev,
the auth flows including OAuth for Anthropic, GitHub Copilot, OpenAI, and
Google, the message and part schema, and the tool set.

Provider normalization is written fresh in Rust in the `swarmy-llm` crate as
a unified request and response type with adapters. Most providers reduce to
four wire dialects: Anthropic Messages, OpenAI Chat and Responses, Gemini, and
OpenAI-compatible generic. Bedrock and Vertex are auth and endpoint variants
of the first three.

**First provider: ChatGPT subscription via the Codex backend.** Slice 1
uses ChatGPT subscription inference, not the OpenAI platform API. Requests go
to the ChatGPT backend Responses endpoint with a Bearer access token and the
account id header, in the Responses API wire dialect. Auth is the Codex OAuth
device-code flow with refresh tokens, stored in the same auth.json shape the
Codex CLI uses so an existing login can be imported. The Codex CLI is itself
Rust, so its login crate and backend client are the reference implementation
to port or depend on. Operating rules learned from running Codex in remote
sandboxes: one credential cache is one refresh chain, concurrent refreshers
get the whole session revoked server-side, so refresh is serialized per
account through a single writer, gateways read the current access token from
the credential store on every request, and a key pool here means many
ChatGPT accounts, each with its own quota counters. Using subscription
inference in a third-party harness is the operator's decision and is taken as
given in this design.

Swarm additions:

- **Key pools.** Many keys per provider, each with its own quota counters.
- **Admission.** Token buckets per provider, key, and model, with request and
  token dimensions. The scheduler consults them before marking a session
  `Runnable` for an inference step, so waiting sessions hold no lease.
- **Affinity.** A session sticks to one provider and key while it is warm so
  prompt caches hit. Broken only on failure or quota exhaustion.
- **Failover.** Ordered fallback list per model class.
- **Journaling.** Streamed deltas are published live. Completed responses are
  written durably before the completion event. A gateway crash costs one
  retry.
- **Metering.** Tokens and cost per session, agent, provider, and key,
  written as counters in FoundationDB.
- **Mock provider.** Configurable latency and canned tool-calling behavior,
  so the control plane can be exercised without quota.

## 10. Channels and self-organization

- Channels are logs in FoundationDB. Membership is explicit. A DM is a
  channel with two members.
- Sending a message appends to the channel log and, for each agent member,
  appends an inbox pointer and wakes the main session if it is `Idle`. If the
  session is mid-step, the inbox accumulates and the next turn receives all
  pending messages as one batch.
- Humans receive over WebSocket through the API. External chat bridges are
  principals added later.
- Agents get tools: `send_message`, `create_channel`, `invite`,
  `list_agents`, `describe_agent`, `spawn_agent`, `spawn_worker`. Because
  agents are trusted there is no permission model beyond membership.
- Default response policy, configurable per agent: always respond in a DM,
  respond in a group channel only when mentioned or when the agent's own
  turn logic decides to. This lives in the harness prompt, not in infra.
- Agent directory: name, description, tags, presence derived from session
  state and sandbox state.

## 11. Compute abstraction

Three traits. No cloud-specific code anywhere else.

```rust
trait NodeProvider {
    async fn provision(&self, spec: NodeSpec) -> Result<NodeHandle>;
    async fn terminate(&self, node: &NodeHandle) -> Result<()>;
    async fn list(&self) -> Result<Vec<NodeHandle>>;
}

trait SandboxRuntime {
    async fn create(&self, spec: SandboxSpec, disk: BlockDevice) -> Result<Sandbox>;
    async fn pause(&self, sb: &Sandbox) -> Result<PauseHandle>;   // may be Unsupported
    async fn resume(&self, h: PauseHandle) -> Result<Sandbox>;
    async fn destroy(&self, sb: Sandbox) -> Result<()>;
    fn capabilities(&self) -> RuntimeCaps;                          // memory_pause, kvm
}

// BlobStore is the `object_store::ObjectStore` trait, used directly.
```

**Substrate decision.** The north star is off-the-shelf compute on AWS, GCP,
and Azure. Managed Kubernetes on each is the pragmatic substrate: node pools
give autoscaling, a privileged DaemonSet runs `swarmyd`, Deployments run the
stateless services, and FoundationDB and NATS have operators. On GKE a
nested-virt node pool gives Firecracker. The first `NodeProvider`
implementation is therefore a Kubernetes node pool scaler. Nomad or bare
metal are additional implementations later, not a redesign. Local development
uses a single machine with `swarmyd` run directly.

## 12. Crate layout

```
swarmy/
  Cargo.toml                 workspace
  crates/
    swarmy-core              ids, events, parts, session and agent records, traits, errors
    swarmy-store             FoundationDB layer: key layout, all transactions
    swarmy-bus               NATS subjects, publish and consume helpers
    swarmy-llm               unified LLM types, provider adapters, auth flows, catalog
    swarmy-volume            chunk store, manifests, NBD and ublk servers, flush and clone
    swarmy-sandbox           SandboxRuntime trait and runc, gVisor, Firecracker impls
    swarmy-harness           the agent loop as pure step functions, prompt assembly, compaction
    swarmy-tools             tool schemas and remote tool impls; sandbox tools as guest RPC
    swarmy-channels          channel logic
    swarmy-api               HTTP and WebSocket API types and server
  bins/
    swarmy-scheduler
    swarmy-worker
    swarmy-gateway
    swarmyd
    swarmy-guest             static musl binary for images
    swarmy                   CLI
  images/                    base image build scripts
  deploy/                    compose for local, Helm for clusters
  docs/
```

## 13. Vertical slices

Each slice ships an end-to-end working system, keeps every earlier slice's
acceptance test passing, and retires a named risk. Chaos tests accumulate
into a suite that kills random processes during runs.

### Slice 1: Immortal echo agent
**Retires:** durable step model, FoundationDB key layout, lease semantics,
event-driven inference completion.
**Build:** workspace, local stack of FoundationDB, NATS, and MinIO, core
types, store with session log and leases, scheduler, step worker, gateway
with the ChatGPT subscription provider and the mock provider, one in-worker tool such as
`get_time`, CLI to create a session and stream events.
**Accept:** `swarmy run "what time is it"` completes. Repeat while killing
the worker, gateway, and scheduler at random points mid-step. Every run
completes with exactly one inference charge per step, except at most one
extra charge for a gateway killed mid-stream.

### Slice 2: Block-level disk
**Retires:** the volume design, the biggest unknown.
**Build:** chunk store on `object_store`, manifests, NBD server in
`swarmyd`, base image build from a Dockerfile-like recipe into an ext4
manifest, attach, flush, snapshot, clone, and detach. Driven by
`swarmy vol` commands with no agent involved first. Then a `bash` tool that
runs in a runc container whose rootfs is the attached volume, with the
manifest recorded in the tool result event.
**Accept:** create a volume from a base image on machine A, install a
package, snapshot, attach on machine B, and the package is there. Clone is
constant time. Kill `swarmyd` between flushes, re-run the tool call, and
the disk is consistent. Run the same test on one GCP VM and one AWS VM with
stock Ubuntu images and record cold boot time, cold read latency, and flush
time for a large install.

### Slice 3: Real coding agent
**Retires:** harness quality, guest RPC design, tool dispatch with leases.
**Build:** guest agent, the OpenCode tool set ported as guest RPC, tool
queue with per-call leases, sandbox placement in the scheduler, system
prompt assembly.
**Accept:** an agent clones a repository, fixes a failing test, and runs the
suite green, while `swarmyd` is killed once mid-turn.

### Slice 4: Persistent agent
**Retires:** the agent and session model, continuity, memory files.
**Build:** Agent entity, home volume per agent, main session chain with
compaction, memory file conventions injected into the prompt, heartbeat
timers, `swarmy agent create tommy`.
**Accept:** tell Tommy a fact, force compaction, restart every process and
node, ask Tommy the fact and Tommy answers from memory files. Tommy's
installed tools from a prior session are still present.

### Slice 5: Channels
**Retires:** messaging, coalescing, self-organization primitives.
**Build:** Channels service, DM and group, WebSocket delivery for humans,
agent messaging tools, spawn agent and spawn worker, agent directory.
**Accept:** a human asks a group of two agents to split a task; they create
a DM, divide the work, each spawns a worker, and report back in the group
channel. Messages sent while an agent is mid-step arrive as one batch.

### Slice 6: Sandbox pause and resume
**Retires:** the compute efficiency claim.
**Build:** lifecycle policy, gVisor runtime with checkpoint and restore,
Firecracker runtime on a KVM host, placement affinity, node death handling.
**Accept:** one hundred agents in a chat loop on a machine sized for ten
resident sandboxes. Kill a node holding twenty; all twenty recover on other
nodes and are told they restarted.

### Slice 7: Browser and screen
**Build:** browser base image variant, devtools and screenshot tools.
**Accept:** an agent logs into a test web app, changes a setting, and
verifies it with a screenshot.

### Slice 8: Provider breadth and quota
**Build:** the remaining OpenCode auth and provider matrix, key pools,
admission token buckets, affinity, failover, metering.
**Accept:** two providers with tiny synthetic quotas; a swarm of agents
saturates both without a single rate-limit error reaching an agent.

### Slice 9: Cloud deploy
**Build:** Helm charts, FoundationDB and NATS operators, Kubernetes
`NodeProvider`, run on GKE then EKS.
**Accept:** the slice 5 scenario runs unchanged on both clouds.

Slice 2's cloud VM check is deliberately early so that the off-the-shelf
cloud constraint is validated against the riskiest component before anything
depends on it.

## 14. Open questions

- **Worker session sandbox sharing.** Default is shared sandbox. Whether a
  worker session editing the same repository as the main session needs a
  clone by default is a harness policy decision to be made in slice 4.
- **Volume size and growth.** Fixed 32 GiB sparse to start. Online growth
  is possible by extending the manifest; deferred.
- **Shared repositories between agents.** Git is the sharing protocol. An
  internal git server on top of the blob store is a natural later addition.
- **Memory index.** Full-text or vector search over memory files is a later
  service. Slice 4 injects files directly.
- **Multi-region.** NATS leaf nodes and per-region chunk caches are
  designed for but not built until after slice 9.
- **Local development without Docker.** The dev machine has no container
  runtime. Options are installing podman, or running FoundationDB, NATS, and
  MinIO as plain processes with a small script. Decide at slice 1 start.
