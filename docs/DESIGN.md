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
   a compact snapshot, and a durable disk. Its computer can keep processes
   running between steps. After idle eviction, the agent costs storage only.
2. **Every worker is stateless and leased.** Work is claimed under an
   expiring lease. A dead worker is an expired lease, and the work is
   re-dispatched. No worker has identity.
3. **Idempotency everywhere.** Every inference request and tool call carries a
   deterministic key derived from session id and step sequence. Committed
   results are deduplicated; external side effects require tool-specific
   reconciliation when an execution outcome is unknown.
4. **Three stateful primitives, none written by us.** FoundationDB is truth.
   NATS is motion. Object storage is bulk. Everything we write is stateless or
   rebuildable from those three.
5. **Quota is the scarce resource.** The scheduler is a quota-aware admission
   controller, not a queue. Millions of live sessions is easy. Concurrent
   inference streams are bounded by provider capacity.
6. **Published disk snapshots are durable.** Writes since the last published
   snapshot can be lost on node failure. Computer memory and running
   processes are local state and are lost on rebuild.
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
| **Sandbox** | The agent's long-lived computer on one node, with its home volume attached. Shared by all its sessions; rebuilt from a durable disk snapshot after eviction or failure. |
| **Placement** | The computer's hosting node, epoch, lease expiry, and last epoch change reason and time. Only the current unexpired holder may act. |
| **Channel** | A durable ordered message log with membership. A DM is a two-member channel. |
| **Principal** | Anything that can be a channel member: an agent, a human, or the system. |
| **Node** | A Linux host running `swarmyd`. Advertises roles and capacity. |

### 3.1 Ephemeral conversations and named agents

A new `chat` or `run` without `--agent` creates an **ephemeral session** with
its own anonymous identity and computer. Two ephemeral sessions have separate
writable disks. Exiting the client leaves the conversation resumable. Explicit
`session close ID` completes it and deletes its computer; the scheduler also
closes Idle ephemeral sessions after the retention period (24 hours by default,
configured by `SWARMY_EPHEMERAL_RETENTION_SECONDS`). Active sessions are excluded.
The idle clock starts at creation and resets on each transition into Idle.

`agent create NAME` creates a **named agent**, pinning its selected image.
By default, `chat --agent NAME` and `run --agent NAME` resume its main
conversation, creating it atomically if absent. `--new` opens a side conversation
without changing the main pointer. These sessions share one computer, writable
disk, and background processes, while retaining separate transcripts. Calls from
all sessions queue for serial execution on that computer. Quitting a client and the ephemeral
retention sweep do not delete named agents. `session close` refuses named
sessions; `agent delete NAME` deletes the identity and shared computer.

Computer deletion atomically removes placement and volume references and fences
further tools. The node stops processes and detaches the disk when renewal
fails. Object storage chunks become eligible for collection after the configured
grace period if no other image, volume, or snapshot references them. Conversation
logs stay readable after either kind of deletion; they do not keep deleted disks
alive. Reusing a deleted name creates a new identity.

Both kinds use the same checkpoint and recovery rules. A checkpoint preserves
files, not processes. After node failure, a named agent's rebuild notice is
recorded once per epoch in every existing session, including idle sessions,
before recovered tool results. `agent show` reports sampled call occupancy and
snapshot age; unknown occupancy must not be inferred to mean an idle computer.
See [the store lifetime contract](agent-lifecycle.md) and the
[command walkthrough](DEV.md#ephemeral-sessions-and-named-agents).

### 3.2 Persistent agent configuration and continuity

Each named agent stores optional system prompt, model, and reasoning-effort
settings. The worker resolves those overrides against the stack defaults before
inference. Updating an agent affects subsequent requests; it does not create a
new identity, disk, or conversation. The selected base image remains pinned.

The agent's main-session pointer is durable and changes only through an explicit
main-session selection or summarization. When a completed main turn reaches
`SWARMY_SUMMARIZE_AT_TOKENS` (by default 75% of the configured context window),
the worker requests a structured summary containing goals, state of work, open
questions, and facts to keep. A
valid summary archives the old session and opens a fresh idle session. The
summary, previous/next links, and main pointer commit together under the worker
lease. Invalid or failed summaries leave the existing conversation usable. Side
sessions do not automatically summarize the main conversation, and old
transcripts remain readable.

Memory files live on the agent's home volume, by default under
`/home/agent/memory`, configured with `SWARMY_MEMORY_DIR`. Agents use the file tools
to record facts there. Before ordinary inference, the worker asks the current
node for a bounded, sorted memory excerpt and includes it in the system prompt.
The node checks placement authority and caches excerpts by file metadata.
Unavailable computers contribute no excerpt until a tool places and opens the
computer again. Memory files and installed packages share the disk snapshot
contract: use `checkpoint` to acknowledge durable storage before a failure test.
Summarization preserves conversation context; it does not replace these files.

`set_timer` stores a note and either a positive `delay_seconds` or an absolute
RFC 3339 `at` timestamp. Past absolute times are eligible immediately. Timers
belong to named agents, including timers set from side conversations, and always
deliver to the current main conversation, opening it if absent. Each agent can
have 32 pending timers; notes are bounded to 1,024 UTF-8 bytes. `list_timers` returns pending timers and
`cancel_timer` cancels one by its ULID. Cancellation is idempotent and cannot
retract a delivered note. Timer mutations and their tool completions commit
under the same worker head and lease fence, so replay cannot create duplicates.

The scheduler pages the durable due-time index on its scan tick. Busy main
conversations leave due timers pending until they become idle; summarization
therefore cannot strand a note in an archived session. Delivery resolves the
main pointer, appends the note as a system message, makes the session runnable,
and records the fired status with the session and event sequence in one
transaction. Competing schedulers cannot deliver twice. A failed append leaves
the timer pending for the next tick. A lost publication after a successful
append is recovered from the runnable index without appending the note again.
All timer state survives scheduler restarts. Deleted agents' due timers are
retired without waking their old sessions. Delivery is at or after the due time,
subject to service availability and completion of the current turn.

The [real-node acceptance](volume-benchmarks.md#2026-09-18-persistent-agent-continuity-across-summarization-and-restart) exercises
memory, installed tools, summarization, and a two-minute timer across a full
stop and restart of all Swarmy services and the node process.

## 4. Services

Control-plane services are stateless Rust binaries. Node agents own local
computers under placement leases; sandbox requests route to the current holder.

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
block device server and the sandbox runtime. Keeps each placed computer alive
between tool calls, renews its placement lease, and executes tools only for
the current unexpired epoch. Stops acting when renewal fails or the lease
expires. Continuously stages dirty chunks, publishes a disk snapshot every
ten minutes, and retains the last ten periodic snapshots. Evicts computers
after thirty minutes idle, after a final checkpoint. Reports occupancy
separately from the registered maximum computer capacity.

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
   lease. Sandbox jobs also carry the agent id, node id, and placement epoch.
   The node checks its unexpired placement before execution; the completion
   transaction checks that same epoch and live lease before appending
   `ToolCallCompleted`. When all outstanding calls are done, set `Runnable`.
   `bash` executes inside the existing computer. Ordinary tool completion
   does not freeze the filesystem or publish a disk snapshot. The explicit
   `checkpoint` tool acknowledges only after publishing a durable manifest.
7. **Fold.** Worker claims, appends tool result parts, returns to 3.
8. **Turn end.** Main session: set `Idle`. Worker session: append result,
   set `Completed`, post a message to the parent agent's mailbox.

Compaction is a step type: when the tail exceeds a threshold, a step
summarizes, writes a snapshot, and starts a new session in the chain.

**Durability of disk and log.** Tool results and session events are durable in
FoundationDB independently of computer disk snapshots. Node failure restores
the latest published manifest, so files written after it may be lost even if
a tool previously reported success. Background chunk upload alone does not
make a recoverable snapshot. Checkpoint is the explicit durability boundary.
In-flight calls lose their processes on rebuild; callers must reconcile an
unknown result and external side effects before retrying. The system does
not promise rollback of a single call or exactly-once external effects.

### 5.1 Turn timeline and proposed latency budget

The user message ID identifies a turn. The store updates `("turn", session_id)`
with the user append and saves `("request_turn", request_id)` with each request.
A retried tool therefore retains its original turn even after another user
message arrives. Inference carries that identity in its prompt. Neither key
changes existing event or session encodings.

Every stage emits a structured tracing event and an ephemeral
`session.timeline.{session_id}` observation: submitted, appended, nudged,
claimed, inference started, inference finished, tool dispatched, tool completed,
idle, final text rendered, and input enabled. Events include the session, turn,
optional request ID, host boot ID, monotonic nanoseconds, and UTC nanoseconds.
Nudged means a successful JetStream publication, timestamped at submission;
claimed means the lease transaction returned. Inference surrounds the provider
stream, excluding its subsequent durable commit. Tool completed means the node
returned from fenced completion. Idle follows the committed state transition.
The client timestamps final text after its renderer writes it, and input after
it observes the stable idle state. Final text can appear before server idle;
input enabling follows it. Rendering and server completion overlap.

Instrumentation does not append extra conversation events. Publication failure
is logged without changing a successful operation. Timeline feeds are lossy;
`swarmy bench turn` subscribes before submission and refuses incomplete samples
instead of reconstructing timestamps from log arrival. Raw records retain every
repeated scheduler, inference, and tool stage. Same-host intervals use the
monotonic clock. Cross-host intervals use UTC and require synchronized clocks;
raw samples flag those comparisons. End-to-end always uses the client's
monotonic clock, from submission through input enabled.

The proposed warm-turn budget for the zero-delay fake provider is **under
100 ms locally**, and **under three client-to-node round trips plus 100 ms
remotely**, for both a text turn and a turn with one trivial bash call. Here
`R` is a measured client-to-node round trip. Cold computer boot, image building,
real provider latency, and nontrivial user tool execution have separate budgets.
The following allocations are totals per turn, including both inference passes
for the bash shape, not allowances for every repeated step:

| Work | Local allowance | Remote allowance | Reason |
| --- | ---: | ---: | --- |
| Submit and commit user append | 10 ms | R + 10 ms | Durable admission |
| Wake, scheduler nudges, and lease claims | 15 ms | R + 15 ms | Dispatch must be driven by readiness, not a scan period |
| Build and deliver inference requests | 15 ms | 15 ms | Small prompts and local control-plane work |
| Fake provider streams | 10 ms | 10 ms | Includes both scripted passes and call logging |
| Dispatch bash and receive fenced completion | 20 ms | R + 20 ms | Warm computer and trivial command |
| Commit results, fold, snapshot, and commit idle | 20 ms | 20 ms | Durable completion and object-store work |
| Render final text and enable input | 5 ms | 5 ms | Live notification; the timer is recovery only |
| Total | 95 ms | 3R + 95 ms | Leaves 5 ms below the target |

These are targets, not current guarantees. The dated turn measurements in
[volume benchmarks](volume-benchmarks.md#2026-09-17-turn-timeline-benchmark)
compare them to the actual default scheduler and client timers. Meeting the
budget requires removing scan waits and the default five-second resend gate
from ordinary progress, distinguishing a new runnable step from a duplicate
nudge for the previous step, and removing stable-head poll waits from ordinary
input enabling. The shorter-timer local bash run still spends about 17 ms in
the fake provider and 31 ms on the tool, above their 10 ms and 20 ms allocations;
those paths also need less bookkeeping and execution overhead. The remote
budget requires batching or moving store-dependent work close to FoundationDB; today's
launcher control plane makes more than three sequential database round trips.
The benchmark intentionally preserves those behaviors so their cost remains
visible.

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
("turn", session_id)                        -> latest user MessageId
("request_turn", request_id)                -> originating user MessageId
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
("placement", agent_id)                     -> {agent_id, node_id, epoch, expires_at, last_change_reason, last_changed_at}
("placement_by_node", node_id, agent_id)     -> PlacementRecord
("placement_hosting", agent_id)             -> {claimed, last_renewed, failure_estimate}
("placement_epoch", agent_id)               -> retained epoch counter, including after release
("placement_count", node_id)                -> occupied computer slots
("node", node_id)                           -> {roles, capacity, last_heartbeat, cached_images}
("channel", channel_id)                     -> ChannelRecord
("channel_msg", channel_id, seq)            -> Message
("member", channel_id, principal_id)        -> MemberRecord
("cursor", principal_id, channel_id)        -> seq
("inbox", agent_id, seq)                    -> {channel_id, msg_seq}
("timer", agent_id, timer_id)                -> TimerRecord {due_at, note, status}
("timer_active", agent_id, timer_id)         -> pending TimerRecord
("timer_due", due_millis, agent_id, timer_id) -> pending TimerRecord
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
session.timeline.{session_id}  ephemeral timestamped turn stages
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

Write path: writes land in a local dirty block store on NVMe. Each write advances
its chunks' generations and invalidates their staged hashes. The
attachment background uploader selects chunks quiet for 250 ms, copies their
bytes and generations under the dirty-store lock, and releases it before remote
uploads. A completed hash is retained only if its generation is still current.
Overwritten uploads are unreferenced objects, never published disk contents.
By default, at most 32 uploads run concurrently across background work and publication.

Every ten minutes, on explicit checkpoint, or before orderly eviction,
publication captures each pending chunk's generation and staged hash under the
dirty-store lock. Writes then continue while uploads and the fenced head
transaction publish exactly that boundary. An overwrite preserves the boundary's
local overlay before changing it; clean blocks still come from the immutable
baseline. Copies use at most 8 MiB of memory per device, then a sparse temporary
file in the dirty directory. Prepared upload buffers are separately bounded by
upload concurrency. In-memory copies are released when prepared for upload;
the spill file is discarded when publication ends. Cancellation releases copies
immediately if the dirty lock is free, or on the next write. A failed copy abandons the snapshot without rejecting the tool's
write. Only unchanged generations become clean after publication.

Snapshots are crash-consistent block images. Ext4 replays its journal on mount,
as after power loss. Explicit checkpoints sync preceding buffered writes before
capturing the boundary, without freezing or pausing subsequent writes.
`swarmy vol flush --freeze` optionally freezes the discovered mount for an
operator who needs a clean filesystem image. `--mount` validates the mount path;
it does not enable freezing. Periodic snapshots never freeze.

While any tool call runs in the node process, upload admission across all its
volumes is limited to four concurrent chunk requests and 16 MiB/s. Requests
already sent finish normally; new admissions yield and share the bandwidth
budget. Tool execution never waits for upload admission or its drain. Flush JSON
reports the current concurrency and bandwidth limits and the number of uploads
admitted at tool priority during the flush. Outside tool activity the configured
per-device concurrency applies, with a default of 32.

Single writer: the volume writer lease is held by the node hosting the
computer. Attaching elsewhere requires the lease to expire or be released.
Placement and writer leases both fence publication; takeover must never bypass
a live volume writer. Each publication checks the placement epoch and its
unexpired lease in the same transaction as advancing the volume head.

### 7.3 Snapshot retention and garbage collection

While a computer is resident, dirty chunks upload continuously and a snapshot
loop publishes a crash-consistent manifest every ten minutes. Keep the last ten periodic
snapshots per volume, plus its current head and any explicitly pinned
checkpoints. An explicit checkpoint publishes immediately. Only an acknowledged
manifest commit establishes durability; staged chunks may never become part
of a manifest. Ten minutes is the target recovery window while publication
is healthy, not a bound during an upload or storage outage. Monitor the age
of the last successful snapshot and report it during recovery.

The garbage collector traces all live volume heads, retained snapshots,
clones, base images, and pinned checkpoints through manifests to chunks. It
reclaims unreferenced manifests and chunks only after a grace window longer
than the maximum permitted upload/publication interval. Objects first seen
unreachable are candidates, not immediate deletions. Before deleting, the
collector rechecks references and coordinates with publication so a concurrent
commit cannot reference an object being removed. In-progress uploads need
protection until commit or abandonment; an upload that exceeds its protection
window must restart or renew that protection. Retention removal only removes
references; physical deletion belongs to the collector. The grace window is
configurable and also covers uploads orphaned by failed or fenced commits.

`swarmy gc` marks retained snapshots of every volume, the head of every
attached volume (including an expired writer until explicitly released), and
every registered image. It reads the two-level roots and their leaves before
sweeping canonical `chunks/{hh}/{hash}` objects. Manifest roots and leaves are
outside the sweep namespace and remain available. The implicit zero hash is
always protected. Missing or corrupt live metadata aborts marking before any
deletions. Unretained provenance links do not keep chunks alive and are not
restore points.

The collector pages volume and image keys in groups of 64. Each volume's live
roots and each immutable header are read in separate transactions. This avoids
FoundationDB's five-second and ten-megabyte transaction limits. The pages are
not one consistent fleet snapshot: publications between pages rely on the
grace window, while unchanged data is protected by live predecessor manifests.

A chunk is eligible only if it is absent from the reference filter and its
object-store last-modified time is strictly older than the run's starting time
minus the grace window. The cutoff never advances during a run. The default
window is six hours, compared with the default ten-minute snapshot period.
Keep it longer than the maximum time from background staging through successful
publication, including upload time, retries, clock skew, and idle eviction.
Pausing or evicting an idle sandbox must publish its pending disk changes before
detaching; eviction must not leave staged data awaiting publication beyond the
grace. A stalled publisher must retry its uploads before reusing staging older
than that bound. Operators must not restore a pruned manifest or register it as
an image without first restoring its data. Content deduplication does not make
an unreferenced, old object a durable staging reference. For that reason,
attached volume writers and image uploads record a reuse timestamp in the
store before trusting an existing chunk. A collector reserves each candidate
in a transaction that checks this timestamp against its fixed cutoff. Reuse
and deletion reservations conflict: if deletion wins, the uploader waits for
it to finish, checks existence again, and recreates the chunk if necessary.
If reuse wins, this run keeps the object. New objects need no reuse row because
their last-modified time already protects them. Reuse rows are one small record
per reused hash and are removed when that chunk is collected. Standalone chunk
uploaders targeting a managed bucket must use `ChunkStore::with_gc_protection`.

A fixed-size Bloom filter holds chunk references, using 64 MiB by default and
seven probes per hash. False positives only keep extra chunks; saturation
reduces reclamation without permitting deletion of referenced data. Roots and
leaves are always traversed, even if their hashes appear in the filter. Memory
also includes one manifest root and leaf, one metadata page, and streaming
object listing pages for at most sixteen of the 256 prefixes. It does not grow
with the total number of chunk objects or live roots.

A store lease admits one collector per metadata namespace, using the same
complete-token and retained-sequence fencing as writer leases. It expires after
120 seconds and renews every 30 seconds. Collection stops on renewal failure
and cancels work before expiry, leaving 30 seconds for outstanding requests.
After a crash, the next collector can acquire the expired lease. Each attempt
records its start and, on completion or a recoverable error, its manifest and
object counts, candidate bytes, deleted bytes, and elapsed milliseconds.
Unfinished records identify interrupted attempts; their final counters are
unknown. Object deletion is not transactional with accounting, so an interrupted
run can reclaim bytes that its record does not report.

`swarmy gc --dry-run` performs the same leased mark and listing and records
candidate counts and bytes, while deleting nothing. `--json` emits the durable
run summary. The scheduler attempts collection after each configured interval,
waiting one full interval at startup and after every attempt. Competing
schedulers skip a busy lease and retry next interval.

One FoundationDB metadata namespace (`store_directory`) must pair with exactly
one object namespace (S3 endpoint, bucket, and prefix). Every service, CLI,
benchmark, and collector using that metadata must use the same object settings.
Distinct metadata namespaces must use disjoint object namespaces: separate
buckets or non-overlapping prefixes. An empty prefix owns the entire bucket;
its namespace cannot share that bucket with another metadata namespace.
Collection cannot discover references in another FoundationDB directory and
could delete its live data if object namespaces overlap.

`swarmy-config::Settings::object_store()` constructs every S3 client and applies
`object_store::prefix::PrefixStore` when `s3_prefix` is non-empty. The bucket
alone is used for S3 requests; object operations and listing queries apply the
prefix separately, and listings expose canonical relative names such as
`chunks/ab/hash`. `s3_prefix` defaults to empty and can be overridden with
`SWARMY_S3_PREFIX`. It must have no leading or trailing slash, empty segments,
`.` or `..` segments, or control characters. Invalid values are rejected,
never trimmed or normalized.

The legacy `s3_bucket = "bucket/run-prefix"` (or `SWARMY_S3_BUCKET`) remains
accepted with a deprecation warning. It selects bucket `bucket` and prefix
`run-prefix`, preserving existing object locations. To migrate, set
`s3_bucket = "bucket"` and `s3_prefix = "run-prefix"` (or their environment
overrides); no objects need moving. A legacy prefix combined with a non-empty
explicit prefix is rejected as ambiguous. Exported configuration includes
`SWARMY_S3_PREFIX`, including when empty, so child services use the same setting.

Configuration is under `[gc]`: `grace_seconds` defaults to 21600,
`interval_seconds` to 3600, and `filter_bytes` to 67108864. All are positive.
The corresponding environment overrides are `SWARMY_GC_GRACE_SECONDS`,
`SWARMY_GC_INTERVAL_SECONDS`, and `SWARMY_GC_FILTER_BYTES`.

The [local scale measurement](gc-benchmarks.md) covers 22,534 chunk objects,
dry-run accounting, deletion, lease renewal, elapsed time, and peak memory.

### 7.4 Risks to retire in slice 2

- Kernel module availability on stock cloud images. Verify on GCP, AWS, and
  Azure Ubuntu LTS images. Both `nbd` and `ublk_drv` are present locally.
- Cold read latency from object storage per chunk. Measure and tune chunk
  size and readahead. Target: interactive shell usable within seconds of cold
  boot on a warm-base-image node.
- Flush time after a heavy tool step such as a full dependency install.
  Measure. Consider a background flush that overlaps with the next step.
- Root privileges on nodes. `swarmyd` runs as root. Acceptable on our own VMs
  and in privileged pods.

### 7.5 Historical tool-boundary latency budget

The slice 2 tool-boundary implementation was measured against this target.
The persistent computer model moves publication to the snapshot loop and
explicit checkpoint; this table remains a baseline for publication latency.
It is a proposed p95 budget for the extra time between a tool finishing and
its durable manifest being acknowledged, including
freeze acquisition, publication, and thaw. It is not a claim that the current
implementation meets it. Collect repeated samples before claiming p95 compliance.

Count changed data as the total coverage of distinct dirty 256 KiB chunks,
including filesystem metadata. A 4 KiB write in each of 64 chunks counts as
16 MiB here. Assume new, nonzero content, a warm base cache and connection, a
healthy colocated FoundationDB, and at most two changed manifest leaves plus
the root, as in the installation workload. Background staging may reduce the
remaining work, but the unstaged case must also meet the budget.

| Chunk coverage changed by the step | AWS added latency | GCP added latency |
| --- | ---: | ---: |
| No dirty chunks | 250 ms | 250 ms |
| Up to 256 KiB | 500 ms | 3 s |
| Up to 1 MiB | 500 ms | 3 s |
| Up to 16 MiB | 750 ms | 4 s |
| Up to 64 MiB | 1.25 s | 8 s |
| Up to 256 MiB | 3 s | 25 s |
| Up to 1 GiB | 12 s | 90 s |

Reasoning comes from the [2026-09-15 instrumented cloud measurements](volume-benchmarks.md#2026-09-15-instrumented-release-flush-and-tool-boundary-budget).
A missing-object HEAD followed by a 256 KiB PUT costs about 50 ms on AWS and
526 ms on the tested GCP placement. With 32 concurrent chunk uploads, the
optimistic request-limited rates are 160 and 15.2 MiB/s. The measured AWS serial
installation flush achieves 4.0 MiB/s; multiplying by 32 gives 128 MiB/s before
contention or bandwidth limits. GCP achieved 0.63 MiB/s serially, or about
20 MiB/s under the same optimistic scaling assumption. Use lower planning rates
of 100 MiB/s on AWS and 12 MiB/s on GCP to leave room for local I/O, hashing,
and scheduling. Linear scaling is an assumption to test, not a measured
concurrent throughput result.

For nonempty changes, estimate a fixed allowance of 350 ms on AWS or 2 s on
GCP for manifest calls, database commit, and freeze/thaw, plus the larger of
`ceil(chunk_count / 32) * (HEAD + PUT)` and `changed_MiB / planning_MiB_per_second`.
The table rounds that estimate up. An extra changed manifest leaf needs another
GET and HEAD/PUT sequence if metadata publication remains serial; account for
that separately for scattered writes. A no-change publication needs no object
requests and gets only the database/freeze allowance.

The GCP VM used local SSD in `us-east1-b` after central-region capacity failures;
its bucket was in `us-central1`. The GCP column budgets this measured
cross-region placement. Remeasure colocated storage before setting a tighter
GCP deployment budget. The AWS bucket and VM were both in `us-east-1`.

A shorter freeze must not hide an equally long delay inside the tool. On AWS,
background uploading reduced frozen time from 140.24 s to 5.17 s, but increased
installation from 11.34 s to 181.53 s and accumulated 178.76 s of dirty-lock
waiting. On GCP, frozen time fell from 889.61 s to 22.42 s while installation
rose from 15.30 s to 1,233.97 s, with 1,256.26 s of lock waiting. Following tasks
must report installation time, total step-plus-flush time, upload amplification,
and lock waiting alongside frozen time. Background
mode must not regress total step-plus-flush time against background-off under
the same workload and placement. The instrumented serial uploader above is the
historical durability and latency baseline.

The [2026-09-15 generation-aware release measurements](volume-benchmarks.md#2026-09-15-generation-aware-background-uploads)
meet the budget for the measured roughly 560 MiB installation workload on both
clouds. Two samples per mode measured added server latency of 9.256–9.812 s
without background staging and 0.958–1.209 s with it on AWS, below the 12 s
up-to-1-GiB limit. Colocated GCP measured 8.571–8.881 s off and 1.063–1.496 s on,
below its 90 s limit. Mean frozen time with staging was 0.970 s on AWS and
1.089 s on GCP. Total step-plus-flush time improved in every paired sample:
means fell from 21.044 to 16.526 s on AWS and 27.488 to 21.649 s on GCP. This
meets the measured nonregression rule, with 1.094–1.101 and 1.114–1.130 upload
amplification respectively. These eight trials do not establish p95 compliance
or validate the smaller changed-data rows; the table remains a proposed budget.

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

### 8.2 Placement, leases, and lifecycle

One computer belongs to each agent and is shared by its main and worker
sessions. An isolated worker uses a clone with a separate computer identity.
A computer remains running across tool calls and inference waits, so shell
state on disk, installed packages, servers, and managed processes persist.
States are `Absent`, `Placing`, `Booting`, `Running`, and `Dead`; future
host-local memory pause is an optimization, not a durability mechanism.

FoundationDB has one placement record per agent: agent id, node id, epoch,
lease expiry, last epoch change reason (`initial`, `failure`, `eviction`, or `unstarted`),
and change time. The store API is `place`, `renew`, `release`, `take_over`,
`get_by_agent`, and `list_by_node`. Place requires absence and a registered
sandbox node with capacity. Registration already advertises the maximum as
`NodeCapacity.sandboxes`. Occupancy is a separate transactional counter, so a
heartbeat cannot reset it. Expired placements still reserve their slots until
takeover or release. Node listings include expired records and paginate by
agent id; schedulers can use them to find computers needing recovery.

The holder renews a live lease using its node id and epoch. Renewals preserve
the epoch and change metadata. Release checks that same authority, removes
the placement and node index, and returns capacity. The epoch counter survives
release. Place increments it, recording `initial` on the first grant and
`eviction` when rebuilding after release. Takeover checks the observed node
and epoch, requires expiry, and atomically transfers capacity and the node
index while increasing the epoch. It records `failure` if a node claimed the
old epoch for hosting, or `unstarted` if no node ever claimed it. Competing takeovers
cannot both win. Rebuilding on the same node also increases the epoch.

Every mutating transaction on an existing placement checks its epoch. Tool
routing carries that epoch through execution and completion, and publication
checks it with the volume writer lease. Reads alone grant no authority. The
node must stop tool execution and background activity before its lease runs
out if it cannot renew. A heartbeat timeout can trigger recovery checks but
cannot override an unexpired placement or writer lease. Leases require
bounded clock skew and renewal margins; a database fence rejects stale commits
but cannot undo a stale process's external side effects. Node shutdown must
stop those processes before releasing authority.

After thirty minutes with no tool activity or managed running processes,
checkpoint, stop the computer, detach its volume, and release placement.
Managed background processes count as activity and prevent idle eviction.
The next tool request places it again and cold boots from its latest published
manifest. Placement prefers nodes with the base image cached, then any node
with capacity. After failure, wait for lease expiry and take over with a new
epoch; all memory and running processes are lost.

Hosting claims and successful lease renewal times are stored transactionally in
`placement_hosting`, separate from the existing binary placement record and
its dispatch fences. The node claims before booting or executing tools, so an
interrupted boot conservatively counts as possible computer loss. Takeover
resets hosting state and retains the lost epoch's latest claim or renewal as
an estimated failure time. Existing placements without this metadata retain
failure behavior, with no failure estimate. This change requires upgrading
node writers along with the store users so all new hosting claims are recorded.

Rebuild messaging derives from the new epoch's reason and time. An initial
grant or takeover of an unstarted placement needs no restart notice. A failure
notice says recovery began, processes were lost, and files return to the latest
snapshot, including its
time and its age before recovery. The change time is labeled as the start of
recovery, since routing precedes sandbox boot and cannot establish when boot
finished or when the old computer failed. When available, the lost placement's
last claim or renewal time is explicitly labeled as an estimated failure time.
An eviction notice says the computer was stopped while idle and rebuilt from its final checkpoint. Deliver these
notices durably to every existing session, deduplicated per session and epoch, before
new tool results are folded. Never claim that successful tool output implies
that its disk changes survived. The placement record retains the latest change;
message delivery must persist each observed notice and its delivery cursor.

The hosting actor, worker routing, process tools, snapshot loop, and collector
now implement this contract. The persistent-computer acceptance procedure is
recorded in [the volume benchmarks](volume-benchmarks.md#2026-09-16-persistent-computers).
Recovery must wait for both placement and volume-writer authority: shortening
the placement lease alone does not remove the volume server's 60-second writer
lease wait. Report that wait separately from rehydration time.

Workers and nodes may use different placement lease durations. Validation found
that a shorter node duration could fail renewal after a longer worker grant.
Hosting renewals now preserve the later of the node's requested expiry and the
stored expiry, advancing the latter by one millisecond to satisfy the store's
strict increase check. Renewal timing and the cancellation margin respect the
remaining effective grant. Epoch and expiry fencing still apply to every
renewal, including after a grant changes while the computer is resident.

### 8.3 Guest agent

Static Rust binary baked into every base image, started as PID 1 or by init.
RPC over vsock in Firecracker, a unix socket bind mount in containers.
Operations: exec with streaming stdio and timeouts, read, write, stat, list,
glob, grep, sync and freeze, display screenshot, and a devtools proxy to the
in-sandbox browser. `bash` is an exec into the long-lived sandbox, not creation
of a fresh container per call. `process_start` launches a managed background
process and returns its id; `process_list` reports state, `process_log` reads
captured output, and `process_stop` terminates it. Process ids and logs belong
to a computer epoch; requests for an earlier epoch report that it restarted.
`checkpoint` syncs and publishes a fenced block-boundary manifest before
reporting success. Processes continue between ordinary calls, but their memory
is never included in a disk checkpoint.

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

### 11.1 Laptop plus development nodes

`swarmy remote` is an early deployment shape for the same service boundaries.
The default `--services laptop` configuration runs the CLI, scheduler, step
worker, and inference gateway on an unprivileged Linux laptop. One Ubuntu EC2
node runs FoundationDB, NATS, SeaweedFS, and privileged `swarmyd`. SSH forwards
all backing services, including FoundationDB at its advertised loopback port
4500. Direct access to the node's private backing-service addresses is not
required. The laptop cannot run another FoundationDB stack on that port.

`swarmy remote up NAME --services node` instead enables systemd services for
the scheduler, worker, and gateway on the first node. `dev up --remote NAME`
then verifies the tunnel and starts no local services. Choose this mode when
network latency dominates turn time or agents must progress while the laptop
is disconnected. Choose laptop services when editing those services locally
or keeping provider credentials on the laptop matters more than latency.
Node services keep internal database transactions and NATS handoffs near the
store; the client still commits messages and receives live events over SSH.

The fake provider requires no credential. A ChatGPT node gateway requires
`--copy-credential`, which prints a warning and sends the configured credential
file over SSH into a private file on the node. Without that acknowledgement,
provisioning refuses a ChatGPT node gateway before creating cloud resources.
Do not run concurrent gateways refreshing the same account on both hosts.
Agent tool processes and disks stay on the nodes in either configuration.

The worker batches its claim and replay page, and independent inference reads
share a read phase. Idle and waiting states need no lease or runnable-index
reads. Immutable session image pins are cached without caching missing rows.
Placement routes expire at the observed lease deadline and are invalidated on
step failure. A dispatch rejected after early release invalidates the route and
resolves it once more; dispatch and execution still check the database epoch.
These caches do not grant authority or replace the store's fencing checks.

Additional nodes join the first node's private backing-service endpoints and
provide more computer capacity. They do not replicate the backing services.
Checkpoints and session history therefore survive execution-process failure,
but not loss or teardown of the first node's backing data. Closing the laptop
stops control-plane progress in laptop-services mode.
Node services continue running; cloud machines remain allocated in either mode.

This path tests cloud provisioning, remote placement, and recovery before
slice 9. It does not satisfy the cloud-deploy goal: that slice still requires
managed orchestration, replicated stateful services, horizontal control-plane
scaling, and the unchanged channel scenario on both GKE and EKS. See the
[developer workflow](DEV.md#remote-node-workflow) for operation and credential
boundaries, and the dated remote-node run in [the benchmarks](volume-benchmarks.md)
for measured coverage and limitations.

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
**Build:** workspace, local stack of FoundationDB, NATS, and SeaweedFS, core
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
  runtime, so FoundationDB, NATS, and SeaweedFS run as plain processes
  started by a small script. SeaweedFS replaces MinIO as the local
  S3-compatible store because MinIO stopped publishing binaries in late
  2025; the services only ever speak the S3 protocol, so the choice of
  local server does not leak into the code.
