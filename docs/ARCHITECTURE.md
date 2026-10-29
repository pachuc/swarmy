# Architecture

This describes the shipped system, not a deployment promise. For planned work
see [ROADMAP.md](ROADMAP.md); for operating swarms see [swarms.md](swarms.md)
and for checkout development see [DEV.md](DEV.md). Each section names its implementation entry points.

## Crate map and dependency direction

The workspace holds thirty crates (`members = ["crates/*"]` in
[Cargo.toml](../Cargo.toml)). These are descriptive tiers, not strict acyclic
boundaries: sandbox uses store and volume; chat and devtools use the HTTP
client. The individual manifests in `crates/*/Cargo.toml` are the
source of truth for edges; this is a layering rule, not a claim that every
crate in a tier depends on every lower tier.

### Libraries

- **swarmy-core-bin** dispatches `swarmy-core` to the scheduler, worker, gateway, API, node, or volume role ([source](../crates/swarmy-core-bin/src/main.rs)).
- **swarmy-core** defines session, agent, event, request, lease, and placement
  types shared across processes ([source](../crates/swarmy-core/src/lib.rs)).
- **swarmy-api-types** owns the JSON request and response contracts rather
  than database access ([source](../crates/swarmy-api-types/src/lib.rs)).
- **swarmy-config** parses settings, the swarm registry and target resolution, and object-store configuration, including the client-side cloud credential store ([source](../crates/swarmy-config/src/lib.rs)).
- **swarmy-version** stamps binary compatibility at build time
  ([source](../crates/swarmy-version/src/lib.rs)).
- **swarmy-aws** builds the one HTTPS client (hyper 1, rustls on ring) that every AWS SDK configuration uses ([source](../crates/swarmy-aws/src/lib.rs)).
- **swarmy-catalog** contains generated model metadata and model lookup
  ([source](../crates/swarmy-catalog/src/lib.rs)).
- **swarmy-llm** implements the wire protocols and provider selection used
  for inference ([source](../crates/swarmy-llm/src/lib.rs)).
- **swarmy-tools** defines and executes sandbox-facing tools
  ([source](../crates/swarmy-tools/src/lib.rs)).
- **swarmy-harness** is the production conversation state machine, not a test
  fixture: step selection from snapshots, prompt assembly, and the tool
  registry used by the worker, the gateway, and tools (integration suites use
  it too) ([source](../crates/swarmy-harness/src/lib.rs)).
- **swarmy-sandbox** starts and manages runc containers through the sandbox
  interface and uses store and volume ([source](../crates/swarmy-sandbox/src/lib.rs)).
- **swarmy-chat** renders conversation events using swarmy-client for interactive clients
  ([source](../crates/swarmy-chat/src/lib.rs)).
- **swarmy-image** builds and registers images from recipes
  ([source](../crates/swarmy-image/src/lib.rs)).

### Storage and transport

- **swarmy-store** implements FoundationDB transactions, indexes, lease checks,
  object-store metadata and metering ([source](../crates/swarmy-store/src/lib.rs),
  [keys](../crates/swarmy-store/src/keys.rs)).
- **swarmy-bus** publishes JetStream work nudges and live events; durable
  ownership remains in the store ([source](../crates/swarmy-bus/src/lib.rs)).
- **swarmy-volume** manages chunked disk manifests, snapshots and the NBD
  server ([source](../crates/swarmy-volume/src/lib.rs)).

### Services

- **swarmy-scheduler** scans runnable sessions and dispatches work
  ([source](../crates/swarmy-scheduler/src/role.rs)).
- **swarmy-worker** drives a leased session step and submits tool or inference
  work ([source](../crates/swarmy-worker/src/role.rs)).
- **swarmy-gateway** handles inference requests and durable replies
  ([source](../crates/swarmy-gateway/src/role.rs)).
- **swarmy-api** exposes HTTP JSON mutations and cursor-based event streams
  ([source](../crates/swarmy-api/src/role.rs)).
- **swarmyd** hosts placements, volumes and sandbox commands on a node
  ([source](../crates/swarmyd/src/role.rs)).

### Client and cloud

- **swarmy-client** calls the HTTP API without linking FoundationDB
  ([source](../crates/swarmy-client/src/lib.rs)).
- **swarmy-cli** provides `swarmy` commands over the client and handles local
  development commands ([source](../crates/swarmy-cli/src/main.rs)).
- **swarmy-devtools** is a binary for standalone provider login and credential
  import over the HTTP client
  ([source](../crates/swarmy-devtools/src/main.rs)).
- **swarmy-cloud** provisions SSH-reachable hosts as swarms, upgrades them, opens per-swarm tunnels, and, with the opt-in remote feature, manages a static-key bucket; the provisioner is its only server-side user
  ([source](../crates/swarmy-cloud/src/lib.rs)).
- **swarmy-provisioner** is the primary-only service that reconciles elastic groups with OpenTofu and the machine ledger ([source](../crates/swarmy-provisioner/src/reconcile.rs)).
- **swarmy-infra** fetches a pinned, checksum-verified OpenTofu release, runs it in a per-swarm working directory with cloud credentials only in the environment, and owns the module output contract and the verified-download helper the swarmy release cache shares ([source](../crates/swarmy-infra/src/lib.rs)), and carries the OpenTofu machine-group modules from infra/modules ([modules](../crates/swarmy-infra/src/modules.rs)).

Services share one bootstrap from `swarmy-config`: `Settings::load` for
configuration, `init_tracing` for stderr logging, `Store::open_store` for the
database handle, and `shutdown_signal` (SIGINT or SIGTERM), which the API, worker, scheduler,
gateway, and swarmyd all wait on to shut down cleanly; the four
control-plane services flush queued turn metrics before exit
([config](../crates/swarmy-config/src/lib.rs),
[store](../crates/swarmy-store/src/lib.rs)).

### Test support and test leaves

- **swarmy-testkit** holds the helpers every integration suite shares: the
  dev-stack gate, a cleanup guard, the `eventually` poll helper, a
  fake-provider script builder, sibling-binary lookup, the image fixture,
  and the docs command walker
  ([source](../crates/swarmy-testkit/src/lib.rs)).
- **swarmy-e2e** owns the real-binary integration suites
  ([manifest](../crates/swarmy-e2e/Cargo.toml)).
- **swarmy-chaos** is a binary that tests crashes, restart continuity, and rate-limit saturation, and runs the coding and desktop agent proofs
  ([manifest](../crates/swarmy-chaos/Cargo.toml)).

Dependency direction is an intention rather than a CI-enforced rule: most
services consume storage and transport, while clients normally consume the
HTTP client. Cloud provisioning is an opt-in CLI feature; swarmy-cloud and swarmy-infra stay out of swarmy-core, and swarmy-provisioner is the only server-side binary that links them. The actual edges are
in the [crate manifests](../crates/swarmy-cli/Cargo.toml) and
[workspace manifest](../Cargo.toml).

## Durable data model

The canonical key registry is [keys.rs](../crates/swarmy-store/src/keys.rs).
Store roots are allocated as FoundationDB directory subspaces
([lib.rs](../crates/swarmy-store/src/lib.rs)); family names and tuple packing
come from `Keys`. Identifiers in keys are binary ULID bytes, not printable ULID
strings; event sequence and epoch components are tuple integers. Stored values
use postcard serialization, with large data redirected to object storage
([lib.rs](../crates/swarmy-store/src/lib.rs),
[blob.rs](../crates/swarmy-store/src/blob.rs)); bucket namespaces and
migration rules live in [objects.rs](../crates/swarmy-store/src/objects.rs).
Treat family names as internal
schema, not an API. Postcard encodes structs positionally: append stored fields
only at the end, with fixed-byte tests for stored formats. Format changes
are one-way breaks: wipe the development store before deployment and record
them in [api-breaks.txt](api-breaks.txt).

- **Sessions and events.** `session` holds versioned session state
  ([session.rs](../crates/swarmy-store/src/session.rs)); `event` is indexed
  by `(session, seq)` and read as an append-only log. `turn`, `request_turn`,
  `runnable`, `runnable_by_session`, `inflight`, `session_by_agent`, and the
  `queued_message` families provide deduplication, scheduling, and
  busy-session queueing; `interrupt` fences operator interruption on the
  session state and log head, and `plans` commits `update_plan` tool results
  under the same head fence. The session header owns session-local state
  ([keys.rs](../crates/swarmy-store/src/keys.rs),
  [turns.rs](../crates/swarmy-store/src/turns.rs),
  [queued.rs](../crates/swarmy-store/src/queued.rs),
  [interrupt.rs](../crates/swarmy-store/src/interrupt.rs),
  [plans.rs](../crates/swarmy-store/src/plans.rs)).
- **Agents, computers, and volumes.** `agent`, `agent_by_name`, `volume`, `manifest`,
  `snapshot`, `image`, `image_display`, `computer_notice`, and `session_chain` relate durable identity,
  disk state and conversations; session image pins live in the versioned
  session record, and volume metadata stays inline so cloning and fencing
  never need object storage. Ephemeral sessions own disposable computers;
  named agents share a persistent computer
  ([agents.rs](../crates/swarmy-store/src/agents.rs),
  [computers.rs](../crates/swarmy-store/src/computers.rs),
  [session_images.rs](../crates/swarmy-store/src/session_images.rs),
  [volumes.rs](../crates/swarmy-store/src/volumes.rs)).
- **Leases, placements, and nodes.** `lease`, `lease_by_expiry`, `node`,
  `placement`, `placement_epoch`, `placement_by_node`, `placement_hosting`,
  `placement_next_reason`, `volume_placement`, and `tool_placement` track
  ownership and where work runs. Node records carry advertised capacity, GPU
  cards, and heartbeats; placements record the GPU mode admitted. Service
  heartbeats stay advisory, tool routing fences durable dispatch across
  worker replicas, and tool claims on agent volumes never publish disk state.
  Placement filters by GPU requirement before preferring a node holding the
  agent's scratch data, but after a placement lease expires the previous
  host is tried last, even if it holds that scratch. Capacity admission
  remains transactional, and a placement epoch fences stale node operations.
  A GPU requirement change marks the hosting row for release at the next
  node renewal; the next placement records `RequirementChange`
  ([placement.rs](../crates/swarmy-worker/src/placement.rs),
  [leases.rs](../crates/swarmy-store/src/leases.rs),
  [placements.rs](../crates/swarmy-store/src/placements.rs),
  [nodes.rs](../crates/swarmy-store/src/nodes.rs),
  [services.rs](../crates/swarmy-store/src/services.rs),
  [tool_routing.rs](../crates/swarmy-store/src/tool_routing.rs),
  [placed_tools.rs](../crates/swarmy-store/src/placed_tools.rs)).
- **Elastic groups.** `elastic_group` holds the swarm-owned definition, desired count, and reconciliation generations; `elastic_creations` holds the creation safety window, while `provisioner_lease` and `provisioner_sequence` fence the single primary provisioner ([elastic.rs](../crates/swarmy-store/src/elastic.rs)).
- **Inference, routes, and credentials.** `route`, `credential_entry`,
  `credential_entry_lease`, `inference_request`, `inference_claim`,
  `inference_result`, `inference_breaker`, and the shared `session_wait` connect
  named failover chains over auth entries, fenced refresh of encrypted
  credential records (including the machine-provider kind used by OpenTofu), provider availability advertisements, and retryable
  waits
  ([routes.rs](../crates/swarmy-store/src/routes.rs),
  [credentials.rs](../crates/swarmy-store/src/credentials.rs),
  [selection.rs](../crates/swarmy-store/src/selection.rs),
  [inference.rs](../crates/swarmy-store/src/inference.rs),
  [breakers.rs](../crates/swarmy-store/src/breakers.rs),
  [session_wait.rs](../crates/swarmy-store/src/session_wait.rs)).
- **Metering.** `usage_record`, `usage_record_by_time`, `metering_hour`,
  `turn_metrics`, `turn_tool`, and `turn_inference` hold per-request usage,
  hourly aggregates and step timings; per-completion attribution and quota
  observation per auth entry sit alongside. Dimensions and hour-bucket
  encoding are defined in [metering.rs](../crates/swarmy-store/src/metering.rs)
  and [usage.rs](../crates/swarmy-store/src/usage.rs), quota observation in
  [quota.rs](../crates/swarmy-store/src/quota.rs), and the turn-metric
  encoding and model in
  [metrics_codec.rs](../crates/swarmy-store/src/metrics_codec.rs) and
  [metrics_model.rs](../crates/swarmy-store/src/metrics_model.rs). The writer
  ([metrics.rs](../crates/swarmy-store/src/metrics.rs)) is bounded and
  non-blocking: observation calls enqueue patches on a queue drained by
  turn, a full queue drops the job with a warning, and flushing waits only
  for jobs that were queued.

- **Timers, collection and tool completion.** `timer`, `timer_active`, `timer_due`, `timer_origin`,
  `gc_run`, `gc_lease`, `gc_deleting`, `tool_job`, `tool_done`, and `api_idempotency`
  track scheduled wakes, collector leases, durable tool execution, and
  deduplicated API writes ([keys.rs](../crates/swarmy-store/src/keys.rs),
  [timers.rs](../crates/swarmy-store/src/timers.rs),
  [gpu_tools.rs](../crates/swarmy-store/src/gpu_tools.rs) for GPU requirement changes by agent tools,
  [tools.rs](../crates/swarmy-store/src/tools.rs),
  [gc.rs](../crates/swarmy-store/src/gc.rs),
  [api_idempotency.rs](../crates/swarmy-store/src/api_idempotency.rs)).

### Context compaction

The worker compacts a named conversation when total context tokens exceed the
model window minus a 16,384-token reserve, or after a recoverable overflow or
early length stop. It serializes the head of the history into a Pi-style
Markdown checkpoint, keeps a recent tail of about 20,000 tokens, and creates
a successor session whose first user message contains that checkpoint. If the
cut splits a turn, a second checkpoint summarizes the turn prefix separately.
An unsuccessful checkpoint leaves the original session available. A recovery
attempt is limited to one compact-and-retry per turn; a second failure is
reported as a session notice, where Pi emits a compaction event. Swarmy retains
its successor-session rollover instead of Pi's in-session compaction.

The prompts and cut handling come from Pi's `compaction.ts` and `utils.ts`
in its coding-agent package;
see [worker/summarize.rs](../crates/swarmy-worker/src/worker/summarize.rs) and
[harness prompts](../crates/swarmy-harness/src/lib.rs) for the implementation.

## What runs where

A local checkout starts FoundationDB, NATS and SeaweedFS with
[dev-stack.sh](../scripts/dev-stack.sh). The control node hosts these backing
services and scheduler, worker, gateway and API processes; the sandbox node
runs `swarmyd`, runc containers and the volume/NBD path
([remote-provision.sh](../scripts/remote-provision.sh),
[swarmyd main](../crates/swarmyd/src/role.rs)). One machine can perform both
roles. Groups supply machines: local, existing hosts, or OpenTofu modules
run by `swarmy-infra`. Placement assigns stack, services and node roles.
Existing hosts and OpenTofu machines are provisioned over the same SSH path;
a local group runs the stack and services as processes on this machine.
Clients reach a remote API through an on-demand per-swarm SSH tunnel
([swarms.md](swarms.md), [cloud source](../crates/swarmy-cloud/src/lib.rs)).

The worker advances events and yields tool execution to placed sandboxes;
the gateway resolves inference separately. The scheduler uses durable runnable
indexes rather than trusting a NATS acknowledgment
([worker source](../crates/swarmy-worker/src/worker.rs),
[scheduler source](../crates/swarmy-scheduler/src/role.rs),
[gateway source](../crates/swarmy-gateway/src/role.rs)). Volumes use
content-addressed chunks and manifests, with dirty periodic snapshots
([volume source](../crates/swarmy-volume/src/lib.rs)).

### Desktop sessions and image inputs

An image's stored display flag controls whether the worker adds the browser and
screen tool schemas and display prompt to a named agent's inference request.
The node runs the sandbox-local Python browser helper and rejects display tools
on other images. Screenshot bytes become tool blobs referenced by the completed
event's `image_object_key`; the worker adds image parts for models with image
input, or an omission note for text-only models. The authenticated tool-images
endpoint lets a client download only screenshots referenced by a session's
completed tool calls. See [graphics.md](graphics.md) for the image, tools and
agent-level proofs.

## Operational invariants

- **GPU admission is transactional.** Shared and dedicated rules are checked
  from the node's placement rows in the same transaction that reserves capacity,
  so two dedicated placements cannot both win a node
  ([placements.rs](../crates/swarmy-store/src/placements.rs)).

- **Rate limits are retryable waits, not admission control.** Swarmy keeps
  no token buckets in front of providers: subscription windows, spend-tiered
  key limits, and cloud quotas are opaque and shift, so a local model of them
  drifts from the provider's. A 429 or outage is instead a retryable failure
  the system is durable to. The protocol client returns it at once, the
  gateway records it and opens that auth entry's circuit breaker until the
  provider's Retry-After or a backoff, and the worker fails over along the
  route or parks the session in `session_wait` with a wake time and releases
  its lease. While the breaker is open neither the scheduler nor the gateway
  sends work to the entry, so hundreds of waiting sessions cost the provider
  nothing; one claimed probe tests it afterwards. Remaining-quota headers
  feed the quota views as observed values
  ([session_wait.rs](../crates/swarmy-store/src/session_wait.rs),
  [attempt.rs](../crates/swarmy-gateway/src/attempt.rs),
  [saturation scenario](../crates/swarmy-chaos/src/saturation.rs)).

- **No room is a wait, not a retry.** When placement finds no node that can
  host a computer, the worker parks the session in `session_wait` with a
  capacity kind, releases its lease and acknowledges the work. It tries again
  every 30 seconds, or at once when capacity waits are woken; the maximum
  session wait turns pending calls into failed calls
  ([session_wait.rs](../crates/swarmy-store/src/session_wait.rs),
  [tools.rs](../crates/swarmy-worker/src/worker/tools.rs)).

- **Fence every owner.** Session leases, inference claims, placement epochs
  and volume leases are checked in the transaction that changes durable state;
  expiry alone does not authorize a stale owner
  ([leases.rs](../crates/swarmy-store/src/leases.rs),
  [inference.rs](../crates/swarmy-store/src/inference.rs),
  [placements.rs](../crates/swarmy-store/src/placements.rs)).
- **Deduplicate mutations.** Request IDs and API idempotency rows prevent
  retries from adding a second logical turn; tool completion rows prevent
  replaying completed calls ([keys.rs](../crates/swarmy-store/src/keys.rs),
  [api_idempotency.rs](../crates/swarmy-store/src/api_idempotency.rs),
  [tools.rs](../crates/swarmy-store/src/tools.rs)).
- **Assume at-least-once transport.** NATS can duplicate or lose a nudge;
  runnable scans and the event log recover from missed delivery. Acknowledge
  transport only after durable state is committed
  ([bus source](../crates/swarmy-bus/src/lib.rs),
  [scheduler source](../crates/swarmy-scheduler/src/role.rs),
  [chaos tests](../crates/swarmy-chaos/tests)).
