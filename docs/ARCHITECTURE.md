# Architecture

This describes the shipped system, not a deployment promise. For planned work
see [ROADMAP.md](ROADMAP.md); for local and remote operation see [DEV.md](DEV.md)
and [REMOTE.md](REMOTE.md). Each section names its implementation entry points.

## Crate map and dependency direction

The workspace membership and shared dependencies are in [Cargo.toml](../Cargo.toml).
These are descriptive tiers, not strict acyclic boundaries: sandbox uses store
and volume; chat and devtools use the HTTP client; harness is shared by
production tools, gateway, and worker as well as tests. The individual manifests in `crates/*/Cargo.toml` are the
source of truth for edges; this is a layering rule, not a claim that every
crate in a tier depends on every lower tier.

### Libraries

- **swarmy-core** defines session, agent, event, request, lease, and placement
  types shared across processes ([source](../crates/swarmy-core/src/lib.rs)).
- **swarmy-api-types** owns the JSON request and response contracts rather
  than database access ([source](../crates/swarmy-api-types/src/lib.rs)).
- **swarmy-config** parses settings, including remote profile and object-store
  configuration ([source](../crates/swarmy-config/src/lib.rs)).
- **swarmy-version** stamps binary compatibility at build time
  ([source](../crates/swarmy-version/src/lib.rs)).
- **swarmy-catalog** contains generated model metadata and model lookup
  ([source](../crates/swarmy-catalog/src/lib.rs)).
- **swarmy-llm** implements the wire protocols and provider selection used
  for inference ([source](../crates/swarmy-llm/src/lib.rs)).
- **swarmy-tools** defines and executes sandbox-facing tools
  ([source](../crates/swarmy-tools/src/lib.rs)).
- **swarmy-sandbox** starts and manages runc containers through the sandbox
  interface and uses store and volume ([source](../crates/swarmy-sandbox/src/lib.rs)).
- **swarmy-chat** renders conversation events using swarmy-client for interactive clients
  ([source](../crates/swarmy-chat/src/lib.rs)).
- **swarmy-image** builds and registers images from recipes
  ([source](../crates/swarmy-image/src/lib.rs)).
- **swarmy-devtools** uses swarmy-client for standalone provider login and credential import
  ([source](../crates/swarmy-devtools/src/main.rs)).

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
  ([source](../crates/swarmy-scheduler/src/main.rs)).
- **swarmy-worker** drives a leased session step and submits tool or inference
  work ([source](../crates/swarmy-worker/src/main.rs)).
- **swarmy-gateway** handles inference requests and durable replies
  ([source](../crates/swarmy-gateway/src/main.rs)).
- **swarmy-api** exposes HTTP JSON mutations and cursor-based event streams
  ([source](../crates/swarmy-api/src/main.rs)).
- **swarmyd** hosts placements, volumes and sandbox commands on a node
  ([source](../crates/swarmyd/src/main.rs)).

### Client and cloud

- **swarmy-client** calls the HTTP API without linking FoundationDB
  ([source](../crates/swarmy-client/src/lib.rs)).
- **swarmy-cli** provides `swarmy` commands over the client and handles local
  development commands ([source](../crates/swarmy-cli/src/main.rs)).
- **swarmy-cloud** implements the opt-in EC2 remote provisioning feature;
  it is not a server dependency ([source](../crates/swarmy-cloud/src/lib.rs)).

### Shared harness and test leaves

- **swarmy-harness** provides fixtures used as normal dependencies by tools,
  gateway, and worker ([source](../crates/swarmy-harness/src/lib.rs),
  [manifests](../crates/swarmy-tools/Cargo.toml)).
- **swarmy-e2e** owns the real-binary integration suites
  ([manifest](../crates/swarmy-e2e/Cargo.toml)).
- **swarmy-chaos** tests crashes and restart continuity
  ([manifest](../crates/swarmy-chaos/Cargo.toml)).

Dependency direction is an intention rather than a CI-enforced rule: most
services consume storage and transport, while clients normally consume the
HTTP client. Cloud provisioning is an opt-in CLI feature. The actual edges are
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
[blob.rs](../crates/swarmy-store/src/blob.rs)). Treat family names as internal
schema, not an API. Postcard encodes structs positionally: append stored fields
only at the end via [swarmy_core::trailing](../crates/swarmy-core/src/encoding.rs),
with fixed-byte compatibility tests in
[store/lib.rs](../crates/swarmy-store/src/lib.rs). When a format changes or is retired,
migrate and clear old rows rather than refusing to start; record one-way
changes in [api-breaks.txt](api-breaks.txt) (see
[store migration](../crates/swarmy-store/src/lib.rs)).

- **Sessions and events.** `session` holds session state; `event` is indexed
  by `(session, seq)` and read as an append-only log. `turn`, `request_turn`,
  `runnable`, `runnable_by_session`, `inflight`, and `session_by_agent` provide
  deduplication and scheduling indexes. Some `session_*` rows are legacy
  hydration data rather than current state
  ([keys.rs](../crates/swarmy-store/src/keys.rs),
  [turns.rs](../crates/swarmy-store/src/turns.rs)).
- **Agents and computers.** `agent`, `agent_by_name`, `volume`, `manifest`,
  `snapshot`, `image`, `image_display`, `computer_notice`, and `session_chain` relate durable identity,
  disk state and conversations. Ephemeral sessions own disposable computers;
  named agents share a persistent computer
  ([agents.rs](../crates/swarmy-store/src/agents.rs),
  [computers.rs](../crates/swarmy-store/src/computers.rs)).
- **Leases and placements.** `lease`, `lease_by_expiry`, `node`,
  `placement`, `placement_epoch`, `placement_by_node`, `placement_hosting`,
  `volume_placement`, and `tool_placement` track ownership and where work
  runs. A placement epoch fences stale node operations
  ([leases.rs](../crates/swarmy-store/src/leases.rs),
  [placements.rs](../crates/swarmy-store/src/placements.rs)).
- **Inference and routes.** `route`, `credential_entry`,
  `credential_entry_lease`, `inference_request`, `inference_claim`,
  `inference_result`, `inference_wait`, and `inference_breaker` connect
  configured routing, fenced credentials and retryable waits
  ([routes.rs](../crates/swarmy-store/src/routes.rs),
  [inference.rs](../crates/swarmy-store/src/inference.rs),
  [inference_wait.rs](../crates/swarmy-store/src/inference_wait.rs)).
- **Metering.** `usage_record`, `usage_record_by_time`, `metering_hour`,
  `turn_metrics`, `turn_tool`, and `turn_inference` hold per-request usage,
  hourly aggregates and step timings; dimensions and hour-bucket encoding
  are defined in [metering.rs](../crates/swarmy-store/src/metering.rs) and
  [metrics.rs](../crates/swarmy-store/src/metrics.rs).

- **Timers, collection and tool completion.** `timer`, `timer_active`, `timer_due`, `timer_origin`,
  `gc_run`, `gc_lease`, `gc_deleting`, `tool_job`, `tool_done`, and `api_idempotency`
  track scheduled wakes, collection progress, durable tool execution, and
  deduplicated API writes ([keys.rs](../crates/swarmy-store/src/keys.rs),
  [timers.rs](../crates/swarmy-store/src/timers.rs),
  [tools.rs](../crates/swarmy-store/src/tools.rs),
  [api_idempotency.rs](../crates/swarmy-store/src/api_idempotency.rs)).

Conversation compaction summarizes older history into a successor session; see
[worker/summarize.rs](../crates/swarmy-worker/src/worker/summarize.rs).

## What runs where

A local checkout starts FoundationDB, NATS and SeaweedFS with
[dev-stack.sh](../scripts/dev-stack.sh). The control node hosts these backing
services and scheduler, worker, gateway and API processes; the sandbox node
runs `swarmyd`, runc containers and the volume/NBD path
([remote-provision.sh](../scripts/remote-provision.sh),
[swarmyd main](../crates/swarmyd/src/main.rs)). One machine can perform both
roles. A remote client uses the HTTP API and SSH tunnels; EC2 launch and
upgrade are CLI operations, not a Kubernetes controller
([REMOTE.md](REMOTE.md), [cloud source](../crates/swarmy-cloud/src/lib.rs)).

The worker advances events and yields tool execution to placed sandboxes;
the gateway resolves inference separately. The scheduler uses durable runnable
indexes rather than trusting a NATS acknowledgment
([worker source](../crates/swarmy-worker/src/worker.rs),
[scheduler source](../crates/swarmy-scheduler/src/main.rs),
[gateway source](../crates/swarmy-gateway/src/main.rs)). Volumes use
content-addressed chunks and manifests, with dirty periodic snapshots
([volume source](../crates/swarmy-volume/src/lib.rs)).

## Operational invariants

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
  [scheduler source](../crates/swarmy-scheduler/src/main.rs),
  [chaos tests](../crates/swarmy-chaos/tests)).
