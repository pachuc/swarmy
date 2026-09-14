# swarmy-worker

The worker consumes runnable nudges, claims one session at a time, loads its
harness snapshot and paginated event tail, and runs worker-local tools under a
renewing lease. It holds no session state between deliveries. Several processes
can share the same partitions and durable consumers. A rejected claim is
acknowledged without processing the session.

The tool registry includes `get_time`. All requests in a tool batch are appended
atomically before execution. Each result is appended separately; replay executes
only calls without a completion. Unknown tools produce `ToolResult::Error`.
Folded tool results are persisted as a tool-role message before the next request.
There is no worker-session flag yet, so every finished turn becomes `Idle`.

## Running

Start the development stack and source `.dev/env`, then build workspace binaries:

```sh
scripts/dev-stack.sh start
source .dev/env
cargo build --workspace --bins --locked
SWARMY_PROVIDER=fake target/debug/swarmy-worker
```

Run the scheduler and gateway against the same store directory and bus prefix.
The fake gateway also needs its script and call-log settings; see
[the gateway README](../swarmy-gateway/README.md).

| Variable | Default | Meaning |
| --- | --- | --- |
| `SWARMY_FDB_CLUSTER_FILE` | required | FoundationDB cluster file |
| `SWARMY_NATS_URL` | required | NATS connection URL |
| `SWARMY_S3_*` | required | The five blob-store settings from `docs/DEV.md` |
| `SWARMY_STORE_DIRECTORY` | `swarmy` | Slash-separated database directory |
| `SWARMY_BUS_PREFIX` | absent | Isolated subject and stream prefix |
| `SWARMY_WORKER_PARTITIONS` | `0-255` | Comma-separated numbers and inclusive ranges, like the scheduler |
| `SWARMY_PROVIDER` | `fake` | `fake` or `chatgpt`; match the gateway |
| `SWARMY_MODEL` | `gpt-5` | Harness model name |
| `SWARMY_REASONING_EFFORT` | `medium` | `none`, `minimal`, `low`, `medium`, `high`, or `xhigh` |
| `SWARMY_SYSTEM_PROMPT` | `You are a helpful assistant. Use tools when needed.` | Literal system prompt |
| `SWARMY_WORKER_LEASE_MS` | `30000` | Lease duration, at least 30 ms |
| `SWARMY_WORKER_RECOVERY_INTERVAL_MS` | `5000` | Inflight scan interval, at least 30 ms |
| `SWARMY_BUS_ACK_WAIT_MS` | `30000` | Match other consumers of the same routes |
| `SWARMY_BUS_MAX_DELIVER` | `5` | Match other consumers of the same routes |
| `RUST_LOG` | `info` | Tracing filter |

While processing, the worker renews its lease at one third of its duration and
extends the nudge's acknowledgement deadline. If the bus deadline is shorter,
heartbeats run more often. Renewal failure cancels the running step. Store writes
for events, request inputs, and inflight records check the live lease token.
Claim logs include session ID, owner ID, and starting sequence for contention
checks. The process boots FoundationDB once, before creating its runtime.

## Submission and recovery

For a new inference request the worker performs these operations in order:

1. Compute `step = head_seq + 1` and `RequestId::for_step(session_id, step)`.
   Save the exact versioned `InferenceJob` with `Store::put_inference_input`
   under the lease. Large inputs use the store's content-addressed blob path.
2. Append `InferenceRequested` with that sequence and request ID.
3. Write the corresponding `InflightRecord`, fenced by the lease.
4. Use `Store::set_state` to enter `WaitingInference`. This atomically removes
   the lease and keeps the session out of the runnable index.
5. Publish the saved job to `WorkQueue::Inference` for the configured provider.

A crash before the request event leaves no submitted request. The scheduler
reaps the lease, and a new worker can save the input and append the event. A
crash after the event leaves the exact input available for replay; the replacement
worker reuses the event and request ID, writes inflight, and finishes submission.
It never appends a second request for that step.

A crash after inflight but before state change still leaves a leased session for
the scheduler to reap. A crash after state change but before queue publication
leaves an inflight record. Every worker scans those records at startup and
periodically, in pages, and republishes jobs whose sessions are waiting for
inference in its configured partitions. Repeated publications are safe because
the gateway claims requests and records completion idempotently. Keep the provider
class consistent across workers sharing a deployment. Saved prompts retain their
original model settings even after a worker restart.

A process interrupted during a tool batch retries only incomplete calls. Local
tools must tolerate execution again when a process dies before recording the
result; `get_time` may return a newer timestamp. Completed tool calls and folded
messages are retained. A snapshot is written at turn end using the core versioned
encoding and the blob store, with its sequence in `Store::write_snapshot`.
Snapshots are derived from an immutable bounded log prefix. An orphaned snapshot
or saved input can be collected by future blob garbage collection.

Every worker-appended event is published on `LiveFeed::SessionEvents`. On each
claim, the worker also publishes the loaded tail, which includes user and gateway
events and retries publications interrupted by a worker crash. This live feed is
ephemeral and can contain duplicates. Observers deduplicate by session/sequence
and use the durable log to fill gaps after disconnection. It is not a durable
subscription or an atomic database/NATS transaction.

## Tests and kill points

`SWARMY_WORKER_KILL_POINT` exits the worker with status 137 at `after_claim`,
`after_request_event`, or `before_release`. `after_release` additionally tests the
waiting-inference publication gap. An instrumented process exits at its first
matching point; restart it without the variable to resume processing.

`cargo test --workspace --locked` builds the service binaries used by the
integration tests. For a package-only run, first use
`cargo build --workspace --bins --locked`, then
`cargo test -p swarmy-worker --locked`. Tests locate scheduler and gateway binaries
beside Cargo's `CARGO_BIN_EXE_swarmy-worker` executable. Each test uses a fresh ULID
for its store directory and bus prefix, kills its child processes, and removes
its snapshots, database directory, and NATS streams. Tests skip with an explanation when
FoundationDB, NATS, or S3 connection settings are absent.

The integration suite covers a scripted clock-tool turn, every logged event on
the live subject, SIGKILL/restart between turns with snapshot replay, unknown-tool
errors, all four submission kill points with the real scheduler reaper, and two
workers processing many duplicate nudges with unique lease-owner records per
step. A slow-tool test resumes a partial batch, observes repeated lease renewals beyond
the original expiry, and verifies completed calls are not executed again. Store
tests cover fencing and inflight scan pagination.
