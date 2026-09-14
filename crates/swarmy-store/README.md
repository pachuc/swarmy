# swarmy-store

The store owns FoundationDB transactions for sessions, events, snapshots, leases,
runnable work, idempotency, and in-flight requests. Shared records live in
`swarmy-core`.

Call `swarmy_store::boot()` once before opening a store. Keep its guard until all
database handles and tasks have stopped. `Store::open` uses the `swarmy` directory
by default and accepts a custom directory path. `Store::with_subspace` accepts an
already allocated prefix for isolated tests.

Keys use FoundationDB tuples. Session and owner identifiers use their 16 ULID
bytes; request identifiers use their 32 hash bytes. The runnable index has 256
partitions, selected by the first byte of BLAKE3 over the session id. Priority is
ascending, followed by wake time and session id. Times are nested tuples of signed
seconds and nanoseconds, preserving ordering before and after the Unix epoch.

In addition to the design's keys, two secondary indexes support atomic updates:

- `("runnable_by_session", session_id)` stores the current runnable entry so a
  claim or reschedule can remove it without scanning a partition.
- `("lease_by_expiry", expires_at, session_id)` stores the lease so the scheduler
  can scan only expiries at or before its supplied time.

Create sessions in Idle or Runnable with an empty log. Runnable creation and state
transitions insert a default priority-zero entry; `insert_runnable` changes its
priority or wake time. Claims require Runnable and atomically remove that entry,
write the lease, and set Leased. Claims use `head_seq + 1` as the step sequence.
Use a fresh owner id for each worker incarnation. Renewal, release, and transitions
out of Leased compare the entire lease record and reject expired tokens. Renewal
returns a new token. The scheduler passes scan results to `reap_lease`, which
rechecks expiry and the token before returning the session to Runnable. A renewed
or replaced lease cannot be reaped using an old scan result.

Each API operation uses one logical FoundationDB transaction, retrying conflicts.
An unknown commit outcome is returned to the caller to reconcile against durable
state. Transactions have a 4.5-second timeout and a retry limit of 20. Append
mutations are bounded to 8 MiB. Session headers contain only fixed-size fields
and the snapshot sequence. Scans take a limit
of 1 through 64. Continue event scans with the last sequence, runnable scans with
the last entry, and expired-lease scans with the last session/lease pair. Runnable
scans include future wake times for the scheduler to evaluate. `list_sessions`
scans session keys in ascending id order and continues strictly after the last
session id. Each page includes hydrated snapshot pointers; pages are independent
views of the database.

Event, snapshot, and request values are versioned envelopes containing either versioned
inline bytes or a content-addressed blob pointer. Payloads over 80 KiB are uploaded
before the transaction begins and fetched after the read transaction completes.
Blob reads verify the content hash. A failed append can leave an unreferenced blob;
blob garbage collection is outside this task. Session headers and index records use
`swarmy_core::encode` directly. Fetching a session reads its header and snapshot
value in the same transaction and then resolves any blob pointer.

`MemoryBlobStore` is useful for tests. `ObjectBlobStore` adapts any
`object_store::ObjectStore`; `from_env` constructs S3 storage from
`SWARMY_S3_ENDPOINT`, `SWARMY_S3_ACCESS_KEY`, `SWARMY_S3_SECRET_KEY`,
`SWARMY_S3_BUCKET`, and `SWARMY_S3_REGION`, with HTTP allowed and path-style bucket
addressing.

The FoundationDB binding requires `libfdb_c` 7.3 and libclang at build time. Run the
integration tests with `scripts/dev-stack.sh start`, source `.dev/env`, then run
`cargo test --workspace --locked`. Tests print a skip message when the relevant
FoundationDB or S3 setting is absent. Each test uses a fresh ULID prefix and removes
its database keys and S3 objects after successful verification.
