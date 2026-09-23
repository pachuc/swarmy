# Control plane API v1

All client operations use HTTP JSON under `/v1`. The `swarmy-api-types` crate is
its wire contract; `docs/openapi.json` is generated from that crate. IDs are
opaque strings. An agent owns a persistent computer and can have multiple
sessions. A session is an ordered append-only log; a turn groups inference and
tool activity, and messages belong to sessions. Images select computer roots,
models belong to providers, and credentials expose only metadata and validity.
Nodes and service health report availability. Credential secrets are accepted
only in create and update requests, never returned by read operations.

## Events and replay

`Event` has a `log_id`, a `sequence`, and a tagged `payload`. `LogId` is a
namespace-tagged value (`{"kind":"session","id":"..."}`); the `channel`
namespace is reserved for future channel logs. Sequences start at one, are
contiguous within a log, and are independent between logs. The pair `(log_id,
sequence)` is the durable cursor. Clients store the last processed cursor per
log; a subscription supplies a set of these cursors and receives events
strictly *after* each sequence, including after reconnect. Sequence zero
requests replay from the beginning. The server reads durable logs for replay,
then follows live changes without a gap. Duplicates across reconnections are
possible; consumers deduplicate by the cursor pair. There is no global order
between logs.

One SSE connection multiplexes all logs in a `Subscription`. Each SSE data
frame contains one JSON `Event` with its own cursor. `token_deltas` defaults
conceptually to false and must be explicitly enabled. Token deltas are live
rendering hints, not durable log records and not replayed. For a token delta,
`sequence` is the most recently committed sequence of its `log_id`, not a new
sequence; clients must not advance durable cursors on deltas. Final messages
and idle events are durable.
The client may submit another message after observing idle.

## Mutations and errors

Every mutation includes a required `idempotency_key`. Reuse the same key for
retries of the same intent; the server returns the original result instead of
performing the mutation again. Use a fresh key for a different intent. The
server rejects reuse with a different body. Errors include a stable `code`
for machine handling, a human-readable `message`, and `provider_text` when a
provider supplied original error text; the latter is preserved verbatim.

## Compatibility

The path is versioned (`/v1`). Within v1 changes are additive: existing fields,
variant names, meanings, and cursor behavior do not change. Consumers should
ignore unknown JSON fields and unknown event variants; producers keep required
fields stable. Incompatible changes require `/v2`, with a migration window.
