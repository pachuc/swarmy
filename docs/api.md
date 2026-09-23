# Control plane API v1

The public HTTP API uses JSON under `/v1`. `swarmy-api-types` owns its wire
shapes, and its schema test generates `docs/openapi.json`. The schema document
has no paths until the server task defines routing. IDs are opaque. Timestamps
are ISO 8601 strings in UTC.

## Resource model

**Agents** are named, persistent identities with a description, a selected
image name and tag, optional provider, model, effort and system prompt, a
creation time, and an optional main session ID. An agent may have side
sessions without moving the main-session pointer.

**Sessions** are ordered, append-only logs attached to a computer. Their kind
is ephemeral or named; their state is idle, runnable, leased,
waiting_inference, waiting_tools, sleeping, or completed. The head sequence is
zero before the first append. `computer_deleted` reports teardown; an optional
`waiting` value gives a wake time and human-readable reasons for a parked
session. A session may reference an agent.

**Turns** group inference and tool work within a session. Each has a status,
start time, and optional finish time.

**Messages** carry a role and text and belong to a session. A client may
append messages but never edit old ones.

**Images** identify built computer roots by name and tag. Clients register
already built images; they do not edit image manifests via this API.

**Models** identify provider models and their context windows. Clients
select them for inference but do not edit catalog records.

**Providers** are read-only catalog entries. Clients select them for inference
but do not create or edit providers.

**Credentials** return only provider, kind (subscription, api_key, cloud),
label, status, and update time. Setting or replacing a provider credential
uses a create request with input-only secret material; no read or event
returns a secret.

**Nodes** report roles, CPU, memory, disk and sandbox capacity, liveness and
last seen time. They are not client-created or edited.

**Service health** reports a service role, instance ID, version, liveness and
last seen time. Health records are not client-created or edited.

## Event stream contract

`Event` has a `log_id`, a `sequence`, and a tagged `payload`. `LogId` is a
namespace-tagged value (`{"kind":"session","id":"..."}`); `channel` is
reserved for future channel logs. Sequences start at one, are contiguous
within a log, and are independent between logs. The pair `(log_id, sequence)`
is the durable cursor. There is no global order between logs.

One SSE connection multiplexes the logs in a `Subscription`, which supplies a
cursor per log. The server replays events strictly after each supplied cursor
and then follows live appends without a gap. Sequence zero requests replay
from the beginning. The SSE `id` is base64url without padding of the UTF-8
JSON `Cursor` (`log_id` and `sequence`), so reconnects can resume; consumers
persist the last processed cursor per log and deduplicate by that pair if
replay repeats a frame. Each SSE data frame contains a JSON `Event`.

Token deltas require `token_deltas: true` in the subscription. They are live
rendering hints, not durable log events, and are not replayed. Their
`sequence` is the last committed sequence of that log, not a new sequence.
A token-delta frame has no durable SSE `id`; it cannot advance a cursor.
Final messages and idle events are durable. Input can be re-enabled after an
idle event.

## Idempotency

Every client mutation has a required `idempotency_key`. The same key and body
on retry return the original result without repeating the mutation. A key
reused with a different body is rejected; a different intent needs a fresh
key. Keys are scoped to the caller and retained with the mutation result.
Errors have a stable machine-readable `code` and a human-readable message;
`provider_text` preserves the provider's original error text when present.

## Versioning policy

The API path is `/v1`. Changes within v1 are additive only: no removal or
reinterpretation of existing fields, event variants, cursors, or behavior.
Deprecations are announced before removal, and existing v1 shapes continue
to work. Clients should ignore unknown fields and event variants. Breaking
changes require `/v2` with a migration window.
