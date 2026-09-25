# Control plane API v1

The public HTTP API uses JSON under `/v1`. `swarmy-api-types` owns its wire
shapes, and its schema test generates `docs/openapi.json`. Every swarm serves
that document at `/v1/openapi.json` and a rendered reference at `/v1/docs`
(Swagger UI), so a running server is always the authority for its own
version. IDs are opaque. Timestamps are ISO 8601 strings in UTC.

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

## Compatibility policy

The route prefix carries the major version: every `1.x` document is served
from `/v1`. Changes within `/v1` are additive only:

- New routes, new optional request fields, new response fields, and new event
  variants may appear at any time. Clients must ignore unknown fields and
  event variants so a newer server never breaks an older client.
- Nothing is removed or reinterpreted: no route or field is deleted, no field
  changes type, and no established behavior changes meaning.
- A route or field on its way out is first marked `deprecated: true` in the
  OpenAPI document with an `x-sunset` date at least sixty days out, and
  announced here. Deprecated shapes keep working for at least two minor
  versions and until their sunset date, whichever is later. The checker
  rejects dateless deprecations, short sunsets, and removals before the
  sunset date; a removal after its sunset date passes.
- Breaking changes ship as a new major API version under `/v2`, with the old
  major version kept serving through a migration window.

`scripts/check-openapi-compat.sh` enforces this in CI. It diffs the generated
`docs/openapi.json` against the merge base with the target branch (a push to
master therefore compares master with itself) with the oasdiff OpenAPI diff
tool and fails on any removed path, removed field, or changed type.

Clients accept any server with the same major API version. `GET /v1/health`
reports `api_version` next to the binary version, and `swarmy doctor` passes
when the server's major API version matches the CLI's, even when the binary
versions differ. Servers older than this contract report no `api_version`
and still require an exact binary match.

## Build a client

This walkthrough builds a client out of plain HTTP calls with `curl`: from a
token to a streamed reply. It assumes a running swarm. The API address and
token live in the `[api]` section of the swarmy config and in `SWARMY_API_URL`
and `SWARMY_API_TOKEN`.

Point shell variables at the swarm:

```sh
BASE="${SWARMY_API_URL:-http://127.0.0.1:8742}"
TOKEN="${SWARMY_API_TOKEN:?set the API token}"
```

Check the server version and read its own reference. Health and the
reference need no token:

```sh
curl -s "$BASE/v1/health" | python3 -m json.tool
curl -s "$BASE/v1/openapi.json" | python3 -m json.tool
# Or open $BASE/v1/docs/ in a browser for the rendered reference.
```

Every mutation takes an `idempotency_key`: retrying the same key and body
returns the original result instead of repeating the work. List the built
images and open an ephemeral session on one of them (substitute a `NAME`
and `TAG` from the listing for the values below):

```sh
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/v1/images?limit=32"
SESSION="$(curl -s -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"walkthrough-session-1","image":{"name":"base-ubuntu","tag":"dev"}}' \
  "$BASE/v1/sessions")"
echo "$SESSION"
ID="$(echo "$SESSION" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
```

Append one user message at the observed head. A fresh session has head zero:

```sh
curl -s -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"walkthrough-message-1","expected_head":0,"text":"Say hi in one sentence."}' \
  "$BASE/v1/sessions/$ID/messages"
```

The turn appends its own events after the user message. Wait for the turn
to finish, then read the transcript:

```sh
curl -s -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/sessions/$ID/wait-idle?after=1&timeout_ms=120000" | python3 -m json.tool
curl -s -H "Authorization: Bearer $TOKEN" \
  "$BASE/v1/sessions/$ID/events?after=0&limit=32" | python3 -m json.tool
```

To watch the reply arrive instead of polling, subscribe to the session log
over server-sent events. The subscription is one JSON value passed as a
query parameter; `token_deltas: true` adds live rendering hints that never
advance the cursor:

```sh
SUB="$(python3 -c 'import json,sys,urllib.parse; print(urllib.parse.quote(json.dumps({"cursors":[{"log_id":{"kind":"session","id":sys.argv[1]},"sequence":0}],"token_deltas":True})))' "$ID")"
curl -N -H "Authorization: Bearer $TOKEN" "$BASE/v1/events?subscription=$SUB"
```

Frames look like this. Persist the last `id` per log; the pair
`(log_id, sequence)` is the durable cursor:

```text
event: connected
id: <cursor>
data: {"connection_id":"..."}

event: event
id: <cursor>
data: {"log_id":{"kind":"session","id":"..."},"sequence":1,"payload":{...}}
```

On disconnect, resume without replaying by sending the newest cursor back
as `Last-Event-ID`; the subscription query parameter can then be omitted:

```sh
curl -N -H "Authorization: Bearer $TOKEN" -H "Last-Event-ID: $CURSOR" "$BASE/v1/events"
```
