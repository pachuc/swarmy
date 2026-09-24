# API event stream

`GET /v1/events` accepts a URL-encoded JSON `subscription` query parameter and
requires the configured bearer token. The subscription has `cursors`, each
with a `log_id` and the last sequence received, and `token_deltas`. For now,
only `{"kind":"session","id":"SESSION_ID"}` log ids are supported; channel
logs are reserved for a later integration.

For example, with a session id and an API token in the environment:

```sh
subscription=$(jq -nc --arg id "$SESSION_ID" '{cursors:[{log_id:{kind:"session",id:$id},sequence:0}],token_deltas:true}')
curl -N -G -H "Authorization: Bearer $SWARMY_API_TOKEN" \
  --data-urlencode "subscription=$subscription" http://127.0.0.1:8742/v1/events
```

The response includes `x-swarmy-connection-id` and begins with a `connected`
SSE event whose data contains the same id. Each durable `event` has the
`swarmy-api-types::Event` JSON shape and an SSE `id` containing the full
subscription and its current per-log cursors, encoded as base64url JSON. Send
the last SSE id as `Last-Event-ID` on reconnection; it overrides the original
query subscription so changes made during a connection survive a reconnect.
A reconnect may omit the query parameter if it sends `Last-Event-ID`.
The cursor advances independently for each log. Durable history is read from
the store in pages and live NATS messages only prompt another store read.

Change logs or token preference without reconnecting with
`PUT /v1/events/CONNECTION_ID/subscription` and a JSON `Subscription` body.
A `subscription` SSE event acknowledges the change after the new live feeds are
registered and the new logs have caught up; it carries the new SSE id.
`token_delta` events include the log id, turn id, byte position and text. They
have no durable sequence and do not change the SSE id; lost tokens are not
replayed. Only subscribed sessions with `token_deltas: true` receive them.

Idle streams send a comment every 15 seconds. The first event advertises a
one-second SSE retry delay. The server bounds the output queue at 64 events;
a client that cannot drain it within two seconds is disconnected and resumes
from its last SSE id.
