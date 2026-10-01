# API conversations

All routes require the API bearer token. Each mutation carries a nonempty
`idempotency_key` of at most 256 bytes. Reusing a key for the same operation
returns its original result rather than repeating the mutation.

- `POST /v1/sessions`: `CreateSession` JSON. Supply `image` for an ephemeral
  session (or configure a default image). Supply `agent_id` to open the named
  agent's main conversation; `new: true` opens a side conversation instead.
  Provider, model, and effort overrides apply only to ephemeral sessions.
  Returns `Session`.
- `POST /v1/sessions/{id}/messages`: `AppendMessage` JSON with `text`,
  `expected_head`, and `idempotency_key`. The expected head must be the last
  sequence observed while the session was idle. The server atomically checks
  that head and idle state, appends the user message, marks the session runnable,
  then nudges the scheduler. A stale head returns 409 `stale_head` with the
  actual head in the message; a non-idle session returns 409 `session_not_idle`.
  Returns `AppendedMessage` with `sequence` and `turn_id`. A retry of the same
  key returns the original sequence even after the session has advanced.
- `POST /v1/sessions/{id}/interrupt`: `InterruptSession` JSON. Returns
  `{"result":"requested"}` or `{"result":"finished"}`. An idle session
  has no turn to interrupt.
- `DELETE /v1/sessions/{id}`: `CloseSession` JSON. Completes the session;
  ephemeral sessions also delete their computer. Named side sessions may be
  closed, but a named main session cannot.
- `GET /v1/sessions/{id}/wait-idle?after=N&timeout_ms=30000`: blocks until
  the session is idle and its head has passed `N`, or returns 408 on timeout.
  The maximum wait is two minutes. A Completed session returns immediately
  with state `completed`. Omit `after` to return immediately if already idle.
  The server listens for session state changes and falls back to a store check
  every three seconds if a live notification is lost. This route is for clients
  that do not need SSE.

The append's durable Runnable index recovers from a missed bus nudge. Turn
Submitted and Appended timing observations are published once for each fresh
append, in that order; idempotent retries do not publish a second Submitted.

The latency tests in `crates/swarmy-api/tests/latency.rs` run against the dev
stack with a built workspace: the fixture registers a metadata-only image and
spawns its own scheduler, worker, and gateway, so they need no root and no
registered image. `fake_turn_records_first_token_metrics` runs on every PR
through `cargo test --workspace`. The fifty-turn
`api_first_fake_token_stays_within_five_ms_of_direct_append` comparison stays
opt-in behind `SWARMY_API_FAKE_BENCH=1`, because a 5 ms comparison is noise on
shared runners. With the stack running, set `SWARMY_API_FAKE_BENCH=1`, then
run `cargo test --locked -p swarmy-api --test latency -- --nocapture`. It
collects fifty turns through each path and compares append-to-first-token p95;
the API p95 must not exceed the direct p95 by more than 5 ms.
