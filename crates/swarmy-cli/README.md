# CLI

Source `.dev/env`, start the scheduler, worker, and gateway, then run:

```sh
swarmy run "what time is it"
swarmy session list
swarmy session show SESSION_ID --json
```

`run` creates an Idle session, records the user prompt, subscribes to both live
feeds, and asks the scheduler to wake it. Text is flushed as model deltas arrive.
Tool requests and results each occupy one line; embedded newlines in tool data
are JSON escaped. The session id is printed to stderr. The command exits with
status zero at Idle. It also reads the durable log to recover missed events and
checks the session record after the log stops moving. A missing or unresponsive
scheduler produces a nonzero exit naming the scheduler; wake requests have a
three-second deadline.

`session show` reads the log through the head recorded when the command starts,
in ascending sequence order. `session list` prints sessions in ascending id
order. Both commands page through the store. Listing pages are independent
views, so a session created behind the current cursor may require another list.

The global `--json` flag works before or after subcommands. Output is compact
newline-delimited JSON, following the auth command's event stream convention:

- `session show`: one serialized `swarmy_core::Event` per line.
- `session list`: one serialized `swarmy_core::SessionRecord` per line.
- `run`: a `session_created` record containing `session_id`, `model_delta` records
  containing `delta`, and `session_event` records containing `value`. Durable
  events appear in sequence order. If Idle is detected through the session
  record without a state event, a final `session_idle` record contains the id.

Errors and tracing go to stderr. JSON consumers should use the process exit
status to detect failures. Model deltas are ephemeral; session events provide
the durable history.

Connection settings match the services: `SWARMY_FDB_CLUSTER_FILE`,
`SWARMY_NATS_URL`, and the `SWARMY_S3_*` settings from
[DEV.md](../../docs/DEV.md). `SWARMY_STORE_DIRECTORY` defaults to `swarmy` and
uses `/` between directory components. `SWARMY_BUS_PREFIX` optionally isolates
bus subjects. Inspection commands only require the store settings. `RUST_LOG`
controls tracing, which defaults to `warn`.

The CLI integration tests use isolated store directories and NATS prefixes.
A stand-in wake handler and worker exercise streaming, tool output, durable
fallback, JSON, pagination, and absent or unresponsive scheduler failures. The
stand-in claims a real store lease and commits the assistant message and Idle
transition. It does not exercise the real worker or provider.
