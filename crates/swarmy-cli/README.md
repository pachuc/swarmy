# CLI

Source `.dev/env`, start the scheduler, worker, and gateway, and configure
`default_image` (or `SWARMY_DEFAULT_IMAGE`) to a registered `NAME:TAG`, then run:

```sh
swarmy run "what time is it"
swarmy session list
swarmy session show SESSION_ID --json
```

Both `run` and `chat` accept `--image NAME:TAG` to override the default for a new
session. There is no built-in default. Creation validates the registration and
pins its manifest atomically; an unknown image reports the registered names and
tags. Existing sessions retain their image when resumed. A node is only needed
when the session first calls a sandbox tool.

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

Errors and tracing go to stderr in the line commands. The terminal client
suppresses tracing to keep its screen intact and restores the terminal before
reporting fatal errors. JSON consumers should use the process exit status to
detect failures. Model deltas are ephemeral; session events provide the durable
history.

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

## Terminal conversations

```sh
swarmy chat                 # choose New session or a recent conversation
swarmy chat SESSION_ID      # resume directly
```

`chat` reads the same `Settings::load` configuration as `dev`, `run`, and the
services. The picker lists the newest 50 session ids, with their first user
message. It starts on **New session**; use Up/Down and Enter to choose.

The screen contains a wrapped transcript, a status bar with the session id,
current session state and configured provider, and a single input line. Type a
message and press Enter to send. Left/Right move the cursor and Backspace deletes
before it. Input stays locked until the session returns to Idle. PageUp/PageDown
scroll the transcript; End follows the newest text again. Esc or Ctrl-C exits,
including during a reply. Mouse input, editing history, and multiple open
sessions are not supported. `chat` requires an interactive terminal and rejects
`--json`; use `run --json` or `session show --json` for machine-readable output.

Live text appears as it arrives. Each tool request has a running line which is
updated with its result on completion. User, agent, and tool lines have distinct
labels and colors. The durable log supplies ordered history and replaces partial
model text with the final message. Resuming reloads the full log, including work
that finished while the client was closed. Both clients share session creation,
message appends, subscriptions confirmed before waking, the three-second
scheduler wake deadline, and a 500 ms durable-log poll. Polling also refreshes
the status bar when state changes do not publish an event. Closing the terminal
client does not cancel the agent. A saved user message whose wake was interrupted
is woken when the conversation resumes.

For a fake conversation with a tool call followed by two text turns, point
`[fake].script` in `.swarmy/config.toml` at a file containing:

```json
{
  "latency_ms": 500,
  "request_based": {
    "steps": 3,
    "tool_steps": [0],
    "final_answer": "The conversation continues."
  }
}
```

Restart services after changing their configuration. The fake provider emits
whole text parts before committing them; text-delta unit tests also cover
incremental token delivery. Transcript tests replay
`tests/fixtures/conversation.jsonl` and render with Ratatui's `TestBackend`,
without a terminal or backing services.

The CLI integration suite also runs `chat` in a pseudo-terminal with a VT100
screen parser. With the backing stack running, it checks two turns against the
request-based fake provider, durable `session show` ordering, tool requests and
results, quitting before commit and resuming, worker and gateway restarts,
terminal restoration, and scheduler failure. A stand-in worker holds a tool
request open and publishes separate text deltas to test those intermediate
screens deterministically.

```sh
scripts/dev-stack.sh start
source .dev/env
cargo test -p swarmy-cli --test session --locked
```

These tests skip when `SWARMY_FDB_CLUSTER_FILE` or `SWARMY_NATS_URL` is absent.

## Volumes

`swarmy vol` supports `create NAME:TAG`, `attach VOLUME`, `flush VOLUME`,
`snapshot VOLUME`, `clone VOLUME`, `detach VOLUME`, `ls`, and `show VOLUME`.
All accept `--json`. Attach requires root and stays in the foreground, printing
its `/dev/nbdX` device once ready. Use another terminal for mount and control
commands. `attach --background` pre-uploads writes; `flush --mount PATH` freezes
a known mount before publication. Detach unmounts, flushes, and disconnects.

Use `node_id` in shared configuration or `SWARMY_NODE_ID` to select a node.
Otherwise the CLI persists an id in `.swarmy/node-id`. Control commands use a
Unix socket under `.swarmy/volumes` and must use the attached writer's node id.
Show prints the manifest chain, newest first. Clone uses the last committed
manifest; snapshot first to include pending local writes. See the
[volume README](../swarmy-volume/README.md#durable-flush-and-attachment-control)
for the full lifecycle, durability boundary, and root acceptance test.
