# CLI

Source `.dev/env`, start the scheduler, worker, and gateway, and configure
`default_image` (or `SWARMY_DEFAULT_IMAGE`) to a registered `NAME:TAG`, then run:

```sh
swarmy run "what time is it"
swarmy session ls
swarmy session show SESSION_ID --json
```

Without `--agent`, new conversations are ephemeral: each session owns a separate
computer. Closing the client keeps the session and computer available for resume;
`swarmy session close ID` explicitly completes the session and deletes its
computer. Transcripts remain readable. Idle ephemeral sessions are also subject
to the configured retention period.

Named agents share one computer across sessions:

```sh
swarmy agent create tommy --description "Work on the compiler"
swarmy agent create builder --image base-ubuntu:dev
swarmy agent ls
swarmy agent show tommy
swarmy chat --agent tommy
swarmy chat --agent tommy --new        # separate conversation, shared computer
swarmy run --agent tommy "Check the build"
swarmy agent delete tommy             # prompts for confirmation
swarmy agent delete builder --yes     # suitable for scripts
```

Names contain 1-64 ASCII letters, digits, hyphens, or underscores. Creation uses
`default_image` unless `--image NAME:TAG` is supplied, and pins that image for
the agent. `--agent` accepts a name or agent id and resumes its main session,
creating it on first use. Add `--new` to `chat` or `run` for a separate side
conversation without changing the main session. `--new` requires `--agent`.
`--agent` cannot be combined with `--image` or a chat session id. A literal name
takes precedence if it also looks like an id. Named sessions need no default
image configured. Their tools, files, and background processes share the agent's
computer. `session close` refuses the main session and points to `agent delete`.
Side sessions can be closed without deleting the shared computer; closed
sessions cannot become main. Agent deletion removes the identity and computer
while retaining all transcripts; it fences further tools and the node discards
its local computer on renewal. Quitting either chat leaves the named agent
available.

`agent ls` lists name, id, image, placement node, session count, and creation
time. `agent show` identifies `main_session` and marks each session with
`main=true` or `main=false` in text output. It adds description, placement
epoch, each session's state, and last disk snapshot time and age in seconds,
using the committed head manifest's ULID timestamp as recovery notices do.
Before a volume exists, snapshot fields are null. The command reads the node's
sampled call status: `busy` means a call holds the computer (including startup)
or calls are queued; `idle` means a resident computer has no holder or queued
calls. Missing, expired, or replaced-placement observations report `unknown`.
Samples expire after three node heartbeat intervals. This is call occupancy, not
a health probe or execution authority.

Every agent and session command supports global `--json` and `--remote NAME`.
JSON create returns an agent record; ls emits one record per line with
`node_id` and `session_count`; show adds `placement`, `sandbox_state`,
`sandbox_state_reason`, `call_status`, `last_snapshot_at`, `last_snapshot_age_seconds`, and
`sessions`. Delete and close emit `agent_deleted` and `session_closed` records.
`--json` still requires confirmation for deletion unless `--yes` is supplied;
prompts go to stderr. `call_status` is null when unknown; otherwise it includes
`agent_id`, `node_id`, `epoch`, `holder_session_id`, `queued_calls`, `observed_at`,
and `expires_at`. Text output includes the same observation fields. `session list` remains an alias for `session ls`.

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
in ascending sequence order. `session ls` prints sessions in ascending id
order. Both commands page through the store. Listing pages are independent
views, so a session created behind the current cursor may require another list.

The global `--json` flag works before or after subcommands. Output is compact
newline-delimited JSON, following the auth command's event stream convention:

- `session show`: one serialized `swarmy_core::Event` per line.
- `session ls`: one serialized `swarmy_core::SessionRecord` per line, with `agent_name`
  (null for ephemeral sessions or a deleted named agent) and a `main` boolean.
- `run`: a `session_created` record (or `session_opened` when resuming) containing
  `session_id` and `agent_name`, `model_delta` records
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

The screen contains a wrapped transcript, a status bar with the agent name (or `ephemeral`),
session id, current session state and configured provider, and a single input line. Type a
message and press Enter to send. Left/Right move the cursor and Backspace deletes
before it. Input stays locked until the session returns to Idle. PageUp/PageDown
scroll the transcript; End follows the newest text again. Esc or Ctrl-C exits,
including during a reply. Mouse input and editing history are not supported.
`chat --agent NAME` skips the recent-session picker; add `--new` in separate
terminals to open side sessions on the same agent. System notices in named conversations
include their originating session id in the transcript, including after resume.

Without `--json`, `chat` requires an interactive terminal. `chat --json` instead
reads one prompt per stdin line and emits the same JSON event stream as `run`,
including the initial history and idle marker. Each prompt waits for the previous
turn to become idle. EOF exits after the last turn without deleting the session.
Resuming emits `session_opened` instead of `session_created`. For example:

```sh
printf '%s\n' 'Check the files' 'Describe the result' | swarmy chat --agent tommy --json
```

Live text appears as it arrives. Each tool request has a running line which is
updated with its result on completion. User, agent, and tool lines have distinct
labels and colors. The durable log supplies ordered history and replaces partial
model text with the final message. Resuming reloads the full log, including work
that finished while the client was closed. Both clients share session creation,
message appends, subscriptions confirmed before waking, the three-second
scheduler wake deadline, and a five-second durable-log poll. Polling also refreshes
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

The named-agent root acceptance test opens two terminal chats, starts a background
process in one, observes it in the other, closes both clients, and deletes the
agent. Build as the ordinary user, then run the built test as root:

```sh
cargo build --workspace --locked
source .dev/env
sudo -E ./target/debug/swarmy image build images/base-ubuntu --tag dev
cargo test -p swarmy-cli --test session --locked --no-run
sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev "$(cargo test -p swarmy-cli --test session --locked --no-run --message-format=json 2>/dev/null | jq -r 'select(.executable != null and .target.name == "session") | .executable')" root_named_chats_share_a_background_process_and_delete --nocapture
```

It skips without root or `SWARMY_TEST_IMAGE`. Drop guards stop the node, destroy
containers, unmount filesystems, and detach NBD devices even if an assertion fails.
