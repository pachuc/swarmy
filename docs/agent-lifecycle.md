# Agent and computer lifetime

`Store::create_agent(name, image, description, now)` creates a unique,
case-sensitive name and pins the registered `NAME:TAG` manifest. Lookup uses
`get_agent` or `get_agent_by_name`; `list_agents` pages by agent id.
`delete_agent` removes the name and identity and deletes its computer. Reusing
a name creates a different id. The collector protects named agents' image pins
even after the image tag changes.

`create_session_for_agent(id, agent, image, now)` creates an idle session.
With no agent it mints an anonymous agent id and requires an image. With an
agent id it requires an existing named agent, uses that agent's pinned image,
and rejects any explicit image with:

> --image cannot be used with a named agent; its pinned image is used

`list_sessions_by_agent` pages the secondary index by session id. New sessions
are indexed at creation. Legacy sessions without index rows remain available
through `fetch_session` and `list_sessions`. The compatibility `create_session`
entry point accepts an existing anonymous id for older callers.

Session kind is stored in a separate postcard row. The original stored session
header is unchanged; an absent kind means ephemeral. `computer_deleted` on
fetched session records is resolved from a durable agent tombstone. This marks
all of an agent's sessions atomically, including legacy records, without a
transaction proportional to the number of sessions. Transcripts and snapshots
of conversation state remain readable.

`delete_computer(agent)` atomically releases the current placement and its
capacity, records the tombstone, and removes the volume and retained disk
snapshot references. The placement epoch counter remains. Deleted ids cannot
be placed or have their volume recreated. A node cancels ongoing tool execution,
stops processes, destroys the sandbox, and detaches its device when its next
placement renewal fails. Stale tool completions and disk publications cannot
commit after deletion. Physical chunk deletion follows the collector's grace
window. Identity deletion and computer deletion are idempotent.

Workers and nodes refuse tools with this message:

> This session's computer has been deleted. Create a new session to run tools.

Worker recovery records this error for pending sandbox calls. A session whose
computer alone was deleted can still retain and read conversation history.
`close_session(id, now)` is for ephemeral sessions: it deletes the computer and
sets the session to Completed. Named sessions cannot be closed by this API.

`swarmy session interrupt SESSION_ID` ends the current turn without closing the
session. A parked inference wait ends immediately with a non-retryable
`InferenceFailed` event. A running turn ends at its next worker step boundary;
an active provider request may have to return first. `session show` displays a
pending interruption, including `interrupt_requested` in JSON output. Idle and
Completed sessions have no turn to interrupt, so the command exits with an error.
If a sandbox tool returns a managed process while interruption is pending, the
node stops that process before committing the tool result.

The scheduler runs `sweep_ephemeral_sessions` every sixty seconds, or every
retention interval when that is shorter. `ephemeral_retention_seconds` defaults
to 86400 and must be positive. The environment override is
`SWARMY_EPHEMERAL_RETENTION_SECONDS`. Only Idle ephemeral sessions whose idle
timestamp is strictly older than the cutoff close. Creation and transitions
into Idle set that timestamp. Each candidate is rechecked in its closure
transaction, so a concurrent wakeup either wins first or observes a completed
session. An old Idle session without timestamp metadata starts its retention
clock when the sweep first observes it. Active and named sessions remain open.

Named agents store optional `provider`, `model`, and `reasoning_effort` fields.
Unset fields inherit the corresponding stack defaults independently. Set them
with `agent create` or `agent set`; `agent set --provider default`,
`--model default`, or `--effort default` clears that override. These updates
preserve the agent's identity, computer, and sessions and apply to the next
inference request. Ephemeral sessions instead persist an `inference` selection
when created. See [choosing a model](providers.md#choosing-a-model).
