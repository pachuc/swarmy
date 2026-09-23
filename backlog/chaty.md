# chaty: a chat layer for agents (formerly slice 5, channels)

Recorded 2026-09-21. The tasky goal `channels` was cancelled and replaced by
this item. Decision: build this as a relatively standalone piece of the stack
called chaty, a chat tool for agents, and integrate it into swarmy the same
way tasky will eventually be integrated. The shape below is roughly agreed
and will be refined when the work starts.

## What it is

Messaging between agents and people as a core service, not a feature bolted
onto one agent. A channel is an ordered message log with an explicit member
list. A direct message is a channel with two members. Members are principals
of three kinds: an agent, a person with a name, or the system. Sending a
message appends it to the channel log and, for every agent member, records
an inbox pointer and wakes that agent's main session if it is idle. If the
agent is mid-step, messages accumulate and arrive together at the start of
its next turn as one batch. People read channels through the swarmy API's
multiplexed event stream (channels and sessions are the same ordered-log
shape there) and send with an HTTP request. An agent directory lists every
agent with a description, tags, and presence derived from session and
sandbox state. The response policy (always answer a direct message, answer in
a group only when mentioned or when the agent decides to) lives in the
harness prompt, not in the infrastructure.

Agents get tools: `send_message`, `create_channel`, `invite`, `list_agents`,
`describe_agent`, `spawn_agent` (create a fresh agent; forking an agent with
its computer is the separate [agent-fork](agent-fork.md) item), and
`spawn_worker` (a side session on the same agent with a task and a
report-back message when it finishes). The ask-the-user tool also lands
here: a message on a channel that includes a person, with the turn parked
until a reply arrives, using the same parking that timers use.

## Why it matters

This is what turns one very good agent into a swarm. Nothing about
self-organization, splitting work, or people supervising many agents exists
until agents can talk to each other and to people. It also unblocks the
sub-agent and ask-the-user tools, which were deliberately left out of the
coding tool set until it exists, and the permission-escalation item in
[tool-diagnostics-and-permissions](tool-diagnostics-and-permissions.md).

## Why not now

The deployment and client work in tasky (the control plane API, the swarm
model, persistent and cloud swarms) has to land first so that chaty has an
API to plug into and swarms worth chatting into. Building it as a standalone
piece also means its own design pass: what it looks like on its own, what its
storage and protocol are, and how it integrates, the way tasky is a separate
tool with its own database and command surface today.

## What exists to build on

- Session logs are append-only with sequence numbers, which is exactly what
  a channel log is.
- The scheduler already delivers timers into idle sessions, and the node
  already injects a "your environment restarted" notice into the next turn,
  so both halves of message delivery, wake and batch-inject, have working
  precedents.
- Named agents have a main session and side sessions; a worker session is a
  side session with a task and a report-back.
- The API's event stream carries a generic log id and per-log cursors, so
  channels appear on the same stream as sessions without a new endpoint.

## Decisions taken so far

- Worker sessions share the agent's computer, as side sessions do today.
  Two sessions editing the same repository on one disk can collide; a
  `--fork` option arrives with the agent-fork item.
- The ask-the-user tool is part of this item.
- A minimal principal type (agent, human with a name, system) exists from day
  one so a person can be a channel member; full multi-user identity is not
  needed yet but the type should not be a migration later.

## Rough task shape when it starts

1. Channel records, membership, the channel log, inbox pointers, and the
   send transaction with wake, in the store (or in chaty's own store, to be
   decided in its design pass).
2. Batch delivery of the inbox at turn start in the scheduler and worker,
   delivered as one message with channel context.
3. The messaging tools plus ask-the-user.
4. Worker sessions with a task prompt and a report-back message.
5. Channels on the API event stream and routes for create, list, send, and
   members; a `swarmy channel create|ls|send|tail` command group and
   `swarmy dm AGENT`.
6. Directory, presence, and the response policy in the prompt.
7. The acceptance scenario as a chaos-style test: a person asks two agents
   in a group channel to split a task; they open a direct message, divide the
   work, each starts a worker session, and report back in the group;
   messages sent while an agent is mid-step arrive as one batch.
8. Documentation, including `docs/DESIGN.md` section 10.

## When to pick it up

After the client-protocol goal completes, when there is a cloud swarm to
chat into and a published API to integrate against. The design pass for
chaty as a standalone tool comes first.

## Related

- `docs/DESIGN.md` section 10 (channels and self-organization).
- [gui-client](gui-client.md), whose main screen is this.
- [agent-fork](agent-fork.md), [tool-diagnostics-and-permissions](tool-diagnostics-and-permissions.md).
