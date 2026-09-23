# Desktop and mobile clients

Recorded 2026-09-21.

## What it is

Graphical clients for a swarm: a desktop application and a mobile
application that show every agent and its conversations, let a person chat
with an agent, join channels where agents and people talk, send a direct
message to an agent, watch a running turn's tokens, attach a terminal to a
sandbox, and see swarm status, nodes, and costs. The command-line tool
remains the operator's client; these are the everyday client.

## Why it matters

A swarm of long-lived agents is something a person checks in on many times a
day, from wherever they are. A terminal is the wrong shape for watching
twenty conversations, and it is unavailable on a phone. This is also how the
product reaches anyone who is not comfortable in a terminal.

## Why not now

The API the clients would use is being built (the `control-plane-api` goal)
and its public contract, SDK, and conformance suite come after it (the
`client-protocol` goal). Channels and direct messages, which are the main
thing a chat client shows, are the chaty item and not built. A
client started before those settles would be rewritten.

## What it would take

1. The API's event stream, already designed for this: one multiplexed
   server-sent-events connection per client with a subscription set and a
   cursor per log, sessions and channels exposed as the same ordered-log
   shape, and token deltas as a separate opt-in subscription for the
   conversation in focus. The client keeps cursors locally and resumes after
   any disconnect. Presence, typing, and the terminal go over the reserved
   WebSocket endpoints.
2. A technology choice for the desktop application. Tauri (a Rust shell
   around a web view, so the interface is web technology and the core can
   reuse `swarmy-client` directly) is the natural fit for a Rust project;
   Electron is the alternative with a larger footprint. The mobile
   application would share the web interface through the same web view or be
   native; decide when the desktop one exists.
3. Push notifications for mobile when the application is closed: a small
   notification service in the control plane that subscribes to the event
   stream for mentions and direct messages and forwards to Apple's and
   Google's push services. Transport-independent of the API.
4. A separate repository. The client is a product of its own with its own
   release cadence, built against the published SDK and conformance suite.

## When to pick it up

After the `client-protocol` goal completes and chaty has direct messages
and channels working from the command line.
