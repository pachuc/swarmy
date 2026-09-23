# Retrieval over archived transcripts

Recorded 2026-09-21.

## What it is

Agents summarize their conversation at three quarters of the model's context
window and continue from the summary. Everything before the summary is still
in the session's event log in the store, but the agent cannot see it. This
item gives the agent a tool to search its own archived history, and gives
people a search across a swarm's conversations.

## Why it matters

After weeks of work an agent's memory of what it did lives in a chain of
summaries, each lossy. "What did I decide about the retry policy in week
two" is answerable from the log and not from the summary. A person operating
a swarm has the same question across many agents.

## Why not now

Summaries plus memory files (the files under `/home/agent/memory`, which the
agent edits and which are injected each turn) cover the daily case, and no
agent has run long enough for the gap to be felt. Retrieval done well is a
service of its own (index, embeddings or full-text, ranking, an API) and
should be designed against a real complaint.

## What it would take

1. Choose full-text first. FoundationDB is not a text index; the cheapest
   correct approach is an indexer that tails session logs (through the same
   event stream the API exposes) into a search engine that is easy to run in
   the stack. Tantivy is a Rust full-text library that would embed in the API
   process with its index on the persistent volume; Meilisearch or
   Typesense are separate services with their own operators. Embedding-based
   search can come later behind the same tool.
2. A `search_history` tool for the agent scoped to its own sessions, returning
   snippets with session and sequence references, and a `read_history` tool
   that fetches a range of events by sequence.
3. An API route and CLI command for people, scoped by agent, channel, or
   swarm, with the same result shape.
4. Index lifecycle: rebuildable from the logs at any time, so it is a cache,
   never a source of truth, and the collector and retention rules do not
   change.

## When to pick it up

The first agent that has been running for more than a couple of weeks and
gives a wrong answer about its own past that the log could have answered; or
a person asking for search across the swarm.
