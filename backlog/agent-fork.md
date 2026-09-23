# Fork an agent into a new agent

Recorded 2026-09-21.

## What it is

`swarmy agent fork SOURCE NEW`: create a new named agent whose computer is a
clone of the source's computer at that moment, and whose main conversation
starts from a chosen point in the source's history (the current summary, or
the full main conversation, or empty). The volume layer already clones a
manifest in constant time, and a session's history is an append-only log, so
the operation is a handful of store writes.

## Why it matters

- Try something risky on a copy. Fork before a large refactor, let the fork
  attempt it, keep whichever worked.
- Fan out. One agent that has set up a repository, installed tools, and
  learned the codebase becomes ten agents each taking a task, without ten
  cold setups. This is the natural companion to channels and worker sessions.
- Templates. A curated agent (tools installed, memory files written) is the
  seed for every new agent of that kind.

## Why not now

Nothing needs it until more than one agent works on the same thing, which is
chaty. Building it before then produces a feature nobody drives.

## What it would take

1. Store: `fork_agent(source, new, history_mode)` in one transaction: read the
   source's computer manifest at its latest boundary snapshot (or take a
   checkpoint first so the fork sees the source's uncommitted writes), create
   the new agent record with the same image, model, effort, provider, and
   system prompt, create its computer from a clone of that manifest, and seed
   its main session with the chosen history. Memory files live on the disk, so
   they come with the clone.
2. Placement: the new computer is unplaced until its first turn; the existing
   placement code handles that.
3. Collector: a clone shares chunks with its source; the mark phase already
   traces every live manifest, so nothing changes.
4. CLI and API: `agent fork` with `--history summary|full|none` and
   `--checkpoint` to snapshot the source first; an API route for the same.
5. Credential handling: the source's GitHub token is per agent and must not be
   inherited silently; the fork gets none unless `--copy-github-token` is
   given.

## When to pick it up

When chaty ([chaty](chaty.md)) is being built and the first "split this task across
agents" scenario is written; forking is the cheapest way to create those
agents.
