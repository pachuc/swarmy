# Tell agents about their disk

Recorded 2026-09-23.

## What it is

An agent's computer has two tiers. The durable volume is chunked into object
storage, snapshotted, and rebuilt on another node after a failure; it holds
clones, memory files, and home configuration. Scratch is a set of paths the
image declares (the cargo target directory, `/tmp`, package caches) that live
on the node's local disk, survive tasks and idle eviction, and are lost only
when the computer leaves the node. Nothing tells the agent this. A model that
does not know which paths are durable may put something it cares about in
`/tmp`, or spend effort protecting a cache that is rebuilt in minutes.

## Why it matters

As agents run for weeks and swarms span nodes, the difference between "kept"
and "may vanish after a node loss" becomes part of how an agent should plan
its work: where to keep notes, where to build, what to commit before a long
pause, and what a restart notice implies.

## Why not now

The scratch tier is being built (dev-fleet goal). Until it exists and the
paths are settled there is nothing accurate to say. The fleet prompt already
tells workers the practical rules for their own disk.

## What it would take

1. A short system prompt section generated from the image's declared scratch
   list at session start: the durable paths, the scratch paths, what each
   survives, and the meaning of the environment-restarted notice. Generated,
   not hand-written, so it cannot drift from the image.
2. The same facts in `docs/agent-lifecycle.md` for people.
3. A `get_disk_info` tool or an extension of `get_time` style status tools
   returning the two lists and the last snapshot time, for agents that want
   to check rather than remember.
4. Later, per-agent caveats: memory file limits, snapshot cadence, and the
   node's scratch eviction policy, once those are configurable per agent.

## When to pick it up

When the scratch tier lands and the first agent is observed misusing a path,
or when named agents start running for more than a few days.
