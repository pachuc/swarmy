# Cost per agent across tokens, compute, and storage

Recorded 2026-09-21.

## What it is

The compute and storage side of cost per agent and per swarm: compute (the
share of node hours the agent's computer occupied) and storage (the chunks
its disk and snapshots hold in object storage, priced at the bucket's rate).
The inference side (tokens and cost by session, agent, provider, auth entry,
and time range) is built by the `provider-breadth-quota` goal in tasky,
which also introduces the hourly rollups and the `swarmy cost` views that
this item extends.

## Why it matters

A swarm is a bill. Deciding which agents to keep, which models to give them,
and how long to retain snapshots needs the number, and the Codex fleet has
already hit spending limits by surprise. Provider quota and admission control
(the `provider-breadth-quota` goal) need the same counters to make decisions.

## Why not now

Inference is the dominant cost today and is covered by the provider goal.
Compute and storage attribution need the cloud topology to settle first,
since node hours and bucket sizes are only meaningful once swarms run
unattended in the cloud.

## What it would take

1. Compute: the placement records already say which node hosted which
   computer and when; a usage counter per agent of sandbox-seconds, written by
   the node daemon at eviction and on a periodic tick for long-lived
   sandboxes, priced by the node's instance type from a small table in the
   swarm record.
2. Storage: the collector's mark phase already walks every live manifest; have
   it record, per computer, the bytes of chunks referenced (exclusive and
   shared, since clones share chunks), and price by the bucket's storage rate.
3. Add compute and storage as dimensions of the provider goal's hourly
   rollups so `swarmy cost` shows them beside inference with the same time
   groupings.
4. Budgets: an optional per-agent or per-swarm monthly limit that pauses new
   turns when exceeded, with a notice delivered to the agent. This is the
   piece that overlaps with admission control and should be designed with it.

## When to pick it up

The first month a cloud swarm runs unattended.
