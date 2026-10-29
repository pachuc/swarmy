# Cost per agent across storage, with budgets

Recorded 2026-09-21.

## What it is

The storage side of cost per agent and per swarm: the chunks its disk and
snapshots hold in object storage, priced at the bucket's rate. Elastic machine
cost is the machine ledger (`swarmy swarm machines` and the machines section
of `swarmy cost`, documented in `docs/swarms.md`); per-agent compute will come
from its assignment intervals.
The inference side (tokens and cost by session, agent, provider, auth entry,
and time range) is already built as hourly rollups with `swarmy cost` views
(see `docs/providers.md`); this item extends those views.

## Why it matters

A swarm is a bill. Deciding which agents to keep, which models to give them,
and how long to retain snapshots needs the number, and the Codex fleet has
already hit spending limits by surprise. Provider quota and admission control
(the `provider-breadth-quota` goal) need the same counters to make decisions.

## Why not now

Inference is the dominant cost today and is covered by the provider goal.
Storage attribution needs the cloud topology to settle first, since bucket
sizes are only meaningful once swarms run unattended in the cloud.

## What it would take

1. Storage: the collector's mark phase already walks every live manifest; have
   it record, per computer, the bytes of chunks referenced (exclusive and
   shared, since clones share chunks), and price by the bucket's storage rate.
2. Add storage as a dimension of the provider goal's hourly
   rollups so `swarmy cost` shows them beside inference with the same time
   groupings.
3. Budgets: an optional per-agent or per-swarm monthly limit that pauses new
   turns when exceeded, with a notice delivered to the agent. This is the
   piece that overlaps with admission control and should be designed with it.

## When to pick it up

The first month a cloud swarm runs unattended.
