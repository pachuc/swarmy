# Control plane high availability

Recorded 2026-09-21.

## What it is

The cloud topology in tasky runs FoundationDB, NATS, the scheduler, the
worker, the gateway, and the API on one control instance backed by a
persistent volume and an S3 bucket. If that instance dies, every agent stops
until `swarm start` launches a replacement and reattaches the volume. Nothing
is lost, but nothing runs either.

High availability means no single machine's loss stops the swarm: a
FoundationDB cluster of three or more processes on separate machines with
triple replication, so the database keeps serving through one failure; NATS
JetStream in a three-node cluster with replicated streams; at least two of
each stateless service so the loss of one leaves the other working; and the
API behind a load balancer with a stable address. FoundationDB and NATS both
cluster on plain machines and need no Kubernetes for this.

## Why it matters

A persistent swarm is only trustworthy for agents that work for weeks if the
control plane is not a single point of failure. The design's promise is that
no single machine failure loses an agent, and the store's correctness argument
(every write checks its lease in the same transaction) assumes the store is
there to check against. Today the promise holds for nodes and for service
processes, not for the machine that holds the store.

## Why not now

- There is one operator and a handful of agents; a control instance failure
  is a `swarm start` away and costs minutes, not data.
- The single-instance shape is what makes persistent swarms cheap (one small
  instance plus a volume). Three database machines triple the fixed cost.
- FoundationDB's cluster configuration (coordinators, process classes,
  storage and log roles, exclusion during replacement) is real operational
  work; doing it before there is a fleet means learning it twice.

## What it would take

1. Provisioning for three control instances in one availability zone (or
   three zones with the latency cost measured), each with a persistent
   volume, FoundationDB configured with `triple` redundancy and the three
   machines as coordinators, and the cluster file distributed to every
   service and node.
2. NATS JetStream clustered across the same three machines with stream
   replicas of three; the bus already tolerates duplicate and lost delivery,
   so only configuration changes.
3. Every stateless service runs on all three; the scheduler and worker
   already partition work by lease, so more instances are additional
   capacity, not a coordination problem. The API sits behind a network load
   balancer with the TLS certificate issued for the balancer's name.
4. `swarm status` reports cluster health from FoundationDB's status JSON, and
   `doctor` warns when redundancy is degraded.
5. A chaos scenario that terminates one control instance mid-turn and expects
   every session log to stay contiguous, extending the existing suite.
6. A replacement procedure: `swarm node` style commands for control instances
   (`swarm control replace ID`) that exclude the old process from FoundationDB,
   launch a new one, and include it.

## When to pick it up

The first persistent swarm whose agents someone would be upset to have paused
for an hour; or a second operator, because at that point a control plane
outage is someone else's problem too.

## Related

- The `cloud-topology` and `persistent-swarms` goals in tasky, which this
  builds on.
- [kubernetes-packaging](kubernetes-packaging.md), which would provide the
  same through operators if that route is chosen.
