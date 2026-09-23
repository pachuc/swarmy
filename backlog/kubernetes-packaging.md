# Kubernetes packaging

Recorded 2026-09-21. The tasky goal `cloud-deploy` (design slice 9) was
cancelled and replaced by this item. The first cloud deployment is the
`cloud-topology` goal on plain EC2 with our own node provider.

## What it is

Kubernetes takes a pool of machines and runs containerized services across
them: you describe what should run and it schedules, restarts, and scales it.
Every major cloud sells a managed version (GKE, EKS, AKS) where the cloud runs
the Kubernetes control plane and you rent worker machines. Helm is its package
format: a chart describes the resources one service needs and installs with a
values file. An operator is a program inside the cluster that manages a
stateful system on your behalf.

Packaging swarmy for Kubernetes means: a Helm chart per stateless service
(scheduler, worker, gateway, API); FoundationDB run by the FoundationDB
Kubernetes operator, which manages a multi-machine cluster, coordinators,
process replacement, and upgrades; NATS installed from its official Helm chart
as a clustered JetStream deployment (the older NATS operator is deprecated);
the node daemon as a privileged DaemonSet on a dedicated sandbox node pool; and
a second `NodeProvider` implementation that scales that pool.

## Why it matters

- **Portability.** The Kubernetes API is the same on every cloud, so one set
  of charts deploys to GCP and Azure without a new provisioning path per
  cloud. Our EC2 provider is AWS only.
- **Operations for free.** Crashed services restart, upgrades roll one replica
  at a time, and certificates, metrics, secrets, and network policy come from
  standard pieces (cert-manager, Prometheus, and so on) rather than scripts we
  maintain.
- **Nested virtualization on GKE.** Google's node pools expose hardware
  virtualization on ordinary instances, which is the cheap way to run
  Firecracker microVMs if the isolation decision ever goes that way. AWS needs
  metal instances for the same.
- **Meeting users where they are.** Anyone who already runs Kubernetes will
  expect a chart.

## Why not now

- The cloud-topology goal on EC2 already retires the risk the slice was
  written to retire, that the design depends on something a stock cloud does
  not offer.
- Weight. Cluster upgrades, operator versions, custom resource definitions,
  and a control plane fee of roughly seventy dollars a month per cluster on
  EKS before any worker machine. Someone has to know Kubernetes to debug it.
- The sandbox path does not fit Kubernetes's model. Agent disks are NBD block
  devices attached on the host and sandboxes are runc containers started by
  our daemon, not pods. The daemon must run as a privileged pod with host
  devices and host process access, and the containers it creates are
  invisible to the cluster. This is how gVisor's and Kata's node agents run,
  so it is a known pattern, but Kubernetes would manage the stateless
  services well and the interesting part of swarmy not at all.
- Two provisioning paths to maintain unless the EC2 path is retired.

## What it would take

1. Decide the shape: everything in the cluster, or a hybrid where the control
   plane runs in the cluster and sandbox nodes stay host machines running the
   node daemon under systemd, joined to the cluster's private network. The
   hybrid is simpler and loses little; start there.
2. Charts for the four stateless services with the settings surface mapped to
   values; a FoundationDBCluster resource with triple replication; the NATS
   chart with JetStream file storage on persistent volumes.
3. Object storage: the cloud's native bucket (S3, GCS, Azure Blob) through the
   `object_store` crate, which already speaks all three.
4. The `NodeProvider` implementation that resizes a node pool through the
   cloud's API and waits for the daemon's heartbeat.
5. Run the chaty acceptance scenario on GKE and EKS unchanged. That was the
   slice's acceptance and it still stands.

## When to pick it up

A user or customer who already runs Kubernetes and wants a chart; a need to
deploy on GCP or Azure; or the control-plane-high-availability item being
picked up and the team preferring operators over hand-run clusters.

## Related

- `docs/DESIGN.md` section 11 (compute abstraction and substrate decision),
  which still names Kubernetes as the eventual substrate.
- [control-plane-high-availability](control-plane-high-availability.md): the
  part of this item's value that can be had without Kubernetes.
