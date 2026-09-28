# Roadmap and retired design proposals

The implemented architecture is [ARCHITECTURE.md](ARCHITECTURE.md). The live
schedule and dependencies are tracked in tasky and summarized in
[AGENTS.md](../AGENTS.md#the-plan-september-2026); this page only records
features formerly presented as shipped in the old design draft.

| Proposal | Status |
|---|---|
| `swarmy-guest` sidecar | Not built; tools run through `swarmyd` and `swarmy-tools`. See [housekeeping](../backlog/housekeeping.md). |
| `swarmy-channels` and channel service | Cancelled as a swarmy slice; standalone chaty is [backlogged](../backlog/chaty.md). |
| Kubernetes-first provider | Replaced by EC2-first remote/cloud topology; Kubernetes packaging is [backlogged](../backlog/kubernetes-packaging.md). |
| Firecracker runtime | Not built; current runtime is runc ([sandbox source](../crates/swarmy-sandbox/src/lib.rs)). |
| gVisor runtime | Deferred; see [runtime backlog](../backlog/gvisor-runtime.md). |
| `deploy/` manifests/tree | Not built; current deployment uses [remote provisioning](../scripts/remote-provision.sh) and [operations guide](REMOTE.md). |
| Browser/screen and GPU sandboxes | Draft goal, not current capability; see [AGENTS.md](../AGENTS.md#the-plan-september-2026). |
| Provider pools, failover and quota views | Draft goal, not a promise of current behavior; see [AGENTS.md](../AGENTS.md#the-plan-september-2026). |
