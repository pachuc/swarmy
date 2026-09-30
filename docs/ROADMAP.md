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
| Provider pools, failover and quota views | Named routes with turn-boundary failover and cost and quota views are current behavior (see [providers](providers.md)); key pools, admission, and the remaining quota work are a draft goal, not a promise of current behavior; see [AGENTS.md](../AGENTS.md#the-plan-september-2026). |

## Historical proposed latency budgets

The former design proposed a warm zero-delay fake-provider turn under 100 ms
locally and under three client-to-node round trips plus 100 ms remotely, for
text and one trivial bash call. Cold boot, image building, real provider
latency, and nontrivial tools were outside that budget. This is a target, not
a claim that measurements satisfy it; see [turn measurements](volume-benchmarks.md).

The former slice-2 tool-boundary budget targeted added p95 publication latency
from a tool finishing to acknowledgement of its durable manifest. It was a
proposal, not measured compliance; the persistent computer model now publishes
through periodic snapshots and explicit checkpoints. For an unstaged change
with warm base cache and at most two manifest leaves plus a root, the historical
allowances were:

| Changed 256 KiB chunk coverage | AWS | GCP |
|---|---:|---:|
| No dirty chunks | 250 ms | 250 ms |
| Up to 256 KiB | 500 ms | 3 s |
| Up to 1 MiB | 500 ms | 3 s |
| Up to 16 MiB | 750 ms | 4 s |
| Up to 64 MiB | 1.25 s | 8 s |
| Up to 256 MiB | 3 s | 25 s |
| Up to 1 GiB | 12 s | 90 s |

The former warm-turn allocation was a total per turn, including both
inference passes for the trivial bash shape:

| Work | Local allowance | Remote allowance |
|---|---:|---:|
| Submit and commit user append | 10 ms | R + 10 ms |
| Wake, scheduler nudges, and lease claims | 15 ms | R + 15 ms |
| Build and deliver inference requests | 15 ms | 15 ms |
| Fake provider streams | 10 ms | 10 ms |
| Dispatch bash and receive fenced completion | 20 ms | R + 20 ms |
| Commit results, fold, snapshot, and idle | 20 ms | 20 ms |
| Render final text and enable input | 5 ms | 5 ms |
| Total | 95 ms | 3R + 95 ms |

Here R is the measured client-to-node round trip. Neither the timing target
nor historical tool-boundary allowances are guarantees. The default scheduler
and client timers measured in the dated benchmark did not meet this target.

The rationale and raw samples remain in [volume benchmarks](volume-benchmarks.md#2026-09-15-instrumented-release-flush-and-tool-boundary-budget).
