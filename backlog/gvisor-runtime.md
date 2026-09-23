# gVisor as the sandbox runtime

Recorded 2026-09-21. Decision at the time: punted. Sandboxes stay runc
containers on shared nodes behind the narrow sandbox interface, and the fully
local topology uses sudo on Linux. On 2026-09-22 the tasky goal
`sandbox-pause-resume` (design slice 6) was cancelled and folded into this
item: memory pause is not needed for the core functionality being aimed at,
and is recorded here as something to explore if gVisor is ever adopted.

## What it is

gVisor is a user-space kernel from Google. Its runtime, `runsc`, is a drop-in
replacement for runc that takes the same OCI container images and lifecycle.
Instead of letting the container's processes call the host kernel directly,
gVisor intercepts every system call and serves it from its own implementation,
so the host kernel is reachable only through a small, filtered set of calls.
It backs Google's GKE Sandbox, Cloud Run, and App Engine, and Modal runs its
sandboxes on it. Releases are monthly. Its default platform no longer needs
hardware virtualization, so it runs on ordinary EC2 instances and laptops.

## Why it matters

Two things, and they are separate.

**Isolation.** Today every agent on a node shares the node's kernel. A kernel
exploit from one agent's sandbox reaches every other agent on that node, the
credential socket, and the node daemon. With gVisor the attack surface is the
gVisor kernel, which is far smaller and written in a memory-safe language.
This matters once agents run untrusted code at density, or once a swarm hosts
more than one tenant.

**Checkpoint and restore.** `runsc checkpoint` writes a sandbox's memory and
state to disk and `runsc restore` brings it back. That is the primitive slice 6
(sandbox pause and resume) needs. runc has no equivalent that is safe for
this use. The intended shape when it arrives is a two-level sleep: pause on the
node first, with memory and disk kept locally so wake is hundreds of
milliseconds, and upload and release only when the node needs the space or the
idle time passes a threshold. Placement affinity, preferring the last node, is
the second half and is already designed.

## Why not now

- An idle agent already costs nothing. The sandbox is evicted after thirty
  minutes and the disk is durable in object storage, so pause and resume only
  returns as a feature once cold start (about two seconds) or lost process
  state (a long build interrupted by eviction) is a real complaint.
- There is no multi-tenant or hostile workload yet. Every agent belongs to the
  operator.
- The disk path has an unmeasured cost. Agent disks arrive on a node as NBD
  block devices and the node mounts the ext4 filesystem. gVisor cannot mount a
  block device inside the sandbox; the host mounts it and gVisor serves the
  files through its file gofer, an extra process on every file operation. All
  of the volume layer's work on write stalls and boundary snapshots would sit
  behind that layer. Nobody has measured it.
- Syscall-heavy work runs slower. Builds, `npm install`, and git operations
  typically run 1.3 to 2 times slower. Coding agents will feel it.
- It does not help the local topology. Rootless gVisor still cannot attach
  NBD devices, so sudo is needed either way.

## What it would take

1. A spike, one to two weeks: install `runsc` on a node, run the base image
   under it with the NBD-backed root filesystem host-mounted and passed as a
   bind mount, and measure the coding scenario from the chaos suite, the
   volume benchmark's write stall, and a `cargo build` of this repository
   against runc. Decide on the numbers.
2. If adopted: the runtime name becomes a node setting (`[sandbox] runtime =
   "runc" | "runsc"`), the node daemon passes the mounted filesystem instead of
   the device, and the `SandboxRuntime` capabilities report `memory_pause`.
   The design's section 11 already shapes this; no placement, snapshot, or
   routing code changes.
3. Then pause and resume, formerly slice 6, can be explored: checkpoint on
   idle to local disk, restore on the next turn, upload and release under
   pressure, placement affinity to the last node. Most of that slice already
   exists without pause: idle eviction after thirty minutes with the disk
   durable, dead-node detection with rebuild elsewhere and a notice to the
   model, and a two-second cold start. Only memory pause and placement
   affinity remain. Its original acceptance, one hundred agents on a machine
   sized for ten resident sandboxes with twenty resuming after a node kill,
   only holds with pause; without it the memory budget from the graphical
   sandboxes goal makes the scheduler wait the extra agents rather than
   crash, which is correct but a different promise.
4. Known gaps to check against the tool set: ptrace-based tools, some
   `io_uring` use, nested containers (Docker inside the sandbox needs extra
   configuration), and restore of open network connections.
5. GPU sandboxes: gVisor supports NVIDIA cards for compute workloads only,
   through its `nvproxy` feature, not for graphics. Firecracker cannot pass
   a card through at all. Once the graphical and GPU sandboxes goal lands,
   any runtime change must keep GPU sandboxes on runc or accept that they
   are excluded from the stronger isolation.

## When to pick it up

Any of: a second tenant on shared nodes; agents running code the operator does
not trust; a measured complaint about cold start or lost process state on
eviction; or the microVM question being reopened, since gVisor is the step
that keeps the container shape and microVMs (Firecracker, Cloud Hypervisor)
are the step that does not. Firecracker needs hardware virtualization, which
on AWS means metal instances at roughly six dollars an hour minimum, so gVisor
is the cheaper first move.

## Related

- `docs/DESIGN.md` sections 8 and 11, and the cancelled `sandbox-pause-resume`
  goal in tasky, whose spec is still readable there.
- What agent-substrate shows: both runtimes behind a pause-then-suspend model,
  request parking under saturation, golden snapshots per base image, and an
  mTLS egress point that injects credentials so nothing secret enters a
  sandbox. Worth borrowing when this is designed.
