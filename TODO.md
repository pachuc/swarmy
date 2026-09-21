# Swarmy: outstanding work and open decisions

Written 2026-09-19 after the coding-agent and persistent-agent slices landed
(master 758da49). Twelve goals are complete: the agent loop, the block-level
disk, persistent computers, remote nodes, named agents, fast turns, non-blocking
snapshots, a real coding agent, and a persistent agent with memory and timers.
This file lists what is left, roughly in the order it is worth doing, and the
decisions that shape it. It is a working note, not a spec.

## 1. Before anyone else relies on it

### Agents that outlive the stack
Today `swarmy remote down` deletes every agent, session, and image with the
node, because FoundationDB, NATS, and SeaweedFS all live on the instance disk.
Move chunks to real S3 (the code already speaks S3 and the benchmark identity
has a bucket) and put the store on an EBS volume that survives termination, or
on a managed database, so `swarmy remote up` can reattach to an existing stack.
Small, and the first thing missing once there is an agent worth keeping.

### Operability debts from first use
- `swarmy doctor` reports binary versions but not whether the scheduler,
  worker, and gateway are actually running; it passed while nothing was up.
- `swarmy chat` locks input silently when no worker is live. It should say so,
  from the absence of a worker heartbeat.
- Root-only tests never run in CI, so a merged PR can leave them broken on
  master until the next agent trips over it. Happened twice. A nightly job
  that runs the root suites on a real node closes the gap.
- One CI flake remains: the NATS nudge deduplication test's five-second wait
  can time out on a loaded runner. Lengthen it the way the routing tests were.
- Local fixtures launch sibling binaries from `target/debug`; after switching
  branches, `cargo build --workspace` before trusting a failure. The version
  guard also refuses stale `~/.cargo/bin` services: `make install` fixes it.

### Retention and collection at scale
The collector claims each candidate chunk in its own transaction, about two
hundred deletes a second. Batch the claims before fleets get large.

## 2. Still on the original plan

### Channels (slice 5)
Agents messaging each other and people: ordered logs with member lists, a
direct message as a two-member channel, sends that wake the recipient. This is
what turns one very good agent into a swarm; nothing else below matters until
more than one agent works together.

### Provider quota and pools (the rest of slice 8)
The provider breadth is built and verified live as of 2026-09-21: anthropic,
openai, chatgpt, xai, meta, openrouter, azure, amazon-bedrock, google, and
google-vertex all pass the direct probe and a routed turn through a node
(`scripts/providers/smoke.sh`). Still to build: key pools with per-key quota
counters, admission control in the scheduler, affinity, and failover. A swarm
burns through a single subscription quickly; the Codex fleet hit its limit
three times in one week.

Known gaps from the live runs:
- Claude on Vertex is unverified live. The project's quota for the model is
  zero and two increase requests were denied; the client reaches the endpoint
  and is refused only on quota, and the same client is proven direct, through
  OpenRouter, and through Bedrock.
- Bedrock API keys issued from the console expire after twelve hours; the
  credential store reports them as expired and does not refresh them.
- The `azure` provider assumes the classic `openai.azure.com` host. A Foundry
  resource needs a `[custom_providers.azure]` base URL override in config.
- The Gemini API refuses Gemini 2.5 models for new keys; the smoke script's
  default should move to a current model.
- The catalog prices Grok on Azure at zero, so its cost totals read zero.

### Browser and screen (slice 7)
A display server and Chromium in the image, tools that drive them through the
DevTools protocol. Memory per computer goes up; see isolation below.

### Cloud deploy (slice 9)
Kubernetes packaging and a node provider that scales a pool. The remote-node
work is a manual version of this; the automatic one is what makes a fleet.

### Sandbox pause and resume (slice 6)
Designed when a computer was a VM. With containers, an idle agent costs
nothing, so this only comes back with microVMs.

## 3. Things you will feel in daily use
- A web or desktop client instead of the terminal.
- Forking an agent's computer into a new agent (the volume layer already
  supports clones).
- Retrieval over archived transcripts rather than only the summary chain.
- A cost view per agent: tokens, compute hours, storage.
- A sub-agent tool and an ask-the-user tool, both deliberately left out of the
  coding tool set for now.
- Diagnostics appended to edit results and permission escalation, also left
  out for now.

## 4. Decisions made, and why

- Every session has a computer. Ephemeral sessions own a throwaway one, deleted
  on close or after 24 hours idle; named agents own a persistent one shared by
  all their sessions. No session exists without a disk.
- Disk backup is best effort and never holds up a turn: a snapshot every ten
  minutes when anything changed, the last ten kept, boundary snapshots without
  freezing, uploads below tool traffic in priority.
- Turns must feel immediate: nudge on append, input re-enabled from the idle
  event, remote path within three round trips plus 100 ms. Measured: 18 ms
  local, 26 ms over SSH for a text turn with the instant fake provider.
- Tools: exact-unique edit with two whitespace fallbacks and no fuzzy matching;
  hybrid head-and-tail truncation with a spill file; Codex-style yield that
  backgrounds a long command instead of failing it; one base image for every
  agent, with the developer tools in it.
- Credentials: a GitHub token lives in the store and reaches git and gh over a
  socket at use time, never on the disk. The ChatGPT credential stays on the
  laptop in the default remote setup because the worker runs there.
- Isolation: runc containers on shared nodes for now. The sandbox interface is
  narrow so a microVM backend can replace it without touching placement,
  snapshots, or routing.

## 5. Open decision: isolation and node shape

Containers share the node's kernel; a kernel exploit from one agent reaches
every other agent on that node, the credential socket, and swarmyd. MicroVMs
(Firecracker, cloud-hypervisor) give each agent its own kernel behind hardware
virtualization, and let the agent be root for real. They need KVM on the node,
which on AWS means metal instances only (the smallest is about 128 vCPUs and
$6 an hour; per-core price is the same as virtual instances, the minimum is
what hurts). GCP and Azure allow nested virtualization on ordinary instances.

For ten agents, the options in rough hourly cost:
- one m6i.xlarge with containers, $0.19: what runs today.
- two or three small nodes with containers, $0.40 to $0.60: a node failure
  takes down only some agents.
- one small instance per agent, about $0.42 for ten t3.medium: a hardware
  boundary per agent with no microVM code, since Nitro is the hypervisor;
  needs a small warm pool because an instance takes a minute to start.
- one m6i.metal with microVMs, $6.14: mostly idle at this size.

The design supports all of these already: a node hosts N computers, N can be
one, placement picks any node with room, add-node joins more instances. What
would need building is the node provider (launch, register, scale, drain) and
an "isolation: dedicated" placement constraint. Metal and microVMs can wait for
a density argument; spot pricing suits them when it comes, since rebuild from
snapshot is exactly what makes interruption cheap.

## 6. Loose ends
- GitHub issue 32 (flush latency) is fixed by PRs 36, 37, 38, 61, 62 but still
  open because the launcher token cannot close issues.
- The sysy example design file keeps picking up drag positions from the open
  viewer; revert before committing there.
- The Codex fleet's three lanes share one ChatGPT usage limit; when it trips,
  all three stop at once and their instances are retained for resume.

## Providers

- Key pools with per-key quota accounting, session affinity, and failover.
- Admission control across provider, model, and key request/token limits.
- Image inputs; catalog modality metadata exists but clients do not send images.
- A wider catalog allowlist beyond the enabled providers, with protocol and auth
  verification before each expansion.
