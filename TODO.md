# Swarmy: state of the system, the plan, and what is left

Updated 2026-09-21 at master `493806f`. This is the orientation document for
anyone, human or agent, picking the project up. It says what exists and has
been proven, how the work is organized, what remains, which of those items are
decided, which still need a decision, and why the settled decisions were made.
`docs/DESIGN.md` is the design; this file is the map. When they disagree,
this file describes what is true today and the design lags.

## 1. What swarmy is

Infrastructure for running very large numbers of long-lived coding agents on
ordinary cloud compute. The promise: no single machine failure loses an agent,
its conversation, or its disk; turns feel immediate; agents keep working for
weeks with memory, timers, and a persistent computer; any model provider can
serve them. Written in Rust, organized as sixteen crates in one workspace.

## 2. What exists today

Every item here is merged to master and has been exercised on real machines,
not only in unit tests. Pull request numbers give the history.

### Control plane
- **Store** (`swarmy-store`): FoundationDB is the source of truth. Every
  record is postcard-encoded after a version byte; values over 80 KiB go to
  object storage under their hash. Every write that matters checks its lease
  or epoch in the same transaction. Key families: sessions and their
  append-only event log, scheduling, agents, placements, volumes, inference
  and tool fences, timers and plans, credentials, gateway advertisements,
  usage counters, and the collector's leases.
- **Bus** (`swarmy-bus`): NATS JetStream work queues (`sched.runnable.{p}`,
  `infer.req.{provider}`, `tool.node.{node}`) with a 30 s ack wait as the
  delivery lease, plus core live feeds for tokens, events, and turn timing.
  Acknowledging never means owning the work; every consumer rechecks the store.
- **Scheduler**, **worker**, **gateway** (`swarmy-scheduler`, `swarmy-worker`,
  `swarmy-gateway`): the step loop. Nudge on append, claim with a lease, build
  the request, publish inference, commit the response under the fence, run
  tools on the hosting node, finish the turn. Measured p95 for a text turn
  is 18 ms locally and 26 ms over SSH tunnels (PRs 58, 61, 62).
- **Chaos suite** (`swarmy-chaos`): kills processes mid-turn and checks that
  every session log stays contiguous with exactly one inference per step.
  A reduced run is in CI; the full scenarios (`--persistent`, `--continuity`,
  `--coding`, `--kill-node-mid-command`) run on real nodes.

### Disks and computers
- **Volumes** (`swarmy-volume`): every agent disk is 256 KiB blake3-addressed
  chunks in object storage with two-level manifests; a dirty store with
  per-chunk generations uploads continuously in the background, below tool
  traffic in priority; boundary snapshots without freezing (largest write
  stall 3.4 ms); a snapshot every ten minutes when dirty, the last ten kept;
  `checkpoint` on demand; constant-time clone. Served to nodes over NBD.
- **Images**: `swarmy image build` from a directory, shell, debootstrap, or
  OCI recipe; `images/base-ubuntu` has git, gh, ripgrep, fd, jq, node,
  python, and build tools. One image for every agent.
- **Collector**: Bloom-filter mark, 256-prefix sweep, six-hour grace,
  run lease, dry run (PR 41). About two hundred deletes a second.
- **Nodes** (`swarmyd`): a hosting actor per agent, runc container from the
  mounted NBD device, sixteen-slot FIFO, lease renewal, idle eviction after
  thirty minutes, rebuild on another node after failure with a notice
  delivered to the model (PRs 39 to 46). Cold rehydration about 2 s.
- **Every session has a computer**: ephemeral sessions own a throwaway one
  deleted on close or after 24 h idle; named agents own a persistent one
  shared by all their sessions (PRs 65 to 69).

### Agents
- **Named agents**: `swarmy agent create|ls|show|set|delete`, a main
  conversation with `--new` side sessions, per-agent system prompt, model,
  effort, and provider, memory files under `/home/agent/memory`,
  summarization at three quarters of the model's context window, timers
  delivered by the scheduler, a private GitHub token reaching git and gh over
  a socket and never touching the disk (PRs 71 to 79).
- **Tools** (`swarmy-tools`, helpers embedded in `swarmyd`): bash with a
  10 s yield and background continuation, process_start/list/log/stop,
  write_stdin, read, write, edit (exact-unique with two whitespace fallbacks,
  no fuzzy matching), glob, grep, ls, web_fetch, checkpoint, update_plan,
  set_timer/list_timers/cancel_timer, get_time. Every schema is a plain object
  at the top level; a test enforces it (PR 93).
- **Proof**: a ChatGPT-driven agent cloned this repository, edited it, and
  opened a pull request through a node kill in 146 s (PR 79, docs/proofs/).

### Inference providers (PRs 80 to 94)
- **Catalog** (`swarmy-llm::catalog`): generated from models.dev and
  OpenRouter's live list by `scripts/models/generate.py` into checked-in JSON;
  770 models across eleven providers; reasoning options and a shared effort
  scale (`none` to `max`) clamped per model; custom models and providers from
  `.swarmy/config.toml` merged over the snapshot through `Settings::catalog()`.
- **Clients**: Anthropic Messages (direct, Vertex, OpenRouter), OpenAI
  Responses (OpenAI, xAI, Meta, Azure, and the ChatGPT Codex backend), Chat
  Completions (OpenRouter), Gemini (API and Vertex, with a Google token
  source for service accounts and gcloud logins), Bedrock Converse through
  the AWS SDK. Reasoning is replayed with its signature only to the same
  provider and model; every client keeps the provider's error text.
- **Credentials**: encrypted records in FoundationDB (XChaCha20-Poly1305,
  cluster key in `~/.swarmy/keyring`), refresh under a store lease so two
  gateways never race, `swarmy auth set|ls|rm|check|import|login`. Logins:
  ChatGPT device flow, OpenRouter PKCE, Azure CLI. Environment variables are
  a development fallback only.
- **Gateway**: serves every provider it holds a credential for, advertises
  them in the store, records provider, model, effort, usage, and cost in
  micro-dollars on each completion; `session show` and `agent show` total it.
- **Selection**: `--provider`, `--model` (also `provider/model`), and
  `--effort` on `run`, `chat`, `agent create`, and `agent set`; the worker
  routes each job to `infer.req.<provider>` and fails fast when no gateway
  serves it. `swarmy models ls|show|search|providers|probe` and
  `scripts/providers/smoke.sh` for operators.
- **Verified live on 2026-09-21** through a real node, direct probe with a
  tool call and a routed turn: anthropic, openai, chatgpt, xai, meta,
  openrouter, azure (Foundry endpoint, Grok), amazon-bedrock, google
  (Gemini 3 models), google-vertex (Gemini). Claude on Vertex reaches the
  endpoint and is refused only on a zero project quota.

### Operating it
- **Local**: `make dev-tools`, `swarmy dev up|down|logs`, `swarmy doctor`;
  the dev stack runs FoundationDB, NATS, and SeaweedFS under the checkout
  with no root. `make install` keeps `~/.cargo/bin` in step with the
  checkout; the version guard refuses stale binaries.
- **Remote**: `swarmy remote up NAME` launches one EC2 node with the stack,
  `swarmyd`, and the base image; `remote connect` opens SSH tunnels so the
  laptop runs scheduler, worker, gateway, and chat against it; `add-node`
  joins more nodes; `--services node` moves the control plane onto the node
  with `--copy-credential` for the keyring. About eleven minutes from
  nothing to a usable node (PRs 49 to 63).
- **CI**: fmt, workspace tests against a live dev stack, pedantic clippy,
  reduced chaos; about ten minutes with the build cache. Root-only suites
  (NBD, images, containers) skip in CI and run with sudo on real nodes.
- **Documentation**: `docs/DESIGN.md` (design), `docs/providers.md`,
  `docs/agent-lifecycle.md`, `docs/REMOTE.md`, `docs/DEV.md`,
  `docs/volume-benchmarks.md` and `docs/gc-benchmarks.md` (every measured
  number with raw samples), `docs/proofs/`.

## 3. How the work is organized

- The plan lives in tasky (`tasky --json goal list --project swarmy`): one
  goal per outcome, one task per mergeable change with a body and a test
  plan. Completed goals so far: immortal-echo-agent, block-level-disk,
  disk-follow-ups, persistent-computers, persistent-follow-ups, remote-node,
  sessions-have-computers, turn-latency, named-agents, real-coding-agent,
  persistent-agent, inference-providers, model-selection. Draft goals still
  hold the remaining slices: channels, sandbox-pause-resume,
  browser-and-screen, provider-breadth-quota (now only the quota half),
  cloud-deploy.
- Tasks are executed by Codex agents on their own EC2 instances through
  `~/code/codex-daytona`, one pull request per task, three to five in
  parallel. The orchestrator reviews, reconciles conflicts (one subagent per
  pull request in its own git worktree, with explicit per-file rules), merges
  on green CI, and marks the task done. `AGENTS.md` is the contract every
  agent reads; task text is the scope.
- Parallel tasks on shared files (`swarmy-llm/src/lib.rs`, gateway config,
  `docs/providers.md`) always conflict; expect reconciliation and design
  tasks so a shared helper has one named owner.

## 4. The plan and where it stands

The design lays out nine vertical slices. Their state:

| Slice | State |
|---|---|
| 1 Immortal echo agent | done |
| 2 Block-level disk | done, plus follow-ups |
| 3 Real coding agent | done |
| 4 Persistent agent | done |
| 5 Channels | not started; next by value |
| 6 Sandbox pause and resume | not started; blocked on the isolation decision |
| 7 Browser and screen | not started |
| 8 Provider breadth and quota | breadth done; quota, pools, admission remain |
| 9 Cloud deploy | not started; remote nodes are the manual version |

Also done outside the slices: persistent computers with placement, remote
nodes from a laptop, every-session-has-a-computer, turn latency, named
agents, model selection flags.

## 5. What is left to build

Ordered by the value it unlocks. Each item says whether its design is
decided or still open.

### 5.1 Before anyone relies on it (decided, small)
- **Agents that outlive the stack.** `swarmy remote down` deletes every
  agent, session, and image with the node because FoundationDB, NATS, and
  SeaweedFS live on the instance disk. Move chunks to real S3 (the code
  speaks S3; the bench identity has a bucket) and put the store on a volume
  that survives termination, or a managed database, so `remote up` can
  reattach. First thing missing once there is an agent worth keeping.
- **Operability debts** found in daily use:
  - `swarmy doctor` reports versions but not whether scheduler, worker, and
    gateway are running; it passed while nothing was up.
  - `swarmy chat` locks input silently when no worker is live; it should say
    so from the absence of a worker heartbeat.
  - `swarmy run` can exit zero after reporting an inference failure; the
    smoke script works around it by checking the reply text.
  - Root-only suites never run in CI, so a merged pull request can leave one
    broken on master; this happened twice. A nightly job on a real node.
  - One CI flake: the NATS nudge deduplication test's five-second wait can
    time out on a loaded runner; lengthen it like the routing tests were.
  - After switching branches, `cargo build --workspace` before trusting a
    fixture failure; sibling binaries launch from `target/debug`.
- **Collector batching.** Each candidate chunk is claimed in its own
  transaction, about two hundred deletes a second; batch before fleets grow.

### 5.2 Channels, slice 5 (design in DESIGN.md section 10, decided)
Agents messaging each other and people: ordered logs with member lists, a
direct message as a two-member channel, sends that wake the recipient, and
worker sessions an agent can spawn. This turns one very good agent into a
swarm; nothing in 5.4 onward matters until more than one agent works
together. Related tools deliberately left out of the coding set until then:
a sub-agent tool and an ask-the-user tool.

### 5.3 Provider quota, pools, and the live gaps (design in DESIGN.md
section 9, decided)
- Key pools with per-key quota counters, admission control in the scheduler
  so waiting sessions hold no lease, affinity that keeps a session on one
  provider and key while warm, ordered failover. The Codex fleet hit one
  subscription's limit three times in a week; a swarm will do it in hours.
- Gaps from the live runs:
  - Claude on Vertex is unverified live: the project's quota for the model
    is zero and two increase requests were denied. The same client is proven
    direct, through OpenRouter, and through Bedrock; only the Vertex URL and
    the `anthropic_version` body field differ. Closing it needs a Google
    support case or spend history on the project.
  - Bedrock API keys issued from the console expire after twelve hours; the
    store reports them expired and does not refresh them. Long-lived use
    needs an IAM identity or a documented rotation.
  - The `azure` provider assumes the classic `openai.azure.com` host; a
    Foundry resource needs `[custom_providers.azure]` with the base URL.
    Worth reading the host from the credential instead.
  - The Gemini API refuses Gemini 2.5 models for new keys; the smoke
    script's default should be a current model.
  - The catalog prices Grok on Azure at zero; cost totals for it read zero.
  - Grok occasionally writes a tool call as text instead of calling the
    function; a model quirk, but the smoke table will show it intermittently.
- Image inputs: the catalog carries modality metadata but no client sends
  images.
- A wider catalog allowlist beyond the enabled providers, verified per
  protocol and auth before each expansion. The generator's allowlist is the
  switch.

### 5.4 Isolation and node shape (open decision; see section 6)
Nothing in slices 6 and 7 can be scheduled until this is decided. The
sandbox interface is narrow on purpose so the runtime can change without
touching placement, snapshots, or routing.

### 5.5 Sandbox pause and resume, slice 6 (open; depends on 5.4)
Originally designed when a computer was a VM. With runc containers an idle
agent already costs nothing because the sandbox is evicted and the disk is
durable, so this only returns with a runtime that can checkpoint a process
(gVisor or a microVM). When it does, the shape to adopt is a two-level
sleep: pause on the node first (memory and disk kept locally, wake in
hundreds of milliseconds), upload and release only when the node needs the
space or the idle time passes a threshold. Placement affinity, preferring
the last node, is the second half and is already planned.

### 5.6 Browser and screen, slice 7 (decided in outline)
A display server and Chromium in the image, tools driving them through the
DevTools protocol. Memory per computer goes up, which feeds the isolation
decision.

### 5.7 Cloud deploy, slice 9 (decided in outline)
Kubernetes packaging and a node provider that launches, registers, scales,
and drains a pool. Remote nodes are the manual version; the automatic one
makes a fleet. An `isolation: dedicated` placement constraint belongs here.

### 5.8 Things you feel in daily use (undecided, any order)
- A web or desktop client instead of the terminal.
- Forking an agent's computer into a new agent; the volume layer already
  clones in constant time.
- Retrieval over archived transcripts rather than only the summary chain.
- A cost view per agent across tokens, compute hours, and storage; the
  token side exists since PR 86.
- Diagnostics appended to edit results and permission escalation for tools,
  both deliberately left out.
- Documentation site: a twelve-page prototype exists at
  `~/code/swarmy-website/docs` outside this repository.

## 6. Open decision: isolation and node shape

Containers share the node's kernel; a kernel exploit from one agent reaches
every other agent on that node, the credential socket, and `swarmyd`. The
options, with what each costs and gives:

- **runc containers on shared nodes** (today). Cheapest and simplest; one
  m6i.xlarge holds ten agents for about $0.19 an hour. No process pause.
- **gVisor (`runsc`)**. A user-space kernel between the container and the
  host: system calls are intercepted and served by gVisor's own
  implementation, so the host kernel is reachable only through a small
  filtered set. Same OCI images and lifecycle as runc, so `swarmyd` changes a
  runtime name. Costs: slower system calls (builds and test suites notice),
  some Linux features missing, and the NBD-mounted disk is reached through
  gVisor's file gofer. Gains: `runsc checkpoint` and `restore`, which is
  what slice 6 needs. This is the step that keeps the container shape.
- **MicroVMs** (Firecracker, Cloud Hypervisor). A hardware boundary per agent
  and real root inside. Needs KVM on the node: on AWS that means metal
  instances (about 128 vCPUs and $6 an hour minimum), on GCP and Azure
  nested virtualization works on ordinary instances. The right end state for
  hostile workloads at density; premature at today's size.
- **One small instance per agent**. Nitro is the hypervisor, so the boundary
  is hardware with no microVM code; about $0.42 an hour for ten t3.medium.
  Needs a warm pool because an instance takes a minute to start, and the node
  provider from slice 9.
- **Two or three small container nodes**, $0.40 to $0.60 an hour, so a node
  failure takes down only some agents.

The design supports all of these already: a node hosts N computers and N can
be one. Recommendation on the current evidence: adopt gVisor next because it
unlocks pause and resume and stronger isolation without changing the node
shape, keep microVMs for when there is a density or hostility argument, and
build the node provider either way. What substrate (agent-substrate) shows is
both runtimes working with a pause-then-suspend model; its request parking
policy under saturation, golden snapshots per base image, and an mTLS egress
point that injects credentials so nothing secret enters a sandbox are worth
borrowing when these slices are designed.

## 7. Decisions made, and why

- **FoundationDB as truth, NATS as transport.** The safety argument is "every
  write checks its lease in the same transaction", which needs serializable
  transactions over arbitrary keys; the bus is allowed to lose or duplicate.
- **Every session has a computer.** No session exists without a disk, so the
  tool path never has a special case.
- **Disk backup is best effort and never holds up a turn.** Snapshot every
  ten minutes when dirty, keep ten, boundary snapshots without freezing,
  uploads below tool traffic.
- **Turns must feel immediate.** Nudge on append, input re-enabled from the
  idle event, the remote path within three round trips plus 100 ms.
- **Tools follow the three reference agents where they agree and choose where
  they differ**: exact-unique edit with deterministic whitespace fallbacks
  and no fuzzy matching; head-and-tail truncation with a spill file; yield
  that backgrounds a long command; one base image with the developer tools.
- **Providers follow OpenCode and Pi in shape**: a data catalog plus a few
  wire protocols, no per-provider clients, no JavaScript dependencies.
  Subscription logins only where the vendor permits: ChatGPT (tolerated by
  OpenAI, not licensed), OpenRouter PKCE, Azure CLI. Anthropic, Copilot,
  Kimi, and xAI subscription flows are prohibited or impersonate other
  clients and are not built; API keys only.
- **Credentials live encrypted in the store, not in files per host**, because
  providers rotate refresh tokens and two hosts with copies invalidate each
  other. Refresh is fenced by a lease. Inference keys never enter a sandbox;
  the GitHub token reaches git over a socket.
- **Effort is one shared scale clamped per model** so the same flag works on
  every provider.
- **Isolation stays runc for now** behind a narrow sandbox interface.
- **Root-only tests run on real machines, not CI**, accepting the nightly-job
  debt above.

## 8. Loose ends
- GitHub issue 32 (flush latency) is fixed by PRs 36, 37, 38, 61, and 62 but
  open because the launcher token cannot close issues.
- The Codex fleet's lanes share one ChatGPT usage limit; when it trips every
  running agent stops at once and the instances are retained for resume.
- `TODO.md` is the map and `docs/DESIGN.md` the design; section 9 of the
  design was rewritten for providers, but sections 4.6 (guest agent) and 8.3
  still describe a `swarmy-guest` that was never built; `runc exec` plus
  helpers in the image replaced it.

## 9. How to resume

1. Read `AGENTS.md`, this file, and the relevant section of `docs/DESIGN.md`.
2. `tasky --json goal list --project swarmy` for the goals, then
   `task ready --project swarmy` for what can start. Draft goals hold the
   remaining slices; activate one and add tasks only once the plan is aligned.
3. Locally: `make install`, `make dev-tools`, `swarmy dev up`, `swarmy doctor`.
   `swarmy auth ls` shows which providers have credentials in the local store.
4. Against a node: `swarmy remote up NAME`, `swarmy remote connect NAME`,
   `swarmy dev up --remote NAME`, then `swarmy remote down NAME` when done;
   it terminates the instance and deletes its key pair.
5. To check providers end to end: `scripts/providers/smoke.sh` with
   `SWARMY_DEFAULT_IMAGE` set to a registered image and model overrides for
   Bedrock (`us.` inference profile ids), Azure (deployment name), and Google
   (a Gemini 3 model).
