# Working on swarmy

swarmy is infrastructure for running very large numbers of long-lived coding
agents on ordinary cloud compute. The promise: no single machine failure loses
an agent, its conversation, or its disk; turns feel immediate; agents keep
working for weeks with memory, timers, and a persistent computer; any model
provider can serve them. Written in Rust, organized as sixteen crates in one
workspace.

Read `docs/DESIGN.md` before changing anything: it explains the concepts, the
services, and the vertical slices the work is organized into. This file is
the map: what exists, where the plan lives, the decisions behind it, and how
to resume. When this file and the design disagree, this file describes what
is true today and the design lags. Each pull request implements one task from
the plan in tasky, and the task text you were given is the source of truth
for scope.

## What exists today

Everything here is merged and has been exercised on real machines. Pull
request numbers give the history; the documents under `docs/` hold the
measured numbers with raw samples.

- **Control plane.** FoundationDB is the source of truth (`swarmy-store`);
  every write that matters checks its lease or epoch in the same transaction.
  NATS JetStream carries work queues and live feeds (`swarmy-bus`); acking
  never means owning the work. Scheduler, worker, and gateway run the step
  loop; p95 for a text turn is 18 ms locally and 26 ms over SSH tunnels. The
  chaos suite (`swarmy-chaos`) kills processes mid-turn and checks every
  session log stays contiguous with exactly one inference per step.
- **Disks and computers.** Every agent disk is 256 KiB blake3-addressed
  chunks in object storage with two-level manifests, served to nodes over
  NBD, snapshotted every ten minutes when dirty without freezing, cloned in
  constant time (`swarmy-volume`, `docs/volume-benchmarks.md`). Images are
  built from a directory, shell, debootstrap, or OCI recipe; `images/
  base-ubuntu` is the one image every agent uses. The collector marks with a
  Bloom filter and sweeps 256 prefixes under a run lease
  (`docs/gc-benchmarks.md`). Nodes (`swarmyd`) host runc containers from the
  mounted device, sixteen slots, idle eviction after thirty minutes, rebuild
  on another node after failure. Every session has a computer: ephemeral
  sessions own a throwaway one, named agents share one persistent one.
- **Agents.** Named agents with a main conversation and side sessions,
  per-agent prompt, model, effort, and provider, memory files under
  `/home/agent/memory`, summarization at three quarters of the context
  window, timers, and a private GitHub token reaching git over a socket
  (`docs/agent-lifecycle.md`). Tools (`swarmy-tools`): bash with a 10 s yield
  and background continuation, process control, read, write, edit (exact
  match with whitespace fallbacks, no fuzzy matching), glob, grep, ls,
  web_fetch, checkpoint, update_plan, timers, get_time. Proof: an agent
  cloned this repository and opened a pull request through a node kill in
  146 s (`docs/proofs/`).
- **Inference providers** (`swarmy-llm`, `docs/providers.md`). A catalog
  generated from models.dev and OpenRouter (770 models, eleven providers)
  with one shared effort scale; clients for Anthropic Messages, OpenAI
  Responses, Chat Completions, Gemini, and Bedrock Converse; encrypted
  credentials in the store with fenced refresh; `--provider`, `--model`,
  `--effort` everywhere; usage and cost recorded per completion. Verified
  live on 2026-09-21: anthropic, openai, chatgpt, xai, meta, openrouter,
  azure, amazon-bedrock, google, google-vertex (Gemini).
- **Operating it.** `make dev-tools` and `swarmy dev up` run the stack under
  the checkout with no root (`docs/DEV.md`). `swarmy remote up NAME` launches
  one EC2 node and `remote connect` tunnels to it, about eleven minutes from
  nothing to a usable node (`docs/REMOTE.md`). CI runs fmt, workspace tests
  against a live dev stack, pedantic clippy, and reduced chaos; root-only
  suites run with sudo on real nodes.

## Where the plan lives

- **tasky** holds the plan: `tasky --json goal list --project swarmy` for the
  goals, `tasky --json task ready --project swarmy` for what can start. One
  goal per outcome, one task per mergeable change with a body and a test
  plan. Activate a goal only when the one before it is merged.
- **`backlog/`** holds items that are understood but not planned, one file
  each with what, why, why not now, what it would take, and the trigger that
  brings it back. `backlog/README.md` is the index. When an item is picked
  up it becomes a tasky goal and the file is deleted.
- **Execution.** Tasks are executed by Codex agents on their own EC2
  instances through `~/code/codex-daytona`, one pull request per task, three
  to five in parallel. The orchestrator reviews, reconciles conflicts (one
  subagent per pull request in its own git worktree, with explicit per-file
  rules), merges on green CI, and marks the task done. Parallel tasks on
  shared files (`swarmy-llm/src/lib.rs`, gateway config, `docs/providers.md`)
  always conflict; expect reconciliation tasks so a shared helper has one
  owner.

## The plan, September 2026

Completed goals: immortal-echo-agent, block-level-disk, disk-follow-ups,
persistent-computers, persistent-follow-ups, remote-node,
sessions-have-computers, turn-latency, named-agents, real-coding-agent,
persistent-agent, inference-providers, model-selection.

The current plan, in dependency order. Each goal's spec in tasky is the
agreed design. The two active goals come first because the Codex fleet that
executes tasks shares one ChatGPT subscription and is the bottleneck; once
dev-fleet lands, the rest of the plan is executed by swarmy agents on
OpenRouter models and the subscription side by side.

| Goal | Delivers |
|---|---|
| operability-batch (active) | Collector batching, `run` exit code, NATS test flake; the nightly root-suite job was dropped in favour of the manual rule in Building and testing |
| dev-fleet (active) | Swarmy as its own development fleet: the `swarmy-dev` image, rate limits as waits with a circuit breaker, a fleet driver over the CLI with tasky integration, a sized long-lived swarm with a runbook and a proof on real tasks over OpenRouter and ChatGPT |
| control-plane-api | The swarmy API (HTTP, JSON, SSE) as the only thing a client talks to; the CLI as a thin client; doctor and chat read live service health |
| one-binary-install | `swarmy` client and `swarmy-core` multi-call binary; signed releases; the client fetches and ships the core; install script, Homebrew, cargo-binstall |
| swarm-model | Swarms as the unit of deployment: registry, `swarm create/up/down/stop/start/status/ls/use`, local and split topologies, sudo sandboxes on Linux, docs rewrite |
| persistent-swarms | Chunks in S3 with an instance role, the store on a surviving EBS volume, `stop` and `start` |
| cloud-topology | Everything in the cloud: node provider with EC2, control plane behind a TLS API, `swarm scale`, a clean-machine proof |
| client-protocol | Served API docs with a compatibility check, conformance suite and published SDK crates, WebSocket sandbox attach |

Draft goals from the original slices that are not yet scheduled, in the
order they should follow the plan above: provider-breadth-quota (slice 8's remaining half, spec rewritten in
September 2026: auth entries and pools, routes as the rule system, failover
at turn boundaries, rate limits as retryable waits with a circuit breaker
instead of admission control, metering rollups and cost and quota views;
scheduled after cloud-topology), browser-and-screen (slice 7, widened in
September 2026 to graphical and GPU sandboxes: image inputs, a memory budget
per node replacing the slot count, a display image with software rendering,
browser tools with accessibility snapshots, a GPU node shape with shared and
dedicated modes, and a spike on GPU displays; scheduled after the provider
goal). Slice 6, sandbox pause and resume, was cancelled: its live half
(idle eviction, dead-node rebuild with a notice, cold start) already exists
and memory pause is not needed for the core functionality; it is noted in
`backlog/gvisor-runtime.md` as something to explore.
Slice 5, channels, was cancelled and will be built as a standalone chat tool
for agents, chaty, integrated into swarmy later; see `backlog/chaty.md`.
Slice 9, cloud deploy on Kubernetes, was cancelled in favor of the EC2
cloud-topology goal and recorded in `backlog/kubernetes-packaging.md`.

| Slice | State |
|---|---|
| 1 Immortal echo agent | done |
| 2 Block-level disk | done, plus follow-ups |
| 3 Real coding agent | done |
| 4 Persistent agent | done |
| 5 Channels | cancelled as a swarmy slice; becomes the standalone chaty tool, see `backlog/chaty.md` |
| 6 Sandbox pause and resume | cancelled; live half exists, memory pause noted in `backlog/gvisor-runtime.md` |
| 7 Browser and screen | widened to graphical and GPU sandboxes; draft goal with eight tasks |
| 8 Provider breadth and quota | breadth done; pools, routes, failover, and metering are a draft goal with eight tasks |
| 9 Cloud deploy | replaced by the cloud-topology goal on EC2; Kubernetes backlogged |

Also done outside the slices: persistent computers with placement, remote
nodes from a laptop, every-session-has-a-computer, turn latency, named
agents, model selection flags.

## Decisions made, and why

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
- **Isolation stays runc for now** behind a narrow sandbox interface. gVisor
  was considered in September 2026 and punted: it does not help the local
  topology (NBD attach needs root either way), its file-gofer cost on the
  NBD-backed disk is unmeasured, and nothing needs process checkpointing yet.
  See `backlog/gvisor-runtime.md`.
- **A swarm is the unit of deployment**, with a lifecycle (ephemeral or
  persistent) and a topology (local, split, cloud) chosen at creation. The
  local topology's sandboxes need sudo on Linux and that is accepted for now.
- **Clients talk only to the control plane API**, over HTTP with JSON and
  server-sent events for streams. Reliability comes from cursors over the
  store's append-only logs, not from the transport; one multiplexed stream
  per client; token deltas opt in; idempotency keys on every mutation; a
  WebSocket only for ephemeral bidirectional traffic. gRPC and WebSocket-first
  were considered and rejected for a browser GUI and third-party clients.
- **EC2 with our own node provider is the first cloud target.** Kubernetes is
  a later packaging target, not the first cloud step; see
  `backlog/kubernetes-packaging.md`.
- **One client binary, one core binary.** The client never links the
  FoundationDB library; it fetches and ships the matching `swarmy-core`.
- **Root-only tests run on real machines, not CI.** A nightly job on a real
  node was built and then dropped in September 2026: automation plus AWS
  secrets was not worth it at this size. The rule is that whoever changes the
  covered code runs the suites by hand and says so in the pull request.

## Building and testing

The toolchain is pinned in `rust-toolchain.toml`; `rustup show` installs it.
Every pull request must pass the same three commands that CI runs:

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Clippy runs with the `pedantic` group denied, so write code that satisfies it
rather than silencing it. If a lint is genuinely wrong for a piece of code,
allow that one lint at the narrowest scope with a comment explaining why.
`unsafe_code` is denied workspace-wide; the crates that need it opt in
explicitly and say why in their `Cargo.toml`.

Integration tests that need FoundationDB, NATS, or SeaweedFS get them from
`scripts/dev-stack.sh start`, which writes connection settings to `.dev/env`.
Source that file before running such tests. Tests must skip cleanly, not
fail, when the relevant environment variable is absent.

Some suites need root and a real kernel, so they skip on CI's hosted runners
and are never run automatically. They are run by hand, by whoever changes the
code they cover, before the pull request is opened. Fleet sandboxes are EC2
instances with root and the NBD module loaded, so run them there with sudo:

| If you changed | Run as root |
|---|---|
| `swarmy-volume` (chunks, manifests, NBD, snapshots) | `swarmy-volume --test nbd`, `swarmy-volume --test image` |
| image recipes or `swarmy image` | `swarmy-cli --test image`, `swarmy-volume --test image` |
| `swarmy vol` or the volume server | `swarmy-cli --test vol` |
| `swarmyd`, `swarmy-sandbox`, `swarmy-tools`, or the tool helpers | `swarmyd --test node`, then the chaos suites below |
| the worker, scheduler, gateway, store, or bus | `swarmy-chaos --test bash`, `--test continuity`, `--test coding`, and `scripts/chaos-ci.sh` |

The command shape, after `scripts/dev-stack.sh start` and `source .dev/env`
and with an image registered by `sudo -E ./target/debug/swarmy image build
images/base-ubuntu --tag dev`:

```sh
sudo -E env SWARMY_TEST_IMAGE=base-ubuntu:dev "$(command -v cargo)" test --locked \
  -p swarmy-volume --test nbd -- --test-threads=1
```

Say in the pull request which root suites you ran and their results, or that
the change touches none of the areas above. A reviewer treats a change in one
of those areas with no root-suite result as unverified.

A swarmy fleet worker cannot run them: its sandbox has no sudo and no NBD
devices, and `swarmy image build` fails there. If you are such a worker and
your change touches a covered area, say so plainly in the pull request and
list the suites that need running; the operator runs them on the node before
merging.

## Working as a fleet worker

Development tasks run on long-lived swarmy agents named `worker-N`, driven
by `scripts/fleet/fleet`. A worker's computer persists between tasks, which
is what keeps its cargo cache warm, and it is also why the worker has to
keep its own disk in order. The rules, which the task prompt repeats:

- Every task is a fresh clone under `~/work/<task suffix>` and one new
  branch `swarmy/<task suffix>` from `origin/master`. Never reuse a
  directory or a branch, and never force-push.
- Before starting a task, remove the other directories under `~/work` and
  run `cargo clean` if `~/.cargo-target` is over 40 GiB. The target
  directory is shared across clones and lives outside them.
- Leave nothing uncommitted at the end, and stop the dev stack with
  `scripts/dev-stack.sh stop` so its processes and ports are free for the
  next task.
- Memory files under `/home/agent/memory` are yours to keep across tasks:
  record what you learn about this repository's tests, tools, and reviewers.
- If the disk is in a state you cannot repair, say so in your last message;
  the operator resets the worker with `scripts/fleet/fleet reset worker-N`.

## Code conventions

- Rust 2024 edition. Add dependencies to `[workspace.dependencies]` in the root
  `Cargo.toml` and reference them with `workspace = true` from crates.
- Shared types live in `swarmy-core`. Crates are named `swarmy-<thing>`.
- Prefer small, explicit types over stringly typed values. Identifiers are
  ULIDs wrapped in newtypes, and request ids are blake3 hashes.
- Use `thiserror` for library errors and `anyhow` only in binaries.
- Use `tracing` for logs, never `println!`, except in CLI output paths.
- Keep `Cargo.lock` committed and up to date.

## Writing

- Never use the section sign symbol (U+00A7) anywhere: code, comments, docs,
  commit messages, or pull request text. Write "section" instead.
- Write comments and docs in plain English with straightforward sentences.
  Explain why, not what, and do not use analogies.

## Pull requests

- One task per pull request, on the branch the launcher created. Do not touch
  files outside the task's scope, and do not weaken lints, tests, or CI.
- Commit messages have an imperative subject line and a body explaining why.
- The pull request description says what was built, lists the exact commands
  you ran to validate it with their results, and notes anything from the test
  plan you could not verify in the sandbox and why.
- Never commit secrets, `.dev/`, or `target/`.

## Operating notes

- The Codex fleet's lanes share one ChatGPT usage limit; when it trips every
  running agent stops at once and the instances are retained for resume. The
  provider quota goal removes this.
- After switching branches, run `cargo build --workspace` before trusting a
  fixture failure; sibling binaries launch from `target/debug`.
- Small chores that belong to no goal are in `backlog/housekeeping.md`.

## How to resume

1. Read this file and the relevant section of `docs/DESIGN.md`.
2. `tasky --json goal list --project swarmy` for the goals, then
   `tasky --json task ready --project swarmy` for what can start. Activate the
   next goal only when the one before it is merged.
3. Locally: `make install`, `make dev-tools`, `swarmy dev up`, `swarmy doctor`.
   `swarmy auth ls` shows which providers have credentials in the local store.
   (These commands change under the swarm-model goal; its docs task updates
   this section.)
4. Against a node: `swarmy remote up NAME`, `swarmy remote connect NAME`,
   `swarmy dev up --remote NAME`, then `swarmy remote down NAME` when done;
   it terminates the instance and deletes its key pair.
5. To check providers end to end: `scripts/providers/smoke.sh` with
   `SWARMY_DEFAULT_IMAGE` set to a registered image and model overrides for
   Bedrock (`us.` inference profile ids), Azure (deployment name), and Google
   (a Gemini 3 model).
