# Working on swarmy

swarmy is infrastructure for running very large numbers of long-lived coding
agents on ordinary cloud compute. The promise: no single machine failure loses
an agent, its conversation, or its disk; turns feel immediate; agents keep
working for weeks with memory, timers, and a persistent computer; any model
provider can serve them. Written in Rust, organized as twenty-six crates in one
workspace.

Read `docs/ARCHITECTURE.md` before changing anything: it explains the concepts, the
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
  loop; measured turn timings live in `docs/volume-benchmarks.md`. The
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
  mounted device, up to node capacity, idle eviction after thirty minutes, rebuild
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
  generated from models.dev and OpenRouter (roughly 790 models, eleven providers
  plus a fake) with one shared effort scale; clients for Anthropic Messages, OpenAI
  Responses, Chat Completions, Gemini, and Bedrock Converse; encrypted
  credentials in the store with fenced refresh; `--provider`, `--model`,
  `--effort` everywhere; usage and cost recorded per completion. Verified
  live on 2026-09-21: anthropic, openai, chatgpt, xai, meta, openrouter,
  azure, amazon-bedrock, google, google-vertex (Gemini).
- **Operating it.** `make dev-tools` and `swarmy dev up` run the stack under
  the checkout with no root (`docs/DEV.md`). `swarmy remote up NAME` launches
  one EC2 node, `swarmy remote adopt NAME` builds on machines already owned
  (Hetzner dedicated servers) with any S3-compatible bucket and static keys,
  and `remote connect` tunnels to either (`docs/REMOTE.md`). CI runs fmt, workspace tests
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
- **Execution.** Tasks run on long-lived swarmy worker agents driven by
  `scripts/fleet/fleet` (see `docs/DEV.md`), one pull request per task, three
  to five in parallel. The orchestrator reviews, reconciles conflicts (one
  subagent per pull request in its own git worktree, with explicit per-file
  rules), merges on green CI, and marks the task done. Parallel tasks on
  shared files (`crates/swarmy-llm/src/lib.rs`, gateway config, `docs/providers.md`)
  always conflict; expect reconciliation tasks so a shared helper has one
  owner.

## The plan, September 2026

Tasky is the source of truth ([Where the plan lives](#where-the-plan-lives)).
Snapshot of the live goal list on 2026-09-30, grouped by state; each goal's
spec in tasky is the agreed design.

Active goals:

| Goal | Delivers |
|---|---|
| control-plane-api | Control plane API and thin client |
| perf-baseline | Performance baseline across dev, dev2, and codex-daytona |
| lifecycle | Task lifecycle stays truthful to what actually happened |
| cleanup-2 | Cleanup 2: delete, deduplicate, enforce |
| hetzner | Hetzner migration: run the fleet on Hetzner dedicated servers |

Draft goals: one-binary-install (one client binary and seamless install),
swarm-model (swarms as first-class: registry, lifecycle, and topology),
persistent-swarms (persistent swarms outlive their compute), cloud-topology
(cloud topology on EC2 with a node provider), client-protocol (published
protocol and client SDK), browser-and-screen (slice 7), provider-breadth-quota
(slice 8).

Completed goals: immortal-echo-agent, block-level-disk, real-coding-agent,
persistent-agent, disk-follow-ups, persistent-computers, persistent-follow-ups,
remote-node, sessions-have-computers, turn-latency, named-agents,
inference-providers, model-selection, operability-batch, dev-fleet, cleanup, v1.

Cancelled: channels (slice 5, see `backlog/chaty.md`), sandbox-pause-resume
(slice 6, see `backlog/gvisor-runtime.md`), cloud-deploy (slice 9, see
`backlog/kubernetes-packaging.md`).

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
- **The fleet is moving from EC2 to Hetzner dedicated servers.** EC2 with
  our own node provider was the first cloud target; the hetzner goal moves
  the fleet to Hetzner dedicated servers with `remote adopt`. Kubernetes is
  a later packaging target; see `backlog/kubernetes-packaging.md`.
- **One client binary, one core binary.** The client never links the
  FoundationDB library; it fetches and ships the matching `swarmy-core`.
- **Swarmy is in development, so format changes are clean breaks, not
  migrations.** Stored formats, APIs, and configuration may change
  incompatibly; delete the legacy code rather than carry it. Record each break
  in `docs/api-breaks.txt` and wipe the development store before deploying
  past one.
- **Root-only tests run on real machines, not CI.** A nightly job on a real
  node was built and then dropped in September 2026: automation plus AWS
  secrets was not worth it at this size. The rule is that whoever changes the
  covered code runs the suites by hand and says so in the pull request.

## Building and testing

The toolchain is pinned in `rust-toolchain.toml`; `rustup show` installs it.
The root `Makefile` holds the only list of CI commands. Each pull-request CI
job runs exactly one `make check-<job>` target, and `make check` runs all of
them in CI order; `make help` lists the targets. Workers run
`cargo test --locked -p <each crate changed>` while iterating, then merge
`origin/master` before the final check and run `make check` once with output
saved to a file and attached to the pull request. Name the crates tested in
the pull request. Before any command expected to take more than ten minutes,
run `git add -A && git commit -m "WIP" && git push`. `make check` needs the dev
stack running (`scripts/dev-stack.sh start`, see `docs/DEV.md`). It builds the
workspace before the end-to-end suites so they find sibling binaries, and it
stops the stack before the script tests, which use the same fixed ports. To
rerun one CI job, run its target, for example `make check-lint`.

The feature-enabled commands always run in CI: the provisioning client is an
opt-in feature that the workspace commands leave off. The CLI remote step skips
the self-managed dev-stack test already covered by the workspace step. The e2e
binaries run serially within each of two parallel CI jobs. Advisory checks
(`make check-advisories`) run on a weekly schedule rather than blocking pull
requests.

Clippy runs with the `all` and `pedantic` groups denied, so write code that
satisfies it rather than silencing it. `unsafe_code` is denied
workspace-wide; the two places that need it (the FoundationDB boot and the
NBD ioctl wrapper) opt in per function with a reason and a SAFETY comment.

### Enforced by tools

The authoritative list of mechanical checks; REVIEWER.md does not repeat it.
The workspace lint table lives in the root `Cargo.toml` `[workspace.lints]`
(every crate sets `[lints] workspace = true`); numeric thresholds and
test-only exemptions live in the root `clippy.toml`. `make check-lint` runs
every Clippy pass CI runs, including the `remote`-feature and
no-default-features passes.

- `clippy::allow_attributes_without_reason`: every `allow` carries a
  `reason = "..."`. Prefer `#[expect(lint, reason = "...")]` so the build
  fails if the exception goes stale; `#[allow(lint, reason = "...")]` stays
  legal only where the lint fires under some feature combinations and not
  others.
- `unreachable_pub`: no `pub` wider than its crate (or parent module) can
  reach. Binaries and private modules use `pub(crate)` or `pub(super)`.
- `unused_qualifications`: paths use the shortest form their imports allow.
  Fix mechanically with the compiler suggestion.
- `redundant_clone`, `needless_collect`, `large_stack_frames`: no cloning
  when ownership would do, no collecting only to iterate, no oversized
  stack frames.
- `clippy::todo`, `clippy::unimplemented`, `clippy::dbg_macro`: none of
  these land in the tree.
- `unsafe_code` (rustc): denied workspace-wide; the FoundationDB boot and the
  NBD ioctl wrapper opt in per function with a reason and a SAFETY comment.
- `disallowed_methods` (test targets only, in `clippy.toml`): no fixed sleeps
  in tests; each test target opts in with `#![deny(clippy::disallowed_methods)]`
  at its root. Poll with `swarmy_testkit::eventually`, which names the awaited
  condition and fails loudly on timeout. A test whose assertion is silence
  over a window keeps its sleep with an `expect` and a reason.

These structural checks fail CI rather than asking for exceptions.
`make check-lint` runs them the same way CI does:

- `scripts/check-anyhow-in-libraries.sh`: library crates use `thiserror`,
  never the `anyhow` package in `[dependencies]`, including under a renamed
  key or through `[workspace.dependencies]` (`swarmyd` counts as a binary: its
  `lib.rs` declares no modules and it has a binary target). Blocking,
  milliseconds.
- `scripts/check-test-sleep-ban.py`: every test root and test module carries
  `#![deny(clippy::disallowed_methods)]`, so fixed sleeps fail the build.
  Blocking, seconds.
- The ast-grep rules in `ast-grep/rules/` (ast-grep matches Rust syntax
  trees; the Makefile pins version 0.45.3 and runs it through `npx`):
  `no-spawn-in-libraries` (`tokio::spawn`, `tokio::task::spawn`, and
  one-argument `.spawn(task)` calls such as `JoinSet::spawn`),
  `no-stringified-errors` (`.map_err(|e| e.to_string())`,
  `.map_err(ToString::to_string)`, and `.map_err(|e| format!(..))`),
  `no-unwrap-in-libraries`, `no-print-in-libraries`, and
  `no-unchained-error-logs` (a tracing macro that keeps an error without
  logging its cause chain). Blocking, under a second. Test modules
  (`#[cfg(test)] mod ...`), integration tests, test support, binaries, and
  entry points are out of scope for the print/spawn/unwrap/stringify rules
  (the error-chain rule covers binaries too, with only tests out of scope);
  their shared `ignores:` list must be identical in every rule
  (`scripts/check-ast-grep-rules.sh`). `ast-grep test` runs each rule's
  passing and failing cases in `ast-grep/rule-tests/`. There are no per-file
  exceptions: an allowed call carries `// ast-grep-ignore: <rule-id>` on its
  own line directly above it, after a comment giving the reason, and a
  suppression that no longer matches anything fails the scan. Prefer fixing
  the code (`?` or `expect` with the invariant, `tracing` or a return value
  instead of printing, an owned task handle); a new suppression is a lint
  exception and must meet the bar below.
- Clone report (`clone-report` CI job; locally
  `npx --yes jscpd@5.3.3 --config .jscpd.json`): advisory numbers in the job
  summary for `REVIEWER.md`'s duplication checklist, never a gate.
- Error logging keeps the cause chain: services log
  `swarmy_core::error_chain(&error)` in an `error` field wherever they keep
  an error instead of returning it, so the log names the underlying failure
  instead of only the top-level message. The `no-unchained-error-logs`
  ast-grep rule enforces this. A `#[source]` variant must not also
  print its source in its message.
- `make check-deps`: `cargo deny check licenses bans sources` and
  `cargo machete --with-metadata`, both blocking. `cargo deny check advisories`
  (`make check-advisories`) runs weekly and does not block pull requests.
  `cargo doc` runs with `-D warnings` in `make check-lint`.

Deliberately not enforced by Clippy: `unwrap_used`, `print_stdout`, and
`print_stderr`. `allow-unwrap-in-tests` covers only `#[cfg(test)]` code, so
denying `unwrap_used` would need a per-file exception in each of the 67
integration-test files that idiomatically panic on failure; the print denies
would need one in each of the 15 test and 7 example files that log skip
diagnostics and progress, plus the same boilerplate in every new test file.
The production half is enforced instead by the `no-unwrap-in-libraries` and
`no-print-in-libraries` ast-grep rules above, which exclude test modules and
files structurally and need no per-test-file exceptions. (`clippy.toml`
carries no `allow-unwrap-in-tests`: the setting does nothing while
`unwrap_used` is not denied.)

### Lint exceptions

The bar for an exception is high. An exception is acceptable only when all of
these hold:

1. The lint is wrong for this code, not merely inconvenient. "The function is
   long" is not a reason; "this is a flat match over every protocol event and
   splitting it would scatter one table across files" can be.
2. You tried the fix the lint asks for, and it made the code harder to read or
   less correct. Say what you tried.
3. The exception covers one item (a function, a statement, a match arm),
   never a module or crate, unless the whole module genuinely is the
   exception (for example a CLI output module and `print_stdout`).
4. It is written as `#[expect(clippy::lint_name, reason = "...")]` with the
   reason in full, so the build fails if the exception stops being needed.

Never add an exception to get a task finished, never loosen a lint or a
threshold in `Cargo.toml`, `clippy.toml`, or CI, and never split a function
into pieces whose only purpose is to get under a limit. If you believe a lint
is wrong for the whole codebase, say so in the pull request description and
leave the lint as it is; the operator decides. Reviewers apply this bar using
`REVIEWER.md`.

### Enforced by complexity, nesting, and error lints

Complexity, nesting, and swallowed errors fail the build through the same
workspace lints and `clippy.toml` thresholds, checked with the clippy
commands above.
- `cognitive_complexity` (denied at 25): split over-limit functions by
  concern, never by line count. `too_many_lines` (100) and
  `too_many_arguments` (7) ride along with pedantic at the owners' defaults.
- `excessive_nesting` (denied at 6): extract the inner block into a named
  helper. Five was measured but flags over a hundred mostly-legitimate
  sites; at six only real offenders fail, and every one was fixed by
  extraction with no exceptions.
- `let_underscore_must_use` and `unused_result_ok` (denied): route every
  best-effort ignore through `swarmy_core::ignore_best_effort(result,
  "what was attempted")`, which logs the failure at `debug`.
  `map_err_ignore` stays allowed on purpose: its sites translate a
  low-level error into a domain error, and naming the discarded source at
  each site would be noise, not information.
- Infallible `String` writes use `write!(...).expect("writing to String
  cannot fail")`, not the best-effort helper. The config `duration` module
  stays under 96 lines through one shared checked constructor for TOML and
  environment inputs.

Integration tests get FoundationDB, NATS, and SeaweedFS from
`scripts/dev-stack.sh start` (see `docs/DEV.md`, "Environment and tests").
Tests must skip cleanly, not fail, when the relevant environment variable is
absent locally; CI must fail when a required stack setting is missing.

Some suites need root and a real kernel, so they skip on CI's hosted runners
and are never run automatically. They are run by hand, by whoever changes the
code they cover, before the pull request is opened. Fleet sandboxes are
dedicated servers with root and the NBD module loaded, so run them there with sudo:

| If you changed | Run as root |
|---|---|
| `swarmy-volume` (chunks, manifests, NBD, snapshots) | `swarmy-volume --test nbd`, `swarmy-volume --test image` |
| image recipes or `swarmy image` | `swarmy-cli --test image` (starts its own API against the dev stack), `swarmy-volume --test image` |
| `swarmyd vol` or the volume server | `swarmyd --test vol` |
| `swarmyd`, `swarmy-sandbox`, `swarmy-tools`, or the tool helpers | `swarmyd --test node`, then the chaos suites below |
| the worker, scheduler, gateway, store, or bus | `swarmy-chaos --test bash`, `--test continuity`, `--test coding`, and `scripts/chaos-ci.sh` |

`swarmy image build` talks to the API, so register the test image through a
service started against the dev stack. After `scripts/dev-stack.sh start` and
`source .dev/env`, start `./target/debug/swarmy-api` with `SWARMY_API_LISTEN`
set to a loopback address and `SWARMY_API_TOKEN` set to any string, export
`SWARMY_API_URL` and the same token, then run `sudo -E ./target/debug/swarmy
image build images/base-ubuntu --tag dev`. The suite command shape is then:

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
- Before starting a task, remove other directories under `~/work` only when
  their branch is pushed and they have no uncommitted changes; never delete
  unpushed work. Run `cargo clean` if `~/.cargo-target` is over 40 GiB. The target
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
- Before marking a pull request ready, check it against `REVIEWER.md`; the
  reviewer will.
- Commit messages have an imperative subject line and a body explaining why.
- The pull request description says what was built, lists the exact commands
  you ran to validate it with their results, and notes anything from the test
  plan you could not verify in the sandbox and why.
- Never commit secrets, `.dev/`, or `target/`.
- This repository is public. Never commit, or write into a pull request
  description, comment, or CI log, anything that identifies our running
  infrastructure: credentials and tokens of any kind, IP addresses and host
  names of real machines, cloud account ids, instance, volume, subnet, and
  security-group ids, ARNs, bucket names, SSH keys or known-hosts entries, and
  the contents of `.swarmy/`, `scripts/fleet/fleet.toml`, or `/etc/swarmy/`.
  Benchmark and proof records use placeholders such as `<account-id>`,
  `<swarm-bucket>`, and `i-<redacted>`, and name machines by their role
  (`dev2-3`, "the suite node"). Public identifiers that are not ours, such as
  a stock Ubuntu image id, are fine. Before pushing, check the diff with
  `git diff origin/master | grep -nE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b|[0-9]{12}|\bi-0[0-9a-f]{8,}|vol-[0-9a-f]'`
  and remove any real value it finds (loopback and documentation addresses
  such as `127.0.0.1` are fine).

## Operating notes

- The operator's laptop is a control plane only: it runs the fleet driver,
  tasky, gh, ssh, and short swarmy CLI calls. Cargo builds, clippy, and tests
  run on fleet workers, on the swarm nodes, or in CI, never on the laptop.
  To update the laptop CLI, build it on the dev node with full features and
  copy the binary back, or use a CI-built binary.

- Subscription limits park workers instead of stopping them: see
  [Subscription limits](docs/fleet-runbook.md#subscription-limits).
- After switching branches, run `cargo build --workspace` before trusting a
  fixture failure; sibling binaries launch from `target/debug`.
- Small chores that belong to no goal are in `backlog/housekeeping.md`.

## How to resume

1. Read this file and the relevant section of `docs/ARCHITECTURE.md`.
2. Pick up work from the plan ([Where the plan lives](#where-the-plan-lives)).
3. Local bring-up, remotes, and provider checks live in `docs/DEV.md`,
   `docs/REMOTE.md`, and `docs/providers.md` and are not repeated here.
   (Those commands change under the swarm-model goal; its docs task updates
   those documents.)
