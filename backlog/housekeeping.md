# Housekeeping

Recorded 2026-09-21. Small chores that do not deserve a goal but should not
be forgotten. Do them opportunistically alongside related work.

- **`docs/DESIGN.md` sections 4.6 and 8.3 describe a `swarmy-guest` agent
  that was never built.** `runc exec` plus helpers embedded in the node daemon
  and the image replaced it. Rewrite both sections to describe what exists,
  and remove `swarmy-guest` from the crate list in section 11. Section 9 was
  already rewritten for providers. The natural moment is the documentation
  rewrite in the `swarm-model` goal.
- **GitHub issue 32 (flush latency) is fixed but open.** Fixed by PRs 36, 37,
  38, 61, and 62; the launcher token cannot close issues. Close it by hand.
- **The Codex fleet's lanes share one ChatGPT usage limit.** When it trips,
  every running agent stops at once and the instances are retained for
  resume. This is an operating note for the orchestrator, not a code change;
  it goes away when the provider quota goal gives the fleet key pools.
- **After switching branches, run `cargo build --workspace` before trusting a
  fixture failure.** Sibling binaries launch from `target/debug`. This belongs
  in `AGENTS.md` if it is not there already, and stops mattering once the
  single `swarmy-core` binary exists.
- **The design's section 11 still says the first `NodeProvider` is a
  Kubernetes pool scaler.** It is the EC2 provider from the `cloud-topology`
  goal; update the substrate paragraph when that goal lands and point at
  [kubernetes-packaging](kubernetes-packaging.md).
- **Widening the catalog allowlist.** The catalog is generated from
  models.dev and OpenRouter into checked-in JSON; the generator's allowlist
  decides which providers are included. Widening it is cheap in code and
  expensive in verification: every provider added must be probed live for
  its wire protocol, authentication, reasoning options, and error text
  before it is trusted. Do it one provider at a time when someone asks for
  that provider, and record each in the live-verification table in
  `docs/providers.md`.

- Intermittent failures seen by a fleet worker on 2026-09-24 during a full
  `make check` with the dev stack running: the `swarmy-scheduler` test binary
  aborted with `malloc(): unsorted double linked list corrupted` after all ten
  tests passed (likely the FoundationDB client's network thread at process
  exit), and `unclaimed_dispatch_expires_without_a_rebuild_notice_or_stuck_job`
  in `swarmy-worker` failed once with "lease is absent, expired, or no longer
  matches". Both passed on rerun. Worth a look if they recur in CI.

- After the CLI management commands moved behind the API (PR 132), the
  store-backed `Agent`, `Image`, and `Session` List/Show handlers in
  `crates/swarmy-session/src/runtime.rs` are unreachable from `swarmy` but
  still callable through `swarmy-session` directly. Remove them so there is
  one implementation.

- `chat::named::open_chat_follows_a_summarized_main_with_a_notice`
  (`crates/swarmy-cli/tests/session/chat_named.rs`) timed out once in CI on
  2026-09-24 waiting for the typed message to appear on the successor
  session; it passed on master minutes earlier. Timing-sensitive; widen the
  wait or make the TUI fixture signal readiness before typing.

- Low priority: session logs are kept forever, and tool outputs over 80 KiB
  spill to blobs that stay referenced as long as the session exists. At some
  point truncate long tool outputs in long-term session storage (keep the
  head and tail, drop the middle) or add a retention policy. Not urgent;
  growth is slow.

- The worker's timer tool did not wake an idle session (worker-3,
  2026-09-25, "Timer ... will wake me in 10 minutes"); the session stayed
  idle until the operator resumed it. Check whether timers survive an idle
  transition and whether the scheduler honours them for side sessions.
