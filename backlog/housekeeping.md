# Housekeeping

Recorded 2026-09-21. Small chores that do not deserve a goal but should not
be forgotten. Do them opportunistically alongside related work.

- **The fleet driver still assumes one pull request per task.** Its launch
  prompt (`prompt()` in `scripts/fleet/fleet`) asks the worker to open a pull
  request and to push "WIP" commits, and `collect` and a plain `release`
  look for that pull request. Goals are now built as a batch with no
  per-task pull requests ("The cycle for a goal" in
  `docs/fleet-operator-handoff.md`), so the operator sends an override with
  `fleet resume` after every launch and releases with `--force`. Change the
  prompt to the branch-delivery wording (report the branch, the head SHA and
  the "Done when" output), give `collect` a branch mode, and let `release`
  accept a branch that has been merged into an integration branch.
- **The unbuilt `swarmy-guest` proposal** was retired in the
  [roadmap](../docs/ROADMAP.md); runc exec and node tool helpers are the
  implemented path. No guest crate is planned.
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

- Low priority: session logs are kept forever, and tool outputs over 80 KiB
  spill to blobs that stay referenced as long as the session exists. At some
  point truncate long tool outputs in long-term session storage (keep the
  head and tail, drop the middle) or add a retention policy. Not urgent;
  growth is slow.
