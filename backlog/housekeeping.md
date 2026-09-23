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
- **Placement affinity to the last node.** Prefer the node that last hosted a
  computer, because its chunk cache is warm and rehydration is faster than a
  cold start. Small, independent of any runtime change, and the one piece of
  the cancelled pause-and-resume slice worth doing on its own. Do it when
  someone is next in the placement code, most likely during the memory
  budget task of the graphical sandboxes goal.
- **Shrink the swarmy-dev image.** Its warm cargo target directory is about
  30 GiB of the 35.6 GiB image because the debug build keeps full debug info,
  `--all-targets` links one binary per test file, and incremental caches are
  on. Set `CARGO_INCREMENTAL=0` and line-tables-only debug info for the warm
  build in `images/swarmy-dev/setup.sh`, and consider dropping
  `--all-targets`. Do it once the fleet proof shows how the image behaves on
  a node.
