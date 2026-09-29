# Reviewing swarmy pull requests

This is the checklist a reviewer (a person, or an agent asked to review) applies
before a pull request merges. It exists because most of swarmy is written by
coding agents, and agent-written code drifts in predictable ways: the same job
done in several places, layers that only pass calls through, errors turned into
strings or dropped, tests that cannot fail, and exceptions added to get past a
lint. Tools catch some of this (see "What the tools already check" at the end);
the rest is judgement, and this file is where that judgement is written down.

Review against the task the pull request claims to implement, then against
this list. A finding is either fixed before merge or recorded as a follow-up
task with its reason; "we'll clean it up later" without a task is not an
outcome.

## Principles

- **Swarmy is in development: no backwards compatibility.** Old stored-format
  generations, migrations, legacy read paths, compatibility API routes, and
  deprecated config keys are deleted, not carried. A format change may be a
  clean break recorded in `docs/api-breaks.txt`; the operator wipes the
  development swarm's store when it deploys.
- **Duplication is the most serious defect.** When a pull request writes code
  that already exists elsewhere, or leaves two ways of doing the same job, it
  is not ready.
- **Delete before you add.** The best cleanup removes code. A refactor that
  moves 500 lines into new files without shrinking them has not cleaned
  anything.
- **Typed, not stringly.** Data keeps its type from the store to the terminal.
  Nothing is flattened to a string or `serde_json::Value` and parsed back.
- **Enforce mechanically where it is cheap and exact; review the rest.**

## Checklist

### Scope

- [ ] The change does what the task asks and nothing unrelated. Drive-by
      refactors go in their own pull request.
- [ ] The pushed commits exist on the branch and contain the work the
      description claims (agents sometimes report work they did not push).
- [ ] The description lists what was deleted, the net line change, the exact
      validation commands with results, and which root-only suite the change
      could affect.

### Duplication

- [ ] No new helper duplicates one that exists elsewhere in the workspace:
      search for the job, not the name (retry, backoff, key building, scans,
      error classification, bootstrap, tracing setup, id sanitising).
- [ ] No copy-pasted block with small edits; similar code is one function
      with a parameter.
- [ ] No second representation of the same data (a `Foo` and a `CliFoo` with
      the same fields, a view struct that copies a stored struct field by
      field without a reason).
- [ ] One way to do each job per crate: if the pull request introduces a new
      pattern, it removes the old one.

### Deletion and dead code

- [ ] Code the change made unused is deleted in the same pull request.
- [ ] No legacy, compatibility, or migration path is added or kept.
- [ ] No `pub` wider than needed; test-only helpers live behind
      `cfg(any(test, feature = "test-support"))`.
- [ ] No commented-out code, no `TODO` without a tasky task id.

### Structure

- [ ] Functions are split by concern, not by line count. A helper that exists
      only to get under a length limit (called once, named `part_two`,
      `apply_rest`, or similar) is rejected.
- [ ] Modules are deep: a small interface over real work. No pass-through
      layers that forward calls unchanged, no trait with a single
      implementation that is never used as a trait object.
- [ ] Service logic lives in the library; `main.rs` wires things together.
- [ ] Records are structs, not tuples; a function taking several booleans takes
      an enum or a small options struct instead.
- [ ] New crates and cross-crate dependencies respect the tiers in
      `docs/ARCHITECTURE.md`.

### Types and errors

- [ ] Libraries return typed errors (`thiserror`) whose variants are the cases
      callers act on; `anyhow` appears only in binaries.
- [ ] No error is turned into a string for a caller to parse back, and no
      caller matches on error text.
- [ ] No error is silently dropped. Best-effort cleanup (killing a process,
      removing a temp file, sending to a closed channel) logs at `debug` or
      above, or says in a comment why losing it is harmless.
- [ ] Conversions between our own enums are exhaustive matches, never a
      `_ =>` catch-all or a serde round trip that turns an unknown variant
      into a default.
- [ ] Ids, paths, durations, efforts, and kinds use their types, not `String`
      or bare integers.

### Concurrency

- [ ] No detached `tokio::spawn` from library code; background work is owned,
      bounded, and flushed or cancelled on shutdown.
- [ ] No lock held across an `.await` on I/O; caches are bounded.
- [ ] Channels are bounded unless the pull request explains why not.

### Constants and configuration

- [ ] Durations, size limits, retry counts, and thresholds are named
      constants in the crate that owns the behaviour, defined once.
- [ ] New settings are typed fields in the right settings group, with the
      environment variable and docs updated in the same pull request.

### Stored formats and the API

- [ ] Every stored struct has a fixed-bytes test for its current format; a
      format change updates that test and records the break in
      `docs/api-breaks.txt`.
- [ ] API changes regenerate `docs/openapi.json`; nothing is advertised in
      the schema that the server never sends.

### Tests

- [ ] New behaviour has a test that goes through a public interface and would
      fail if the behaviour broke. Ask: what change to the code would this
      test miss?
- [ ] Every new test codifies a requirement: something the application, API,
      client, or services must do, stated so that a failure tells someone a
      real behaviour broke. Reject tests of incidental properties:
    - asserting what the test just set up (a constructor returns its fields,
      a default equals the literal copied from the code);
    - a serde or encode-decode round trip with no pinned bytes;
    - testing the language, the standard library, or a dependency;
    - pinning log wording, internal error text, `Debug` output, or a private
      helper's intermediate value that nothing depends on;
    - computing the expected value with the same code or formula under test;
    - a near-copy of an existing test with trivially different inputs;
    - a private-function test whose behaviour the public interface already
      covers;
    - asserting only `is_ok()` or "does not panic" where the value matters.
- [ ] No test was weakened to pass: no loosened assertion, no new `#[ignore]`,
      no `is_ok()` where the value matters.
- [ ] No fixed `sleep`; waits poll a condition with a budget. Time-dependent
      code takes a clock instead of reading the wall clock.
- [ ] The test runs somewhere: in CI, or in a named root suite. A test gated
      on an environment variable that nothing sets is dead.
- [ ] Assertions in multi-step tests say which step failed.
- [ ] Setup uses the shared test support rather than a new copy of a
      fixture.

### Lint exceptions

- [ ] Every exception meets the bar in AGENTS.md ("Lint exceptions"): the
      lint is wrong for this code, not inconvenient; the fix would make the
      code worse; the reason says why; the scope is one item.
- [ ] Complexity, length, argument-count, and nesting exceptions are rare:
      accept one only for a flat dispatch table or a state machine whose
      steps would be harder to follow if split, and say so in review.
- [ ] No lint was turned off or loosened in `Cargo.toml`, `clippy.toml`, or
      CI to get the change through.

### Comments and docs

- [ ] Comments explain why, not what; no comment restates the next line.
- [ ] Docs, `AGENTS.md`, crate READMEs, and `docs/ARCHITECTURE.md` are
      updated in the same pull request as the behaviour they describe, and
      no doc still names something the change removed.
- [ ] References to outside code (for example the Pi agent's source) pin a
      commit, not a line number on a moving branch.
- [ ] No section sign, no analogies (see "Writing" in AGENTS.md).

### Public repository

- [ ] No credentials, host names, IP addresses, account, instance, volume,
      subnet or security-group ids, ARNs, or bucket names in code, docs,
      commits, the description, or CI logs.

### Verification

- [ ] CI is green on the head being merged.
- [ ] If the change touches code a root-only suite exercises (node daemon,
      sandbox, volume, image, stored formats, worker recovery), that suite
      passed on the exact head, pinned with `suite-queue.sh --at`.

## What the tools already check

Do not spend review time on what these enforce; do check that none of them was
weakened. The authoritative list is the "Enforced by tools" part of AGENTS.md.

- rustfmt; Clippy `all` and `pedantic` denied, plus the extra lints listed in
  the root `Cargo.toml` `[workspace.lints]`, with thresholds in `clippy.toml`.
- `unsafe_code` and `unreachable_pub` denied.
- CI scripts for public-repository identifiers, unused dependencies, supply
  chain, documentation build, and the OpenAPI compatibility check.
