# Working on swarmy

swarmy is infrastructure for running very large numbers of long-lived coding
agents on ordinary cloud compute. Read `docs/DESIGN.md` before changing
anything: it explains the concepts, the services, and the vertical slices the
work is organized into. Each pull request implements one task from that plan,
and the task text you were given is the source of truth for scope.

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
