# Swarmy development image

`swarmy image build images/swarmy-dev` builds and registers `swarmy-dev:dev`;
pass `--tag NAME` for another tag, and `swarmy remote up NAME --image-recipe
images/swarmy-dev` builds it on a node as that node's default image. The
recipe uses the same Ubuntu Noble source and package set as `base-ubuntu`,
plus Clang and libclang for the workspace's FoundationDB bindings. It runs the
pinned checkout's `images/base-ubuntu/setup.sh` to keep the base image's
`agent` account and GitHub credential helper. No credential is copied in.

What the setup script adds, all owned by `agent`:

- The repository's pinned Rust toolchain through rustup, with rustfmt and
  clippy from `rust-toolchain.toml`.
- The FoundationDB server, CLI, and client library, the NATS server, and
  SeaweedFS under `/home/agent/.local`, through `scripts/install-dev-tools.sh`,
  so `scripts/dev-stack.sh start` works inside a sandbox without root.
- The dependency sources for the pinned commit, through `cargo fetch`, so a
  task never downloads from crates.io at start.
- A `.profile` and `.bashrc` that put Cargo and the local binaries on `PATH`,
  set `SWARMY_FDB_LIB_DIR`, and point `CARGO_TARGET_DIR` at
  `/home/agent/.cargo-target`, outside any clone.

The image deliberately holds no compiled artifacts. Fleet workers are
long-lived agents: each task is a fresh clone in its own directory, but the
shared target directory on the worker's persistent disk stays warm from task
to task. The first build on a new worker is cold (about ten minutes for the
workspace); later ones reuse what the previous task built. That keeps the
image at a few gigabytes instead of the tens of gigabytes a warmed target
directory would add, and the cache tracks whatever the worker last built
rather than a commit pinned in this recipe.

The image has a 32 GiB virtual disk, enough for the toolchain and tools,
several clones, and a target directory that the worker prunes when it passes
20 GiB (see the fleet worker rules in `AGENTS.md`).

## Rebuilding from a new source commit

Update `source_commit` in `recipe.toml` to the new full commit hash and set
`source_date_epoch` to that commit's Unix timestamp (`git show -s
--format=%ct COMMIT`). The commit must be reachable from the public
repository at build time. Keep the package set aligned with
`images/base-ubuntu/recipe.toml` and the service versions with
`scripts/install-dev-tools.sh`. Upstream downloads are live; use a frozen
mirror if byte-for-byte reproduction across upstream updates is required.
