# Swarmy development image

`swarmy image build images/swarmy-dev` builds and registers `swarmy-dev:dev`.
Pass `--tag NAME` to choose another tag. The recipe uses the same Ubuntu Noble
source and package set as `base-ubuntu`, then adds Clang and libclang for the
workspace's FoundationDB bindings. It runs the pinned checkout's
`images/base-ubuntu/setup.sh` to retain the base image's `agent` account and
GitHub credential helper. No credential is copied into the image.

The setup script installs the repository's pinned Rust toolchain through rustup
for `agent`. `rust-toolchain.toml` supplies rustfmt and clippy. CI does not use
cargo-nextest, so this image does not install it. It runs the pinned checkout's
`scripts/install-dev-tools.sh` as `agent`, which installs the FoundationDB
server, CLI, and client library, NATS server, and SeaweedFS under
`/home/agent/.local`. The `.profile` and `.bashrc` add the Cargo and local
binary directories to `PATH`, set `SWARMY_FDB_LIB_DIR`, and point
`CARGO_TARGET_DIR` at `/home/agent/.cargo-target`.

The setup script clones the pinned commit into `/tmp`, runs `cargo fetch
--locked`, `cargo build --workspace --all-targets --locked`, and `cargo build
--workspace --locked` as `agent`, and
removes the clone. Cargo's registry and target directory remain in the agent's
home for later clones. The image has a 64 GiB virtual disk to hold those
artifacts. A local dev stack starts without root from any fresh checkout:

```sh
scripts/dev-stack.sh start
source .dev/env
cargo test --workspace --locked
```

## Rebuilding from a new source commit

Update `source_commit` in `recipe.toml` to the new full commit hash. The image
builder passes it to `setup.sh`. Set `source_date_epoch` in the recipe
to that commit's Unix timestamp (`git show -s --format=%ct COMMIT`). The commit
must already be available from the public repository at build time. Keep the
base package set aligned with `images/base-ubuntu/recipe.toml` and the service
versions in `scripts/install-dev-tools.sh`. The Ubuntu archive and upstream
tool downloads are live; use a frozen mirror or archived artifacts if byte-for-byte
reproduction across future upstream updates is required.

## Measurements

Measured on the Ubuntu 24.04 x86-64 sandbox host on 2026-09-23, using pinned
commit `88faa9e3fe068dd1d8accb6c9484545f462d53d3`:

| Measurement | `base-ubuntu` recipe | `swarmy-dev:dev` |
| --- | ---: | ---: |
| Virtual disk size | 8,589,934,592 bytes | 68,719,476,736 bytes |
| Nonzero chunk coverage | 864,813,056 bytes (824.75 MiB) | 38,217,711,616 bytes (35.59 GiB) |
| `cargo build --workspace --locked` from a fresh clone | Cargo unavailable | 79 s |

The final image build registered `swarmy-dev:dev` with 145,789 nonzero chunks
out of 262,144 total. Its initial all-targets warm build took 11 minutes; the
additional plain workspace warm build took 6 minutes 32 seconds. A fresh clone
at the pinned commit then ran the plain workspace build in 79 seconds, using
the agent's retained `CARGO_TARGET_DIR`.

The base image has no Rust toolchain, so it cannot run a direct Cargo timing.
Its nonzero coverage above comes from the 2026-09-18 build recorded in the
`base-ubuntu` README; the archive may have changed since then.
For a cold comparison on the same host, a fresh pinned clone with the same
Rust toolchain and downloaded registry but an empty `CARGO_TARGET_DIR` took
598 seconds for `cargo build --workspace --locked`. The warm image time is
about 13% of that cold-target time. These timings include compilation and
linking but exclude cloning. A remote node build and timing were not available
in this sandbox.
