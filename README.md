# swarmy

Scalable, failure-resistant agent swarm infrastructure in Rust.

Start with [docs/DESIGN.md](docs/DESIGN.md).

The current architecture as a [sysy](https://github.com/pachuc/sysy) design lives in
[docs/architecture.sysy.json](docs/architecture.sysy.json). Open it with
`sysy ui docs/architecture.sysy.json`, or read it with `sysy show`.

## Install

Use a Linux x86-64 machine with Git, Bash, curl 7.75+, tar, a C/C++ toolchain,
pkg-config, clang/libclang (for Rust bindgen), and Rust via rustup. On Ubuntu,
those build prerequisites are `build-essential pkg-config clang libclang-dev
ca-certificates curl tar git`. Provision them first if they are absent; the
backing tools installer itself needs no root access.

Clone the repository and install the pinned Rust toolchain:

```sh
git clone https://github.com/pachuc/swarmy.git
cd swarmy
rustup show
```

### Install backing tools without root

```sh
scripts/install-dev-tools.sh
```

This installs FoundationDB 7.3.79, NATS 2.14.6, and SeaweedFS 4.47 under
`~/.local`. Run the one-line Cargo command it prints (in Bash):

```bash
for crate in cli scheduler worker gateway; do SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo install --locked --path "crates/swarmy-$crate"; done
```

For a different location, run `scripts/install-dev-tools.sh --prefix /absolute/path`
and use its printed command. The library directory is embedded in every binary's
runtime search path. swarmy adds that prefix's `bin` directory and `~/.local/bin`
to the search path of its backing-stack subprocesses, so no library or executable
path exports are needed. Keep the library at that location, or reinstall swarmy
with the new `SWARMY_FDB_LIB_DIR`.

### Install from a checkout with system backing tools

If the pinned backing tools and FoundationDB client library are already installed
system-wide (as in CI), install directly without any build-time variable:

```bash
cargo install --locked --path crates/swarmy-cli
for crate in scheduler worker gateway; do cargo install --locked --path "crates/swarmy-$crate"; done
```

Cargo installs `swarmy`, its `swarmy-session` companion, and the three services
into `~/.cargo/bin` by default. Keep them together. With rustup's normal shell
setup, start the development system from this checkout:

```sh
swarmy version
swarmy dev up
swarmy doctor
swarmy run "hello"
swarmy dev down
```

If your shell does not have Cargo's bin directory on PATH, use
`~/.cargo/bin/swarmy` directly. Retain the checkout: `dev` uses its
`scripts/dev-stack.sh`. `dev up` creates `.swarmy/config.toml` and defaults to a
fake provider that requires no credentials. `doctor` reports each dependency,
the resolved configuration path, credentials when using ChatGPT, and backing
stack connectivity. It exits 1 if any check fails; `swarmy doctor --json` returns
one JSON object with `ok` and `checks` fields.

See [docs/DEV.md](docs/DEV.md) for configuration, credentials, and development.
