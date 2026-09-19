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
`~/.local` (`make dev-tools` does the same). Then install every swarmy binary
with one command:

```sh
make install
```

It finds the FoundationDB client library in `~/.local/lib` or a system
directory, builds the CLI, its `swarmy-session` companion, and the three
services, and installs them into `~/.cargo/bin`. `make install-node` adds
`swarmyd` for a machine with root. `make check` runs the CI commands and
`make uninstall` removes the binaries. For a library in another location, run
`make install SWARMY_FDB_LIB_DIR=/absolute/path/lib`. The library directory is
embedded in every binary's runtime search path. swarmy adds that prefix's `bin` directory and `~/.local/bin`
to the search path of its backing-stack subprocesses, so no library or executable
path exports are needed. Keep the library at that location, or reinstall swarmy
with the new `SWARMY_FDB_LIB_DIR`.

### Install from a checkout with system backing tools

If the pinned backing tools and FoundationDB client library are already installed
system-wide (as in CI), the same `make install` finds the library in
`/usr/local/lib` or `/usr/lib` and needs no variable.

Cargo installs `swarmy`, its `swarmy-session` companion, and the three services
into `~/.cargo/bin` by default. Keep them together: `swarmy dev up` refuses to
start a service whose version differs from the CLI, and `make install` is the
way to bring them back in step. With rustup's normal shell
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
2026-09-19: This line was written by a swarmy agent.
