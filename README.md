# swarmy

Scalable, failure-resistant agent swarm infrastructure in Rust.

Start with [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

The current architecture as a [sysy](https://github.com/pachuc/sysy) design lives in
[docs/architecture.sysy.json](docs/architecture.sysy.json). Open it with
`sysy ui docs/architecture.sysy.json`, or read it with `sysy show`.

## Getting started

Install the client from a release:

```sh
curl -fsSL https://raw.githubusercontent.com/pachuc/swarmy/master/scripts/install.sh | sh
```

For a pinned release or another install root, pass `sh -s -- --version vX.Y.Z
--prefix /your/install/root` to the same pipeline. Linux and macOS on x86_64
and aarch64 are supported. The archive is checked against the release's
SHA256SUMS, but releases are not signed. The default destination is
`~/.local/bin`; the installer prints a PATH line if needed.

```sh
swarmy install-tools
swarmy swarm create dev --local
swarmy swarm up dev
ANTHROPIC_API_KEY=... swarmy auth set anthropic --from-env
swarmy chat --provider anthropic
```

`swarmy swarm create dev --local` writes `~/.config/swarmy/swarms/dev.toml`
and makes `dev` current when none is selected. `swarmy swarm up dev` keeps its
record and stack in `~/.local/share/swarmy/swarms/dev/`, and logs and pids
in `~/.local/state/swarmy/swarms/dev/`. Downloads go to `~/.cache/swarmy/`.
`swarmy swarm status dev`, `swarmy swarm logs dev`, `swarmy swarm stop dev`,
and `swarmy chat` then work from any directory. On Linux x86-64, up downloads
and registers the release base image and sets `default_image`. Chat needs no
`--image`; tool-calling turns still need a sandbox node. Use `--no-image` to
skip the download, or retry with `swarmy image pull base-ubuntu --set-default`
and `swarmy swarm up dev`.

`swarmy install-tools` is Linux x86-64 only. `swarmy swarm up dev` defaults to
the fake provider without credentials. Run `swarmy auth set` after `swarmy swarm up dev`:
it stores the key in the cluster. ChatGPT sign-in (`swarmy auth login`) currently
needs the `swarmy-auth` helper from a source install. See
[docs/providers.md](docs/providers.md) for provider setup. Run `swarmy doctor`
to check dependencies and connectivity.

## Building from source

See the [backing service installation](docs/DEV.md#backing-service-installation)
and [quick start](docs/DEV.md#quick-start) for build prerequisites, checkout
installation, and development.
