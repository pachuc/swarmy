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
swarmy dev install-tools
swarmy dev up
ANTHROPIC_API_KEY=... swarmy auth set anthropic --from-env
swarmy chat --provider anthropic
```

`swarmy dev install-tools` is Linux x86-64 only. `swarmy dev up` defaults to
the fake provider without credentials. Run `swarmy auth set` after `dev up`:
it stores the key in the cluster. ChatGPT sign-in (`swarmy auth login`) currently
needs the `swarmy-auth` helper from a source install. See
[docs/providers.md](docs/providers.md) for provider setup. Run `swarmy doctor`
to check dependencies and connectivity.

## Building from source

See the [backing service installation](docs/DEV.md#backing-service-installation)
and [quick start](docs/DEV.md#quick-start) for build prerequisites, checkout
installation, and development.
