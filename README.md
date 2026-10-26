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

### On this machine

```sh
swarmy install-tools
swarmy swarm create dev --local
swarmy swarm up dev
ANTHROPIC_API_KEY=... swarmy auth set anthropic --from-env
swarmy chat --provider anthropic
```

`swarmy install-tools` supports Linux x86-64. The first local up downloads
and registers the base image on Linux x86-64; see
[DEV.md](docs/DEV.md#local-swarms) for development setup and
[docs/swarms.md](docs/swarms.md#swarms-on-this-machine) for `--no-image` and
`swarmy image pull base-ubuntu --set-default`. Up uses the fake provider by
default; configure credentials after up. Run `swarmy doctor` to check
connectivity. ChatGPT login needs the `swarmy-auth` helper from a source
install; see [docs/providers.md](docs/providers.md).

### On a server you own

```sh
swarmy swarm create box --host 203.0.113.10 --ssh-key ~/.ssh/id_ed25519 --local-storage dir:/srv/swarmy-local
swarmy swarm up box
swarmy --swarm box chat
```

### On Hetzner Cloud

```sh
swarmy cloud login hetzner-cloud
swarmy cloud login hetzner-s3
swarmy swarm create fleet --on hetzner-cloud --size cpx41 --nodes 1
swarmy swarm up fleet
swarmy --swarm fleet chat
swarmy swarm down fleet --destroy
```

See [docs/swarms.md](docs/swarms.md) for swarm specs, target selection and lifecycle.

## Building from source

See the [backing service installation](docs/DEV.md#backing-service-installation)
and [quick start](docs/DEV.md#quick-start) for build prerequisites, checkout
installation, and development.
