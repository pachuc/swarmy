# Running agents on swarmy in Daytona

This directory holds the environment that Codex agents use when they work on
swarmy through the codex-daytona launcher. Daytona is a hosted service that runs
disposable Linux sandboxes; the launcher clones this repository into one, runs a
Codex session there, and expects a pull request at the end.

`Dockerfile` describes the sandbox image. It contains the pinned Rust toolchain
and the three backing systems that slice 1 tests need: FoundationDB (fdbserver,
fdbcli, and the client library), the NATS server, and SeaweedFS as the
S3-compatible object store. No project source or credentials go into the image.

`build-snapshot.mjs` builds that image once as a named Daytona snapshot and
writes the name into `.codex-daytona.json` at the repository root. The name is
derived from the Dockerfile contents, so after editing the Dockerfile run the
script again and commit the updated config. The script reads the Daytona API key
and SDK from a local checkout of codex-daytona:

```sh
node .daytona/build-snapshot.mjs
```

`.codex-daytona.json` also pins the Codex version, the model and reasoning
effort, and the setup commands that run in the fresh clone before the agent
starts: installing the toolchain from `rust-toolchain.toml` and prefetching
dependencies.
