# Local development

Run FoundationDB, NATS with JetStream, and SeaweedFS as background processes on
Linux. The script uses Bash, curl 7.75 or newer (for AWS request signing), standard
Linux utilities, and the service binaries. It does not need a container runtime
or root privileges. Installation requires root privileges.

Use these exact versions, matching `.daytona/Dockerfile` and CI:

| Service | Version | Binaries |
| --- | --- | --- |
| FoundationDB | 7.3.79 | `fdbserver`, `fdbcli`, and the client library |
| NATS | 2.14.6 | `nats-server` |
| SeaweedFS | 4.47 | `weed` |

The installation commands below target x86-64 machines. Release assets come from
[FoundationDB](https://github.com/apple/foundationdb/releases/tag/7.3.79),
[NATS](https://github.com/nats-io/nats-server/releases/tag/v2.14.6), and
[SeaweedFS](https://github.com/seaweedfs/seaweedfs/releases/tag/4.47).

## Install on Ubuntu

Run this in Bash. Extract the verified FoundationDB Debian packages to install
their binaries, headers, and client library without package hooks starting a
system service. This is also how the development image and CI install them.

```bash
set -euo pipefail
sudo apt-get update
sudo apt-get install -y ca-certificates curl tar
install_dir=$(mktemp -d)
cd "$install_dir"
FDB_VERSION=7.3.79
for pkg in clients server; do
  deb="foundationdb-${pkg}_${FDB_VERSION}-1_amd64.deb"
  curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${deb}"
  curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${deb}.sha256"
  sha256sum -c "${deb}.sha256"
  sudo dpkg-deb -x "$deb" /
done
sudo ldconfig
cd -
rm -rf -- "$install_dir"
```

Then install NATS and SeaweedFS using the shared instructions below.

## Install on Arch Linux

Install the pinned upstream FoundationDB binaries and client library instead of
relying on the version currently packaged by Arch or the AUR. Run this in Bash:

```bash
set -euo pipefail
sudo pacman -S --needed ca-certificates curl tar
install_dir=$(mktemp -d)
cd "$install_dir"
FDB_VERSION=7.3.79
for asset in fdbserver.x86_64 fdbcli.x86_64 libfdb_c.x86_64.so; do
  curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${asset}"
  curl --retry 3 -fsSLO "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/${asset}.sha256"
  sha256sum -c "${asset}.sha256"
done
sudo install -Dm755 fdbserver.x86_64 /usr/local/bin/fdbserver
sudo install -Dm755 fdbcli.x86_64 /usr/local/bin/fdbcli
sudo install -Dm755 libfdb_c.x86_64.so /usr/local/lib/libfdb_c.so
printf '/usr/local/lib\n' | sudo tee /etc/ld.so.conf.d/swarmy-fdb.conf >/dev/null
sudo ldconfig
cd -
rm -rf -- "$install_dir"
```

## Install NATS and SeaweedFS on either distribution

```bash
set -euo pipefail
install_dir=$(mktemp -d)
cd "$install_dir"
NATS_VERSION=2.14.6
SEAWEEDFS_VERSION=4.47
curl --retry 3 -fsSL "https://github.com/nats-io/nats-server/releases/download/v${NATS_VERSION}/nats-server-v${NATS_VERSION}-linux-amd64.tar.gz" | tar -xz
sudo install -m 0755 "nats-server-v${NATS_VERSION}-linux-amd64/nats-server" /usr/local/bin/nats-server
curl --retry 3 -fsSL "https://github.com/seaweedfs/seaweedfs/releases/download/${SEAWEEDFS_VERSION}/linux_amd64.tar.gz" | tar -xz
sudo install -m 0755 weed /usr/local/bin/weed
cd -
rm -rf -- "$install_dir"
export PATH="$PATH:/usr/sbin"
fdbserver --version
fdbcli --version
nats-server --version
weed version
```

## Run the stack

From the repository root:

```bash
scripts/dev-stack.sh start
source .dev/env
scripts/dev-stack.sh status
```

`start` waits for FoundationDB to become available, for the NATS JetStream
monitoring endpoint, and for an authenticated S3 bucket listing. It configures
a new single-node FoundationDB database with the `ssd` engine on first start,
creates the S3 bucket `swarmy`, and writes `.dev/env` only after readiness checks
pass. Repeating `start` reuses running processes and existing data. You can also
invoke the script by absolute path from another directory.

All data, configuration, PID files, and logs live under the ignored `.dev/`
directory. Logs are in `.dev/logs/` and FoundationDB trace logs are in
`.dev/fdb/logs/`. PID files record both the PID and Linux process start time so
that `stop` does not signal an unrelated process after PID reuse. `status` reports
process liveness; use the requests below to check service health.

Services bind to `127.0.0.1`. Reserve ports 4500 for FoundationDB; 4222 and 8222
for NATS; and 8080, 8333, 8888, 9333, 18080, 18333, 18888, and 19333 for
SeaweedFS HTTP and gRPC APIs. Stop any system-installed service using these ports
first. S3 uses the fixed local development credentials `swarmy-dev` and
`swarmy-dev-secret`, with admin rights. These credentials are for this local stack.

```bash
fdbcli -C "$SWARMY_FDB_CLUSTER_FILE" --exec status
curl --fail http://127.0.0.1:8222/jsz
curl --fail --aws-sigv4 "aws:amz:$SWARMY_S3_REGION:s3" \
  --user "$SWARMY_S3_ACCESS_KEY:$SWARMY_S3_SECRET_KEY" "$SWARMY_S3_ENDPOINT/"
```

The last request lists buckets and should include `swarmy`. An unsigned request
to the S3 endpoint returns an authentication error. When uploading a body with
curl, also pass `--header 'x-amz-content-sha256: UNSIGNED-PAYLOAD'` so older curl
versions and SeaweedFS agree on payload signing.

Stop the processes cleanly, preserving data for the next start:

```bash
scripts/dev-stack.sh stop
scripts/dev-stack.sh status
```

`stop` sends SIGTERM and waits up to 30 seconds per process. If a process does
not exit, it reports a failure and retains its PID file for a retry. Failed or
interrupted startup stops the processes launched by that invocation. A lock
prevents simultaneous `start` and `stop` operations. If the script itself was
killed with SIGKILL, first verify no `start` or `stop` invocation is still running,
then remove the stale lock with `rmdir .dev/lock` and run `stop` before retrying.
To reset all local data, stop successfully and then remove `.dev/`.

## Environment and tests

Source `.dev/env` in every shell that runs services or integration tests. Its
exports are inherited by child processes; starting the stack alone cannot change
the calling shell's environment.

| Variable | Value |
| --- | --- |
| `SWARMY_FDB_CLUSTER_FILE` | Absolute path to `.dev/fdb.cluster` |
| `SWARMY_NATS_URL` | `nats://127.0.0.1:4222` |
| `SWARMY_S3_ENDPOINT` | `http://127.0.0.1:8333` |
| `SWARMY_S3_ACCESS_KEY` | `swarmy-dev` |
| `SWARMY_S3_SECRET_KEY` | `swarmy-dev-secret` |
| `SWARMY_S3_BUCKET` | `swarmy` |
| `SWARMY_S3_REGION` | `us-east-1` |

S3 clients should use path-style bucket addressing with this endpoint. Tests
requiring a backing system must skip cleanly when its environment variables are
absent. Run the same checks as CI:

```bash
source .dev/env
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

The Ubuntu CI job installs these pinned versions, starts the stack, and copies
the exported settings into `GITHUB_ENV` so subsequent test steps inherit them.
Its cleanup step runs even if an earlier step fails.
