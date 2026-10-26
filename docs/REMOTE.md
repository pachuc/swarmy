# Swarms on cloud machines and existing hosts

## Swarms on cloud machines (OpenTofu groups)

An OpenTofu group describes machines created and tracked by OpenTofu, an
infrastructure-as-code tool. A group's `module` is `"hetzner-cloud"`, `"aws"`,
or an absolute path to your own module. Hetzner Cloud needs credentials from
`swarmy cloud login hetzner-cloud`, and its stack bucket also needs
`swarmy cloud login hetzner-s3`; AWS needs `swarmy cloud login aws`.
Credentials reach OpenTofu only through its environment, not through tfvars.

```sh
printf '%s\n' "$HCLOUD_TOKEN" | swarmy cloud login hetzner-cloud
swarmy cloud login hetzner-s3
swarmy swarm create fleet --on hetzner-cloud --size cpx41 --nodes 2
swarmy swarm plan fleet
swarmy swarm up fleet --copy-credential
swarmy --swarm fleet chat
swarmy swarm down fleet --destroy
```

`swarmy swarm plan fleet` previews every group's changes without applying.
`up` creates a working directory and state per group under the swarm's data
directory, generates one SSH key per group, shares the network between groups of the same module,
and creates a bucket for a hetzner-cloud stack group only. It waits for SSH
and cloud-init before the same provisioning used for existing hosts. `down`
destroys groups in reverse order and removes the bucket with the data;
persistent swarms require `--destroy`. Cloud and existing-host groups may coexist in a mixed
swarm. `SWARMY_TOFU_BIN` is a development and test override for the pinned,
checksum-verified OpenTofu download.

### Scaling node groups

```sh
swarmy swarm scale fleet workers 4
swarmy swarm scale fleet workers 2 --drain-timeout 1200
```

Growing updates the spec and reruns the normal provisioning path; already provisioned
machines are skipped. Shrinking removes the highest-numbered nodes first. It waits
up to `--drain-timeout` seconds per node (600 by default) for running sandbox
commands and turns to finish. The node agent then stops cleanly, checkpointing
and releasing its computers, which start on another node at their next turn.
`--force` stops nodes that remain busy or removes unreachable nodes; their
computers recover when leases expire. The node tunnel key is revoked on the
stack host. OpenTofu node machines are destroyed, while existing hosts have
swarmy removed but the machines themselves are kept. Add an existing host with
`swarmy swarm node add` rather than growing its count. Stack and services
groups and local swarms cannot be scaled. Scale rewrites the spec without
preserving comments. After a failure, rerun the same scale command to finish;
hand-lowering `count` and running `up` remains refused.

### Acceptance run on Hetzner Cloud

Only the owner runs the approved acceptance test. The script creates a control
and a workers group (three machines at peak), checks a model turn and durable
sandbox file, scales the workers group to two and back to one, and verifies teardown against Hetzner Cloud
and Object Storage. It prints a maximum cost estimate before the `--yes` gate,
bounds the run to 90 minutes by default, and allows 30 more minutes for teardown.
If leftovers remain, follow the printed manual cleanup instructions in the
Hetzner console before attempting another run. The dry run uses fake OpenTofu
and needs no cloud access. The real run needs a configured swarmy config.toml
so doctor can verify the configuration. Provide a nonempty 0600 model API key file; for
ChatGPT, omit the key file and use its configured credential instead.

```sh
printf '%s\n' "$HCLOUD_TOKEN" | swarmy cloud login hetzner-cloud
swarmy cloud login hetzner-s3
scripts/acceptance-hetzner-cloud.sh --dry-run
scripts/acceptance-hetzner-cloud.sh --provider PROVIDER --model MODEL --api-key-file /path/to/key --yes
```

## Swarms on existing hosts

An existing-host swarm provisions machines you already own over SSH. Swarmy
installs its stack, control services, and sandbox node on the primary host;
additional hosts run sandbox nodes. It never creates or deletes machines.
The persistent swarm's spec is in `~/.config/swarmy/swarms/NAME.toml` and its
private record, keys, and on-demand tunnel are in
`~/.local/share/swarmy/swarms/NAME/`.

Create a swarm and provision its first host:

```sh
swarmy swarm create demo --host 203.0.113.10 --ssh-key <bootstrap-key> --local-storage dir:/srv/swarmy-local
swarmy swarm up demo
swarmy --swarm demo doctor
swarmy swarm status demo
```

The SSH key is copied to the private record directory. SSH and rsync must
reach the host with a login able to install systemd units and prepare local
storage. `--ssh-user` sets that login; `--service-user` sets the account that
owns the node checkout and runs services (default `swarmy`). Set
`--private-address` when hosts talk over a separate internal interface.
`--sandboxes` sets node capacity. A node group may be extended with
`swarmy swarm node add demo GROUP --host 203.0.113.11 --ssh-key <bootstrap-key>`.
The primary is provisioned first; upgrade and teardown visit nodes first.
Remove existing node hosts with `swarmy swarm scale`.

## Up, authenticate, and chat

On the first `swarmy swarm up demo`, optionally pass `--copy-credential` to
copy the configured provider credential to the control host over SSH.
Ordinary node hosts need no provider credential. After provisioning, client
commands such as `swarmy --swarm demo doctor` and `swarmy --swarm demo chat`
reach the API through the on-demand tunnel. Run `swarmy swarm use demo` to
make it the default. `swarmy swarm status demo` reports the hosts and tunnel;
`swarmy swarm logs demo --unit swarmyd` follows the node journal. Once the
primary is provisioned, another `up --copy-credential` skips it and does not
copy a credential; decide whether to copy it before the first up.

## Spec and host layout

A spec may describe multiple existing hosts. The stack and services share a
group; `placement.nodes` names groups that run sandboxes. For example:

```toml
name = "demo"
lifecycle = "persistent"
service_user = "swarmy"
[groups.box]
module = "existing"
sandboxes = 64
local_storage = "dir:/srv/swarmy-local"
hosts = [{ address = "203.0.113.10", ssh_user = "root", ssh_key = "/home/me/.ssh/id_ed25519" }]
[placement]
stack = "box"
services = "box"
nodes = ["box"]
```

`sandboxes` defaults to 64 on node hosts; set it to zero for a control-only
host. `private_address` defaults to `address`. SSH key paths must be absolute.
`swarmy swarm up demo` can be re-run after a partial failure: it skips hosts
already marked provisioned in the record. `swarmy swarm node add demo box
--host 203.0.113.11 --ssh-key <bootstrap-key>` writes the new host to the
spec (discarding comments), then provisions it. `swarmy swarm down demo
--destroy` keeps the spec so the swarm can be provisioned again.

## Node directory and installed services

The provisioner installs under `~<service-user>/swarmy`, with the service
configuration at `.swarmy/config.toml`:

```
~<service-user>/swarmy/
  scripts/           provisioning, upgrade, services, decommission, dev-stack
  images/            carried image recipes
  .swarmy/release/   uploaded release binaries
  source/            development checkout copy and its target/
  .swarmy/config.toml, .swarmy/node/, .swarmy/volumes,
  .swarmy/tunnel-key*, .dev/   persistent node state; uploads do not overwrite it
```

Scripts match the client binary. Rebuild a development client after editing a
script. Do not delete `.swarmy/` or `.dev/` when clearing unused checkout files.
The node's binaries and scripts are
copied from a checkout for development builds, or fetched from the matching
release for release builds. `SWARMY_RELEASE_BASE_URL` overrides the release
archive source. The provisioner keeps the SSH tunnel key and systemd unit
files in private node state. On a stack+services host, the units are:

| Unit | Purpose |
| --- | --- |
| `swarmy-stack` | FoundationDB, NATS, object store |
| `swarmyd` | Sandbox node |
| `swarmy-api` | Control plane API |
| `swarmy-scheduler` | Scheduling |
| `swarmy-worker` | Step execution |
| `swarmy-gateway` | Inference gateway |
| `swarmy-tunnel` | Host tunnel endpoint |

`swarmy swarm logs demo --unit swarmyd` follows the primary journal. Pass
`--host NAME` to follow a specific recorded host. `swarmy swarm status demo`
shows SSH reachability and whether the client's tunnel is open. Client
commands open one per-swarm SSH tunnel on demand, selecting free local ports.

## Persistent object storage

The backing object store can be an S3-compatible service. Add public bucket
coordinates to the spec, never keys:

```toml
[bucket]
name = "demo-bucket"
endpoint = "https://objects.example.invalid"
region = "eu-west-1"
prefix = "runs/demo"
conditional_create = true
```

On the first up, provide both static keys, with the secret from a private
0600 file or one line on standard input:

```sh
swarmy swarm up demo --s3-access-key KEY --s3-secret-stdin
```

Keys are stored in the private swarm record and uploaded to nodes; they are
never in the spec, process arguments, or status output. Up checks access
before recording the keys. Ownership is recorded as `managed-by=swarmy` and
`swarmy-remote=NAME` bucket tags where supported, or as a marker under the
prefix otherwise.
An owned bucket's swarm prefix is removed on down; an unmanaged bucket is
left untouched. Persistent swarms require `--destroy` for down. The bucket
client is an opt-in build feature (`make install-client`).

The static keys need `HeadBucket`, `CreateBucket`, `GetBucketTagging`,
`PutBucketTagging`, `GetObject`, `PutObject`, `ListObjectsV2`,
`ListObjectVersions`, `DeleteObjects`, and `DeleteBucket` permissions on the
bucket and objects. The node's ordinary object-store operations use the same
keys. There is no IAM role or instance profile for an existing-host swarm.
If conditional PUT is unsupported, set `conditional_create = false` in the
spec. The object storage compatibility probe in the volume tests exercises
these operations against a test bucket before using it for agent disks.

### Object storage compatibility probe

Before depending on a provider, run the probe against a test bucket. It checks
bucket existence with `HeadBucket`, PUT, create-only PUT on new and existing
keys, GET, HEAD, prefixed pagination, and DELETE. It writes under a temporary
`probe-<id>/` prefix and removes it afterward. Ranged GET is not needed for
chunks, manifests, or blobs, which are always read whole. The output has one
JSON line per check, without endpoint, bucket, prefix, or key material.

```sh
cargo run --locked -p swarmy-volume --example s3-compat-probe -- \
  --endpoint https://objects.example.invalid --region eu-west-1 \
  --bucket NAME --access-key KEY --secret-file ~/.local/share/swarmy/demo-s3-secret
```

Coordinates can instead come from `SWARMY_S3_ENDPOINT`, `SWARMY_S3_REGION`,
`SWARMY_S3_BUCKET`, `SWARMY_S3_ACCESS_KEY`, and `SWARMY_S3_SECRET_KEY`.
The secret may come from a private `--secret-file`, `--secret-stdin`, or the
environment. For a provider without conditional PUT, run the probe again with
`--conditional-create=false` and set `conditional_create = false` in the spec.

| Check | SeaweedFS | Hetzner Object Storage |
|---|---|---|
| `bucket_exists` (`HeadBucket`) | pass | pending operator run |
| `put` | pass | pending operator run |
| `create_new` (`If-None-Match: *` on a new key) | pass | pending operator run |
| `create_existing` (second create rejected) | pass | pending operator run |
| `get` | pass | pending operator run |
| `head` | pass | pending operator run |
| `list_prefix` | pass | pending operator run |
| `list_pagination` (two keys per page, continuation tokens) | pass | pending operator run |
| `delete` | pass | pending operator run |
| `conditional_create` stays on | yes | pending operator run |

SeaweedFS supports conditional PUT in the development stack. If a provider
fails `create_existing` by overwriting, disable conditional create and rerun
the probe. `scripts/test-s3-compat-probe.sh` runs both modes against the
development stack in CI.

In-place static-key rotation is not supported: a subsequent `swarmy swarm up`
refuses different keys because provisioned hosts keep the old ones. There is
no `--keep-bucket` for `swarmy swarm down`; `down --destroy` deletes owned
bucket data. Do not use it to rotate keys unless that deletion is intended.
Plan a separate data-preserving rotation or migration with the operator.

## Upgrades and credentials

```sh
swarmy swarm upgrade demo
swarmy swarm upgrade demo --services-only
swarmy swarm upgrade demo --drain-timeout 600
```

Upgrade reconciles binaries and units in place. It drains sandboxes before
restarting nodes and upgrades the primary last. Development checkouts with
uncommitted changes are refused unless `--allow-dirty` is set. Machines
provisioned before the `swarmy-core` binary should be upgraded with the same
command; the installed units are reconciled. To copy the configured provider
credential, pass `--copy-credential` on the first `swarmy swarm up demo`;
subsequent ups skip an already-provisioned primary. The credential stays on
the host; normal provisioning never copies it. Named agent memory files
remain on agent disks and follow their snapshots, not the client tunnel.

### Upgrading a host provisioned before swarmy-core

Run a full `swarmy swarm upgrade NAME` without `--services-only` from a
matching client. The `units` list should include `swarmyd.service` and the
installed service units. Check `systemctl cat swarmyd.service` for
`ExecStart=/usr/local/bin/swarmy-core node`, and inspect
`ls /usr/local/bin`. Old binaries are removed once no installed unit refers
to them. Do not use down to fix an upgrade: down removes the backing stack's
data. If installed service settings do not match, inspect
`/etc/swarmy/units.env` and the installed units before reprovisioning.

### Node scripts and joining hosts

On the host, `sudo systemctl status swarmy-stack swarmyd` checks the installed
services; `sudo journalctl -u swarmyd -f` follows node logs. Provisioning
writes `/etc/swarmy/node.env`, enables services at boot, and uses
`Restart=always` for swarmyd. To rerun the low-level provisioning script
manually, first inspect the saved spec and node environment, then on the
primary use:

```sh
cd ~/swarmy && bash scripts/remote-provision.sh stack PRIVATE_IP BUCKET REGION ENDPOINT PREFIX CONDITIONAL_CREATE STATIC_KEYS SANDBOXES SERVICE_USER LOCAL_STORAGE PREBUILT
```

`PREBUILT` is true for an uploaded release bundle; the service user defaults
to `swarmy`. A joining host uses `node FIRST_HOST_PRIVATE_IP` followed by the
same bucket and storage arguments. Its cluster file is copied from the
primary, preserving cluster identity and loopback coordinator address. Node
add generates a dedicated tunnel key on the joining host, authorizes only
service forwards on the primary, and pins the primary's host key through the
authenticated provisioning connection. The key allows no interactive shell.
The tunnel forwards local 4500 and 4222 to the primary's loopback ports and
8333 when SeaweedFS is used. It starts before swarmyd and restarts after SSH
failure; inspect it with `sudo journalctl -u swarmy-tunnel`. Tunnel identity
and pinned host key live under `/etc/swarmy/`, readable only by the service
user. Binaries install to `/usr/local/bin`.

## Stop, down, and recovery

`swarmy swarm stop demo` stops services but retains data;
`swarmy swarm start demo` restarts them. `swarmy swarm down demo --destroy`
removes swarmy units and the node checkout, and deletes owned bucket scope. The
On existing hosts the machines keep running; OpenTofu groups are destroyed
(see above). The service user, fstab entry, sandbox data on local
storage, and tunnel authorization keys may remain; inspect and remove these
by hand before repurposing a host. A broken node directory with installed
units can be restored with `swarmy swarm upgrade demo`, or its units removed
by hand. `swarmy swarm rm demo --force` forgets the local registration; it
does not decommission hosts.

## Agent memory files

Named-agent memory files are read from `[memory] dir`
(`SWARMY_MEMORY_DIR`, default `/home/agent/memory`), bounded by
`[memory] max_bytes` (`SWARMY_MEMORY_MAX_BYTES`, default 32768). The node
reads regular files in filename order, skipping directories, symlinks, and
special files; it notes when the byte budget truncates content. Use an
absolute directory without symlink components. See
[worker memory handling](../crates/swarmy-worker/src/worker.rs).

## Fleet-specific operations

The fleet uses `--swarm hz` for client commands and upgrades with
`swarmy swarm upgrade hz`. hz runs on the old client until its record is
converted into a swarm (see the goal plan); until then use the master client
for it. Do not run the new client against hz before that conversion.

## Root suites and maintainer acceptance

Use [`scripts/node-suites/`](../scripts/node-suites/) for the manual root
suites; record results in the pull request. The host and sandbox root suites
require a real node with `/dev/nbd0`, sudo, and the dev stack. Run them on the suite node, not inside an agent sandbox.
`scripts/test-remote.sh` checks the on-demand SSH tunnel against localhost
sshd with an authorized test key. It skips without `SWARMY_REMOTE_TEST_KEY`.
