# Swarm hosts: what swarmy installs and how to repair it

This is the host-level reference. For creating and operating swarms, see [swarms.md](swarms.md).

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
archive source. Systemd is the Linux service manager that starts and
restarts the installed services. The provisioner keeps the SSH tunnel key
and systemd unit files in private node state. On a stack+services host, the units are:

| Unit | Purpose |
| --- | --- |
| `swarmy-stack` | FoundationDB, NATS, object store |
| `swarmyd` | Sandbox node |
| `swarmy-api` | Control plane API |
| `swarmy-scheduler` | Scheduling |
| `swarmy-worker` | Step execution |
| `swarmy-gateway` | Inference gateway |
| `swarmy-provisioner` | Elastic machine groups; primary only |
| `swarmy-tunnel` | Host tunnel endpoint |

The swarmy-provisioner service runs OpenTofu for elastic groups on the primary.
It keeps local state under `~<service-user>/.local/share/swarmy/provisioner/tofu/GROUP/`
and group SSH keys under `~<service-user>/.local/share/swarmy/provisioner/keys/groups/`.
SSH runs from the primary as the service user. Node tunnel keys are added to and
removed from `~<service-user>/.ssh/authorized_keys` on the primary.
A provisioner restart resumes interrupted joins. Follow joins in the same journal.
After every apply it copies state to the private bucket at
`tofu-state/GROUP/terraform.tfstate`. To recover after losing the local state,
stop `swarmy-provisioner.service`, copy that object back to the local path,
and start the unit. Follow it with `sudo journalctl -u swarmy-provisioner -f`.
The creation interlock allows at most four machine creations per hour per swarm;
a blocked group logs `creation limit reached` with the next available slot.

## Object storage permissions

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

The S3 client is an opt-in build feature (`make install-client`).

## What down leaves on a host

Down removes swarmy units and the node checkout. The service user, fstab
entry, sandbox data on local storage and tunnel authorization keys may remain.
Inspect and remove these by hand before repurposing a host. A broken node
directory with installed units can be restored with
`swarmy swarm upgrade demo`, or its units removed by hand.
`swarmy swarm rm demo --force` forgets the local registration; it does not
decommission hosts.

Down removes the drop-in files provisioning wrote only while they start with
the line `# Written by swarmy provisioning; swarmy swarm down removes this file.`:
`/etc/modules-load.d/swarmy-provisioned.conf` and
`/etc/ld.so.conf.d/swarmy-provisioned.conf`. A file at either path without that
line belongs to the host: provisioning refuses to replace it and down leaves
it. Hosts provisioned before October 30, 2026 may also keep
`/etc/modules-load.d/swarmy.conf` and `/etc/ld.so.conf.d/swarmy.conf` from older
provisioning. Down no longer removes those names, so delete them by hand if
the host does not need them.

## Draining a node

Before stopping a node, swarmy checks for running sandbox commands and
turns. It waits for them to finish, then stops the node agent cleanly so
computers checkpoint and release. Swarmy does not cordon the node (mark it
unavailable for new work) before draining. See
[scaling](swarms.md#scaling-node-groups) for timeouts and force.

## Host repair

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

See [infra/README.md](../infra/README.md) for OpenTofu working directories.

## Root suites and maintainer acceptance

Use [`scripts/node-suites/`](../scripts/node-suites/) for the manual root
suites; record results in the pull request. The host and sandbox root suites
require a real node with `/dev/nbd0`, sudo and the dev stack. Run them on
the suite node, not inside an agent sandbox.
`scripts/test-remote.sh` checks the on-demand SSH tunnel against localhost
sshd with an authorized test key. It skips without `SWARMY_REMOTE_TEST_KEY`.
