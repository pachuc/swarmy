# Swarms on existing hosts

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

## Up, authenticate, and chat

Client commands reach the API through the on-demand tunnel.

## Node directory and installed services

The provisioner installs under `~<service-user>/swarmy`, with the service
configuration at `.swarmy/config.toml`. The node's binaries and scripts are
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
before recording the keys and creates an ownership marker under the prefix.
An owned bucket's swarm prefix is removed on down; an unmanaged bucket is
left untouched. Persistent swarms require `--destroy` for down. The bucket
client is an opt-in build feature (`make install-client`).

The static keys need S3 permissions for bucket inspection, creation if
absent, ownership tags or marker objects, listing objects and versions,
reading and writing data, and deletion of objects in the swarm's prefix.
If conditional PUT is unsupported, set `conditional_create = false` in the
spec. The object storage compatibility probe in the volume tests exercises
these operations against a test bucket before using it for agent disks.

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
command; the installed units are reconciled. To put inference control
services on the host and copy the configured credential explicitly, run
`swarmy swarm up demo --copy-credential`. The credential stays on the host;
normal provisioning never copies it. Named agent memory files remain on
agent disks and follow their snapshots, not the client tunnel.

## Stop, down, and recovery

`swarmy swarm stop demo` stops services but retains data;
`swarmy swarm start demo` restarts them. `swarmy swarm down demo --destroy`
removes swarmy units and data on hosts and deletes owned bucket scope. The
machines keep running. The service user, fstab entry, sandbox data on local
storage, and tunnel authorization keys may remain; inspect and remove these
by hand before repurposing a host. A broken node directory with installed
units can be restored with `swarmy swarm upgrade demo`, or its units removed
by hand. `swarmy swarm rm demo --force` forgets the local registration; it
does not decommission hosts.

## Fleet-specific operations

The fleet uses `--swarm hz` for client commands and upgrades with
`swarmy swarm upgrade hz`. hz runs on the old client until its record is
converted into a swarm (see the goal plan); until then use the master client
for it. Do not run the new client against hz before that conversion.

## Root suites and maintainer acceptance

The host and sandbox root suites require a real node with `/dev/nbd0`, sudo,
and the dev stack. Run them on the suite node, not inside an agent sandbox.
`scripts/test-remote.sh` checks the on-demand SSH tunnel against localhost
sshd with an authorized test key. It skips without `SWARMY_REMOTE_TEST_KEY`.
