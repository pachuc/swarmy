# Swarms

A swarm is one deployment of a backing stack, control services and sandbox
nodes. Its spec describes the deployment once. Swarmy creates or adopts
machines, installs services and runs the swarm lifecycle.

```sh
swarmy swarm create fleet --on hetzner-cloud --size cpx41 --nodes 2
swarmy swarm plan fleet
swarmy swarm up fleet
swarmy --swarm fleet chat
swarmy swarm scale fleet workers 3
swarmy swarm down fleet --destroy
```

## Concepts

A **swarm** is one deployment of a backing stack, control services and sandbox
nodes.

A **spec** describes the machines and their roles.

The private **record** tracks provisioned hosts, credentials and connection
details.

A **group** supplies machines for a role.

A **module** defines where those machines come from: this computer, hosts you
own, or cloud infrastructure.

**Placement** assigns the `stack`, `services` and `nodes` roles to groups.
Only the fully remote layout has been tested end to end. Services on this
machine with a remote stack are refused.

**Lifecycle** decides what `down` does. An ephemeral swarm's data is deleted
by plain `down`. A persistent swarm refuses `down` unless you pass
`--destroy`. Local swarms are ephemeral unless created with `--persistent`;
host and cloud swarms are persistent unless created with
`--lifecycle ephemeral`.

OpenTofu is an open-source infrastructure-as-code tool. It creates machines
from a description and records them in a state file. Swarmy downloads a
pinned, checksum-verified release on first use.

| Module | Machines | Who deletes them |
|---|---|---|
| `local` | this machine | nothing to delete |
| `existing` | hosts you list | never deleted, only cleaned |
| `hetzner-cloud` | created by OpenTofu | destroyed on `down` |
| `aws` | created by OpenTofu | destroyed on `down` |
| a path | your OpenTofu module | destroyed on `down` |

```toml
name = "fleet"
lifecycle = "persistent"
[groups.control]
module = "hetzner-cloud"
size = "cpx41"
location = "fsn1"
count = 1
[groups.workers]
module = "hetzner-cloud"
size = "cpx41"
location = "fsn1"
count = 2
[placement]
stack = "control"
services = "control"
nodes = ["workers"]
```

For hosts you own, a spec can describe multiple machines in one group. The
`service_user` is the account that runs services on those hosts:

```toml
name = "box"
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

## Where files live

Swarmy follows the XDG Base Directory convention, the Linux standard for where
programs keep configuration, data, state and cache. Each location below is a
`swarmy` directory inside `$XDG_CONFIG_HOME` (default `~/.config`),
`$XDG_DATA_HOME` (`~/.local/share`), `$XDG_STATE_HOME` (`~/.local/state`) or
`$XDG_CACHE_HOME` (`~/.cache`). Relative XDG values are ignored.

| Path under swarmy's XDG root | Contents |
|---|---|
| config/swarms/NAME.toml | Private swarm spec |
| data/swarms/NAME/record.toml | Private API URL, token and host record |
| data/swarms/NAME/keys/ | Private swarm SSH keys |
| data/swarms/NAME/keys/groups/ | Private SSH keys for machine groups |
| data/swarms/NAME/tofu/GROUP/ | OpenTofu state and working directory per group |
| data/swarms/NAME/tunnel/ | On-demand SSH tunnel state |
| data/swarms/NAME/services.toml | Local service settings, regenerated on up/start |
| data/swarms/NAME/stack/ | Default local backing stack data |
| data/cloud-credentials.toml | Private cloud API credentials |
| data/keyring | Shared local cluster keyring |
| state/current | Name of the selected swarm |
| state/local.lock | Machine-wide local lifecycle lock |
| state/swarms/NAME/ | Supervisor and service pids, ready marker and logs |
| cache/core/ | Matching `swarmy-core` binaries |
| cache/images/ | Base image downloads |
| cache/tofu/ | Pinned OpenTofu releases by version |

## Choosing the target swarm

Client commands choose their API in order: `--swarm NAME`, `SWARMY_SWARM`, the
`SWARMY_API_URL` and `SWARMY_API_TOKEN` pair, then the current swarm. Without
any target, the client explains how to list, use, or create a swarm.
Configuration files never choose the client's API: `[api] url` and `token`
tune local services. `swarmy swarm up NAME` records a local swarm's API, and
`swarm create` makes the new swarm current when none is selected.

```sh
swarmy swarm ls
swarmy swarm show
swarmy swarm use dev
printf '%s' "$TOKEN" | swarmy swarm attach staging --url https://swarmy.example.test --token-stdin
swarmy --swarm staging session ls
swarmy swarm rm staging
```

## Swarms on this machine

FoundationDB stores durable control-plane data. NATS carries work and live
events, and SeaweedFS supplies the local object store. A local swarm runs
these backing services plus the scheduler, worker, gateway and API on this
machine. It does not run a sandbox node, so tool-calling turns still need a
node. Create it once, then use the same name to manage it.

```sh
swarmy swarm create demo --local
swarmy swarm up demo
swarmy swarm status demo
swarmy swarm logs demo worker
swarmy swarm stop demo
swarmy swarm start demo
swarmy swarm down demo
```

A release client downloads its matching `swarmy-core`; `--core-bin PATH` (or
`SWARMY_CORE_BIN`) chooses one explicitly. On Linux x86-64, the first local up
downloads and registers the base image unless you pass `--no-image`. If you
skip it, retry with `swarmy image pull base-ubuntu --set-default` and up again.
See [DEV.md](DEV.md#local-swarms) for development builds and stack sharing.
Fixed ports allow only one local swarm to run at a time. Stop a running
swarm before `swarmy swarm rm demo` will forget it.

## Swarms on hosts you own

An existing-host swarm provisions machines you already own over SSH, the
secure remote-shell protocol. Swarmy installs its stack, control services and
sandbox node on the primary host; additional hosts run sandbox nodes. It never
creates or deletes machines.

Create a swarm and provision its first host:

```sh
swarmy swarm create demo --host 203.0.113.10 --ssh-key <bootstrap-key> --local-storage dir:/srv/swarmy-local
swarmy swarm up demo
swarmy --swarm demo doctor
swarmy swarm status demo
```

The SSH key is copied to the private record directory. SSH and rsync must
reach the host with a login able to prepare local storage. Systemd starts
and supervises services on Linux hosts; provisioning installs its units. `--ssh-user`
sets that login; `--service-user` sets the account that owns the node checkout
and runs services (default `swarmy`). Set `--private-address` when hosts talk
over a separate internal interface. `--sandboxes` sets node capacity. A node
group may be extended with:

```sh
swarmy swarm node add demo box --host 203.0.113.11 --ssh-key <bootstrap-key>
```

The primary is provisioned first; upgrade and teardown visit nodes first.
Remove existing node hosts with `swarmy swarm scale`.

After provisioning, client commands such as `swarmy --swarm demo doctor` and
`swarmy --swarm demo chat` reach the API through an on-demand SSH tunnel. The
tunnel selects free local ports. Run `swarmy swarm use demo` to make it the
default. `swarmy swarm status demo` reports the hosts and tunnel;
`swarmy swarm logs demo --unit swarmyd` follows the node journal.

`sandboxes` defaults to 64 on node hosts; set it to zero for a control-only
host. `private_address` defaults to `address`. SSH key paths must be absolute.
`swarmy swarm up demo` can be rerun after a partial failure: it skips hosts
already marked provisioned in the record. Node add writes the new host to the
spec, discarding comments, then provisions it. Down keeps the spec so the
swarm can be provisioned again.

S3 is an object-storage API for buckets and keys. The backing object store
can use a compatible service. Add public bucket coordinates to the spec,
never keys:

```toml
[bucket]
name = "demo-bucket"
endpoint = "https://objects.example.invalid"
region = "eu-west-1"
prefix = "runs/demo"
conditional_create = true
```

On the first up, provide both static keys, with the secret from a private 0600
file or one line on standard input:

```sh
swarmy swarm up demo --s3-access-key KEY --s3-secret-stdin
```

Keys are stored in the private swarm record and uploaded to nodes; they are
never in the spec, process arguments, or status output. Up checks access
before recording the keys. Ownership is recorded as `managed-by=swarmy` and
`swarmy-remote=NAME` bucket tags where supported, or as a marker under the
prefix otherwise. Down removes the swarm's prefix in a bucket swarmy created.
A bucket swarmy does not manage is left untouched.

See [REMOTE.md](REMOTE.md#object-storage-permissions) for required permissions
and the compatibility probe. In-place static-key rotation is not supported: a
subsequent up refuses different keys on provisioned hosts. `down --destroy`
deletes owned bucket data, so plan a data-preserving migration instead.

## Cloud credentials

Cloud keys create machines before a swarm exists. They are stored on the
client, separate from `swarmy auth set`, whose credential store lives inside a
running swarm. The file is `~/.local/share/swarmy/cloud-credentials.toml` by
default; `XDG_DATA_HOME` moves it. It is written mode 0600 and refused if
readable by other users.

```sh
swarmy cloud login hetzner-cloud            # paste the token at the hidden prompt
swarmy cloud login hetzner-s3               # access key, then secret key (made in the Hetzner console)
swarmy cloud login aws --region eu-central-1
swarmy cloud ls
swarmy cloud logout aws
```

Pipe one value per line for unattended use, for example:

```sh
printf '%s\n' "$HCLOUD_TOKEN" | swarmy cloud login hetzner-cloud
```

Secrets are never accepted as arguments: arguments can appear in shell
history and process lists. Ctrl-C during a hidden prompt may leave terminal
echo off; run `stty echo` or `reset` to restore it.

| Entry | OpenTofu environment variables |
| --- | --- |
| hetzner-cloud | `HCLOUD_TOKEN` |
| hetzner-s3 | `MINIO_USER`, `MINIO_PASSWORD` |
| aws | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_REGION` |

Values reach OpenTofu only through the environment of each run. The Hetzner S3
endpoint comes from the bucket location in the swarm specification, not from
credentials.

## Swarms on cloud machines

An OpenTofu group describes machines created and tracked by OpenTofu, an
infrastructure-as-code tool. A group's `module` is `"hetzner-cloud"`, `"aws"`,
or an absolute path to your own module. Hetzner Cloud needs credentials from
`swarmy cloud login hetzner-cloud`, and its stack bucket also needs
`swarmy cloud login hetzner-s3`; AWS needs `swarmy cloud login aws`.
Credentials reach OpenTofu only through its environment. They are not put in
tfvars, the files
that supply module input values.

```sh
swarmy cloud login hetzner-cloud
swarmy cloud login hetzner-s3
swarmy swarm create fleet --on hetzner-cloud --size cpx41 --nodes 2
```

Up creates a working directory and state per group under the swarm's data
directory. It generates one SSH key per group and shares a network between
groups of the same module. It creates a bucket only when the stack group uses
hetzner-cloud and the spec has no `[bucket]` section.

Groups of the same module share one network, so they must be in the same
Hetzner network zone (fsn1, nbg1 and hel1 count as one zone) or the same AWS
region; otherwise up refuses. Swarmy waits for SSH and cloud-init, the
machine's first-boot configuration service, before provisioning hosts. Cloud
and existing-host groups can coexist in a mixed swarm. Down destroys OpenTofu
groups in reverse order and removes the bucket with the data.
`SWARMY_TOFU_BIN` is a development and test override for the pinned,
checksum-verified OpenTofu download.

## Planning changes

`swarmy swarm plan fleet` previews OpenTofu changes to each machine group
without applying them. Use it before up or scale to inspect the proposed
infrastructure.

## Lifecycle: stop, start, down

`stop` leaves data in place; `start` restarts stopped services. For an
ephemeral swarm, `down` removes data. A persistent swarm requires
`down --destroy`. The spec remains for future use.

| Kind | `stop` / `start` | `down` (ephemeral) | `down --destroy` (persistent) |
|---|---|---|---|
| local | stop / restart processes; keep data | stop processes, delete owned stack data; keep explicit `stack_dir` | same; keep explicit `stack_dir` |
| existing | stop / restart units; keep hosts and data | remove units and the swarm's prefix in a bucket swarmy created; keep machines | same |
| cloud | stop / restart units; keep machines and data | destroy machines and owned bucket | same |

Down does not forget the spec. On existing hosts it never deletes machines.
For leftover service users, fstab entries, sandbox files and tunnel keys, see
[REMOTE.md](REMOTE.md#what-down-leaves-on-a-host).

## Status and logs

`swarmy swarm status fleet` shows hosts and tunnel state. For local swarms,
`swarmy swarm logs fleet` follows service logs. Select `scheduler`, `worker`,
`gateway`, `api` or `supervisor` to follow one service.

Host swarms default to the primary host's `swarmyd` journal. Pass
`--host NAME` to select another recorded host, or `--unit swarmyd` to select
a unit.
Client commands open a per-swarm SSH tunnel on demand.

## Upgrading

```sh
swarmy swarm upgrade demo
swarmy swarm upgrade demo --services-only
swarmy swarm upgrade demo --drain-timeout 600
```

Upgrade reconciles binaries and units in place. It drains sandboxes before
restarting nodes and upgrades the primary last. Development checkouts with
uncommitted changes are refused unless `--allow-dirty` is set. Machines
provisioned before the `swarmy-core` binary should be upgraded with the same
command; the installed units are reconciled.

## When no node has room

A full swarm, or a GPU requirement no node meets, parks sessions instead of
failing their calls. The scheduler retries every 30 seconds and sessions
continue on their own when capacity appears. After the configured maximum
session wait (one hour by default), pending calls fail without running.

Elastic groups will later add machines for these waits.

## Scaling node groups

```sh
swarmy swarm scale fleet workers 4
swarmy swarm scale fleet workers 2 --drain-timeout 1200
```

Count must be between 1 and 50, so a group cannot be scaled to zero. Growing
updates the spec and reruns the normal provisioning path; already provisioned
machines are skipped. Shrinking removes the highest-numbered nodes first. It
waits up to `--drain-timeout` seconds per node (600 by default) for running
sandbox commands and turns to finish. The node agent then stops cleanly,
checkpointing and releasing its computers, which start on another node at
their next turn. `--force` stops nodes that remain busy or removes unreachable
nodes; their computers recover when leases expire. The node tunnel key is
revoked on the stack host. OpenTofu node machines are destroyed, while
existing hosts have swarmy removed but the machines themselves are kept. Add
an existing host with `swarmy swarm node add` rather than growing its count.
Stack and services groups and local swarms cannot be scaled. Scale rewrites
the spec without preserving comments. After a failure, rerun the same scale
command to finish; hand-lowering `count` and running `up` remains refused.

See [REMOTE.md](REMOTE.md#draining-a-node) for how draining works on a node.

## Model provider credentials

On the first `swarmy swarm up NAME`, `--copy-credential` copies the ChatGPT
credential file (`selection.credential_file`, by default
`~/.local/share/swarmy/auth.json`) and the local cluster keyring to the
control host over SSH. Up refuses without it when your configuration selects
ChatGPT, and refuses with it when that file does not exist. Other providers need
no copied file.
A later up skips an already-provisioned primary and copies nothing, so decide
before the first up. Node hosts need no provider credential.

For an API-key provider, run
`swarmy --swarm NAME auth set anthropic --from-env` after up instead; the key
is stored inside the swarm. See [providers.md](providers.md).

## Acceptance run on Hetzner Cloud

Only the owner runs the approved acceptance test. The script creates a control
and a workers group (three machines at peak), checks a model turn and durable
sandbox file, scales the workers group to two and back to one using the
`swarmy swarm scale` command, and verifies teardown against Hetzner Cloud and
Object Storage. It prints a maximum cost estimate before the `--yes` gate,
bounds the run to 90 minutes by default, and allows 30 more minutes for
teardown. The default server type, cpx32, keeps the three machines at 12
shared vCPUs, under the limit new Hetzner accounts start with; pass
`--server-type` for another type that is available in the location. If leftovers remain, follow the printed manual cleanup instructions
in the Hetzner console before attempting another run. The dry run uses fake
OpenTofu and needs no cloud access. The real run needs a configured swarmy
`config.toml` so doctor can verify the configuration. Provide a nonempty 0600
model API key file; for ChatGPT, omit the key file and use its configured
credential instead.

```sh
printf '%s\n' "$HCLOUD_TOKEN" | swarmy cloud login hetzner-cloud
swarmy cloud login hetzner-s3
scripts/acceptance-hetzner-cloud.sh --dry-run
scripts/acceptance-hetzner-cloud.sh --provider PROVIDER --model MODEL --api-key-file /path/to/key --yes
```

## Further reading

[REMOTE.md](REMOTE.md) covers host internals;
[infra/README.md](../infra/README.md) covers modules;
[cloud-substrate.md](cloud-substrate.md) describes cloud boundaries;
[fleet-runbook.md](fleet-runbook.md) covers the fleet; [DEV.md](DEV.md) covers
checkout development.
