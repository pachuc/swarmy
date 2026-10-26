# Swarms

A swarm is one deployment: a backing stack, control services, and sandbox nodes, described once as a spec. Swarmy creates or adopts its machines, installs everything, and runs its lifecycle.

```sh
swarmy swarm create fleet --on hetzner-cloud --size cpx41 --nodes 2
swarmy swarm plan fleet
swarmy swarm up fleet
swarmy --swarm fleet chat
swarmy swarm scale fleet workers 3
swarmy swarm down fleet --destroy
```

## Concepts

A **swarm** is the unit of deployment. Its **spec** describes desired infrastructure and placement; the private **record** tracks provisioned hosts, credentials and connection details. A **group** supplies machines from one **module**. **Placement** assigns stack, services and node roles to groups. **Lifecycle** is ephemeral (down removes data) or persistent (down requires explicit `--destroy`).

| Module | Machines | Who deletes them |
|---|---|---|
| `local` | this machine | nothing to delete |
| `existing` | hosts you list | never deleted, only cleaned |
| `hetzner-cloud` | created by OpenTofu | destroyed on `down` |
| `aws` | created by OpenTofu | destroyed on `down` |
| a path | your OpenTofu module | destroyed on `down` |

OpenTofu is an open-source infrastructure-as-code tool that creates machines from a description and records them in a state file. Swarmy downloads a pinned, checksum-verified release on first use. Placement assigns `stack`, `services`, and `nodes`. Only the fully remote layout (all three roles on machines) is tested end to end; services on this machine with a remote stack is refused.

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

For hosts you own, use a group like this:

```toml
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

The XDG defaults are `~/.config`, `~/.local/share`, `~/.local/state`, and `~/.cache`; their respective `XDG_*_HOME` variables override them. Specs and records are private.

(`XDG_CONFIG_HOME`, `XDG_DATA_HOME`, and
`XDG_STATE_HOME`, with HOME-based defaults):

| Path under swarmy's XDG root | Contents |
|---|---|
| config/swarms/NAME.toml | Private swarm spec |
| data/swarms/NAME/record.toml | Private API URL and token record |
| data/swarms/NAME/services.toml | Effective service settings, regenerated on each up/start; do not edit |
| data/swarms/NAME/stack/ | Default backing stack data, owned by the swarm |
| state/swarms/NAME/ | Supervisor and service pids, ready marker, logs, fake files, API uploads |
| state/local.lock | Machine-wide local lifecycle lock |
| data/keyring | Shared local cluster keyring |

The config root holds `swarms/NAME.toml`; the data root holds `swarms/NAME/record.toml`, `keys/`, `keys/groups/`, `tofu/<group>/`, `tunnel/`, and for local swarms `services.toml` and `stack/`. The state root holds `swarms/NAME/` with pids and logs, plus `current` and `local.lock`. The cache holds the matching `swarmy-core`, base images, and OpenTofu.

## Choosing the target swarm

Client commands choose their API in order: `--swarm NAME`, `SWARMY_SWARM`,
the `SWARMY_API_URL` and `SWARMY_API_TOKEN` pair, then the current swarm.
Without any target, the client explains how to list, use, or create a swarm.
Configuration files never choose the client's API: `[api] url` and `token`
tune local services. `swarmy swarm up NAME` records a local swarm's API, and `swarm create` makes the new swarm current when none is selected.

Specs live under `$XDG_CONFIG_HOME/swarmy/swarms`, records under
`$XDG_DATA_HOME/swarmy/swarms`, and the `current` file under
`$XDG_STATE_HOME/swarmy` (with standard HOME-based XDG defaults).

```sh
swarmy swarm ls
swarmy swarm show
swarmy swarm use dev
printf '%s' "$TOKEN" | swarmy swarm attach staging --url https://swarmy.example.test --token-stdin
swarmy --swarm staging session ls
swarmy swarm rm staging
```

## Swarms on this machine

A local swarm runs FoundationDB, NATS, SeaweedFS, and the scheduler, worker,
gateway, and API on this machine. Create it once, then use the same name to
manage it. It does not run a sandbox node, so tool-calling turns still need a
node. Ephemeral swarms delete their data on `down`; persistent swarms require
`down --destroy` to delete data. `stop` and `start` preserve data.

```sh
swarmy swarm create demo --local
swarmy swarm up demo
swarmy swarm status demo
swarmy swarm logs demo worker
swarmy swarm stop demo
swarmy swarm start demo
swarmy swarm down demo
```

A release client downloads its matching `swarmy-core`; a development client finds the sibling binary or one on PATH. Use `swarmy swarm up demo --core-bin PATH` to select one explicitly. Fixed ports allow only one local swarm to run at a time. `swarmy swarm rm demo` refuses a running swarm; stop it first. See [DEV.md](DEV.md#local-swarms) for checkout-specific stack sharing and tuning.

## Swarms on hosts you own

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

On the first `swarmy swarm up demo`, optionally pass `--copy-credential` to
copy the configured provider credential to the control host over SSH.
Ordinary node hosts need no provider credential. After provisioning, client
commands such as `swarmy --swarm demo doctor` and `swarmy --swarm demo chat`
reach the API through the on-demand tunnel. Run `swarmy swarm use demo` to
make it the default. `swarmy swarm status demo` reports the hosts and tunnel;
`swarmy swarm logs demo --unit swarmyd` follows the node journal. Once the
primary is provisioned, another `up --copy-credential` skips it and does not
copy a credential; decide whether to copy it before the first up.

`sandboxes` defaults to 64 on node hosts; set it to zero for a control-only
host. `private_address` defaults to `address`. SSH key paths must be absolute.
`swarmy swarm up demo` can be re-run after a partial failure: it skips hosts
already marked provisioned in the record. `swarmy swarm node add demo box
--host 203.0.113.11 --ssh-key <bootstrap-key>` writes the new host to the
spec (discarding comments), then provisions it. `swarmy swarm down demo
--destroy` keeps the spec so the swarm can be provisioned again.

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

See [REMOTE.md](REMOTE.md#object-storage-permissions) for required permissions and the compatibility probe. In-place static-key rotation is not supported: a subsequent up refuses different keys on provisioned hosts. `down --destroy` deletes owned bucket data, so plan a data-preserving migration instead.

## Cloud credentials

Cloud keys create machines before a swarm exists. They are stored on the client,
separate from `swarmy auth set`, whose credential store lives inside a running
swarm. The file is `~/.local/share/swarmy/cloud-credentials.toml` by default;
`XDG_DATA_HOME` moves it. It is written mode 0600 and refused if readable by
other users.

```sh
swarmy cloud login hetzner-cloud            # paste the token at the hidden prompt
swarmy cloud login hetzner-s3               # access key, then secret key (made in the Hetzner console)
swarmy cloud login aws --region eu-central-1
swarmy cloud ls
swarmy cloud logout aws
```

Pipe one value per line for unattended use, for example
`printf '%s\n' "$HCLOUD_TOKEN" | swarmy cloud login hetzner-cloud`.
Secrets are never accepted as arguments: arguments can appear in shell history
and process lists. Ctrl-C during a hidden prompt may leave terminal echo off;
run `stty echo` or `reset` to restore it.

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

## Planning changes

`swarmy swarm plan fleet` previews OpenTofu changes to each machine group without applying them. Use it before up or scale to inspect the proposed infrastructure.

## Lifecycle: stop, start, down

`stop` leaves data in place; `start` restarts the stopped services. `down` deletes data for an ephemeral swarm. A persistent swarm refuses `down` unless `--destroy` is supplied. The spec remains for future use.

| Kind | `stop` / `start` | `down` (ephemeral) | `down --destroy` (persistent) |
|---|---|---|---|
| local | stop / restart processes; keep stack and database | stop processes, delete owned stack data | same; explicit checkout `stack_dir` remains |
| existing | stop / restart units; keep hosts and database | remove units and owned bucket scope; keep machines | same; deletes owned bucket scope |
| cloud | stop / restart units; keep machines and database | remove units, destroy machines and owned bucket | same; deletes owned bucket data |

Down does not forget the spec. On existing hosts it never deletes machines. For leftover service users, fstab entries, sandbox files, and tunnel keys, see [REMOTE.md](REMOTE.md#what-down-leaves-on-a-host).

## Status and logs

`swarmy swarm status fleet` shows hosts and tunnel state. `swarmy swarm logs fleet` follows service logs; add a service name for local swarms, or `--host NAME` and `--unit swarmyd` for a host journal. Client commands open a per-swarm SSH tunnel on demand.

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

## Scaling node groups

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

On the node, draining checks for busy sandbox commands, waits for a clean stop and does not cordon. See [REMOTE.md](REMOTE.md#draining-a-node).

## Model provider credentials

On the first `swarmy swarm up demo`, `--copy-credential` copies the configured provider credential to control services over SSH. A later up skips an already-provisioned primary and does not copy it. Ordinary node hosts need no provider credential. Against the selected swarm, use `swarmy auth set anthropic --from-env` or `swarmy auth login chatgpt` for credentials stored in the swarm. See [providers.md](providers.md) for provider setup.

## Acceptance run on Hetzner Cloud

Only the owner runs the approved acceptance test. The script creates a control
and a workers group (three machines at peak), checks a model turn and durable
sandbox file, scales the workers, and verifies teardown against Hetzner Cloud
and Object Storage. It prints a maximum cost estimate before the `--yes` gate,
bounds the run to 90 minutes by default, and allows 30 more minutes for teardown.
If leftovers remain, follow the printed manual cleanup instructions in the
Hetzner console before attempting another run. The dry run uses fake OpenTofu
and needs no cloud access. Provide a nonempty 0600 model API key file; for
ChatGPT, omit the key file and use its configured credential instead.

```sh
printf '%s\n' "$HCLOUD_TOKEN" | swarmy cloud login hetzner-cloud
swarmy cloud login hetzner-s3
scripts/acceptance-hetzner-cloud.sh --dry-run
scripts/acceptance-hetzner-cloud.sh --provider PROVIDER --model MODEL --api-key-file /path/to/key --yes
```

## Further reading

[REMOTE.md](REMOTE.md) covers host internals; [infra/README.md](../infra/README.md) covers modules; [cloud-substrate.md](cloud-substrate.md) describes cloud boundaries; [fleet-runbook.md](fleet-runbook.md) covers the fleet; [DEV.md](DEV.md) covers checkout development.

