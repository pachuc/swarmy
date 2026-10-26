# Remote development nodes

The laptop-services layout (the former `dev up` with a remote profile) has been removed;
adopt remotes with `--services node`; remotes adopted with the default laptop
services are no longer usable from client commands. `swarmy remote connect NAME`
registers the node's API as swarm NAME, so client commands take `--swarm NAME`.

This guide covers existing Ubuntu 24.04 hosts provisioned over SSH with
`swarmy remote adopt NAME`. EC2 launching was removed on 2026-10-26.
The client uploads scripts, image recipes and verified release binaries.
Development clients run inside a checkout, which is copied to the host and
built there. For fleet-driver commands see [fleet-runbook.md](fleet-runbook.md).

### Node directory

```
~<service user>/swarmy/
  scripts/   provisioning, upgrade, services, decommission, dev-stack, install-dev-tools (replaced on every up/adopt/add-node/upgrade)
  images/    carried recipes (base-ubuntu, base-desktop, swarmy-dev, common)
  .swarmy/release/   release binaries (release clients)
  source/    development clients' checkout copy and its target/
  .swarmy/config.toml, .swarmy/node/, .swarmy/volumes, .swarmy/tunnel-key*, .dev/   node state; uploads never touch it
```

Scripts always match the client binary. Rebuild a development client after
editing a script. Nodes provisioned before this change retain unused top-level
checkout files: remove `crates/`, `target/`, and other old checkout files by
hand, never `.swarmy/` or `.dev/`. Alternatively, re-provision.

Pass `--services node` so scheduler, worker, and gateway run on the control
node; client commands reach only node services. The client needs
`ssh`, `ssh-keygen`, and `rsync` 3.2.3 or newer. Build with the opt-in
`remote` feature to enable the remote commands.

Configure an existing host in `.swarmy/config.toml`:

```toml
[remote]
provider = "existing"
region = "eu-west-1"
service_user = "swarmy"
local_storage = "dir:/srv/swarmy-local"
# bucket = "NAME"
```

Older `[remote.aws]` keys still parse for saved records, but nothing uses
them. Sandbox nodes require a local block device or `dir:/path` directory;
control-only nodes (`--sandboxes 0`) can use the root disk. Provisioning
creates the service user if missing. The SSH bootstrap login is independent
of the service user.

```sh
swarmy remote adopt demo --host <machine-address> --ssh-key <bootstrap-key> --local-storage dir:/srv/swarmy-local --services node --sandboxes 0
swarmy remote add-node demo --host <machine-address> --ssh-key <bootstrap-key>
swarmy remote connect demo
swarmy doctor --swarm demo
swarmy chat --swarm demo
swarmy remote ls
swarmy remote disconnect demo
swarmy remote down demo
```

`adopt` builds the default image on the first host. `add-node` provisions
another operator-owned host and does not create a machine. Both accept a
sandbox limit. `remote down` decommissions the hosts and removes swarmy's
files and local state; it never deletes the machines.

### Upgrade a running remote

Run `swarmy remote upgrade NAME` from any directory with a release client, or
from a clean local checkout with a development client. It reports the
local CLI version and every node's installed `swarmy-core` version before copying
anything. Joining nodes upgrade first; the first node, which owns the backing
services, upgrades last. The upgrade uses the provisioning rsync exclusions and
keeps instances, disks, node state, and `/etc/swarmy/node.env` unchanged. It
installs changed binaries and rewrites installed swarmy units whose text differs
from provisioning. A changed service or node unit restarts just like a changed
binary; the node still waits for its sandbox commands to drain. Systemd is
reloaded once if any unit changed. The backing `swarmy-stack.service` and
`swarmy-tunnel.service` units are rewritten but never restarted by upgrade:
changes take effect on their next restart or reboot. FDB, NATS, and the data
in `~<user>/swarmy/.dev` are untouched.

Provisioning records unit settings in `/etc/swarmy/units.env`. For older nodes,
upgrade derives those settings once from installed units and `node.env`, then
records them. Units still waiting for restart are kept in
`.swarmy/upgrade-pending-units` and retried on the next upgrade. `--services-only`
rewrites only installed service units; it neither rewrites nor restarts the node
or backing units, and it never restarts the node role. It leaves any old binary
still referenced by an installed unit. Restarting the node evicts every
placement, so drain it first. A full upgrade completes
that work. `--drain-timeout SECONDS` defaults to 600. `--allow-dirty` applies
only to development clients. `--json` reports `changed` binaries, `units`
rewritten, and `restarted` roles per node. A missing API token is filled once
without rotating an existing token, and the API restarts to load it.
The script accepts `build` or `prebuilt` as its optional fourth argument.
Development-client upgrades on release-provisioned nodes fail without a Rust
toolchain; use a release client or `SWARMY_RELEASE_BASE_URL` instead.
`remote ls` reports each remote's token state as `set`, `missing`, or
`not-applicable`.

### Upgrading a node provisioned before swarmy-core

Run a full `swarmy remote upgrade NAME` without `--services-only` from a client
at this version. The `units` list should include `swarmyd.service` and the
installed service units; old binaries in `/usr/local/bin` are removed once no
installed unit references them. Check `systemctl cat swarmyd.service` for
`ExecStart=/usr/local/bin/swarmy-core node`, and inspect `ls /usr/local/bin`.
Do not use `down` or adopt again: `down` deletes the node directory and its FDB
data. If the recorded mode or user does not match, provision the node again
with the same flags to rewrite `units.env`. Upgrading from an older client
after this is not supported.

`--image-recipe` accepts the carried recipes `images/base-ubuntu`,
`images/base-desktop`, and `images/swarmy-dev`. For a custom recipe, use
`swarmy image build --swarm NAME DIR`. The selected recipe still registers
as `base-ubuntu:NAME`. Use `--no-image` to skip the build; the two options
cannot be combined.
`add-node` reuses the stack's registry and does not rebuild the image.
After a successful build, the node record stores `default_image`.
`<state_dir>/remote/NAME.tunnel/` holds its SSH record and log. The node's
API uses its recorded `default_image` for new sessions; an explicit `--image`
still wins. Remotes created with `--no-image` require an explicit registered
image if the node has no default.

Node records are private JSON files under `<state directory>/remote`.
They retain host addresses, SSH user, key path, port settings, and the
provisioned nodes. A failed `adopt` or `add-node` keeps its record for a
retry with `remote down` before re-adopting. The last host is never deleted
by swarmy; only its services and files are decommissioned.

All service units share one `swarmy-core` executable. `swarmyd.service` runs `/usr/local/bin/swarmy-core node` as root; it needs NBD and runc. Service units run `swarmy-core scheduler`, `swarmy-core worker`, `swarmy-core gateway`, and `swarmy-core api`. A changed core restarts each service unit; the node unit restarts only after the drain.

On the node, `sudo systemctl status swarmy-stack swarmyd` shows the services,
and `sudo journalctl -u swarmyd -f` follows node logs. The provisioning script
writes `/etc/swarmy/node.env`, enables both units at boot, and configures
`Restart=always` for swarmyd. To rerun provisioning, use
`cd ~/swarmy && bash scripts/remote-provision.sh stack PRIVATE_IP BUCKET REGION ENDPOINT PREFIX CONDITIONAL_CREATE STATIC_KEYS SANDBOXES SERVICE_USER LOCAL_STORAGE PREBUILT`
on the first node. `PREBUILT` is `true` for an uploaded release bundle and defaults
to `false`; `SERVICE_USER` defaults to `swarmy`; sandbox nodes require a block
device or `dir:/path` as `LOCAL_STORAGE`. Joining nodes run swarmyd and
`swarmy-tunnel.service`; rerun with `node FIRST_NODE_PRIVATE_IP` followed by
the same bucket and storage arguments. Their cluster file is
copied from the first node, preserving its cluster identity and loopback
coordinator address. `add-node` generates a dedicated tunnel key on the joining
node and authorizes it for service forwards on the first node. It pins the first
node's host key using the authenticated provisioning connection. The key permits
no interactive shell. The tunnel forwards local ports 4500 and 4222 to
the same loopback ports on the first node, plus 8333 only when SeaweedFS is used. It starts before swarmyd and restarts
automatically after SSH failure. Inspect it with
`sudo journalctl -u swarmy-tunnel`. Its identity and pinned host key are stored
under `/etc/swarmy/`, readable only by the service user. The scheduler, gateway, worker,
and CLI release binaries are also installed in `/usr/local/bin`.

A local disk mount persists across reboot. `down` deletes swarmy's node
directory and its data, but never the machine or operator-owned storage.
`add-node` adds compute capacity, not replicas of backing services. Losing
the first node still loses this development stack.

Doctor's checks are described under [Tunnel and profile reference](#tunnel-and-profile-reference):
control master and port mapping, the control-plane API snapshot, and the S3
check.

Connect's JSON preserves the profile fields and adds `timing` with
`elapsed_seconds`, `address_probe_seconds`, `tunnel_startup_seconds`, and
`reused`. Total time includes local setup and profile publication. Startup
includes port allocation, SSH readiness, and fetching the cluster identity.
Reusing a healthy control master reports `reused: true` and zero for the two
skipped phases.

## Persistent object storage

Adopt with a static-key S3-compatible bucket and its endpoint:

```sh
swarmy remote adopt demo --host <machine-address> --ssh-key <bootstrap-key> \
  --bucket NAME --s3-endpoint https://objects.example.invalid \
  --s3-region eu-west-1 --s3-prefix runs/team \
  --s3-access-key KEY --s3-secret-file ~/.local/share/swarmy/demo-s3-secret
```

The secret can also arrive from a private `--s3-secret-file`,
`--s3-secret-stdin`, or `AWS_SECRET_ACCESS_KEY`; the access key can come from
`--s3-access-key` or `AWS_ACCESS_KEY_ID`. Static keys are saved in mode 0600
state and copied to node environment files over SSH stdin. Never put secrets
on the command line or in logs. The bucket description may instead be stored
in `[remote.bucket]` with `[remote.bucket.credentials] source = "static_keys"`.

`adopt` creates a bucket when absent or adopts an empty bucket. Ownership is
recorded with `managed-by=swarmy` and `swarmy-remote=NAME` tags where the
provider supports tags, otherwise with a marker object under the prefix.
`remote down` deletes only the owned prefix scope and deletes the bucket only
if nothing else remains. `--keep-bucket` retains it. Missing ownership leaves
it in place; to clean it up later grant tagging permissions and tag it by
hand. There is no IAM role or instance profile.

The keys need `HeadBucket`, `CreateBucket`, `GetBucketTagging`,
`PutBucketTagging`, `GetObject`, `PutObject`, `ListObjectsV2`,
`ListObjectVersions`, `DeleteObjects`, and `DeleteBucket` permissions on
the bucket and objects. The service's regular object-store operations use
the same keys. Providers without conditional PUT support can set
`[remote.bucket] conditional_create = false`.

### Object storage compatibility probe

Before the fleet depends on a provider, run the compatibility probe against
one of its buckets. It exercises every S3 call swarmy makes: bucket existence
through the provisioning client's `HeadBucket`, then PUT, create-only PUT on
a new and an existing key, GET, HEAD, prefixed listing with pagination, and
DELETE through the same object client the volume and blob stores use. Ranged
GETs are not probed: chunks, manifests, and blobs are always read whole, so
swarmy never sends a range request. The probe writes only under one
`probe-<id>/` prefix and deletes it afterwards. Each check prints one JSON
line naming only the check and whether it passed; the endpoint, bucket,
prefix, and key material never appear, so the output is safe to paste into a
pull request.

```sh
cargo run --locked -p swarmy-volume --example s3-compat-probe -- \
  --endpoint https://objects.example.invalid --region eu-west-1 \
  --bucket NAME --access-key KEY --secret-file ~/.local/share/swarmy/demo-s3-secret
```

Coordinates fall back to `SWARMY_S3_ENDPOINT`, `SWARMY_S3_REGION`,
`SWARMY_S3_BUCKET`, `SWARMY_S3_ACCESS_KEY`, and `SWARMY_S3_SECRET_KEY`, so
with a sourced `.dev/env` the command takes no flags. The secret key comes
from `--secret-file` (readable only by its owner), `--secret-stdin` (one
line), or the environment, in the same precedence `remote adopt` accepts. When
the bucket description sets `conditional_create = false`, pass
`--conditional-create=false` so the second create-only PUT is expected to
overwrite through the same plain-PUT fallback the nodes use.

Results, measured with the probe (SeaweedFS from the development stack,
Hetzner Object Storage from the operator run):

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

SeaweedFS supports the create-only PUT, so the development stack keeps
`conditional_create = true`. If the Hetzner run reports `create_existing` as
failed with the overwrite detail, set `conditional_create = false` in the
bucket description and re-run the probe with `--conditional-create=false`
before provisioning. `scripts/test-s3-compat-probe.sh` runs both modes
against the development stack in CI.

Rotating static keys is a re-provisioning operation: `remote upgrade` never
modifies `node.env` or service units for key changes, so run `remote down
--keep-bucket` followed by `remote adopt` with the new keys. The kept bucket is
still owned by the remote, so `adopt` reuses it instead of creating a new one.

## Costs and recovery

Swarmy does not create or delete machines. The operator owns host allocation
and billing. Inference and external object storage may also cost money.
`swarmy remote ls` shows saved hosts and their SSH and heartbeat status;
it does not query a cloud provider's billing or power state. Disconnecting
leaves host services running. To decommission:

```sh
swarmy remote disconnect demo
swarmy remote down demo --yes
```

`remote down` asks for confirmation before deleting an owned bucket unless
`--yes` is set. `--keep-bucket` leaves that bucket untouched. Failed host
cleanup retains its record so a later `down` can retry. It removes swarmy's
files and node directory, never the underlying machine.

## Client prerequisites

From a checkout with the pinned Rust toolchain, C/C++ compiler, pkg-config,
clang/libclang, `ssh`, `ssh-keygen`, and `rsync`, install user-owned backing
tools and build the remote-enabled CLI:

```sh
scripts/install-dev-tools.sh
rustup show
SWARMY_FDB_LIB_DIR="$HOME/.local/lib" cargo build --workspace --locked --features swarmy-cli/remote
export PATH="$PWD/target/debug:$PATH"
```

The client itself does not link FoundationDB; a source workspace build still
needs its library for services. Supply AWS provisioning credentials through
the SDK credential chain, not the checkout. No local root, NBD device, cloud
CLI, or container runtime is required by `swarmy remote`; provisioning needs
passwordless sudo on the Ubuntu node. The supported client and supervisor
target for this procedure is Linux x86-64. See
[DEV.md](DEV.md) for local-stack prerequisites and
[remote implementation](../crates/swarmy-cloud/src/lib.rs) for provisioning.

## Workflow and reference procedures

The following examples exercise client behavior and recovery on a disposable
remote; the provisioning and tunnel contracts above still apply.

### Up, authenticate, and chat


```bash
chmod 600 .swarmy/config.toml
swarmy remote adopt demo --services node  # builds binaries and base-ubuntu:demo; prints build times and SSH command
swarmy remote connect demo
swarmy auth login              # dedicated ChatGPT login, on the laptop
swarmy doctor --swarm demo
swarmy remote ls
```

The API, NATS and S3 use automatically selected free local ports.

`adopt` finishes by building `images/base-ubuntu` as root on the node using
`/etc/swarmy/node.env`. It streams the build progress, reports its duration,
and registers `base-ubuntu:demo`. The node records that image as its
`default_image`, so new sessions need no image flag or local image configuration. Image construction needs node root; it never needs laptop root.
`swarmy remote ls` lists registered images while the remote is connected.

Start a chat on the laptop and ask the agent to run `pwd` in its sandbox:

```bash
swarmy chat --swarm demo
# Alternatively, start a session and resume its printed id:
swarmy run --swarm demo 'Run pwd in the sandbox, then wait for my next instruction.'
swarmy chat --swarm demo SESSION_ID
```

Use `--image-recipe images/base-desktop` to select another carried recipe.
For a custom directory use `swarmy image build --swarm NAME DIR`. The registered
name remains `base-ubuntu:demo`. `--no-image` skips the build and leaves the remote
without a saved default; provide an already registered image through `--image`
or `default_image` before starting a new session. The two options cannot be
combined. An explicit session `--image NAME:TAG` overrides the node's default.

In the session, ask the agent to use `process_start` to run
`python3 -u -m http.server 18765 --bind 127.0.0.1`. In the next turn ask it to
fetch `http://127.0.0.1:18765/` with `bash` and verify the server is still listed
by `process_list`. Then ask it to write a marker file and call `checkpoint`.
An acknowledged checkpoint makes the disk durable; it does not save running
processes. Escape or Ctrl-C closes chat while the session remains stored.

In the older file-backed laptop-services mode, a local gateway read the
configured credential file; current credentials are encrypted in the store. Remote provisioning
copies the development checkout into `source/` while excluding `.swarmy/`, `.dev/`, `.git/`, `target/`,
`.env`, and `.env.*`; it does not copy the laptop's home or credential cache.
The configured credential path is also excluded from the checkout copy.
Do not share its refresh writer with a running Codex login. Node services have
the explicit credential-transfer option described below. Prompts, outputs, and session
history do live in the remote backing services. The fake-provider acceptance
run requires no ChatGPT credential; see the dated benchmark evidence.


### Ephemeral sessions and named agents


Without `--agent`, each new conversation gets its own computer and writable disk:

```bash
swarmy chat --swarm demo
# In another terminal, create a separate ephemeral conversation:
swarmy chat --swarm demo
swarmy session ls --swarm demo
swarmy chat --swarm demo SESSION_ID   # resume an existing conversation
swarmy session close SESSION_ID --swarm demo
swarmy session show SESSION_ID --swarm demo --json
```

Escape, Ctrl-C, or EOF in `chat --json` closes the client only. The session
and computer remain available for resume. `session close` completes an ephemeral
session and deletes its computer while keeping its transcript. The scheduler
also closes Idle ephemeral sessions after 24 hours by default. For a disposable
retention test on node services, set the override in the scheduler's systemd
environment and restart it. The sweep
runs every 60 seconds or every retention interval if shorter. Only idle sessions
strictly older than the cutoff qualify; active sessions and named agents do not.

Create a named agent to keep a computer independently of any one conversation:

```bash
swarmy agent create tommy --swarm demo --description 'Shared development computer'
swarmy chat --agent tommy --swarm demo
# In a second terminal, open another session on the same computer:
swarmy chat --agent tommy --new --swarm demo
swarmy agent ls --swarm demo
swarmy agent show tommy --swarm demo --json
swarmy run --agent tommy --swarm demo 'Read the files created in the other chat'
```

Ask the first chat to write a file and use `process_start` to start
`python3 -u -m http.server 18765 --bind 127.0.0.1`. Ask the second to read that
file, fetch the server with `curl`, and list managed processes. Both sessions
share those files and processes. Simultaneous tool calls queue for the shared
computer, with one call running at a time. Each conversation has its own log.
`agent show` reports the sampled holder session, queued call count, node, epoch,
and observation expiry, plus the last committed disk snapshot time. Missing
or stale samples mean unknown activity. Busy includes computer startup.

Creation pins `default_image` or an explicit `--image NAME:TAG`. New sessions
on the named agent use that pin, so `--agent` cannot be combined with `--image`
or a resume session id. Names and agent ids both work. `chat --agent` and
`run --agent` resume the main session by default, creating it if absent. `--new`
opens a side conversation without changing that pointer. Quitting either client
leaves the agent available, and ephemeral retention never deletes it.

Ask for `checkpoint` before the daemon-kill procedure below. After recovery,
both transcripts receive the same rebuild notice, including idle chats. Read
the checkpointed file from both sessions and restart the server: files survive
at the snapshot boundary, while background processes and memory are lost.
Then delete the shared computer:

```bash
swarmy agent delete tommy --swarm demo            # interactive confirmation
# For scripts, use --yes on the same command.
swarmy session show FIRST_SESSION_ID --swarm demo --json
swarmy session show SECOND_SESSION_ID --swarm demo --json
```

`session close` refuses the main session and points to `agent delete` instead.
Side sessions can be closed while retaining the shared computer.
Deletion removes the identity, placement, and disk references immediately;
physical processes and attachments disappear on the node's next failed renewal.
Transcripts remain readable and further sandbox tools are refused. Recreating
`tommy` makes a new identity and computer.

Chunks are reclaimed separately. On an isolated disposable stack with no
pending writes, wait beyond a short grace window, inspect candidates, collect,
and confirm a subsequent pass has nothing more to delete:

`swarmy gc` starts the run on the control plane and follows its progress,
so the collection policy comes from the API host's configuration, not the
client's environment. For this disposable-stack procedure, set the short grace
in the API service's environment and restart it first:

```bash
swarmy gc --swarm demo --dry-run --json
swarmy gc --swarm demo --json
swarmy gc --swarm demo --dry-run --json
```

Use the normal six-hour grace for ongoing work. Images, other volumes, and
retained snapshots protect shared chunks, so freed bytes need not equal the
logical disk size. These commands report object bytes, not SeaweedFS filesystem
space after compaction. See the dated lifecycle run in
[volume benchmarks](volume-benchmarks.md) for transcript and reclamation evidence.


### Add capacity and exercise recovery


```bash
swarmy remote add-node demo
swarmy remote ls
swarmy remote logs demo         # Ctrl-C stops following the primary's journal
swarmy swarm logs dev worker          # local routing and placement diagnostics
```

`add-node` provisions another existing host using the saved settings and the
first node's private backing-service endpoints. It adds compute, not database
replicas. The first node must remain available. For a controlled failure, locate
the hosting node from worker/node logs and the saved JSON in
`.swarmy/remote/demo.json`. SSH to that node with its saved key and address,
then run `sudo systemctl kill --signal=SIGKILL swarmyd` **there**. The unit
restarts automatically after five seconds. Submit another tool turn in chat;
recovery must wait for expired placement and volume-writer leases. Check that
the durable rebuild notice reports the restart, lost processes, and latest
snapshot. An interrupted call may return the notice as an error; retry the
read-only marker check after recovery. The checkpointed marker should survive;
the HTTP server must be started again. A kill does not guarantee migration to the added node.


### Tunnel and profile reference


Only one remote tunnel can be active on the laptop: FoundationDB requires
its local port 4500 and rejects a remapped coordinator. Disconnect the old
remote before connecting another; the remote workers continue running while
the laptop is disconnected.

`swarmy remote connect NAME` reads `<state_dir>/remote/NAME.json`, starts an
SSH control master, and writes `NAME.profile.json` beside it. Its record lives
in `<state_dir>/remote/NAME.tunnel/`. `state_dir` defaults to the discovered
project's `.swarmy` directory, or `~/.local/state/swarmy`
(`$XDG_STATE_HOME/swarmy`) without a project file.
`SWARMY_STATE_DIR` can select another directory. Provisioning and tunnel commands share the
`swarmy_config::RemoteNode` JSON contract.

### Existing hosts

For providers where swarmy does not create machines itself (dedicated
servers today, any SSH-reachable host in general), `swarmy remote adopt`
builds a swarm on machines already owned. It records the remote with the
existing-host provider, then runs the same provisioning and service
installation `remote adopt` runs after its machine is ready: no machine is
created and no cloud machine call happens. Only the bucket setup touches
object-storage APIs.

```sh
swarmy remote adopt plain --host <machine-address> --ssh-key <bootstrap-key> \
  --local-storage dir:/srv/swarmy-local \
  --bucket <bucket> --s3-endpoint <endpoint-url> \
  --s3-access-key <access-key> --s3-secret-file <secret-file> \
  --service-user swarmy
```

`--host` is the machine's IP address, used for provisioning and tunnels.
`--ssh-user` (default `root`) is the bootstrap login and `--ssh-key` is its
private key file, copied into the 0600 remote state; provisioning creates
the `--service-user` login (default from configuration) with passwordless
sudo when it is missing. `--local-storage` is required for sandbox nodes
(a block device or `dir:/path`); control-only nodes (`--sandboxes 0`) leave
it empty. The bucket flags are the same object-bucket options `adopt` takes.
`--services`, `--copy-credential`, `--sandboxes`, `--no-image`, and
`--image-recipe` behave as in `adopt`.

`swarmy remote add-node NAME --host <machine-address> --ssh-key
<bootstrap-key>` joins another existing machine the same way, with
`--sandboxes`, `--local-storage`, and the bootstrap login. Joining nodes
reach the primary's backing services through its recorded private address;
where that is wrong (servers talking over public addresses or a private
network between dedicated servers), pass `--primary-address` with the
address to use instead. There is deliberately no AWS VPC assumption: the
setting selects the address. The service user stays the primary's so the
tunnel login matches.

`down`, `status`, `upgrade`, and `connect` work for adopted remotes
without any cloud machine call: no EC2 or instance API is ever touched
(`down` still deletes the owned bucket scope through object-storage APIs).
`down` stops and disables every swarmy unit
(`swarmy-stack` or `swarmy-tunnel`, `swarmyd`, and any
`scheduler`/`worker`/`gateway`/`api` units) through
`scripts/remote-decommission.sh`, which reads the same unit and binary
lists provisioning installs, then removes the units, the binaries in
`/usr/local/bin`, `/etc/modules-load.d/swarmy.conf`, the node environment
(`/etc/swarmy`, which holds static bucket keys and copied credentials),
and the node directory on every host. It deletes the owned bucket scope unless
`--keep-bucket` is passed, once every host is torn down (a failed host
keeps its record and the bucket until a re-run completes), and drops local
state. It never deletes the
machines themselves, and says so. A failed host does not block the others:
its real failure reason is reported, its record (and the bucket) is kept,
and `down` waits only briefly per host instead of the full provisioning
wait, so re-running `down` retries exactly the failed hosts.
`status` reports no instance type for existing hosts, and owned buckets still ask for confirmation before deletion.

`down` deliberately leaves the service user, the fstab line and its mount,
local sandbox disk data, and the tunnel keys joining nodes authorized on
the primary, so a re-provisioned host keeps its login, disks, and trust.
To remove those by hand after `down`, on each adopted host as root:

```sh
umount /mnt/swarmy-local  # only for device storage; skip for dir:/path
sed -i '/^LABEL=swarmy-local /d' /etc/fstab
userdel -r <service-user>  # also removes its home and any checkout remains
rm -f /etc/sudoers.d/90-swarmy-<service-user>
```

On the primary, edit the service user's `~/.ssh/authorized_keys` and
delete the joining nodes' restricted tunnel lines (the ones starting with
`restrict,port-forwarding,command="/bin/false"`). For `dir:/path` storage,
delete the directory used when its contents are no longer needed.

```sh
swarmy remote connect test
swarmy doctor --swarm test
swarmy run --swarm test 'what time is it'
swarmy chat --swarm test
swarmy remote ls
swarmy remote logs test
swarmy remote disconnect test
```

Selection also works with `export SWARMY_SWARM=test` or `swarmy swarm use test`;
see [DEV.md, Choosing the target swarm](DEV.md#choosing-the-target-swarm).

Node services stay running after disconnect. Disconnect stops the SSH control
master and removes its profile, cluster file, tunnel record in
`<state_dir>/remote/NAME.tunnel/`, and control socket; instance state and logs
remain. Reconnecting a healthy tunnel is
idempotent. SSH uses the configured identity, batch authentication, normal host
key verification with first-use acceptance, and keepalives. SSH diagnostics are
saved in `<state_dir>/remote/NAME.tunnel/ssh.log`. `remote logs` follows
the `swarmyd` systemd journal until interrupted; the SSH user needs permission to read that journal.

The API, NATS and S3 forwards use free local ports. FoundationDB first tries its
advertised port and falls back to a free port only on collision. Node-services
remotes forward only the API. All forwards bind to `127.0.0.1`. The profile
records the actual ports, PID, and socket;
`<state_dir>/remote/NAME.tunnel/tunnel.json` holds the tunnel record. Port selection and SSH binding cannot be atomic;
if another process claims a selected port, SSH fails startup and no profile is
published. Retry connect after resolving the collision.

FoundationDB needs special care: the local coordinator file preserves the
remote cluster identity and rewrites its address to the local tunnel. New
remotes and joining nodes forward to the first node's loopback address. Its transport verifies that the connected
port matches the server's advertised port. A remapped coordinator port fails
that check even when the destination is localhost. The local `127.0.0.1:4500` endpoint must reach the same remote database.
Free that port before connecting, or use a separate network namespace. Connect
still records and opens the alternative forward for inspection, but warns;
doctor reports the mapping failure, and configuration loading rejects it before
starting the native client. NATS and S3 support alternative ports normally.
See the port assertion in [FoundationDB's transport source](https://github.com/apple/foundationdb/blob/7.3.63/fdbrpc/FlowTransport.actor.cpp).

Doctor verifies the SSH control master, the FoundationDB port mapping, and a
live API snapshot (service heartbeats, nodes, images, credentials) through
the selected profile. A healthy control master alone does not pass: a
remapped coordinator port fails the port check, and only a readable API
snapshot passes. S3 remains a TCP check, and AWS-bucket remotes warn that
object storage is verified on the API host. Joining nodes use a systemd SSH
tunnel for all three services.

Connect prints total elapsed time, address probing time, and tunnel startup
time. JSON adds these seconds under `timing`, alongside `reused`; an existing
healthy tunnel reports zero for the skipped probe and startup phases.

Status reports saved host records and SSH reachability. An unreachable host has
unknown live state; this command does not query a cloud API. For each
connected stack it scans all registered swarmyd nodes, including stale records,
and shows heartbeat age (live means at most 30 seconds old). Registrations belong
to the stack; they are not attributed to a specific host registration. Store failures and disconnected tunnels report unknown
registration rather than claiming the node is absent. `--json` emits the same
information for scripts.

### Maintainer tunnel acceptance tests

These localhost tests exercise the SSH workflow without a cloud provider.
To run the SSH acceptance test on the launcher, authorize a temporary SSH key for
`ubuntu@127.0.0.1`, then run:

```sh
cargo build --workspace --locked --features swarmy-cli/remote
scripts/dev-stack.sh start
SWARMY_REMOTE_TEST_KEY=<path-to-test-key> scripts/test-remote.sh
```

The default run checks collisions against localhost, forwarded NATS traffic,
profile recording, the FoundationDB mapping diagnostic, and disconnect. Full
service acceptance needs a separate client network namespace, with the host's
sshd reachable over a veth pair and port 4500 free in the client. For example, use an unused subnet and namespace name:

```sh
sudo ip netns add swarmy-remote-test
sudo ip link add swarmy-host type veth peer name swarmy-client
sudo ip link set swarmy-client netns swarmy-remote-test
sudo ip addr add <host-veth-address>/30 dev swarmy-host
sudo ip link set swarmy-host up
sudo ip netns exec swarmy-remote-test ip addr add <client-veth-address>/30 dev swarmy-client
sudo ip netns exec swarmy-remote-test ip link set swarmy-client up
sudo ip netns exec swarmy-remote-test ip link set lo up
sudo ip netns exec swarmy-remote-test sudo -u ubuntu env \
  SWARMY_REMOTE_TEST_KEY=<path-to-test-key> \
  SWARMY_REMOTE_TEST_HOST=<host-veth-address> scripts/test-remote.sh
sudo ip netns delete swarmy-remote-test
```

That run checks the tunnel's forwards and disconnect from an isolated client.
The laptop-services checks it used to run were removed with that layout.
Without `SWARMY_REMOTE_TEST_KEY` it skips cleanly.
Remove the temporary authorized key and network namespace after testing.


### Running control-plane services on the node


Client commands reach only node services. The control plane runs under
systemd beside the store; `remote connect` registers its API with the swarm
registry.

```sh
# With provider = "fake", no credential is copied.
swarmy remote adopt demo --services node
swarmy remote connect demo
swarmy chat --swarm demo
```

Alternatively set `services = "node"` in `[remote]` before `remote adopt`.
`--services laptop` overrides that setting. The selected mode is saved in the
remote's launch settings; later edits to local config do not switch an existing
node. Node mode uses the configured provider, model, reasoning effort, system
prompt, store/bus namespace, and fake script. The fake script is sent over SSH;
if no script exists, the default greeting script is installed.

For a ChatGPT gateway, use `swarmy remote adopt demo --services node
--copy-credential`. This explicit flag acknowledges that the configured
`credential_file` leaves the laptop. A warning precedes transfer. SSH sends the
contents on stdin, and `/etc/swarmy/auth.json` is owned by ubuntu with mode 0600.
The cluster keyring goes to `~<service user>/.local/share/swarmy/keyring`
(mode 0600), where the node's services look for it.
The development source copy at `source/` excludes the configured credential path as well as
`.swarmy`, `.dev`, and environment files. Without the flag a ChatGPT node
fails before creating resources. Do not run a laptop gateway that refreshes the
same account concurrently. Stopping the services does not erase the copied file;
`remote down` terminates the node and its root disk.

`remote connect demo` registers the node's API as swarm `demo`; the services
keep running when the laptop disconnects. Use SSH and
`sudo systemctl stop swarmy-{scheduler,worker,gateway}` to pause node services.
Inspect their logs with `sudo journalctl -u swarmy-worker -u swarmy-gateway
-u swarmy-scheduler`. `remote logs` continues to follow the execution node log.

For node services, disconnect the tunnel before tearing down the remote:

```sh
swarmy remote disconnect demo
swarmy remote down demo
```

See the dated measurements in [volume benchmarks](volume-benchmarks.md) for
latency results and the remaining round trips. Node services are a development
mode on one backing-store node, without replicated storage or high availability.



### Agent memory files

The worker includes memory files in named-agent inference, including side
sessions. `[memory] dir` (`SWARMY_MEMORY_DIR`) defaults to
`/home/agent/memory`, and `[memory] max_bytes` (`SWARMY_MEMORY_MAX_BYTES`)
defaults to 32768. The node reads regular files in filename order and notes
when the byte budget truncates content; it skips directories, symlinks, and
special files. Use an absolute directory without symlink components. See
[worker memory handling](../crates/swarmy-worker/src/worker.rs).

## Fleet-specific node operations

Use `swarmy remote adopt` on each dedicated server with its bootstrap SSH
key and static-key object bucket. Run `swarmy remote add-node` with `--host`
and `--ssh-key` to add another existing host. `swarmy remote upgrade` updates
services in place. For recovery, re-adopt an available host after safely
removing failed state; teardown decommissions swarmy services, not hardware.

## Root suites and the nightly timer

Root-only NBD and sandbox suites are a manual operator responsibility under
[AGENTS.md](../AGENTS.md#building-and-testing). The former nightly node-suite
timer was removed; do not expect a scheduled root run. Use the scripts in
[`scripts/node-suites/`](../scripts/node-suites/) on a suitable node and record
results in the pull request. CI's reduced chaos checks are defined in
[ci.yml](../.github/workflows/ci.yml).
