# Remote development nodes

This is the operations guide for provisioning, upgrades, recovery and teardown.
For fleet-driver commands see [fleet-runbook.md](fleet-runbook.md).

Run `swarmy remote up NAME` from a swarmy checkout to launch one Ubuntu 24.04
EC2 node, copy the checkout, build the release binaries, and start FoundationDB,
NATS, SeaweedFS, and swarmyd under systemd. As its last step, `up` builds
`images/base-ubuntu` as root with `/etc/swarmy/node.env` and registers
`base-ubuntu:NAME` in the stack's store. Progress is streamed through SSH and
the image build duration is printed separately from the total provisioning time.
The default laptop-services mode runs scheduler, worker, and gateway on the
client, while `--services node` runs them under systemd on the control node.
The local machine needs `ssh`, `ssh-keygen`, and `rsync`. A client built with the `remote` cargo feature
provides the `remote` subcommands below; `make install-client` enables it,
while a plain `cargo build` leaves it out for the slimmer node binary and
such a binary rejects `remote` invocations with an error. AWS credentials use the SDK's standard credential chain.
The subnet must provide outbound internet access and the security group must
allow SSH from your machine and between group members. All backing services
bind to loopback. FoundationDB advertises `127.0.0.1:4500`, and every client,
including each joining node, reaches that endpoint through its own SSH tunnel.
Clients need no route to private service ports. No database ports need inbound
security group rules.

Add this to the discovered `.swarmy/config.toml` (or user configuration file):

```toml
[remote]
provider = "aws"
region = "us-east-1"
disk_gb = 100
managed_by_tag = "swarmy"
# bucket = "NAME"
# Login owning the checkout and the node units. Plain servers use the
# default `swarmy`; EC2 launches write `ubuntu` explicitly.
# service_user = "swarmy"
# Local disk for sandbox data: a block device to format and mount at
# /mnt/swarmy-local or dir:/path for an existing directory. Sandbox nodes
# require it; control-only nodes leave it empty for the root disk.
# local_storage = "/dev/nvme1n1"

[remote.aws]
subnet = "subnet-..."
security_group = "sg-..."
instance_type = "m6id.xlarge"
# image = "ami-..."
# iam_role = "custom-role"
```

`provider` selects the cloud substrate (`swarmy-cloud` implements only
`aws` today; see `docs/cloud-substrate.md` for the provider
interface). The EC2-only settings live under `[remote.aws]`:
placement, the instance type, the AMI override, and an optional IAM
role override. Without `iam_role`, bucket-backed remotes use
`swarmy-NAME` for the role and instance profile. The older flat
`[remote]` keys (`subnet`, `security_group`, `instance_type`,
`image`, `iam_role`) still parse and fill the sub-table when it
leaves them unset, so existing configuration files keep working; the
sub-table wins when both spellings are present. `region`, `disk_gb`,
`bucket`, ownership, and tunnel selection stay top-level because every
provider needs them.

The image defaults to Canonical's current Ubuntu 24.04 amd64 image, resolved
through SSM in the configured region. Custom images must be compatible with
Ubuntu 24.04 and grant the service user passwordless sudo; cloud-init is
waited on only where it is installed, so plain servers boot without it.
Provisioning creates the service user when it is missing, so a plain server
arriving with only a root login can be adopted. Records saved before the
setting existed have no `service_user` in `launch_settings` and keep the SSH
login they were provisioned with (`ubuntu` for the existing fleet).
Sandbox nodes need local storage for their volume caches and dirty data: set
`local_storage` to a block device to format and mount at `/mnt/swarmy-local`,
or to `dir:/path` for an existing directory when the server's disks are
already partitioned. EC2 launches resolve the instance-store device over SSH
by disk model before provisioning, since Nitro instances name NVMe disks by
attachment order; other servers fail with a message until the device or
directory is set. A control-only first node
(`--sandboxes 0`) needs no local storage; its volume server and scratch
directories remain on its root disk. The repository, backing databases, and
node identity stay on the root disk. The script refuses the root disk and
refuses to reuse unrecognized filesystems.

```sh
swarmy remote up demo --services node --instance-type m6i.large --disk-gb 40 --sandboxes 0
# up prints image build time, total elapsed time, and an SSH command
swarmy remote add-node demo --instance-type m6id.4xlarge --disk-gb 100
swarmy remote connect demo
# connect reports total, address probing, and tunnel startup seconds
swarmy doctor --remote demo
swarmy dev up --remote demo
swarmy chat --remote demo        # ask it to run pwd; no image flag needed
swarmy remote ls
swarmy dev down
swarmy remote disconnect demo
swarmy remote down demo
```

For a quick single-node setup, `swarmy remote up demo --services node`
still runs services and sandboxes together on an NVMe-backed instance.
Both `remote up` and `remote add-node` accept `--sandboxes N` (default 64); zero advertises only
the volume role and no disk capacity, so placement cannot select that node.
`remote ls` displays each saved node's sandbox count.

### Upgrade a running remote

From a clean local checkout, run `swarmy remote upgrade NAME`. It reports the
local CLI version and every node's installed `swarmyd` version before copying
anything. Joining nodes upgrade first; the first node, which owns the backing
services, upgrades last. The upgrade uses the provisioning rsync exclusions and
keeps instances, disks, node state, and `/etc/swarmy/node.env` unchanged. It
rebuilds the release binaries, installs changed binaries, and restarts installed service units whose running executables differ from the
installed binaries. A changed node daemon restarts last, after its managed
sandbox commands finish; restarting it evicts every placement on that node,
even though they are idle after the drain. `--drain-timeout SECONDS`
defaults to 600. `--services-only` never restarts `swarmyd` even if its binary
changed. `--allow-dirty` explicitly opts into deploying uncommitted source.
Use `--json` to emit one machine-readable summary per node. A timeout leaves
the upgraded binaries on disk but does not restart the busy node daemon; retry
the command after its commands finish. The retry compares each running unit's
executable with the installed binary, including previously installed upgrades. This command does not stop the backing
FoundationDB, NATS, or object store. On a node serving the API, the upgrade
also fills a missing `[api]` token in the node's `.swarmy/config.toml` without
rotating an existing one, and restarts the API when the fill changed it, so
nodes provisioned before token provisioning start accepting requests.
`remote ls` reports each remote's token state as `set`, `missing`, or
`not-applicable`.

Use `--image-recipe images/custom` on `remote up` to select a recipe directory
within the copied checkout. Relative paths are resolved from the checkout root;
absolute paths must also be inside the checkout. The recipe must contain
`recipe.toml` and cannot be in an excluded directory such as `.swarmy` or
`target`. It still registers as `base-ubuntu:NAME`. Use `--no-image` to skip the
build; this does not set a remote default. These options cannot be combined.
`add-node` reuses the stack's registry and does not rebuild the image.
Both commands accept `--instance-type` and `--disk-gb` overrides; absent flags
use `[remote]` defaults for the first node and its saved settings for joining
nodes. Each node's resolved settings remain in its state record. The example
uses a 40 GiB `m6i.large` control node with no sandboxes and a 100 GiB
`m6id.4xlarge` sandbox node with local NVMe. Without `--sandboxes 0`, a node
hosting sandboxes must have local NVMe.

After a successful build, the node record stores `default_image`.
`connect` copies it into `<state directory>/remote/<name>.profile.json` alongside
the service endpoints. Selecting that profile with `--remote NAME`,
`SWARMY_REMOTE`, or `[remote] profile` applies the stack's default to local
commands, overriding a local configuration or environment default.
An explicit session `--image NAME:TAG` still takes precedence. Older profiles
and remotes created with `--no-image` preserve any locally configured default;
otherwise a new session requires an explicit registered image.

The instance, root volume, and imported ed25519 key pair receive `Name` and
`managed-by` tags at creation. IAM needs EC2 `DescribeImages`,
`DescribeInstances`, `DescribeKeyPairs`, `RunInstances`, `ImportKeyPair`, `CreateTags`,
`TerminateInstances`, and `DeleteKeyPair`, plus SSM `GetParameter` when no image
is supplied. Tag-restricted identities should set `managed_by_tag` to their
permitted value. The root volume is encrypted and deleted on termination.

Node records are JSON at `<state directory>/remote/<name>.json`. The state
directory is the configuration file's parent, or `.swarmy` in the current
directory when no configuration exists. Records include region, instance id,
public/private IPs, SSH user and key path, ports, creation time, and joining
instances in `nodes`, including each node's sandbox count.
`launch_settings` retains the resolved AMI and launch
configuration, so later config edits do not change the subnet, security group,
instance type, disk size, region, or ownership tag used by `add-node`.
Older records without saved launch settings still support connect and down;
recreate those remotes before adding nodes. Provisioning, tunnels, logs, and status try the public address and then the private address, so launchers in the
same VPC can use security group membership rules. The final SSH command uses the
address that answered. Keys and records are private local files. SSH stores host keys next to the generated key.

An interrupted or failed `up` or `add-node` retains its state so `down` can clean up. A unique
client token identifies a launch if its response was lost before the instance id
was saved. Run `down` before retrying an interrupted `up` with the same name.
A completed bucket-backed remote accepts a repeat `up --bucket` without creating
more resources. `down` attempts instance termination and key-pair deletion
independently, and reports missing AWS permissions. It removes local state once
all instances are confirmed terminated or never launched, even if key-pair
cleanup is denied. If an instance could still exist, it retains state for retry.
The bucket, objects, IAM role, and instance profile remain for reuse.

On the node, `sudo systemctl status swarmy-stack swarmyd` shows the services,
and `sudo journalctl -u swarmyd -f` follows node logs. The provisioning script
writes `/etc/swarmy/node.env`, enables both units at boot, and configures
`Restart=always` for swarmyd. To rerun provisioning, use
`cd ~/swarmy && bash scripts/remote-provision.sh stack PRIVATE_IP BUCKET REGION SANDBOXES SERVICE_USER LOCAL_STORAGE` on the first
node (`SERVICE_USER` defaults to `swarmy`; sandbox nodes require a block
device or `dir:/path` as `LOCAL_STORAGE`). Joining nodes run swarmyd and `swarmy-tunnel.service`; rerun their
provisioning with `node FIRST_NODE_PRIVATE_IP BUCKET REGION SANDBOXES SERVICE_USER LOCAL_STORAGE` instead. Their cluster file is
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

The local NVMe mount persists across reboot. Instance stop/start can discard
instance-store data and is not supported as a preservation mechanism. `down`
permanently deletes the development node and its EBS backing data.

`add-node` adds compute capacity, not replicas of the backing services. Losing
the first node still loses this development stack. For a recovery exercise,
place a computer on a joining node and terminate that node while the first node
remains running. `status` lists every saved instance and the stack's live and
stale swarmyd registrations, plus the registered image names, tags, and manifest
ids. JSON output includes `images` and `image_error`. Image and registration
queries require a connected tunnel; disconnected or unavailable stores are
reported as unknown rather than as an empty registry. The laptop tunnel forwards to the first node's
loopback address and keeps local FoundationDB port 4500; stop any local dev stack
before connecting. Remotes created with private FoundationDB advertising must
be recreated with this version before using `add-node`.

Doctor's checks are described under [Tunnel and profile reference](#tunnel-and-profile-reference):
control master and port mapping, the control-plane API snapshot, and the S3
check.

Connect's JSON preserves the profile fields and adds `timing` with
`elapsed_seconds`, `address_probe_seconds`, `tunnel_startup_seconds`, and
`reused`. Total time includes local setup and profile publication. Startup
includes port allocation, SSH readiness, and fetching the cluster identity.
Reusing a healthy control master reports `reused: true` and zero for the two
skipped phases.

For a proof from a launcher in the same VPC, block direct access before running
the workflow as the ordinary user. Keep SSH port 22 allowed. Add port 8333
when using SeaweedFS instead of an S3 bucket. For example:

```sh
sudo iptables -I OUTPUT -m owner --uid-owner ubuntu -d FIRST_NODE_PRIVATE_IP \
  -p tcp -m multiport --dports 4500,4222 -j REJECT
# Run up/connect/doctor/run/chat/checkpoint/add-node/recovery/down as ubuntu.
sudo iptables -D OUTPUT -m owner --uid-owner ubuntu -d FIRST_NODE_PRIVATE_IP \
  -p tcp -m multiport --dports 4500,4222 -j REJECT
```

Record the block, failed direct probes, successful doctor checks, and
provider teardown queries with the run. Remove the rule even after a failure.

## Persistent object storage

Pass `--bucket NAME` to `swarmy remote up` (or set `[remote] bucket = "NAME"`).
`remote down` empties and deletes the bucket, including previous object versions,
only if both `managed-by=swarmy` and `swarmy-remote=NAME` tags match. It also
checks that no other local remote state file records the bucket. Untagged or
mismatched resources are left in place with a message; local state is still
removed. A remote created before ownership tags were introduced needs manual
migration: inspect the exact bucket, role, and instance profile in its local
state and run `swarmy remote tag NAME` interactively. Type each resource name
to authorize tagging; never adopt a resource belonging to another swarm. The
command will not run without a terminal. Newly created resources are tagged
by `remote up`; already-existing resources are not silently adopted.
Use `remote down --keep-bucket` to retain the bucket, role, and instance profile
as a fallback. The instance profile grants access only to that
bucket. Only the services on the nodes talk to S3, using the instance role;
the dev stack uses static keys from settings. No laptop command opens the
object store: every volume, image, GC, and doctor command runs through the
control-plane API. The laptop identity needs bucket permissions only for
`swarmy remote up` and `remote down`, which create and remove the bucket
through the provisioning SDK. `swarmy doctor --remote NAME` reads node and service heartbeats, image
metadata, and credentials through the API; it does not check the bucket. Image
builds and chunk operations use S3 through the node services.

The laptop identity needs the following permissions for bucket-backed remotes,
scoped to the `swarmy-*` resources it manages. The example covers S3 and IAM;
EC2, SSM, and host provisioning permissions are separate. Grant tagging before
`remote up` when possible. If tagging is denied during creation, `up` warns and
continues, but `down` will retain the untagged resource. After granting the
missing permission, run `swarmy remote tag NAME` interactively to adopt it.
An AccessDenied on an ownership read or version listing stops `down` or `tag`
with local state intact; grant the named permission and retry.

- `remote up`: create/configure the bucket and IAM role/profile, tag new resources,
  and pass the role to EC2.
- `remote tag`: read and write bucket tags, read role/profile tags, and tag both.
- `remote down`: read ownership and bucket versions, empty/delete the bucket,
  detach/delete the role's policies, and remove the profile and role.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket", "s3:GetBucketLocation", "s3:GetEncryptionConfiguration",
        "s3:PutEncryptionConfiguration", "s3:PutBucketPublicAccessBlock",
        "s3:GetBucketTagging", "s3:PutBucketTagging", "s3:GetBucketVersioning",
        "s3:ListBucket", "s3:ListBucketVersions", "s3:DeleteBucket"
      ],
      "Resource": "arn:aws:s3:::swarmy-*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
      "Resource": "arn:aws:s3:::swarmy-*/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole", "iam:GetRole", "iam:PutRolePolicy", "iam:TagRole",
        "iam:ListRoleTags", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
        "iam:DetachRolePolicy", "iam:DeleteRolePolicy", "iam:DeleteRole", "iam:PassRole"
      ],
      "Resource": "arn:aws:iam::<account-id>:role/swarmy-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateInstanceProfile", "iam:GetInstanceProfile",
        "iam:TagInstanceProfile", "iam:ListInstanceProfileTags",
        "iam:AddRoleToInstanceProfile", "iam:RemoveRoleFromInstanceProfile",
        "iam:DeleteInstanceProfile"
      ],
      "Resource": "arn:aws:iam::<account-id>:instance-profile/swarmy-*"
    }
  ]
}
```

Existing remotes without a bucket continue using SeaweedFS.

### S3-compatible buckets with static keys

A remote can store its volumes, images, and blobs in any S3-compatible
bucket (for example Hetzner Object Storage) reached by endpoint URL with a
static access key and secret key, instead of AWS S3 through an IAM instance
role. The saved bucket description is one value everywhere: endpoint,
region, bucket name, prefix, and a credential source that is either the
instance role or static keys.

```sh
swarmy remote up demo --bucket NAME --s3-endpoint https://objects.example.invalid \
  --s3-region eu-west-1 --s3-prefix runs/team \
  --s3-access-key KEY --s3-secret-file ~/.swarmy/demo-s3-secret
```

The secret key comes from `--s3-secret-file` (a file readable only by its
owner), from `--s3-secret-stdin` (one line on stdin), or from the standard
`AWS_SECRET_ACCESS_KEY` environment variable, with the access key from
`--s3-access-key` or `AWS_ACCESS_KEY_ID`. The keys are stored in the laptop's
remote state file (mode 0600) and copied to each node into
`/etc/swarmy/node.env` (mode 0600) over SSH stdin, the same way
`--copy-credential` uploads the ChatGPT credential. They never appear on a
command line, in a log, or in `remote status` output. The same description
can live in the configuration file instead of flags:

```toml
[remote.bucket]
endpoint = "https://objects.example.invalid"
region = "eu-west-1"
bucket = "NAME"
prefix = "runs/team"

[remote.bucket.credentials]
source = "static_keys"
access_key = "KEY"
secret_key = "SECRET"
```

For a static-key bucket, `remote up` creates the bucket through the S3 API
when it does not exist, or reuses an existing empty bucket. Ownership is
recorded with bucket tags where the provider supports them; bucket tags are
optional in the S3 API, so providers without tag support record ownership in
a marker object under the prefix (`<prefix>/.swarmy-owner`). `remote down`
deletes only what the swarm owns: its prefix scope, plus the bucket itself
when nothing else remains. No IAM, public-access-block, or encryption calls
are made for non-AWS endpoints. `remote tag` adopts only the bucket.

Volume chunks and manifests are content-addressed and written with a
create-only PUT (`If-None-Match: *`); overwriting identical bytes is safe.
Providers that reject that header need the plain-PUT fallback, which is part
of the bucket description so it reaches the nodes:

```toml
[remote.bucket]
conditional_create = false
```

The description is carried into `/etc/swarmy/node.env` at provisioning and
into the connect profile, which is what the node services read. `[s3]
conditional_create` (or `SWARMY_S3_CONDITIONAL_CREATE=false`) remains the
service-level setting for local development stacks. The fallback still
dedupes through the pre-write existence check and reads still verify the
content hash.

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
  --bucket NAME --access-key KEY --secret-file ~/.swarmy/demo-s3-secret
```

Coordinates fall back to `SWARMY_S3_ENDPOINT`, `SWARMY_S3_REGION`,
`SWARMY_S3_BUCKET`, `SWARMY_S3_ACCESS_KEY`, and `SWARMY_S3_SECRET_KEY`, so
with a sourced `.dev/env` the command takes no flags. The secret key comes
from `--secret-file` (readable only by its owner), `--secret-stdin` (one
line), or the environment, in the same precedence `remote up` accepts. When
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
--keep-bucket` followed by `remote up` with the new keys. The kept bucket is
still owned by the remote, so `up` reuses it instead of creating a new one.

## Costs and recovery

Check current EC2 prices for the control and sandbox shapes, plus EBS,
object storage requests and inference. Use `--sandboxes 0` for a control-only
node; sandbox nodes need local NVMe. Check `df -h /` for the store and
`df -h /mnt/swarmy-local` for scratch. The collector and snapshot retention
settings are described in [gc-benchmarks.md](gc-benchmarks.md) and
[volume-benchmarks.md](volume-benchmarks.md).

A joining node failure does not replicate the backing store: replace the
sandbox node with `swarmy remote add-node NAME` after checking its record with
`swarmy remote ls`. If the first node fails, its development stack and
backing data are lost unless stored elsewhere; `remote down` cleans up the
saved deployment, and `remote up` creates a new one. Pushed branches survive.
If the client disconnects, `swarmy remote connect NAME` restores its profile
without stopping workers. Run `swarmy remote down NAME` only after collecting
open work; it terminates instances and removes the managed key pair, while
bucket resources are deleted unless `--keep-bucket` is specified.

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
swarmy dev down                 # stop any local stack before reserving port 4500
swarmy remote up demo           # builds binaries and base-ubuntu:demo; prints build times and SSH command
swarmy remote connect demo
swarmy auth login              # dedicated ChatGPT login, on the laptop
swarmy dev up --remote demo
swarmy doctor --remote demo
swarmy remote ls
```

FoundationDB must bind local `127.0.0.1:4500` exactly. A local stack using that
port prevents a usable remote connection. Stop a conflicting local stack, disconnect, and reconnect.
NATS and S3 alone can use automatically selected alternative local ports.

`up` finishes by building `images/base-ubuntu` as root on the node using
`/etc/swarmy/node.env`. It streams the build progress, reports its duration,
and registers `base-ubuntu:demo`. `connect` copies that image into the remote
profile's `default_image`, so new sessions need no image flag or local image
configuration. Image construction needs node root; it never needs laptop root.
`swarmy remote ls` lists registered images while the remote is connected.

Start a chat on the laptop and ask the agent to run `pwd` in its sandbox:

```bash
swarmy chat --remote demo
# Alternatively, start a session and resume its printed id:
swarmy run --remote demo 'Run pwd in the sandbox, then wait for my next instruction.'
swarmy chat --remote demo SESSION_ID
```

Use `swarmy remote up demo --image-recipe images/custom` to build another recipe
directory within the checkout. Relative paths are resolved from the checkout
root; absolute paths must also be inside that checkout. The registered name
remains `base-ubuntu:demo`. `--no-image` skips the build and leaves the remote
without a saved default; provide an already registered image through `--image`
or `default_image` before starting a new session. The two options cannot be
combined. An explicit session `--image NAME:TAG` overrides the profile default.

In the session, ask the agent to use `process_start` to run
`python3 -u -m http.server 18765 --bind 127.0.0.1`. In the next turn ask it to
fetch `http://127.0.0.1:18765/` with `bash` and verify the server is still listed
by `process_list`. Then ask it to write a marker file and call `checkpoint`.
An acknowledged checkpoint makes the disk durable; it does not save running
processes. Escape or Ctrl-C closes chat while the session remains stored.

In the older file-backed laptop-services mode, a local gateway read the
configured credential file; current credentials are encrypted in the store. Remote provisioning
copies the checkout while excluding `.swarmy/`, `.dev/`, `.git/`, `target/`,
`.env`, and `.env.*`; it does not copy the laptop's home or credential cache.
The configured credential path is also excluded from the checkout copy.
Do not share its refresh writer with a running Codex login. Node services have
the explicit credential-transfer option described below. Prompts, outputs, and session
history do live in the remote backing services. The fake-provider acceptance
run requires no ChatGPT credential; see the dated benchmark evidence.


### Ephemeral sessions and named agents


Without `--agent`, each new conversation gets its own computer and writable disk:

```bash
swarmy chat --remote demo
# In another terminal, create a separate ephemeral conversation:
swarmy chat --remote demo
swarmy session ls --remote demo
swarmy chat --remote demo SESSION_ID   # resume an existing conversation
swarmy session close SESSION_ID --remote demo
swarmy session show SESSION_ID --remote demo --json
```

Escape, Ctrl-C, or EOF in `chat --json` closes the client only. The session
and computer remain available for resume. `session close` completes an ephemeral
session and deletes its computer while keeping its transcript. The scheduler
also closes Idle ephemeral sessions after 24 hours by default. For a disposable
retention test, set the interval on the scheduler before starting services:

```bash
swarmy dev down
SWARMY_EPHEMERAL_RETENTION_SECONDS=90 swarmy dev up --remote demo
```

This override applies to local services in the default laptop mode. Node services
need the override in their systemd environment and a scheduler restart. The sweep
runs every 60 seconds or every retention interval if shorter. Only idle sessions
strictly older than the cutoff qualify; active sessions and named agents do not.

Create a named agent to keep a computer independently of any one conversation:

```bash
swarmy agent create tommy --remote demo --description 'Shared development computer'
swarmy chat --agent tommy --remote demo
# In a second terminal, open another session on the same computer:
swarmy chat --agent tommy --new --remote demo
swarmy agent ls --remote demo
swarmy agent show tommy --remote demo --json
swarmy run --agent tommy --remote demo 'Read the files created in the other chat'
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
swarmy agent delete tommy --remote demo            # interactive confirmation
# For scripts, use --yes on the same command.
swarmy session show FIRST_SESSION_ID --remote demo --json
swarmy session show SECOND_SESSION_ID --remote demo --json
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
swarmy gc --remote demo --dry-run --json
swarmy gc --remote demo --json
swarmy gc --remote demo --dry-run --json
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
swarmy dev logs worker          # local routing and placement diagnostics
```

`add-node` launches another `swarmyd` using the saved launch settings and the
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


### Costs, inspection, and teardown


EC2 time, the 100 GiB gp3 root disk on each node, public IPv4 addresses, and
applicable network transfer cost money. Instance storage is part of the instance
allocation. Every `add-node` adds another instance and disk. Real inference also
uses the operator's provider account. Without `--bucket`, the development stack uses SeaweedFS on the first node.
With `--bucket`, the instance role accesses S3 objects until teardown. Disconnecting, closing
chat, or stopping local services leaves cloud resources running and billable.

`swarmy dev status` shows local processes. `swarmy remote ls` shows saved
instances, SSH reachability, and node heartbeat ages; it does **not** query EC2
power state or billing. Use the AWS console or provider queries for that.
Save the instance ids and key names before teardown, since `down` removes the
local record. With the optional AWS CLI installed as your user:

```bash
aws ec2 describe-instances --region us-east-1 \
  --filters Name=tag:Name,Values=demo,demo-2 \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name,Key:KeyName}'
swarmy dev down
swarmy remote disconnect demo
swarmy remote down demo --yes
# Replace the following values with every id/key saved above.
aws ec2 describe-instances --region us-east-1 --instance-ids i-<first-id> i-<second-id> \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name}'
aws ec2 describe-volumes --region us-east-1 \
  --filters Name=tag:Name,Values=demo,demo-2 --query 'Volumes[].VolumeId'
aws ec2 describe-key-pairs --region us-east-1 \
  --filters Name=key-name,Values=swarmy-FIRST,swarmy-SECOND --query 'KeyPairs[].KeyName'
```

Expect `terminated` for all recorded instances (or eventual absence), and empty
volume and key-pair lists. `down` waits for termination, deletes imported keys,
empties and deletes its bucket, removes the instance profile and role,
and removes local state. Use `--keep-bucket` to leave the bucket and its
guarding role and instance profile intact. Without that flag, `down` asks for
confirmation naming every owned resource it will delete on a terminal; `--json`
requires `--yes` when there are owned resources to remove. `--keep-bucket`
terminates the instances without a prompt. It permanently deletes this stack's backing data;
checkpoints here do not survive teardown of the first node. Failed provisioning
retains its state for `remote down`; failed cleanup retains state for retry.
Never delete that state to work around an error while resources still exist.
See the state and service contract above.


### Tunnel and profile reference


Only one remote tunnel can be active on the laptop: FoundationDB requires
its local port 4500 and rejects a remapped coordinator. Disconnect the old
remote before connecting another; the remote workers continue running while
the laptop is disconnected.

`swarmy remote connect NAME` reads `<state_dir>/remote/NAME.json`, starts an
SSH control master, and writes `NAME.profile.json` beside it. `state_dir`
defaults to the discovered project's `.swarmy` directory; `SWARMY_STATE_DIR`
can select another directory. Provisioning and tunnel commands share the
`swarmy_config::RemoteNode` JSON contract. For an existing host, a state file
can be written directly:

```json
{
  "name": "test",
  "region": "us-east-1",
  "instance_id": "i-<instance-id>",
  "public_ip": "<public-address>",
  "private_ip": "<private-address>",
  "key_path": "<path-to-test-key>",
  "ssh_user": "ubuntu",
  "ports": { "fdb": 4500, "nats": 4222, "s3": 8333 },
  "nodes": [],
  "created_at": "2026-09-16T00:00:00Z"
}
```

A plain Ubuntu 24.04 server (no cloud-init, root login, already partitioned
disks) is adopted the same way: write the state with the bootstrap login as
`ssh_user` and the desired owner and disk in `launch_settings`, copy the
checkout to the service home, and run the provisioning script on the server
as root, for example `bash /home/swarmy/swarmy/scripts/remote-provision.sh
stack <private-address> "" <region> 64 swarmy dir:/srv/swarmy-local`. The
script creates the service user with passwordless sudo, waits for cloud-init
only where it is installed, and uses the configured device or directory.
`remote down` terminates cloud instances, so it does not apply to an adopted
server: decommission the server itself, then remove its state file.

```json
{
  "name": "plain",
  "region": "us-east-1",
  "instance_id": "plain",
  "public_ip": "<public-address>",
  "private_ip": "<private-address>",
  "key_path": "<path-to-test-key>",
  "ssh_user": "root",
  "ports": { "fdb": 4500, "nats": 4222, "s3": 8333 },
  "nodes": [],
  "launch_settings": {
    "provider": "aws",
    "services": "laptop",
    "region": "us-east-1",
    "disk_gb": 100,
    "managed_by_tag": "swarmy",
    "service_user": "swarmy",
    "local_storage": "dir:/srv/swarmy-local"
  },
  "created_at": "2026-09-16T00:00:00Z"
}
```

```sh
swarmy remote connect test
swarmy dev up --remote test
swarmy doctor --remote test
swarmy run --remote test 'what time is it'
swarmy chat --remote test
swarmy remote ls
swarmy remote logs test
swarmy dev down
swarmy remote disconnect test
```

Selection also works with `export SWARMY_REMOTE=test` or `profile = "test"`
in the config's `[remote]` table. The flag takes precedence over the variable,
which takes precedence over the config setting. The selected profile overrides
the FoundationDB cluster file, NATS URL, and S3 endpoint after other environment
overrides. Credentials, bucket, object prefix, and store directory retain their
normal configuration. Scheduler, worker, and gateway binaries honor the same
variable and config setting without additional flags.

Remote `dev up` starts scheduler, gateway, and worker locally only for a
remote created with laptop services. A remote created with node services starts
none locally, including when local service binaries are not installed. It records that
choice so `dev down` leaves the backing services running, even without a remote
flag. Disconnect stops only the SSH control master and removes its profile and
cluster file; instance state and logs remain. Reconnecting a healthy tunnel is
idempotent. SSH uses the configured identity, batch authentication, normal host
key verification with first-use acceptance, and keepalives. SSH diagnostics are
saved in `NAME.ssh.log`. `remote logs` follows the `swarmyd` systemd journal until
interrupted; the SSH user needs permission to read that journal.

Local ports prefer 4500, 4222, and 8333, with free ephemeral ports chosen on
collision. All forwards bind only to `127.0.0.1`. The profile records the actual
ports, PID, and control socket. Port selection and SSH binding cannot be atomic;
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
tunnel for all three services; see the tunnel reference above.

Connect prints total elapsed time, address probing time, and tunnel startup
time. JSON adds these seconds under `timing`, alongside `reused`; an existing
healthy tunnel reports zero for the skipped probe and startup phases.

Status reports saved instance IDs and SSH reachability. An unreachable host has
unknown instance state; this command does not query a cloud API. For each
connected stack it scans all registered swarmyd nodes, including stale records,
and shows heartbeat age (live means at most 30 seconds old). Registrations belong
to the stack; they are not attributed to an instance because node records contain
no instance address. Store failures and disconnected tunnels report unknown
registration rather than claiming the node is absent. `--json` emits the same
information for scripts.

### Maintainer tunnel acceptance tests

These localhost tests are separate from the no-root EC2 workflow above.
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

That run uses an isolated project, starts the three local services, runs the
fake provider end to end, checks doctor and a live swarmyd registration, and
verifies that backing process IDs remain unchanged. The script removes its
services and tunnel on exit. Without `SWARMY_REMOTE_TEST_KEY` it skips cleanly.
Remove the temporary authorized key and network namespace after testing.


### Running control-plane services on the node


Laptop services are the default. They keep ChatGPT credentials local and make
worker/gateway development convenient, but every store transaction crosses the
tunnel. For lower turn latency, run the control plane under systemd beside the
store:

```sh
# With provider = "fake", no credential is copied.
swarmy remote up demo --services node
swarmy remote connect demo
swarmy dev up --remote demo
swarmy chat --remote demo
```

Alternatively set `services = "node"` in `[remote]` before `remote up`.
`--services laptop` overrides that setting. The selected mode is saved in the
remote's launch settings; later edits to local config do not switch an existing
node. Node mode uses the configured provider, model, reasoning effort, system
prompt, store/bus namespace, and fake script. The fake script is sent over SSH;
if no script exists, the default greeting script is installed.

For a ChatGPT gateway, use `swarmy remote up demo --services node
--copy-credential`. This explicit flag acknowledges that the configured
`credential_file` leaves the laptop. A warning precedes transfer. SSH sends the
contents on stdin, and `/etc/swarmy/auth.json` is owned by ubuntu with mode 0600.
The ordinary checkout copy excludes the configured credential path as well as
`.swarmy`, `.dev`, and environment files. Without the flag a ChatGPT node launch
fails before creating resources. Do not run a laptop gateway that refreshes the
same account concurrently. Stopping the services does not erase the copied file;
`remote down` terminates the node and its root disk.

`dev up --remote demo` stops any recorded local control-plane processes, checks
the tunnel, and starts nothing locally in node mode. The services continue when
the laptop disconnects. `dev down` stops only local processes; use SSH and
`sudo systemctl stop swarmy-{scheduler,worker,gateway}` to pause node services.
Inspect their logs with `sudo journalctl -u swarmy-worker -u swarmy-gateway
-u swarmy-scheduler`. `remote logs` continues to follow the execution node log.

Both configurations use the same port-4500 tunnel requirement, tool placement,
rebuild notices, and teardown commands:

```sh
swarmy dev down
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



| Piece | Value | Why |
|---|---|---|
| Control node | minimum `m6i.large` (8 GiB RAM), 40 GiB root disk, `--sandboxes 0` | release build needs at least 6 GiB available; node CLI omits EC2 provisioning SDKs |
| Sandbox node | `m6id.4xlarge`, 100 GiB root disk, local NVMe | worker builds use instance-store scratch |
| Sandboxes per sandbox node | 4 | one worker per lane |
| Control plane | on the node (`--services node`) | the laptop can disconnect |
| Image | `images/swarmy-dev` registered as `base-ubuntu:dev`, the node's default | toolchain, dev stack, fetched crates; no warm build |
| Snapshots and collection | retention 3, collector grace 30 min, interval 10 min (set by provisioning) | build caches churn; ten snapshots and six hours of grace filled a 100 GB root disk in an hour |
| Scratch | local NVMe at `/mnt/swarmy-local/scratch`; `/home/agent/.cargo-target` and `/tmp` in each worker | build outputs stay warm on the same node without entering snapshots or S3 |
| Cost | check current EC2 prices for both shapes, plus S3 and inference | separate control and sandbox nodes |

The node build uses the default features, which omit provisioning commands
and the EC2, S3, IAM, and SSM clients; those belong on the laptop, where
`make install` builds the client with the `remote` feature. A 6 GiB fleet
sandbox measured 957,428 KiB peak resident memory for the release build without
the `remote` feature. The build with it reached 6,123,248 KiB in the EC2
compiler and was killed by its memory limit. The node checks for 6 GiB
`MemAvailable` before building; use at least an 8 GiB instance so the OS and
backing services have room too. Inference's Bedrock SDK remains part of the
session binary; the node build omits the provisioning clients, not inference.

Observed with three workers building at once: 3 GiB used, load 4. The root
disk, which holds the node's object store, filled to 96 percent within an
hour because every build cache chunk was uploaded and kept; scratch mounts
(a dev-fleet task) move caches off the durable volume, and the retention and
collection settings above bound what remains. Watch `df -h /` on the node
until scratch lands.

### Fleet bring-up

1. Local config: `.swarmy/config.toml` has `[remote]` with region, subnet,
   security group, and `instance_type = "m6id.2xlarge"`. The standard AWS credential chain
   holds an identity with the EC2 permissions in this guide.
   Run `swarmy auth login` for a permitted login; keep the local keyring private.
2. Build and install the CLI from the commit you want the swarm to run:
   `make install`. The version guard refuses a mismatch later.
3. Launch the control node: `swarmy remote up dev --services node
   --sandboxes 0 --instance-type m6i.large --disk-gb 40 --copy-credential
   --image-recipe images/swarmy-dev --bucket <swarm-bucket>`. Then add each
   sandbox node: `swarmy remote add-node dev --instance-type m6id.4xlarge
   --disk-gb 100 --sandboxes 4`. The first node runs backing and control
   services, swarmyd (for image registration), and no sandboxes; the joining
   nodes host the sandboxes on local NVMe. For a quick single-node setup on
   an NVMe-backed type, omit `--sandboxes 0` and the add-node commands.
4. Connect: `swarmy remote connect dev`. FoundationDB and NATS use tunnels;
   Only provisioning uses the laptop AWS identity; node services use the instance role for S3.
5. Credentials go into the swarm's encrypted store, not into files:
   `swarmy auth import --remote dev` for the ChatGPT login,
   `swarmy auth set openrouter --file <key-file> --remote dev` for OpenRouter.
   The gateway watches the credential store; no manual restart is needed.
6. Check: `swarmy doctor --remote dev` shows each provider as
   `gateway=served`; `swarmy remote ls` shows the node heartbeat and the
   image. A live turn: `swarmy --remote dev run --provider openrouter --model
   openai/gpt-6-sol "reply with the word ready"`.
7. Fleet config: copy `scripts/fleet/fleet.example.toml` to
   `scripts/fleet/fleet.toml`, set the GitHub token and pool size, `chmod
   600`. The file is ignored by git.


### Disk and upgrades



Build output lives in scratch on the NVMe; only clones and home files reach
the durable volume. Three workers running full test builds hold about 110 GB
of scratch. The collector deletes unreferenced chunks ten minutes after
their thirty-minute grace, but SeaweedFS returns the space only when it
compacts a volume file, on its own schedule and threshold. To get space back
now, ask its master to compact anything more than ten percent garbage:

```sh
curl -s "127.0.0.1:9333/vol/vacuum?garbageThreshold=0.1" >/dev/null
```

Watch `df -h /` for the root disk (the store), `df -h /mnt/swarmy-local` for
scratch, and `curl -s 127.0.0.1:9333/vol/status` for deleted bytes awaiting
compaction. The node daemon evicts the least recently hosted scratch when
the NVMe passes 80 percent.

Snapshots of worker disks run every 30 minutes on provisioned nodes
(`SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS=1800` in `node.env`). Each snapshot
uploads only changed chunks, and on a real bucket every chunk is one PUT
request, so the period is the main lever on request cost; retention of
three snapshots plus the 30-minute collection grace keeps live data bounded
to the disks' contents plus recent churn.

### Adding a node

`swarmy remote add-node dev` joins a second node to the same backing
services and doubles the lanes; raise `workers` in `fleet.toml`. Nodes do
not replicate the backing services: the first node holds the store.

### Updating the swarm in place

Commit and install the checkout you want to deploy, then run
`swarmy remote upgrade dev`. The command prints the local and node versions,
updates joining nodes before the first node, and copies the checkout with the
same credential exclusions as `remote up`. It rebuilds changed binaries and
restarts only the affected services. A changed `swarmyd` waits for managed
sandbox commands to finish before restarting; this evicts every placement on
that node (all idle after the drain).
Use `--drain-timeout 1200` for long builds, or `--services-only` when node
sandboxes must stay untouched. A dirty local checkout requires the explicit
`--allow-dirty` acknowledgement. `--json` prints one summary per node.
Upgrade all gateway nodes together. Stored credential entries use labelled
records; there is no migration for older provider records.


### Fleet recovery



- Node died or was terminated: `swarmy remote up dev` again and repeat
  bring-up steps 4 to 7. Everything on the node's disk is gone, including
  the store, so workers and their caches are recreated on first launch.
  Sessions are one pull request each, so nothing precious is lost; pushed
  branches survive on GitHub.
- A worker's disk in a bad state: `scripts/fleet/fleet reset worker-N`
  deletes it; the next launch recreates it from the image.
- The laptop closed: nothing stops. Reconnect with `swarmy remote connect
  dev` and `fleet status`.
- Tear down: `swarmy remote down dev --yes` terminates the instance and deletes
  its key pair. Collect and merge open pull requests first.


## Root suites and the nightly timer

Root-only NBD and sandbox suites are a manual operator responsibility under
[AGENTS.md](../AGENTS.md#building-and-testing). The former nightly node-suite
timer was removed; do not expect a scheduled root run. Use the scripts in
[`scripts/node-suites/`](../scripts/node-suites/) on a suitable node and record
results in the pull request. CI's reduced chaos checks are defined in
[ci.yml](../.github/workflows/ci.yml).
