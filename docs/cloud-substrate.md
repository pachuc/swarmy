# Cloud substrate

Everything that knows about a cloud provider lives in `swarmy-cloud`
behind one public interface, so a second provider is an implementation,
not a rewrite. The `swarmy remote` subcommand in the CLI is a thin
dispatch into this crate behind the existing `remote` cargo feature;
nothing else in the CLI imports a provider SDK. Without the feature the
crate still provides the command parser, the SSH helpers, and the
interface, but no provider. The fake implementation is compiled by the
feature-enabled provisioning tests.

## The interface

`Cloud` is the provider boundary for machines, SSH keys, images, buckets,
and the node role that guards a bucket. It speaks in provider-neutral types
and returns `None` for missing resources, so callers never match on provider
error codes. SSH provisioning is a separate interface, `Host`, described
below.

```rust
pub trait Cloud {
    // Buckets and the node credentials that guard them
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    fn verify_bucket_access(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    fn bucket_ownership(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<Ownership>>;
    fn role_ownership(&self, name: &str, owner: &str)
        -> impl Future<Output = Result<(Ownership, Ownership)>>;
    fn tag_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
    fn tag_node_role(&self, name: &str, owner: &str) -> impl Future<Output = Result<()>>;
    fn delete_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<BucketRemoval>>;
    fn delete_node_role(&self, name: &str, owner: &str)
        -> impl Future<Output = Result<(bool, bool)>>;
    // Machines, keys, and images
    fn base_image(&self) -> impl Future<Output = Result<String>>;
    fn import_ssh_key(&self, name: &str, public_key: Vec<u8>, owner: &str)
        -> impl Future<Output = Result<()>>;
    fn create(&self, spec: &MachineSpec) -> impl Future<Output = Result<String>>;
    fn get(&self, id: &str) -> impl Future<Output = Result<Option<Machine>>>;
    fn find_by_tag(&self, token: &str) -> impl Future<Output = Result<Option<String>>>;
    fn destroy(&self, id: &str) -> impl Future<Output = Result<()>>;
    fn delete_ssh_key(&self, name: &str) -> impl Future<Output = Result<()>>;
}
```

What each method is for:

- `ensure_bucket` creates the bucket and the node credentials guarding it,
  idempotently.
- `verify_bucket_access` checks the operator's bucket credentials with a cheap
  read-only call before any state is saved, so wrong static keys fail early.
- `bucket_ownership` reports whether the bucket's ownership tags name this
  remote (`Owned`), name nothing (`Unmanaged`), or the bucket is `Absent`.
- `role_ownership` reports the same separately for the instance profile and
  the role, so an unowned profile is never altered.
- `tag_bucket` and `tag_node_role` adopt an older remote's bucket, role, and
  profile after the operator confirms their exact names (`swarmy remote tag`).
- `delete_bucket` empties and deletes an owned bucket; a static-key bucket
  deletes only the remote's prefix and removes the bucket only when nothing
  else remains.
- `delete_node_role` deletes the instance profile and its role and reports
  which of the two existed.
- `base_image` resolves the stock machine image for the configured region.
- `import_ssh_key` imports a public key under a name and tags it with the
  owner.
- `create` launches a machine and returns its id; the launch token is the key
  name.
- `get` describes a machine, or returns `None` when it does not exist.
- `find_by_tag` finds a machine by its launch token, so a launch whose reply
  was lost can be recovered.
- `destroy` terminates a machine; missing or already terminated machines count
  as success.
- `delete_ssh_key` deletes a key; a missing key counts as success.

`MachineSpec` describes one machine to create. `name` is the human
name, `image` is the image reference, `key_name` names an SSH key
imported separately with `import_ssh_key`, and `managed_by` carries the
ownership tag value. `cpus`, `memory_mib`, and `disk_gb` are the
generic shape for providers that size machines directly.
`instance_type`, `subnet`, and `security_group` are the
provider-specific shape and placement; AWS sizes from `instance_type`
and ignores `cpus` and `memory_mib`. `ssh_public_key` carries the raw
public key for providers that install keys inline at creation; AWS
ignores it because keys are imported before launch. `profile` carries
the node credentials to attach, and `bootstrap` carries an optional
first-boot script; AWS leaves it unset because provisioning runs over
SSH after launch instead.

`Machine` is the provider-neutral view of one machine: `id`,
`public_ip`, `private_ip`, and the provider-reported lifecycle `state`
(`pending`, `running`, `terminated`, and similar). `ObjectBucket`
describes the object bucket backing a remote: `spec` is the one bucket
description (`swarmy_config::BucketSpec`: endpoint, region, bucket name,
prefix, and either the instance role or static keys), `owner` is the
owning remote, and `node_credentials` names the credentials attached to
nodes (the IAM instance profile on AWS, nothing for static-key buckets).
`remote down` deletes the bucket only when this remote owns it, unless
`--keep-bucket` is passed.

`Host` covers the SSH half of provisioning and stays
provider-independent: key generation and adoption, provisioning over SSH,
service installation, image builds, and host decommissioning. The crate
wires the standard SSH host
into `run`; tests replace it with a fake, as they replace `Cloud`
with a fake.

`for_settings` builds the substrate selected by
`RemoteSettings.provider`: `aws` machines, or the existing-host substrate
that reuses the bucket calls and fails every machine operation. One `Cloud`
implementation reads the provider enum where behavior must differ:
`for_settings` records it in the substrate (whose `machines` gate refuses
machine calls for existing hosts), `run` picks host decommissioning over
instance termination for teardown, and `add_node::resolve_existing`
validates join flags per provider. Status never matches on the provider
itself: instance-type presentation goes through `display_instance_type`.
Teardown reads
the provider from the node's saved launch
settings through `RemoteNode::cloud_settings`, falling back to
defaults in the node's region for records saved before launch settings
existed, so `down` never depends on a later configuration edit. `down`
decommissions existing hosts over SSH instead of terminating machines.

## What AWS implements

`Aws` implements `Cloud` with EC2, SSM, S3, and IAM, nearly as the old
inline code did:

- `ensure_bucket` stats the bucket with `GetBucketLocation` and
  creates it when it does not exist, then enforces AES-256
  server-side encryption and a full public-access block. It ensures
  the IAM role (the `node_credentials`, defaulting to
  `swarmy-{owner}`) with a policy scoped to that bucket plus the
  matching instance profile, and waits twenty seconds after creating
  either so the identity can propagate before launch.
- `verify_bucket_access` calls `HeadBucket` with the static keys (or the
  caller's identity for instance-role buckets). A missing bucket still
  passes: S3 answers an authenticated request with 404 and a bad signature
  with 403, and the bucket is created right after.
- `bucket_ownership` reads `GetBucketTagging` and maps a missing bucket to
  `Absent` and a missing tag set to `Unmanaged`. `role_ownership` does the
  same with `GetInstanceProfile` and `GetRole`.
- `tag_bucket` and `tag_node_role` refuse resources tagged for another remote,
  then write the `managed-by` and remote tags. `tag_node_role` checks both the
  role and the profile before changing either.
- `delete_bucket` requires `Owned`, deletes every object, then the bucket.
  `delete_node_role` refuses unmanaged resources, removes the role from the
  profile, deletes the profile, deletes the role's policies, and deletes the
  role.
- `base_image` resolves Canonical's current Ubuntu 24.04 amd64 image
  through the SSM public parameter.
- `import_ssh_key` calls `ImportKeyPair` and tags the key pair with
  `Name` and `managed-by`.
- `create` resolves the AMI root device with `DescribeImages`, then
  calls `RunInstances` with the instance type, key name, instance
  profile, client token, subnet, security group, public address, an
  encrypted gp3 root volume sized to `disk_gb`, and `Name` plus
  `managed-by` tags on the instance and the volume. The client token
  is the key name, so a lost response is recoverable with
  `find_by_tag`, and launches retry for ninety seconds while the
  identity system reports a not-yet-propagated instance profile.
- `get` calls `DescribeInstances` and maps a missing instance to
  `None`, keeping the EC2 state names in `state`.
- `find_by_tag` filters `DescribeInstances` on the client token.
- `destroy` treats missing and terminated machines as success (a
  tag-restricted identity cannot authorize mutations of missing
  resources) and otherwise calls `TerminateInstances`, treating a
  disappearing instance as success.
- `delete_ssh_key` treats missing keys as success around
  `DeleteKeyPair`.

Polling lives with the interface, not the provider: creation waits up
to 120 checks for `running` with both addresses assigned and rejects
any other state, while teardown waits the same way for `terminated`
or absent.

## Adopted hosts

`swarmy remote adopt` builds a remote on a machine that already exists, such
as a dedicated server (see `docs/REMOTE.md`, "Existing hosts"). It does not add
a second `Cloud` implementation. The node is saved with the existing-host
provider, and `for_settings` returns the same substrate with its machine
methods switched off: `base_image`, `import_ssh_key`, `create`, `get`,
`find_by_tag`, `destroy`, and `delete_ssh_key` fail with an error, while the
bucket and role methods still work. An existing host cannot use an AWS
instance-role bucket, because the host could never assume the role; adopt
refuses it and asks for static bucket keys.

Adoption checks the bucket keys with `verify_bucket_access`, saves the node
record before touching anything so `remote down` can clean up an interrupted
run, ensures the bucket, copies the operator's bootstrap key with
`Host::adopt_key`, and then runs the same provisioning and service
installation as `remote up`. `remote add-node --host` joins further existing
machines the same way. `remote down` calls `Host::decommission` over SSH
instead of `destroy`, then deletes the owned bucket scope.

## What a second provider must provide

No second machine provider is built yet. The target is a bare VM host with an
S3-compatible bucket, which must implement the same fifteen methods:

- Machine lifecycle: `create` from the generic shape (`cpus`,
  `memory_mib`, `disk_gb`), `image`, the SSH key, and the bootstrap
  script; `get` with equivalent `pending`, `running`, and
  `terminated` states and both addresses once running; `find_by_tag`
  for launch-token recovery; idempotent `destroy`.
- Keys: idempotent `import_ssh_key` and `delete_ssh_key` under the
  same name contract, with the same ownership tagging (or its
  equivalent where tags do not exist).
- Images: a `base_image` default plus the configured-image override,
  meeting the same guest contract as today (Ubuntu 24.04, the
  `ubuntu` SSH user, cloud-init, passwordless sudo, local NVMe on
  sandbox nodes).
- Buckets: `ensure_bucket` with server-side encryption and no public
  access, honoring `endpoint` for the S3-compatible store and
  attaching `node_credentials` to nodes in place of the IAM instance
  profile.
- Ownership: the `managed_by` value must scope every mutation the way
  the `managed-by` tag does on AWS, so two remotes can share one host
  account without touching each other's machines.
- Launch safety: creation must remain recoverable when the response
  is lost before the id is saved, and must wait out the host's own
  identity propagation the way AWS waits out IAM.

The provider-neutral names stay fixed; only the inside of each verb
and the meaning of the provider-specific `MachineSpec` fields change.
