# Cloud substrate

Everything that knows about a cloud provider lives in `swarmy-cloud`
behind one public interface, so a second provider is an implementation,
not a rewrite. The `swarmy remote` subcommand in the CLI is a thin
dispatch into this crate behind the existing `remote` cargo feature;
nothing else in the CLI imports a provider SDK. Without the feature the
crate still provides the command parser, the SSH helpers, and the
interface, but no provider.

## The interface

`Cloud` is the whole boundary. It speaks in provider-neutral types and
returns `None` for missing resources, so callers never match on
provider error codes.

```rust
pub trait Cloud {
    fn ensure_bucket(&self, bucket: &ObjectBucket) -> impl Future<Output = Result<()>>;
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
describes the object bucket backing a remote: `name`, `region`, and
the owning remote in `owner`, plus an `endpoint` override for
S3-compatible providers and the `node_credentials` to attach to nodes.
The bucket survives `remote down`; the credentials guard it.

`Host` covers the SSH half of provisioning and stays
provider-independent: key generation, provisioning over SSH, service
installation, and image builds. The crate wires the standard SSH host
into `run`; tests replace it with a fake, as they replace `Cloud`
with a fake.

`for_settings` builds the provider selected by
`RemoteSettings.provider` (only `aws` today) and rejects anything
else. Teardown reads the provider from the node's saved launch
settings through `RemoteNode::cloud_settings`, falling back to
defaults in the node's region for records saved before launch settings
existed, so `down` never depends on a later configuration edit.

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

## What a second provider must provide

No second provider is built yet. The target is a bare VM host with an
S3-compatible bucket, which must implement the same eight verbs:

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
