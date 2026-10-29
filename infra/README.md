# Machine-group infrastructure modules

Each directory under `infra/modules` is a complete OpenTofu root configuration for
one machine group. The client carries these files compiled in, and will write
them into a separate working directory per group. The module owns its state;
credentials are supplied only through the environment, never through inputs.
The Hetzner Cloud module uses `HCLOUD_TOKEN` for machines and `MINIO_USER` and
`MINIO_PASSWORD` for object storage. Scaleway uses `SCW_ACCESS_KEY`, `SCW_SECRET_KEY` and `SCW_DEFAULT_PROJECT_ID`. Do not put credentials in tfvars or state.

## Contract inputs

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `swarm` | string | required | Swarm name (1-20 lowercase letters, digits, hyphens; start with a letter, no trailing hyphen) |
| `group` | string | required | Group name, same naming rule |
| `machine_count` | number | required | Whole number of machines, 0-50 |
| `size` | string | required | Provider machine size |
| `location` | string | required | Hetzner: fsn1, nbg1, hel1, ash, hil, sin; Scaleway: a zone such as fr-par-2 |
| `image` | string | `ubuntu-24.04` | OS image; provisioning expects Ubuntu |
| `ssh_public_key` | string | required | Unique OpenSSH public key per group |
| `labels` | map(string) | `{}` | Extra labels; swarmy labels win |
| `ssh_allowed_cidrs` | list(string) | public IPv4 and IPv6 | Allowed sources for SSH |
| `bucket_name` | string | empty | Optional bucket name |
| `bucket_location` | string | empty | Bucket region; empty uses group location |
| `bucket_force_destroy` | bool | `false` | Delete bucket data on destroy when requested |
| `network_id` | string | empty | Join an existing swarm network; empty creates one |

## Outputs

There are exactly three root outputs: `machines`, `bucket`, and `network_id`.
Machines are ordered by number and each object has exactly name, public_ipv4,
public_ipv6, private_ip, and ssh_user. Missing addresses are JSON null, never
empty strings. With zero machines the list is empty. Bucket is null when no
bucket was requested. For hetzner-cloud, network ID is a decimal string for the created or joined
network; for aws it is a subnet id. For example the values could be:

```json
{
  "machines": [
    {"name": "fleet-workers-1", "public_ipv4": "203.0.113.10", "public_ipv6": "2001:db8::1", "private_ip": "10.0.0.2", "ssh_user": "root"}
  ],
  "bucket": {"endpoint": "https://fsn1.your-objectstorage.com", "name": "fleet-data-7f3a", "region": "fsn1"},
  "network_id": "1001"
}
```

Plain `tofu output -json` wraps each output separately: its `machines`,
`bucket`, and `network_id` entries each have `sensitive`, `type`, and `value`
keys. Swarmy reads `value` from the `machines` and `bucket` entries.
For example:

```json
{
  "machines": {"sensitive": false, "type": ["tuple", [["object", {"name": "string", "private_ip": "string", "public_ipv4": "string", "public_ipv6": "string", "ssh_user": "string"}]]], "value": [{"name": "fleet-workers-1", "public_ipv4": "203.0.113.10", "public_ipv6": "2001:db8::1", "private_ip": "10.0.0.2", "ssh_user": "root"}]},
  "bucket": {"sensitive": false, "type": ["object", {"endpoint": "string", "name": "string", "region": "string"}], "value": {"endpoint": "https://fsn1.your-objectstorage.com", "name": "fleet-data-7f3a", "region": "fsn1"}},
  "network_id": {"sensitive": false, "type": "string", "value": "1001"}
}
```

Only the stack group should create the swarm bucket. Set
`bucket_force_destroy` before a destroy that must delete bucket data. Apply
the stack group first, pass its `network_id` to later groups, and destroy
those groups before the stack group. Hetzner rejects duplicate SSH key
fingerprints, so generate one key pair per group. Lowering `machine_count`
removes highest-numbered machines first; drain them before applying the change.

Install the pinned OpenTofu with `scripts/install-opentofu.sh`, then run
`make check-infra` for formatting, readonly provider-lock initialization,
validation and offline mock-provider tests. A new module needs its own
root configuration files (`versions.tf`, `variables.tf`, `main.tf`,
`outputs.tf`, `.terraform.lock.hcl`) and mock tests. It must implement the
same input and output names; the script guards that contract. aws, hetzner-cloud and scaleway implement the same contract.

## aws

The `aws` module implements the same thirteen inputs with these meanings:

| Input | AWS meaning |
|---|---|
| `swarm`, `group`, `machine_count` | Names and number of EC2 machines |
| `size` | EC2 instance type, such as m6i.large |
| `location` | AWS region for the machines and network |
| `image` | Current Canonical Ubuntu 24.04 SSM image or explicit AMI id |
| `ssh_public_key` | The group's EC2 key pair (RSA or ed25519) |
| `labels` | Resource tags; swarmy's three ownership tags win |
| `ssh_allowed_cidrs` | SSH ingress from these IPv4 and IPv6 CIDRs |
| `bucket_name`, `bucket_location`, `bucket_force_destroy` | Optional S3 bucket name, region and deletion policy |
| `network_id` | Existing subnet id to join, or empty to create a VPC and public subnet |

Supply `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally
`AWS_SESSION_TOKEN` in the runner environment only. The region always comes
from `location`, never the environment. Here `network_id` is an existing
subnet id, whereas for hetzner-cloud it is a decimal network id. Swarmy
treats it as opaque and passes it from the first group to later groups of the same module.
An empty value creates a VPC and public subnet.

Machines have `ssh_user` ubuntu, no public IPv6, and a fixed encrypted
100 GiB root disk. Image changes are ignored after launch to avoid replacing
running machines when Canonical updates its SSM parameter. An AWS key needs
permissions for EC2 instances, key pairs, VPC, subnet, route table, internet
gateway and security groups, including `ec2:DescribeInstanceTypes`;
`ssm:GetParameter` for Canonical's public parameters; and S3 bucket
creation, tagging, `s3:PutBucketPublicAccessBlock`, and deletion.
This module has only been checked by `tofu validate` and offline mock tests.
It has not been run against real AWS.

## scaleway

The `scaleway` module implements the thirteen inputs with these meanings:

| Input | Scaleway meaning |
|---|---|
| `swarm`, `group`, `machine_count` | Names and number of instances (0-50) |
| `size` | Instance commercial type, such as L4-1-24G |
| `location` | Instance zone, such as fr-par-2 |
| `image` | Ubuntu image label `ubuntu_noble` or image UUID |
| `ssh_public_key` | Group root login key installed by cloud-init, not a project-wide IAM SSH key |
| `labels` | Instance tags; swarmy's ownership tags win |
| `ssh_allowed_cidrs` | SSH ingress from these CIDRs |
| `bucket_name`, `bucket_location`, `bucket_force_destroy` | No bucket: the name and location must be empty; force-destroy is unused |
| `network_id` | Must be empty: this module creates no private network |

The machines output uses `ssh_user` root and public IPv4 only; `private_ip`,
`bucket` and `network_id` are always null. Each instance has a fixed 200 GB
`sbs_volume` root disk, deleted on termination. Image and user_data changes
are ignored after launch. Use an API key for a dedicated project, with
the InstancesFullAccess and BlockStorageFullAccess permission sets on that
project only. The root disk is a Block Storage (SBS) volume, and OpenTofu
reads and deletes it through the Block Storage API. This module has only
been checked by tofu validate and offline mock tests. It has not yet been
run against real Scaleway.

## How swarmy runs these modules

Swarmy keeps one OpenTofu working directory per machine group beneath the
swarm's record directory. It writes the swarm and group names, machine count,
optional size and location, one group SSH public key, SSH CIDRs, bucket name,
force-destroy flag, and network ID to the variables file. Each group has its
own private SSH key pair. The stack group is applied first, followed by the
services group when distinct and the node groups in placement order. A
`network_id` output is passed only to later groups using the same module;
custom path modules may omit it. Custom module paths must be absolute.

Only a hetzner-cloud stack group without a spec bucket creates an object
bucket. Its name is chosen once; swarmy retrieves the S3 keys from the cloud
credential store instead of copying them into the record. AWS groups create
no bucket until instance-role access is supported, and custom modules create
none because swarmy does not know their provider's bucket credentials.
Hetzner groups get their cloud credentials and, when stored, S3 credentials;
AWS groups get AWS credentials. Scaleway groups get Scaleway credentials. Custom modules get all stored provider
credentials. OpenTofu receives these only through its environment, never
through variables or module files. Every group also sets
`AWS_SHARED_CREDENTIALS_FILE=/dev/null`, `AWS_CONFIG_FILE=/dev/null`, and
`AWS_EC2_METADATA_DISABLED=true` to prevent fallback to local AWS credentials.
Every group also gets `SCW_CONFIG_PATH=/nonexistent/swarmy/scw-config.yaml`
so a local Scaleway CLI profile is never used.
On teardown, orphan group directories no longer named in the spec are destroyed
first, then the remaining groups in reverse placement order. A bucket-owning
group gets the force-destroy flag before destruction.
