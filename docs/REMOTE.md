# Remote development nodes

Run `swarmy remote up NAME` from a swarmy checkout to launch one Ubuntu 24.04
EC2 node, copy the checkout, build the release binaries, and start FoundationDB,
NATS, SeaweedFS, and swarmyd under systemd. The local machine needs `ssh`,
`ssh-keygen`, and `rsync`. AWS credentials use the SDK's standard credential chain.
The subnet must provide outbound internet access and the security group must
allow SSH from your machine. Backing services listen only on loopback.

Add this to the discovered `.swarmy/config.toml` (or user configuration file):

```toml
[remote]
region = "us-east-1"
subnet = "subnet-..."
security_group = "sg-..."
instance_type = "m6id.xlarge"
disk_gb = 100
managed_by_tag = "swarmy"
# image = "ami-..."
```

The image defaults to Canonical's current Ubuntu 24.04 amd64 image, resolved
through SSM in the configured region. Custom images must be compatible with
Ubuntu 24.04, the `ubuntu` SSH user, cloud-init, and passwordless sudo. Use an
instance type with local NVMe instance storage. Provisioning labels and mounts
an unused instance-store disk at `/mnt/swarmy-local` and puts volume caches and
dirty data there. EBS holds the repository, backing databases, and node identity.
The script refuses to format EBS disks or reuse unrecognized filesystems.

```sh
swarmy remote up demo
# up prints an SSH command and its elapsed time
swarmy remote down demo
```

The instance, root volume, and imported ed25519 key pair receive `Name` and
`managed-by` tags at creation. IAM needs EC2 `DescribeImages`,
`DescribeInstances`, `DescribeKeyPairs`, `RunInstances`, `ImportKeyPair`, `CreateTags`,
`TerminateInstances`, and `DeleteKeyPair`, plus SSM `GetParameter` when no image
is supplied. Tag-restricted identities should set `managed_by_tag` to their
permitted value. The root volume is encrypted and deleted on termination.

Node records are JSON at `<state directory>/remote/<name>.json`. The state
directory is the configuration file's parent, or `.swarmy` in the current
directory when no configuration exists. Records include region, instance id,
public/private IPs, SSH user and key path, ports, creation time, and an empty
`nodes` list reserved for future multi-node support. SSH tries the public address and then the private address, so launchers in the
same VPC can use security group membership rules. The final SSH command uses the
address that answered. Keys and records are private local files. SSH stores host keys next to the generated key.

An interrupted or failed `up` retains its state so `down` can clean up. A unique
client token identifies a launch if its response was lost before the instance id
was saved. Run `down` before retrying `up` with the same name. `down` waits for
termination and deletes the AWS key and local record even when the instance was
already deleted. API failures retain state for retry. Do not delete the state
directory while cloud resources still exist.

On the node, `sudo systemctl status swarmy-stack swarmyd` shows the services,
and `sudo journalctl -u swarmyd -f` follows node logs. The provisioning script
writes `/etc/swarmy/node.env`, enables both units at boot, and configures
`Restart=always` for swarmyd. To rerun provisioning, use
`cd ~/swarmy && bash scripts/remote-provision.sh`. The scheduler, gateway, worker,
and CLI release binaries are also installed in `/usr/local/bin`.

The local NVMe mount persists across reboot. Instance stop/start can discard
instance-store data and is not supported as a preservation mechanism. `down`
permanently deletes the development node and its EBS backing data.
