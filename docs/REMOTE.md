# Remote development nodes

Run `swarmy remote up NAME` from a swarmy checkout to launch one Ubuntu 24.04
EC2 node, copy the checkout, build the release binaries, and start FoundationDB,
NATS, SeaweedFS, and swarmyd under systemd. The local machine needs `ssh`,
`ssh-keygen`, and `rsync`. AWS credentials use the SDK's standard credential chain.
The subnet must provide outbound internet access and the security group must
allow SSH from your machine and TCP between group members. New remotes advertise
the first node's private address. Backing services listen on all interfaces so
both loopback and private-network clients can connect; restrict ingress to the
security group and do not expose these development services to the internet.

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
swarmy remote add-node demo
swarmy remote connect demo
swarmy dev up --remote demo
swarmy remote status
swarmy dev down
swarmy remote disconnect demo
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
public/private IPs, SSH user and key path, ports, creation time, and joining
instances in `nodes`. `launch_settings` retains the resolved AMI and launch
configuration, so later config edits do not change the subnet, security group,
instance type, disk size, region, or ownership tag used by `add-node`.
Older records without saved launch settings still support connect and down;
recreate those remotes before adding nodes. Provisioning, tunnels, logs, and status try the public address and then the private address, so launchers in the
same VPC can use security group membership rules. The final SSH command uses the
address that answered. Keys and records are private local files. SSH stores host keys next to the generated key.

An interrupted or failed `up` or `add-node` retains its state so `down` can clean up. A unique
client token identifies a launch if its response was lost before the instance id
was saved. Run `down` before retrying `up` with the same name. `down` waits for
termination of every node and deletes their AWS keys and local records even
when instances were already deleted. Joining nodes are terminated first. API failures retain state for retry. Do not delete the state
directory while cloud resources still exist.

On the node, `sudo systemctl status swarmy-stack swarmyd` shows the services,
and `sudo journalctl -u swarmyd -f` follows node logs. The provisioning script
writes `/etc/swarmy/node.env`, enables both units at boot, and configures
`Restart=always` for swarmyd. To rerun provisioning, use
`cd ~/swarmy && bash scripts/remote-provision.sh stack PRIVATE_IP` on the first
node. Joining nodes run only swarmyd, with no backing-services unit dependency;
rerun their provisioning with `node FIRST_NODE_PRIVATE_IP` instead. Their cluster
file is copied from the first node, preserving its cluster identity and private
coordinator address. The scheduler, gateway, worker,
and CLI release binaries are also installed in `/usr/local/bin`.

The local NVMe mount persists across reboot. Instance stop/start can discard
instance-store data and is not supported as a preservation mechanism. `down`
permanently deletes the development node and its EBS backing data.

`add-node` adds compute capacity, not replicas of the backing services. Losing
the first node still loses this development stack. For a recovery exercise,
place a computer on a joining node and terminate that node while the first node
remains running. `status` lists every saved instance and the stack's live and
stale swarmyd registrations. The laptop tunnel forwards to the first node's
private address and keeps local FoundationDB port 4500; stop any local dev stack
before connecting.
