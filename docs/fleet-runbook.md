# Fleet runbook

How swarmy's own development runs on swarmy: a long-lived split swarm on one
EC2 node, a pool of worker agents, and the driver in `scripts/fleet/`. This
is the manual version of what the swarm-model and cloud-topology goals will
automate; every step here is a plain command.

## Shape and cost

| Piece | Value | Why |
|---|---|---|
| Control node | `m6i.large`, 40 GiB root disk, `--sandboxes 0` | keep the control plane on a small EBS-backed node |
| Sandbox node | `m6id.4xlarge`, 100 GiB root disk, local NVMe | worker builds use instance-store scratch |
| Sandboxes per sandbox node | 4 | one worker per lane |
| Control plane | on the node (`--services node`) | the laptop can disconnect |
| Image | `images/swarmy-dev` registered as `base-ubuntu:dev`, the node's default | toolchain, dev stack, fetched crates; no warm build |
| Snapshots and collection | retention 3, collector grace 30 min, interval 10 min (set by provisioning) | build caches churn; ten snapshots and six hours of grace filled a 100 GB root disk in an hour |
| Scratch | local NVMe at `/mnt/swarmy-local/scratch`; `/home/agent/.cargo-target` and `/tmp` in each worker | build outputs stay warm on the same node without entering snapshots or S3 |
| Cost | check current EC2 prices for both shapes, plus S3 and inference | separate control and sandbox nodes |

Observed with three workers building at once: 3 GiB used, load 4. The root
disk, which holds the node's object store, filled to 96 percent within an
hour because every build cache chunk was uploaded and kept; scratch mounts
(a dev-fleet task) move caches off the durable volume, and the retention and
collection settings above bound what remains. Watch `df -h /` on the node
until scratch lands.

## Bring-up

1. Local config: `.swarmy/config.toml` has `[remote]` with region, subnet,
   security group, and `instance_type = "m6id.2xlarge"`. `~/.aws/credentials`
   holds an identity with the EC2 permissions in `docs/REMOTE.md`.
   `~/.swarmy/auth.json` holds swarmy's own ChatGPT login (`swarmy auth
   login` once), and `~/.swarmy/keyring` the cluster key.
2. Build and install the CLI from the commit you want the swarm to run:
   `make install`. The version guard refuses a mismatch later.
3. Launch the control node: `swarmy remote up dev --services node
   --sandboxes 0 --instance-type m6i.large --disk-gb 40 --copy-credential
   --image-recipe images/swarmy-dev --bucket YOUR-BUCKET`. Then add each
   sandbox node: `swarmy remote add-node dev --instance-type m6id.4xlarge
   --disk-gb 100 --sandboxes 4`. The first node runs backing and control
   services, swarmyd (for image registration), and no sandboxes; the joining
   nodes host the sandboxes on local NVMe. For a quick single-node setup on
   an NVMe-backed type, omit `--sandboxes 0` and the add-node commands.
4. Connect: `swarmy remote connect dev`. FoundationDB and NATS use tunnels;
   S3 uses the laptop's AWS credentials directly.
5. Credentials go into the swarm's encrypted store, not into files:
   `swarmy auth import --remote dev` for the ChatGPT login,
   `swarmy auth set openrouter --file KEYFILE --remote dev` for OpenRouter.
   The gateway watches the credential store; no manual restart is needed.
6. Check: `swarmy doctor --remote dev` shows each provider as
   `gateway=served`; `swarmy remote status` shows the node heartbeat and the
   image. A live turn: `swarmy --remote dev run --provider openrouter --model
   openai/gpt-6-sol "reply with the word ready"`.
7. Fleet config: copy `scripts/fleet/fleet.example.toml` to
   `scripts/fleet/fleet.toml`, set the GitHub token and pool size, `chmod
   600`. The file is ignored by git.

## Daily operation

- `scripts/fleet/fleet status`: one line per worker with task, provider,
  model, state and its age, task elapsed time, and cost. A recent wait can show
  `waiting_inference 2m; waiting for inference: ...`. A session in
  `waiting_inference` or `leased` longer than `stall_minutes` (default 10)
  shows `STALLED` in the state column and makes status exit 2; an operator or
  monitoring loop should investigate. Other statuses exit 0. The age is the
  last durable state transition from `session ls --json`, not the task age.
  Older sessions without that timestamp show `?` until their next transition.
  A provider-limited worker holds no lease and resumes when the limit clears.
- Launch: `scripts/fleet/fleet launch TASK --provider P --model M`. The
  driver prefers an idle worker created with that provider and creates one
  while the pool is below `workers`. Workers keep their disks, so the second
  task on a worker builds in a minute or two instead of ten.
- Collect and release: when a worker reports a pull request,
  `fleet collect TASK` verifies and records it in tasky; after the merge,
  `fleet release TASK` frees the worker. `fleet kill TASK` ends a turn that
  should not continue, including a parked one.
- Review every pull request before merging. Medium-reasoning workers do what
  the task text says; the task text and the review are the quality control.
- Cost: `swarmy agent show worker-N --remote dev` totals a worker's spend;
  the cost views from the provider goal will replace this.

## Subscription limits

All ChatGPT workers and the codex-daytona lanes share one subscription's
usage limit. When it trips, the gateway opens the provider's breaker, the
affected workers park without a lease, and `fleet status` shows the reason.
They continue when the limit clears; a turn gives up only after
`[inference] max_wait_seconds` (default one hour). OpenRouter workers are
unaffected. To stop waiting instead, `fleet kill TASK`.

## Disk on the node

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

## Adding a node

`swarmy remote add-node dev` joins a second node to the same backing
services and doubles the lanes; raise `workers` in `fleet.toml`. Nodes do
not replicate the backing services: the first node holds the store.

## Updating the swarm in place

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

## Recovery

- Node died or was terminated: `swarmy remote up dev` again and repeat
  bring-up steps 4 to 7. Everything on the node's disk is gone, including
  the store, so workers and their caches are recreated on first launch.
  Sessions are one pull request each, so nothing precious is lost; pushed
  branches survive on GitHub.
- A worker's disk in a bad state: `scripts/fleet/fleet reset worker-N`
  deletes it; the next launch recreates it from the image.
- The laptop closed: nothing stops. Reconnect with `swarmy remote connect
  dev` and `fleet status`.
- Tear down: `swarmy remote down dev` terminates the instance and deletes
  its key pair. Collect and merge open pull requests first.
