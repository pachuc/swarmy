# Fleet runbook

How swarmy's own development runs on swarmy: a long-lived split swarm on one
EC2 node, a pool of worker agents, and the driver in `scripts/fleet/`. This
is the manual version of what the swarm-model and cloud-topology goals will
automate; every step here is a plain command.

## Shape and cost

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

The node build uses `swarmy-cli --no-default-features`: provisioning commands
and the EC2, S3, IAM, and SSM clients belong on the laptop. A 6 GiB fleet
sandbox measured 957,428 KiB peak resident memory for the release build without
that feature. The default-feature build reached 6,123,248 KiB in the EC2
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

## Running the driver on a control node

`scripts/fleet/fleet` normally talks to a swarm through a saved tunnel profile
(`remote = "dev"` in `fleet.toml`). The laptop holds one tunnel at a time, so a
benchmark against a second swarm would blind the driver for the fleet it is
operating. Leave `remote` empty in a `fleet.toml` on the swarm's own control
node instead: the driver then calls `swarmy` with the node's local API
configuration and no tunnel, which is how `benchmarks/run-swarm.sh` runs for
each environment during the perf baseline.

## Perf baseline runners

`benchmarks/run-swarm.sh REMOTE LABEL` runs the three fixed tasks twice each
through `fleet benchmark`, records each invocation's wall time, and writes
`.dev/benchmarks/LABEL-*`; run it on each
swarm's control node with an empty `remote` (previous section).
`benchmarks/run-daytona.py LABEL` runs the same prompts through codex-daytona
in disposable remote sandboxes with `--no-publish`; Codex never runs on the
laptop. Both read `BENCH_PROVIDER`, `BENCH_MODEL`, and `BENCH_EFFORT`. The
baseline uses the same OpenRouter model everywhere so it compares
infrastructure, not models: `BENCH_PROVIDER=openrouter
BENCH_MODEL=meta/muse-spark-1.3-contributor BENCH_EFFORT=medium`. The
codex-daytona leg needs `OPENROUTER_API_KEY` in the launcher's `.env` (it is
placed in the sandbox as a private file) and `CODEX_DAYTONA_DIR` when the
launcher is not at `~/code/codex-daytona`. The runner closes by invoking
`scripts/fleet/fleet report --remote REMOTE --label LABEL --session ID
--wall ID=SECONDS ...`, which prints each run's wall time as `wall_s` next to
`duration_s`; `benchmark` and `report` need no `fleet.toml` and can also run
ad hoc against the local API with `--remote local`.

## The split layout in practice

The first split swarm (`dev2`, 2026-09-24) runs a control node on an
m6i.xlarge (FoundationDB, NATS, scheduler, worker, gateway, API, no
sandboxes) and a sandbox node on an m6id.4xlarge with four sandboxes on its
870 GB local NVMe, with chunks in a real bucket. Bring-up took 28 minutes for
the control node (16 of them building and uploading the dev image) and 10
minutes for the sandbox node. A live turn takes two to three seconds.

Things learned on the way, all fixed in code or documented here:

- The control node needs at least 8 GiB; the node CLI is now built without
  the provisioning SDKs (which needed 5.8 GiB to compile), and provisioning
  refuses early when less than 6 GiB is available.
- The instance role and profile stay with the bucket across `remote down`;
  recreating them seconds before a launch bound the instance to a stale
  profile whose credentials were rejected.
- The laptop can hold a tunnel to only one remote at a time, because
  FoundationDB advertises port 4500 and refuses a remapped port. Run
  `swarmy remote disconnect OLD` before `swarmy remote connect NEW`. While
  the tunnel points elsewhere the fleet keeps running on its nodes; only
  the driver's status, collect, and launch need the tunnel.
- `swarmy doctor` still reports the S3 endpoint of a bucket remote as
  invalid (a dev-fleet task fixes the check); the nodes and the image
  build use the bucket correctly.

## Daily operation

- Each task runs in a fresh side conversation on its worker (`run --agent
  NAME --new`): same disk and memory files, empty context. The worker's main
  conversation is unused. On a metered provider this keeps every turn's
  billed context to the current task instead of the worker's whole history.
  `fleet resume` and `fleet kill` address the task's own session.
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
usage limit. When it trips, the gateway opens that entry's breaker, the
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

Snapshots of worker disks run every 30 minutes on provisioned nodes
(`SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS=1800` in `node.env`). Each snapshot
uploads only changed chunks, and on a real bucket every chunk is one PUT
request, so the period is the main lever on request cost; retention of
three snapshots plus the 30-minute collection grace keeps live data bounded
to the disks' contents plus recent churn.

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
Upgrade all gateway nodes together: a new gateway migrates provider records
to labelled entries on first read, and an old gateway sharing the store loses
the provider once its legacy record is migrated.

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
