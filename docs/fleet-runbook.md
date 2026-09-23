# Fleet runbook

How swarmy's own development runs on swarmy: a long-lived split swarm on one
EC2 node, a pool of worker agents, and the driver in `scripts/fleet/`. This
is the manual version of what the swarm-model and cloud-topology goals will
automate; every step here is a plain command.

## Shape and cost

| Piece | Value | Why |
|---|---|---|
| Instance | m6id.2xlarge (8 vCPU, 32 GiB, local NVMe) | four concurrent `cargo build` runs need more than 16 GiB |
| Sandboxes per node | 4 (`SWARMY_NODE_SANDBOXES` in `scripts/remote-provision.sh`) | one worker per lane |
| Control plane | on the node (`--services node`) | the laptop can disconnect |
| Image | `images/swarmy-dev` registered as `base-ubuntu:dev`, the node's default | toolchain, dev stack, fetched crates; no warm build |
| Snapshots and collection | retention 3, collector grace 30 min, interval 10 min (set by provisioning) | build caches churn; ten snapshots and six hours of grace filled a 100 GB root disk in an hour |
| Cost | about $0.48 an hour, about $350 a month, plus a few dollars of S3 and inference | one node; add a second when four lanes stay saturated |

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
3. Launch: `swarmy remote up dev --services node --copy-credential
   --image-recipe images/swarmy-dev`. About ten minutes: instance, backing
   services, release build on the node, systemd units, the dev image.
4. Connect: `swarmy remote connect dev`. Tunnels to FoundationDB, NATS, and
   S3 stay up as a recorded background process.
5. Credentials go into the swarm's encrypted store, not into files:
   `swarmy auth import --remote dev` for the ChatGPT login,
   `swarmy auth set openrouter --file KEYFILE --remote dev` for OpenRouter.
   Until the gateway learns to watch the store (a dev-fleet task), restart
   the gateway on the node after adding a credential:
   `ssh ... sudo systemctl restart swarmy-gateway`.
6. Check: `swarmy doctor --remote dev` shows each provider as
   `gateway=served`; `swarmy remote status` shows the node heartbeat and the
   image. A live turn: `swarmy --remote dev run --provider openrouter --model
   openai/gpt-6-sol "reply with the word ready"`.
7. Fleet config: copy `scripts/fleet/fleet.example.toml` to
   `scripts/fleet/fleet.toml`, set the GitHub token and pool size, `chmod
   600`. The file is ignored by git.

## Daily operation

- `scripts/fleet/fleet status`: one line per worker with task, provider,
  model, state, elapsed time, and cost. A worker showing `waiting for
  inference: ...` is parked behind a provider limit and holds no lease; it
  resumes by itself when the limit clears. Nothing to do.
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

## Adding a node

`swarmy remote add-node dev` joins a second node to the same backing
services and doubles the lanes; raise `workers` in `fleet.toml`. Nodes do
not replicate the backing services: the first node holds the store.

## Updating the swarm in place

There is no upgrade command yet (a swarm-model task). The manual procedure,
which is what the automation will do:

1. Ship the commit: `git archive --format=tar COMMIT | ssh NODE 'tar -x -C
   ~/swarmy'`. The archive gives files the commit's timestamp, which can be
   older than the last build's, so also `find ~/swarmy/crates -type f -exec
   touch {} +` or cargo will skip the rebuild.
2. On the node: `cargo build --release --locked -p swarmy-cli -p swarmyd -p
   swarmy-scheduler -p swarmy-gateway -p swarmy-worker`, `sudo install` the
   six binaries into `/usr/local/bin`, then `sudo systemctl restart
   swarmy-scheduler swarmy-worker swarmy-gateway`. These three can restart
   at any time; sessions resume from the store.
3. Restart `swarmyd` only when no worker is mid-task: it tears down the
   sandboxes it hosts, and a running command fails.
4. `make install` on the laptop so the CLI matches.

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
