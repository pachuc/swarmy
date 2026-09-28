# Operating the development fleet: handoff

This is the method for running swarmy's own development on the fleet, written
so that a fresh session (any model, any operator) can pick the work up. Live
state changes hourly and is not here: read it from tasky (`tasky task order`
for the `cleanup` goal), `scripts/fleet/fleet status`, and `gh pr list`. The
owner's standing decisions are in the auto-memory notes for this repository;
the durable ones are repeated below.

## Ground rules from the owner

- The laptop is a personal gaming machine and a control plane only. Run only
  light commands on it: the fleet driver, tasky, `gh`, `ssh`, short `swarmy`
  calls, Python unit tests, `rustfmt` on single files. Never `cargo build`,
  `cargo test`, or `cargo clippy` locally. Build a laptop CLI on the dev2
  control node and copy it back (see "Rebuilding the laptop CLI").
- Operate autonomously. The owner checks in and wants: what merged, what is
  in flight, issues, the current phase, and what remains, in plain English.
- Review every pull request before merging; use a review subagent for large
  diffs. Merge on CI green plus, for changes in the areas AGENTS.md lists,
  the root-only suites on the sandbox node.
- Never print credentials. Keys live in `~/api_keys.md`. The permission
  classifier refuses to write tokens onto remote nodes or to grep transcripts
  for keys; do not work around it.
- Do not automate git pushes in the driver or with a timer; the push rule is
  prompt wording only.
- Breaking API changes are acceptable while swarmy is its own only client,
  provided they are recorded in `docs/api-breaks.txt` with a date and reason
  (pull request 173 adds the file and wires it into the compat check).
- Order of work: the `cleanup` goal first, then the roadmap goals. The
  cleanup goal must show build time, test time, and lines of code falling;
  the baseline is `docs/proofs/cleanup-baseline-2026-09.md` and the final
  measurement task closes the goal.
- Keep `~/muse-spark-issues.md` updated with every shortcoming of a worker
  running Muse Spark: date, task id, pull request, commits, what the
  specification asked, what went wrong. The owner uses it for model feedback.

## The swarm

- `dev2`: control node `dev2` (m6i.xlarge, no sandboxes); worker node
  `dev2-3` (m6id.8xlarge, four sandboxes at 24 GiB each); and suite node
  `dev2-2` (m6id.4xlarge, `SWARMY_NODE_SANDBOXES=0` in `/etc/swarmy/node.env`,
  so it takes no workers and runs the root-only suites). Addresses, instance
  ids, and the bucket name are in the local remote state (`swarmy remote
  status`, `.swarmy/remote/`), never in the repository. Images
  `base-ubuntu:dev2` and `swarmy-dev:dev2`. The earlier `dev` swarm was
  retired on 2026-09-26.
- Tunnel: `swarmy remote connect dev2`. Local ports are fixed, so only one
  swarm can be connected at a time. Plain `swarmy` commands that need the API
  take `--remote dev2`. `swarmy remote status` shows nodes, services, images.
- SSH helpers: `~/.local/bin/ssh-dev2.sh "cmd"` and `ssh-dev2-2.sh "cmd"`
  (keys under `.swarmy/remote/`). Long commands on a node run detached:
  `setsid nohup bash script.sh > log 2>&1 < /dev/null &`.
- Upgrade: `git merge --ff-only origin/master` locally first (upgrade ships
  the local checkout), then `swarmy remote upgrade dev2` detached with a log;
  about six minutes. Do it when workers are idle.
- Approved, not started: a larger sandbox node (m6id.8xlarge, four workers at
  about 24 GiB each so builds can use several jobs) through
  `swarmy remote add-node dev2 --instance-type ... --disk-gb ... --sandboxes 4`,
  and a separate node for a self-hosted CI runner (task c05a). Do both
  between cleanup waves, since migrating workers drains the fleet. Registering
  the runner needs a GitHub registration token with repository admin rights;
  the owner mints it.

## The fleet driver

`scripts/fleet/fleet` (Python, tests in `scripts/fleet/test_fleet.py`).
Config `scripts/fleet/fleet.toml` (gitignored, mode 600): remote `dev2`,
provider `chatgpt`, model `gpt-6-sol`, effort medium, four workers,
`memory_mib = 8192`, image `swarmy-dev:dev2`, `state_dir` `.dev/fleet-dev2`,
and the GitHub token. Get the token for `gh` with

```sh
export GH_TOKEN=$(grep -E '^(github_token|token)' scripts/fleet/fleet.toml | head -1 | sed -E 's/.*= *"([^"]+)".*/\1/')
```

- `fleet launch TASK [--worker worker-N]`, `fleet status`, `fleet collect TASK`
  (records the pull request in tasky), `fleet release [--force] TASK`,
  `fleet kill TASK`, `fleet resume TASK "text"`, `fleet reset worker-N`,
  `fleet report`, `fleet benchmark`.
- A task's branch is `swarmy/` plus the last six characters of the task ULID,
  lowercased. State files: `.dev/fleet-dev2/<suffix>.json|.jsonl` and
  `worker-N.meta.json` (provider, model, image the worker was created with;
  `launch` matches an idle worker on provider and model).
- Workers created on OpenRouter Muse must be re-pointed once idle to use the
  subscription: `swarmy --remote dev2 agent set worker-N --provider chatgpt
  --model gpt-6-sol --route subscription`, then edit the meta file to
  provider `chatgpt`, model `gpt-6-sol`. The route `subscription` on dev2 is
  `chatgpt/default=gpt-6-sol` then `openrouter/default=meta/muse-spark-1.3-contributor`,
  so workers fail over to Muse when the subscription depletes.
- ChatGPT login expires; when `swarmy --remote dev2 auth ls` shows
  `needs_login`, the owner runs `swarmy --remote dev2 auth login chatgpt`
  (device code in a browser).
- Steering a busy worker: `swarmy --remote dev2 --json session interrupt ID`,
  wait for `fleet status` to show idle, then
  `swarmy --remote dev2 --json run --session ID "message"`. The interrupt
  drops only the in-flight tool call. `fleet kill` removes the launch record,
  so use it only when abandoning a task. Session logs:
  `swarmy --remote dev2 --json session show ID` (one JSON event per line).
- `fleet status` follows the current session after compaction.
  Workers poll long builds with one inference per minute; that is normal and
  cheap on Muse, less so on the subscription.

## The loop for one task

1. Launch, then watch `fleet status` (a shell loop comparing worker, task,
   and state every one or two minutes; strip the age suffix).
2. When the worker finishes: `fleet collect TASK`, `git fetch`, and review.
   Read small diffs directly. For large ones, spawn a review subagent with
   the task body (`tasky task show ID`, JSON), the diff, and the pull request
   description; ask for numbered verdicts per requested point, defects ranked
   with file and line, and a one-line mergeable verdict. Do not run cargo on
   the laptop; the subagent reads code only.
3. If a round is needed: append `Revision (date): ...` to the task body
   (`tasky task body ID --file FILE`), numbered points with files and lines
   and the commands to run; `tasky task fail ID`; `fleet release --force ID`;
   `fleet launch --worker worker-N ID`. Tell the worker not to start over.
4. Merge criteria: CI green on the current head. CI builds the branch merged
   with master, so a branch whose last master merge is stale can fail on code
   it never touched, and once GitHub marks it CONFLICTING it silently stops
   running CI until a merge commit is pushed. Trial-merge in a scratch
   worktree (`git worktree add`) to see the conflicts, then either resolve
   and push yourself for trivial ones or write a rebase round for the worker.
5. Root suites (worker, store, gateway, scheduler, bus, sandbox, volume,
   image changes) run on the suite node dev2-2, which hosts no workers, so
   stopping its node daemon for a run affects nothing else. The scripts are
   in `scripts/node-suites/`; copy them to the node's home directory after
   changing them. Queue a run detached:
   `setsid nohup bash ~/suite-queue.sh --plus swarmy/xxxxxx >/dev/null 2>&1 </dev/null &`
   (`--plus` adds the image, vol, and nbd suites to the default node and chaos
   set; `--node` runs the node suite alone; `--only "PACKAGE TEST"` reruns one
   suite with full output). Every run takes `~/suite.lock`, so queued runs
   wait for each other however they were started. Each branch appends
   `QUEUE_DONE <branch> <mode> SUITES_EXIT=<0|1>` to `~/suite-queue.log`, with
   the run log at `~/suite-logs/<suffix>-<mode>.log` and each suite's full
   output at `~/suite-logs/<suffix>-<package>-<test>.log`. A full `--plus` run
   takes about forty minutes. `swarmy image build` needs an API, which
   `root-suites.sh` serves on a loopback port.

   The chaos suites run the service executables (scheduler, worker, gateway,
   API, node daemon) from `target/debug`; `root-suites.sh` builds them
   explicitly before any suite runs. Before 2026-09-27 it did not, so the
   chaos suites ran the executables an earlier branch had left behind and
   failed within seconds whenever the stored formats differed (for example
   `unsupported stored value version 2`); a failure like that on a new run
   is real. An interrupted nbd or node test can leave `/dev/nbdN`
   attached with no owner, which breaks later nbd tests; `nbd-orphans.sh`
   runs before every suite and detaches such devices. The suite node's build
   directory and dev-stack data live on its local NVMe drive
   (`~/chaos/target` and `~/chaos/.dev` are symbolic links into
   `/mnt/swarmy-local/suites/`); the 100 GB root disk filled once and made
   image uploads fail. When stopping anything on a node over SSH, kill by
   process id: a `pkill -f` pattern also matches the SSH command running it.
6. Merge with `gh pr merge N --squash --delete-branch` and check its output.
   Only then `tasky task test`, `pass`, `done`, and `fleet release TASK`. A
   done task cannot be reopened (a task in the `tasky` project adds a reopen
   command). Then `git merge --ff-only origin/master` locally.
7. Flakes: retrigger with an empty commit from a scratch worktree, and record
   the test in the test-trim task (c05) so it gets fixed there.

## What review rounds have caught (check these first)

- Stored structs that embed API types under postcard's positional encoding;
  the repository's rule is frozen stored structs plus `swarmy_core::trailing`
  for additions, with a fixed-bytes decode test.
- Full scans and in-memory filtering on the hot path; count transactions.
- Feature gates whose off configuration does not compile; tests for the
  negative path written and then deleted.
- Renamed wire tags or removed enum variants without recording the break.
- Docs, CI, Makefile, and scripts not updated for a feature change.
- `#[allow(clippy::...)]` added instead of fixing the lint; hand-collapsed
  lines that rustfmt reflows; descriptions claiming clean checks that fail.
- Over-literal safety readings ("check no rows remain" became "refuse to
  boot"). Say "migrate then clear" in task bodies, and verify configuration
  claims with grep before writing them into a task.

## Tasky

Project `swarmy`; goals `cleanup` (active, sixteen tasks c00 to c15 plus
c05a, dependency chain in the goal spec), `dev-fleet` (active: reopen and
teardown follow-ups), and the roadmap goals. Project `tasky` holds the reopen
command task. Commands used: `task show|add|body|test-plan|fail|test|pass|
done|depend|ready|order|pr`, `goal show|add|activate|spec`. Task bodies are
the specification a worker receives; write them in the style CLAUDE.md
describes, with files, line regions, commands, and a measurable test plan.

## Rebuilding the laptop CLI

On the dev2 control node (`~/swarmy` is a copy of the checkout the last
upgrade shipped): `cargo build --release --locked -p swarmy-cli --features
remote`, detached with a log. Copy `target/release/swarmy` back, fix its
library path with `uvx patchelf --set-rpath
/home/pachu/.local/lib:/usr/lib:/usr/local/lib:/usr/lib/x86_64-linux-gnu`,
and replace the running binary by copying to `swarmy.new` and renaming
(`cp` onto a busy binary fails). Keep a backup under `~/.cargo/bin/`.

## Where things are

- `docs/fleet-runbook.md`: bring-up, daily operation, recovery.
- `docs/proofs/cleanup-baseline-2026-09.md`: the numbers the cleanup must beat.
- `backlog/build-time.md`, `backlog/final-qa.md`: pinned discussions.
- `scripts/node-suites/`: the node-side suite scripts.
- `~/muse-spark-issues.md`: the model issue log.
- `.dev/fleet-dev2/`: driver state; `.dev/fleet/`: the retired dev swarm's.
