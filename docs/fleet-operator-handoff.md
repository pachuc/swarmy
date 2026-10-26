# Operating the development fleet: handoff

This is the method for running swarmy's own development on the fleet, written
so that a fresh session (any model, any operator) can pick the work up. Live
state changes hourly and is not here: read it from tasky (`tasky goal list` and
`tasky task order` for each active goal), `scripts/fleet/fleet status`, and `gh pr list`. The
owner's standing decisions are in the auto-memory notes for this repository;
the durable ones are repeated below.

For the one-time conversion of hz from the old client, see "Converting hz to a swarm (one time)" below.

## Ground rules from the owner

- The laptop is a personal gaming machine and a control plane only. Run only
  light commands on it: the fleet driver, tasky, `gh`, `ssh`, short `swarmy`
  calls, Python unit tests, `rustfmt` on single files. Never `cargo build`,
  `cargo test`, or `cargo clippy` locally. Build a laptop CLI on the Hetzner
  server and copy it back (see "Rebuilding the laptop CLI").
- Operate autonomously. The owner checks in and wants: what merged, what is
  in flight, issues, the current phase, and what remains, in plain English.
- Build every goal with "The cycle for a goal" below. Review every task
  branch before integrating it. The goal's one pull request to master merges
  on CI green, after the batch's local CI run and, for the areas AGENTS.md
  lists, the root suites on the suite node have passed.
- Never print credentials. Keys live in `~/api_keys.md`. The permission
  classifier refuses to write tokens onto remote nodes or to grep transcripts
  for keys; do not work around it.
- Do not automate git pushes in the driver or with a timer; the push rule is
  prompt wording only.
- Breaking API changes are acceptable while swarmy is its own only client,
  provided they are recorded in `docs/api-breaks.txt` with a date and reason
  (pull request 173 adds the file and wires it into the compat check).
- Order of work, as of 2026-10-01: the `hetzner` goal (its migration task
  waits on the owner's Hetzner account), then the roadmap goals.
- Keep `~/muse-spark-issues.md` updated with every shortcoming of a worker
  running Muse Spark: date, task id, pull request, commits, what the
  specification asked, what went wrong. The owner uses it for model feedback.

## The swarm

- `hz` (since 2026-10-02): one dedicated server rented from Hetzner's server
  auction (16-core i9-12900K, 128 GB, two NVMe drives mirrored, Ubuntu 24.04),
  adopted as an existing host (REMOTE.md, "Swarms on existing hosts"). The same
  machine runs the control plane and the node, with four
  sandboxes on local storage `dir:/srv/swarmy-local`. The bucket is Hetzner
  Object Storage with static keys. Images `base-ubuntu:hz` and
  `swarmy-dev:hz`. Addresses, server numbers, the bucket name and every
  credential live outside the repository: `~/swarmy-infra.md` and
  `~/api_keys.md` on the laptop, and `~/.local/share/swarmy/swarms/hz/keys/`.
- The suite node is a second, smaller auction server that is not part of the
  swarm; it runs the root suites and the local CI ("The suite node").
- AWS is gone: the `dev2` swarm, the earlier `dev` swarm, their buckets and
  images were decommissioned on 2026-10-01.
- Tunnel: commands with `--swarm hz` open one on-demand tunnel for the swarm
  on free local ports. `swarmy swarm status hz` shows SSH and tunnel status.
- SSH: the laptop's `~/.ssh/config` has an alias for each server, using the
  key registered in Robot as `swarmy-fleet`; `swarmy swarm` keeps its own
  keys under `~/.local/share/swarmy/swarms/hz/keys/`. Long commands on a server run detached:
  `setsid nohup bash script.sh > log 2>&1 < /dev/null &`.
- Images: `swarmy image build` builds on the machine it runs on and needs
  root, so build on the server the way provisioning builds the base image: as root
  in the service user's checkout, with `/etc/swarmy/node.env` sourced, run
  `/usr/local/bin/swarmy image build images/swarmy-dev --name swarmy-dev --tag hz`.
- Upgrade: `git merge --ff-only origin/master` locally first (upgrade ships
  the local checkout), then `swarmy swarm upgrade hz` detached with a log.
  Do it when workers are idle.
- Hetzner itself: dedicated servers are managed through the Robot webservice
  (order, SSH keys, rescue system, reset), with its login in `~/api_keys.md`;
  Object Storage keys can only be created in the Cloud Console. Ask the owner
  before ordering anything, because orders bill the account. Auction servers
  ordered through the API arrive IPv6-only: add the `primary_ipv4` add-on
  (it needs a `reason`), because GitHub has no IPv6. A new account's further
  orders go to manual review, and the API answers 412 meanwhile. Install
  Ubuntu from the rescue system with `bash -ic "installimage -a -c FILE"`, and
  reboot the rescue system first if an address was added after it booted.

## Converting hz to a swarm (one time)

With the old client, disconnect hz to close its old SSH master. Install the new
laptop client. Run the one-shot converter with the old record path first in dry
run mode and review its plan, then run it without dry run. It reads the API token
from the server over SSH; it does not change the server and will not overwrite
an existing swarm. Keep its output private even though secrets are redacted.

```sh
scripts/convert-remote-record-2026-10.py --old-record <checkout>/.swarmy/remote/hz.json --dry-run
scripts/convert-remote-record-2026-10.py --old-record <checkout>/.swarmy/remote/hz.json
swarmy swarm show hz
swarmy --swarm hz doctor
swarmy swarm use hz
swarmy swarm status hz
```

Do not run `swarmy swarm up hz` as a test, and do not rely on
`--copy-credential`. Later, `swarmy swarm upgrade hz` installs a
`swarmy-core` that ignores hz's old `[remote]` table. To undo, run
`swarmy swarm rm hz --force`: it forgets the spec, record and state and leaves
the server untouched.

## The fleet driver

`scripts/fleet/fleet` (Python, tests in `scripts/fleet/test_fleet.py`).
Config `scripts/fleet/fleet.toml` (gitignored, mode 600): swarm `hz`,
provider `openrouter`, model `meta/muse-spark-1.3-contributor`, effort
medium, four workers, `memory_mib = 24576`, image `swarmy-dev:hz`,
`state_dir` `.dev/fleet-hz`, and the GitHub token. Get the token for `gh` with

```sh
export GH_TOKEN=$(grep -E '^(github_token|token)' scripts/fleet/fleet.toml | head -1 | sed -E 's/.*= *"([^"]+)".*/\1/')
```

- `fleet launch TASK [--worker worker-N]`, `fleet status`, `fleet collect TASK`
  (checks the pushed branch, links its head SHA in tasky, moves the task to testing,
  and prints the report), `fleet release [--into BRANCH] [--force] TASK`,
  `fleet kill TASK`, `fleet resume TASK "text"`, `fleet reset worker-N`,
  `fleet report`, `fleet benchmark`.
- `scripts/fleet/last-message TASK [COUNT]` prints earlier assistant messages
  when COUNT is given. Set `FLEET_DRIVER` to run it against another copy of
  the driver. `fleet release` frees the worker once a commit on the integration
  branch (`--into` or `integration` in fleet.toml) contains `tasky <id>`.
- A task's branch is `swarmy/` plus the last six characters of the task ULID,
  lowercased. State files: `.dev/fleet-hz/<suffix>.json|.jsonl` and
  `worker-N.meta.json` (provider, model, image the worker was created with;
  `launch` matches an idle worker on provider and model).
- The `hz` cluster's credential store holds only the OpenRouter key. Before
  workers can use the ChatGPT subscription, import the owner's credential
  (`swarmy auth import --swarm hz`; `adopt --copy-credential` put the file
  on the server) and create a route `subscription` that tries
  `chatgpt/default=gpt-6-sol` then `openrouter/default=meta/muse-spark-1.3-contributor`
  (`swarmy auth routes`), so workers fail over to Muse when the subscription
  depletes. Then re-point idle workers with `swarmy --swarm hz agent set
  worker-N --provider chatgpt --model gpt-6-sol --route subscription` and
  edit each meta file to provider `chatgpt`, model `gpt-6-sol`.
- ChatGPT login expires; when `swarmy --swarm hz auth ls` shows
  `needs_login`, the owner runs `swarmy --swarm hz auth login chatgpt`
  (device code in a browser).
- Steering a busy worker: `swarmy --swarm hz --json session interrupt ID`,
  wait for `fleet status` to show idle, then
  `swarmy --swarm hz --json run --session ID "message"`. The interrupt
  drops only the in-flight tool call. `fleet kill` removes the launch record,
  so use it only when abandoning a task. Session logs:
  `swarmy --swarm hz --json session show ID` (one JSON event per line).
- `fleet status` follows the current session after compaction.
  Workers poll long builds with one inference per minute; that is normal and
  cheap on Muse, less so on the subscription.

## The cycle for a goal

The owner asked on 2026-10-01 that every goal follow this cycle. It was first
used for cleanup-3, which went from approval to one pull request in about ten
hours. Its point is to spend CI and root-suite time once per goal instead of
once per task. It also turns each task into a job a worker can hardly get
wrong.

1. **Plan small tasks.** Write the goal and its tasks in tasky. Give each task
   one concern, so that its diff is a few hundred lines. Split anything
   bigger, or anything that mixes concerns, before launch. Note which tasks
   touch the same files; those overlaps are where the merge conflicts will
   come from.
2. **Refine every task into a recipe before launch.** Spawn read-only
   research subagents, one per one or two related tasks, with no cargo. Each
   one reads the current code and rewrites the body as exact steps:
   - every file, function and line to change;
   - every call site, found by grep and listed as a checklist;
   - the full signature and a short sketch of any new helper;
   - an out-of-scope list and the pitfalls found while reading;
   - "Done when" commands with their expected results: `cargo fmt`, the
     `make check-*` targets, specific tests, and `rg` checks that must print
     nothing.

   The researchers check every claim in the original body against the code.
   On cleanup-3 they corrected about a dozen wrong assumptions; one task had
   33 call sites, not 13. For scripts and config, have the researcher write
   and test the new files, and put them on a seed branch the workers copy
   from instead of pasting them into the body. Keep the refined bodies in the
   scratchpad under the goal's name, and load each one into tasky
   (`tasky task body`, `tasky task test-plan`) with the delivery footer
   below appended.
3. **Create the integration branch.** Run
   `git push origin master:refs/heads/<goal>` and keep a scratch worktree on
   it. Do not use tasky dependencies within the goal. A dependent task only
   becomes ready once its prerequisite is done, and nothing is done until the
   whole batch reaches master. Control the order by when you launch tasks
   instead, and start a task that builds on merged work from the integration
   branch.
4. **Launch.** Run `fleet launch --worker worker-N TASK`. The launch prompt
   asks for branch delivery (no pull request; report the branch, head SHA and
   "Done when" output) and names the branch `swarmy/<task suffix>`, so a task
   body never names a branch; it says "Base: the integration branch" when the
   task builds on merged work. Keep every worker busy: when one frees up,
   launch the next ready recipe before you review its branch.
5. **When a worker goes idle, read its report** with `fleet collect TASK`.
   The report gives the branch (`swarmy/` plus the task suffix), the head SHA,
   the "Done when" output, and the deviations. Keep the worker assigned through
   review so `fleet resume` still works for fixes.
6. **Review every branch before integrating it.** Spawn a review subagent
   with the refined body, the worker's reported deviations, and the diff
   against the integration branch. Ask for numbered checks and a merge or
   fix-first verdict. Also ask it to run
   `git merge-tree --write-tree origin/<goal> origin/<branch>`. That command
   shows the textual conflicts, but the reviewer must also look for breaks
   that git does not report:
   - a function this branch deletes or gates that other merged work still
     calls;
   - a dependency that another merged task made unused, or a feature another
     task now needs.

   Send real defects back to the worker with `fleet resume`, as one numbered
   round.
7. **Integrate with a squash.** Run `git merge --squash origin/<branch>` on
   the integration worktree. Commit with a clean message: the worker's final
   commit message, or one you write, plus `tasky <id>` and the co-author
   trailer. Never keep a "WIP" title. Resolve only trivial conflicts yourself.
   Do not hand-edit code during integration. Twice on cleanup-3 a quick
   operator edit did not compile, and a worker had to fix it. Anything beyond
   a one-line resolution goes to an **integration fixer** task on an idle
   worker, branched from the integration branch, followed by `cargo fmt` and
   `make check-lint`. Push the squash commit with its `tasky <id>` line,
   then run `fleet release TASK` and launch the next task on that worker. Set
   `integration = "<goal>"` in fleet.toml for each goal. `--force` frees a worker
   early at the cost of `fleet resume` for that task.
8. **Validate in batches on the suite node.** After every few merges, run
   `suite-queue.sh --at <sha> --ci <goal>`. It runs
   `scripts/node-suites/ci-local.sh`, every CI job's `make check-*` target,
   in about fifteen minutes. Always pin `--at`, because every run of a branch
   writes the same log file. Once all tasks are merged, run `--plus` (the
   root suites) and a final `--ci` on the head. Reinstall the node scripts in
   `~` whenever the batch changes them, and compare `sha1sum` against the
   branch.
9. **One pull request to master.** Write a description of what the goal
   fixed, removed and changed, the suite-node validation with its commits,
   and every tasky id. GitHub CI is the merge gate. Merge with
   `gh pr merge N --merge` to keep each task's commit. Then run
   `tasky task pass` and `tasky task done` for every task (`fleet collect`
   already ran `tasky task test`), then
   `tasky goal complete`, and fast-forward the local checkout.

The delivery footer appended to every refined body:

> **How to deliver.** Do NOT open a pull request. Work on your task branch.
> Follow the steps above exactly and in order; if the code does not match
> what this body says, follow the intent and say what differed. Do not
> invent extra changes. Before pushing, run every command under "Done when"
> and make sure each passes, and run `cargo fmt --all` before committing.
> Push the branch and report its name, the head SHA, the full output of the
> "Done when" commands, and anything you could not do and why.

Things this cycle has taught:

- Workers push "WIP" commits for safety, so every branch is squashed when it
  is integrated.
- Use background agents (the Agent tool) for parallel reviews and recipe
  research, never an approval-gated orchestration tool while the owner is
  away: a review batch once waited eight hours on a permission prompt with
  every worker idle.
- Reviews routinely find small issues; batch them into one follow-up "fix the
  review findings" task per merged branch instead of editing by hand while
  integrating.
- The suite node also runs `swarmyd` and a tunnel that forwards the fleet's
  NATS on port 4222. `ci-local.sh` stops both and restores them on exit, and
  it refuses to run tests if the dev stack does not start.
- `check-docs-accuracy.py` refuses to inspect ignored paths behind a
  symlink. That is why `ci-local.sh` runs from a second checkout (`~/ci-src`)
  and shares the build directory through `CARGO_TARGET_DIR`.
- The script tests must run without the dev stack's exported variables, as
  they do in CI. A leftover `SWARMY_DEV_FDB_PORT` broke them.
- Tests that a goal switches on run for the first time against the real
  stack, and they can fail for reasons the task never touched. Hand each
  such failure to an idle worker as an investigation task that must find the
  root cause and must not loosen the test.

## The suite node

Root suites run on the suite node, which hosts no workers, so
stopping its node daemon for a run affects nothing else. They are serial
and slow (a full `--plus` run takes forty to sixty minutes), so run only
what the change can break, and nothing for docs, CLI-only, test-only,
fleet-script, or fixture-covered provider changes: the node suite for
swarmyd, sandbox, placement, and hosting; the chaos suites for worker,
scheduler, gateway, and bus paths the chaos harness kills; image, vol,
and nbd for those crates; stored-format changes for every suite that
reads them. State in one line which suite a change could break before
queueing. The scripts are
in `scripts/node-suites/`; copy them to the node's home directory after
changing them. Queue a run detached:
`setsid nohup bash ~/suite-queue.sh --at COMMIT --chaos swarmy/xxxxxx >/dev/null 2>&1 </dev/null &`
(`--ci` runs every CI job's `make check-*` target through `ci-local.sh`; `--plus` adds the image, vol, and nbd suites to the default node and chaos
set; `--node` runs the node suite alone; `--chaos` runs the three chaos
suites and `chaos-ci.sh`; `--only "PACKAGE TEST"` runs one suite;
`--at COMMIT` pins the commit, because the branch head is read when the
run starts and a worker still pushing can leave it uncompilable). Every run takes `~/suite.lock`, so queued runs
wait for each other however they were started. Each branch appends
`QUEUE_DONE <branch> <mode> SUITES_EXIT=<0|1>` to `~/suite-queue.log`, with
the run log at `~/suite-logs/<suffix>-<mode>.log` and each suite's full
output at `~/suite-logs/<suffix>-<package>-<test>.log`.
`swarmy image build` and the chaos suites need an API, which
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

A test that fails once and passes on a rerun is a flake: note it in the
handoff memory note, and open a task when the same test flakes again.

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

Project `swarmy`. `tasky goal list` shows the goals and their states, and
each active goal has one integration branch named after its slug. Project
`tasky` holds the reopen command task. Commands used: `task show|add|body|
test-plan|fail|test|pass|done|depend|undepend|ready|order|pr`, `goal
list|show|add|activate|spec|complete`. Task bodies are the specification a
worker receives. Write them as the recipes "The cycle for a goal" describes,
in the style CLAUDE.md asks for.

## Rebuilding the laptop CLI

On the server, as root in `/root/cli-build` (a plain clone of the
repository): `git fetch` and check out `origin/master`, then `cargo build
--release --locked -p swarmy-cli --features remote` (about two minutes). The feature supplies the S3 client for swarm buckets.
Copy `target/release/swarmy` back and replace the running binary by copying
to `swarmy.new` and renaming (`cp` onto a busy binary fails). The CLI does
not link the FoundationDB client, so it needs no library-path fix. Keep a
backup under `~/.cargo/bin/`.

## Where things are

- `docs/fleet-runbook.md`: bring-up, daily operation, recovery.
- `docs/proofs/cleanup-baseline-2026-09.md`: the numbers the cleanup must beat.
- `backlog/build-time.md`, `backlog/final-qa.md`: pinned discussions.
- `scripts/node-suites/`: the node-side suite scripts.
- `~/muse-spark-issues.md`: the model issue log.
- `.dev/fleet-hz/`: driver state; `.dev/fleet-dev2/` and `.dev/fleet/`: the retired AWS swarms'.
- `~/swarmy-infra.md` and `~/api_keys.md` (laptop only): infrastructure facts and secrets.
