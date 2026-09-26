# Backlog

Work that is understood well enough to write down but is not worth the lift at
this phase of the project. Nothing here is planned; the plan lives in tasky
(`tasky --json goal list --project swarmy`). Each item has its own file with
what it is, why it matters, why it is not being done now, what it would take,
and the trigger that should bring it back onto the plan.

When an item is picked up, turn it into a tasky goal with the file's content as
the starting spec and delete the file. When an item stops being worth keeping,
delete the file and say why in the commit.

| Item | One line |
|---|---|
| [chaty](chaty.md) | Chat between agents and people as a standalone tool integrated into swarmy (formerly slice 5, channels) |
| [gvisor-runtime](gvisor-runtime.md) | Stronger isolation and process checkpointing by swapping runc for gVisor; sandbox pause and resume (former slice 6) lives here as something to explore |
| [kubernetes-packaging](kubernetes-packaging.md) | Helm charts and operators as a second deployment target, and the path to GCP and Azure |
| [control-plane-high-availability](control-plane-high-availability.md) | A swarm that survives the loss of its control instance |
| [agent-disk-guidance](agent-disk-guidance.md) | Tell agents which paths are durable and which are scratch, generated from the image |
| [agent-fork](agent-fork.md) | Fork an agent, including its computer, into a new agent |
| [transcript-retrieval](transcript-retrieval.md) | Search over archived conversation history instead of only the summary chain |
| [cost-view](cost-view.md) | Cost per agent across tokens, compute hours, and storage |
| [tool-diagnostics-and-permissions](tool-diagnostics-and-permissions.md) | Diagnostics appended to edit results and permission escalation for dangerous tools |
| [gui-client](gui-client.md) | Desktop and mobile clients with chat, channels, and direct messages to agents |
| [documentation-site](documentation-site.md) | A public documentation site from the prototype outside this repository |
| [build-time](build-time.md) | Twenty-minute cold builds in workers and benchmarks: drop the AWS SDK from the common path, shared sccache, warm images, cheaper profiles |
| [housekeeping](housekeeping.md) | Stale design sections, an issue that cannot be closed, and other small chores |
