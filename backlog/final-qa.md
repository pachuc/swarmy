# Final QA: a newcomer brings up a second swarm

## What it is

The bring-up check that was item 5 of the dev-fleet proof task
(01M33VTRF36SH139438WZKZ4GH), moved here on 2026-09-27. A person who has
not seen the system before takes `docs/fleet-runbook.md` and
`docs/REMOTE.md`, nothing else, and brings up a second swarm from scratch:
AWS identity and local config, `make install`, `remote up` with a control
node and `add-node` for a sandbox node, connecting, importing credentials,
`doctor`, registering the image, and a first `fleet launch` to a pull
request. They keep a log of every point where they had to ask, guess, or
read code. Each entry in that log becomes a fix to one of the two documents
(or to an error message, when the document was right and the tool was
unclear). The check passes when a second run by another newcomer, or the
same person on a clean machine, needs no log entries.

## Why it matters

The runbook was written by the person who built the swarm, while building
it, and every step in it has only been followed by that person. The two
swarms so far (`dev` on 2026-09-23, `dev2` on 2026-09-24) were each brought
up with the author fixing things in code as they went, and the "Things
learned on the way" list in the runbook is the residue of that. Whether
the document is complete for someone else is unknown until someone else
tries. This is also the only test of the recovery section that does not
involve losing the real swarm.

## Why not now

The cleanup goal is about to change the build (dropping the provisioning
SDKs from the common path, cheaper profiles, the memory cap) and the
roadmap build-out changes the commands the runbook describes (routes,
cost views, automated bring-up). A newcomer run today would report gaps
that are already scheduled to close, and the runbook would need the same
pass again afterwards.

## Trigger

After the cleanup goal and the roadmap build-out land, before the runbook
is presented to anyone outside the project. Bring it back as a single
tasky task with the two documents as its inputs and a list of doc fixes
as its output.
