# Diagnostics on edit results and permission escalation for tools

Recorded 2026-09-21. Both were deliberately left out of the coding tool set
when it was built (see the tool decisions in the former `TODO.md`, section 7, at commit
493806f), and both are common in the reference agents.

## Diagnostics appended to edit results

After `edit` or `write`, some agents run the project's language server or
compiler and append the resulting errors and warnings for the changed file to
the tool result, so the model sees a type error in the same turn it made it
rather than several turns later when it runs the build.

Why it matters: fewer wasted turns on coding tasks and less chance the model
declares a change done that does not compile.

Why not now: it needs a language server per language installed in the image,
a way to keep it warm inside the sandbox between turns, and a decision about
which diagnostics to show (the file only, or the project). The bash tool with
`cargo check` or `tsc` gets most of the value when the model chooses to use
it, and the system prompt can ask it to.

What it would take: a `diagnostics` helper in the image that runs a
configured command per language on a changed file with a short timeout, the
edit tool calling it and appending a bounded block of output, and a per-agent
setting to turn it off for repositories where a check is slow.

## Permission escalation for tools

The reference agents ask a person before running commands they judge
dangerous (deleting files outside the project, pushing to a shared branch,
spending money), and remember the answer. Swarmy's tools run whatever the
model asks inside the sandbox, with the sandbox as the only boundary.

Why it matters: once agents have credentials that reach outside the sandbox
(the GitHub token already does; provider keys never will), a wrong command
has consequences outside the disk. A person will want a say before a force
push or a release.

Why not now: it needs the ask-the-user tool, which is deliberately deferred to
chaty ([chaty](chaty.md)) because asking a person is a message on a channel, and a
policy language for what counts as dangerous. Until then the GitHub token's
scope and branch protection on the repositories are the controls.

What it would take: a policy per agent (allow, deny, ask) matched against
tool name and arguments with a small set of built-in rules (git push to
protected branches, `gh release`, `rm` outside the working tree), an `ask`
outcome that posts a question on the agent's channel and parks the turn
until a member answers, and the answer remembered for the session or the
agent. The turn parking already exists for timers, so the mechanism is
available.

## When to pick it up

Diagnostics: when a coding-scenario measurement shows a meaningful share of
turns spent on errors a check would have caught immediately. Permissions:
with chaty, once the ask-the-user tool exists.
