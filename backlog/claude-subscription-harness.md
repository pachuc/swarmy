# Claude Code as a session harness on a Claude subscription

Recorded 2026-09-29. Decision at the time: not built. The approach works (every
piece below was run on the operator's laptop), but the policy position is only
defensible for the operator's own use, the economics depend on a billing
change Anthropic has paused rather than dropped, and the design adds a second
agent runtime next to swarmy's own loop. If it is ever built, build the
"allowlist" shape described here, for the operator only, off by default.

## What it is

Today swarmy's worker runs the agent loop itself and the gateway calls the
Anthropic Messages API with a Console API key, billed per token. This item is
a new kind of session in which the official Claude Code binary runs the loop
instead, signed in with the operator's own Claude Pro or Max subscription, so
inference draws from the subscription's usage limits rather than per-token
billing.

Some background on the pieces:

- **Claude Code** is Anthropic's coding agent command-line tool. Besides its
  interactive mode it has a headless mode, `claude -p`, which reads and writes
  a stream of JSON events (`--input-format stream-json --output-format
  stream-json`) and can resume a stored conversation with `--resume <id>`.
- **The Claude Agent SDK** is Anthropic's TypeScript and Python library for
  building agents on Claude Code. It launches the Claude Code binary and talks
  to it over that JSON stream.
- **ACP, the Agent Client Protocol,** is a JSON-RPC protocol over stdin and
  stdout, created by Zed, that lets an editor drive any coding agent. Claude
  Code does not speak it natively. Claude support comes from an adapter,
  `@agentclientprotocol/claude-agent-acp` (formerly Zed's
  `claude-code-acp`), a Node program built on the Agent SDK.
- **MCP, the Model Context Protocol,** is how Claude Code loads tools from
  outside programs. It is how swarmy would hand Claude Code its own tools.

### The allowlist shape

The assessed design keeps Claude Code out of the sandbox and keeps swarmy in
charge of every action the agent takes:

- Claude Code runs on a harness host next to the worker, not inside the
  agent's sandbox, so the subscription login never enters a sandbox the agent
  could read it from.
- All built-in tools that touch the local machine are switched off. The ones
  that do not (the todo list, subagents, web search, which runs on Anthropic's
  side) may stay. Swarmy's tools are served to it over HTTP MCP, and each call
  becomes an ordinary durable tool job in the agent's sandbox, with the same
  fencing, placement, managed processes, checkpoints, and timers as today.
  Every MCP call carries Claude's tool-use id in
  `_meta["claudecode/toolUseId"]`, which is the natural idempotency key for
  the tool completion row.
- Claude Code's own session files are the source of truth for the
  conversation and are stored byte for byte. Swarmy's event log is a one-way
  projection of them for display, streaming, and metering. Nothing is ever
  converted back into a Claude Code transcript. See "Storing sessions" below.

## What was verified

On 2026-09-29, with Claude Code 2.1.283, the ACP adapter 0.84.0, and a Max
login, using a handful of Haiku turns:

- The ACP adapter runs headless on an existing login. It advertises session
  load, resume, fork, list, close, and delete, offers no login method of its
  own, and reports token usage and an API-equivalent cost each turn.
- A transcript was moved out to a stand-in store, deleted, and restored. A
  fresh adapter process continued the conversation through `session/load`,
  which replays the history to the client, and through `session/resume`,
  which does not. The next turn read 24,240 tokens from the prompt cache.
- `claude --resume <id>` found a restored transcript under a different working
  directory, and even under an unrelated project directory.
- With `--tools "" --strict-mcp-config --mcp-config <file> --setting-sources
  "" --disable-slash-commands`, the model's only tool was the MCP stand-in for
  swarmy's `bash`. Asked to read a file on the local machine, it used the
  remote tool. The prompt was about 15,000 tokens.
- Without those flags the adapter loaded the operator's skills and claude.ai
  connectors (mail and drive) into the session. Isolation is not optional.
- Memory: about 250 MB per `claude -p` turn; about 560 to 580 MB for the
  adapter and its Claude child after one turn; 2.6 seconds to open a session.

## Storing sessions

The first question when this was assessed was whether swarmy should
convert Claude Code sessions into its own format and back. It should not
convert back.

- **Store the raw files.** Claude Code keeps each session as a JSON-lines
  transcript under `$CLAUDE_CONFIG_DIR/projects/<directory>/<id>.jsonl`, with
  subagent transcripts and large tool outputs beside it. Its documentation
  says the format is internal and changes between releases; the version
  number moved from 2.1.261 to 2.1.283 in eight days. Replies are split one
  content block per line, compaction starts a new chain, and the system
  prompt is recorded and replayed as-is on resume.
- **Mirror as it is written.** The Agent SDK has an official `sessionStore`
  option (alpha) that copies every transcript write to an external store in
  batches about every 100 ms, deduplicates entries by id, and restores the
  session into a temporary configuration directory on resume. It does not
  mirror file checkpoints or large tool-output files. It cannot be passed
  through the ACP adapter, because it is a callback. Without it, mirror by
  following the file from Claude Code hooks, which receive the transcript
  path.
- **Restore under the same conditions.** Same absolute working directory
  (the recorded system prompt keeps showing the old one until the next
  compaction), same organization login (thinking-block signatures are tied to
  it), and the same or a newer CLI version.
- **Cross between runtimes with a summary.** To move a conversation between
  Claude Code and swarmy's own loop, for example when the subscription limit
  runs out, use swarmy's existing compaction checkpoint and start a successor
  session. Claude Code accepts the checkpoint as its first user message. There
  is no supported way to import a foreign transcript, and unknown tool names
  have crashed resume in past releases.

## Why it matters

- Cost. A subscription is a flat monthly price, and the development fleet
  already runs on a subscription (ChatGPT, through the Codex login).
- Claude Code's loop, compaction, and subagents are tuned for Claude models.
- An ACP client in swarmy would also let it host other coding agents (Codex,
  Gemini CLI, OpenCode, Goose all speak ACP) as session kinds.

## Why not now

- **Policy is only partly on our side.** Claude Code's legal and compliance
  page (updated late August 2026) says its developer rules do not prevent "an
  end user from signing in to the unmodified Claude Code binary with their own
  Claude subscription, including where a platform hosts Claude Code". Flags
  such as `--tools` are not modifications. The same page forbids offering
  Claude login to other users, routing requests "through Free, Pro, or Max
  plan credentials on behalf of their users", and collecting, storing, or
  intermediating Claude credentials, so swarmy must never hold the login. The
  Consumer Terms still forbid access "through automated or non-human means"
  unless Anthropic explicitly permits it, and plan limits "assume ordinary,
  individual usage". The allowlist shape, where Claude Code carries only
  another product's tools, resembles what Anthropic's classifiers flag: in
  April 2026 they moved even genuine Claude Code traffic to paid extra usage
  over a file name associated with another harness. The observed risk is
  billing being rerouted, not bans.
- **The recorded terms decision.** `docs/providers.md` records on 2026-09-19
  that Anthropic subscription login is not built. This route has no swarmy
  OAuth client, but building it still means revising that decision
  explicitly.
- **The economics are fragile.** Each account has a five-hour and a weekly
  limit; on Max, Fable models may use at most half of the weekly limit. A
  swarm divides one account's allowance, which is already the fleet's
  bottleneck on ChatGPT. In May 2026 Anthropic announced that Agent SDK and
  `claude -p` usage would move to a small monthly credit and then API rates,
  and paused that on 2026-06-15, promising notice "before anything takes
  effect". If it returns, this design becomes API pricing with extra steps.
- **It is a second agent runtime, not a provider.** Claude Code owns the loop:
  model calls, retries, compaction, and subagents happen inside its process.
  A crash mid-turn loses the in-flight model call (completed tool calls stay
  durable in swarmy). Route failover, effort clamping, and swarmy's own
  compaction do not apply to these sessions. Claude Code's system prompt is
  written for its own tools, so with swarmy's tools most of what is gained is
  the loop and the billing.

## What it would take

About one goal of eight to twelve tasks.

1. Revise the terms decision in `docs/providers.md`, scoped to the operator's
   own login and the unmodified binary.
2. A spike on a remote node, not the laptop, comparing two drivers against
   the same swarmy MCP stand-in: a Rust client on the `agent-client-protocol`
   crate driving the ACP adapter, and Rust driving `claude -p` with
   stream-json directly. Only the stream-json output has been tried so far,
   not the input.
3. Choose the integration route:
   - **Claude only:** Rust drives `claude -p --input-format stream-json
     --output-format stream-json` directly, with `--tools`, `--mcp-config`,
     `--allowedTools mcp__swarmy__*`, and `--resume`. No Node and no adapter.
     This is the preferred route.
   - **Claude only, with the official session mirror:** a small TypeScript
     process on the Agent SDK, so `sessionStore` writes into swarmy's store.
   - **Several agent types:** the ACP adapter through the official Rust crate
     `agent-client-protocol` (2.2.0, 1.0 since June 2026, Apache-2.0, needs
     Rust 1.88, uses async-io rather than tokio). The protocol side is small;
     the crate's one-shot client example is 113 lines. The cost is the
     adapter: it is below 1.0 with 16 releases in September 2026; the
     allowlist settings go through its untyped `_meta.claudeCode.options` and
     must be re-sent on every load and resume; session-scoped stdio MCP
     servers do not reach the model, so swarmy's tools must be served over
     HTTP; and open issues include sessions dying with "Query closed before
     response received", orphaned `claude --resume` processes, and fork not
     working. It needs Node 22 and ships its own 243 MB `claude` binary, which
     it uses instead of the one on PATH unless `CLAUDE_CODE_EXECUTABLE` is
     set.
4. A `claude-code` session kind and a harness host service that starts Claude
   Code for a turn under the session lease and lets it exit when idle,
   resuming on the next turn.
5. Swarmy's tools as an HTTP MCP server feeding durable tool jobs, keyed by
   Claude's tool-use id.
6. Transcript mirroring and restore as described above, and a one-way
   projection of text, thinking, and usage into session events. Completions
   record zero cost and count against the subscription entry's quota.
7. Isolation per agent: its own `CLAUDE_CONFIG_DIR`, a pinned
   `CLAUDE_CODE_PROJECT_DIR_NAME`, `--setting-sources ""`,
   `--strict-mcp-config`, and `--disable-slash-commands`.
8. The operator signs in on the harness host through Claude Code's own flow
   (`claude auth login`, or `claude setup-token` for a one-year
   inference-only token). Swarmy never reads or stores the credential.
9. A concurrency cap per account, limit errors surfaced as retryable waits,
   and a fallback through the compaction checkpoint to the API-key route.
10. The Claude Code version pinned in the harness image and bumped on a
    schedule; old versions are rejected for new models.

## Alternatives considered

- **The Hermes relay** (assessed 2026-09-21): run `claude -p` per request and
  intercept its single upstream request through a local relay. It works, but
  a relay carrying another harness's prompts is what the classifiers look for,
  and the legal page's carve-out does not clearly cover it.
- **CLIProxyAPI** (`router-for-me/CLIProxyAPI`): not a sample of this design.
  It never runs the binary. It performs its own login with Claude Code's OAuth
  client id and rebuilds Claude Code's traffic (fingerprint headers, request
  signatures, TLS handshake, fake identifiers, renamed tools). That is what
  Anthropic prohibits and has blocked since January 2026. Better references
  are `agentclientprotocol/claude-agent-acp`, `openclaw/acpx`,
  `rynfar/meridian`, and OpenClaw's claude-cli backend.
- **Self-hosted environments** (Claude Team and Enterprise, beta since
  2026-08-06): the sanctioned way to run Claude Code sessions on your own
  machines on subscription usage. Anthropic's control plane stores sessions
  and transcripts, which removes the storage problem entirely; a
  `spawn-runner` hook would let swarmy provide a sandbox per session; sessions
  are created with `claude -p --environment ccpool_...` and continued with
  `--cloud <id>`. It needs a Team plan, swarmy becomes only the compute
  provider, and the login that creates sessions must be refreshed every 30
  days.
- **Routines** (Pro and Max): an API trigger starts a Claude Code session,
  but it runs on Anthropic's infrastructure with a daily run cap. Not a swarm
  substrate.
- **The status quo:** swarmy's own loop on a Console API key, which keeps
  every swarmy guarantee and is the supported path.

## When to pick it up

Any of: Anthropic explicitly allows self-hosted multi-agent use of one's own
subscription, or resolves the paused Agent SDK billing change in a way that
keeps plan limits; Claude API spend becomes the fleet's dominant cost; swarmy
wants to host other coding agents as session kinds (then take the ACP route);
or the operator moves to a Team plan (then evaluate self-hosted environments
first).

## Related

- `docs/providers.md`, "Recorded terms decisions".
- Claude Code legal and compliance:
  https://code.claude.com/docs/en/legal-and-compliance
- Agent SDK overview: https://code.claude.com/docs/en/agent-sdk/overview
- Agent SDK usage on Claude plans:
  https://support.claude.com/en/articles/15036540
- Self-hosted environments:
  https://code.claude.com/docs/en/self-hosted-environments
- ACP adapter: https://github.com/agentclientprotocol/claude-agent-acp
- ACP Rust crate: https://github.com/agentclientprotocol/rust-sdk
