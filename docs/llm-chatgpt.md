# ChatGPT subscription provider

`swarmy-llm` exposes one object-safe `Provider` contract. Requests use
`swarmy-core::Message` and `Part`; responses retain those parts and token usage.
A successful stream ends in `Delta::Completed`. Errors end the stream without a
completed response. Dropping a stream cancels its HTTP request. The fake provider
assigns zero-based turns at the call to `request`, counts even calls whose streams
are never polled, and has separate response and tool-call scripts.

## Dependency versus port spike

Reference: OpenAI Codex tag `rust-v0.153.4`, commit
`3d2ee51ca2d5db578f328aa75e20aa22c0197c9a`. Source was read from
`raw.githubusercontent.com`; no upstream source was vendored.

The dependency option was evaluated by inspecting the pinned manifests and
public interfaces. The candidate dependencies would be `codex-login` and
`codex-core` from `https://github.com/openai/codex.git` at that revision.
[The login manifest](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/login/Cargo.toml)
pulls in keyring storage, agent identity, workload identity, configuration,
telemetry, browser launch, and a local HTTP server.
[The core manifest](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/core/Cargo.toml)
also brings the agent runtime, tools, shell execution, MCP, and many unrelated
workspace crates. Login persistence accepts Codex storage and keyring settings,
so depending on it would not remove the need for swarmy's credential-store rules.
[The HTTP client manifest](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/http-client/Cargo.toml)
includes native TLS and platform proxy support. That conflicts with the requested
rustls-only transport. This option was rejected at source inspection; the full
Codex workspace was not built.

The port option was implemented and exercised against JSON/SSE fixtures and
wiremock servers. It keeps device-code login, the token exchange and refresh,
Codex-compatible JSON storage, and an HTTP Responses SSE client. Dependencies are
reqwest with default features disabled and rustls enabled, tokio, futures,
async-stream, base64, fs2, and tempfile, plus the workspace's existing types and
serialization crates. It has no keyring, proxy discovery customization, identity
management, FoundationDB, or NATS dependency. **Decision: use this minimal port.**
Upstream changes require reviewing these protocol boundaries and updating the
fixtures deliberately.

## Verified wire protocol

[Device login](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/login/src/device_code_auth.rs)
posts the Codex client id to
`https://auth.openai.com/api/accounts/deviceauth/usercode`. It displays
`https://auth.openai.com/codex/device` and the short user code, then polls
`/api/accounts/deviceauth/token`. HTTP 403 and 404 mean pending. The flow times
out after 15 minutes. The successful reply supplies an authorization code and
PKCE verifier. The form-encoded exchange uses `/oauth/token`, grant type
`authorization_code`, and redirect URI `https://auth.openai.com/deviceauth/callback`.
See the pinned [token exchange](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/login/src/server.rs).

[Refresh](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/login/src/auth/manager.rs)
posts JSON to `https://auth.openai.com/oauth/token` with grant type
`refresh_token`, the current refresh token, and client id
`app_EMoamEEZ73f0CkXaXp7hrann`. Optional returned id and refresh tokens replace
existing values only when supplied. The account comes from the id token's
`https://api.openai.com/auth.chatgpt_account_id` claim. JWT payload decoding is
used for account consistency and expiration scheduling, not signature
verification; credentials are accepted only from the HTTPS issuer or an
operator-selected file.

The [provider configuration](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/model-provider-info/src/lib.rs)
selects `https://chatgpt.com/backend-api/codex`; inference is `POST /responses`
with `Authorization: Bearer <access_token>` and `chatgpt-account-id`, as
implemented in the pinned [bearer auth provider](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/model-provider/src/bearer_auth_provider.rs).
The [request shape](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/codex-api/src/common.rs)
uses ordered input items, `stream: true`, and `store: false`.
Function arguments are JSON strings on the wire. Encrypted reasoning is requested
and the complete reasoning item is preserved under `Part::Reasoning.metadata`'s
`chatgpt` key for replay. Optional generation settings are only sent when supplied;
model and backend support for temperature and output limits must be checked by
the caller. There is no model catalog or automatic model substitution in this task.

The parser follows the pinned [SSE client](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/codex-api/src/sse/responses.rs).
It handles arbitrary UTF-8 chunk boundaries, LF/CRLF/CR, comments, multiline data,
text, refusal text, function argument fragments, reasoning summaries, complete
items, usage, failed responses, and incomplete responses. It ignores unknown
notification events but rejects unsupported completed output items rather than
silently losing output. EOF before a terminal response is an error. Individual
SSE events are limited to 8 MiB. Partial indices refer to Responses output items;
`PartDone` contains completed parts, and `Completed` contains authoritative output.

## Credential ownership

The sandbox operating rules supplied with this task are binding: copies share
one refresh chain, concurrent refreshers can revoke it, and an account must not
change under a credential file. No codex-daytona project checkout was available
in the sandbox; launcher control directories and credential caches were not read.

Use one authoritative credential file per account. All workers using that account
must use the same store. Every inference call loads the current access token from
that store. Tokens are refreshed when the access JWT expires within a minute,
a token without an expiration has not refreshed for eight days, or the backend
returns 401. Inference retries a 401 once; refresh HTTP requests are never retried
automatically inside the refresh operation because an uncertain reply may have
already rotated the chain. If refresh fails or persistence fails after a successful
exchange, resolve the credentials before retrying work; a new dedicated login may
be necessary.

A process-wide async mutex serializes refresh per account. A file lock named by
the account hash in the credential directory also serializes cooperating local
processes. After acquiring both locks, refresh reloads the file and compares the
observed credentials; a waiting caller returns the already updated tokens. Locks
remain held through persistence. File locks are advisory: they do not coordinate
with Codex or independent credential copies. Multiple hosts require a shared
single refresh owner and an appropriate `CredentialStore` implementation; copying
files to each host does not establish coordination.

Writes use a separate file lock for account validation, a same-directory temporary
file with mode 0600, file sync, atomic rename, and directory sync. New directories
use mode 0700 on Unix. Existing account ids and id-token claims must agree; writes
cannot switch accounts. A live store also detects external account replacement.
Import and refresh preserve unknown root and token fields. The fixture round trip
compares **parsed JSON values**: whitespace and key order can change. The source
file remains byte-for-byte unchanged. Neither the library nor CLI accepts an
OpenAI API key for this provider.

## CLI and manual validation

Create a dedicated login:

```sh
cargo run -p swarmy-cli -- auth --auth-file /tmp/swarmy-dedicated/auth.json login
```

The default destination is `$HOME/.swarmy/auth.json`; `SWARMY_CHATGPT_AUTH` or
`--auth-file` overrides it. `--json` emits a device-code event before polling and
a credentials-saved event on success. Tokens are never printed. Import is explicit:

```sh
cargo run -p swarmy-cli -- auth --auth-file /path/to/swarmy/auth.json import /path/to/codex/auth.json
```

Stop the original refresh owner before using an imported chain. Import copies
credentials; it does not create an independent session. Prefer a dedicated login
when the original Codex session will continue running.

The ignored live test must only use a dedicated login, never a personal Codex or
launcher credential file. Select the model ids to probe from the account's model
availability information and run:

```sh
SWARMY_CHATGPT_AUTH=/tmp/swarmy-dedicated/auth.json \
SWARMY_CHATGPT_MODELS=model-id-one,model-id-two \
SWARMY_CHATGPT_REPORT=/tmp/swarmy-chatgpt-models.json \
cargo test -p swarmy-llm --locked --test live -- --ignored
```

It sends a short prompt to each selected model and writes a timestamped report of
successes and failures. The report establishes only the tested models' availability
at that time. It does not enumerate the account's entire catalog. Without the auth
environment variable it skips cleanly. This test was intentionally not run in the
sandbox, so no real account model availability is claimed.
