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
Codex-compatible JSON credentials, and an HTTP Responses SSE client. Dependencies
are reqwest with default features disabled and rustls enabled, tokio, futures,
async-stream, and base64, plus the workspace's existing types and serialization
crates. It has no keyring, proxy discovery customization, identity management,
FoundationDB, or NATS dependency. **Decision: use this minimal port.**
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
the caller. The request carries the caller's model id unchanged; the provider
never substitutes another model.

The parser follows the pinned [SSE client](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/codex-api/src/sse/responses.rs).
It handles arbitrary UTF-8 chunk boundaries, LF/CRLF/CR, comments, multiline data,
text, refusal text, function argument fragments, reasoning summaries, complete
items, usage, failed responses, and incomplete responses. It ignores unknown
notification events but rejects unsupported completed output items rather than
silently losing output. EOF before a terminal response is an error. Individual
SSE events are limited to 8 MiB. Partial indices refer to Responses output items;
`PartDone` contains completed parts, and `Completed` contains authoritative output.

## Credential ownership

ChatGPT credentials live in the cluster credential store, encrypted with the
cluster keyring, as one labelled entry per account (`default` unless
`--label` names another). Each entry is an OAuth record: the access token, the
refresh token, an expiry taken from the access token's `exp` claim (or eight
days after the last refresh when it has none), and two extra fields. The
`account_id` field holds the ChatGPT account. The `chatgpt_json` field holds the
rest of the Codex-format JSON with both tokens removed, so unknown root and token
fields survive a later refresh or export. Credentials that carry an OpenAI API
key, or whose `auth_mode` is not `chatgpt`, are rejected.

Gateways resolve credentials through `swarmy_llm::auth::Resolver`, backed by
the cluster store. They never read a credential file. Resolution picks a ready
entry whose rate-limit breaker is closed. For ChatGPT it returns a live
credential source instead of copied tokens: every inference call reloads the
entry from the store, and the source remembers the first account id it saw. If
the stored account later changes, calls fail with an account-changed error
instead of silently using the other account.

A token is refreshed when the access token expires within five minutes, when it
has no expiry and the last refresh is at least eight days old, or when the
backend answers 401. After a 401 the call refreshes once and retries once.
Refresh itself is never retried automatically: an uncertain reply may already
have rotated the refresh chain.

Refresh is coordinated through the store, so it works across gateways on
different hosts. The refreshing gateway first re-reads the entry; if it no
longer matches the credentials the call observed, another gateway already
refreshed it and the new record is used. Otherwise the gateway takes a
45-second lease on that entry in FoundationDB, reads the entry again under the
lease, and only then exchanges the refresh token. Other labels keep their own
leases. A refresh reply for a different account is rejected. A failed
exchange marks the entry `needs_login`, and the provider reports that it needs
a new login; run `swarmy auth login chatgpt` again for that entry.

Each account must have exactly one refresh owner. Codex, another swarmy
cluster, or a copied credential file refreshing the same chain will revoke it.
Import a Codex file only after stopping the Codex session that owns it.

## CLI and manual validation

Log in with a dedicated account, or import a Codex credential file:

```sh
swarmy auth login chatgpt
swarmy auth login chatgpt --label second-account
swarmy auth import --file /path/to/codex/auth.json
swarmy auth check chatgpt
```

`swarmy auth login` and `swarmy auth import` run the `swarmy-auth` helper
(installed by `make install-client`), which keeps the terminal OAuth flow out
of the main binary. Login runs the device-code flow above and prints the
verification URL and code; with `--json` it prints a `device_code` event and
then a `saved` event. Import reads the file without changing it, checks it,
and prints `imported`. Both upload the record to the cluster through the API
and never print tokens. Without `--file`, import reads
`[selection] credential_file` (normally `~/.swarmy/auth.json`; `--auth-file` or
`SWARMY_CHATGPT_AUTH` override it).

The ignored live test reads a Codex-format credential file directly and never
refreshes it, so the access token in that file must still be valid. Use a
file from a dedicated login that nothing else refreshes, never a personal Codex
or launcher credential file. Select the model ids to probe from the account's
model availability information and run:

```sh
SWARMY_CHATGPT_AUTH=/tmp/swarmy-dedicated/auth.json \
SWARMY_CHATGPT_MODELS=model-id-one,model-id-two \
SWARMY_CHATGPT_REPORT=/tmp/swarmy-chatgpt-models.json \
cargo test -p swarmy-llm --locked --test live -- --ignored
```

It sends a short prompt to each selected model and writes a timestamped report of
successes and failures. The report shows only whether the tested models were
available at that time; it does not list the account's whole catalog. Without
`SWARMY_CHATGPT_AUTH` the test skips cleanly.

## Request headers and reasoning replay

Every request carries the bearer access token, the chatgpt-account-id header, and the same client identification headers Codex sends: OpenAI-Beta set to responses=experimental, originator set to codex_cli_rs, and a matching User-Agent. The backend uses these to decide which features a request may use; a bare request without them has not been verified against the live service.

Reasoning parts are replayed into a request only when they carry the reasoning item this backend returned, stored under the chatgpt key of the part metadata. Reasoning produced by another provider, such as the fake provider in tests, is skipped rather than rejected, so a session that switches providers keeps working.

The live backend answers with a full event stream but sends no Content-Type header. The provider therefore rejects only an explicit non-stream content type, such as an HTML block page, and otherwise lets the event parser decide whether the body is valid.
