# Provider catalog

`swarmy-llm::catalog::Catalog::get()` exposes a generated, embedded snapshot of
provider and model metadata. Rust builds and lookups need no network access.
`providers()`, `provider(id)`, and `model(provider, id)` perform exact lookups;
`find(pattern)` matches a case-insensitive substring of `provider/model` and
returns provider/model pairs in stable order.

The table describes the intended provider support. This catalog task wires only
the existing ChatGPT client into `client_for`. Other protocol arms, including
the fake placeholder, return `Error::Unsupported`; the existing scripted fake
provider remains available through its current constructor. Catalog membership
does not mean a protocol client or login command has been implemented.

| Provider id | Wire protocol | Auth | Environment variables and notes |
|---|---|---|---|
| `anthropic` | Anthropic Messages | API key | `ANTHROPIC_API_KEY`. Subscription OAuth is prohibited by Anthropic's terms for third-party harnesses and is not built. |
| `openai` | OpenAI Responses | API key | `OPENAI_API_KEY`; platform API at api.openai.com. |
| `chatgpt` | Responses on the Codex backend | Codex OAuth device login | Credential store; no API key environment variable. Existing flow, documented as tolerated by OpenAI, not licensed. |
| `xai` | OpenAI Responses | API key | `XAI_API_KEY`; api.x.ai. |
| `meta` | OpenAI Responses | API key | `META_MODEL_API_KEY`; Muse Spark at api.meta.ai. |
| `openrouter` | Chat Completions; Anthropic Messages for `anthropic/*` | API key or PKCE login that mints a key | `OPENROUTER_API_KEY`. All live models advertising tools are included. |
| `azure` | OpenAI Responses | API key or Azure CLI Entra token | `AZURE_API_KEY`, `AZURE_OPENAI_API_KEY`; configure the endpoint with `AZURE_RESOURCE_NAME` or `AZURE_OPENAI_BASE_URL`. Models name deployments. |
| `amazon-bedrock` | Bedrock Converse stream | AWS credential chain or bearer token | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`, `AWS_REGION`, `AWS_BEARER_TOKEN_BEDROCK`; the future client uses the official AWS Rust SDK for SigV4. |
| `google` | Gemini generateContent | API key | `GOOGLE_API_KEY`, `GOOGLE_GENERATIVE_AI_API_KEY`, `GEMINI_API_KEY`. |
| `google-vertex` | Gemini generateContent on Vertex | Application Default Credentials or service account key | `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_CLOUD_PROJECT`, `GOOGLE_CLOUD_LOCATION`; project/location aliases `GOOGLE_VERTEX_PROJECT`, `GOOGLE_VERTEX_LOCATION`. |
| `google-vertex-anthropic` | Anthropic Messages on Vertex | Same as `google-vertex` | Same environment variables as `google-vertex`. |
| `fake` | Scripted | None | No environment variables; scripts supply the models. |

`env_keys` includes endpoint and cloud configuration variables as well as key
variables. It is discovery metadata, not a rule to treat every value as a secret
or an HTTP authorization header. Empty cloud base URLs require configuration.
New clients identify themselves with `User-Agent: swarmy/<version>`.

## Regeneration

Run from the checkout with Python 3 (standard library only):

```sh
make models
# Equivalent:
python3 scripts/models/generate.py
python3 scripts/models/generate.py --check
python3 -m unittest discover -s scripts/models -v
```

The generator downloads [models.dev](https://models.dev/api.json) and
[OpenRouter's live catalog](https://openrouter.ai/api/v1/models), keeps the
provider allowlist and only models that support tool calling, and writes
`crates/swarmy-llm/catalog/<provider>.json`. OpenRouter's live list replaces its
models.dev entries. Azure retains its tool-capable models.dev entries and gains
every OpenAI model, with OpenAI metadata taking precedence on matching ids and
Azure compatibility flags applied. Its base URL stays empty for deployment
configuration.

ChatGPT's six models are hand-listed in the generator using Pi's Codex limits:
`gpt-5.5`, `gpt-5.3-codex-spark`, `gpt-5.6-sol`, `gpt-5.6-terra`,
`gpt-5.6-luna`, and `gpt-6-astra`. Their token costs are zero because subscription
billing is not a per-token API charge. The fake catalog has no fixed models.

Review and commit the JSON with generator changes. `manifest.json` records the
UTC generation time and SHA-256 of the exact downloaded bytes for both sources.
The timestamp is retained when those hashes are unchanged, so repeated runs on
the same sources are byte-for-byte idempotent. `--check` downloads both sources,
regenerates in a temporary directory, and exits nonzero for changed, missing, or
extra JSON files without modifying the checkout. Upstream source changes also
cause a check failure and require a reviewed refresh. Download or parse failures
do not silently fall back to stale data.

## Metadata and reasoning

Provider files contain `ProviderInfo` and a map of `ModelInfo` by id. A model can
override `api` and `base_url`, as OpenRouter's Anthropic entries do. Costs are US
dollars per million tokens, including cache reads and writes and context pricing
tiers. OpenRouter per-token prices are converted using decimal arithmetic.
OpenRouter routing aliases with negative sentinel prices use zero placeholders
and `compat.dynamic_pricing = true`; these require the resolved model price for
actual billing and must not be treated as free. Missing cache prices default to
zero; missing output limits remain null rather
than claiming a known limit. Context tiers apply above `input_tokens_above`.

The reasoning scale is `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`.
Explicit effort lists restrict support to the listed levels. When a source
provides several reasoning mechanisms, the generator prefers efforts, then a
budget range, then a toggle. Budgets and toggles expose the ordinary scale from
`none` through `high`; `xhigh` and `max` require explicit effort entries.
OpenRouter's effort metadata permits `none` unless reasoning is mandatory.
`clamp_effort` keeps a supported request, otherwise selects the next higher
supported level, otherwise the next lower; it returns the level and whether it
changed. Models without reasoning always clamp to `none`.

`compat` is an open map with typed accessors. It records token-limit fields,
developer-role and strict-tool support, thinking and cache-control formats,
adaptive Anthropic thinking, temperature restrictions, and long cache retention.
Unknown keys survive parsing. The quirk table is based on Pi's
`packages/ai/scripts/generate-models.ts` and OpenCode's provider option tables;
it contains metadata only, with no JavaScript runtime dependency.

Custom model configuration and additional protocol clients belong to separate tasks.

## Credentials

Run `swarmy dev up` to create the local keyring and development stack. The
credential commands use your configured FoundationDB directory (including
`--remote NAME`) and the local cluster keyring. All auth commands accept
`--json`; list and check emit one JSON object per provider, with no secrets.

```sh
swarmy auth set anthropic --api-key sk-example
swarmy auth set openai --from-env
swarmy auth set azure --file /private/api-key --extra resource_name=my-resource
swarmy auth set google --from-env --extra project=my-project --extra region=us-central1
swarmy auth ls --json
swarmy auth check
swarmy auth check chatgpt --json
swarmy auth rm anthropic
```

Choose exactly one of `--api-key`, `--from-env`, or `--file`. A key file is UTF-8
text with surrounding whitespace removed. Repeat `--extra name=value` for
resource names, deployments, projects, regions, bearer tokens, or other
provider settings. Extra values are encrypted too. No shell commands are
executed to resolve values. `--file` avoids putting a key in shell history or
process arguments. `auth check` verifies decryption and local status; it does
not make a provider request. Missing providers or expired/needs-login records
exit nonzero. OAuth output includes signed seconds until expiry.

| Provider | `--from-env` lookup order |
| --- | --- |
| anthropic | `ANTHROPIC_API_KEY` |
| openai | `OPENAI_API_KEY` |
| xai | `XAI_API_KEY` |
| meta | `META_MODEL_API_KEY` |
| openrouter | `OPENROUTER_API_KEY` |
| azure | `AZURE_API_KEY`, `AZURE_OPENAI_API_KEY` |
| amazon-bedrock | `AWS_BEARER_TOKEN_BEDROCK` |
| google | `GEMINI_API_KEY`, `GOOGLE_API_KEY` |
| google-vertex, google-vertex-anthropic | `GOOGLE_CLOUD_API_KEY` |

### Interactive logins

All three logins write directly to the encrypted cluster credential store.
They require a running cluster and its keyring on the CLI host.

```sh
swarmy auth login chatgpt
swarmy auth login openrouter
az login
swarmy auth login azure --resource my-resource
swarmy auth login azure --resource my-resource --scope https://cognitiveservices.azure.com/.default
swarmy auth set azure --api-key KEY --extra resource_name=my-resource
```

ChatGPT prints a device URL and short code, polls for approval, and stores the
access token, refresh token, expiry, and account id as OAuth credentials. The
existing device flow is tolerated by OpenAI rather than licensed. No new product
identity headers are sent: inference identifies itself as `swarmy/<version>`.
The existing device flow retains its existing OAuth client id as the exception
specified by the provider plan.

OpenRouter prints an authorization URL and attempts to open the browser. Choose
browser callback to receive the code on an ephemeral `127.0.0.1` port, or pasted
code for headless use. The callback listener is opened before the URL is shown.
The login generates a random 32-byte PKCE verifier and S256 challenge, exchanges
the code and verifier, and stores the returned API key. This is OpenRouter's
[documented public flow](https://openrouter.ai/docs/guides/overview/auth/oauth);
it has no client id and needs no refresh token.

Azure runs `az account get-access-token --scope URL --output json` on the login
host. The default scope is `https://cognitiveservices.azure.com/.default`. The
OAuth record has an empty refresh token and retains `resource_name` and `scope`.
Refresh runs the same command on the gateway host, which must have Azure CLI
installed and signed in. Missing `az` or a failed token request reports
`NeedsLogin`; rerun login or set an API key. The parser accepts Azure's local
`expiresOn` timestamp and prefers the unambiguous `expires_on` epoch when the CLI
supplies both, as described in the
[Azure CLI authentication documentation](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli).

Anthropic, GitHub Copilot, Kimi, and xAI subscription logins are not offered under
the project's terms policy: their terms prohibit third-party clients or require
an authorized integration that swarmy does not have. Use permitted API keys.

### Credential resolution

The gateway uses `swarmy_llm::auth::Resolver::resolve(provider)` (also exposed as
`auth::resolve(provider, &resolver)`). The explicit resolver context holds the
cluster store; it avoids process-wide database globals. Resolution order is:

1. Read the provider's cluster record. Refresh OAuth credentials with less than
   five minutes remaining through `refresh_with_lease`. Competing gateways use
   the winner's record. A failed refresh marks the record `needs_login` and never
   falls back to environment variables.
2. When no record exists, inspect the provider's catalog key variables:
   `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `XAI_API_KEY`, `META_MODEL_API_KEY`,
   `OPENROUTER_API_KEY`, `AZURE_API_KEY` (with `AZURE_RESOURCE_NAME`), and
   `GEMINI_API_KEY`. Azure also accepts `AZURE_OPENAI_API_KEY`; Google also accepts
   `GOOGLE_API_KEY` and `GOOGLE_GENERATIVE_AI_API_KEY`. Bedrock accepts
   `AWS_BEARER_TOKEN_BEDROCK`. Endpoint, profile, and project settings are not
   mistaken for API keys.
3. For `amazon-bedrock`, `google-vertex`, and `google-vertex-anthropic`, return
   `Ambient` so their protocol clients can use the host SDK credential chain.
   Fake inference needs no credential. Other missing credentials report
   `NeedsLogin`.

Azure key and bearer credentials retain their resource metadata for protocol
client construction. ChatGPT reads the current stored access token for every
request and uses the database lease for refresh, including its single retry
after an unauthorized response. `credential_file` has no gateway role.

### ChatGPT migration

```sh
swarmy auth import
# Or read a particular existing ChatGPT auth.json:
swarmy auth import --file /private/auth.json
```

Import reads `credential_file` (normally `~/.swarmy/auth.json`), overridden by
`--auth-file` / `SWARMY_CHATGPT_AUTH` or `auth import --file`. It validates the
file, preserves provider metadata, and stores an OAuth record under `chatgpt`
without modifying the source. Stop any other process refreshing that account
before importing it. Login writes the store directly, so it needs no subsequent
import. The gateway never falls back to the credential file.

### Keyring and remote gateways

The keyring is a base64 32-byte key with mode 600. Override its path with
`SWARMY_KEYRING`. `swarmy dev up` creates it atomically only when absent, and
`swarmy doctor` reports its presence and mode. Keep a separate backup: a new
key cannot decrypt old records. Gateways fail clearly when stored credentials
cannot be decrypted.

```sh
swarmy remote up demo --services node --copy-credential
swarmy remote add-node demo --copy-credential
```

The same explicit flag copies both the legacy ChatGPT file and the keyring
over SSH. The keyring lands at `/home/ubuntu/.swarmy/keyring` with mode 600.
Without this flag, additional nodes run only `swarmyd` and receive neither
secret. Ordinary checkout copying excludes the configured credential and
keyring paths. Use the same cluster key on the CLI and every gateway.

## Choosing a model

New ephemeral sessions accept provider, model, and reasoning effort overrides:

```sh
swarmy run 'Review this repository' --model openai/gpt-5.5 --effort max
swarmy chat --provider openai --model gpt-5.5 --effort high
swarmy agent create tommy --provider openrouter --model anthropic/claude-sonnet-4-6
swarmy agent set tommy --effort medium
swarmy agent set tommy --model default
```

A bare model id uses the selected provider, or the stack provider when omitted.
The `provider/model` shorthand splits at the first slash. An explicit provider
must agree with that prefix, except when the full model id exists under that
provider, as with OpenRouter's `anthropic/claude-sonnet-4.6`.
OpenRouter accepts dashed version aliases such as `anthropic/claude-sonnet-4-6`
and stores the matching canonical catalog id `anthropic/claude-sonnet-4.6`.
Unknown selections show up to five catalog suggestions. The CLI currently uses
the embedded catalog; custom catalog validation will arrive with settings overlays.

Effort accepts `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, and `max`.
At request time the worker clamps effort to the model's supported scale and
records the first clamp in the session log. Session overrides take precedence
over agent overrides, which take precedence over stack defaults. Overrides remain
in the session across later turns. Agent updates affect subsequent requests.
`agent set --provider default`, `--model default`, and `--effort default` clear
individual overrides. A cleared field inherits the stack setting independently;
choose a compatible provider and model together when changing providers.

Inference flags apply only to a new ephemeral session. With `--agent`, configure
the named agent instead. Resuming a session id uses its stored selection.
`session show` marks inherited values, `session ls` includes `provider/model`,
and the chat status header shows provider, model, and effort. `agent show` marks
unset agent fields as stack defaults. With `--json`, `session show` emits a
`session_selection` record containing stored overrides and resolved values before
the event rows.

Gateways advertise availability in an expiring `("gateway_provider", provider)`
record, refreshed every 30 seconds. The store exposes `put_gateway_provider` with
`GatewayProvider { expires_at }` and `gateway_serves`. The parallel gateway task
owns startup and refresh calls. A missing advertisement produces
`no gateway serves provider X; run swarmy auth set X or start a gateway with it`
in the session log. The scripted `fake` provider needs no credential advertisement.
