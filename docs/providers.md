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

## Browsing models

```sh
swarmy models ls --provider anthropic
swarmy models ls --reasoning --json
swarmy models show openrouter/anthropic/claude-sonnet-4.6
swarmy models search sonnet
swarmy models providers
```

These commands use the embedded snapshot plus your configuration and need no
network or credentials. Lists sort by provider id, then model id. `--reasoning`
keeps models with at least one supported effort other than `none`. Search uses
`Catalog::find`, a case-insensitive substring match against `provider/model`;
no matches exit nonzero. Show splits at the first slash, so model ids can contain
slashes, and prints all metadata, including compatibility flags.

The terminal list uses several short lines per model, with context and output
limits in tokens, input and output prices in dollars per million tokens, and
supported efforts. Unknown output limits display as `unknown`. Provider output
lists wire protocols, authentication kinds, and environment variable names.
The credential column currently reports `unknown`; use `swarmy auth ls` to
inspect stored credentials.

Use the stable `--json` form for scripts. List and search return a JSON array
of model objects; show returns one object. Each includes `key` (`provider/model`),
`provider`, every `ModelInfo` field, `supported_efforts`, `effective_api`, and
`effective_base_url`. The effective fields resolve model overrides against the
provider defaults. Providers returns an array with `id`, `api`, `auth_kinds`,
`env_keys`, and `credential`. JSON output never truncates identifiers.

## Custom providers and models

Add entries to `.swarmy/config.toml` (or the user configuration file discovered
by swarmy). A private OpenAI-compatible endpoint needs no snapshot regeneration:

```toml
[providers.private]
api = "OpenAiCompletions"
base_url = "http://localhost:8000/v1"

[[models]]
provider = "private"
id = "team/coder"
name = "Private coder"
context_window = 65536
max_output_tokens = 8192
reasoning = ["none", "low", "medium", "high"]
cost = { input = 0.5, output = 1.5, cache_read = 0.1, cache_write = 0.5 }
compat = { max_tokens_field = "max_tokens", supports_developer_role = false }

[[models]]
provider = "openai"
id = "gpt-5.5"
context_window = 128000
```

Provider tables accept `api` and `base_url`. Both are required for a new provider;
an existing provider retains omitted settings, models, auth kinds, and environment
names. New providers use their id as their name, advertise API-key auth, and have
no implicit environment variable names. Catalog definitions do not implement a
wire client or credential flow; dispatch support still depends on the protocol
tasks described above.

Each model requires `provider` and `id`. Optional fields are `name`, `api`,
`base_url`, `context_window`, `max_output_tokens`, `reasoning`, `cost`, and `compat`.
New models default to their id as name, text input, tool calling enabled, no
attachments or reasoning, 128000 context tokens, 16384 output tokens, and zero
price placeholders. Supply actual limits and prices for your endpoint. New
models inherit the provider's API and base URL unless overridden.

An existing `provider`/`id` entry retains omitted snapshot fields, including any
model-specific API or base URL. Explicit fields override them. Multiple entries
for the same model apply in file order. Compatibility keys merge individually;
`cost` replaces the complete price object, requiring `input` and `output` and
defaulting omitted cache prices and tiers to zero and empty. Costs support the
same `tiers` schema described above. `reasoning` replaces the effort list; an
empty list disables reasoning. The allowed efforts are `none`, `minimal`, `low`,
`medium`, `high`, `xhigh`, and `max`.

API strings use these exact names: `AnthropicMessages`, `OpenAiResponses`,
`OpenAiCodexResponses`, `OpenAiCompletions`, `GoogleGenerativeAi`, `GoogleVertex`,
`BedrockConverse`, and `Fake`. An unknown API fails configuration loading and
reports the allowed names. A model referring to an undeclared provider also
fails loading.

`Settings::catalog()` returns the merged, owned catalog without changing the
embedded snapshot. Selection flags, and later worker and gateway catalog
lookups, should call this method instead of `Catalog::get()`. This change does
not alter session selection or gateway dispatch.

## Credentials

Run `swarmy dev up` to create the local keyring and development stack. The
credential commands use your configured FoundationDB directory (including
`--remote NAME`) and the local cluster keyring. All auth commands accept
`--json`; list and check emit one JSON object per provider, with no secrets.

```sh
swarmy auth set anthropic --api-key sk-example
swarmy auth set openai --from-env
swarmy auth set azure --file /private/api-key --extra resource=my-resource
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
| azure | `AZURE_OPENAI_API_KEY` |
| amazon-bedrock | `AWS_BEARER_TOKEN_BEDROCK` |
| google | `GEMINI_API_KEY`, `GOOGLE_API_KEY` |
| google-vertex, google-vertex-anthropic | `GOOGLE_CLOUD_API_KEY` |

This store does not change the other providers' authentication yet. AWS and
Google SDK credential chains and additional login flows belong to their
provider tasks. ChatGPT requires OAuth and rejects `auth set`.

### ChatGPT migration

```sh
swarmy auth login chatgpt
swarmy auth import
# Or read a particular existing ChatGPT auth.json:
swarmy auth import --file /private/auth.json
```

Login still writes `credential_file` (normally `~/.swarmy/auth.json`);
`--auth-file` / `SWARMY_CHATGPT_AUTH` overrides that path for login and import.
Login prints a note about the later migration to cluster persistence. Import
validates the ChatGPT file, preserves its provider metadata, and writes an
OAuth record under `chatgpt` without deleting or modifying the source. Stop
any other process refreshing that account before importing it. The gateway
prefers the stored record, falls back to the file only when no record exists,
and preserves its existing account identity checks. ChatGPT third-party
subscription access is treated as tolerated, not licensed. Anthropic
subscription OAuth is not supported.

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
||||||| 758da49
