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

Custom model configuration and credential resolution belong to later tasks.
This change supplies the catalog and client dispatch point without changing
session selection or gateway behavior.
