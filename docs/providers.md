# Inference providers

swarmy chooses providers and models per session or named agent. The checked-in
catalog supplies model limits, reasoning options, compatibility flags, and
prices. Rust protocol clients send requests; credentials live in the encrypted
cluster store or resolve from the gateway host. Builds never fetch model data.

## Supported providers

| Provider id | Protocol | Credential kinds | Environment and endpoint settings |
|---|---|---|---|
| `anthropic` | Anthropic Messages | API key | `ANTHROPIC_API_KEY` |
| `openai` | OpenAI Responses | Platform API key | `OPENAI_API_KEY` |
| `chatgpt` | Responses on the Codex backend | Existing Codex OAuth device login | Cluster store; no API key variable. Tolerated, not licensed. |
| `xai` | OpenAI Responses | API key | `XAI_API_KEY` |
| `meta` | OpenAI Responses | API key | `META_MODEL_API_KEY` |
| `openrouter` | Chat Completions; Anthropic Messages for `anthropic/*` | API key or public PKCE login that mints a key | `OPENROUTER_API_KEY` |
| `azure` | OpenAI Responses | API key or Azure CLI Entra token | `AZURE_API_KEY`, `AZURE_OPENAI_API_KEY`; `AZURE_RESOURCE_NAME` or `AZURE_OPENAI_BASE_URL`. Model ids name deployments. |
| `amazon-bedrock` | Bedrock Converse stream | AWS SDK credential chain or bearer token | `AWS_BEARER_TOKEN_BEDROCK`; otherwise `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, profiles and instance/container roles. `AWS_REGION` selects the region. |
| `google` | Gemini generateContent | API key | `GEMINI_API_KEY`, `GOOGLE_API_KEY`, `GOOGLE_GENERATIVE_AI_API_KEY` |
| `google-vertex` | Gemini generateContent on Vertex | Application Default Credentials (ADC), service account key, or stored access token | `GOOGLE_APPLICATION_CREDENTIALS` or gcloud ADC; `GOOGLE_CLOUD_PROJECT`, `GOOGLE_CLOUD_LOCATION` |
| `google-vertex-anthropic` | Anthropic Messages on Vertex | Same as `google-vertex` | Same as `google-vertex` |
| `fake` | Scripted responses | None | `[fake]` script and call log settings |

`env_keys` in the catalog also includes endpoint and cloud configuration. A
project, region, or profile name is not itself a secret key. Clients identify
swarmy as `swarmy/<version>`; they do not send another coding product's identity
headers or system prompt prefixes. Images are catalog metadata only: image
inputs are not sent yet.

## Credentials and auth commands

Start a local cluster with `swarmy dev up`. It creates a base64 32-byte keyring
at `~/.swarmy/keyring` with mode 600, or uses `SWARMY_KEYRING`. The key encrypts
FoundationDB credential records with XChaCha20-Poly1305. Keep a separate backup:
replacing the key cannot decrypt existing records. CLI and gateway hosts need
the same key. `remote up --services node --copy-credential` and
`remote add-node --copy-credential` explicitly copy it over SSH; ordinary node
hosts need no provider credentials.

```sh
swarmy auth set anthropic --from-env
swarmy auth set openai --file /private/openai-key
swarmy auth set azure --from-env --extra resource_name=my-resource
swarmy auth set openrouter --api-key YOUR_KEY
swarmy auth ls
swarmy auth check
swarmy auth check chatgpt
swarmy auth rm openai
swarmy auth login chatgpt
swarmy auth login openrouter
az login
swarmy auth login azure --resource my-resource
swarmy auth import --file /private/chatgpt-auth.json
```

Prefer `--file` or `--from-env` to keep keys out of shell history and process
arguments. `--extra name=value` is repeatable; its values are encrypted too.
`--from-env` follows the table's API key names, except Google key entry accepts
`GEMINI_API_KEY` and `GOOGLE_API_KEY` only. Vertex key entry reads
`GOOGLE_CLOUD_API_KEY`; this does not replace configuring ADC and project/location
for inference. Stored Vertex extras accept `project`, `location`,
`service_account_json`, or `access_token`. For Azure, stored credentials need
`resource_name` (classic) or `base_url` (Foundry endpoint); an environment
API key needs `AZURE_RESOURCE_NAME` or `AZURE_OPENAI_BASE_URL`.
`auth login azure --resource` accepts either a classic resource name or the full
Foundry endpoint URL. Credential `base_url` takes precedence over the classic
resource name; no `[custom_providers.azure]` entry is needed.

`auth ls` and `auth check` expose metadata, never secrets. Check verifies local
decryption and status, not provider acceptance. Status is `ready`, `expired`, or
`needs_login`. Use a probe below to verify the actual credential and protocol.

ChatGPT login prints a device URL and code. OpenRouter uses its public PKCE
flow, with a browser callback on an ephemeral loopback port or a pasted code;
the result is an API key. Azure shells out to
`az account get-access-token --scope https://cognitiveservices.azure.com/.default --output json`.
`--scope` can override that scope. Azure refresh requires an installed, signed-in
Azure CLI on the host using the credential. For key credentials use
`swarmy auth set azure --from-env --extra base_url=https://RESOURCE.services.ai.azure.com`
for a Foundry resource; the URL is stored with the key.

All logins write the cluster store directly. `auth import` copies an existing
ChatGPT file without changing it; without `--file`, it reads `credential_file`
(normally `~/.swarmy/auth.json`, also overridden by `--auth-file` or
`SWARMY_CHATGPT_AUTH`). Stop other refresh owners before importing. Gateways do
not fall back to that file.

Resolution uses `swarmy_llm::auth::resolve` with an explicit `Resolver`:

1. Read the cluster record. An unreadable or failed stored credential never
   falls back to an environment key. Refresh near expiry uses a database lease;
   other processes read the winner's rotated token. Failed refresh marks
   `needs_login`.
2. With no stored record, read the provider's API key environment variables.
3. Bedrock uses the official AWS Rust SDK chain. Vertex builds shared Google
   authentication from stored extras or host ADC and project/location settings.
   Fake requests need no credentials.

Bedrock console-issued bearer API keys expire after twelve hours and are for
development only. Long-lived gateways need an IAM identity (for example, an
instance role when the gateway runs in the cloud); SDK credentials can be
rotated without storing a short-lived console key. An expired console key
requires replacement, not refresh.

## Choosing and inspecting models

```sh
swarmy models providers
swarmy models ls --provider anthropic
swarmy models ls --reasoning --json
swarmy models search sonnet
swarmy models show openrouter/anthropic/claude-sonnet-4.6
swarmy run --provider openai --model gpt-5.5 --effort high 'Review this repository'
swarmy chat --model openai/gpt-5.5
swarmy agent create tommy --provider openrouter --model anthropic/claude-sonnet-4.6
swarmy agent set tommy --effort medium
swarmy agent set tommy --model default
```

Model browsing is offline and uses `Settings::catalog()`: snapshot plus local
configuration. Lists and search sort by provider/model. Show splits at the first
slash, so model ids can contain slashes. `--json` returns model objects with
`key`, `provider`, `supported_efforts`, `effective_api`, `effective_base_url`, and
all model metadata. Providers lists protocol, auth kinds, and environment names;
its credential column remains `unknown`. Use doctor for credential presence.

A bare model id uses the selected or stack provider. `provider/model` shorthand
must agree with an explicit provider unless the full id exists under that
provider, such as OpenRouter's `anthropic/claude-sonnet-4.6`. OpenRouter dashed
version aliases resolve to canonical catalog ids. Unknown models get suggestions.

Effort is `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max`. Clamping
keeps a supported request, otherwise chooses the next higher level, then the
next lower. `xhigh` and `max` require explicit model support. Non-reasoning models
use `none`. Session overrides precede agent overrides and stack defaults; the
first clamp is recorded in the session log. Inference flags apply to new ephemeral
sessions. For `--agent`, configure the agent; resumed sessions keep their selection.
`default` clears an agent override. `session show` and `agent show` distinguish
stored overrides from inherited defaults.

## Probe, doctor, and smoke checks

```sh
swarmy models probe chatgpt/gpt-5.5
swarmy models probe chatgpt/gpt-5.5 --effort low --tools
swarmy doctor
scripts/providers/smoke.sh
```

Probe resolves credentials and calls `client_for` inside the CLI companion
process, without NATS, workers, or gateway routing. It streams text, reasoning,
and tool deltas, then prints usage (including cache and reasoning tokens),
catalog cost, effort used, and elapsed time. `--tools` requires exactly one
`get_time` call, supplies the current UTC time, and requests the final answer.
A provider or protocol failure exits nonzero with its error. `--json` emits
newline-delimited delta records followed by a `probe_summary`. Without an
explicit effort, probe starts at `none` and clamps it to the model.

An uninitialized checkout can probe host environment credentials without a
cluster. A configured cluster must be reachable to preserve store precedence.
Fake probes use the configured script and model, for example `model = 'scripted'`
and `swarmy models probe fake/scripted`.

Doctor's provider section lists every configured catalog provider, credential
source, local status, store availability, and gateway advertisement. It works
without a gateway. Store status comes from decrypting records without refreshing
them; environment and ambient credentials are `unverified`. An unavailable store
or gateway report is `unknown`, not proof of absence. Advertisements come from
expiring gateway provider records; `served` requires an unexpired record.
Doctor does not contact cloud metadata services, so ambient instance roles may
have no visible local credential hint. Its JSON adds a `providers` array beside
`checks`; unused providers without credentials do not make doctor fail.

The smoke script is an operator acceptance tool and refuses to run when `CI` is
set. It first probes one fixed model per real provider, including tool round trips,
then runs `swarmy run --provider X --model Y 'reply with the word ready'` for each
configured provider. It continues after failures, prints a Markdown table with
errors, and exits nonzero if any attempted check fails. Providers with no local
credential and no stored record report `SKIP (no credential)`. Routed checks
need the stack, a gateway, and a default image just like ordinary `swarmy run`.
Restart the gateway after adding credentials so it discovers the new provider.

Use `SWARMY_BIN=/path/to/swarmy` to choose a build and `SWARMY_SMOKE_TIMEOUT=180`
to set each command's timeout in seconds. Deployment ids or account-specific
models can override defaults with `SWARMY_SMOKE_MODEL_AZURE`, or the corresponding
upper-case provider id with hyphens replaced by underscores. For ambient
instance credentials without a local hint, set the comma-separated
`SWARMY_SMOKE_PROVIDERS=amazon-bedrock,google-vertex` to force those attempts.
The script captures command output, redacts recognizable secrets, and prints
only results and failure diagnostics; it does not read credential files.

## Live verification (2026-09-21)

| Provider / route | Result | Follow-up |
|---|---|---|
| Azure Foundry and classic | Classic verified; Foundry required a custom provider entry during the original live run. | Probe both with the credential endpoint fix and no custom entry. |
| Google Gemini | The original Gemini 2.5 default was refused by a new key. | Run the smoke script with its new `gemini-3-flash-preview` default and a fresh key. |
| Azure Grok 4.6 | Live inference worked, but the original catalog showed zero cost. | Confirm `swarmy session show` cost on a billed turn. |
| Bedrock console key | Expires after twelve hours; development only. | Use IAM for long-lived gateways. |
| Claude on Vertex (`google-vertex-anthropic`) | Unverified: project quota was zero. | Obtain nonzero Claude quota, then probe and run the smoke script. |
| Grok tool calls | Occasionally returned tool calls as text rather than structured calls. | Monitor rate; change the client only if more than one in ten smoke turns reproduce it. |

## Custom providers and models

```toml
[custom_providers.private]
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

Put these entries in `.swarmy/config.toml` or the discovered user configuration.
Store private endpoint credentials with `swarmy auth set private --file PATH`.
New provider tables require `api` and `base_url`; existing ones keep omitted
fields. A top-level `providers = [...]` restricts gateway discovery.

Models require `provider` and `id`. Optional fields are `name`, `api`, `base_url`,
`context_window`, `max_output_tokens`, `reasoning`, `cost`, and `compat`. New
models default to text input, tool calling, 128000 context tokens, 16384 output
tokens, no reasoning, and zero placeholder prices. Supply actual values. Existing
models keep omitted fields; repeated entries apply in file order. Compatibility
keys merge individually. `reasoning` replaces the effort list (empty disables
reasoning). `cost` replaces the full object, requiring `input` and `output`;
omitted cache prices and tiers default to zero and empty.

API names are `AnthropicMessages`, `OpenAiResponses`, `OpenAiCodexResponses`,
`OpenAiCompletions`, `GoogleGenerativeAi`, `GoogleVertex`, `BedrockConverse`, and
`Fake`. Builds need no regeneration for custom models. Dev stack children
receive the same definitions through `SWARMY_CUSTOM_PROVIDERS` and `SWARMY_MODELS`.

## Catalog metadata and regeneration

```sh
make models
# Equivalent generator command:
python3 scripts/models/generate.py
python3 scripts/models/generate.py --check
python3 -m unittest discover -s scripts/models -v
```

The Python standard-library generator fetches models.dev and OpenRouter's live
list, retains tool-capable models from the provider allowlist, applies protocol
quirks, and writes `crates/swarmy-llm/catalog/<provider>.json`. OpenRouter uses its
live list; Azure also inherits OpenAI model metadata. The checked-in
`scripts/models/overrides.json` drops Gemini 2.5 models from Google catalogs
because new keys cannot use them and prices Azure's Grok 4.6 at its published
per-million-token rate. ChatGPT models are explicitly listed with zero token cost because subscription billing is not per-token.
Fake fixture models come from configuration.

Commit reviewed JSON changes with the generator. The manifest records source
SHA-256 hashes and a generation timestamp, retained when sources are unchanged.
`--check` downloads and regenerates into a temporary directory, rejecting changed,
missing, or extra files. Upstream changes require review; failures never silently
use old downloads. Normal Rust builds read only the committed snapshot.

Prices are US dollars per million tokens, including cache reads and writes.
Context tiers apply above `input_tokens_above`; each tier has `input`, `output`,
`cache_read`, and `cache_write`. Usage includes cached input within input tokens
and reasoning within output tokens. Gateway and probe cost calculations account
for those subsets. OpenRouter dynamic-price aliases carry zero placeholders and
`compat.dynamic_pricing = true`; their estimates do not mean free inference.

Models can override protocol and endpoint. Reasoning metadata prefers an effort
list, then a budget range, then a toggle; budgets and toggles expose `none`
through `high`. The compatibility map carries token fields, developer roles,
strict tools, thinking/replay formats, and cache options. The implementation was
informed by the OpenCode and Pi surveys dated 2026-09-19; no JavaScript clients
or SDKs are vendored.

## Recorded terms decisions

These are the project's decisions from the terms audit dated **2026-09-19**,
not a new grant of vendor permission:

| Flow | Decision recorded on 2026-09-19 |
|---|---|
| Anthropic subscription OAuth and internal console login | Not built. The audit records explicit prohibition of third-party subscription harnesses and OpenCode's removal after a legal request. Use a Console API key. |
| Existing ChatGPT Codex device flow | Kept as tolerated, not licensed. No written permission was established. The existing OAuth client id is the plan's exception; no new product identity impersonation is added. |
| GitHub Copilot subscription login | Not built. Reusing VS Code identity is not an authorized integration; a partnership is outside this scope. |
| Kimi Code OAuth | Not built. The audit directs third-party tools to API keys. |
| xAI grok-cli OAuth | Not built. No public registration was established and backend rejection was observed in the audit. Use an API key. |
| OpenCode console login | Not built. Its console flow was unclear and its free tier was restricted to OpenCode. |
| OpenRouter PKCE | Built as the documented public flow that mints an API key. |
| Azure CLI Entra tokens | Built through the installed Azure CLI; no embedded Azure client identity. |

Key pools, admission, affinity, failover, image inputs, and expanding the
models.dev provider allowlist remain future work; see the `provider-breadth-quota` goal in tasky.
