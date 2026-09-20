# Provider credentials

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
| meta | `META_API_KEY` |
| openrouter | `OPENROUTER_API_KEY` |
| azure | `AZURE_OPENAI_API_KEY` |
| amazon-bedrock | `AWS_BEARER_TOKEN_BEDROCK` |
| google | `GEMINI_API_KEY`, `GOOGLE_API_KEY` |
| google-vertex, google-vertex-anthropic | `GOOGLE_CLOUD_API_KEY` |

This store does not change the other providers' authentication yet. AWS and
Google SDK credential chains and additional login flows belong to their
provider tasks. ChatGPT requires OAuth and rejects `auth set`.

## ChatGPT migration

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

## Keyring and remote gateways

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
