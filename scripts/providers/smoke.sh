#!/usr/bin/env bash
# Operator acceptance only: these requests consume real provider quota.
set +x
set -euo pipefail
if [[ -n ${CI:-} ]]; then
    printf '%s\n' 'Provider smoke checks must not run in CI.' >&2
    exit 2
fi
exec python3 - <<'PY'
import json
import os
import re
import subprocess
import sys

binary = os.environ.get("SWARMY_BIN", "swarmy")
timeout = int(os.environ.get("SWARMY_SMOKE_TIMEOUT", "180"))
models = [
    ("anthropic", "claude-sonnet-4-6"),
    ("openai", "gpt-5.5"),
    ("chatgpt", "gpt-5.5"),
    ("xai", "grok-4.6"),
    ("meta", "muse-spark-1.3"),
    ("openrouter", "anthropic/claude-sonnet-4.6"),
    ("azure", "gpt-5.5"),
    ("amazon-bedrock", "anthropic.claude-sonnet-4-6"),
    ("google", "gemini-2.5-flash"),
    ("google-vertex", "gemini-2.5-flash"),
    ("google-vertex-anthropic", "claude-sonnet-4-6@default"),
]
# An operator can explicitly include ambient instance credentials that doctor
# cannot inspect without contacting the cloud metadata service.
force = set(filter(None, os.environ.get("SWARMY_SMOKE_PROVIDERS", "").split(",")))
secrets = [value for name, value in os.environ.items()
           if any(word in name.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD"))
           and len(value) >= 8]


def safe(text):
    for secret in secrets:
        text = text.replace(secret, "[redacted]")
    text = re.sub(r"(?i)bearer\s+\S+|\beyJ[A-Za-z0-9_.-]+|\bsk-[A-Za-z0-9_-]+", "[redacted]", text)
    text = re.sub(r"(?i)([?&](?:key|api_key|access_token|token)=)[^&\s]+", r"\1[redacted]", text)
    text = re.sub(r'(?i)("(?:api_key|access_token|refresh_token|authorization|key)"\s*:\s*")[^"]*"', r'\1[redacted]"', text)
    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
    return " ".join(text.split()).replace("|", "\\|")


def run(args):
    try:
        result = subprocess.run([binary, *args], capture_output=True, text=True,
                                timeout=timeout, check=False)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return 1, "", f"timed out after {timeout}s"
    except OSError as error:
        return 1, "", str(error)


_, output, error = run(["doctor", "--json"])
try:
    providers = {row["provider"]: row for row in json.loads(output)["providers"]}
except (ValueError, KeyError, TypeError):
    sys.exit("Cannot inspect provider credentials: " + safe(error or "invalid doctor output"))

rows = []
for provider, default_model in models:
    model = os.environ.get("SWARMY_SMOKE_MODEL_" + provider.upper().replace("-", "_"), default_model)
    state = providers.get(provider, {})
    skip = state.get("credential") == "none" and state.get("store") == "absent" and provider not in force
    rows.append({"provider": provider, "model": model, "skip": skip, "errors": []})

# Finish all direct probes before testing routing, so a routing failure cannot
# hide a provider's independent credential and protocol result.
for phase in ("probe", "routed"):
    for row in rows:
        if row["skip"]:
            row[phase] = "SKIP (no credential)"
            continue
        provider, model = row["provider"], row["model"]
        print(f"Checking {phase}: {provider}/{model}", file=sys.stderr, flush=True)
        args = (["models", "probe", f"{provider}/{model}", "--tools"] if phase == "probe"
                else ["run", "--provider", provider, "--model", model, "reply with the word ready"])
        code, output, error = run(args)
        row[phase] = "PASS" if code == 0 else "FAIL"
        if phase == "routed":
            row["reply"] = safe(output)
        if code:
            row["errors"].append(f"{phase}: " + safe(error or output or f"exit {code}"))

print("| Provider | Model | Direct probe + tools | Routed run | Reply | Error |")
print("|---|---|---|---|---|---|")
for row in rows:
    print("| " + " | ".join([row["provider"], safe(row["model"]), row["probe"], row["routed"],
                              row.get("reply", "—"), "; ".join(row["errors"]) or "—"]) + " |")
sys.exit(any(row[phase] == "FAIL" for row in rows for phase in ("probe", "routed")))
PY
