#!/usr/bin/env python3
"""Generate swarmy's embedded provider catalog using only the standard library."""

import argparse
import copy
import fnmatch
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "crates/swarmy-llm/catalog"
SOURCES = {
    "models.dev": "https://models.dev/api.json",
    "openrouter": "https://openrouter.ai/api/v1/models",
}
# Explicit auth and endpoints keep unsupported subscription login flows out of
# the catalog even when an upstream provider advertises them.
PROVIDERS = {
    "anthropic": ("AnthropicMessages", "https://api.anthropic.com", ["ANTHROPIC_API_KEY"], ["api_key"]),
    "openai": ("OpenAiResponses", "https://api.openai.com/v1", ["OPENAI_API_KEY"], ["api_key"]),
    "xai": ("OpenAiResponses", "https://api.x.ai/v1", ["XAI_API_KEY"], ["api_key"]),
    "meta": ("OpenAiResponses", "https://api.meta.ai/v1", ["META_MODEL_API_KEY"], ["api_key"]),
    "openrouter": ("OpenAiCompletions", "https://openrouter.ai/api/v1", ["OPENROUTER_API_KEY"], ["api_key", "pkce"]),
    "azure": ("OpenAiResponses", "", ["AZURE_API_KEY", "AZURE_OPENAI_API_KEY", "AZURE_RESOURCE_NAME", "AZURE_OPENAI_BASE_URL"], ["api_key", "entra"]),
    "amazon-bedrock": ("BedrockConverse", "", ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_REGION", "AWS_BEARER_TOKEN_BEDROCK"], ["aws", "bearer"]),
    "google": ("GoogleGenerativeAi", "https://generativelanguage.googleapis.com/v1beta", ["GOOGLE_API_KEY", "GOOGLE_GENERATIVE_AI_API_KEY", "GEMINI_API_KEY"], ["api_key"]),
    "google-vertex": ("GoogleVertex", "", ["GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION", "GOOGLE_VERTEX_PROJECT", "GOOGLE_VERTEX_LOCATION"], ["adc", "service_account"]),
    "google-vertex-anthropic": ("AnthropicMessages", "", ["GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION", "GOOGLE_VERTEX_PROJECT", "GOOGLE_VERTEX_LOCATION"], ["adc", "service_account"]),
    "chatgpt": ("OpenAiCodexResponses", "https://chatgpt.com/backend-api/codex", [], ["codex_oauth"]),
    "fake": ("Fake", "", [], ["none"]),
}
EFFORTS = ["none", "minimal", "low", "medium", "high", "xhigh", "max"]

# Pi packages/ai/scripts/generate-models.ts (detectOpenAICompletionsCompat and
# applyThinkingMetadata), and OpenCode's provider option tables. Keep flags in
# an open object so later clients can add protocol-specific capabilities.
QUIRKS = {
    "OpenAiResponses": {"supports_developer_role": True, "supports_strict_mode": False, "supports_long_cache_retention": True},
    "OpenAiCodexResponses": {"supports_developer_role": True, "supports_strict_mode": False, "supports_long_cache_retention": True},
    "OpenAiCompletions": {"max_tokens_field": "max_completion_tokens", "supports_strict_mode": True},
}


def claude_version(model_id):
    match = re.search(r"(?:^|[/.])claude-(opus|sonnet|fable|mythos)[.-](\d+)(?:[.-](\d{1,2})(?!\d))?", model_id.lower())
    return (match[1], int(match[2]), int(match[3] or 0)) if match else None


def adaptive(model_id):
    version = claude_version(model_id)
    if not version:
        return False
    family, major, minor = version
    return (family in ("opus", "sonnet") and (major, minor) >= (4, 6)) or (family in ("fable", "mythos") and major >= 5)


def compat(provider, model_id, api, existing=None):
    flags = {**(existing or {}), **QUIRKS.get(api, {})}
    if api in ("OpenAiResponses", "OpenAiCodexResponses"):
        flags.update(supports_strict_mode=provider == "openai", supports_long_cache_retention=provider != "xai")
    if provider == "openrouter":
        # Also retain completions flags on Anthropic models for clients that
        # explicitly choose OpenRouter's alternate completions endpoint.
        flags.update(QUIRKS["OpenAiCompletions"])
        flags.update(supports_developer_role=model_id.startswith(("anthropic/", "openai/")), thinking_format="openrouter")
        if model_id.startswith("anthropic/"):
            flags["cache_control_format"] = "anthropic"
    if api == "AnthropicMessages" or (api == "BedrockConverse" and claude_version(model_id)):
        flags["force_adaptive_thinking"] = adaptive(model_id)
        version = claude_version(model_id)
        flags["supports_temperature"] = not (version and version[0] == "opus" and (version[1] == 5 or (version[1] == 4 and version[2] in (7, 8))))
    return flags


def effort_options(values):
    return {"effort": [effort for effort in EFFORTS if effort in values]}


def reasoning_options(model):
    if not model.get("reasoning"):
        return None
    options = model.get("reasoning_options") or []
    values = [v for option in options if option["type"] == "effort" for v in option["values"]]
    if any(value in EFFORTS for value in values):
        return effort_options(values)
    for option in options:
        if option["type"] == "budget_tokens":
            return {"budget_tokens": {"min": option.get("min"), "max": option.get("max")}}
    return "toggle"


def cost_from_models_dev(source):
    result = {key: source.get(key, 0) for key in ("input", "output", "cache_read", "cache_write")}
    tiers = []
    for tier in source.get("tiers", []):
        if tier["tier"]["type"] != "context":
            raise ValueError(f"unsupported cost tier: {tier['tier']}")
        tiers.append({"input_tokens_above": tier["tier"]["size"], **{key: tier.get(key, result[key]) for key in result}})
    # Older snapshots used this field before introducing explicit tiers.
    if not tiers and source.get("context_over_200k"):
        tiers.append({"input_tokens_above": 200000, **result, **source["context_over_200k"]})
    result["tiers"] = sorted(tiers, key=lambda tier: tier["input_tokens_above"])
    return result


def from_models_dev(provider, model):
    api = PROVIDERS[provider][0]
    return {
        "id": model["id"], "name": model["name"], "family": model.get("family"),
        "reasoning": reasoning_options(model), "tool_call": True,
        "attachment": model.get("attachment", False),
        "input_modalities": model.get("modalities", {}).get("input", ["text"]),
        "limit": {"context": model["limit"]["context"], "output": model["limit"].get("output")},
        "cost": cost_from_models_dev(model.get("cost", {})),
        "release_date": model.get("release_date"), "status": model.get("status"),
        "compat": compat(provider, model["id"], api, model.get("compat")),
    }


def from_openrouter(model):
    parameters = model.get("supported_parameters", [])
    metadata = model.get("reasoning") or {}
    reasoning = None
    if "reasoning" in parameters:
        if metadata.get("supported_efforts"):
            # OpenRouter allows disabling reasoning unless it is mandatory.
            values = set(metadata["supported_efforts"])
            if metadata.get("mandatory"):
                values.discard("none")
            else:
                values.add("none")
            reasoning = effort_options(values)
        elif metadata.get("mandatory"):
            reasoning = effort_options(EFFORTS[1:5])
        else:
            reasoning = "toggle"
    modalities = model.get("architecture", {}).get("input_modalities", ["text"])
    pricing = model.get("pricing", {})
    prices = {dest: Decimal(pricing.get(src) or "0") for dest, src in (
        ("input", "prompt"), ("output", "completion"), ("cache_read", "input_cache_read"), ("cache_write", "input_cache_write"))}
    # Router aliases report -1 for prices determined by the eventual model.
    # A negative price must not become a credit in downstream cost accounting.
    dynamic_pricing = any(price < 0 for price in prices.values())
    cost = {key: float(max(price, 0) * 1_000_000) for key, price in prices.items()}
    cost["tiers"] = []
    api = "AnthropicMessages" if model["id"].startswith("anthropic/") else "OpenAiCompletions"
    result = {
        "id": model["id"], "name": model["name"], "family": None,
        "reasoning": reasoning, "tool_call": True,
        "attachment": any(m != "text" for m in modalities), "input_modalities": modalities,
        "limit": {"context": model["context_length"], "output": (model.get("top_provider") or {}).get("max_completion_tokens")},
        "cost": cost, "release_date": datetime.fromtimestamp(model["created"], timezone.utc).date().isoformat() if model.get("created") else None,
        "status": None, "compat": compat("openrouter", model["id"], api),
    }
    if api == "AnthropicMessages":
        result.update(api=api, base_url="https://openrouter.ai/api")
    if dynamic_pricing:
        result["compat"]["dynamic_pricing"] = True
    return result


def codex_models():
    # The Codex models the ChatGPT backend serves, with their limits. Subscription prices do not
    # represent per-token API charges, so all costs here are zero.
    names = {
        "gpt-5.5": "GPT-5.5", "gpt-5.3-codex-spark": "GPT-5.3 Codex Spark",
        "gpt-5.6-sol": "GPT-5.6 Sol", "gpt-5.6-terra": "GPT-5.6 Terra",
        "gpt-5.6-luna": "GPT-5.6 Luna", "gpt-6-astra": "GPT-6 Astra",
        "gpt-6-sol": "GPT-6 Sol",
    }
    models = {}
    for model_id, name in names.items():
        spark = model_id == "gpt-5.3-codex-spark"
        values = EFFORTS[:-1] if model_id in ("gpt-5.5", "gpt-5.3-codex-spark") else EFFORTS
        models[model_id] = {
            "id": model_id, "name": name, "family": "gpt", "reasoning": effort_options(values),
            "tool_call": True, "attachment": not spark, "input_modalities": ["text"] if spark else ["text", "image"],
            "limit": {"context": 128000 if spark else 272000, "output": 128000},
            "cost": cost_from_models_dev({}), "release_date": None, "status": None,
            "compat": compat("chatgpt", model_id, "OpenAiCodexResponses"),
        }
    return models


def generate(models_dev, openrouter):
    result = {}
    for provider, (api, base_url, env_keys, auth_kinds) in PROVIDERS.items():
        if provider == "chatgpt":
            models = codex_models()
        elif provider == "fake":
            models = {}
        elif provider == "openrouter":
            models = {m["id"]: from_openrouter(m) for m in openrouter["data"] if "tools" in m.get("supported_parameters", [])}
        else:
            models = {m["id"]: from_models_dev(provider, m) for m in models_dev[provider]["models"].values() if m.get("tool_call")}
        result[provider] = {
            "id": provider, "name": {"chatgpt": "ChatGPT", "fake": "Fake"}.get(provider) or models_dev[provider]["name"],
            "api": api, "base_url": base_url, "env_keys": env_keys, "auth_kinds": auth_kinds, "models": models,
        }
    for model_id, model in result["openai"]["models"].items():
        azure_model = copy.deepcopy(model)
        azure_model["compat"] = compat("azure", model_id, "OpenAiResponses", model["compat"])
        result["azure"]["models"][model_id] = azure_model
    overrides = json.loads((ROOT / "scripts/models/overrides.json").read_text())
    for provider, patterns in overrides["exclude_models"].items():
        result[provider]["models"] = {
            model_id: model for model_id, model in result[provider]["models"].items()
            if not any(fnmatch.fnmatchcase(model_id, pattern) for pattern in patterns)
        }
    for provider, prices in overrides["model_costs"].items():
        for model_id, cost in prices.items():
            if model_id in result[provider]["models"]:
                result[provider]["models"][model_id]["cost"] = cost
    return result


def write_json(path, value):
    payload = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    path.write_bytes(payload.encode("utf-8"))


def write_catalog(directory, providers, manifest):
    directory.mkdir(parents=True, exist_ok=True)
    for provider, data in providers.items():
        write_json(directory / f"{provider}.json", data)
    write_json(directory / "manifest.json", manifest)


def differences(expected, actual):
    names = {p.name for p in expected.glob("*.json")} | {p.name for p in actual.glob("*.json")}
    return sorted(name for name in names if not (expected / name).exists() or not (actual / name).exists() or (expected / name).read_bytes() != (actual / name).read_bytes())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="regenerate in a temporary directory and fail on differences")
    args = parser.parse_args()
    version = re.search(r'^version = "([^"]+)"', (ROOT / "Cargo.toml").read_text(), re.MULTILINE)[1]
    sources = {}
    data = {}
    for name, url in SOURCES.items():
        request = urllib.request.Request(url, headers={"User-Agent": f"swarmy/{version}", "Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read()
        sources[name] = {"url": url, "sha256": hashlib.sha256(payload).hexdigest()}
        data[name] = json.loads(payload)
    providers = generate(data["models.dev"], data["openrouter"])
    manifest = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "sources": sources}
    previous_path = CATALOG / "manifest.json"
    if previous_path.exists():
        previous = json.loads(previous_path.read_text())
        # A timestamp records the source snapshot, not each invocation. Keeping
        # it when sources match makes generation and --check reproducible.
        if previous.get("sources") == sources:
            manifest["generated_at"] = previous["generated_at"]
    with tempfile.TemporaryDirectory(prefix="swarmy-models-") as temporary:
        directory = Path(temporary)
        write_catalog(directory, providers, manifest)
        changed = differences(directory, CATALOG)
        if args.check:
            if changed:
                print("Catalog differs: " + ", ".join(changed), file=sys.stderr)
                return 1
            print("Catalog is up to date.")
        else:
            CATALOG.mkdir(parents=True, exist_ok=True)
            for name in changed:
                generated = directory / name
                if generated.exists():
                    (CATALOG / name).write_bytes(generated.read_bytes())
                else:
                    (CATALOG / name).unlink()
            print(f"Generated {sum(len(p['models']) for p in providers.values())} models; {len(changed)} files changed.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError) as error:
        sys.exit(f"Catalog generation failed: {error}")
