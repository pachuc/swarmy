"""Offline regression tests for source conversion and reproducible generation."""

import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import generate


class GeneratorTests(unittest.TestCase):
    def test_openrouter_live_metadata_replaces_models_dev(self):
        source = {"id": "anthropic/claude-sonnet-4.6", "name": "Sonnet", "context_length": 1000000,
                  "supported_parameters": ["tools", "reasoning"],
                  "reasoning": {"supported_efforts": ["high", "max"]},
                  "top_provider": {"max_completion_tokens": 128000},
                  "pricing": {"prompt": "0.000003", "completion": "0.000015", "input_cache_read": "0.0000003"},
                  "architecture": {"input_modalities": ["text", "image"]}}
        model = generate.from_openrouter(source)
        self.assertEqual(model["api"], "AnthropicMessages")
        self.assertEqual(model["base_url"], "https://openrouter.ai/api")
        self.assertEqual(model["reasoning"], {"effort": ["none", "high", "max"]})
        self.assertEqual(model["limit"], {"context": 1000000, "output": 128000})
        self.assertEqual(model["cost"]["input"], 3)
        self.assertEqual(model["cost"]["cache_read"], 0.3)
        self.assertEqual(model["input_modalities"], ["text", "image"])
        self.assertTrue(model["compat"]["force_adaptive_thinking"])
        self.assertEqual(model["compat"]["cache_control_format"], "anthropic")
        source["reasoning"]["mandatory"] = True
        self.assertEqual(generate.from_openrouter(source)["reasoning"], {"effort": ["high", "max"]})
        source["supported_parameters"] = ["tools"]
        source["top_provider"] = None
        self.assertIsNone(generate.from_openrouter(source)["reasoning"])
        self.assertIsNone(generate.from_openrouter(source)["limit"]["output"])
        source["pricing"]["prompt"] = "-1"
        dynamic = generate.from_openrouter(source)
        self.assertEqual(dynamic["cost"]["input"], 0)
        self.assertTrue(dynamic["compat"]["dynamic_pricing"])

    def test_quirks_cover_provider_and_model_boundaries(self):
        for name in ["opus-4-6", "sonnet-4.6", "sonnet-4-9", "opus-5", "sonnet-5", "fable-5", "mythos-5"]:
            with self.subTest(name=name):
                self.assertTrue(generate.compat("anthropic", "claude-" + name, "AnthropicMessages")["force_adaptive_thinking"])
        self.assertFalse(generate.adaptive("claude-sonnet-4-20250514"))
        self.assertFalse(generate.adaptive("claude-opus-4-5"))
        self.assertTrue(generate.adaptive("us.anthropic.claude-opus-4-6-v1:0"))
        for name in ["opus-4-7", "opus-4.8", "opus-5-20260723"]:
            self.assertFalse(generate.compat("anthropic", "claude-" + name, "AnthropicMessages")["supports_temperature"])
        for provider in ["openai", "xai", "meta", "azure", "chatgpt"]:
            flags = generate.compat(provider, "model", "OpenAiResponses")
            self.assertTrue(flags["supports_developer_role"])
            self.assertEqual(flags["supports_strict_mode"], provider == "openai")
            self.assertEqual(flags["supports_long_cache_retention"], provider != "xai")
        for model_id in ["anthropic/claude", "openai/gpt", "meta/llama"]:
            flags = generate.compat("openrouter", model_id, "OpenAiCompletions", {"future": 1})
            self.assertEqual(flags["max_tokens_field"], "max_completion_tokens")
            self.assertEqual(flags["supports_developer_role"], model_id != "meta/llama")
            self.assertTrue(flags["supports_strict_mode"])
            self.assertEqual(flags["thinking_format"], "openrouter")
            self.assertEqual(flags["future"], 1)

    def test_effort_budget_and_cost_tiers(self):
        self.assertIsNone(generate.reasoning_options({"reasoning": False}))
        self.assertEqual(generate.reasoning_options({"reasoning": True}), "toggle")
        source = {"reasoning": True, "reasoning_options": [{"type": "toggle"}, {"type": "budget_tokens", "min": 1024}]}
        self.assertEqual(generate.reasoning_options(source), {"budget_tokens": {"min": 1024, "max": None}})
        source["reasoning_options"].append({"type": "effort", "values": ["high", "xhigh", "max"]})
        self.assertEqual(generate.reasoning_options(source), {"effort": ["high", "xhigh", "max"]})
        cost = generate.cost_from_models_dev({"input": 3, "output": 15, "tiers": [{"tier": {"type": "context", "size": 200000}, "input": 6}]})
        self.assertEqual(cost["tiers"][0], {"input_tokens_above": 200000, "input": 6, "output": 15, "cache_read": 0, "cache_write": 0})

    def test_generation_filters_copies_and_check_detects_drift(self):
        models_dev = {provider: {"name": provider, "models": {}} for provider in generate.PROVIDERS}
        models_dev["openai"]["models"] = {
            "gpt-test": {"id": "gpt-test", "name": "Test", "tool_call": True, "limit": {"context": 1000}},
            "no-tools": {"tool_call": False},
        }
        models_dev["openrouter"]["models"] = {"stale": {"tool_call": True}}
        openrouter = {"data": [{"id": "no-tools", "supported_parameters": []}]}
        payloads = [json.dumps(source).encode() for source in [models_dev, openrouter]]
        def download(*_args, **_kwargs):
            return io.BytesIO(payloads.pop(0))
        def run(check=False):
            payloads[:] = [json.dumps(source).encode() for source in [models_dev, openrouter]]
            with patch.object(generate.urllib.request, "urlopen", side_effect=download), patch("sys.argv", ["generate.py"] + (["--check"] if check else [])), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                return generate.main()
        with tempfile.TemporaryDirectory() as directory, patch.object(generate, "CATALOG", Path(directory)):
            self.assertEqual(run(), 0)
            first = {p.name: p.read_bytes() for p in Path(directory).glob("*.json")}
            self.assertEqual(run(), 0)
            self.assertEqual(first, {p.name: p.read_bytes() for p in Path(directory).glob("*.json")})
            self.assertEqual(run(check=True), 0)
            azure = json.loads(first["azure.json"])
            self.assertEqual(azure["base_url"], "")
            self.assertEqual(set(azure["models"]), {"gpt-test"})
            self.assertFalse(azure["models"]["gpt-test"]["compat"]["supports_strict_mode"])
            self.assertEqual(json.loads(first["openrouter.json"])["models"], {})
            self.assertEqual(len(json.loads(first["chatgpt.json"])["models"]), 7)
            for model in json.loads(first["chatgpt.json"])["models"].values():
                self.assertEqual(model["cost"]["input"], 0)
                self.assertEqual(model["cost"]["output"], 0)
            manifest = json.loads(first["manifest.json"])
            self.assertEqual(manifest["sources"]["models.dev"]["sha256"], hashlib.sha256(json.dumps(models_dev).encode()).hexdigest())
            path = Path(directory) / "openai.json"
            path.write_bytes(path.read_bytes() + b"\n")
            self.assertEqual(run(check=True), 1)
            self.assertEqual(run(), 0)
            path.unlink()
            self.assertEqual(run(check=True), 1)
            self.assertEqual(run(), 0)
            (Path(directory) / "unexpected.json").write_text("{}")
            self.assertEqual(run(check=True), 1)
            self.assertEqual(run(), 0)
            self.assertFalse((Path(directory) / "unexpected.json").exists())


if __name__ == "__main__":
    unittest.main()
