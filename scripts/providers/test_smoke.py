"""Offline tests of the operator script; never call a real provider."""
import ast
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("smoke.sh").resolve()


class SmokeTests(unittest.TestCase):
    def test_default_models_exist_in_catalog(self):
        source = SCRIPT.read_text().split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
        tree = ast.parse(source)
        models = next(ast.literal_eval(node.value) for node in tree.body
                      if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "models" for target in node.targets))
        for provider, model in models:
            catalog = SCRIPT.parents[2] / "crates/swarmy-llm/catalog" / (provider + ".json")
            self.assertIn(model, json.loads(catalog.read_text())["models"], provider)

    def test_continues_after_failure_skips_absent_and_redacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stub = root / "swarmy"
            stub.write_text("""#!/usr/bin/env python3
import json, os, sys
with open(os.environ['CALL_LOG'], 'a') as log:
    log.write(json.dumps(sys.argv[1:]) + '\\n')
if sys.argv[1] == 'doctor':
    print(json.dumps({'providers': [
        {'provider': p, 'credential': 'environment' if p in ('openai', 'chatgpt') else 'none', 'store': 'absent'}
        for p in ('anthropic', 'openai', 'chatgpt', 'xai', 'meta', 'openrouter', 'azure', 'amazon-bedrock', 'google', 'google-vertex', 'google-vertex-anthropic')]}))
    sys.exit(1)  # Unrelated doctor failures must not discard the provider report.
if sys.argv[1:4] == ['models', 'probe', 'openai/custom']:
    print('provider rejected ' + os.environ['OPENAI_API_KEY'] + ' https://fixture/?key=stored-secret', file=sys.stderr)
    sys.exit(1)
if sys.argv[1:4] == ['run', '--provider', 'openai']:
    print('Error: routed fixture rejection', file=sys.stderr)
    sys.exit(0)  # run can finish idle after reporting an inference failure.
print('ready')
""")
            stub.chmod(0o700)
            environment = dict(os.environ, SWARMY_BIN=str(stub), CALL_LOG=str(root / "calls"),
                               SWARMY_SMOKE_MODEL_OPENAI="custom", OPENAI_API_KEY="secret-for-fixture")
            environment.pop("CI", None)
            environment.pop("SWARMY_SMOKE_PROVIDERS", None)
            result = subprocess.run([str(SCRIPT)], env=environment, capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertIn("SKIP (no credential)", result.stdout)
            self.assertIn("| openai | custom | FAIL | FAIL |", result.stdout)
            self.assertIn("| chatgpt | gpt-5.5 | PASS | PASS |", result.stdout)
            self.assertIn("provider rejected [redacted]", result.stdout)
            self.assertNotIn("secret-for-fixture", result.stdout + result.stderr)
            self.assertNotIn("stored-secret", result.stdout + result.stderr)
            calls = [json.loads(line) for line in (root / "calls").read_text().splitlines()]
            self.assertEqual([call[0] for call in calls], ["doctor", "models", "models", "run", "run"])
            self.assertTrue(all("--tools" in call for call in calls[1:3]))

    def test_refuses_ci(self):
        result = subprocess.run([str(SCRIPT)], env=dict(os.environ, CI="true"), capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("must not run in CI", result.stderr)


if __name__ == "__main__":
    unittest.main()
