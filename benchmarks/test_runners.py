"""Offline checks for the fixed workload and command/report contracts."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import sys
import unittest

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from tasks import PIN, TASKS, load  # noqa: E402

spec = importlib.util.spec_from_file_location("run_daytona", ROOT / "run-daytona.py")
daytona = importlib.util.module_from_spec(spec)
spec.loader.exec_module(daytona)


class BenchmarkTests(unittest.TestCase):
    def test_front_matter_and_criteria(self):
        for name in TASKS:
            task = load(name)
            self.assertEqual(task["commit"], PIN)
            self.assertIn("git diff", task["pass_criterion"])
            self.assertIn("test" if name != "small" else "make check", task["pass_criterion"])

    def test_swarm_dry_run(self):
        output = subprocess.check_output([str(ROOT / "run-swarm.sh"), "dev", "trial", "--dry-run"], text=True)
        self.assertEqual(output.count("--prompt-file"), 6)
        report = output.splitlines()[-1]
        self.assertIn("scripts/fleet/fleet report --label trial", report)
        self.assertNotIn("--remote", report)
        self.assertIn("--remote dev", output.splitlines()[0])
        self.assertIn("--provider chatgpt --model gpt-6-sol", output)

    def test_fleet_benchmark_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stub = root / "swarmy"
            stub.write_text("#!/bin/sh\nprintf '%s\\n' '{\"event\":\"session_created\",\"session_id\":\"fixture\"}'\n")
            stub.chmod(0o755)
            prompt = root / "prompt.txt"
            prompt.write_text("Benchmark prompt")
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"])
            output = subprocess.check_output([sys.executable, str(ROOT.parent / "scripts/fleet/fleet"),
                                              "benchmark", "--remote", "dev", "--provider", "openrouter",
                                              "--model", "model", "--effort", "medium",
                                              "--prompt-file", str(prompt)], env=env, text=True)
            self.assertIn("fixture", output)

    def test_swarm_cold_marker(self):
        from run_swarm import cold_from_events, prompt
        self.assertIn("BENCH_COLD=<true-or-false>", prompt(load("small")))
        self.assertIs(cold_from_events([{"event": "session_event", "value":
                                        {"message_appended": {"text": "BENCH_COLD=true"}}}]), True)
        with self.assertRaises(ValueError):
            cold_from_events([])

    def test_daytona_adapter_dry_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prompt_file = root / "prompt.txt"
            prompt_file.write_text("test task")
            workspace = root / "workspace"
            workspace.mkdir()
            command = [str(ROOT / "daytona-lane.sh"), "--provider", "chatgpt",
                       "--model", "gpt-6-sol", "--effort", "medium", "--workspace",
                       str(workspace), "--json", "--prompt-file", str(prompt_file)]
            env = dict(os.environ, BENCH_DRY_RUN="1", CODEX_LANE_STATE=str(root / "state"),
                       CARGO_TARGET_DIR=str(root / "empty-target"), CARGO_HOME=str(root / "cargo"))
            result = subprocess.run(command, env=env, text=True, capture_output=True, check=True)
            self.assertIn('"cold":true', result.stdout)
            self.assertIn("XDG_STATE_HOME=" + str(root / "state"), result.stdout)
            self.assertIn("codex exec --json -m gpt-6-sol -c model_reasoning_effort=medium --cd", result.stdout)
            refused = subprocess.run(command[:2] + ["openrouter"] + command[3:], env=env,
                                     text=True, capture_output=True)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn("requires chatgpt", refused.stderr)

    def test_daytona_runner_with_fixture_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lane = root / "lane"
            lane.write_text("#!/bin/sh\nprintf '%s\\n' '{\"event\":\"benchmark_cache\",\"cold\":true}'\ncat " + str(ROOT / "fixtures" / "codex.jsonl") + "\n")
            lane.chmod(0o755)
            output = root / "report.json"
            env = dict(os.environ, DAYTONA_LANE_CMD=str(lane))
            subprocess.run([sys.executable, str(ROOT / "run-daytona.py"), "trial",
                            "--output", str(output)], env=env, check=True, capture_output=True)
            report = json.loads(output.read_text())
            self.assertEqual(len(report["runs"]), 6)
            self.assertTrue(all(run["input_tokens"] == 21 for run in report["runs"]))
            self.assertTrue(all(run["cold"] is True for run in report["runs"]))
            self.assertEqual(len(list(root.glob("trial-*.jsonl"))), 6)

    def test_daytona_dry_run_and_report_fixture(self):
        output = subprocess.check_output([sys.executable, str(ROOT / "run-daytona.py"),
                                          "trial", "--dry-run"], text=True)
        self.assertEqual(output.count("--prompt-file"), 6)
        self.assertIn("--provider chatgpt --model gpt-6-sol", output)
        events = [json.loads(line) for line in (ROOT / "fixtures" / "codex.jsonl").read_text().splitlines()]
        result = daytona.record("trial", "small", 1, 12.5, events, "chatgpt", "gpt-6-sol", "medium", False)
        self.assertEqual(result["input_tokens"], 21)
        self.assertEqual(result["output_tokens"], 8)
        self.assertEqual(result["tool_calls"], 1)
        self.assertIsNone(result["placement_ms"])
        self.assertIsNone(result["session_id"])
        self.assertEqual(result["wall_seconds"], 12.5)
        self.assertIs(result["cold"], False)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            daytona.write_report(path, "trial", [result])
            report = json.loads(path.read_text())
        self.assertEqual(report["label"], "trial")
        self.assertEqual(report["runs"], [result])


if __name__ == "__main__":
    unittest.main()
