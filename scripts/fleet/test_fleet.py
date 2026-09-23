"""Offline fleet behavior with fake command line clients."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

FLEET = Path(__file__).with_name("fleet")
STUB = '''#!/usr/bin/env python3
import json, os, subprocess, sys
from pathlib import Path
args = sys.argv[1:]
root = Path(os.environ['STUB_STATE'])
with (root / 'calls').open('a') as out:
    out.write(json.dumps(args) + '\\n')
if args[:3] == ['--json', 'task', 'show']:
    print(json.dumps({'task': {'title':'Repair widget', 'body':'Fix the widget', 'test_plan':'Run widget test'}}))
elif args[:2] == ['pr', 'view']:
    print(json.dumps({'url':args[2], 'state':os.environ.get('PR_STATE','OPEN'),
                      'baseRefName':'master', 'headRefName':os.environ.get('PR_BRANCH','swarmy/ewr2hd')}))
elif args[:2] == ['--remote', 'dev']:
    rest = args[2:]
    if rest[:2] == ['agent', 'create']:
        token = sys.stdin.read()
        (root / 'token_ok').write_text(str(token == 'private-token\\n'))
        (root / 'process_args').write_text(subprocess.check_output(
            ['ps', '-o', 'args=', '-p', str(os.getpid())], text=True))
        print('{}')
    elif rest[:2] == ['run', '--agent']:
        if 'Repair widget' in args[-1]:
            (root / ('prompt-' + rest[2])).write_text(args[-1])
        else:
            (root / 'followup').write_text(args[-1])
        print(json.dumps({'event':'session_opened', 'session_id':'01AAAA', 'agent_name': rest[2]}), flush=True)
        print(json.dumps({'event':'run_outcome', 'outcome':'completed'}), flush=True)
    elif rest[:2] == ['agent', 'show']:
        print(json.dumps({'name': rest[2], 'main_session':'01AAAA','provider':'fake','model':'fake',
            'cost_dollars':1.25, 'created_at':'2026-09-23T00:00:00Z'}))
    elif rest[:3] == ['session', 'ls', '--json']:
        if (root / 'interrupted').exists():
            count = int((root / 'ls_count').read_text()) if (root / 'ls_count').exists() else 0
            (root / 'ls_count').write_text(str(count + 1))
            state = 'idle' if count else 'sleeping'
        else:
            state = 'sleeping'
        print(json.dumps({'session_id':'01AAAA','state':state,'agent_name':'worker-1'}))
    elif rest[:3] == ['session', 'show', '01AAAA']:
        if not (root / 'interrupted').exists():
            print(json.dumps({'state':'waiting_for_inference','reasons':['429 rate limited']}))
        print(json.dumps({'inference_completed': {'message': {'role':'assistant',
            'parts':[{'text':{'text':os.environ.get('LAST_MESSAGE',
            'Done https://github.com/pachuc/swarmy/pull/42')}}]}}}))
    elif rest[:3] == ['session', 'interrupt', '01AAAA']:
        (root / 'interrupted').write_text('yes')
    else:
        print('{}')
'''


class FleetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name in ("swarmy", "tasky", "gh"):
            path = self.bin / name
            path.write_text(STUB)
            path.chmod(0o700)
        # Never touch the real fleet.toml next to the driver; tests use their own.
        self.config = self.root / "fleet.toml"
        self.config.write_text(f'''remote = "dev"\nrepo = "pachuc/swarmy"\nprovider = "fake"\nmodel = "fake"\nworkers = 2\ngithub_token = "private-token"\nstate_dir = "{self.root / 'state'}"\n''')
        self.config.chmod(0o600)
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                        STUB_STATE=str(self.root), FLEET_CONFIG=str(self.config))

    def call(self, *args, env=None):
        return subprocess.run([sys.executable, str(FLEET), *args], env=env or self.env,
                              capture_output=True, text=True, check=False)

    def calls(self):
        return [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]

    def creates(self):
        return [call for call in self.calls() if call[2:4] == ["agent", "create"]]

    def test_launch_status_collect_resume_and_release(self):
        launch = self.call("launch", "EWR2HD", "--provider", "fake", "--model", "fake", "--effort", "high")
        self.assertEqual(launch.returncode, 0, launch.stderr)
        self.assertIn("worker-1", launch.stdout)
        create = self.creates()[0]
        self.assertEqual(create[4], "worker-1")
        self.assertNotIn("--image", create)
        self.assertIn("--github-token-stdin", create)
        self.assertIn("high", create)
        self.assertNotIn("private-token", json.dumps(self.calls()))
        self.assertNotIn("private-token", (self.root / "process_args").read_text())
        self.assertEqual((self.root / "token_ok").read_text(), "True")
        run_call = next(call for call in self.calls() if call[2:4] == ["run", "--agent"])
        for flag in ("--provider", "--model", "--effort"):
            self.assertNotIn(flag, run_call)
        prompt = (self.root / "prompt-worker-1").read_text()
        for expected in ("AGENTS.md", "Repair widget", "Fix the widget", "Run widget test",
                         "swarmy/ewr2hd", "~/work/ewr2hd", "cargo clean", "from origin/master"):
            self.assertIn(expected, prompt)
        self.assertIn(["task", "start", "EWR2HD"], self.calls())
        self.assertNotIn("private-token", (self.root / "state" / "ewr2hd.jsonl").read_text())
        status = self.call("status")
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertIn("worker-1\tEWR2HD", status.stdout)
        self.assertIn("waiting for inference: 429 rate limited", status.stdout)
        self.assertIn("$1.25", status.stdout)
        collect = self.call("collect", "EWR2HD")
        self.assertEqual(collect.returncode, 0, collect.stderr)
        self.assertIn(["task", "pr", "EWR2HD", "https://github.com/pachuc/swarmy/pull/42"], self.calls())
        self.assertIn(["task", "test", "EWR2HD"], self.calls())
        resume = self.call("resume", "EWR2HD", "Please address review")
        self.assertEqual(resume.returncode, 0, resume.stderr)
        self.assertEqual((self.root / "followup").read_text(), "Please address review")
        open_env = dict(self.env, PR_STATE="OPEN")
        self.assertEqual(self.call("release", "EWR2HD", env=open_env).returncode, 1)
        merged_env = dict(self.env, PR_STATE="MERGED")
        release = self.call("release", "EWR2HD", env=merged_env)
        self.assertEqual(release.returncode, 0, release.stderr)
        self.assertFalse(any("delete" in call for call in self.calls()))
        workers = json.loads((self.root / "state" / "workers.json").read_text())
        self.assertEqual(workers, {"worker-1": None})

    def test_pool_reuses_idle_workers_and_caps_creation(self):
        self.assertEqual(self.call("launch", "AAAAA1").returncode, 0)
        second = self.call("launch", "AAAAA2")
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertIn("worker-2", second.stdout)
        self.assertEqual([c[4] for c in self.creates()], ["worker-1", "worker-2"])
        third = self.call("launch", "AAAAA3")
        self.assertEqual(third.returncode, 1)
        self.assertIn("busy", third.stderr)
        self.assertEqual(self.call("release", "AAAAA1", "--force").returncode, 0)
        fourth = self.call("launch", "AAAAA3")
        self.assertEqual(fourth.returncode, 0, fourth.stderr)
        self.assertIn("worker-1", fourth.stdout)
        self.assertEqual(len(self.creates()), 2)
        # A different provider on a full pool falls back to an idle worker with a note.
        self.assertEqual(self.call("release", "AAAAA2", "--force").returncode, 0)
        other = self.call("launch", "AAAAA4", "--provider", "chatgpt", "--model", "gpt-6-sol")
        self.assertEqual(other.returncode, 0, other.stderr)
        self.assertIn("worker-2", other.stdout)
        self.assertIn("not the requested chatgpt/gpt-6-sol", other.stderr)
        self.assertEqual(len(self.creates()), 2)
        self.assertEqual(self.call("release", "AAAAA4", "--force").returncode, 0)
        busy = self.call("reset", "worker-1")
        self.assertEqual(busy.returncode, 1)
        self.assertEqual(self.call("release", "AAAAA3", "--force").returncode, 0)
        reset = self.call("reset", "worker-1")
        self.assertEqual(reset.returncode, 0, reset.stderr)
        self.assertEqual(self.calls()[-1][2:5], ["agent", "delete", "worker-1"])
        workers = json.loads((self.root / "state" / "workers.json").read_text())
        self.assertNotIn("worker-1", workers)

    def test_kill_interrupts_waits_and_releases_without_completing_task(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        result = self.call("kill", "EWR2HD", "--timeout-seconds", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("still in progress", result.stdout)
        self.assertIn(["--remote", "dev", "session", "interrupt", "01AAAA"], self.calls())
        shows = [call for call in self.calls() if call[2:5] == ["session", "ls", "--json"]]
        self.assertGreaterEqual(len(shows), 2)
        self.assertEqual(json.loads((self.root / "state" / "workers.json").read_text()), {"worker-1": None})
        self.assertFalse((self.root / "state" / "ewr2hd.json").exists())
        self.assertFalse(any(call[:3] == ["task", "done", "EWR2HD"] for call in self.calls()))

    def test_provider_match_prefers_matching_idle_worker(self):
        self.assertEqual(self.call("launch", "AAAAA1", "--provider", "openrouter", "--model", "m").returncode, 0)
        self.assertEqual(self.call("launch", "AAAAA2", "--provider", "chatgpt", "--model", "n").returncode, 0)
        self.assertEqual(self.call("release", "AAAAA1", "--force").returncode, 0)
        self.assertEqual(self.call("release", "AAAAA2", "--force").returncode, 0)
        chosen = self.call("launch", "AAAAA3", "--provider", "chatgpt", "--model", "n")
        self.assertEqual(chosen.returncode, 0, chosen.stderr)
        self.assertIn("worker-2", chosen.stdout)
        self.assertEqual(len(self.creates()), 2)

    def test_collect_refuses_missing_open_pr(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        env = dict(self.env, PR_STATE="CLOSED")
        result = self.call("collect", "EWR2HD", env=env)
        self.assertEqual(result.returncode, 1)
        self.assertIn("not open", result.stderr)
        self.assertFalse(any(call[:2] == ["task", "pr"] for call in self.calls()))

    def test_config_requires_owner_only_permissions(self):
        self.config.chmod(0o644)
        result = self.call("status")
        self.assertEqual(result.returncode, 1)
        self.assertIn("owner-only", result.stderr)


if __name__ == "__main__":
    unittest.main()
