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
if args[0] == 'tasky':
    pass
if args[:3] == ['--json', 'task', 'show']:
    print(json.dumps({'task': {'title':'Repair widget', 'body':'Fix the widget', 'test_plan':'Run widget test'}}))
elif args[:2] == ['pr', 'view']:
    print(json.dumps({'url':args[2], 'state':os.environ.get('PR_STATE','OPEN'),
                      'baseRefName':'master', 'headRefName':'swarmy/ewr2hd'}))
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
            (root / 'prompt').write_text(args[-1])
        else:
            (root / 'followup').write_text(args[-1])
        print(json.dumps({'event':'session_created', 'session_id':'01AAAA'}), flush=True)
        print(json.dumps({'event':'run_outcome', 'outcome':'completed'}), flush=True)
    elif rest[:3] == ['agent', 'ls', '--json']:
        print(json.dumps({'name':'task-ewr2hd'}))
    elif rest[:3] == ['agent', 'show', 'task-ewr2hd']:
        print(json.dumps({'main_session':'01AAAA','provider':'fake','model':'fake',
            'cost_dollars':1.25, 'created_at':'2026-09-23T00:00:00Z'}))
    elif rest[:3] == ['session', 'show', '01AAAA']:
        print(json.dumps({'session_id':'01AAAA','state':'sleeping','agent_name':'task-ewr2hd'}))
        print(json.dumps({'state':'waiting_for_inference','reasons':['429 rate limited']}))
        print(json.dumps({'inference_completed': {'message': {'role':'assistant',
            'parts':[{'text':{'text':os.environ.get('LAST_MESSAGE',
            'Done https://github.com/pachuc/swarmy/pull/42')}}]}}}))
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
        self.config = FLEET.with_name("fleet.toml")
        self.config.write_text(f'''remote = "dev"\nrepo = "pachuc/swarmy"\nprovider = "fake"\nmodel = "fake"\ngithub_token = "private-token"\nstate_dir = "{self.root / 'state'}"\n''')
        self.config.chmod(0o600)
        self.addCleanup(self.config.unlink)
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                        STUB_STATE=str(self.root))

    def call(self, *args, env=None):
        return subprocess.run([sys.executable, str(FLEET), *args], env=env or self.env,
                              capture_output=True, text=True, check=False)

    def calls(self):
        return [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]

    def test_launch_status_collect_resume_and_remove(self):
        launch = self.call("launch", "EWR2HD", "--provider", "fake", "--model", "fake", "--effort", "high")
        self.assertEqual(launch.returncode, 0, launch.stderr)
        calls = self.calls()
        self.assertIn("--image", calls[1])
        self.assertIn("swarmy-dev:dev", calls[1])
        self.assertIn("--github-token-stdin", calls[1])
        self.assertIn("high", calls[1])
        self.assertNotIn("private-token", json.dumps(calls))
        self.assertNotIn("private-token", (self.root / "process_args").read_text())
        self.assertEqual((self.root / "token_ok").read_text(), "True")
        prompt = (self.root / "prompt").read_text()
        for expected in ("AGENTS.md", "Repair widget", "Fix the widget", "Run widget test", "swarmy/ewr2hd"):
            self.assertIn(expected, prompt)
        self.assertIn(["task", "start", "EWR2HD"], calls)
        self.assertNotIn("private-token", (self.root / "state" / "task-ewr2hd.jsonl").read_text())
        status = self.call("status")
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertIn("waiting for inference: 429 rate limited", status.stdout)
        self.assertIn("$1.25", status.stdout)
        collect = self.call("collect", "EWR2HD")
        self.assertEqual(collect.returncode, 0, collect.stderr)
        self.assertIn(["task", "pr", "EWR2HD", "https://github.com/pachuc/swarmy/pull/42"], self.calls())
        self.assertIn(["task", "test", "EWR2HD"], self.calls())
        resume = self.call("resume", "EWR2HD", "Please address review")
        self.assertEqual(resume.returncode, 0, resume.stderr)
        self.assertEqual((self.root / "followup").read_text(), "Please address review")
        merged_env = dict(self.env, PR_STATE="MERGED")
        self.assertEqual(self.call("rm", "EWR2HD", env=merged_env).returncode, 0)
        self.assertIn("delete", self.calls()[-1])

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
