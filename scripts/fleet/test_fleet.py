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
from datetime import datetime, timedelta, timezone
from pathlib import Path
args = sys.argv[1:]
root = Path(os.environ['STUB_STATE'])
with (root / 'calls').open('a') as out:
    out.write(json.dumps(args) + '\\n')
if args[:3] == ['--json', 'task', 'show']:
    print(json.dumps({'task': {'title':'Repair widget', 'body':'Fix the widget', 'test_plan':'Run widget test'}, 'status': os.environ.get('TASK_STATUS', 'todo')}))
elif args[:2] == ['pr', 'list']:
    print(os.environ.get('PR_LIST', '[]'))
elif args[:2] == ['pr', 'view']:
    print(json.dumps({'url':args[2], 'state':os.environ.get('PR_STATE','OPEN'),
                      'baseRefName':'master', 'headRefName':os.environ.get('PR_BRANCH','swarmy/ewr2hd')}))
elif args[:2] == ['--remote', 'dev'] or args[:1] in (['agent'], ['run'], ['session']):
    # An empty remote in fleet.toml drops the --remote flag (local API mode).
    rest = args[2:] if args[:2] == ['--remote', 'dev'] else args
    if rest[:2] == ['agent', 'create']:
        token = sys.stdin.read()
        (root / 'token_ok').write_text(str(token == 'private-token\\n'))
        (root / 'process_args').write_text(subprocess.check_output(
            ['ps', '-o', 'args=', '-p', str(os.getpid())], text=True))
        print('{}')
    elif rest[:2] == ['run', '--agent'] or rest[:2] == ['run', '--session']:
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
        state = os.environ.get('SESSION_STATE', state)
        since = (datetime.now(timezone.utc) - timedelta(minutes=float(os.environ.get('STATE_AGE_MINUTES', 1)))).isoformat()
        successor = os.environ.get('SUCCESSOR', '')
        if successor and ':' in successor:
            old, new = successor.split(':', 1)
            print(json.dumps({'session_id':old,'state':'completed','state_since':since,'agent_name':'worker-1','next_session':new}))
            print(json.dumps({'session_id':new,'state':state,'state_since':since,'agent_name':'worker-1'}))
        else:
            print(json.dumps({'session_id':'01AAAA','state':state,'state_since':since,'agent_name':'worker-1'}))
    elif rest[:2] == ['session', 'show']:
        sid = rest[2] if len(rest) > 2 else ''
        if sid not in ('01AAAA', '01BBBB'):
            print(json.dumps({'state':'waiting_for_inference','reasons':[]}))
        else:
            if not (root / 'interrupted').exists() and not os.environ.get('SUCCESSOR'):
                print(json.dumps({'state':'waiting_for_inference','reasons':json.loads(os.environ.get('WAIT_REASONS', '["429 rate limited"]'))}))
            if os.environ.get('PRESSURE', '') == '1':
                print(json.dumps({'message_appended': {'message': {'role':'system',
                    'parts':[{'text':{'text':'context_pressure: input 80 tokens at 75 percent'}}]}}}))
            if os.environ.get('SUCCESSOR') and sid == '01AAAA':
                text = os.environ.get('OLD_MESSAGE', 'Working, no link yet')
            else:
                text = os.environ.get('LAST_MESSAGE', 'Done https://github.com/pachuc/swarmy/pull/42')
            print(json.dumps({'inference_completed': {'message': {'role':'assistant',
                'parts':[{'text':{'text':text}}]}}}))
    elif rest[:2] == ['session', 'metrics']:
        # Fake durable per-turn records for fleet report tests. REPORT_METRICS
        # maps a session id to its JSON array of turn records.
        mapping = json.loads(os.environ.get('REPORT_METRICS', '{}'))
        sid = rest[2] if len(rest) > 2 else ''
        print(json.dumps(mapping.get(sid, [])))
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
        self.assertIn("--new", run_call)
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
        self.assertEqual(json.loads((self.root / "state" / "ewr2hd.json").read_text())["pr_source"],
                         "last_message")
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

    def test_empty_remote_uses_the_local_api_configuration(self):
        # On a control node the driver runs without a tunnel profile.
        self.config.write_text(self.config.read_text().replace('remote = "dev"', 'remote = ""'))
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0, "launch failed")
        swarmy_calls = [call for call in self.calls() if call[:1] in (["agent"], ["run"], ["session"])]
        self.assertTrue(swarmy_calls, "no swarmy calls recorded")
        self.assertFalse(any("--remote" in call for call in self.calls()))

    def test_benchmark_local_remote_drops_the_flag(self):
        prompt = self.root / "prompt.txt"
        prompt.write_text("Print BENCH_COLD=true")
        for remote, expect_flag in (("dev", True), ("local", False), ("", False)):
            result = self.call("benchmark", "--remote", remote, "--provider", "fake", "--model", "fake",
                               "--effort", "low", "--prompt-file", str(prompt))
            self.assertEqual(result.returncode, 0, result.stderr)
            run_calls = [call for call in self.calls() if "run" in call[:3]]
            self.assertTrue(run_calls)
            self.assertEqual("--remote" in run_calls[-1], expect_flag)
        result = self.call("benchmark", "--remote", "local", "--provider", "fake", "--model", "fake",
                           "--effort", "low", "--prompt-file", str(prompt), "--image", "swarmy-dev:dev")
        self.assertEqual(result.returncode, 0, result.stderr)
        last = [call for call in self.calls() if "run" in call[:3]][-1]
        self.assertIn("--image", last)
        self.assertEqual(last[last.index("--image") + 1], "swarmy-dev:dev")

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
        # Idle worker-2 is reused first; the launch after it recreates worker-1
        # rather than colliding with the live worker-2.
        again = self.call("launch", "AAAAA5")
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertIn("worker-2", again.stdout)
        recreated = self.call("launch", "AAAAA6")
        self.assertEqual(recreated.returncode, 0, recreated.stderr)
        self.assertIn("worker-1", recreated.stdout)
        self.assertEqual(self.creates()[-1][4], "worker-1")

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

    def test_relaunch_of_in_progress_task_skips_start(self):
        env = dict(self.env, TASK_STATUS="in_progress")
        result = self.call("launch", "EWR2HD", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(call[:3] == ["task", "start", "EWR2HD"] for call in self.calls()))

    def test_full_task_id_uses_the_six_character_suffix(self):
        result = self.call("launch", "01M38DCQ5BFPXNPNF0NWRXRZH2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.root / "state" / "rxrzh2.json").exists())
        self.assertIn("swarmy/rxrzh2", (self.root / "prompt-worker-1").read_text())

    def test_collect_refuses_missing_open_pr(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        env = dict(self.env, PR_STATE="CLOSED")
        result = self.call("collect", "EWR2HD", env=env)
        self.assertEqual(result.returncode, 1)
        self.assertIn("not open", result.stderr)
        self.assertFalse(any(call[:2] == ["task", "pr"] for call in self.calls()))

    def test_status_exposes_state_age_and_stalls_without_wait_reasons(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        old = dict(self.env, SESSION_STATE="waiting_inference", STATE_AGE_MINUTES="35", WAIT_REASONS="[]")
        stalled = self.call("status", env=old)
        self.assertEqual(stalled.returncode, 2, stalled.stderr)
        self.assertIn("STALLED waiting_inference 35m", stalled.stdout)
        self.assertNotIn("waiting for inference:", stalled.stdout)
        fresh = dict(old, STATE_AGE_MINUTES="2")
        running = self.call("status", env=fresh)
        self.assertEqual(running.returncode, 0, running.stderr)
        self.assertIn("waiting_inference 2m", running.stdout)
        self.assertNotIn("STALLED", running.stdout)
        leased = self.call("status", env=dict(old, SESSION_STATE="leased"))
        self.assertEqual(leased.returncode, 2, leased.stderr)
        self.assertIn("STALLED leased 35m", leased.stdout)
        self.assertNotIn("waiting for inference:", leased.stdout)
        self.config.write_text(self.config.read_text() + "stall_minutes = 3\n")
        custom = self.call("status", env=dict(old, STATE_AGE_MINUTES="4"))
        self.assertEqual(custom.returncode, 2, custom.stderr)
        self.assertIn("STALLED waiting_inference 4m", custom.stdout)

    def test_collect_uses_exactly_one_open_or_merged_branch_pr_when_message_has_no_url(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        url = "https://github.com/pachuc/swarmy/pull/42"
        env = dict(self.env, LAST_MESSAGE="Finished without a link",
                   PR_LIST=json.dumps([{"url": url, "state": "OPEN"}]))
        result = self.call("collect", "EWR2HD", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(url, result.stdout)
        lookup = next(call for call in self.calls() if call[:2] == ["pr", "list"])
        self.assertIn("swarmy/ewr2hd", lookup)
        saved = json.loads((self.root / "state" / "ewr2hd.json").read_text())
        self.assertEqual((saved["pr_url"], saved["pr_source"]), (url, "branch_lookup"))
        self.assertIn(["task", "pr", "EWR2HD", url], self.calls())

    def test_collect_fallback_rejects_zero_or_multiple_prs(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        url = "https://github.com/pachuc/swarmy/pull/42"
        for prs, expected in [([], "found 0"),
                              ([{"url": url, "state": "OPEN"},
                                {"url": url + "3", "state": "MERGED"}], "found 2")]:
            env = dict(self.env, LAST_MESSAGE="Finished without a link", PR_LIST=json.dumps(prs))
            result = self.call("collect", "EWR2HD", env=env)
            self.assertEqual(result.returncode, 1)
            self.assertIn(expected, result.stderr)
        self.assertFalse(any(call[:2] == ["task", "pr"] for call in self.calls()))

    def test_collect_accepts_merged_pr_from_branch_lookup(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        url = "https://github.com/pachuc/swarmy/pull/42"
        env = dict(self.env, LAST_MESSAGE="Finished without a link", PR_STATE="MERGED",
                   PR_LIST=json.dumps([{"url": url, "state": "MERGED"}]))
        result = self.call("collect", "EWR2HD", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads((self.root / "state" / "ewr2hd.json").read_text())["pr_source"],
                         "branch_lookup")

    def test_release_and_kill_match_short_and_full_task_ids(self):
        full = "01M38DCQ5BFPXNPNF0NWEWR2HD"
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        self.assertEqual(self.call("release", full, "--force").returncode, 0)
        self.assertEqual(json.loads((self.root / "state" / "workers.json").read_text()),
                         {"worker-1": None})
        self.assertEqual(self.call("launch", full).returncode, 0)
        self.assertEqual(self.call("release", "EWR2HD", "--force").returncode, 0)
        self.assertEqual(json.loads((self.root / "state" / "workers.json").read_text()),
                         {"worker-1": None})
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        self.assertEqual(self.call("kill", full, "--timeout-seconds", "2").returncode, 0)
        self.assertEqual(json.loads((self.root / "state" / "workers.json").read_text()),
                         {"worker-1": None})

    def test_config_requires_owner_only_permissions(self):
        self.config.chmod(0o644)
        result = self.call("status")
        self.assertEqual(result.returncode, 1)
        self.assertIn("owner-only", result.stderr)

    def report_env(self):
        """Two fake sessions with hand-computed aggregates for report tests."""
        def request(input_tokens, cached, output, reasoning, micros, tps,
                    retries=0, waits=(0, 0, 0), error=None, streamed=True):
            return {"request_id": "r", "provider": "fake", "model": "fake",
                    "input_tokens": input_tokens, "cached_input_tokens": cached,
                    "output_tokens": output, "reasoning_tokens": reasoning,
                    "cost_micros": micros, "output_tokens_per_second": tps,
                    "retries": retries, "rate_limit_waits": waits[0],
                    "gateway_waits": waits[1], "provider_failures": waits[2],
                    "error": error, "streamed": streamed}

        def tool(dispatched_ns, completed_ns):
            return {"request_id": "t", "name": "bash",
                    "dispatched_ns": dispatched_ns, "completed_ns": completed_ns}

        def turn(a2f, infer, idle, requests, tools, computer, error=None):
            return {"session_id": "SESA", "turn_id": "t",
                    "append_to_first_token_ms": a2f, "inference_duration_ms": infer,
                    "append_to_idle_ms": idle, "inference": requests,
                    "tools": tools, "computer": computer, "error": error}

        sesa = [
            turn(100.0, 200.0, 1000.0, [request(100, 10, 50, 5, 2000, 10.0, retries=1, waits=(1, 0, 0), streamed=False)],
                 [tool(0, 100_000_000)],
                 {"placement_ms": 50.0, "cold": True, "chunks_fetched": 4, "bytes_fetched": 1000}),
            turn(200.0, 300.0, 2000.0, [request(200, 20, 150, 15, 4000, 20.0)],
                 [tool(0, 200_000_000), tool(0, 300_000_000)],
                 {"placement_ms": 150.0, "cold": False, "chunks_fetched": 2, "bytes_fetched": 500}),
            turn(300.0, 400.0, 3000.0, [request(300, 30, 100, 10, 6000, 30.0, retries=2, waits=(0, 2, 1), error="boom", streamed=None)],
                 [], None, error="boom"),
        ]
        sesb = [
            turn(1000.0, 2000.0, 5000.0, [request(1000, 0, 500, 0, 100000, 50.0)],
                 [tool(0, 1_000_000_000)],
                 {"placement_ms": 500.0, "cold": False, "chunks_fetched": 1, "bytes_fetched": 100}),
        ]
        return dict(self.env, REPORT_METRICS=json.dumps({"SESA": sesa, "SESB": sesb}))

    def test_report_table_and_json_with_hand_computed_percentiles(self):
        env = self.report_env()
        table = self.call("report", "--session", "SESA", "--session", "SESB", env=env)
        self.assertEqual(table.returncode, 0, table.stderr)
        # Percentiles use the store rollup's nearest rank: three append-to-idle
        # samples [100, 200, 300] give p50 200 and p95 300. No --wall flags
        # were passed, so wall_s prints as missing next to duration_s.
        self.assertIn("| SESA | 3 | 6.0 | - | 200.0 | 300.0 | 300.0 | 400.0 | 20.0 | 30.0 | 1 | 3 | 200.0 | 300.0 |", table.stdout)
        self.assertIn("| SESB | 1 | 5.0 | - | 1000.0 | 1000.0 | 2000.0 | 2000.0 | 50.0 | 50.0 | 0 | 1 | 1000.0 | 1000.0 |", table.stdout)
        # The totals row pools every turn: four append-to-first-token samples
        # [100, 200, 300, 1000] give p50 300 and p95 1000.
        self.assertIn("| total | 4 | 11.0 | - | 300.0 | 1000.0 | 400.0 | 2000.0 | 27.5 | 50.0 | 1 | 4 | 300.0 | 1000.0 |", table.stdout)
        as_json = self.call("report", "--session", "SESA", "--session", "SESB", "--json", env=env)
        self.assertEqual(as_json.returncode, 0, as_json.stderr)
        payload = json.loads(as_json.stdout)
        first, second, total = payload["rows"][0], payload["rows"][1], payload["total"]
        self.assertEqual((first["turns"], first["waits"], first["retries"], first["errors"]), (3, 4, 3, 2))
        self.assertEqual((first["input_tokens"], first["cached_input_tokens"],
                          first["output_tokens"], first["reasoning_tokens"]), (600, 60, 300, 30))
        self.assertAlmostEqual(first["cost_dollars"], 0.012)
        self.assertEqual((first["place_cold"], first["place_warm"]), (1, 1))
        self.assertEqual(first["place_p50_ms"], 150.0)
        self.assertEqual((first["chunks_fetched"], first["bytes_fetched"]), (6, 1500))
        self.assertEqual(first["single_chunk"], 1)
        self.assertEqual(total["single_chunk"], 1)
        self.assertEqual((total["turns"], total["waits"], total["retries"], total["errors"]), (4, 4, 3, 2))
        self.assertEqual((total["input_tokens"], total["output_tokens"]), (1600, 800))
        self.assertAlmostEqual(total["cost_dollars"], 0.112)
        self.assertAlmostEqual(total["tps_mean"], 27.5)
        self.assertEqual(second["session_id"], "SESB")
        self.assertIsNone(first["wall_s"])
        self.assertIsNone(total["wall_s"])

    def test_report_wall_seconds_next_to_duration(self):
        env = self.report_env()
        table = self.call("report", "--session", "SESA", "--wall", "SESA=12.5",
                          "--session", "SESB", "--wall", "SESB=7.5", env=env)
        self.assertEqual(table.returncode, 0, table.stderr)
        self.assertIn("| task | turns | duration_s | wall_s | a2f_p50_ms |", table.stdout)
        self.assertIn("| SESA | 3 | 6.0 | 12.5 | 200.0 |", table.stdout)
        self.assertIn("| SESB | 1 | 5.0 | 7.5 | 1000.0 |", table.stdout)
        # The totals row sums the known runner-measured walls.
        self.assertIn("| total | 4 | 11.0 | 20.0 | 300.0 |", table.stdout)
        as_json = self.call("report", "--session", "SESA", "--wall", "SESA=12.5",
                            "--session", "SESB", "--wall", "SESB=7.5", "--json", env=env)
        self.assertEqual(as_json.returncode, 0, as_json.stderr)
        payload = json.loads(as_json.stdout)
        self.assertEqual(payload["rows"][0]["wall_s"], 12.5)
        self.assertEqual(payload["rows"][1]["wall_s"], 7.5)
        self.assertEqual(payload["total"]["wall_s"], 20.0)
        labeled = self.call("report", "--label", "dev", "--session", "SESA", "--wall", "SESA=12.5",
                            "--label", "dev2", "--session", "SESB", "--wall", "SESB=7.5",
                            "--json", env=env)
        self.assertEqual(labeled.returncode, 0, labeled.stderr)
        medians = {row["label"]: row for row in json.loads(labeled.stdout)["medians"]}
        self.assertEqual(medians["dev"]["wall_s"], 12.5)
        self.assertEqual(medians["dev2"]["wall_s"], 7.5)
        bad = self.call("report", "--session", "SESA", "--wall", "SESA=soon", env=env)
        self.assertEqual(bad.returncode, 1)
        self.assertIn("SESSION=SECONDS", bad.stderr)

    def test_report_and_benchmark_run_without_a_config_file(self):
        # Reporting reads only session metrics, so neither action needs the
        # GitHub token or repo that worker actions require.
        env = dict(self.env, FLEET_CONFIG=str(self.root / "absent-fleet.toml"),
                   REPORT_METRICS=json.dumps({"SESA": []}))
        table = self.call("report", "--remote", "local", "--session", "SESA", env=env)
        self.assertEqual(table.returncode, 0, table.stderr)
        self.assertIn("wall_s", table.stdout)
        self.assertIn("| SESA | 0 | 0.0 | - |", table.stdout)
        walled = self.call("report", "--remote", "local", "--session", "SESA",
                           "--wall", "SESA=12.5", env=env)
        self.assertEqual(walled.returncode, 0, walled.stderr)
        self.assertIn("| SESA | 0 | 0.0 | 12.5 |", walled.stdout)
        prompt = self.root / "prompt.txt"
        prompt.write_text("Print BENCH_COLD=true")
        bench = self.call("benchmark", "--remote", "local", "--provider", "fake",
                          "--model", "fake", "--effort", "low",
                          "--prompt-file", str(prompt), env=env)
        self.assertEqual(bench.returncode, 0, bench.stderr)

    def test_auth_actions_still_require_a_token(self):
        self.config.write_text(f'''remote = "dev"\nrepo = "pachuc/swarmy"\nprovider = "fake"\nmodel = "fake"\nworkers = 2\nstate_dir = "{self.root / 'state'}"\n''')
        self.config.chmod(0o600)
        result = self.call("status")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing github_token", result.stderr)

    def test_report_task_ids_and_since_select_assignments(self):
        env = self.report_env()
        self.assertEqual(self.call("launch", "RRRPT1", env=env).returncode, 0)
        self.assertEqual(self.call("launch", "RRRPT2", env=env).returncode, 0)
        state = self.root / "state"
        first = json.loads((state / "rrrpt1.json").read_text())
        first["session_id"] = "SESA"
        first["started_at"] = "2026-09-20T00:00:00+00:00"
        (state / "rrrpt1.json").write_text(json.dumps(first))
        second = json.loads((state / "rrrpt2.json").read_text())
        second["session_id"] = "SESB"
        second["started_at"] = "2026-09-24T00:00:00+00:00"
        (state / "rrrpt2.json").write_text(json.dumps(second))
        both = self.call("report", "RRRPT1", "RRRPT2", env=env)
        self.assertEqual(both.returncode, 0, both.stderr)
        self.assertIn("| RRRPT1 | 3 |", both.stdout)
        self.assertIn("| RRRPT2 | 1 |", both.stdout)
        recent = self.call("report", "--since", "2026-09-23T00:00:00Z", env=env)
        self.assertEqual(recent.returncode, 0, recent.stderr)
        self.assertNotIn("| RRRPT1 |", recent.stdout)
        self.assertIn("| RRRPT2 | 1 |", recent.stdout)

    def test_report_labels_print_side_by_side_tables_and_medians(self):
        env = self.report_env()
        labeled = self.call("report", "--label", "dev", "--session", "SESA",
                            "--label", "dev2", "--session", "SESB", env=env)
        self.assertEqual(labeled.returncode, 0, labeled.stderr)
        self.assertIn("## dev\n", labeled.stdout)
        self.assertIn("## dev2\n", labeled.stdout)
        self.assertIn("## medians\n", labeled.stdout)
        self.assertIn("| SESA | 3 |", labeled.stdout)
        self.assertIn("| SESB | 1 |", labeled.stdout)
        as_json = self.call("report", "--label", "dev", "--session", "SESA",
                            "--label", "dev2", "--session", "SESB", "--json", env=env)
        self.assertEqual(as_json.returncode, 0, as_json.stderr)
        payload = json.loads(as_json.stdout)
        self.assertEqual([table["label"] for table in payload["labels"]], ["dev", "dev2"])
        self.assertEqual(payload["labels"][0]["rows"][0]["turns"], 3)
        self.assertEqual(payload["labels"][0]["total"]["turns"], 3)
        medians = {row["label"]: row for row in payload["medians"]}
        # Each label holds one task, so its medians equal that task's row.
        self.assertEqual(medians["dev"]["turns"], 3)
        self.assertEqual(medians["dev2"]["turns"], 1)
        self.assertEqual(medians["dev"]["a2f_p50_ms"], 200.0)
        self.assertEqual(medians["dev2"]["a2f_p50_ms"], 1000.0)


    def test_report_released_task_resolves_from_launch_log(self):
        env = self.report_env()
        self.assertEqual(self.call("launch", "RRRPT1", env=env).returncode, 0)
        state = self.root / "state"
        log = state / "rrrpt1.jsonl"
        # Point the launch log at the fixture session so aggregates are known.
        log.write_text(log.read_text().replace("01AAAA", "SESA"))
        self.assertEqual(self.call("release", "RRRPT1", "--force", env=env).returncode, 0)
        self.assertFalse((state / "rrrpt1.json").exists())
        self.assertTrue(log.exists())
        table = self.call("report", "RRRPT1", env=env)
        self.assertEqual(table.returncode, 0, table.stderr)
        self.assertIn("| RRRPT1 | 3 |", table.stdout)
        as_json = self.call("report", "RRRPT1", "--json", env=env)
        self.assertEqual(as_json.returncode, 0, as_json.stderr)
        payload = json.loads(as_json.stdout)
        self.assertEqual(payload["rows"][0]["session_id"], "SESA")
        self.assertEqual(payload["rows"][0]["turns"], 3)
        # The released log has a fresh mtime, so an old --since still finds it
        # under its file stem.
        recent = self.call("report", "--since", "2026-09-20T00:00:00Z", env=env)
        self.assertEqual(recent.returncode, 0, recent.stderr)
        self.assertIn("| rrrpt1 | 3 |", recent.stdout)
        # An event timestamp takes precedence over the file mtime: backdate
        # the first event and the same --since stops matching.
        lines = log.read_text().splitlines()
        first = json.loads(lines[0])
        first["timestamp"] = "2020-01-01T00:00:00Z"
        lines[0] = json.dumps(first)
        log.write_text("\n".join(lines) + "\n")
        stale = self.call("report", "--since", "2026-09-20T00:00:00Z", env=env)
        self.assertEqual(stale.returncode, 1)
        self.assertIn("no tasks launched since", stale.stderr)


    def test_status_and_collect_follow_side_successor(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        env = dict(self.env, SUCCESSOR="01AAAA:01BBBB",
                   SESSION_STATE="idle",
                   OLD_MESSAGE="Working, no link yet",
                   LAST_MESSAGE="Done https://github.com/pachuc/swarmy/pull/42",
                   PR_LIST="[]")
        status = self.call("status", env=env)
        self.assertEqual(status.returncode, 0, status.stderr)
        # The old session is completed; status reports the successor state.
        self.assertNotIn("completed", status.stdout)
        self.assertIn("idle", status.stdout)
        collect = self.call("collect", "EWR2HD", env=env)
        self.assertEqual(collect.returncode, 0, collect.stderr)
        self.assertIn("https://github.com/pachuc/swarmy/pull/42", collect.stdout)

    def test_status_shows_context_pressure_warning(self):
        self.assertEqual(self.call("launch", "EWR2HD").returncode, 0)
        env = dict(self.env, PRESSURE="1", SESSION_STATE="sleeping")
        status = self.call("status", env=env)
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertIn("context_pressure", status.stdout)

    def test_resolve_helpers_follow_chain_and_pressure(self):
        import importlib.util
        spec = importlib.util.spec_from_loader("fleetmod", loader=None)
        import types
        source = Path("scripts/fleet/fleet").read_text()
        mod = types.ModuleType("fleetmod")
        mod.__dict__["__file__"] = str(Path("scripts/fleet/fleet").resolve())
        exec(compile(source, "fleet", "exec"), mod.__dict__)
        sessions = {
            "01AAAA": {"session_id": "01AAAA", "state": "completed", "next_session": "01BBBB"},
            "01BBBB": {"session_id": "01BBBB", "state": "idle"},
        }
        self.assertEqual(mod.current_session({}, "01AAAA", sessions), "01BBBB")
        self.assertEqual(mod.current_session({}, "01BBBB", sessions), "01BBBB")
        warning = {"message_appended": {"message": {"role": "system",
                   "parts": [{"text": {"text": "context_pressure: input 80 tokens"}}]}}}
        tool_output = {"message_appended": {"message": {"role": "tool",
                       "parts": [{"tool_result": {"result": {"completed": {"output": "docs say context_pressure"}}}}]}}}
        user_text = {"message_appended": {"message": {"role": "user",
                     "parts": [{"text": {"text": "context_pressure in a prompt"}}]}}}
        self.assertTrue(mod.has_pressure([warning]))
        self.assertFalse(mod.has_pressure([tool_output, user_text]))
        self.assertFalse(mod.has_pressure([{"message_appended": {"text": "hello"}}]))

if __name__ == "__main__":
    unittest.main()
