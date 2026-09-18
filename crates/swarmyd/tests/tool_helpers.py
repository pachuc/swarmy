import http.server
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

SOURCE = Path(__file__).resolve().parents[1] / 'src'


class Processes(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.script = (SOURCE / 'processes.py').read_text().replace(
            "Path('/var/lib/swarmy/processes')", f'Path({str(self.root / "processes")!r})').replace(
            "Path('/home/agent/.swarmy/output')", f'Path({str(self.root / "output")!r})')

    def tearDown(self):
        for path in self.root.glob('processes/*/record.json'):
            record = json.loads(path.read_text())
            try:
                os.killpg(record['pid'], signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.temp.cleanup()

    def call(self, action, process_id='', command='', epoch=1, **options):
        result = subprocess.run(['python3', '-c', self.script, action, str(epoch), process_id,
                                 command, json.dumps(options)], capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        return json.loads(result.stdout)

    def bash(self, command, **options):
        process_id = uuid.uuid4().hex
        return self.call('bash', process_id, command, call_id=process_id, **options)

    def test_yield_timeout_and_stdin(self):
        started = time.monotonic()
        result = self.bash('echo ready; read -r line; printf "received:%s" "$line"', yield_seconds=1)
        self.assertLess(time.monotonic() - started, 3)
        self.assertTrue(result['backgrounded'])
        self.assertFalse(result['timed_out'])
        self.assertEqual(result['stdout'], 'ready\n')
        self.assertEqual(self.call('process_list')[0]['status'], 'running')
        self.assertEqual(self.call('write_stdin', result['process_id'], 'hello\n')['bytes_written'], 6)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            log = self.call('process_log', result['process_id'])
            if log['status'] == 'exited':
                break
        self.assertEqual(log['output'], 'ready\nreceived:hello')
        self.assertEqual(log['status'], 'exited')
        result = self.bash('sleep 300', timeout_ms=100)
        self.assertTrue(result['timed_out'])
        self.assertTrue(result['backgrounded'])
        self.assertIn('timeout', result['status'])
        self.assertEqual(self.call('process_log', result['process_id'])['status'], 'running')
        self.call('process_stop', result['process_id'])
        self.assertEqual(self.call('process_log', result['process_id'])['status'], 'exited')

    def test_head_tail_spill_exit_and_separate_streams(self):
        result = self.bash("printf out; printf err >&2; exit 7")
        self.assertEqual((result['stdout'], result['stderr'], result['exit_code']), ('out', 'err', 7))
        result = self.bash("printf HEAD; head -c 70000 /dev/zero | tr '\\0' x; printf TAIL", output_budget_bytes=1024)
        output = result['stdout']
        self.assertTrue(output.startswith('HEAD'))
        self.assertTrue(output.endswith('TAIL'))
        self.assertIn(f"{result['elided_bytes']} bytes elided; full output at {result['log_path']}", output)
        self.assertLessEqual(len(output.encode()), 1024)
        self.assertEqual(Path(result['log_path']).read_bytes(), b'HEAD' + b'x' * 70000 + b'TAIL')
        log = self.call('process_log', result['process_id'])
        self.assertTrue(log['output'].startswith('HEAD'))
        self.assertTrue(log['output'].endswith('TAIL'))
        self.assertLessEqual(len(log['output'].encode()), 32768)
        self.assertEqual(self.call('process_list', epoch=2)[0]['status'], 'restarted')

    def test_full_stdin_pipe_returns_without_blocking(self):
        result = self.bash('sleep 300', yield_seconds=0)
        text = 'x' * 100000
        start = time.monotonic()
        written = self.call('write_stdin', result['process_id'], text)['bytes_written']
        self.assertGreater(written, 0)
        self.assertLess(written, len(text))
        self.assertEqual(self.call('write_stdin', result['process_id'], text)['bytes_written'], 0)
        self.assertLess(time.monotonic() - start, 2)
        self.call('process_stop', result['process_id'])
        result = subprocess.run(['python3', '-c', self.script, 'write_stdin', '1', result['process_id'], 'bad'], capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'has exited', result.stderr)

    def test_unicode_and_invalid_bytes_stay_bounded(self):
        for command in ["python3 -c 'print(\"🙂\" * 20000)'", "head -c 1000 /dev/zero | tr '\\0' '\\377'"]:
            result = self.bash(command, output_budget_bytes=1024)
            self.assertLessEqual(len(result['stdout'].encode()) + len(result['stderr'].encode()), 1024)
            self.assertTrue(Path(result['log_path']).exists())

    def test_process_start_accepts_input_and_rejects_stale_epoch(self):
        process_id = uuid.uuid4().hex
        self.call('process_start', process_id, 'read -r line; echo "$line"')
        self.call('write_stdin', process_id, 'started\n')
        result = subprocess.run(['python3', '-c', self.script, 'write_stdin', '2', process_id, 'bad'], capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'earlier lifetime', result.stderr)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path == '/redirect':
            self.send_response(302)
            self.send_header('Location', '/html')
            self.end_headers()
            return
        if self.path == '/bad-redirect':
            self.send_response(302)
            self.send_header('Location', 'file:///etc/passwd')
            self.end_headers()
            return
        self.send_response(200)
        content = {
            '/html': ('text/html', b'<h1>Hello &amp; world</h1><script>hidden()</script><style>hidden</style><p>Good <b>day</b></p>'),
            '/json': ('application/json', b'{"ok": true}\n'),
            '/plain': ('text/plain', b'plain\ntext\n'),
        }
        kind, body = content.get(self.path, ('text/plain', b'x' * (5 * 1024 * 1024 + 1)))
        self.send_header('Content-Type', kind)
        if self.path == '/large':
            self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            if self.path == '/slow':
                # Continuing traffic must not reset the overall 30 second limit.
                for _ in range(70):
                    self.wfile.write(b'.')
                    self.wfile.flush()
                    time.sleep(0.5)
            else:
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


class WebFetch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever)
        cls.thread.start()
        cls.url = f'http://127.0.0.1:{cls.server.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def fetch(self, path):
        return subprocess.run(['python3', str(SOURCE / 'web_fetch.py'), self.url + path],
                              capture_output=True, timeout=35)

    def test_html_redirect_json_and_plain(self):
        for path in ['/html', '/redirect']:
            result = self.fetch(path)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['output'], 'Hello & world\nGood day')
        for path, expected in [('/json', '{"ok": true}\n'), ('/plain', 'plain\ntext\n')]:
            result = self.fetch(path)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['output'], expected)

    def test_size_cap_with_and_without_content_length(self):
        for path in ['/large', '/unknown-length']:
            result = self.fetch(path)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'5 MiB', result.stderr)

    def test_total_timeout_despite_ongoing_traffic(self):
        start = time.monotonic()
        result = self.fetch('/slow')
        elapsed = time.monotonic() - start
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'timeout', result.stderr)
        self.assertGreaterEqual(elapsed, 29)
        self.assertLess(elapsed, 33)

    def test_non_http_schemes_and_redirects_rejected(self):
        result = subprocess.run(['python3', str(SOURCE / 'web_fetch.py'), 'file:///etc/passwd'], capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'only HTTP', result.stderr)
        self.assertNotEqual(self.fetch('/bad-redirect').returncode, 0)


if __name__ == '__main__':
    unittest.main()
