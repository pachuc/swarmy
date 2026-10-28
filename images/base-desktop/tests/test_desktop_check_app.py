import http.cookiejar
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from desktop_check_app import ALPHABET, GLYPHS, App


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class AppTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.record = os.path.join(self.tmp.name, 'record.json')
        self.app = App(('127.0.0.1', 0), self.record, 'desk', 'pass', 'K7MWX3')
        self.addCleanup(self.app.server_close)
        thread = threading.Thread(target=self.app.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.app.shutdown)
        self.url = f'http://127.0.0.1:{self.app.server_port}'
        self.cookies = http.cookiejar.CookieJar()
        self.client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookies), NoRedirect())
        self.plain = urllib.request.build_opener(NoRedirect())

    def request(self, path, data=None, client=None):
        request = urllib.request.Request(self.url + path, data=urllib.parse.urlencode(data).encode() if data else None)
        try:
            return (client or self.client).open(request, timeout=5)
        except urllib.error.HTTPError as error:
            return error

    def state(self):
        with open(self.record) as source:
            return json.load(source)

    def test_login_settings_and_wait(self):
        self.assertEqual(self.request('/wait?event=login&timeout=0').code, 504)
        self.assertIn(b'<h1>Log in</h1>', self.request('/').read())
        self.assertNotIn(b'K7MWX3', self.request('/').read())
        self.assertEqual(self.request('/settings', client=self.plain).code, 303)
        self.request('/login', {'username': 'desk', 'password': 'bad'})
        self.assertEqual(self.state()['failed_logins'], 1)
        self.assertFalse(self.state()['login']['ok'])
        login = self.request('/login', {'username': 'desk', 'password': 'pass'})
        self.assertEqual(login.code, 303)
        self.assertIn('sid=', login.headers['Set-Cookie'])
        self.assertEqual(self.state()['login']['user'], 'desk')
        settings = self.request('/settings').read()
        self.assertNotIn(b'K7MWX3', settings)
        self.assertIn(b'aria-label="Verification code"', settings)
        self.assertIn(b'const R=[', settings)
        self.assertEqual(self.state()['settings_views'], 1)
        self.assertEqual(self.request('/settings', {'theme': 'unknown'}).code, 400)
        self.assertEqual(self.request('/settings', {'theme': 'dark'}).code, 303)
        self.assertTrue(self.state()['setting']['saved'])
        self.assertEqual(self.state()['setting']['theme'], 'dark')
        self.assertIn(b'Theme saved: Dark', self.request('/settings').read())
        self.assertEqual(self.request('/wait?event=setting_saved&timeout=0').code, 200)

    def test_glyphs(self):
        self.assertEqual(set(ALPHABET), set(GLYPHS))
        self.assertEqual(len({tuple(glyph) for glyph in GLYPHS.values()}), len(ALPHABET))
        for glyph in GLYPHS.values():
            self.assertEqual(len(glyph), 7)
            self.assertTrue(all(len(row) == 5 and set(row) <= {'0', '1'} for row in glyph))

    def test_background_ready_before_return(self):
        path = os.path.join(os.path.dirname(__file__), 'desktop_check_app.py')
        pid_file = os.path.join(self.tmp.name, 'pid')
        run = subprocess.run([sys.executable, path, '--record', os.path.join(self.tmp.name, 'background.json'),
                              '--username', 'desk', '--password', 'pass', '--port', '0',
                              '--background', '--log', os.path.join(self.tmp.name, 'app.log'),
                              '--pid-file', pid_file], capture_output=True, text=True, check=True, timeout=5)
        port = int(re.search(r'listening on 127\.0\.0\.1:(\d+)', run.stdout).group(1))
        with open(pid_file) as source:
            pid = int(source.read())
        def stop_background():
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        self.addCleanup(stop_background)
        self.assertEqual(urllib.request.urlopen(f'http://127.0.0.1:{port}/healthz', timeout=5).read(), b'ok')
