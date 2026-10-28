"""Check the image's Chromium policy and launcher without a root image build."""
import concurrent.futures
import io
import json
import pathlib
import tempfile
import types
import unittest
from unittest import mock

SETUP = pathlib.Path(__file__).resolve().parents[1] / 'setup.sh'


class ChromiumSetupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script = SETUP.read_text()

    def test_managed_policy_disables_password_prompts(self):
        marker = "cat > /etc/chromium/policies/managed/swarmy.json <<'JSON'\n"
        policy = json.loads(self.script.split(marker, 1)[1].split('\nJSON\n', 1)[0])
        self.assertIs(policy['PasswordManagerEnabled'], False)
        self.assertIs(policy['PasswordLeakDetectionEnabled'], False)
        self.assertIs(policy['PromotionalTabsEnabled'], False)
        self.assertIn('chmod 644 /etc/chromium/policies/managed/swarmy.json', self.script)

    def test_concurrent_cold_browser_waits_and_launches_once(self):
        helper = self.script.split("cat > /usr/local/libexec/swarmy-browser <<'PY'\n", 1)[1].split('\nPY\n', 1)[0]
        namespace = {'__name__': 'browser_helper_test'}
        exec(compile(helper, 'swarmy-browser', 'exec'), namespace)
        attempts = []

        def urlopen(_url, timeout):
            attempts.append(timeout)
            if len(attempts) <= 6:
                raise ConnectionRefusedError('browser starting')
            return io.StringIO(json.dumps([{'type': 'page',
                                            'webSocketDebuggerUrl': 'ws://127.0.0.1:9222/devtools/page/1'}]))

        class Socket:
            def settimeout(self, _timeout):
                pass

            def sendall(self, _message):
                pass

            def recv(self, _size):
                return b'HTTP/1.1 101 Switching Protocols\r\n\r\n'

        with tempfile.TemporaryDirectory() as directory:
            namespace['CHROMIUM_START_LOCK'] = str(pathlib.Path(directory) / 'start.lock')
            with mock.patch.object(namespace['urllib'].request, 'urlopen', side_effect=urlopen), \
                 mock.patch.object(namespace['subprocess'], 'Popen',
                                   return_value=types.SimpleNamespace(pid=123)) as launch, \
                 mock.patch.object(namespace['os'], 'kill'), \
                 mock.patch.object(namespace['socket'], 'create_connection', return_value=Socket()), \
                 mock.patch.object(namespace['CDP'], 'call'):
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    clients = list(pool.map(lambda _: namespace['CDP'](), range(2)))
                self.assertEqual(len(clients), 2)
                self.assertGreater(len(attempts), 6)
                launch.assert_called_once()

    def test_launcher_hides_warning_without_dropping_required_flag(self):
        wrapper = self.script.split("cat > /usr/local/bin/chromium <<'SH'\n", 1)[1].split('\nSH\n', 1)[0]
        self.assertIn('chrome --no-sandbox --test-type', wrapper)
        self.assertIn('exec su -p -s /bin/sh agent', wrapper)


if __name__ == '__main__':
    unittest.main()
