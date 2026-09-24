"""Exercise the image's CDP helper without needing a root-only display build."""
import base64
import pathlib
import tempfile
import unittest
from unittest import mock

SETUP = pathlib.Path(__file__).resolve().parents[1] / 'setup.sh'
text = SETUP.read_text().split("cat > /usr/local/libexec/swarmy-browser <<'PY'\n", 1)[1].split('\nPY\n', 1)[0]
namespace = {'__name__': 'browser_helper_test'}
exec(compile(text, 'swarmy-browser', 'exec'), namespace)


class FakeCDP(namespace["CDP"]):
    page = 'blank'
    typed = {}
    focused = None
    exception = None
    navigation_error = None
    oversized_depth = None

    def __init__(self):
        pass

    def evaluate(self, expression):
        if expression == 'location.href':
            return 'http://localhost/' + self.page
        if expression == 'document.title':
            return 'Welcome' if self.page == 'logged-in' else 'Login'
        if expression == 'document.readyState':
            return 'complete'
        return None

    def call(self, method, params=None):
        params = params or {}
        if method == 'Page.navigate':
            if FakeCDP.navigation_error:
                return {'errorText': FakeCDP.navigation_error}
            self.page = params['url'].rsplit('/', 1)[-1]
            FakeCDP.page = self.page
            return {}
        if method == 'Accessibility.getFullAXTree':
            if FakeCDP.oversized_depth and params['depth'] >= FakeCDP.oversized_depth:
                raise ValueError('DevTools reply too large')
            nodes = [{'nodeId': 'root', 'role': {'value': 'RootWebArea'}, 'name': {'value': ''}}]
            if self.page == 'logged-in':
                nodes.append({'nodeId': 'welcome', 'parentId': 'root',
                              'role': {'value': 'heading'}, 'name': {'value': 'Signed in'}})
            else:
                for index, (role, name) in enumerate([('textbox', 'Username'),
                                                       ('textbox', 'Password'),
                                                       ('button', 'Log in')], 1):
                    nodes.append({'nodeId': str(index), 'parentId': 'root',
                                  'backendDOMNodeId': index,
                                  'role': {'value': role}, 'name': {'value': name},
                                  'properties': [{'name': 'focused', 'value': {'value': index == 1}}]})
            return {'nodes': nodes}
        if method in ('Accessibility.enable', 'Page.enable', 'DOM.enable', 'Runtime.enable'):
            return {}
        if method == 'DOM.resolveNode':
            return {'object': {'objectId': str(params['backendNodeId'])}}
        if method == 'Runtime.callFunctionOn':
            if FakeCDP.exception:
                return {'exceptionDetails': {'text': FakeCDP.exception}}
            if 'this.focus()' in params['functionDeclaration']:
                FakeCDP.focused = params['objectId']
            if 'requestSubmit()' in params['functionDeclaration']:
                FakeCDP.page = 'logged-in'
            if 'this.click()' in params['functionDeclaration']:
                FakeCDP.page = 'logged-in'
            return {'result': {'value': None}}
        if method == 'Input.insertText':
            FakeCDP.typed[FakeCDP.focused] = params['text']
            return {}
        if method == 'Page.captureScreenshot':
            return {'data': base64.b64encode(b'\x89PNG\r\n\x1a\n').decode()}
        raise AssertionError(method)


class BrowserTests(unittest.TestCase):
    def test_login_and_screenshot(self):
        with tempfile.TemporaryDirectory() as directory:
            namespace['REFS'] = str(pathlib.Path(directory) / 'refs.json')
            namespace['CDP'] = FakeCDP
            FakeCDP.page = 'blank'
            FakeCDP.typed = {}
            FakeCDP.exception = None
            FakeCDP.navigation_error = None
            FakeCDP.oversized_depth = None
            run = namespace['run']
            first = run('browser_navigate', {'url': 'http://localhost/login'})['output']
            self.assertIn('[e1] textbox "Username"', first)
            self.assertIn('[e3] button "Log in"', first)
            self.assertIn('focused=true', first)
            self.assertIn('expire on navigation', first)
            FakeCDP.oversized_depth = 8
            self.assertIn('depth 4 (truncated)', run('browser_snapshot', {})['output'])
            FakeCDP.oversized_depth = None
            run('browser_type', {'ref': 'e1', 'text': 'alice', 'submit': False})
            self.assertEqual(FakeCDP.typed['1'], 'alice')
            run('browser_click', {'ref': 'e3'})
            self.assertIn('Signed in', run('browser_snapshot', {})['output'])
            shot = run('browser_screenshot', {})
            self.assertEqual(shot['image_media_type'], 'image/png')
            self.assertEqual(base64.b64decode(shot['image_base64']), b'\x89PNG\r\n\x1a\n')
            with self.assertRaises(ValueError):
                run('browser_click', {'ref': 'e1'})

    def test_action_exception_and_navigation_error(self):
        with tempfile.TemporaryDirectory() as directory:
            namespace['REFS'] = str(pathlib.Path(directory) / 'refs.json')
            namespace['CDP'] = FakeCDP
            FakeCDP.page = 'blank'
            FakeCDP.exception = None
            FakeCDP.navigation_error = None
            FakeCDP.oversized_depth = None
            run = namespace['run']
            run('browser_snapshot', {})
            FakeCDP.exception = 'click failed'
            with self.assertRaisesRegex(ValueError, 'JavaScript exception: click failed'):
                run('browser_click', {'ref': 'e3'})
            FakeCDP.exception = None
            FakeCDP.navigation_error = 'net::ERR_CONNECTION_REFUSED'
            with self.assertRaisesRegex(ValueError, 'ERR_CONNECTION_REFUSED'):
                run('browser_navigate', {'url': 'http://localhost/missing'})
            FakeCDP.navigation_error = None

    def test_screen_tools_include_non_browser_windows(self):
        run = namespace['run']
        with mock.patch.object(namespace['subprocess'], 'check_output', return_value='0x001 Blender\n'):
            self.assertIn('Blender', run('screen_windows', {})['output'])
        def capture(args, **_kwargs):
            pathlib.Path(args[-1]).write_bytes(b'\x89PNG\r\n\x1a\n')
        with mock.patch.object(namespace['subprocess'], 'run', side_effect=capture):
            result = run('screen_screenshot', {})
            self.assertEqual(base64.b64decode(result['image_base64']), b'\x89PNG\r\n\x1a\n')

    def test_size_limits(self):
        self.assertIn('truncated', namespace['preview']('x' * 100000))
        with self.assertRaises(ValueError):
            namespace['image'](b'x' * (5 * 1024 * 1024 + 1))


if __name__ == '__main__':
    unittest.main()
