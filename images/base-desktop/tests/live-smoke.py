"""Run from a desktop sandbox to exercise browser and whole-display tools."""
import base64
import http.server
import json
import os
import re
import subprocess
import threading
import time

HELPER = os.environ.get('SWARMY_BROWSER_HELPER', '/usr/local/libexec/swarmy-browser')


class App(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        if self.path == '/welcome':
            body = '<html><title>Dashboard</title><h1>Signed in</h1></html>'
        elif self.path == '/select':
            body = '<html><title>Select</title><label>Color <select><option value="red">Red</option><option value="blue">Blue</option></select></label></html>'
        else:
            body = ('<html><title>Login</title><form method="POST" action="/login">'
                    '<label>Username <input name="username"></label>'
                    '<label>Password <input name="password" type="password"></label>'
                    '<button type="submit">Log in</button></form></html>')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write(body.encode())

    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length', 0)))
        self.send_response(303)
        self.send_header('Location', '/welcome')
        self.end_headers()


def call(name, arguments):
    result = subprocess.run(['/usr/bin/python3', HELPER, name], input=json.dumps(arguments),
                            text=True, capture_output=True, timeout=45)
    if result.returncode:
        raise RuntimeError(name + ': ' + result.stderr)
    return json.loads(result.stdout)


def main():
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), App)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = 'http://127.0.0.1:%d' % server.server_port
    login = call('browser_navigate', {'url': base + '/login'})['output']
    refs = dict((name, ref) for ref, name in
                re.findall(r'\[(e\d+)\] (?:textbox|button) "([^"]+)"', login))
    assert all(name in refs for name in ('Username', 'Password', 'Log in')), login
    call('browser_type', {'ref': refs['Username'], 'text': 'alice', 'submit': False})
    call('browser_type', {'ref': refs['Password'], 'text': 'secret', 'submit': False})
    call('browser_click', {'ref': refs['Log in']})
    for _ in range(25):
        logged_in = call('browser_snapshot', {})['output']
        if 'Signed in' in logged_in:
            break
        time.sleep(0.2)
    assert 'Signed in' in logged_in, logged_in
    assert base + '/welcome' in logged_in
    shot = call('browser_screenshot', {})
    assert base64.b64decode(shot['image_base64']).startswith(b'\x89PNG\r\n\x1a\n')
    select = call('browser_navigate', {'url': base + '/select'})['output']
    reference = re.search(r'\[(e\d+)\] combobox "Color"', select)
    assert reference, select
    call('browser_select', {'ref': reference.group(1), 'value': 'blue'})
    assert call('browser_evaluate', {'js': 'document.querySelector("select").value'})['output'] == '"blue"'
    call('browser_scroll', {'direction': 'down'})
    blender = subprocess.Popen(['blender', '--factory-startup'], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            windows = call('screen_windows', {})['output']
            if 'Blender' in windows:
                break
            time.sleep(0.2)
        assert 'Blender' in windows, windows
        screen = call('screen_screenshot', {})
        assert base64.b64decode(screen['image_base64']).startswith(b'\x89PNG\r\n\x1a\n')
    finally:
        blender.terminate()
        try:
            blender.wait(timeout=5)
        except subprocess.TimeoutExpired:
            blender.kill()
    print('browser login, select, viewport screenshot, Blender window, and display screenshot: ok')


if __name__ == '__main__':
    main()
