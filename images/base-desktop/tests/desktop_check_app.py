"""Small local desktop acceptance app; the verification code exists only as canvas pixels."""
import argparse
import http.server
import json
import os
import secrets
import tempfile
import threading
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

ALPHABET = 'ACEFHKMNPRTVWXY3479'
# Five columns by seven rows; avoid look-alike characters in the alphabet.
GLYPHS = {
    'A': ['01110','10001','10001','11111','10001','10001','10001'],
    'C': ['01111','10000','10000','10000','10000','10000','01111'],
    'E': ['11111','10000','10000','11110','10000','10000','11111'],
    'F': ['11111','10000','10000','11110','10000','10000','10000'],
    'H': ['10001','10001','10001','11111','10001','10001','10001'],
    'K': ['10001','10010','10100','11000','10100','10010','10001'],
    'M': ['10001','11011','10101','10101','10001','10001','10001'],
    'N': ['10001','11001','10101','10011','10001','10001','10001'],
    'P': ['11110','10001','10001','11110','10000','10000','10000'],
    'R': ['11110','10001','10001','11110','10100','10010','10001'],
    'T': ['11111','00100','00100','00100','00100','00100','00100'],
    'V': ['10001','10001','10001','10001','10001','01010','00100'],
    'W': ['10001','10001','10001','10101','10101','10101','01010'],
    'X': ['10001','10001','01010','00100','01010','10001','10001'],
    'Y': ['10001','10001','01010','00100','00100','00100','00100'],
    '3': ['11110','00001','00001','01110','00001','00001','11110'],
    '4': ['00010','00110','01010','10010','11111','00010','00010'],
    '7': ['11111','00001','00010','00100','01000','01000','01000'],
    '9': ['01110','10001','10001','01111','00001','00001','11110'],
}
PAD, CELL = 2, 12
W, H = (PAD * 2 + 6 * 6 - 1) * CELL, (PAD * 2 + 7) * CELL


def now():
    return datetime.now(timezone.utc).isoformat()


class App(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, record_path, username, password, code):
        super().__init__(address, Handler)
        self.record_path = record_path
        self.username, self.password = username, password
        self.sids = set()
        self.condition = threading.Condition()
        self.record = dict(version=1, code=code, username=username, login={'ok': False},
                           failed_logins=0, settings_views=0,
                           setting={'theme': 'light', 'saved': False}, events=[])
        self.save()

    def save(self):
        with self.condition:
            directory = os.path.dirname(os.path.abspath(self.record_path))
            os.makedirs(directory, exist_ok=True)
            fd, path = tempfile.mkstemp(dir=directory)
            try:
                with os.fdopen(fd, 'w') as out:
                    json.dump(self.record, out, indent=2, sort_keys=True)
                os.replace(path, self.record_path)
            finally:
                if os.path.exists(path):
                    os.unlink(path)
            self.condition.notify_all()


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def authenticated(self):
        return any(part.strip().removeprefix('sid=') in self.server.sids
                   for part in self.headers.get('Cookie', '').split(';')
                   if part.strip().startswith('sid='))

    def reply(self, status, body='', headers=()):
        data = body.encode()
        self.send_response(status)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        for key, value in headers:
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def redirect(self, path, headers=()):
        self.reply(303, headers=[('Location', path), *headers])

    def login_page(self, failed=False):
        return ('<title>Login</title><h1>Log in</h1><form method="POST" action="/login">'
                '<label>Username <input name="username"></label>'
                '<label>Password <input name="password" type="password"></label>'
                '<button type="submit">Log in</button></form>'
                + ('<p role="alert">Wrong username or password</p>' if failed else ''))

    def settings_page(self):
        record = self.server.record
        theme = record['setting']['theme']
        label = {'light': 'Light', 'dark': 'Dark', 'contrast': 'High contrast'}
        cells = [[PAD + i * 6 + c, PAD + r]
                 for i, char in enumerate(record['code'])
                 for r, row in enumerate(GLYPHS[char]) for c, pixel in enumerate(row)
                 if pixel == '1']
        options = ''.join(f'<option value="{key}"{(" selected" if key == theme else "")}>{value}</option>'
                          for key, value in label.items())
        saved = f'<p>Theme saved: {label[theme]}</p>' if record['setting']['saved'] else ''
        return (f'<title>Settings</title><h1>Settings</h1>{saved}'
                f'<h2>Verification code</h2><canvas id="code" role="img" aria-label="Verification code" width="{W}" height="{H}"></canvas>'
                f'<script>const R={json.dumps(cells, separators=(",", ":"))};'
                "const g=document.getElementById('code').getContext('2d');"
                f"g.fillStyle='#fff';g.fillRect(0,0,{W},{H});g.fillStyle='#111';"
                'for (const [x,y] of R) g.fillRect(x*12,y*12,12,12);</script>'
                f'<form method="POST" action="/settings"><label>Theme <select name="theme">{options}'
                '</select></label><button type="submit">Save</button></form>')

    def do_GET(self):
        path = urlsplit(self.path)
        if path.path == '/healthz':
            return self.reply(200, 'ok')
        if path.path == '/':
            return self.redirect('/settings') if self.authenticated() else self.reply(200, self.login_page())
        if path.path == '/settings':
            if not self.authenticated():
                return self.redirect('/')
            # Waiters must not see the event before its record is on disk.
            with self.server.condition:
                self.server.record['settings_views'] += 1
                self.server.record['events'].append('settings_view')
                self.server.save()
            return self.reply(200, self.settings_page())
        if path.path == '/wait':
            args = parse_qs(path.query)
            event = args.get('event', [''])[0]
            try:
                timeout = min(60, max(0, float(args.get('timeout', ['20'])[0])))
            except ValueError:
                return self.reply(400)
            if event not in ('login', 'settings_view', 'setting_saved'):
                return self.reply(400)
            with self.server.condition:
                ok = self.server.condition.wait_for(
                    lambda: event in self.server.record['events'], timeout=timeout)
            return self.reply(200 if ok else 504, 'ok' if ok else 'timeout')
        return self.reply(404)

    def do_POST(self):
        path = urlsplit(self.path).path
        form = parse_qs(self.rfile.read(int(self.headers.get('Content-Length', '0'))).decode())
        if path == '/login':
            if (form.get('username') == [self.server.username]
                    and form.get('password') == [self.server.password]):
                sid = secrets.token_hex(16)
                self.server.sids.add(sid)
                with self.server.condition:
                    self.server.record['login'] = {'ok': True, 'user': self.server.username, 'at': now()}
                    self.server.record['events'].append('login')
                    self.server.save()
                return self.redirect('/settings', [('Set-Cookie', f'sid={sid}; HttpOnly; Path=/; SameSite=Strict')])
            with self.server.condition:
                self.server.record['failed_logins'] += 1
                self.server.save()
            return self.reply(200, self.login_page(True))
        if path == '/settings':
            theme = form.get('theme', [''])[0]
            if not self.authenticated() or theme not in ('light', 'dark', 'contrast'):
                return self.reply(400)
            with self.server.condition:
                self.server.record['setting'] = {'theme': theme, 'saved': True, 'saved_at': now()}
                self.server.record['events'].append('setting_saved')
                self.server.save()
            return self.redirect('/settings')
        return self.reply(404)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--record', required=True)
    parser.add_argument('--username', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--code')
    parser.add_argument('--background', action='store_true')
    parser.add_argument('--log')
    parser.add_argument('--pid-file')
    args = parser.parse_args()
    code = args.code or ''.join(secrets.choice(ALPHABET) for _ in range(6))
    if len(code) != 6 or any(char not in ALPHABET for char in code):
        parser.error('--code must be six characters from ALPHABET')
    server = App((args.host, args.port), args.record, args.username, args.password, code)
    if args.background:
        if not args.log:
            parser.error('--background requires --log')
        pid = os.fork()
        if pid:
            print(f'desktop-check app listening on {args.host}:{server.server_port} pid {pid}', flush=True)
            os._exit(0)
        os.setsid()
        null = os.open('/dev/null', os.O_RDONLY)
        os.dup2(null, 0)
        log = os.open(args.log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        os.dup2(log, 1)
        os.dup2(log, 2)
        if args.pid_file:
            with open(args.pid_file, 'w') as out:
                out.write(str(os.getpid()))
    else:
        print(f'desktop-check app listening on {args.host}:{server.server_port} pid {os.getpid()}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
