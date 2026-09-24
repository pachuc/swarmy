set -eu

if [ "$(uname -m)" != x86_64 ]; then
    echo 'base-desktop requires x86-64 (Chromium snapshot and lavapipe ICD)' >&2
    exit 1
fi

# Ubuntu's chromium-browser package is a snap launcher, which cannot work in
# this container. Pin an official Chromium snapshot instead of installing snapd.
revision=1704040
curl --retry 3 -fsSL "https://storage.googleapis.com/chromium-browser-snapshots/Linux_x64/$revision/chrome-linux.zip" -o /tmp/chromium.zip
printf '%s  %s\n' '3077c26bcc11b1e8abad762122a26698e3d620d97dd7d9620a22c52fa9eeb2fa' /tmp/chromium.zip | sha256sum -c -
unzip -q /tmp/chromium.zip -d /opt
rm /tmp/chromium.zip
cat > /usr/local/bin/chromium <<'SH'
#!/bin/sh
set -eu
export DISPLAY=${DISPLAY:-:99}
export LIBGL_ALWAYS_SOFTWARE=1
export VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.x86_64.json
# The sandbox is already isolated by runc; Chromium's namespace sandbox is
# unavailable there. Do not run the browser as root.
if [ "$(id -u)" = 0 ]; then
    exec su -p -s /bin/sh agent -c 'exec /usr/local/bin/chromium "$@"' chromium "$@"
fi
exec /opt/chrome-linux/chrome --no-sandbox --no-first-run --no-default-browser-check \
    --disable-background-networking --disable-component-update \
    --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 \
    --remote-allow-origins=http://127.0.0.1 \
    --user-data-dir="${CHROMIUM_USER_DATA_DIR:-/home/agent/.config/chromium-debug}" "$@"
SH
chmod 755 /usr/local/bin/chromium
mkdir -p /etc/swarmy
cat > /etc/swarmy/environment <<'ENV'
DISPLAY=:99
LIBGL_ALWAYS_SOFTWARE=1
GALLIUM_DRIVER=llvmpipe
VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.x86_64.json
ENV
cat > /usr/local/libexec/swarmy-init <<'SH'
#!/bin/sh
# PID 1 keeps the display alive even if an application kills Xvfb.
set -u
export DISPLAY=:99
export LIBGL_ALWAYS_SOFTWARE=1
export GALLIUM_DRIVER=llvmpipe
export VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.x86_64.json
trap 'kill "$xpid" "$wpid" 2>/dev/null || :; wait 2>/dev/null || :; exit 0' TERM INT
while :; do
    mkdir -p /tmp/.X11-unix
    chmod 1777 /tmp/.X11-unix
    rm -f /tmp/.X99-lock /tmp/.X11-unix/X99
    setpriv --reuid agent --regid agent --clear-groups Xvfb :99 -screen 0 1280x800x24 -nolisten tcp &
    xpid=$!
    # Wait until clients can connect before starting the window manager.
    while kill -0 "$xpid" 2>/dev/null && ! xdpyinfo -display :99 >/dev/null 2>&1; do
        sleep 0.2
    done
    if ! kill -0 "$xpid" 2>/dev/null; then
        wait "$xpid" 2>/dev/null || :
        sleep 1
        continue
    fi
    setpriv --reuid agent --regid agent --clear-groups openbox &
    wpid=$!
    wait "$xpid" 2>/dev/null || :
    kill "$wpid" 2>/dev/null || :
    wait "$wpid" 2>/dev/null || :
    sleep 1
done
SH
chmod 755 /usr/local/libexec/swarmy-init
cat > /usr/local/libexec/swarmy-browser <<'PY'
#!/usr/bin/python3
"""Small local CDP client. Browser state lives in Chromium, not this process."""
import base64
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request

PORT = 'http://127.0.0.1:9222'
REFS = '/tmp/swarmy-browser-refs.json'
LIMIT = 32768
IMAGE_LIMIT = 5 * 1024 * 1024


def preview(text):
    data = text.encode()
    if len(data) <= LIMIT:
        return text
    omitted = len(data) - LIMIT + 100
    return (data[:LIMIT // 2 - 100].decode(errors='replace') +
            '\n[... %d bytes elided; page truncated ...]\n' % omitted +
            data[-LIMIT // 2:].decode(errors='replace'))


def image(data):
    if len(data) > IMAGE_LIMIT:
        raise ValueError('screenshot exceeds 5 MiB; reduce the display size')
    return {'image_base64': base64.b64encode(data).decode(),
            'image_media_type': 'image/png', 'output': 'PNG screenshot'}


class CDP:
    def __init__(self):
        pages = None
        for attempt in range(30):
            try:
                pages = json.load(urllib.request.urlopen(PORT + '/json', timeout=1))
                break
            except (OSError, ValueError):
                if attempt == 0:
                    subprocess.Popen(['chromium', 'about:blank'], stdin=subprocess.DEVNULL,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                     start_new_session=True)
                time.sleep(0.2)
        if pages is None:
            raise ValueError('Chromium debugging port 9222 is unavailable')
        page = next((p for p in pages if p.get('type') == 'page'), None)
        if page is None:
            raise ValueError('Chromium has no page target')
        from urllib.parse import urlsplit
        url = urlsplit(page['webSocketDebuggerUrl'])
        self.sock = socket.create_connection(('127.0.0.1', url.port), timeout=10)
        self.sock.settimeout(10)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall(('GET %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n'
                           'Upgrade: websocket\r\nConnection: Upgrade\r\n'
                           'Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n'
                           'Origin: http://127.0.0.1\r\n\r\n' %
                           (url.path, url.port, key)).encode())
        response = b''
        while b'\r\n\r\n' not in response and len(response) < 8192:
            response += self.sock.recv(1024)
        if b' 101 ' not in response.split(b'\r\n', 1)[0]:
            raise ValueError('Chromium rejected the DevTools connection')
        self.remaining = response.split(b'\r\n\r\n', 1)[1]
        self.seq = 0
        self.call('Page.enable')
        self.call('Runtime.enable')
        self.call('DOM.enable')

    def read(self, n):
        while len(self.remaining) < n:
            chunk = self.sock.recv(max(4096, n - len(self.remaining)))
            if not chunk:
                raise ValueError('DevTools connection closed')
            self.remaining += chunk
        result, self.remaining = self.remaining[:n], self.remaining[n:]
        return result

    def frame(self):
        first, length = self.read(2)
        length &= 127
        if length == 126:
            length = struct.unpack('!H', self.read(2))[0]
        elif length == 127:
            length = struct.unpack('!Q', self.read(8))[0]
        if length > 16 * 1024 * 1024:
            raise ValueError('DevTools reply too large')
        data = self.read(length)
        if first & 15 == 8:
            raise ValueError('DevTools connection closed')
        return json.loads(data)

    def call(self, method, params=None):
        self.seq += 1
        payload = json.dumps({'id': self.seq, 'method': method,
                              'params': params or {}}).encode()
        mask = os.urandom(4)
        size = len(payload)
        header = bytes([0x81, 0x80 | size]) if size < 126 else (
            bytes([0x81, 0xfe]) + struct.pack('!H', size) if size < 65536 else
            bytes([0x81, 0xff]) + struct.pack('!Q', size))
        self.sock.sendall(header + mask + bytes(b ^ mask[i % 4]
                                                 for i, b in enumerate(payload)))
        while True:
            reply = self.frame()
            if reply.get('id') == self.seq:
                if 'error' in reply:
                    raise ValueError(str(reply['error'].get('message', reply['error'])))
                return reply.get('result', {})

    def evaluate(self, expression):
        result = self.call('Runtime.evaluate', {'expression': expression,
                           'returnByValue': True, 'awaitPromise': True})
        if 'exceptionDetails' in result:
            raise ValueError('JavaScript exception: ' + str(result['exceptionDetails'].get('text')))
        return result.get('result', {}).get('value')

    def element(self, ref):
        page = self.evaluate('location.href')
        with open(REFS, encoding='utf8') as source:
            state = json.load(source)
        if state['url'] != page or ref not in state['refs']:
            raise ValueError('stale element ref; call browser_snapshot again')
        return self.call('DOM.resolveNode', {'backendNodeId': state['refs'][ref]})['object']['objectId']

    def on_element(self, ref, function, arguments=None):
        return self.call('Runtime.callFunctionOn', {'objectId': self.element(ref),
                         'functionDeclaration': function, 'arguments':
                         [{'value': arg} for arg in (arguments or [])],
                         'returnByValue': True})


def snapshot(cdp):
    cdp.call('Accessibility.enable')
    nodes = cdp.call('Accessibility.getFullAXTree')['nodes']
    by_parent = {}
    for node in nodes:
        by_parent.setdefault(node.get('parentId'), []).append(node)
    refs = {}
    lines = []
    def visit(node, depth):
        if not node.get('ignored'):
            role = node.get('role', {}).get('value', '')
            name = node.get('name', {}).get('value', '')
            value = node.get('value', {}).get('value')
            reference = ''
            if node.get('backendDOMNodeId') and role not in ('StaticText', 'InlineTextBox'):
                reference = 'e' + str(len(refs) + 1)
                refs[reference] = node['backendDOMNodeId']
            label = ('[%s] ' % reference if reference else '') + str(role)
            if name:
                label += ' ' + json.dumps(name, ensure_ascii=False)
            if value is not None:
                label += ' value=' + json.dumps(value, ensure_ascii=False)
            lines.append('  ' * min(depth, 20) + label)
            depth += 1
        for child in by_parent.get(node.get('nodeId'), []):
            visit(child, depth)
    roots = by_parent.get(None, [])
    for root in roots:
        visit(root, 0)
    url = cdp.evaluate('location.href')
    title = cdp.evaluate('document.title')
    with open(REFS, 'w', encoding='utf8') as destination:
        json.dump({'url': url, 'refs': refs}, destination)
    return {'output': preview('URL: %s\nTitle: %s\n%s' %
                              (url, title, '\n'.join(lines)))}


def run(action, args):
    if action == 'screen_windows':
        return {'output': preview(subprocess.check_output(['wmctrl', '-l'], text=True))}
    if action == 'screen_screenshot':
        with tempfile.NamedTemporaryFile(suffix='.png') as file:
            subprocess.run(['scrot', '-z', '-o', file.name], check=True, timeout=10)
            file.seek(0)
            return image(file.read())
    cdp = CDP()
    if action == 'browser_navigate':
        if not args['url'].startswith(('http://', 'https://')):
            raise ValueError('only HTTP and HTTPS navigation is allowed')
        previous = cdp.evaluate('location.href')
        cdp.call('Page.navigate', {'url': args['url']})
        for _ in range(50):
            if cdp.evaluate('location.href') != previous and cdp.evaluate('document.readyState') == 'complete':
                break
            time.sleep(0.1)
        return snapshot(cdp)
    if action == 'browser_snapshot':
        return snapshot(cdp)
    if action == 'browser_click':
        cdp.on_element(args['ref'], 'function() { this.click(); }')
    elif action == 'browser_type':
        cdp.on_element(args['ref'], 'function(text, submit) { this.focus(); this.value = text; this.dispatchEvent(new Event("input", {bubbles:true})); this.dispatchEvent(new Event("change", {bubbles:true})); if (submit && this.form) this.form.requestSubmit(); }',
                       [args['text'], args.get('submit', False)])
    elif action == 'browser_select':
        cdp.on_element(args['ref'], 'function(value) { this.value = value; this.dispatchEvent(new Event("change", {bubbles:true})); }', [args['value']])
    elif action == 'browser_scroll':
        if args.get('ref'):
            cdp.on_element(args['ref'], 'function() { this.scrollIntoView({block:"center"}); }')
        else:
            direction = args['direction']
            dx = 600 if direction == 'right' else -600 if direction == 'left' else 0
            dy = 600 if direction == 'down' else -600 if direction == 'up' else 0
            cdp.evaluate('window.scrollBy(%d,%d)' % (dx, dy))
    elif action == 'browser_screenshot':
        encoded = cdp.call('Page.captureScreenshot', {'format': 'png',
                           'captureBeyondViewport': False})['data']
        return image(base64.b64decode(encoded, validate=True))
    elif action == 'browser_evaluate':
        return {'output': preview(json.dumps(cdp.evaluate(args['js']), ensure_ascii=False))}
    else:
        raise ValueError('unknown browser command')
    return {'output': preview('Done; call browser_snapshot for updated refs.')}


if __name__ == '__main__':
    try:
        print(json.dumps(run(sys.argv[1], json.load(sys.stdin))))
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
PY
chmod 755 /usr/local/libexec/swarmy-browser
