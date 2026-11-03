set -eu

if [ "$(uname -m)" != x86_64 ]; then
    echo 'the desktop setup requires x86-64 (Chromium snapshot and lavapipe ICD)' >&2
    exit 1
fi

# The display stack shared by base-desktop and swarmy-dev. No NVIDIA package is
# installed: on a GPU node the container runtime mounts the host driver's own
# libraries into the sandbox (CDI), so they always match the host's kernel
# module. glvnd and the Vulkan loader pick NVIDIA's libraries up from there.

# fonts-wqy-microhei draws Chinese, Japanese and Korean text in about 5 MB; Noto CJK would add about 91 MB.
env DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends -- \
    x11-utils xvfb xserver-xorg-core openbox util-linux unzip scrot wmctrl \
    fonts-dejavu-core fonts-liberation fonts-wqy-microhei fontconfig \
    libglvnd0 libgl1 libglx-mesa0 libegl1 libegl-mesa0 libgles2 libgl1-mesa-dri mesa-utils \
    libvulkan1 mesa-vulkan-drivers vulkan-tools \
    libnss3 libatk-bridge2.0-0 libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
    libasound2t64 libcups2t64 libxss1 libgtk-3-0t64

# Ubuntu's chromium-browser package is a snap launcher, which cannot work in
# this container. Pin an official Chromium snapshot instead of installing snapd.
revision=1704040
curl --retry 3 -fsSL "https://storage.googleapis.com/chromium-browser-snapshots/Linux_x64/$revision/chrome-linux.zip" -o /tmp/chromium.zip
printf '%s  %s\n' '3077c26bcc11b1e8abad762122a26698e3d620d97dd7d9620a22c52fa9eeb2fa' /tmp/chromium.zip | sha256sum -c -
unzip -q /tmp/chromium.zip -d /opt
rm /tmp/chromium.zip
mkdir -p /etc/chromium/policies/managed
cat > /etc/chromium/policies/managed/swarmy.json <<'JSON'
{
  "PasswordManagerEnabled": false,
  "PasswordLeakDetectionEnabled": false,
  "PromotionalTabsEnabled": false
}
JSON
chmod 644 /etc/chromium/policies/managed/swarmy.json
cat > /usr/local/bin/chromium <<'SH'
#!/bin/sh
set -eu
export DISPLAY=${DISPLAY:-:99}
# On the software display, keep Chromium on Mesa's CPU renderers, as before.
# On the NVIDIA display, let it find the card.
if [ "$(cat /run/swarmy-display/renderer 2>/dev/null || :)" != nvidia ]; then
    export LIBGL_ALWAYS_SOFTWARE=1
    export VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.x86_64.json
fi
# The sandbox is already isolated by runc; Chromium's namespace sandbox is
# unavailable there. Do not run the browser as root. --test-type hides the
# unsupported --no-sandbox infobar without changing the container boundary.
if [ "$(id -u)" = 0 ]; then
    exec su -p -s /bin/sh agent -c 'exec /usr/local/bin/chromium "$@"' chromium "$@"
fi
exec /opt/chrome-linux/chrome --no-sandbox --test-type --no-first-run --no-default-browser-check \
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
ENV
cat > /usr/local/libexec/swarmy-display-select <<'SH'
#!/bin/sh
# Print the renderer for this sandbox's display: "nvidia" when the card's
# device node and the NVIDIA X driver, X GLX module and GLX client library are
# all present (the container runtime injects them on a GPU node), otherwise
# "software". Arguments, for tests: a device directory, then library
# directories. swarmy-init passes none and gets the defaults below.
set -u
dev=/dev
if [ "$#" -gt 0 ]; then
    dev=$1
    shift
fi
if [ "$#" -eq 0 ]; then
    set -- /usr/lib/x86_64-linux-gnu /usr/lib/x86_64-linux-gnu/nvidia/xorg \
        /usr/lib/xorg/modules/drivers /usr/lib/xorg/modules/extensions
fi
present() {
    name=$1
    shift
    for dir in "$@"; do
        for file in "$dir/$name"*; do
            if [ -e "$file" ]; then
                return 0
            fi
        done
    done
    return 1
}
if [ -e "$dev/nvidia0" ] && present nvidia_drv.so "$@" \
    && present libglxserver_nvidia.so "$@" && present libGLX_nvidia.so "$@"; then
    echo nvidia
else
    echo software
fi
SH
chmod 755 /usr/local/libexec/swarmy-display-select
mkdir -p /etc/X11
cat > /etc/X11/swarmy-nvidia.conf <<'CONF'
# Xorg on the NVIDIA driver with a virtual screen and no monitor. swarmy-init
# uses this only when swarmy-display-select prints nvidia.
Section "ServerFlags"
    Option "AutoAddDevices" "false"
    Option "AutoAddGPU" "false"
    Option "DontVTSwitch" "true"
EndSection

Section "Files"
    ModulePath "/usr/lib/x86_64-linux-gnu/nvidia/xorg,/usr/lib/xorg/modules"
EndSection

Section "Device"
    Identifier "card"
    Driver "nvidia"
    Option "AllowEmptyInitialConfiguration" "true"
EndSection

Section "Screen"
    Identifier "screen"
    Device "card"
    DefaultDepth 24
    SubSection "Display"
        Depth 24
        Virtual 1280 800
    EndSubSection
EndSection

Section "ServerLayout"
    Identifier "layout"
    Screen "screen"
EndSection
CONF
chmod 644 /etc/X11/swarmy-nvidia.conf
cat > /usr/local/libexec/swarmy-init <<'SH'
#!/bin/sh
# PID 1 keeps the display alive even if an application kills the X server.
# The renderer is chosen once per sandbox start: Xorg on the NVIDIA driver
# when the card and its libraries are present, otherwise the Xvfb software
# display. /run/swarmy-display/renderer records the choice.
set -u
export DISPLAY=:99
renderer=$(/usr/local/libexec/swarmy-display-select)
mkdir -p /run/swarmy-display
chmod 755 /run/swarmy-display
printf '%s\n' "$renderer" > /run/swarmy-display/renderer
chmod 644 /run/swarmy-display/renderer
if [ "$renderer" != nvidia ]; then
    export LIBGL_ALWAYS_SOFTWARE=1
    export GALLIUM_DRIVER=llvmpipe
    export VK_ICD_FILENAMES=/usr/share/vulkan/icd.d/lvp_icd.x86_64.json
fi
trap 'kill "$xpid" "$wpid" 2>/dev/null || :; wait 2>/dev/null || :; exit 0' TERM INT
while :; do
    mkdir -p /tmp/.X11-unix
    chmod 1777 /tmp/.X11-unix
    rm -f /tmp/.X99-lock /tmp/.X11-unix/X99
    if [ "$renderer" = nvidia ]; then
        # Xorg must run as root to open the card. The sandbox has no virtual
        # terminals, so it must not switch or claim one.
        Xorg :99 -config /etc/X11/swarmy-nvidia.conf -nolisten tcp -noreset \
            -novtswitch -sharevts -logfile /run/swarmy-display/Xorg.99.log &
    else
        setpriv --reuid agent --regid agent --clear-groups Xvfb :99 -screen 0 1280x800x24 -nolisten tcp &
    fi
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
import fcntl
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
# A cold first start can take longer than the usual connection-refused retry window.
CHROMIUM_START_TIMEOUT = 60
CHROMIUM_START_LOCK = '/tmp/swarmy-chromium-start.lock'
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


def start_chromium_once():
    # Serialize launch decisions and retain the launcher PID across helper calls.
    # Without both, simultaneous cold calls can each launch a competing browser.
    with open(CHROMIUM_START_LOCK, 'a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        lock.seek(0)
        try:
            pid = int(lock.read().strip())
            os.kill(pid, 0)
            return
        except (ValueError, ProcessLookupError):
            pass
        process = subprocess.Popen(['chromium', 'about:blank'], stdin=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   start_new_session=True)
        lock.seek(0)
        lock.truncate()
        lock.write(str(process.pid))
        lock.flush()


class CDP:
    def __init__(self):
        pages = None
        deadline = time.monotonic() + CHROMIUM_START_TIMEOUT
        launched = False
        while time.monotonic() < deadline:
            try:
                pages = json.load(urllib.request.urlopen(PORT + '/json', timeout=1))
                break
            except (OSError, ValueError):
                if not launched:
                    start_chromium_once()
                    launched = True
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        if pages is None:
            raise ValueError('Chromium debugging port 9222 is unavailable after %d s' %
                             CHROMIUM_START_TIMEOUT)
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
            # Drain the frame so a shallower tree can be requested on this socket.
            for _ in range(0, length, 65536):
                self.read(min(65536, length - _))
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
            raise ValueError('JavaScript exception: ' + str(result['exceptionDetails'].get('exception', {}).get('description') or result['exceptionDetails'].get('text')))
        return result.get('result', {}).get('value')

    def element(self, ref):
        page = self.evaluate('location.href')
        with open(REFS, encoding='utf8') as source:
            state = json.load(source)
        if state['url'] != page or ref not in state['refs']:
            raise ValueError('stale element ref; call browser_snapshot again')
        return self.call('DOM.resolveNode', {'backendNodeId': state['refs'][ref]})['object']['objectId']

    def on_element(self, ref, function, arguments=None):
        result = self.call('Runtime.callFunctionOn', {'objectId': self.element(ref),
                         'functionDeclaration': function, 'arguments':
                         [{'value': arg} for arg in (arguments or [])],
                         'returnByValue': True})
        if 'exceptionDetails' in result:
            raise ValueError('JavaScript exception: ' + str(result['exceptionDetails'].get('exception', {}).get('description') or result['exceptionDetails'].get('text')))
        return result.get('result', {}).get('value')


def snapshot(cdp):
    cdp.call('Accessibility.enable')
    for depth in (8, 4, 2, 1):
        try:
            nodes = cdp.call('Accessibility.getFullAXTree', {'depth': depth})['nodes']
            break
        except ValueError as error:
            if str(error) != 'DevTools reply too large' or depth == 1:
                raise
    by_parent = {}
    for node in nodes:
        by_parent.setdefault(node.get('parentId'), []).append(node)
    url = cdp.evaluate('location.href')
    try:
        with open(REFS, encoding='utf8') as source:
            previous = json.load(source)
        old_refs = previous['refs'] if previous['url'] == url else {}
    except (OSError, ValueError, KeyError):
        old_refs = {}
    refs = {}
    old_by_node = {node: ref for ref, node in old_refs.items()}
    next_ref = max((int(ref[1:]) for ref in old_refs if ref.startswith('e') and ref[1:].isdigit()), default=0)
    lines = []
    def visit(node, depth):
        nonlocal next_ref
        if not node.get('ignored'):
            role = node.get('role', {}).get('value', '')
            name = node.get('name', {}).get('value', '')
            value = node.get('value', {}).get('value')
            reference = ''
            if node.get('backendDOMNodeId') and role not in ('StaticText', 'InlineTextBox'):
                backend = node['backendDOMNodeId']
                reference = old_by_node.get(backend)
                if reference is None:
                    next_ref += 1
                    reference = 'e' + str(next_ref)
                refs[reference] = backend
            label = ('[%s] ' % reference if reference else '') + str(role)
            if name:
                label += ' ' + json.dumps(name, ensure_ascii=False)
            if value is not None:
                label += ' value=' + json.dumps(value, ensure_ascii=False)
            for prop in node.get('properties', []):
                if prop.get('name') in ('checked', 'disabled', 'focused', 'expanded', 'url'):
                    label += ' %s=%s' % (prop['name'], json.dumps(prop.get('value', {}).get('value'), ensure_ascii=False))
            lines.append('  ' * min(depth, 20) + label)
            depth += 1
        for child in by_parent.get(node.get('nodeId'), []):
            visit(child, depth)
    roots = by_parent.get(None, [])
    for root in roots:
        visit(root, 0)
    title = cdp.evaluate('document.title')
    with open(REFS, 'w', encoding='utf8') as destination:
        json.dump({'url': url, 'refs': refs}, destination)
    return {'output': preview('URL: %s\nTitle: %s\nRefs like [e3] are stable within a page and expire on navigation.\nTree limited to depth %d (truncated).\n%s' %
                              (url, title, depth, '\n'.join(lines)))}


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
        navigation = cdp.call('Page.navigate', {'url': args['url']})
        if navigation.get('errorText'):
            raise ValueError('Navigation failed: ' + navigation['errorText'])
        try:
            os.unlink(REFS)
        except FileNotFoundError:
            pass
        deadline = time.monotonic() + 20
        while cdp.evaluate('document.readyState') != 'complete':
            if time.monotonic() >= deadline:
                raise ValueError('Navigation timed out waiting for page load')
            time.sleep(0.1)
        return snapshot(cdp)
    if action == 'browser_snapshot':
        return snapshot(cdp)
    if action == 'browser_click':
        cdp.on_element(args['ref'], 'function() { this.click(); }')
    elif action == 'browser_type':
        cdp.on_element(args['ref'], 'function() { if (!("value" in this) && !this.isContentEditable) throw new Error("element is not editable"); this.focus(); if ("value" in this) this.select?.(); else { const selection = window.getSelection(); selection.selectAllChildren(this); selection.deleteFromDocument(); } }')
        cdp.call('Input.insertText', {'text': args['text']})
        cdp.on_element(args['ref'], 'function(text) { const actual = ("value" in this) ? this.value : this.textContent; if (actual !== text) throw new Error("typing did not update the element"); }', [args['text']])
        if args.get('submit', False):
            cdp.on_element(args['ref'], 'function() { if (this.form) this.form.requestSubmit(); }')
    elif action == 'browser_select':
        cdp.on_element(args['ref'], 'function(value) { if (this.tagName !== "SELECT") throw new Error("element is not a dropdown"); const options = Array.from(this.options); const option = options.find(o => o.value === value) ?? options.find(o => o.label.trim() === value) ?? options.find(o => o.label.trim().toLowerCase() === value.toLowerCase()); if (!option) throw new Error("option not found; available options: " + options.slice(0, 20).map(o => o.value + " / " + o.label.trim()).join(", ")); this.value = option.value; this.dispatchEvent(new Event("input", {bubbles:true})); this.dispatchEvent(new Event("change", {bubbles:true})); }', [args['value']])
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
