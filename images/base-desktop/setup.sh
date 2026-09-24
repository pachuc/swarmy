set -eu

if [ "$(uname -m)" != x86_64 ]; then
    echo 'base-desktop requires x86-64 (Chromium snapshot and lavapipe ICD)' >&2
    exit 1
fi

# Keep the developer tools and account identical to base-ubuntu. Recipe scripts
# execute in a chroot without the source tree, so embed only the account setup.
if ! id agent >/dev/null 2>&1; then
    useradd --no-log-init --no-create-home --home-dir /home/agent --shell /bin/bash agent
fi
mkdir -p /home/agent/work /usr/local/bin /usr/local/libexec /opt
chown agent:agent /home/agent /home/agent/work
if test -x /usr/bin/fdfind; then ln -sf /usr/bin/fdfind /usr/local/bin/fd; fi

cat > /usr/local/libexec/swarmy-github <<'PY'
#!/usr/bin/python3
"""Fetch credentials from the sandbox's private socket, without a disk cache."""
import json
import os
import resource
import socket
import sys

# Credentials in process memory must not be included in a core dump.
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def token():
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(10)
        client.connect('/run/swarmy/github.sock')
        client.sendall(b'github-token\n')
        with client.makefile('rb') as response:
            data = response.readline(16385)
    if len(data) > 16384:
        raise ValueError('invalid credential response')
    value = json.loads(data).get('token')
    if not isinstance(value, str) or not value or not all('!' <= c <= '~' for c in value):
        raise ValueError('GitHub credential unavailable')
    return value


def main():
    if os.path.basename(sys.argv[0]) == 'gh':
        if sys.argv[1:] not in (['--version'], ['--help'], ['help']):
            value = token()
            os.environ['GH_TOKEN'] = value
            os.environ['GH_ENTERPRISE_TOKEN'] = value
        # gh configuration and any incidental auth state live only in tmpfs.
        os.environ['GH_CONFIG_DIR'] = '/run/swarmy-gh'
        os.execv('/usr/bin/gh', ['/usr/bin/gh', *sys.argv[1:]])
    if sys.argv[1:] != ['get']:
        return  # Git's store and erase operations must never persist a token.
    fields = {}
    for line in sys.stdin:
        if line == '\n':
            break
        key, _, value = line.rstrip('\n').partition('=')
        fields[key] = value
    if fields.get('protocol') != 'https' or fields.get('host') != os.environ.get('GH_HOST', 'github.com'):
        return
    value = token()
    sys.stdout.write('username=x-access-token\npassword=' + value + '\n\n')


try:
    main()
except (OSError, ValueError):
    sys.stderr.write('GitHub credential unavailable\n')
    sys.exit(1)
PY
chmod 755 /usr/local/libexec/swarmy-github
ln -sf /usr/local/libexec/swarmy-github /usr/local/bin/gh
if command -v git >/dev/null 2>&1; then
    git config --system --replace-all credential.helper /usr/local/libexec/swarmy-github
fi

# Ubuntu's chromium-browser package is a snap launcher, which cannot work in
# this container. Pin an official Chromium snapshot instead of installing snapd.
revision=1704040
curl --retry 3 -fsSL "https://storage.googleapis.com/chromium-browser-snapshots/Linux_x64/$revision/chrome-linux.zip" -o /tmp/chromium.zip
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
    --user-data-dir="${CHROMIUM_USER_DATA_DIR:-/home/agent/.config/chromium-debug}" "$@"
SH
chmod 755 /usr/local/bin/chromium
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
    rm -f /tmp/.X99-lock /tmp/.X11-unix/X99
    Xvfb :99 -screen 0 1280x800x24 -nolisten tcp &
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
    su -s /bin/sh agent -c 'DISPLAY=:99 openbox' &
    wpid=$!
    wait "$xpid" 2>/dev/null || :
    kill "$wpid" 2>/dev/null || :
    wait "$wpid" 2>/dev/null || :
    sleep 1
done
SH
chmod 755 /usr/local/libexec/swarmy-init
