# This script is also applied by swarmyd when opening an older image.
set -eu
if ! id agent >/dev/null 2>&1; then
    useradd --no-log-init --no-create-home --home-dir /home/agent --shell /bin/bash agent
fi
mkdir -p /home/agent/work /usr/local/libexec /usr/local/bin
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
