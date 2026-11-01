#!/usr/bin/env python3
"""Remote node lifecycle hook for swarmy-chaos. /opt/swarmy holds swarmy-node."""
import os
import shlex
import subprocess
import sys
import tempfile
from urllib.parse import urlparse


def node_api_port(environ):
    """Return the loopback port from SWARMY_NODE_API_URL."""
    url = urlparse(environ["SWARMY_NODE_API_URL"])
    if url.scheme != "http" or url.hostname != "127.0.0.1" or url.port is None:
        raise ValueError("SWARMY_NODE_API_URL must be an HTTP loopback URL with a port")
    return url.port


def ssh_forwards(environ):
    """Forward only NATS and the node API to the remote node."""
    port = node_api_port(environ)
    return ["-R", "127.0.0.1:4222:127.0.0.1:4222",
            "-R", f"127.0.0.1:{port}:127.0.0.1:{port}"]


def remote_environment(environ, token_path):
    """Export node settings, replacing the local token path with the remote one."""
    values = {key: value for key, value in environ.items()
              if key.startswith("SWARMY_") and not key.startswith("SWARMY_BENCH_")
              and key not in ("SWARMY_FDB_CLUSTER_FILE", "SWARMY_NODE_TOKEN_FILE")}
    values["SWARMY_NODE_TOKEN_FILE"] = token_path
    return "".join(f"export {key}={shlex.quote(value)}\n" for key, value in values.items())


def main():
    action = sys.argv[1]
    host = "ubuntu@" + os.environ["SWARMY_BENCH_REMOTE_IP"]
    options = ["-i", "/opt/swarmy/ssh-key", "-o", "StrictHostKeyChecking=accept-new"]
    if action == "kill":
        agent = sys.argv[2]
        assert len(agent) == 26 and agent.isalnum()
        subprocess.run(["ssh", *options, host,
                        "sudo bash -c '. /opt/swarmy/node.env; export PATH=/opt/swarmy:/usr/sbin:/usr/bin:/sbin:/bin; cd /mnt/swarmy/node; python3 /opt/swarmy/freeze-kill.py " + agent + "'"], check=True)
    if action in ("kill", "stop"):
        subprocess.run(["aws", "ec2", "terminate-instances", "--instance-ids",
                        os.environ["SWARMY_BENCH_REMOTE_INSTANCE"], "--no-cli-pager"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    assert action == "start"
    # Only SSH is admitted by the supplied security group.
    subprocess.run(["ssh", *options, "-o", "ExitOnForwardFailure=yes", "-fN",
                    *ssh_forwards(os.environ), host], check=True)
    subprocess.run(["scp", *options, os.environ["SWARMY_NODE_TOKEN_FILE"],
                    host + ":/opt/swarmy/node-token"], check=True)
    subprocess.run(["ssh", *options, host,
                    "sudo chmod 600 /opt/swarmy/node-token"], check=True)
    with tempfile.NamedTemporaryFile(mode="w") as file:
        file.write(remote_environment(os.environ, "/opt/swarmy/node-token"))
        file.flush()
        subprocess.run(["scp", *options, file.name, host + ":/opt/swarmy/node.env"], check=True)
    subprocess.run(["ssh", *options, host, '''sudo bash -c 'set -eu
. /opt/swarmy/node.env
export PATH=/opt/swarmy:/usr/sbin:/usr/bin:/sbin:/bin
mkdir -p /mnt/swarmy/node/.swarmy
cd /mnt/swarmy/node
: > .swarmy/config.toml
nohup swarmy-node >/opt/swarmy/node.log 2>&1 </dev/null &
echo $! > /opt/swarmy/node.pid
for attempt in $(seq 1 30); do
    if grep -q "node registered and ready" /opt/swarmy/node.log; then exit 0; fi
    kill -0 $(cat /opt/swarmy/node.pid) || { cat /opt/swarmy/node.log; exit 1; }
    sleep 1
done
cat /opt/swarmy/node.log
exit 1
' '''], check=True)


if __name__ == "__main__":
    main()
