#!/usr/bin/env python3
"""Remote node lifecycle hook for swarmy-chaos on the primary AWS machine."""
import os
import shlex
import subprocess
import sys
import tempfile


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
    # Only SSH is admitted by the supplied security group. FDB advertises its
    # loopback address, which resolves to these forwards on the second machine.
    subprocess.run(["ssh", *options, "-o", "ExitOnForwardFailure=yes", "-fN",
                    "-R", "127.0.0.1:4500:127.0.0.1:4500",
                    "-R", "127.0.0.1:4222:127.0.0.1:4222", host], check=True)
    with tempfile.NamedTemporaryFile(mode="w") as file:
        for key, value in os.environ.items():
            if key.startswith("SWARMY_") and not key.startswith("SWARMY_BENCH_"):
                file.write(f"export {key}={shlex.quote(value)}\n")
        file.flush()
        subprocess.run(["scp", *options, file.name, host + ":/opt/swarmy/node.env"], check=True)
    subprocess.run(["ssh", *options, host, '''sudo bash -c 'set -eu
. /opt/swarmy/node.env
export PATH=/opt/swarmy:/usr/sbin:/usr/bin:/sbin:/bin
mkdir -p /mnt/swarmy/node/.swarmy
cd /mnt/swarmy/node
: > .swarmy/config.toml
nohup swarmyd >/opt/swarmy/node.log 2>&1 </dev/null &
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
