#!/usr/bin/env python3
"""AWS two-node workload for cloud.py; use a freshly built local debug workspace.

Cloud credentials are transferred only over SSH into mode-0600 temporary files.
The lifecycle parent owns teardown even when installation or assertions fail.
Selected results are copied to SWARMY_BENCH_RESULTS outside the checkout.
"""
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
STATE = json.loads(Path(os.environ["SWARMY_BENCH_STATE"]).read_text())
KEY = os.environ["SWARMY_BENCH_SSH_KEY"]
RESULTS = Path(os.environ["SWARMY_BENCH_RESULTS"])
SSH = ["-i", KEY, "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=5"]


def run(args, **kwargs):
    return subprocess.run(args, check=True, timeout=1800, **kwargs)


def ssh(ip, command, **kwargs):
    return run(["ssh", *SSH, "ubuntu@" + ip, command], **kwargs)


def copy(ip, source, destination):
    run(["scp", *SSH, str(source), "ubuntu@" + ip + ":" + destination])


def ready(ip):
    for _ in range(120):
        try:
            ssh(ip, "true", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return
        except subprocess.SubprocessError:
            time.sleep(2)
    raise RuntimeError("SSH readiness timed out")


def prepare(ip, bundle):
    ready(ip)
    copy(ip, bundle, "/tmp/swarmy-bench.tar.gz")
    ssh(ip, '''set -eu
sudo cloud-init status --wait >/dev/null
sudo apt-get update -qq
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq runc e2fsprogs debootstrap curl python3 nbd-client
sudo modprobe nbd nbds_max=16 max_part=0
sudo mkdir -p /opt/swarmy
sudo tar xzf /tmp/swarmy-bench.tar.gz -C /opt/swarmy
sudo cp /opt/swarmy/libfdb_c.so /usr/lib/
sudo ldconfig
sudo chown -R ubuntu:ubuntu /opt/swarmy
# Format only the EC2 instance-store device, never an EBS volume.
disk=$(lsblk -dnpo NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print $1; exit}')
test -n "$disk"
sudo mkfs.ext4 -q "$disk"
sudo mkdir -p /mnt/swarmy
sudo mount "$disk" /mnt/swarmy
sudo chown ubuntu:ubuntu /mnt/swarmy
mkdir -p /mnt/swarmy/tmp
''')


def main():
    RESULTS.mkdir(parents=True, exist_ok=True)
    nodes = STATE["instances"]
    assert len(nodes) == 2
    primary, remote = (node["private_ip"] for node in nodes)
    with tempfile.TemporaryDirectory() as directory:
        staging = Path(directory)
        for binary in ("swarmy", "swarmy-chaos", "swarmy-scheduler", "swarmy-worker", "swarmy-gateway", "swarmyd"):
            run(["strip", "-o", str(staging / binary), str(ROOT / "target/debug" / binary)])
        for binary in ("fdbserver", "fdbcli", "nats-server"):
            shutil.copy2(shutil.which(binary) or "/usr/sbin/" + binary, staging / binary)
        shutil.copy2("/usr/lib/libfdb_c.so", staging / "libfdb_c.so")
        shutil.copytree(ROOT / "images/base-ubuntu", staging / "base-ubuntu")
        for name in ("persistent-cloud-node.py", "persistent-volume.py", "volume.py", "freeze-kill.py"):
            shutil.copy2(ROOT / "scripts/benchmarks" / name, staging / name)
        bundle = staging / "bundle.tar.gz"
        bundle.touch()
        run(["tar", "czf", str(bundle), "--exclude=bundle.tar.gz", "-C", str(staging), "."])
        for ip in (primary, remote):
            prepare(ip, bundle)
        config = {
            "TMPDIR": "/mnt/swarmy/tmp",
            "SWARMY_FDB_CLUSTER_FILE": "/opt/swarmy/fdb.cluster",
            "SWARMY_NATS_URL": "nats://127.0.0.1:4222",
            "SWARMY_S3_ENDPOINT": "https://s3.us-east-1.amazonaws.com",
            "SWARMY_S3_REGION": "us-east-1",
            "SWARMY_S3_BUCKET": os.environ["SWARMY_S3_BUCKET"],
            "SWARMY_S3_PREFIX": os.environ.get("SWARMY_S3_PREFIX", ""),
            "SWARMY_S3_ACCESS_KEY": os.environ["AWS_ACCESS_KEY_ID"],
            "SWARMY_S3_SECRET_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "SWARMY_STORE_DIRECTORY": STATE["name"],
            "SWARMY_GC_INTERVAL_SECONDS": "86400",
            "SWARMY_BENCH_REMOTE_IP": remote,
            "SWARMY_BENCH_REMOTE_INSTANCE": nodes[1]["instance_id"],
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_DEFAULT_REGION": "us-east-1",
            "PATH": "/opt/swarmy:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin",
        }
        environment = staging / "bench.env"
        environment.write_text("".join(f"export {k}={shlex.quote(v)}\n" for k, v in config.items()))
        environment.chmod(0o600)
        cluster = staging / "fdb.cluster"
        cluster.write_text("bench:bench@127.0.0.1:4500\n")
        for ip in (primary, remote):
            copy(ip, cluster, "/opt/swarmy/fdb.cluster")
        copy(primary, environment, "/opt/swarmy/bench.env")
        copy(primary, KEY, "/opt/swarmy/ssh-key")
        ssh(primary, f'''set -eu
sudo snap install aws-cli --classic
mkdir -p /mnt/swarmy/fdb /mnt/swarmy/nats /mnt/swarmy/run
nohup /opt/swarmy/fdbserver -p 127.0.0.1:4500 -C /opt/swarmy/fdb.cluster -d /mnt/swarmy/fdb -L /mnt/swarmy/fdb >/opt/swarmy/fdb.log 2>&1 </dev/null &
nohup /opt/swarmy/nats-server -js -a 127.0.0.1 -sd /mnt/swarmy/nats >/opt/swarmy/nats.log 2>&1 </dev/null &
sleep 3
/opt/swarmy/fdbcli -C /opt/swarmy/fdb.cluster --exec 'configure new single ssd'
''')
        try:
            ssh(primary, '''sudo bash -c 'set -eu
. /opt/swarmy/bench.env
cd /mnt/swarmy/run
swarmy --json image build /opt/swarmy/base-ubuntu --tag persistent > /opt/swarmy/image.json
swarmy-chaos --no-start-stack --bin-dir /opt/swarmy --persistent --image base-ubuntu:persistent --sessions 2 --gateways 1 --kills 0 --node-driver /opt/swarmy/persistent-cloud-node.py --measurements /opt/swarmy/persistent-volume.py > /opt/swarmy/persistent.log 2>&1
' ''')
        finally:
            for name in ("persistent.log", "image.json"):
                subprocess.run(["scp", *SSH, "ubuntu@" + primary + ":/opt/swarmy/" + name, str(RESULTS / name)], check=False)

            # The lifecycle parent independently audits versions and multipart uploads
            # after terminating compute. This accelerates the common unversioned case.
            run(["aws", "s3", "rm", "s3://" + STATE["bucket"] + "/" + STATE["prefix"],
                 "--recursive", "--only-show-errors"])


if __name__ == "__main__":
    RESULTS.mkdir(parents=True, exist_ok=True)
    with (RESULTS / "setup.log").open("w") as log:
        os.chmod(log.name, 0o600)
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
        main()
