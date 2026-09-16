#!/usr/bin/env python3
"""Stop the node only after a writer observes its checkpoint freeze.

The cloud node driver then terminates the machine. A timeout fails the scenario
instead of claiming a kill during publication without observing a freeze.
"""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

agent = sys.argv[1]
mount = Path('.swarmy/node/bundles') / agent / 'rootfs'
writer = subprocess.Popen([sys.executable, '-c', '''import os, sys, time
with open(sys.argv[1], 'w') as f:
    while True:
        f.write('probe\\n'); f.flush(); os.fsync(f.fileno()); time.sleep(0.001)
''', str(mount / 'root/freeze-probe')], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
checkpoint = None
try:
    time.sleep(0.1)
    checkpoint = subprocess.Popen(['swarmy', '--json', 'vol', 'checkpoint', agent,
                                   '--mount', str(mount.resolve())],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        channel = Path(f'/proc/{writer.pid}/wchan').read_text().strip()
        if 'sb_start_write' in channel or 'percpu_rwsem_wait' in channel:
            os.kill(int(Path('/opt/swarmy/node.pid').read_text()), signal.SIGSTOP)
            assert checkpoint.poll() is None, 'checkpoint finished before node stop'
            print('snapshot freeze observed; node stopped at writer wait channel ' + channel, flush=True)
            break
        assert checkpoint.poll() is None, 'checkpoint finished before freeze observation'
        time.sleep(0.001)
    else:
        raise TimeoutError('writer did not observe checkpoint freeze')
finally:
    writer.kill()
    # Frozen tasks may remain uninterruptible until the machine terminates.
    if checkpoint is not None:
        checkpoint.kill()
