# Executed inside the sandbox. Disk records let later execs recover all state.
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path('/var/lib/swarmy/processes')


def identity(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
        return fields[19] if fields[0] not in ('Z', 'X') else None
    except (FileNotFoundError, ProcessLookupError):
        return None


def running(record):
    return record['start_ticks'] is not None and identity(record['pid']) == record['start_ticks']


def status(record, lifetime):
    if record['lifetime'] != lifetime:
        return 'restarted'
    return 'running' if running(record) else 'exited'


def main():
    action, epoch, process_id, command = sys.argv[1:]
    lifetime = epoch + ':' + Path('/proc/sys/kernel/random/boot_id').read_text().strip() + ':' + str(identity(1))
    ROOT.mkdir(parents=True, exist_ok=True)
    directory = ROOT / process_id
    if action == 'process_start':
        directory.mkdir()
        log_path = directory / 'output.log'
        # The session leader waits for bash so its identity remains stable while
        # the command runs. No pipe or parent daemon is needed after this exec.
        with log_path.open('ab', buffering=0) as log:
            process = subprocess.Popen(
                ['/bin/bash', '-c', 'trap "" HUP; /bin/bash -c "$1" & wait $!', 'swarmy', command],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
            )
        record = dict(process_id=process_id, pid=process.pid, command=command,
                      started_at=time.time(), start_ticks=identity(process.pid),
                      log_path=str(log_path), lifetime=lifetime)
        temporary = directory / 'record.tmp'
        temporary.write_text(json.dumps(record))
        temporary.rename(directory / 'record.json')
        print(json.dumps(dict(process_id=process_id, log_path=str(log_path))))
    elif action == 'process_list':
        records = []
        for path in sorted(ROOT.glob('*/record.json')):
            record = json.loads(path.read_text())
            record['status'] = status(record, lifetime)
            records.append({key: record[key] for key in ('process_id', 'command', 'started_at', 'log_path', 'status')})
        print(json.dumps(records))
    else:
        record = json.loads((directory / 'record.json').read_text())
        if record['lifetime'] != lifetime:
            raise ValueError('sandbox restarted; this process belongs to an earlier lifetime')
        if action == 'process_log':
            with Path(record['log_path']).open('rb') as log:
                log.seek(0, 2)
                size = log.tell()
                log.seek(max(0, size - 65536))
                print(json.dumps(dict(process_id=process_id, output=log.read(65536).decode('utf-8', errors='replace'), truncated=size > 65536)))
        elif action == 'process_stop':
            if running(record):
                os.killpg(record['pid'], signal.SIGTERM)
                # Keep the leader unreaped until escalation, avoiding PID reuse.
                time.sleep(0.25)
                try:
                    os.killpg(record['pid'], signal.SIGKILL)
                except ProcessLookupError:
                    pass
                for _ in range(100):
                    if not running(record):
                        break
                    time.sleep(0.01)
                else:
                    raise ValueError('process did not stop after KILL')
            print(json.dumps(dict(process_id=process_id, status='exited')))


try:
    main()
except (OSError, ValueError, KeyError) as error:
    print(str(error), file=sys.stderr)
    sys.exit(1)
