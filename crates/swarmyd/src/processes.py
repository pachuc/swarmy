# Executed inside the sandbox. Disk records let later execs recover all state.
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from urllib.parse import quote

ROOT = Path('/var/lib/swarmy/processes')
OUTPUT = Path('/home/agent/.swarmy/output')
BUDGET = 32768

# A detached supervisor holds the FIFO open and records the command's exit code.
# Keeping stdin open allows an interactive read after the launching exec returns.
SUPERVISOR = '''
import json, os, selectors, signal, subprocess, sys, time
from pathlib import Path
directory = Path(sys.argv[1])
stopping = False
def stop(signum, frame):
    global stopping
    stopping = True
signal.signal(signal.SIGTERM, stop)
with os.fdopen(os.open(directory / 'stdin', os.O_RDWR), 'rb', buffering=0) as stdin:
    # A login shell so the image's profile applies: it sets PATH for the
    # toolchain and CARGO_TARGET_DIR to the scratch mount; a plain -c shell
    # would build inside the clone on the durable volume.
    child = subprocess.Popen(['/bin/bash', '-lc', sys.argv[2]], stdin=stdin,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (directory / 'ready').touch()
    with selectors.DefaultSelector() as ready, (directory / 'stdout').open('wb', buffering=0) as out, (directory / 'stderr').open('wb', buffering=0) as err:
        ready.register(child.stdout, selectors.EVENT_READ, out)
        ready.register(child.stderr, selectors.EVENT_READ, err)
        while ready.get_map():
            for key, _ in ready.select():
                data = os.read(key.fileobj.fileno(), 65536)
                if data:
                    key.data.write(data)
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                else:
                    ready.unregister(key.fileobj)
                    key.fileobj.close()
    code = child.wait()
    temporary = directory / 'exit.tmp'
    temporary.write_text(json.dumps(code))
    temporary.rename(directory / 'exit.json')
    # Retain the process group identity until process_stop escalates to KILL.
    while stopping:
        time.sleep(1)
'''


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


def preview(path, budget=BUDGET):
    # Read only the head and tail, even when a process has written a huge log.
    with path.open('rb') as log:
        size = os.fstat(log.fileno()).st_size
        if size <= budget:
            text = log.read(size).decode('utf-8', errors='replace')
            if len(text.encode()) <= budget:
                return dict(output=text, truncated=False, elided_bytes=0)
        keep = min(size, budget)
        while True:
            head_size, tail_size = (keep + 1) // 2, keep // 2
            log.seek(0)
            head = log.read(head_size).decode('utf-8', errors='replace')
            log.seek(size - tail_size)
            tail = log.read(tail_size).decode('utf-8', errors='replace')
            marker = f'\n[... {size - keep} bytes elided; full output at {path} ...]\n'
            text = head + marker + tail
            excess = len(text.encode()) - budget
            if excess <= 0:
                return dict(output=text, truncated=True, elided_bytes=size - keep)
            # Decoding may expand invalid UTF-8. Shrink the raw slices too so
            # the marker counts the exact bytes omitted from the spill file.
            keep = max(0, keep - max(1, (excess + 2) // 3))


def start(directory, process_id, command, lifetime, call_id=None):
    directory.mkdir()
    if call_id is None:
        log_path = directory / 'output.log'
    else:
        OUTPUT.mkdir(parents=True, exist_ok=True)
        # Tool call ids are provider strings, never filesystem paths.
        filename = quote(call_id, safe='')[:160]
        log_path = OUTPUT / (filename + '.log')
        if log_path.exists():
            log_path = OUTPUT / (filename + '-' + process_id + '.log')
    os.mkfifo(directory / 'stdin', 0o600)
    with log_path.open('xb', buffering=0) as log:
        process = subprocess.Popen(
            [sys.executable, '-c', SUPERVISOR, str(directory), command],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
        )
    record = dict(process_id=process_id, pid=process.pid, command=command,
                  started_at=time.time(), start_ticks=identity(process.pid),
                  log_path=str(log_path), lifetime=lifetime)
    temporary = directory / 'record.tmp'
    temporary.write_text(json.dumps(record))
    temporary.rename(directory / 'record.json')
    deadline = time.monotonic() + 5
    while not (directory / 'ready').exists():
        if not running(record):
            raise ValueError('process supervisor exited during startup')
        if time.monotonic() >= deadline:
            os.killpg(record['pid'], signal.SIGKILL)
            raise ValueError('process supervisor did not initialize stdin')
        time.sleep(0.005)
    return record, process


def bash(directory, record, options):
    timeout = options.get('timeout_ms', 120000) / 1000
    wait = min(options.get('yield_seconds', 10), timeout)
    deadline = time.monotonic() + wait
    while not (directory / 'exit.json').exists() and running(record):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.01, remaining))
    finished = (directory / 'exit.json').exists()
    backgrounded = not finished and running(record)
    timed_out = backgrounded and wait == timeout
    result = preview(Path(record['log_path']), options.get('output_budget_bytes', BUDGET))
    # Preserve separate streams for ordinary small results. When truncation is
    # needed, return a single combined preview so both streams share one budget.
    stdout, stderr = result.pop('output'), ''
    if not result['truncated']:
        streams = []
        for name in ('stdout', 'stderr'):
            path = directory / name
            with path.open('rb') if path.exists() else open(os.devnull, 'rb') as stream:
                streams.append(stream.read(options.get('output_budget_bytes', BUDGET) + 1).decode('utf-8', errors='replace'))
        if sum(len(text.encode()) for text in streams) <= options.get('output_budget_bytes', BUDGET):
            stdout, stderr = streams
    result.update(process_id=record['process_id'], log_path=record['log_path'],
                  stdout=stdout, stderr=stderr, backgrounded=backgrounded,
                  timed_out=timed_out,
                  exit_code=json.loads((directory / 'exit.json').read_text()) if finished else None,
                  status=('timeout reached; command continues in background' if timed_out else
                          'yield elapsed; command continues in background') if backgrounded else 'exited')
    return result


def main():
    action, epoch, process_id, command, *extra = sys.argv[1:]
    lifetime = epoch + ':' + Path('/proc/sys/kernel/random/boot_id').read_text().strip() + ':' + str(identity(1))
    ROOT.mkdir(parents=True, exist_ok=True)
    directory = ROOT / process_id
    if action in ('process_start', 'bash'):
        options = json.loads(extra[0]) if extra else {}
        record, process = start(directory, process_id, command, lifetime,
                                options['call_id'] if action == 'bash' else None)
        if action == 'bash':
            result = bash(directory, record, options)
            if not result['backgrounded']:
                # Reap foreground supervisors here: the container's PID 1 may
                # not reap children orphaned by an exec that has returned.
                try:
                    process.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    pass
        else:
            result = dict(process_id=process_id, log_path=record['log_path'])
    elif action == 'process_list':
        result = []
        for path in sorted(ROOT.glob('*/record.json')):
            record = json.loads(path.read_text())
            record['status'] = status(record, lifetime)
            result.append({key: record[key] for key in ('process_id', 'command', 'started_at', 'log_path', 'status')})
    else:
        record = json.loads((directory / 'record.json').read_text())
        if record['lifetime'] != lifetime:
            raise ValueError('sandbox restarted; this process belongs to an earlier lifetime')
        if action == 'process_log':
            result = preview(Path(record['log_path']))
            result.update(process_id=process_id, log_path=record['log_path'], status=status(record, lifetime))
        elif action == 'write_stdin':
            if not running(record) or (directory / 'exit.json').exists():
                raise ValueError('process has exited')
            # Nonblocking writes cannot hang tool execution on a full input pipe.
            with os.fdopen(os.open(directory / 'stdin', os.O_WRONLY | os.O_NONBLOCK), 'wb', buffering=0) as stdin:
                try:
                    written = os.write(stdin.fileno(), command.encode())
                except BlockingIOError:
                    written = 0
            result = dict(process_id=process_id, bytes_written=written)
        elif action == 'process_stop':
            if running(record):
                try:
                    os.killpg(record['pid'], signal.SIGTERM)
                except ProcessLookupError:
                    pass
                time.sleep(0.25)
                if running(record):
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
            result = dict(process_id=process_id, status='exited')
        else:
            raise ValueError('unknown process action')
    print(json.dumps(result))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
