# One exec reads the directory. Never follow links or open devices or FIFOs.
import os
import stat
import sys

path, limit = sys.argv[1], int(sys.argv[2])
output = bytearray()
truncated = False
try:
    names = sorted(os.listdir(path))
except FileNotFoundError:
    names = []
for name in names:
    filename = os.path.join(path, name)
    try:
        fd = os.open(filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except (FileNotFoundError, OSError):
        continue
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        continue
    with os.fdopen(fd, 'rb') as file:
        header = ('\n--- ' + name + ' ---\n').encode('utf-8', errors='replace')
        remaining = limit - len(output)
        data = header + file.read(max(0, remaining - len(header)) + 1)
        output.extend(data[:remaining])
        if len(data) > remaining:
            truncated = True
            break
text = bytes(output).decode('utf-8', errors='replace').encode('utf-8')
if len(text) > limit:
    truncated = True
sys.stdout.write(text[:limit].decode('utf-8', errors='ignore'))
if truncated:
    sys.stdout.write('\n[Agent memory truncated at memory_max_bytes.]\n')
