# Read only regular instruction files, with a bounded excerpt and no symlink traversal.
import os
import stat
import sys

root, limit = sys.argv[1], int(sys.argv[2])
output = bytearray()
truncated = False
try:
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
except OSError:
    sys.exit(0)


def append(directory, label):
    global truncated
    try:
        fd = os.open('AGENTS.md', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                     dir_fd=directory)
    except OSError:
        return
    with os.fdopen(fd, 'rb') as file:
        if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
            return
        header = ('\n--- ' + label + '/AGENTS.md ---\n').encode('utf-8', errors='replace')
        remaining = max(0, limit - len(output))
        data = header + file.read(max(0, remaining - len(header)) + 1)
        output.extend(data[:remaining])
        truncated |= len(data) > remaining


try:
    append(root_fd, root)
    for name in sorted(os.listdir(root_fd)):
        if truncated:
            break
        try:
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=root_fd)
        except OSError:
            continue
        try:
            # .git can be a directory or a worktree's regular gitdir file.
            if stat.S_ISDIR(os.stat('.git', dir_fd=child, follow_symlinks=False).st_mode) or \
               stat.S_ISREG(os.stat('.git', dir_fd=child, follow_symlinks=False).st_mode):
                append(child, root + '/' + name)
        except OSError:
            pass
        finally:
            os.close(child)
finally:
    os.close(root_fd)
sys.stdout.write(bytes(output).decode('utf-8', errors='ignore'))
if truncated:
    sys.stdout.write('\n[Repository instructions truncated; read AGENTS.md for the complete text.]\n')
