"""File tools use one JSON request on stdin and one JSON result on stdout."""
import base64
import difflib
import struct
import fnmatch
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

CAP = 100


def completed(action, output, **metadata):
    return {"completed": {"title": action, "output": output, "metadata": metadata}}


def text_file(path):
    data = Path(path).read_bytes()
    if b"\0" in data:
        raise ValueError("binary file: contains a null byte")
    return data.decode("utf-8")


def image_dimensions(data, kind):
    if kind == "image/png" and data.startswith(b"\x89PNG\r\n\x1a\n"):
        return struct.unpack(">II", data[16:24])
    if kind == "image/gif" and data[:3] == b"GIF":
        return struct.unpack("<HH", data[6:10])
    if kind == "image/webp" and data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        if data[12:16] == b"VP8X":
            return (int.from_bytes(data[24:27], "little") + 1,
                    int.from_bytes(data[27:30], "little") + 1)
        if data[12:16] == b"VP8 " and data[23:26] == b"\x9d\x01\x2a":
            return (int.from_bytes(data[26:28], "little") & 0x3fff,
                    int.from_bytes(data[28:30], "little") & 0x3fff)
        if data[12:16] == b"VP8L" and data[20] == 0x2f:
            bits = int.from_bytes(data[21:25], "little")
            return ((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1)
    if kind == "image/jpeg" and data.startswith(b"\xff\xd8"):
        pos = 2
        while pos + 4 < len(data):
            if data[pos] != 0xff:
                break
            marker = data[pos + 1]
            if marker in (0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7,
                          0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf):
                return struct.unpack(">HH", data[pos + 5:pos + 9])[::-1]
            length = int.from_bytes(data[pos + 2:pos + 4], "big")
            if length < 2:
                break
            pos += length + 2
    raise ValueError("invalid or unsupported image")


def read(args):
    path = Path(args["path"])
    kinds = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
             ".gif": "image/gif", ".webp": "image/webp"}
    kind = kinds.get(path.suffix.lower())
    if kind:
        size = path.stat().st_size
        if size > 5 * 1024 * 1024:
            raise ValueError("image exceeds the 5 MiB read cap")
        data = path.read_bytes()
        width, height = image_dimensions(data, kind)
        result = completed("read", f"Image {path} ({width}x{height}).")
        result["completed"]["metadata"] = {
            "image_media_type": kind, "image_base64": base64.b64encode(data).decode("ascii"),
            "image_width": width, "image_height": height,
        }
        return result
    lines = text_file(path).splitlines()
    offset, limit = args.get("offset", 1), args.get("limit", 2000)
    if offset < 1 or limit < 1:
        raise ValueError("offset and limit must be positive")
    selected = lines[offset - 1:offset - 1 + limit]
    output = []
    for number, line in enumerate(selected, offset):
        if len(line) > 2000:
            line = line[:2000] + " [line truncated]"
        output.append(f"{number}: {line}")
    next_offset = offset + len(selected)
    more = next_offset <= len(lines)
    if more:
        output.append(f"More lines remain. Continue with offset={next_offset}.")
    return completed("read", "\n".join(output), next_offset=next_offset if more else None)


def write(args):
    path = Path(args["path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(args["content"].encode("utf-8"))
    return completed("write", f"Wrote {path}.")


def occurrences(text, needle):
    start = 0
    while (index := text.find(needle, start)) >= 0:
        yield index, index + len(needle)
        start = index + 1


def whitespace_matches(text, needle, normalize):
    lines = text.splitlines(keepends=True)
    wanted = needle.splitlines(keepends=True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))
    # Only whitespace at line boundaries is relaxed. Internal spacing is exact.
    wanted = [normalize(line.rstrip("\r\n")) for line in wanted]
    for index in range(len(lines) - len(wanted) + 1):
        window = lines[index:index + len(wanted)]
        if [normalize(line.rstrip("\r\n")) for line in window] == wanted:
            end = offsets[index + len(wanted)]
            if not needle.endswith("\n"):
                end -= len(window[-1]) - len(window[-1].rstrip("\r\n"))
            yield offsets[index], end


def unique(matches, text):
    if len(matches) > 1:
        numbers = sorted({text.count("\n", 0, start) + 1 for start, _ in matches})
        raise ValueError("ambiguous match at lines " + ", ".join(map(str, numbers)))
    return matches[0] if matches else None


def edit(args):
    path = Path(args["path"])
    before = text_file(path)
    old, new = args["old_string"], args["new_string"]
    if not old:
        raise ValueError("old_string must not be empty")
    matches = list(occurrences(before, old))
    mode = "exact"
    if args.get("replace_all", False):
        if not matches:
            raise ValueError("no exact match; replace_all only uses exact matches")
        after = before.replace(old, new)
        count = before.count(old)
    else:
        match = unique(matches, before)
        for name, normalize in [("ignore trailing whitespace", str.rstrip),
                                ("ignore leading and trailing whitespace", str.strip)]:
            if match is not None:
                break
            match = unique(list(whitespace_matches(before, old, normalize)), before)
            mode = name
        if match is None:
            raise ValueError("old_string did not match")
        start, end = match
        after = before[:start] + new + before[end:]
        count = 1
    diff = []
    for line in difflib.unified_diff(before.splitlines(keepends=True),
                                     after.splitlines(keepends=True),
                                     fromfile=str(path), tofile=str(path)):
        diff.append(line if line.endswith("\n") else line + "\n\\ No newline at end of file\n")
    path.write_bytes(after.encode("utf-8"))
    return completed("edit", f"Matched using {mode}; replaced {count} occurrence(s).\n" + "".join(diff),
                     match_mode=mode, replacements=count)


def walk(path):
    if path.is_file():
        yield path
        return
    if not path.is_dir():
        raise ValueError(f"not a directory: {path}")
    for root, directories, files in os.walk(path):
        directories[:] = sorted(d for d in directories if d != ".git" and not Path(root, d).is_symlink())
        for name in sorted(files):
            candidate = Path(root, name)
            if not candidate.is_symlink():
                yield candidate


def glob_match(path, pattern):
    # A pattern without slashes matches a basename at any depth, as with rg -g.
    if "/" not in pattern:
        return fnmatch.fnmatchcase(path.name, pattern)
    parts = path.parts
    patterns = pattern.split("/")

    def matches(names, globs):
        if not globs:
            return not names
        if globs[0] == "**":
            return matches(names, globs[1:]) or bool(names and matches(names[1:], globs))
        return bool(names and fnmatch.fnmatchcase(names[0], globs[0]) and matches(names[1:], globs[1:]))

    return matches(parts, patterns)


def rg_lines(command):
    # Stop and reap rg even when the caller reaches the cap or parsing fails.
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        try:
            yield from process.stdout
            error = process.stderr.read().decode("utf-8", errors="replace")
            if process.wait() not in (0, 1):
                raise ValueError(error.strip())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


def search(action, args):
    path = Path(args.get("path", "."))
    pattern = args["pattern"]
    rg = shutil.which("rg")
    if not path.exists():
        raise ValueError(f"path does not exist: {path}")
    rows = []
    if action == "glob":
        def candidates():
            if rg and path.is_dir():
                for raw in rg_lines([rg, "--files", "--hidden", "--no-ignore", "--sort", "path",
                                     "-g", "!.git", "--", str(path)]):
                    yield Path(os.fsdecode(raw.rstrip(b"\n")))
            else:
                yield from walk(path)
        source = candidates()
        try:
            for candidate in source:
                relative = candidate.relative_to(path) if path.is_dir() else Path(candidate.name)
                if glob_match(relative, pattern):
                    rows.append(str(candidate))
                    if len(rows) == CAP:
                        break
        finally:
            source.close()
    elif rg:
        source = rg_lines([rg, "--json", "--hidden", "--no-ignore", "--sort", "path",
                           "-g", "!.git", "-e", pattern, "--", str(path)])
        try:
            for raw in source:
                record = json.loads(raw)
                if record["type"] == "match":
                    data = record["data"]
                    name = data["path"].get("text")
                    line = data["lines"].get("text")
                    if name is not None and line is not None:
                        rows.append(f'{name}:{data["line_number"]}:{line.rstrip(chr(10))}')
                        if len(rows) == CAP:
                            break
        finally:
            source.close()
    else:
        expression = re.compile(pattern)
        for candidate in walk(path):
            try:
                lines = text_file(candidate).splitlines()
            except (UnicodeError, ValueError):
                continue
            for number, line in enumerate(lines, 1):
                if expression.search(line):
                    rows.append(f"{candidate}:{number}:{line}")
                    if len(rows) == CAP:
                        break
            if len(rows) == CAP:
                break
    capped = len(rows) == CAP
    if capped:
        rows.append(f"Result cap of {CAP} reached; narrow the search for more results.")
    return completed(action, "\n".join(rows), capped=capped, backend="ripgrep" if rg else "python")


def ls(args):
    path = Path(args.get("path", "."))
    entries = [entry.name + ("/" if entry.is_dir() else "") for entry in sorted(path.iterdir())]
    return completed("ls", "\n".join(entries))


def main():
    try:
        action = sys.argv[1]
        args = json.load(sys.stdin)
        if action in ("glob", "grep"):
            result = search(action, args)
        else:
            result = {"read": read, "write": write, "edit": edit, "ls": ls}[action](args)
    except (OSError, ValueError, KeyError, TypeError, re.error) as error:
        result = {"error": {"error": str(error)}}
    print(json.dumps(result, ensure_ascii=True))


if __name__ == "__main__":
    main()
