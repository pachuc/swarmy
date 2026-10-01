#!/usr/bin/env python3
"""Fail when a test root or test module lacks the sleep-ban deny.

Fixed sleeps in tests hide timing assumptions, so every test target opts
into the ban with `#![deny(clippy::disallowed_methods)]` at its root and
polls with `swarmy_testkit::eventually` instead. This script enforces the
opt-in structurally, the same way `check-anyhow-in-libraries.sh` enforces
the error-type rule:

- Test roots are every `crates/*/tests/*.rs` file, top level only.
  Subdirectories are submodules and inherit the root's setting.
- Test modules are, in `crates/*/src/**/*.rs`, a `#[cfg(test)]` or
  `#[cfg(all(test, ...))]` attribute followed, possibly after other `#[...]`
  attributes, by `mod NAME {` or `mod NAME;`. `cfg(any(test, ...))` is not
  test-only and is skipped.
- A file is compliant when its leading inner attributes contain the deny
  line. The scan skips blank lines, `//` comments, and `//!` lines; any
  other `#![...]` is also allowed; the scan stops at the first item. A
  commented-out deny does not count.
- An inline module is compliant when its first lines inside the brace
  carry the deny. A file module must resolve to a compliant file:
  `#[path = "..."]` resolves relative to the declaring file's directory,
  otherwise `dir/NAME.rs` or `dir/NAME/mod.rs` when declared from lib.rs,
  main.rs, or mod.rs, and `dir/STEM/NAME.rs` or `dir/STEM/NAME/mod.rs`
  otherwise. An unresolvable module is an internal error.
- A module is exempt when its declaring file is compliant, or when it is
  nested (by rustfmt indentation) inside a compliant inline module.

Output is `path:line: test module NAME lacks
#![deny(clippy::disallowed_methods)]`, one line per finding. Exit 1 on any
finding, 0 when clean, 2 on an internal error.
"""

import os
import re
import sys

DENY = "#![deny(clippy::disallowed_methods)]"

MOD_RE = re.compile(
    r"(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*([;{])"
)
PATH_RE = re.compile(r'#\[\s*path\s*=\s*"([^"]+)"\s*\]')
CFG_RE = re.compile(r"#\[\s*cfg\s*\(")


def fail(message):
    """Report an internal error: exit 2, never a finding."""
    print(f"check-test-sleep-ban: error: {message}", file=sys.stderr)
    sys.exit(2)


def read_lines(path):
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read().split("\n")
    except OSError as error:
        fail(f"cannot read {path}: {error}")


def leading_compliant(lines):
    """True when the leading inner attributes contain the deny line."""
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue
        if stripped.startswith("#!["):
            if stripped == DENY:
                return True
            continue
        return False
    return False


def is_test_cfg(inner):
    """True for `test` and `all(test, ...)`; `any(test, ...)` is not test-only."""
    code = re.sub(r'"[^"]*"', "", inner).strip()
    if code == "test":
        return True
    if re.match(r"all\s*\(", code):
        return re.search(r"(?<![A-Za-z0-9_])test(?![A-Za-z0-9_])", code) is not None
    return False


def split_attributes(line):
    """Split one line into its `#[...]` groups plus the remaining code."""
    groups = []
    rest = []
    index = 0
    while True:
        start = line.find("#[", index)
        if start == -1:
            rest.append(line[index:])
            break
        rest.append(line[index:start])
        depth = 0
        pos = start
        while pos < len(line):
            if line[pos] == "[":
                depth += 1
            elif line[pos] == "]":
                depth -= 1
                if depth == 0:
                    break
            pos += 1
        if depth != 0:
            return groups, None
        groups.append(line[start : pos + 1])
        index = pos + 1
    return groups, "".join(rest)


def cfg_inner(group):
    """Return the text inside `cfg(...)`, or None when unbalanced."""
    match = CFG_RE.match(group)
    if not match:
        return None
    depth = 0
    for pos in range(match.end() - 1, len(group)):
        if group[pos] == "(":
            depth += 1
        elif group[pos] == ")":
            depth -= 1
            if depth == 0:
                return group[match.end() : pos]
    return None


def inner_compliant(lines, start):
    """True when the lines inside an inline module's brace carry the deny."""
    for line in lines[start:]:
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue
        if stripped.startswith("#!["):
            if stripped == DENY:
                return True
            continue
        return False
    return False


def resolve_module(decl_file, name, path_attr):
    """Resolve a `mod NAME;` declaration to its file, or None."""
    base = os.path.dirname(decl_file)
    if path_attr is not None:
        candidate = os.path.normpath(os.path.join(base, path_attr))
        return candidate if os.path.isfile(candidate) else None
    stem = os.path.splitext(os.path.basename(decl_file))[0]
    if stem in ("lib", "main", "mod"):
        candidates = [
            os.path.join(base, name + ".rs"),
            os.path.join(base, name, "mod.rs"),
        ]
    else:
        candidates = [
            os.path.join(base, stem, name + ".rs"),
            os.path.join(base, stem, name, "mod.rs"),
        ]
    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate
    return None


def check_file(path, relpath, findings):
    """Check one `src` file's test modules, recording findings."""
    lines = read_lines(path)
    file_compliant = leading_compliant(lines)
    stack = []
    pending = None
    depth = 0
    pending_depth_start = 0

    def pop_to(indent):
        while stack and stack[-1][0] >= indent:
            stack.pop()

    for number, raw in enumerate(lines, start=1):
        line = raw.expandtabs(8)
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        pop_to(indent)

        if depth > 0:
            depth += line.count("[") - line.count("]")
            for group in split_attributes(line)[0]:
                match = PATH_RE.match(group.strip())
                if match and pending is not None:
                    pending["path"] = match.group(1)
            if depth <= 0:
                depth = 0
            continue

        groups, rest = split_attributes(line)
        if rest is None:
            for group in groups:
                text = group.strip()
                inner = cfg_inner(text)
                if inner is not None and is_test_cfg(inner):
                    pending = {"line": number, "path": None}
                else:
                    match = PATH_RE.match(text)
                    if (
                        match
                        and pending is not None
                        and pending["path"] is None
                    ):
                        pending["path"] = match.group(1)
            depth = 1
            continue
        code = rest.split("//", 1)[0]

        for group in groups:
            text = group.strip()
            inner = cfg_inner(text)
            if inner is not None and is_test_cfg(inner):
                pending = {"line": number, "path": None}
            else:
                match = PATH_RE.match(text)
                if match and pending is not None and pending["path"] is None:
                    pending["path"] = match.group(1)

        match = MOD_RE.search(code)
        if match and pending is not None:
            name, kind = match.group(1), match.group(2)
            exempt = file_compliant or any(compliant for _, compliant in stack)
            if not exempt:
                if kind == "{":
                    brace = code.find("{", match.start())
                    after = code[brace + 1 :].split("//", 1)[0].strip()
                    if after:
                        compliant = after.startswith("#![") and DENY in after
                    else:
                        compliant = inner_compliant(lines, number)
                    if not compliant:
                        findings.append((relpath, number, name))
                    stack.append((indent, compliant))
                else:
                    target = resolve_module(path, name, pending["path"])
                    if target is None:
                        fail(f"{relpath}:{number}: cannot resolve test module {name}")
                    if not leading_compliant(read_lines(target)):
                        findings.append((relpath, number, name))
            elif kind == "{":
                stack.append((indent, True))
            pending = None
        elif match:
            pending = None
        elif code.strip():
            pending = None


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    crates = os.path.join(root, "crates")
    if not os.path.isdir(crates):
        fail(f"no crates directory under {root}")
    findings = []

    for crate in sorted(os.listdir(crates)):
        tests = os.path.join(crates, crate, "tests")
        if os.path.isdir(tests):
            for entry in sorted(os.listdir(tests)):
                if not entry.endswith(".rs"):
                    continue
                path = os.path.join(tests, entry)
                if not os.path.isfile(path):
                    continue
                if not leading_compliant(read_lines(path)):
                    relpath = os.path.relpath(path, root)
                    findings.append((relpath, 1, os.path.splitext(entry)[0]))

    for dirpath, _dirnames, filenames in os.walk(os.path.join(crates)):
        rel = os.path.relpath(dirpath, crates)
        parts = rel.split(os.sep)
        if len(parts) < 2 or parts[1] != "src":
            continue
        for entry in sorted(filenames):
            if not entry.endswith(".rs"):
                continue
            path = os.path.join(dirpath, entry)
            check_file(path, os.path.relpath(path, root), findings)

    for relpath, number, name in findings:
        print(f"{relpath}:{number}: test module {name} lacks {DENY}")
    sys.exit(1 if findings else 0)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as error:  # noqa: BLE001 - any crash is exit 2 by contract
        fail(f"internal error: {error}")
