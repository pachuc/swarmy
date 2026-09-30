#!/usr/bin/env bash
# Fail when markdown names a repository path or a swarmy/swarmyd command that
# does not exist, so docs cannot drift from the code silently.
#
# Paths: every backticked span that reads as a repository-relative path (one
# line, no spaces, contains a slash) must resolve to a file or directory in
# this checkout, either from the repository root or from the markdown file's
# own directory (for `../` references). Spans that cannot be repository paths
# are skipped: URLs, absolute machine paths, environment and home references,
# shell globs, all-caps placeholders, model ids, and paths the tooling
# creates at runtime (`.dev/`, `.swarmy/`, `target/`, anything gitignored).
# Bare filenames without a slash are not repository paths: they name the
# surrounding command's working directory, not this repository.
#
# Commands: every `swarmy ...` and `swarmyd ...` invocation in backticks and
# fenced code blocks must resolve against the command definition in the
# source. Subcommand words must name a real clap subcommand (explicit names
# and aliases count) down to a leaf; past the leaf, flags, placeholders, and
# arguments are not checked, and neither are flags anywhere else. `X --test
# TARGET` (and `cargo test -p PKG --test TARGET`) must name a real
# integration-test target of that package. `cargo run -p swarmy-cli -- ARGS`
# is checked as a `swarmy` invocation. Other binaries (for example
# `swarmy-chaos --continuity`), bare crate names, and prose mentions are out
# of scope. The command tree is parsed from the clap source, so renaming a
# command without updating the docs fails the check; anything the parser
# cannot resolve is an error, never a silent pass.
#
# The only exceptions are backlog proposals for commands that do not exist
# yet (see ALLOWLIST in the program below); each carries a comment naming
# the proposal.
# Usage: scripts/check-docs-accuracy.sh
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_dir"
exec python3 - <<'PYEOF'
import os
import re
import shlex
import subprocess
import sys

ROOT = os.getcwd()

# (binary, subcommand words) prefixes that name proposed commands rather
# than implemented ones. Each entry carries the proposal that owns it.
ALLOWLIST = (
    # Proposed, not implemented: backlog/agent-fork.md specifies a future
    # `swarmy agent fork` that clones an agent's computer.
    ("swarmy", "agent", "fork"),
    # Proposed, not implemented: backlog/chaty.md specifies a future
    # `swarmy channel` command group for the standalone chat tool.
    ("swarmy", "channel"),
    # Proposed, not implemented: backlog/chaty.md specifies a future
    # `swarmy dm` shortcut for direct agent messages.
    ("swarmy", "dm"),
)

GLOBAL_BOOL_FLAGS = {"--json"}
GLOBAL_VALUE_FLAGS = {"--remote", "--auth-file"}
BINARY_PACKAGE = {"swarmy": "swarmy-cli", "swarmyd": "swarmyd"}


def fail(message):
    sys.stderr.write("check-docs-accuracy: %s\n" % message)
    sys.exit(2)


def mask_rust(text):
    """Replace comments, strings, and chars with blanks, keeping offsets."""
    res = list(text)

    def blank(start, end):
        for k in range(start, min(end, len(res))):
            if res[k] != "\n":
                res[k] = " "

    i, n = 0, len(text)
    while i < n:
        c = text[i]
        two = text[i : i + 2]
        if two == "//":
            end = text.find("\n", i)
            end = n if end == -1 else end
            blank(i, end)
            i = end
        elif two == "/*":
            end = text.find("*/", i + 2)
            end = n if end == -1 else end + 2
            blank(i, end)
            i = end
        elif c == '"':
            j = i + 1
            while j < n:
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == '"':
                    j += 1
                    break
                j += 1
            blank(i, j)
            i = j
        elif c == "'":
            m = re.match(r"'(?:\\.|[^'\\])'", text[i:])
            if m:
                blank(i, i + m.end())
                i += m.end()
            else:
                i += 1
        else:
            i += 1
    return "".join(res)


def match_bracket(masked, opening):
    """Return the index of the bracket matching masked[opening]."""
    pairs = {"{": "}", "(": ")", "[": "]"}
    closer = pairs[masked[opening]]
    depth = 0
    for k in range(opening, len(masked)):
        if masked[k] == masked[opening]:
            depth += 1
        elif masked[k] == closer:
            depth -= 1
            if depth == 0:
                return k
    fail("unbalanced brackets while parsing Rust source")


def kebab(name):
    out = []
    for index, char in enumerate(name):
        if char.isupper() and index:
            out.append("-")
        out.append(char.lower())
    return "".join(out)


def attr_names(attr):
    """All command spellings from one #[command(...)] attribute."""
    names = []
    m = re.search(r'name\s*=\s*"([^"]+)"', attr)
    if m:
        names.append(m.group(1))
    for key in ("alias", "aliases", "visible_alias", "visible_aliases"):
        for m in re.finditer(key + r'\s*=\s*(?:"([^"]+)"|\[([^\]]*)\])', attr):
            if m.group(1):
                names.append(m.group(1))
            else:
                names.extend(re.findall(r'"([^"]+)"', m.group(2)))
    return names


def parse_variants(path, masked, orig, start, end):
    """Parse one enum body into [(spellings, subcommand ref or None)]."""
    variants = []
    pending_attrs = []
    i = start
    while i < end:
        char = masked[i]
        if char in " \t\n\r,":
            i += 1
            continue
        if char == "#":
            close = masked.find("]", i, end)
            if close == -1:
                fail("%s: unterminated attribute" % path)
            pending_attrs.append(orig[i : close + 1])
            i = close + 1
            continue
        m = re.match(r"[A-Z][A-Za-z0-9]*", masked[i:])
        if not m:
            fail("%s: cannot parse enum variant near %r" % (path, masked[i : i + 40]))
        name = m.group(0)
        i += m.end()
        while i < end and masked[i] in " \t\n\r":
            i += 1
        sub = None
        if i < end and masked[i] in "({":
            close = match_bracket(masked, i)
            body = orig[i : close + 1]
            m = re.search(r"#\[command\(subcommand\)\]\s*command\s*:\s*([\w:]+)", body)
            if m:
                sub = m.group(1)
            i = close + 1
        spellings = {kebab(name)}
        for attr in pending_attrs:
            if re.search(r"\bcommand\s*\(", attr):
                spellings.update(attr_names(attr))
        pending_attrs = []
        variants.append((sorted(spellings), sub))
    if not variants:
        fail("%s: parsed an empty command enum" % path)
    return variants


def parse_enum(path, enum_name):
    with open(path, encoding="utf-8") as handle:
        orig = handle.read()
    masked = mask_rust(orig)
    m = re.search(r"\benum\s+" + re.escape(enum_name) + r"\b", masked)
    if not m:
        fail("%s: no enum %s" % (path, enum_name))
    opening = masked.find("{", m.end())
    if opening == -1:
        fail("%s: cannot find body of enum %s" % (path, enum_name))
    return parse_variants(path, masked, orig, opening + 1, match_bracket(masked, opening))


def mod_map(path):
    """Map `mod foo;` names to files next to the given source file."""
    with open(path, encoding="utf-8") as handle:
        masked = mask_rust(handle.read())
    directory = os.path.dirname(path)
    mapping = {}
    for m in re.finditer(r"(?m)^\s*mod\s+([a-z_][a-z0-9_]*)\s*;", masked):
        name = m.group(1)
        for candidate in (
            os.path.join(directory, name + ".rs"),
            os.path.join(directory, name, "mod.rs"),
        ):
            if os.path.isfile(candidate):
                mapping[name] = candidate
                break
    return mapping


def find_command_enum(crate_dir):
    """Locate the file declaring `enum Command` in a crate's sources."""
    found = []
    src = os.path.join(crate_dir, "src")
    for dirpath, _, filenames in os.walk(src):
        for filename in sorted(filenames):
            if not filename.endswith(".rs"):
                continue
            path = os.path.join(dirpath, filename)
            with open(path, encoding="utf-8") as handle:
                if re.search(r"\benum\s+Command\b", mask_rust(handle.read())):
                    found.append(path)
    if not found:
        fail("%s: no enum Command in crate sources" % crate_dir)
    if len(found) > 1:
        fail("%s: ambiguous enum Command: %s" % (crate_dir, ", ".join(found)))
    return found[0]


def resolve_ref(ref, current_file, crate_src):
    """Resolve a subcommand type to (file, enum name)."""
    parts = ref.split("::")
    if len(parts) == 1:
        return current_file, parts[0]
    if parts[0] == "crate":
        path = os.path.join(crate_src, *parts[1:-1]) + ".rs"
        if not os.path.isfile(path):
            fail("cannot resolve subcommand type %s from %s" % (ref, current_file))
        return path, parts[-1]
    head = parts[0]
    sibling = os.path.join(os.path.dirname(current_file), head + ".rs")
    if os.path.isfile(sibling):
        return sibling, parts[-1]
    mapping = mod_map(current_file)
    if head in mapping:
        return mapping[head], parts[-1]
    crate_dir = os.path.join(ROOT, "crates", head.replace("_", "-"))
    if os.path.isdir(crate_dir):
        return find_command_enum(crate_dir), parts[-1]
    fail("cannot resolve subcommand type %s from %s" % (ref, current_file))


def build_tree(path, enum_name, seen=None):
    """Build {spelling: child tree or None} for one command enum."""
    seen = seen or set()
    key = (path, enum_name)
    if key in seen:
        fail("recursive subcommand type %s in %s" % (enum_name, path))
    seen = seen | {key}
    variants = parse_enum(path, enum_name)
    try:
        crate_src = os.path.dirname(path).split(os.sep + "src" + os.sep)[0] + os.sep + "src"
    except IndexError:
        crate_src = os.path.dirname(path)
    if not os.path.isdir(crate_src):
        crate_src = os.path.dirname(path)
    tree = {}
    for spellings, sub in variants:
        child = None
        if sub is not None:
            sub_path, sub_enum = resolve_ref(sub, path, crate_src)
            child = build_tree(sub_path, sub_enum, seen)
        for spelling in spellings:
            if spelling in tree:
                fail("%s: duplicate command spelling %r" % (path, spelling))
            tree[spelling] = child
    return tree


def test_targets(package):
    """All integration-test target names of a package."""
    crate_dir = os.path.join(ROOT, "crates", package)
    tests_dir = os.path.join(crate_dir, "tests")
    targets = set()
    if os.path.isdir(tests_dir):
        for entry in os.listdir(tests_dir):
            if entry.endswith(".rs"):
                targets.add(entry[:-3])
            elif os.path.isfile(os.path.join(tests_dir, entry, "mod.rs")):
                targets.add(entry)
    manifest = os.path.join(crate_dir, "Cargo.toml")
    if os.path.isfile(manifest):
        try:
            import tomllib
        except ImportError:
            tomllib = None
        if tomllib is not None:
            with open(manifest, "rb") as handle:
                data = tomllib.load(handle)
            tests = data.get("test", [])
            if isinstance(tests, dict):
                tests = [tests]
            for table in tests:
                if isinstance(table, dict) and table.get("name"):
                    targets.add(table["name"])
    return targets


def tracked_files():
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z"],
            capture_output=True,
            text=True,
            check=True,
            cwd=ROOT,
        )
    except (subprocess.CalledProcessError, OSError):
        return None
    return set(out.stdout.split("\0")) - {""}


def ignored_paths(paths):
    try:
        proc = subprocess.run(
            ["git", "check-ignore", "--stdin"],
            capture_output=True,
            text=True,
            cwd=ROOT,
            input="\n".join(sorted(set(paths))),
        )
    except OSError:
        return set()
    if proc.returncode not in (0, 1):
        return set()
    return set(line for line in proc.stdout.splitlines() if line)


BINARY_RE = re.compile(r"(?:^|[\s'\";(|&])(?:[\w./$~-]*\/)?(swarmy|swarmyd)(?![\w-])")
TEST_RE = re.compile(r"(swarmy(?:-[\w]+)*|swarmyd)\s+--test\s+([\w-]+)")
CARGO_RUN_RE = re.compile(r"cargo\s+run\b.*?-p\s+([\w-]+)\s+--\s+(.*)$")
WORD_RE = re.compile(r"[a-z][a-z0-9_-]*\Z")
PATH_CHARS_RE = re.compile(r"[A-Za-z0-9_.][A-Za-z0-9_./-]*\Z")
CAPS_PART_RE = re.compile(r"[A-Z][A-Z0-9_]*\Z")
EXT_RE = re.compile(r"\.[A-Za-z][A-Za-z0-9-]*\Z")
BACKTICK_RE = re.compile(r"`([^`\n]+)`")


def candidate_rels(text, docdir):
    """Repo-relative paths a backticked span could name, or None."""
    if not text or "/" not in text:
        return None
    if " " in text or "\t" in text:
        return None
    if "://" in text:
        return None
    if text[0] in "/$~":
        return None
    if re.search(r"[{}<>$|=&;!()\[\]\"'\\*?]", text):
        return None
    if not re.search(r"[a-z]", text):
        return None
    if not PATH_CHARS_RE.match(text):
        return None
    if any(CAPS_PART_RE.match(part) for part in text.strip("/").split("/")):
        return None
    core = text[2:] if text.startswith("./") else text
    first = core.split("/")[0]
    last = core.rstrip("/").split("/")[-1]
    if first not in Checker.root_entries and not core.startswith("../") and not EXT_RE.search(last):
        return None
    rels = [os.path.normpath(core)]
    if docdir:
        resolved = os.path.normpath(os.path.join(docdir, core))
        if not resolved.startswith(".."):
            rels.append(resolved)
    # Anything still escaping the checkout cannot be a repository path.
    rels = [rel for rel in rels if not rel.startswith("..")]
    if not rels:
        return None
    return rels


class Checker:
    root_entries = set()

    def __init__(self):
        cli_main = os.path.join(ROOT, "crates", "swarmy-cli", "src", "main.rs")
        vol_command = os.path.join(ROOT, "crates", "swarmyd", "src", "vol_command.rs")
        for path in (cli_main, vol_command):
            if not os.path.isfile(path):
                fail("missing CLI source %s" % path)
        cli_tree = build_tree(cli_main, "Command")
        # swarmyd dispatches `vol` by inspecting argv directly in main.rs
        # rather than through clap, so the top level is stated here while
        # the volume subcommands still parse from vol_command.rs.
        self.trees = {
            "swarmy": cli_tree,
            "swarmyd": {"vol": build_tree(vol_command, "Command")},
        }
        self.errors = []
        self.seen = set()
        self.path_count = 0
        self.command_count = 0
        self.files = 0

    def error(self, location, text, message):
        key = (location, text, message)
        if key not in self.seen:
            self.seen.add(key)
            self.errors.append("%s: %s: %s" % (location, message, text))

    def check_test_target(self, package, target, location, text):
        if not os.path.isdir(os.path.join(ROOT, "crates", package)):
            self.error(location, text, "unknown package %r" % package)
            return
        if target not in test_targets(package):
            self.error(
                location, text, "unknown test target %r for package %r" % (target, package)
            )

    def walk(self, binary, tokens, location, text):
        node = self.trees[binary]
        consumed = [binary]
        for token in tokens:
            if node is None:
                return
            if token.startswith("-"):
                if token in GLOBAL_BOOL_FLAGS:
                    continue
                if token in GLOBAL_VALUE_FLAGS:
                    continue
                if token.startswith("--") and "=" in token:
                    continue
                return
            if WORD_RE.match(token) and token in node:
                consumed.append(token)
                node = node[token]
                continue
            if token in ("...", "\u2026"):
                return
            attempted = tuple(consumed + [token])
            if any(entry[: len(attempted)] == attempted for entry in ALLOWLIST):
                return
            self.error(location, text, "unknown command %r" % " ".join(attempted))
            return

    def check_command_text(self, binary, rest, location, text):
        before, _, _ = rest.partition(" #")
        try:
            tokens = shlex.split(before, posix=True)
        except ValueError:
            tokens = before.split()
        if tokens and tokens[0].startswith("/"):
            return
        if tokens:
            first = tokens[0]
            # Table cells, prose fragments, paths, and values only name an
            # invocation when the binary is followed by a flag or a plain
            # word; anything else (quotes, pipes, punctuation, emails,
            # brace shapes) is not a command.
            if not first.startswith("-") and not re.fullmatch(r"[A-Za-z][A-Za-z0-9_/-]*", first):
                return
        self.command_count += 1
        # A global value flag consumes its argument; anything else shaped
        # like a flag ends static checking, since flags are not validated.
        expanded = []
        skip_next = False
        for token in tokens:
            if skip_next:
                skip_next = False
                continue
            expanded.append(token)
            if token in GLOBAL_VALUE_FLAGS:
                skip_next = True
        self.walk(binary, expanded, location, text)

    def scan_commands(self, line, location):
        for match in BINARY_RE.finditer(line):
            self.check_command_text(match.group(1), line[match.end() :], location, line.strip())
        for match in TEST_RE.finditer(line):
            token, target = match.group(1), match.group(2)
            package = token if "-" in token else BINARY_PACKAGE[token]
            self.check_test_target(package, target, location, line.strip())
        if re.search(r"cargo\s+test\b", line):
            packages = re.findall(r"-p\s+([\w-]+)", line)
            targets = re.findall(r"--test\s+([\w-]+)", line)
            if packages and targets:
                for package in packages:
                    for target in targets:
                        self.check_test_target(package, target, location, line.strip())
        match = CARGO_RUN_RE.search(line)
        if match and match.group(1) == "swarmy-cli":
            self.check_command_text("swarmy", match.group(2), location, line.strip())

    def check_paths(self, pending, ignored):
        for text, location, rels in pending:
            self.path_count += 1
            if any(os.path.exists(os.path.join(ROOT, rel)) for rel in rels):
                continue
            if any(rel in self.tracked for rel in rels):
                self.error(location, text, "tracked path was deleted")
                continue
            # Directory-only ignore patterns match only with a trailing
            # slash, which normpath strips, so try both spellings.
            if any(rel in ignored or rel + "/" in ignored for rel in rels):
                continue
            self.error(location, text, "unknown repository path")

    def logical_lines(self, lines):
        """Split lines into (lineno, text, in_fence), joining continuations."""
        logical = []
        in_fence = False
        buffer = []
        start = 0
        for index, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("```") and stripped.count("```") == 1:
                if buffer:
                    logical.append((start, " ".join(buffer), True))
                    buffer = []
                logical.append((index + 1, line, False))
                in_fence = not in_fence
                continue
            if in_fence and line.rstrip().endswith("\\"):
                if not buffer:
                    start = index + 1
                buffer.append(line.rstrip()[:-1])
                continue
            if buffer:
                buffer.append(line)
                logical.append((start, " ".join(buffer), True))
                buffer = []
                continue
            logical.append((index + 1, line, in_fence))
        if buffer:
            logical.append((start, " ".join(buffer), True))
        return logical

    def run(self):
        Checker.root_entries = set(os.listdir(ROOT)) - {".git"}
        self.tracked = tracked_files()
        if self.tracked is None:
            self.tracked = set()
            md_files = []
            for dirpath, dirnames, filenames in os.walk(ROOT):
                if ".git" in dirnames:
                    dirnames.remove(".git")
                for filename in sorted(filenames):
                    if filename.endswith(".md"):
                        md_files.append(os.path.relpath(os.path.join(dirpath, filename), ROOT))
        else:
            md_files = sorted(path for path in self.tracked if path.endswith(".md"))
        self.files = len(md_files)
        pending = []
        for path in md_files:
            with open(os.path.join(ROOT, path), encoding="utf-8") as handle:
                lines = handle.read().splitlines()
            docdir = os.path.dirname(path)
            for lineno, line, in_fence in self.logical_lines(lines):
                location = "%s:%d" % (path, lineno)
                for match in BACKTICK_RE.finditer(line):
                    text = match.group(1).strip()
                    rels = candidate_rels(text, docdir)
                    if rels is not None:
                        pending.append((text, location, rels))
                    self.scan_commands(match.group(1), location)
                if in_fence:
                    self.scan_commands(line, location)
        queries = []
        for _, _, rels in pending:
            for rel in rels:
                queries.append(rel)
                queries.append(rel + "/")
        ignored = ignored_paths(
            [rel for rel in queries if not rel.startswith("..")]
        )
        self.check_paths(pending, ignored)
        if self.errors:
            sys.stderr.write("\n".join(self.errors) + "\n")
            sys.stderr.write(
                "check-docs-accuracy: %d problem(s) in %d markdown file(s)\n"
                % (len(self.errors), self.files)
            )
            return 1
        print(
            "check-docs-accuracy: ok (%d files, %d paths, %d commands checked)"
            % (self.files, self.path_count, self.command_count)
        )
        return 0


sys.exit(Checker().run())
PYEOF
