#!/usr/bin/env python3
# Fail when markdown names a repository path or test target that does not
# exist, so docs cannot drift from the code silently. Command names and flags
# are out of scope here: the swarmy-docs crate checks those against the real
# clap trees in Rust unit tests.
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
# Test targets: `X --test TARGET` and `cargo test -p PKG --test TARGET` must
# name a real integration-test target of that package.
#
# Files under `backlog/` are proposals that name files which do not exist yet,
# so they are never checked.
# Usage: scripts/check-docs-accuracy.py
import os
import re
import subprocess
import sys
import tomllib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BINARY_PACKAGE = {"swarmy": "swarmy-cli", "swarmyd": "swarmyd"}


def fail(message):
    sys.stderr.write("check-docs-accuracy: %s\n" % message)
    sys.exit(2)


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
    except (subprocess.CalledProcessError, OSError) as error:
        fail("cannot list tracked files: %s" % error)
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


TEST_RE = re.compile(r"(swarmy(?:-[\w]+)*|swarmyd)\s+--test\s+([\w-]+)")
WORD_RE = re.compile(r"[a-z][a-z0-9_-]*\Z")
PATH_CHARS_RE = re.compile(r"[A-Za-z0-9_.][A-Za-z0-9_./-]*\Z")
CAPS_PART_RE = re.compile(r"[A-Z][A-Z0-9_]*\Z")
EXT_RE = re.compile(r"\.[A-Za-z][A-Za-z0-9-]*\Z")
BACKTICK_RE = re.compile(r"`([^`\n]+)`")


def candidate_rels(text, docdir, root_entries):
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
    if first not in root_entries and not core.startswith("../") and not EXT_RE.search(last):
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
    def __init__(self):
        self.errors = []
        self.seen = set()
        self.path_count = 0
        self.target_count = 0
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

    def scan_test_targets(self, line, location):
        for match in TEST_RE.finditer(line):
            token, target = match.group(1), match.group(2)
            package = token if "-" in token else BINARY_PACKAGE[token]
            self.target_count += 1
            self.check_test_target(package, target, location, line.strip())
        if re.search(r"cargo\s+test\b", line):
            packages = re.findall(r"-p\s+([\w-]+)", line)
            targets = re.findall(r"--test\s+([\w-]+)", line)
            if packages and targets:
                for package in packages:
                    for target in targets:
                        self.target_count += 1
                        self.check_test_target(package, target, location, line.strip())

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
        root_entries = set(os.listdir(ROOT)) - {".git"}
        self.tracked = tracked_files()
        md_files = sorted(
            path
            for path in self.tracked
            if path.endswith(".md") and not path.startswith("backlog/")
        )
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
                    rels = candidate_rels(text, docdir, root_entries)
                    if rels is not None:
                        pending.append((text, location, rels))
                    self.scan_test_targets(match.group(1), location)
                if in_fence:
                    self.scan_test_targets(line, location)
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
            "check-docs-accuracy: ok (%d files, %d paths, %d test targets checked)"
            % (self.files, self.path_count, self.target_count)
        )
        return 0


sys.exit(Checker().run())
