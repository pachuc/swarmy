#!/usr/bin/env python3
# Docs accuracy: paths, Markdown links and anchors, and test targets (default),
# or documented commands (--commands). --commands prints location, binary, and
# args split into words (tab-separated, args joined by \x1f, quoted values
# masked) for the Rust clap walks in swarmy-cli, swarmyd, and swarmy-chaos. Both
# modes share tracked listing, fence joining, backtick spans, and the backlog/
# skip.
import os
import re
import subprocess
import sys
import tomllib
import urllib.parse
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Binaries whose documented commands are walked; each needs a clap-tree test
# that calls swarmy_testkit::check_docs_commands.
BINARIES = ("swarmy-chaos", "swarmyd", "swarmy")
BINARY_PACKAGE = {"swarmy": "swarmy-cli", "swarmyd": "swarmyd"}
CARGO_BINARY = {"swarmy-cli": "swarmy", "swarmyd": "swarmyd", "swarmy-chaos": "swarmy-chaos"}
def fail(message):
    sys.stderr.write("check-docs-accuracy: %s\n" % message)
    sys.exit(2)
def mask_quoted(line):
    out, in_q, esc = [], False, False
    for c in line:
        if in_q:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_q = False
                out.append(c)
                continue
            out.append(" ")
        elif c == '"':
            in_q, out = True, out + [c]
        else:
            out.append(c)
    return "".join(out)
def logical_lines(lines):
    logical, in_fence, buf, start = [], False, [], 0
    for i, line in enumerate(lines):
        s = line.strip()
        if s.startswith("```") and s.count("```") == 1:
            if buf:
                logical.append((start, " ".join(buf), True))
                buf = []
            logical.append((i + 1, line, False))
            in_fence = not in_fence
            continue
        if in_fence and line.rstrip().endswith("\\"):
            if not buf:
                start = i + 1
            buf.append(line.rstrip()[:-1])
            continue
        if buf:
            buf.append(line)
            logical.append((start, " ".join(buf), True))
            buf = []
            continue
        logical.append((i + 1, line, in_fence))
    if buf:
        logical.append((start, " ".join(buf), True))
    return logical
def tracked_files():
    try:
        out = subprocess.run(["git", "ls-files", "-z"], capture_output=True,
                             text=True, check=True, cwd=ROOT)
    except (subprocess.CalledProcessError, OSError) as e:
        fail("cannot list tracked files: %s" % e)
    return set(out.stdout.split("\0")) - {""}
def ignored_paths(paths):
    try:
        proc = subprocess.run(["git", "check-ignore", "--stdin"], capture_output=True,
                              text=True, cwd=ROOT, input="\n".join(sorted(set(paths))))
    except OSError as e:
        fail("cannot check ignored paths: %s" % e)
    if proc.returncode not in (0, 1):
        fail("cannot check ignored paths: %s" % proc.stderr.strip())
    return set(l for l in proc.stdout.splitlines() if l)
def test_targets(package):
    d = os.path.join(ROOT, "crates", package, "tests")
    targets = set()
    if os.path.isdir(d):
        for e in os.listdir(d):
            if e.endswith(".rs"):
                targets.add(e[:-3])
            elif os.path.isfile(os.path.join(d, e, "mod.rs")):
                targets.add(e)
    m = os.path.join(ROOT, "crates", package, "Cargo.toml")
    if os.path.isfile(m):
        with open(m, "rb") as h:
            tests = tomllib.load(h).get("test", [])
        if isinstance(tests, dict):
            tests = [tests]
        for t in tests:
            if isinstance(t, dict) and t.get("name"):
                targets.add(t["name"])
    return targets
TEST_RE = re.compile(r"(swarmy(?:-[\w]+)*|swarmyd)\s+--test\s+([\w-]+)")
BACKTICK_RE = re.compile(r"`([^`\n]+)`")
PATH_CHARS_RE = re.compile(r"[A-Za-z0-9_.][A-Za-z0-9_./-]*\Z")
CAPS_PART_RE = re.compile(r"[A-Z][A-Z0-9_]*\Z")
EXT_RE = re.compile(r"\.[A-Za-z][A-Za-z0-9-]*\Z")
def candidate_rels(text, docdir, root_entries):
    if not text or "/" not in text or " " in text or "\t" in text:
        return None
    if "://" in text or text[0] in "/$~":
        return None
    if re.search(r"[{}<>$|=&;!()\[\]\"'\\*?]", text):
        return None
    if not re.search(r"[a-z]", text) or not PATH_CHARS_RE.match(text):
        return None
    if any(CAPS_PART_RE.match(p) for p in text.strip("/").split("/")):
        return None
    core = text[2:] if text.startswith("./") else text
    first, last = core.split("/")[0], core.rstrip("/").split("/")[-1]
    if first not in root_entries and not core.startswith("../") and not EXT_RE.search(last):
        return None
    rels = [os.path.normpath(core)]
    if docdir:
        r = os.path.normpath(os.path.join(docdir, core))
        if not r.startswith(".."):
            rels.append(r)
    rels = [r for r in rels if not r.startswith("..")]
    return rels or None
# Inline Markdown links and images: [text](target) or ![alt](target), with an
# optional "title". Reference-style links are not used in this repository.
LINK_RE = re.compile(r"\[(?:[^\[\]]|\[[^\]]*\])*\]\(\s*<?([^()\s<>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
CODE_SPAN_RE = re.compile(r"`[^`\n]*`")
HEADING_RE = re.compile(r"\s{0,3}(#{1,6})\s+(.*?)(?:\s+#+)?\s*\Z")
SCHEME_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*:")
def heading_slug(text):
    # GitHub's rule: drop link targets, HTML tags, code and emphasis markers,
    # lowercase, delete everything but letters, digits, underscores, hyphens,
    # and spaces, then turn each space into a hyphen.
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text).replace("`", "").replace("*", "")
    return re.sub(r"[^\w\- ]", "", text.strip().lower()).replace(" ", "-")
_anchor_cache = {}
def anchors(path):
    # Headings inside fenced code blocks (shell comments) are not anchors.
    # Repeated headings get -1, -2, ... suffixes, as on GitHub.
    if path not in _anchor_cache:
        found, counts = set(), {}
        with open(os.path.join(ROOT, path), encoding="utf-8") as h:
            lines = h.read().splitlines()
        for _, line, in_fence in logical_lines(lines):
            m = None if in_fence else HEADING_RE.match(line)
            if m:
                slug = heading_slug(m.group(2))
                n = counts.get(slug, 0)
                counts[slug] = n + 1
                found.add(slug if n == 0 else "%s-%d" % (slug, n))
        _anchor_cache[path] = found
    return _anchor_cache[path]
def md_files():
    return sorted(p for p in tracked_files() if p.endswith(".md") and not p.startswith("backlog/"))
def split_args(text):
    words, cur, quote, chars, i = [], [], None, list(text), 0
    while i < len(chars):
        c = chars[i]
        if quote:
            if c == "\\" and i + 1 < len(chars):
                i += 1
                cur.append(" ")
            elif c == quote:
                quote = None
            else:
                cur.append(" " if c.isspace() else "x")
        elif c in ("'", '"'):
            quote = c
        elif c == "\\" and i + 1 < len(chars):
            i += 1
            cur.append(chars[i])
        elif c.isspace():
            if cur:
                words.append("".join(cur))
                cur = []
        else:
            cur.append(c)
        i += 1
    if cur:
        words.append("".join(cur))
    return words
def is_command_word(t):
    return bool(re.match(r"[A-Za-z][A-Za-z0-9_/-]*\Z", t))
def find_invocations(line):
    found, i = [], 0
    while i < len(line):
        prev = line[i - 1] if i else "\0"
        if i == 0 or prev.isspace() or prev in "'\";(|&":
            starts, j = [i], i
            while j < len(line) and (line[j].isalnum() or line[j] in "_./$~-"):
                if line[j] == "/":
                    starts.append(j + 1)
                j += 1
            hit = False
            for s in reversed(starts):
                for name in BINARIES:
                    if line[s:].startswith(name):
                        e = s + len(name)
                        nxt = line[e] if e < len(line) else ""
                        if not (nxt.isalnum() or nxt in "_-"):
                            found.append((name, line[e:]))
                            i, hit = e, True
                            break
                if hit:
                    break
            if hit:
                continue
            i += 1
            continue
        i += 1
    return found
def invocation_args(rest):
    tokens = split_args(rest.split(" #", 1)[0])
    if not tokens or tokens[0].startswith("/"):
        return None
    if not tokens[0].startswith("-") and not is_command_word(tokens[0]):
        return None
    return tokens
def invocations_in(text):
    out, words = [], text.split()
    try:
        pkg = words[words.index("-p", words.index("run", words.index("cargo") + 1) + 1) + 1]
        di = words.index("--", words.index("-p", words.index("run", words.index("cargo") + 1) + 1) + 1)
        if pkg in CARGO_BINARY:
            inv = invocation_args(" ".join(words[di + 1:]))
            if inv is not None:
                out.append((CARGO_BINARY[pkg], inv))
    except (ValueError, IndexError):
        pass
    for binary, rest in find_invocations(text):
        inv = invocation_args(rest)
        if inv is not None:
            out.append((binary, inv))
    return out
def command_rows():
    rows, seen = [], set()
    for path in md_files():
        with open(os.path.join(ROOT, path), encoding="utf-8") as h:
            lines = h.read().splitlines()
        for lineno, text, in_fence in logical_lines(lines):
            loc = "%s:%d" % (path, lineno)
            spans = [m.group(1).strip() for m in BACKTICK_RE.finditer(text)]
            if in_fence:
                spans.append(mask_quoted(text))
            for span in spans:
                if span:
                    for binary, args in invocations_in(span):
                        if (binary, tuple(args)) not in seen:
                            seen.add((binary, tuple(args)))
                            src = re.sub(r"\s+", " ", span.strip())[:200]
                            rows.append("%s\t%s\t%s\t%s" % (loc, binary, "\x1f".join(args), src))
    return rows
class Checker:
    def __init__(self):
        self.errors, self.seen, self.paths, self.targets, self.links = [], set(), 0, 0, 0
    def error(self, loc, text, msg):
        if (loc, text, msg) not in self.seen:
            self.seen.add((loc, text, msg))
            self.errors.append("%s: %s: %s" % (loc, msg, text))
    def check_target(self, package, target, loc, text):
        if not os.path.isdir(os.path.join(ROOT, "crates", package)):
            self.error(loc, text, "unknown package %r" % package)
        elif target not in test_targets(package):
            self.error(loc, text, "unknown test target %r for package %r" % (target, package))
    def scan_targets(self, line, loc):
        for m in TEST_RE.finditer(mask_quoted(line)):
            tok, tgt = m.group(1), m.group(2)
            self.targets += 1
            self.check_target(tok if "-" in tok else BINARY_PACKAGE[tok], tgt, loc, line.strip())
        if re.search(r"cargo\s+test\b", mask_quoted(line)):
            pkgs, tgts = re.findall(r"-p\s+([\w-]+)", line), re.findall(r"--test\s+([\w-]+)", line)
            if pkgs and tgts:
                for p in pkgs:
                    for t in tgts:
                        self.targets += 1
                        self.check_target(p, t, loc, line.strip())
    def check_links(self, path, line, loc):
        for m in LINK_RE.finditer(CODE_SPAN_RE.sub("", line)):
            target = m.group(1)
            if SCHEME_RE.match(target):
                continue
            self.links += 1
            file_part, _, anchor = target.partition("#")
            rel = path
            if file_part:
                rel = os.path.normpath(os.path.join(os.path.dirname(path), urllib.parse.unquote(file_part)))
                if rel.startswith("..") or not os.path.exists(os.path.join(ROOT, rel)):
                    self.error(loc, target, "broken link target")
                    continue
            if anchor and rel.endswith(".md") and urllib.parse.unquote(anchor) not in anchors(rel):
                self.error(loc, target, "unknown link anchor")
    def run(self):
        root_entries = set(os.listdir(ROOT)) - {".git"}
        self.tracked = tracked_files()
        files, pending = md_files(), []
        for path in files:
            with open(os.path.join(ROOT, path), encoding="utf-8") as h:
                lines = h.read().splitlines()
            docdir = os.path.dirname(path)
            for lineno, line, in_fence in logical_lines(lines):
                loc = "%s:%d" % (path, lineno)
                for m in BACKTICK_RE.finditer(line):
                    rels = candidate_rels(m.group(1).strip(), docdir, root_entries)
                    if rels is not None:
                        pending.append((m.group(1).strip(), loc, rels))
                    self.scan_targets(m.group(1), loc)
                if in_fence:
                    self.scan_targets(line, loc)
                else:
                    self.check_links(path, line, loc)
        ignored = ignored_paths([r for _, _, rels in pending for r in rels for r in (r, r + "/")
                                 if not r.startswith("..")])
        for text, loc, rels in pending:
            self.paths += 1
            if any(os.path.exists(os.path.join(ROOT, r)) for r in rels):
                continue
            if any(r in self.tracked for r in rels):
                self.error(loc, text, "tracked path was deleted")
            elif any(r in ignored or r + "/" in ignored for r in rels):
                continue
            else:
                self.error(loc, text, "unknown repository path")
        if self.errors:
            sys.stderr.write("\n".join(self.errors) + "\n")
            sys.stderr.write("check-docs-accuracy: %d problem(s) in %d markdown file(s)\n"
                             % (len(self.errors), len(files)))
            return 1
        print("check-docs-accuracy: ok (%d files, %d paths, %d links, %d test targets checked)"
              % (len(files), self.paths, self.links, self.targets))
        return 0
if __name__ == "__main__":
    if "--commands" in sys.argv[1:]:
        for row in command_rows():
            print(row)
    else:
        sys.exit(Checker().run())
