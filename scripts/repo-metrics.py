#!/usr/bin/env python3
"""Repository size metrics for the cleanup baseline.

Walks every workspace crate under ``crates/`` and reports, per crate:

- lines of non-test source (files under ``src/`` minus ``#[cfg(test)]``
  modules),
- lines of tests (files under ``tests/`` plus inline ``#[cfg(test)]``
  modules and whole files pulled in by ``#[cfg(test)] mod name;``
  declarations),
- the number of ``#[test]`` and ``#[tokio::test]`` functions.

It also reports the number of packages in ``Cargo.lock`` (and how many of
them are ``aws-*``), the ten largest source files, and the internal
dependency edges between crates read from each ``Cargo.toml``.

Only the standard library is used. Run from the repository root::

    python3 scripts/repo-metrics.py
    python3 scripts/repo-metrics.py --json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11 has no tomllib; not expected here.
    tomllib = None  # type: ignore[assignment]

CFG_TEST_RE = re.compile(r"^\s*#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*$")
MOD_INLINE_RE = re.compile(r"(pub(\s*\([^)]*\))?\s+)?mod\s+\w+")
MOD_EXTERNAL_RE = re.compile(r"(pub(\s*\([^)]*\))?\s+)?mod\s+(\w+)\s*;")
PATH_ATTR_RE = re.compile(r"#\s*\[\s*path\s*=\s*\"([^\"]+)\"\s*\]")
# Matches #[test], #[test(...)] and the tokio equivalents. Classified by
# whether the attribute names tokio::test.
TEST_ATTR_RE = re.compile(r"#\s*\[\s*(tokio::\s*test|test)\b")

REPO_ROOT = Path(__file__).resolve().parent.parent


def find_crates(root: Path) -> list[tuple[str, Path]]:
    """Return (name, dir) for each crate dir holding a Cargo.toml, sorted."""
    crates_dir = root / "crates"
    found = []
    for child in sorted(crates_dir.iterdir()):
        manifest = child / "Cargo.toml"
        if child.is_dir() and manifest.is_file():
            found.append((child.name, child))
    return found


def inline_test_spans(lines: list[str]) -> list[tuple[int, int]]:
    """Return (start, end) inclusive line spans of ``#[cfg(test)]`` modules.

    A span starts at the ``#[cfg(test)]`` attribute line and ends at the
    closing brace matching the ``mod name {`` that follows it. Attributes
    that are not followed by a ``mod`` item (for example ``#[cfg(test)]``
    on a helper function) are ignored, so their lines stay source lines.
    """
    spans = []
    i = 0
    n = len(lines)
    while i < n:
        if CFG_TEST_RE.match(lines[i]):
            j = i + 1
            while j < n and lines[j].strip() == "":
                j += 1
            if j < n and MOD_INLINE_RE.match(lines[j].strip()):
                # Find the opening brace of the module body.
                k = j
                while k < n and "{" not in lines[k]:
                    k += 1
                if k < n:
                    depth = 0
                    m = k
                    while m < n:
                        depth += lines[m].count("{") - lines[m].count("}")
                        if depth <= 0:
                            spans.append((i, m))
                            i = m
                            break
                        m += 1
        i += 1
    return spans


def external_test_files(lines: list[str], decl_dir: Path) -> set[Path]:
    """Resolve ``#[cfg(test)] mod name;`` declarations to test-only files.

    A file under ``src/`` that is only reachable through such a declaration
    (``mod tests;`` in ``mod.rs``, ``mod upload_tests;`` in ``device.rs``,
    or ``#[path = "tests.rs"] mod tests;``) is test code even though it is
    not wrapped in an inline ``#[cfg(test)]`` span. Returns resolved paths
    for declarations whose target file exists, including any ``name/``
    sibling directory holding that module's children.
    """
    resolved: set[Path] = set()
    i = 0
    n = len(lines)
    while i < n:
        if CFG_TEST_RE.match(lines[i]):
            j = i + 1
            path_attr = None
            while j < n and lines[j].strip() == "":
                j += 1
            attr_match = PATH_ATTR_RE.search(lines[j]) if j < n else None
            if attr_match:
                path_attr = attr_match.group(1)
                j += 1
                while j < n and lines[j].strip() == "":
                    j += 1
            if j < n:
                mod_match = MOD_EXTERNAL_RE.match(lines[j].strip())
                if mod_match:
                    name = mod_match.group(3)
                    if path_attr is not None:
                        candidates = [decl_dir / path_attr]
                    else:
                        candidates = [
                            decl_dir / f"{name}.rs",
                            decl_dir / name / "mod.rs",
                        ]
                    for candidate in candidates:
                        if candidate.is_file():
                            resolved.add(candidate.resolve())
                    sibling = decl_dir / name
                    if sibling.is_dir():
                        for child in sibling.rglob("*.rs"):
                            resolved.add(child.resolve())
        i += 1
    return resolved


def split_file(path: Path, test_only: bool = False) -> tuple[int, int]:
    """Return (source_lines, inline_test_lines) for one ``.rs`` file."""
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    if test_only:
        return 0, len(lines)
    spans = inline_test_spans(lines)
    test_lines = sum(end - start + 1 for start, end in spans)
    return len(lines) - test_lines, test_lines


def count_test_attrs(path: Path) -> tuple[int, int]:
    """Return (plain_test_count, tokio_test_count) for one ``.rs`` file."""
    text = path.read_text(encoding="utf-8", errors="replace")
    plain = 0
    tokio = 0
    for match in TEST_ATTR_RE.finditer(text):
        if match.group(1).startswith("tokio"):
            tokio += 1
        else:
            plain += 1
    return plain, tokio


def crate_metrics(name: str, crate_dir: Path, root: Path) -> dict:
    """Collect line and test counts for one crate."""
    src_dir = crate_dir / "src"
    tests_dir = crate_dir / "tests"
    src_files = sorted(src_dir.rglob("*.rs")) if src_dir.is_dir() else []
    test_files = sorted(tests_dir.rglob("*.rs")) if tests_dir.is_dir() else []

    # Files under src/ reachable only through #[cfg(test)] mod declarations
    # are test code, not source.
    test_only: set[Path] = set()
    for path in src_files:
        text = path.read_text(encoding="utf-8", errors="replace")
        test_only |= external_test_files(text.splitlines(), path.parent)

    source_lines = 0
    inline_test_lines = 0
    largest: list[dict] = []
    plain_tests = 0
    tokio_tests = 0
    for path in src_files:
        is_test_only = path.resolve() in test_only
        src, inline = split_file(path, test_only=is_test_only)
        source_lines += src
        inline_test_lines += inline
        total = src + inline
        if not is_test_only:
            largest.append(
                {"path": str(path.relative_to(root)), "lines": total}
            )
        plain, tokio = count_test_attrs(path)
        plain_tests += plain
        tokio_tests += tokio

    test_file_lines = 0
    for path in test_files:
        text = path.read_text(encoding="utf-8", errors="replace")
        test_file_lines += len(text.splitlines())
        plain, tokio = count_test_attrs(path)
        plain_tests += plain
        tokio_tests += tokio

    return {
        "name": str(name),
        "src_files": len(src_files),
        "test_files": len(test_files),
        "source_lines": source_lines,
        "test_inline_lines": inline_test_lines,
        "test_files_lines": test_file_lines,
        "test_lines": inline_test_lines + test_file_lines,
        "test_fns": plain_tests,
        "tokio_test_fns": tokio_tests,
        "largest": largest,
    }


def lock_metrics(root: Path) -> tuple[int, int]:
    """Return (package_count, aws_package_count) from Cargo.lock."""
    lock_path = root / "Cargo.lock"
    if tomllib is None:
        raise RuntimeError("tomllib is required to read Cargo.lock")
    with lock_path.open("rb") as handle:
        lock = tomllib.load(handle)
    packages = lock.get("package", [])
    aws = sum(1 for pkg in packages if pkg.get("name", "").startswith("aws-"))
    return len(packages), aws


def _dep_tables(manifest: dict) -> list[dict]:
    """Collect every dependency table in a parsed Cargo.toml."""
    tables = []
    for key in ("dependencies", "dev-dependencies", "build-dependencies"):
        table = manifest.get(key)
        if isinstance(table, dict):
            tables.append(table)
    for target in manifest.get("target", {}).values():
        if isinstance(target, dict):
            for key in ("dependencies", "dev-dependencies", "build-dependencies"):
                table = target.get(key)
                if isinstance(table, dict):
                    tables.append(table)
    return tables


def internal_edges(
    crates: list[tuple[str, Path]], root: Path
) -> list[dict[str, str]]:
    """Return sorted {from, to} edges between workspace crates."""
    names = {name for name, _ in crates}
    edges: set[tuple[str, str]] = set()
    for name, crate_dir in crates:
        if tomllib is None:
            raise RuntimeError("tomllib is required to read Cargo.toml files")
        with (crate_dir / "Cargo.toml").open("rb") as handle:
            manifest = tomllib.load(handle)
        for table in _dep_tables(manifest):
            for dep_name in table:
                if dep_name in names and dep_name != name:
                    edges.add((name, dep_name))
    return [{"from": src, "to": dst} for src, dst in sorted(edges)]


def collect(root: Path) -> dict:
    """Gather every metric into one JSON-serializable object."""
    crates = find_crates(root)
    per_crate = [crate_metrics(name, crate_dir, root) for name, crate_dir in crates]
    lock_packages, aws_packages = lock_metrics(root)
    edges = internal_edges(crates, root)

    largest_all = sorted(
        (entry for crate in per_crate for entry in crate["largest"]),
        key=lambda entry: entry["lines"],
        reverse=True,
    )[:10]

    crate_rows = []
    for crate in per_crate:
        row = {k: v for k, v in crate.items() if k != "largest"}
        crate_rows.append(row)

    return {
        "crates": crate_rows,
        "total_source_lines": sum(c["source_lines"] for c in per_crate),
        "total_test_lines": sum(c["test_lines"] for c in per_crate),
        "total_test_fns": sum(c["test_fns"] for c in per_crate),
        "total_tokio_test_fns": sum(c["tokio_test_fns"] for c in per_crate),
        "lock_packages": lock_packages,
        "aws_packages": aws_packages,
        "largest_files": largest_all,
        "edges": edges,
    }


def render_markdown(data: dict) -> str:
    """Render the metrics object as Markdown tables."""
    out = ["## Lines and tests per crate", ""]
    out.append(
        "| Crate | Source lines | Test lines (inline + files) "
        "| `#[test]` | `#[tokio::test]` |"
    )
    out.append("| --- | ---: | ---: | ---: | ---: |")
    for crate in data["crates"]:
        out.append(
            f"| {crate['name']} | {crate['source_lines']} "
            f"| {crate['test_lines']} ({crate['test_inline_lines']} + "
            f"{crate['test_files_lines']}) "
            f"| {crate['test_fns']} | {crate['tokio_test_fns']} |"
        )
    out.append(
        f"| **Total** | **{data['total_source_lines']}** "
        f"| **{data['total_test_lines']}** "
        f"| **{data['total_test_fns']}** "
        f"| **{data['total_tokio_test_fns']}** |"
    )
    out += ["", "## Dependencies", ""]
    out.append(
        f"Cargo.lock has {data['lock_packages']} packages, "
        f"{data['aws_packages']} of them `aws-*`."
    )
    out += ["", "## Ten largest source files", ""]
    out.append("| File | Lines |")
    out.append("| --- | ---: |")
    for entry in data["largest_files"]:
        out.append(f"| `{entry['path']}` | {entry['lines']} |")
    out += ["", "## Internal dependency edges", ""]
    out.append("| From | To |")
    out.append("| --- | --- |")
    for edge in data["edges"]:
        out.append(f"| {edge['from']} | {edge['to']} |")
    out.append("")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``repo-metrics.py`` script."""
    parser = argparse.ArgumentParser(description="Print repository size metrics.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the same numbers as one JSON object.",
    )
    parser.add_argument(
        "--root",
        default=str(REPO_ROOT),
        help="Repository root (defaults to the checkout holding this script).",
    )
    args = parser.parse_args(argv)
    data = collect(Path(args.root))
    if args.json:
        json.dump(data, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_markdown(data))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
