#!/usr/bin/env python3
"""Unit test for scripts/repo-metrics.py against a small fixture tree."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "repo-metrics.py"

ALPHA_LIB = """\
fn add(a: u32, b: u32) -> u32 {
    a + b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds() {
        assert_eq!(add(1, 2), 3);
    }

    #[tokio::test]
    async fn adds_async() {
        assert_eq!(add(1, 2), 3);
    }
}
"""

ALPHA_EXTRA = """\
pub fn hello() -> &'static str {
    "hello"
}
"""

ALPHA_LOCK = """\
[[package]]
name = "alpha"
version = "0.1.0"

[[package]]
name = "aws-smithy-types"
version = "1.0.0"

[[package]]
name = "serde"
version = "1.0.0"
"""


def write(path: Path, content: str) -> None:
    """Write fixture content, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def make_fixture(root: Path) -> None:
    """Build a two-crate fixture tree with known line counts."""
    write(
        root / "crates" / "alpha" / "Cargo.toml",
        "[package]\nname = \"alpha\"\nversion = \"0.1.0\"\n"
        "\n[dependencies]\nbeta = { path = \"../beta\" }\n",
    )
    write(root / "crates" / "alpha" / "src" / "lib.rs", ALPHA_LIB)
    write(root / "crates" / "alpha" / "src" / "extra.rs", ALPHA_EXTRA)
    write(
        root / "crates" / "alpha" / "tests" / "integration.rs",
        "#[test]\nfn works() {}\n",
    )
    write(root / "crates" / "beta" / "Cargo.toml",
        "[package]\nname = \"beta\"\nversion = \"0.1.0\"\n",
    )
    write(
        root / "crates" / "beta" / "src" / "lib.rs",
        "pub fn x() {}\n#[cfg(test)]\nmod tests;\n",
    )
    write(
        root / "crates" / "beta" / "src" / "tests.rs",
        "#[test]\nfn x_works() {}\n",
    )
    write(root / "Cargo.lock", ALPHA_LOCK)


class RepoMetricsTest(unittest.TestCase):
    """Check the metrics script against the fixture tree."""

    def test_json_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_fixture(root)
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), "--json", "--root", str(root)],
                capture_output=True,
                text=True,
                check=True,
            )
            data = json.loads(proc.stdout)

            alpha = next(c for c in data["crates"] if c["name"] == "alpha")
            beta = next(c for c in data["crates"] if c["name"] == "beta")

            # ALPHA_LIB has 18 lines; the #[cfg(test)] span covers the
            # attribute line through the module's closing brace (14 lines).
            self.assertEqual(alpha["source_lines"], 4 + 3)
            self.assertEqual(alpha["test_inline_lines"], 14)
            self.assertEqual(alpha["test_files_lines"], 2)
            self.assertEqual(alpha["test_lines"], 16)
            # Two plain #[test] (one inline, one integration) and one tokio.
            self.assertEqual(alpha["test_fns"], 2)
            self.assertEqual(alpha["tokio_test_fns"], 1)

            # The two declaration lines stay source; the tests.rs file moves
            # to tests wholesale.
            self.assertEqual(beta["source_lines"], 3)
            self.assertEqual(beta["test_lines"], 2)
            self.assertEqual(beta["test_fns"], 1)

            self.assertEqual(data["lock_packages"], 3)
            self.assertEqual(data["aws_packages"], 1)
            self.assertEqual(data["edges"], [{"from": "alpha", "to": "beta"}])

            largest = data["largest_files"]
            self.assertEqual(largest[0]["path"], "crates/alpha/src/lib.rs")
            self.assertEqual(largest[0]["lines"], 18)
            # Test-only files reached through #[cfg(test)] mod declarations
            # are not source files.
            self.assertNotIn(
                "crates/beta/src/tests.rs", [e["path"] for e in largest]
            )

    def test_markdown_matches_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_fixture(root)
            json_proc = subprocess.run(
                [sys.executable, str(SCRIPT), "--json", "--root", str(root)],
                capture_output=True,
                text=True,
                check=True,
            )
            md_proc = subprocess.run(
                [sys.executable, str(SCRIPT), "--root", str(root)],
                capture_output=True,
                text=True,
                check=True,
            )
            data = json.loads(json_proc.stdout)
            table = md_proc.stdout
            for crate in data["crates"]:
                self.assertIn(f"| {crate['name']} | {crate['source_lines']} ", table)
            self.assertIn(str(data["lock_packages"]), table)
            for entry in data["largest_files"]:
                self.assertIn(entry["path"], table)


if __name__ == "__main__":
    unittest.main()
