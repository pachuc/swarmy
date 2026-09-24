import base64
import json
import struct
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

HELPER = Path(__file__).resolve().parents[1] / "src" / "files.py"


class FileTools(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def call(self, action, args, fallback=False):
        env = dict(os.environ)
        if fallback:
            env["PATH"] = ""
        result = subprocess.run([sys.executable, str(HELPER), action],
                                input=json.dumps(args), text=True, capture_output=True,
                                cwd=self.root, env=env, check=True)
        self.assertEqual(result.stderr, "")
        return json.loads(result.stdout)

    def done(self, action, args, **kwargs):
        result = self.call(action, args, **kwargs)
        self.assertIn("completed", result, result)
        return result["completed"]

    def test_read_png_returns_image_and_dimensions(self):
        payload = b"\x89PNG\r\n\x1a\n" + b"\0" * 8 + struct.pack(">II", 12, 34)
        (self.root / "sample.png").write_bytes(payload)
        result = self.done("read", {"path": "sample.png"})
        self.assertIn("sample.png (12x34)", result["output"])
        self.assertEqual(result["metadata"]["image_media_type"], "image/png")
        self.assertEqual(base64.b64decode(result["metadata"]["image_base64"]), payload)
        (self.root / "huge.png").write_bytes(payload + b"x" * (5 * 1024 * 1024))
        self.assertIn("5 MiB", self.call("read", {"path": "huge.png"})["error"]["error"])

    def test_read_default_offset_limit_and_hint(self):
        (self.root / "text").write_text("\n".join(map(str, range(1, 2003))))
        result = self.done("read", {"path": "text"})
        self.assertTrue(result["output"].startswith("1: 1\n2: 2\n"))
        self.assertIn("2000: 2000\nMore lines remain. Continue with offset=2001.", result["output"])
        self.assertEqual(result["metadata"]["next_offset"], 2001)
        result = self.done("read", {"path": "text", "offset": 2001, "limit": 1})
        self.assertEqual(result["output"], "2001: 2001\nMore lines remain. Continue with offset=2002.")
        result = self.done("read", {"path": "text", "offset": 2002})
        self.assertEqual(result["output"], "2002: 2002")
        self.assertIsNone(result["metadata"]["next_offset"])
        self.assertEqual(self.done("read", {"path": "text", "offset": 3000})["output"], "")

    def test_read_truncates_characters_and_rejects_binary_beyond_limit(self):
        (self.root / "text").write_text("é" * 2001)
        self.assertEqual(self.done("read", {"path": "text"})["output"], "1: " + "é" * 2000 + " [line truncated]")
        (self.root / "text").write_bytes(b"first\nsecond\0")
        self.assertIn("null byte", self.call("read", {"path": "text", "limit": 1})["error"]["error"])

    def test_write_creates_parents_and_preserves_large_literal_content(self):
        content = "$(touch injected) `false` ' \" \\ \n" * 10000
        self.done("write", {"path": "nested/deep/text", "content": content})
        self.assertEqual((self.root / "nested/deep/text").read_text(), content)
        self.assertFalse((self.root / "injected").exists())
        self.done("write", {"path": "nested/deep/text", "content": ""})
        self.assertEqual((self.root / "nested/deep/text").read_text(), "")

    def test_edit_modes_and_diff(self):
        for before, old, mode in [
            ("one\ntwo\n", "two", "exact"),
            ("one\ntwo  \n", "two\n", "ignore trailing whitespace"),
            ("one\n  two  \n", "two\n", "ignore leading and trailing whitespace"),
        ]:
            with self.subTest(mode=mode):
                (self.root / "text").write_text(before)
                result = self.done("edit", {"path": "text", "old_string": old, "new_string": "three\n"})
                self.assertEqual(result["metadata"]["match_mode"], mode)
                self.assertIn(mode, result["output"])
                self.assertIn("--- text\n+++ text\n@@", result["output"])
                self.assertIn("+three\n", result["output"])
                self.assertNotIn("two", (self.root / "text").read_text())

    def test_multiline_fallback_and_preserved_surroundings(self):
        (self.root / "text").write_bytes(b"before\r\n  first \r\n  second\r\nafter\r\n")
        result = self.done("edit", {"path": "text", "old_string": "first\nsecond", "new_string": "replacement"})
        self.assertEqual(result["metadata"]["match_mode"], "ignore leading and trailing whitespace")
        self.assertEqual((self.root / "text").read_bytes(), b"before\r\nreplacement\r\nafter\r\n")

    def test_exact_wins_over_whitespace_matches(self):
        (self.root / "text").write_text("one\n one \n")
        result = self.done("edit", {"path": "text", "old_string": "one\n", "new_string": "two\n"})
        self.assertEqual(result["metadata"]["match_mode"], "exact")
        self.assertEqual((self.root / "text").read_text(), "two\n one \n")

    def test_ambiguity_lists_lines_and_leaves_file_unchanged(self):
        for text, old in [("one\none\n", "one"), ("one \none \n", "one\n"),
                          (" one \n one \n", "one\n")]:
            (self.root / "text").write_text(text)
            result = self.call("edit", {"path": "text", "old_string": old, "new_string": "two"})
            self.assertIn("ambiguous match at lines 1, 2", result["error"]["error"])
            self.assertEqual((self.root / "text").read_text(), text)

    def test_replace_all_exact_only_and_no_other_fuzzy_matching(self):
        (self.root / "text").write_text("one\none\n")
        result = self.done("edit", {"path": "text", "old_string": "one", "new_string": "two", "replace_all": True})
        self.assertEqual(result["metadata"]["replacements"], 2)
        self.assertEqual((self.root / "text").read_text(), "two\ntwo\n")
        for old, all_matches in [(" two ", True), ("t wo", False), ("", False)]:
            self.assertIn("error", self.call("edit", {"path": "text", "old_string": old, "new_string": "three", "replace_all": all_matches}))

    def test_glob_and_grep_caps_with_both_backends(self):
        for number in range(105):
            (self.root / f"{number:03}.txt").write_text("needle\nneedle\n")
        for fallback in [False, True]:
            for action, pattern in [("glob", "*.txt"), ("grep", "^needle$")]:
                with self.subTest(action=action, fallback=fallback):
                    result = self.done(action, {"pattern": pattern}, fallback=fallback)
                    lines = result["output"].splitlines()
                    self.assertEqual(len(lines), 101)
                    self.assertIn("cap of 100 reached", lines[-1])
                    self.assertTrue(result["metadata"]["capped"])
                    expected = "ripgrep" if not fallback and shutil.which("rg") else "python"
                    self.assertEqual(result["metadata"]["backend"], expected)
                    if action == "grep":
                        self.assertTrue(lines[0].endswith("000.txt:1:needle"))

    def test_search_patterns_hidden_files_and_errors(self):
        (self.root / "dir").mkdir()
        (self.root / ".git").mkdir()
        for name in ["dir/nested.txt", "top.txt", ".hidden.txt", ".git/ignored.txt"]:
            (self.root / name).write_text("first\nmatch\n")
        for fallback in [False, True]:
            result = self.done("glob", {"pattern": "**/*.txt"}, fallback=fallback)
            self.assertEqual(set(result["output"].splitlines()), {"dir/nested.txt", "top.txt", ".hidden.txt"})
            self.assertFalse(result["metadata"]["capped"])
            self.assertEqual(self.done("glob", {"pattern": "dir/?.txt"}, fallback=fallback)["output"], "")
            result = self.done("grep", {"path": "top.txt", "pattern": "match"}, fallback=fallback)
            self.assertEqual(result["output"], "top.txt:2:match")
            self.assertIn("error", self.call("grep", {"pattern": "["}, fallback=fallback))
            self.assertIn("error", self.call("glob", {"path": "missing", "pattern": "*"}, fallback=fallback))

    def test_ls_sorted_including_hidden_entries(self):
        (self.root / "dir").mkdir()
        (self.root / ".hidden").touch()
        (self.root / "file").touch()
        self.assertEqual(self.done("ls", {})["output"], ".hidden\ndir/\nfile")
        self.assertIn("error", self.call("ls", {"path": "missing"}))


if __name__ == "__main__":
    unittest.main()
