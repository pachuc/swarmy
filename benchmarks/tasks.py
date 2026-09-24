#!/usr/bin/env python3
"""Read the deliberately small, dependency-free benchmark front matter format."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
TASKS = ("small", "medium", "large")
PIN = "8c670aa1b9926cd23badb8a056d2ac68751ad520"
FIELDS = {"name", "scope", "commit", "prompt", "pass_criterion", "expected_duration_band"}


def load(name):
    if name not in TASKS:
        raise ValueError(f"unknown benchmark task: {name}")
    text = (ROOT / "tasks" / f"{name}.md").read_text()
    lines = text.splitlines()
    if not lines or lines[0] != "---":
        raise ValueError(f"{name}: missing front matter")
    values = {}
    index = 1
    while index < len(lines) and lines[index] != "---":
        key, sep, value = lines[index].partition(": ")
        if not sep or key not in FIELDS or key in values:
            raise ValueError(f"{name}: invalid front matter line: {lines[index]}")
        if value == "|":
            index += 1
            block = []
            while index < len(lines) and lines[index].startswith("  "):
                block.append(lines[index][2:])
                index += 1
            values[key] = "\n".join(block).strip() + "\n"
            continue
        values[key] = value
        index += 1
    if index == len(lines) or set(values) != FIELDS:
        raise ValueError(f"{name}: missing front matter fields or closing delimiter")
    if values["scope"] != name or values["commit"] != PIN:
        raise ValueError(f"{name}: scope or pinned commit differs")
    if not re.fullmatch(r"[a-z][a-z0-9-]+", values["name"]):
        raise ValueError(f"{name}: invalid name")
    if not all(values.values()):
        raise ValueError(f"{name}: empty field")
    return values


if __name__ == "__main__":
    for task in TASKS:
        print(task, load(task)["name"])
