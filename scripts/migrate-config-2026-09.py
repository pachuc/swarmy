#!/usr/bin/env python3
"""Rewrite an old flat swarmy config.toml into the new grouped layout.

Old files (master) keep every setting as a top-level key (`fdb_cluster_file`,
`provider`, `bus_prefix`, ...) with duration keys like `period_seconds` or
`bus_ack_wait_ms`. New files group them (`[store] cluster_file`,
`[selection] provider`, `[bus] ack_wait_ms`, ...) and carry the unit in the
key (`*_secs` / `*_ms`).

Usage:
    scripts/migrate-config-2026-09.py <config.toml> [--print-breaks] [--check]

- Rewrites the file in place, keeping a `.bak` copy of the original.
- Idempotent: a file already in the new layout is left unchanged (no `.bak`).
- Unknown keys are kept, not dropped, and reported to stderr.
- `--print-breaks` prints the old-to-new key list (the source for
  docs/api-breaks.txt) and exits without touching any file.
- `--check` exits 0 when the file is already migrated, 1 when it needs
  migration, without writing.

The MOVES table below is the single source of truth for the rename list.
"""

import copy
import os
import re
import shutil
import stat
import sys
import tempfile
import tomllib
from pathlib import Path

# Old dotted path -> new dotted path. Generated from master's flat Settings
# versus the grouped Settings; docs/api-breaks.txt is printed from this table
# via --print-breaks so the two cannot disagree.
MOVES = [
    ("fdb_cluster_file", "store.cluster_file"),
    ("store_directory", "store.directory"),
    ("nats_url", "bus.nats_url"),
    ("bus_prefix", "bus.prefix"),
    ("bus_ack_wait_ms", "bus.ack_wait_ms"),
    ("bus_max_deliver", "bus.max_deliver"),
    ("s3_endpoint", "s3.endpoint"),
    ("s3_access_key", "s3.access_key"),
    ("s3_secret_key", "s3.secret_key"),
    ("s3_bucket", "s3.bucket"),
    ("s3_prefix", "s3.prefix"),
    ("s3_region", "s3.region"),
    ("provider", "selection.provider"),
    ("providers", "selection.providers"),
    ("custom_providers", "selection.custom_providers"),
    ("models", "selection.models"),
    ("model", "selection.model"),
    ("default_image", "selection.default_image"),
    ("reasoning_effort", "selection.effort"),
    ("credential_file", "selection.credential_file"),
    ("system_prompt", "context.system_prompt"),
    ("summarize_at_tokens", "context.summarize_at"),
    ("model_context_window_tokens", "context.context_window"),
    ("memory_dir", "memory.dir"),
    ("memory_max_bytes", "memory.max_bytes"),
    ("worker_partitions", "worker.partitions"),
    ("scheduler_partitions", "scheduler.partitions"),
    ("scheduler_scan_interval_ms", "scheduler.scan_interval_ms"),
    ("scheduler_resend_interval_ms", "scheduler.resend_interval_ms"),
    ("worker_lease_ms", "worker.lease_ms"),
    ("worker_recovery_interval_ms", "worker.recovery_interval_ms"),
    ("gateway_concurrency", "gateway.concurrency"),
    ("worker_kill_point", "worker.kill_point"),
    ("image_upload_max_bytes", "image.upload_max_bytes"),
    ("ephemeral_retention_seconds", "scheduler.ephemeral_retention_secs"),
    ("placement_lease_seconds", "scheduler.placement_lease_secs"),
    ("sandbox_idle_seconds", "sandbox.idle_secs"),
    ("node_id", "node.id"),
    ("node_roles", "node.roles"),
    ("node_capacity", "node.capacity"),
    ("node_memory_reserve_mib", "node.memory_reserve_mib"),
    ("node_heartbeat_interval_ms", "node.heartbeat_interval_ms"),
    ("volume_snapshots.period_seconds", "volume_snapshots.period_secs"),
    ("gc.grace_seconds", "gc.grace_secs"),
    ("gc.interval_seconds", "gc.interval_secs"),
    ("inference.max_wait_seconds", "inference.max_wait_secs"),
    ("inference.max_backoff_seconds", "inference.max_backoff_secs"),
    ("inference.gateway_wait_seconds", "inference.gateway_wait_secs"),
]

# Top-level tables in the new layout. Anything else at top level is unknown:
# it is kept and reported, never dropped.
KNOWN_TOP = {
    "api",
    "state_dir",
    "remote",
    "volume_snapshots",
    "sandbox",
    "gc",
    "inference",
    "metering",
    "node",
    "store",
    "s3",
    "bus",
    "selection",
    "scheduler",
    "worker",
    "gateway",
    "context",
    "memory",
    "fake",
    "image",
}

# Fixed leaf paths in the new layout. Anything under a known top-level table
# but not listed here (and not under a dynamic prefix below) is an unknown
# nested key: kept and reported, never dropped.
KNOWN_LEAVES = {
    "state_dir",
    "api.listen",
    "api.url",
    "api.token",
    "remote.provider",
    "remote.services",
    "remote.region",
    "remote.bucket",
    "remote.disk_gb",
    "remote.managed_by_tag",
    "remote.profile",
    "remote.aws.subnet",
    "remote.aws.security_group",
    "remote.aws.instance_type",
    "remote.aws.image",
    "remote.aws.iam_role",
    "volume_snapshots.period_secs",
    "volume_snapshots.retention",
    "sandbox.idle_secs",
    "sandbox.scratch_idle_days",
    "sandbox.scratch_high_water",
    "sandbox.scratch_low_water",
    "gc.grace_secs",
    "gc.interval_secs",
    "gc.filter_bytes",
    "gc.batch_size",
    "gc.delete_concurrency",
    "inference.max_wait_secs",
    "inference.max_backoff_secs",
    "inference.gateway_wait_secs",
    "inference.default_route",
    "metering.raw_retention_days",
    "node.id",
    "node.roles",
    "node.capacity.cpu_millis",
    "node.capacity.memory_bytes",
    "node.capacity.disk_bytes",
    "node.capacity.sandboxes",
    "node.memory_reserve_mib",
    "node.heartbeat_interval_ms",
    "store.cluster_file",
    "store.directory",
    "s3.endpoint",
    "s3.access_key",
    "s3.secret_key",
    "s3.bucket",
    "s3.prefix",
    "s3.region",
    "bus.nats_url",
    "bus.prefix",
    "bus.ack_wait_ms",
    "bus.max_deliver",
    "selection.provider",
    "selection.providers",
    "selection.model",
    "selection.effort",
    "selection.default_image",
    "selection.credential_file",
    "scheduler.partitions",
    "scheduler.scan_interval_ms",
    "scheduler.resend_interval_ms",
    "scheduler.ephemeral_retention_secs",
    "scheduler.placement_lease_secs",
    "worker.partitions",
    "worker.lease_ms",
    "worker.recovery_interval_ms",
    "worker.kill_point",
    "gateway.concurrency",
    "context.summarize_at",
    "context.context_window",
    "context.system_prompt",
    "memory.dir",
    "memory.max_bytes",
    "fake.script",
    "fake.call_log",
    "image.upload_max_bytes",
}

# Dynamic subtrees whose keys are user data, not config keys.
KNOWN_PREFIXES = (
    "selection.custom_providers.",
    "selection.models",
)


def _get(data, dotted):
    parts = dotted.split(".")
    node = data
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            return None, False
        node = node[part]
    return node, True


def _pop(data, dotted):
    parts = dotted.split(".")
    node = data
    for part in parts[:-1]:
        if not isinstance(node, dict) or part not in node:
            return None, False
        node = node[part]
    if not isinstance(node, dict) or parts[-1] not in node:
        return None, False
    value = node.pop(parts[-1])
    # Prune tables left empty by the move (for example [node_capacity]).
    parent = data
    chain = []
    for part in parts[:-1]:
        chain.append((parent, part))
        parent = parent.get(part, {})
    for parent, part in reversed(chain):
        child = parent.get(part)
        if isinstance(child, dict) and not child:
            del parent[part]
        else:
            break
    return value, True


def _set(data, dotted, value):
    parts = dotted.split(".")
    node = data
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[parts[-1]] = value


def needs_migration(data):
    for old, _new in MOVES:
        _, found = _get(data, old)
        if found:
            return True
    return False


def _walk_unknown(node, prefix, out):
    if isinstance(node, dict):
        for key, value in node.items():
            dotted = f"{prefix}.{key}" if prefix else key
            if dotted.startswith(KNOWN_PREFIXES):
                continue
            if isinstance(value, dict):
                _walk_unknown(value, dotted, out)
            elif isinstance(value, list) and value and all(
                isinstance(item, dict) for item in value
            ):
                for item in value:
                    _walk_unknown(item, dotted, out)
            elif dotted not in KNOWN_LEAVES:
                out.append(dotted)
    elif isinstance(node, list):
        for item in node:
            _walk_unknown(item, prefix, out)


def find_unknown(data):
    """Top-level unknown keys plus unknown leaves inside known tables."""
    unknown = []
    for key, value in data.items():
        if key not in KNOWN_TOP:
            unknown.append(key)
        else:
            _walk_unknown(value, key, unknown)
    return unknown


def migrate(data):
    data = copy.deepcopy(data)
    moved = []
    for old, new in MOVES:
        value, found = _pop(data, old)
        if found:
            _set(data, new, value)
            moved.append((old, new))
    return data, moved, find_unknown(data)


_BARE = re.compile(r"^[A-Za-z0-9_-]+$")


def _escape(value):
    out = []
    for char in value:
        code = ord(char)
        if char == "\\":
            out.append("\\\\")
        elif char == '"':
            out.append('\\"')
        elif char == "\n":
            out.append("\\n")
        elif char == "\r":
            out.append("\\r")
        elif char == "\t":
            out.append("\\t")
        elif char == "\u0008":
            out.append("\\b")
        elif char == "\u000c":
            out.append("\\f")
        elif code < 0x20 or code == 0x7F:
            out.append(f"\\u{code:04X}")
        else:
            out.append(char)
    return "".join(out)


def _quote_key(key):
    if _BARE.match(key):
        return key
    return f'"{_escape(key)}"'


def _join(path, key):
    quoted = _quote_key(key)
    return f"{path}.{quoted}" if path else quoted


def _format_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        return f'"{_escape(value)}"'
    if isinstance(value, list):
        return "[" + ", ".join(_format_value(item) for item in value) + "]"
    if isinstance(value, dict):
        parts = ", ".join(
            f"{_quote_key(key)} = {_format_value(item)}" for key, item in value.items()
        )
        return "{ " + parts + " }"
    raise TypeError(f"unsupported TOML value: {value!r}")


def _emit_table(lines, path, table):
    scalars = [(k, v) for k, v in table.items() if not isinstance(v, dict)]
    plain_subtables = [(k, v) for k, v in table.items() if isinstance(v, dict)]
    array_tables = []
    # Lists of tables are emitted as [[path]] blocks; other lists stay inline.
    for key, value in scalars:
        if isinstance(value, list) and value and all(
            isinstance(item, dict) for item in value
        ):
            array_tables.append((key, value))
        else:
            lines.append(f"{_quote_key(key)} = {_format_value(value)}")
    for key, value in plain_subtables:
        dotted = _join(path, key)
        lines.append(f"[{dotted}]")
        _emit_table(lines, dotted, value)
    for key, value in array_tables:
        dotted = _join(path, key)
        for item in value:
            lines.append(f"[[{dotted}]]")
            _emit_table(lines, dotted, item)


def dump(data):
    lines = []
    _emit_table(lines, "", data)
    return "\n".join(lines) + "\n"


def print_breaks():
    for old, new in MOVES:
        print(f"{old} -> {new}")


def main(argv):
    if "--print-breaks" in argv:
        print_breaks()
        return 0
    args = [arg for arg in argv[1:] if not arg.startswith("--")]
    check_only = "--check" in argv
    if not args:
        print("usage: migrate-config-2026-09.py <config.toml> [--check]", file=sys.stderr)
        return 2
    path = Path(args[0])
    try:
        data = tomllib.loads(path.read_text())
    except Exception as error:
        print(f"cannot parse {path}: {error}", file=sys.stderr)
        return 1
    if not needs_migration(data):
        print(f"{path}: already in the new layout")
        return 0
    if check_only:
        print(f"{path}: needs migration")
        return 1
    migrated, moved, unknown = migrate(data)
    text = dump(migrated)
    try:
        reparsed = tomllib.loads(text)
    except Exception as error:
        print(f"refusing to write corrupt output: {error}", file=sys.stderr)
        return 1
    if reparsed != migrated:
        print("refusing to write corrupt output: round trip mismatch", file=sys.stderr)
        return 1
    backup = path.with_suffix(path.suffix + ".bak")
    mode = stat.S_IMODE(os.stat(path).st_mode)
    shutil.copy2(path, backup)
    os.chmod(backup, mode)
    # Atomic write: a temp file in the same directory keeps the original mode,
    # then replaces the config so a crash never leaves a half-written file.
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=path.name + ".", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w") as tmp:
            tmp.write(text)
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    for old, new in moved:
        print(f"moved {old} -> {new}")
    for key in unknown:
        print(f"kept unknown key: {key}", file=sys.stderr)
    print(f"wrote {path} (backup at {backup})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
