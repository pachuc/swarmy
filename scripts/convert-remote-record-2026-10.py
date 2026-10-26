#!/usr/bin/env python3
"""Convert an old remote JSON record into a persistent existing-host swarm.

Reads the API token over SSH, never prints secrets, and refuses to overwrite.
Undo with `swarmy swarm rm NAME --force`: it forgets the spec, record and state
without touching the server.
"""

import argparse
import datetime
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib


NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def require(condition, field, message="invalid or missing"):
    if not condition:
        raise ValueError(f"{field}: {message}")


def directory(variable, fallback):
    value = os.environ.get(variable, "")
    if value and Path(value).is_absolute():
        return Path(value) / "swarmy"
    home = os.environ.get("HOME", "")
    require(home and Path(home).is_absolute(), variable,
            "set an absolute XDG directory or HOME")
    return Path(home) / fallback / "swarmy"


def ipv4(value, field):
    try:
        require(isinstance(value, str) and ipaddress.ip_address(value).version == 4,
                field, "must be IPv4")
    except ValueError as error:
        raise ValueError(f"{field}: must be IPv4") from None


def key_for(value, old_dir, field):
    require(isinstance(value, str) and value, field)
    path = Path(value)
    if not (path.is_absolute() and path.is_file()):
        path = old_dir / path.name
    require(path.is_file(), field, f"key file missing at {path}")
    known = path.with_suffix(".known_hosts")
    require(known.is_file(), field, f"known hosts missing at {known}")
    return path, known


def host(value, old_dir, swarm_dir, group, index, check_key=True):
    require(isinstance(value, dict), f"{group}[{index}]")
    for field in ("public_ip", "ssh_user", "key_path"):
        require(field in value, f"{group}[{index}].{field}")
    ipv4(value["public_ip"], f"{group}[{index}].public_ip")
    private = value.get("private_ip") or ""
    if private:
        ipv4(private, f"{group}[{index}].private_ip")
    require(isinstance(value["ssh_user"], str) and NAME.fullmatch(value["ssh_user"]),
            f"{group}[{index}].ssh_user")
    key, known = (key_for(value["key_path"], old_dir, f"{group}[{index}].key_path")
                  if check_key else (None, None))
    name = f"{swarm_dir.name}-{group}-{index + 1}"
    address = value["public_ip"]
    private = private if private and private != address else None
    target = swarm_dir / "keys" / name
    # Plan checks this copy on each up; adopt_key reads it before writing it.
    # The spec can safely point at the record's copy instead of the old key.
    spec = dict(address=address, ssh_user=value["ssh_user"], ssh_key=str(target))
    record = dict(name=name, public_address=address, ssh_user=value["ssh_user"],
                  ssh_key=str(target), provisioned=True)
    if private:
        spec["private_address"] = private
        record["private_address"] = private
    return spec, record, (key, known, target)


def group(sandboxes, storage, hosts):
    require(isinstance(sandboxes, int) and not isinstance(sandboxes, bool)
            and sandboxes >= 0, "sandboxes")
    require(not sandboxes or isinstance(storage, str) and bool(storage),
            "local_storage", "required when sandboxes > 0")
    result = {"module": "existing", "sandboxes": sandboxes}
    if storage:
        result["local_storage"] = storage
    result["hosts"] = hosts
    return result


def convert(old_path):
    try:
        old = json.loads(old_path.read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"{old_path}: cannot parse JSON ({type(error).__name__})") from None
    require(isinstance(old, dict), str(old_path))
    for field in ("name", "public_ip", "ssh_user", "key_path", "launch_settings"):
        if field == "launch_settings" and not old.get(field):
            raise ValueError("launch_settings: record predates launch settings; convert by hand")
        require(field in old, field)
    name = old["name"]
    require(isinstance(name, str) and NAME.fullmatch(name), "name")
    settings = old["launch_settings"]
    require(isinstance(settings, dict), "launch_settings")
    require(settings.get("provider") == "existing", "launch_settings.provider",
            "only existing hosts can be converted")
    require(settings.get("services") == "node", "launch_settings.services",
            "laptop services cannot be converted")
    nodes = old.get("nodes", [])
    require(isinstance(nodes, list), "nodes")
    host(old, old_path.parent, Path(name), "main", 0, check_key=False)
    for index, node in enumerate(nodes):
        host(node, old_path.parent, Path(name), "nodes", index, check_key=False)
    service_user = settings.get("service_user") or old["ssh_user"]
    require(isinstance(service_user, str) and NAME.fullmatch(service_user),
            "launch_settings.service_user")
    bucket = settings.get("bucket")
    bucket_spec = None
    keys = None
    if bucket is not None:
        if not isinstance(bucket, dict) or not isinstance(bucket.get("credentials"), dict) or bucket["credentials"].get("source") != "static_keys":
            raise ValueError("launch_settings.bucket: AWS instance-role buckets cannot be converted")
        credentials = bucket["credentials"]
        for field in ("access_key", "secret_key"):
            require(isinstance(credentials.get(field), str) and bool(credentials[field]),
                    f"launch_settings.bucket.credentials.{field}")
        endpoint = bucket.get("endpoint")
        require(isinstance(endpoint, str) and endpoint.startswith(("http://", "https://")),
                "launch_settings.bucket.endpoint")
        region = bucket.get("region") or old.get("region")
        require(isinstance(region, str) and bool(region), "launch_settings.bucket.region")
        for field in ("bucket", "prefix"):
            require(isinstance(bucket.get(field), str), f"launch_settings.bucket.{field}")
        bucket_spec = dict(name=bucket["bucket"], endpoint=endpoint, region=region,
                           prefix=bucket["prefix"],
                           conditional_create=bucket.get("conditional_create", True))
        require(isinstance(bucket_spec["conditional_create"], bool),
                "launch_settings.bucket.conditional_create")
        keys = dict(access_key=credentials["access_key"], secret_key=credentials["secret_key"])
    primary_sandboxes = old.get("sandboxes", 64)
    primary_storage = settings.get("local_storage") or ""
    group(primary_sandboxes, primary_storage, [])
    node_settings = []
    for index, node in enumerate(nodes):
        launch = node.get("launch_settings") or settings
        require(isinstance(launch, dict), f"nodes[{index}].launch_settings")
        require((launch.get("service_user") or service_user) == service_user,
                f"nodes[{index}].launch_settings.service_user",
                "differs from the primary's; a swarm has one service user")
        values = (node.get("sandboxes", primary_sandboxes), launch.get("local_storage") or "")
        group(*values, [])
        node_settings.append(values)
    if node_settings:
        require(all(item == node_settings[0] for item in node_settings), "nodes",
                "joined nodes disagree on sandboxes or local_storage")
    config = directory("XDG_CONFIG_HOME", ".config")
    data = directory("XDG_DATA_HOME", ".local/share")
    spec_path = config / "swarms" / f"{name}.toml"
    swarm_dir = data / "swarms" / name
    main, main_record, main_copy = host(old, old_path.parent, swarm_dir, "main", 0)
    groups = {"main": group(primary_sandboxes, primary_storage, [main])}
    hosts = {"main": [main_record]}
    copies = [main_copy]
    if nodes:
        node_hosts = []
        node_records = []
        for index, node in enumerate(nodes):
            entry, recorded, copy = host(node, old_path.parent, swarm_dir, "nodes", index)
            node_hosts.append(entry)
            node_records.append(recorded)
            copies.append(copy)
        groups["nodes"] = group(*node_settings[0], node_hosts)
        hosts["nodes"] = node_records
    # Bucket ownership is checked against spec.name; changing the name breaks up.
    spec = dict(name=name, lifecycle="persistent", service_user=service_user,
                groups=groups, placement=dict(stack="main", services="main",
                                              nodes=list(groups)))
    if bucket_spec:
        spec["bucket"] = bucket_spec
    existing = [path for path in (spec_path, swarm_dir) if os.path.lexists(path)]
    if existing:
        raise ValueError(f"swarm {name} already exists ({', '.join(map(str, existing))}); nothing was written. To convert again, run swarmy swarm rm {name} --force first")
    return spec, hosts, keys, copies, spec_path, swarm_dir


def scalar(value):
    if isinstance(value, str):
        # JSON surrogate-pair escapes are not valid TOML basic strings.
        return '"' + "".join(
            "\\" + c if c in '\"\\'
            else f"\\u{ord(c):04x}" if ord(c) < 0x20 or ord(c) == 0x7F
            else c
            for c in value) + '"'
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(map(scalar, value)) + "]"
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{key} = {scalar(item)}" for key, item in value.items()) + " }"
    raise ValueError("unsupported TOML value")


def fields(items):
    return "".join(f"{key} = {scalar(value)}\n" for key, value in items.items())


def spec_toml(spec):
    text = fields({key: spec[key] for key in ("name", "lifecycle", "service_user")})
    if "bucket" in spec:
        text += "\n[bucket]\n" + fields(spec["bucket"])
    for name, entry in spec["groups"].items():
        text += f"\n[groups.{name}]\n" + fields({k: v for k, v in entry.items() if k != "hosts"})
        text += "hosts = " + scalar(entry["hosts"]) + "\n"
    return text + "\n[placement]\n" + fields(spec["placement"])


def record_toml(name, token, keys, hosts):
    text = fields(dict(name=name, created_at=datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")))
    text += "\n[api]\n" + fields(dict(token=token))
    if keys:
        text += "\n[bucket_keys]\n" + fields(keys)
    for group_name, entries in hosts.items():
        for entry in entries:
            text += f"\n[[hosts.{group_name}]]\n" + fields(entry)
    return text


def write_file(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as output:
        os.fchmod(output.fileno(), 0o600)
        output.write(content)
        output.flush()
        os.fsync(output.fileno())


def private_dir(path):
    if path.exists():
        require(path.is_dir(), str(path), "not a directory")
    else:
        os.mkdir(path, mode=0o700)
        os.chmod(path, 0o700)


def token_from_node(spec, copies):
    old_key, known, _ = copies[0]
    primary = spec["groups"]["main"]["hosts"][0]
    command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o",
               "StrictHostKeyChecking=yes", "-o", "IdentitiesOnly=yes", "-o",
               f"UserKnownHostsFile={known}", "-i", str(old_key), "-l",
               primary["ssh_user"], primary["address"],
               f"cat ~{spec['service_user']}/swarmy/.swarmy/config.toml"]
    result = subprocess.run(command, capture_output=True, stdin=subprocess.DEVNULL,
                            timeout=60, check=False)
    if result.returncode:
        raise ValueError("ssh: " + (result.stderr.decode(errors="replace").splitlines() or ["failed"])[-1])
    try:
        token = tomllib.loads(result.stdout.decode()).get("api", {}).get("token")
    except (ValueError, UnicodeError, AttributeError):
        token = None
    require(isinstance(token, str) and bool(token), "node configuration",
            "the node configuration has no [api] token")
    return token


def publish(spec, hosts, keys, copies, spec_path, swarm_dir, token):
    temp_dir = swarm_dir.parent / f".{swarm_dir.name}.convert-{os.getpid()}"
    temp_spec = spec_path.parent / f".{swarm_dir.name}.toml.tmp-{os.getpid()}"
    renamed = False
    for root in (spec_path.parent.parent, swarm_dir.parent.parent):
        missing = []
        parent = root
        while not parent.exists():
            missing.append(parent)
            parent = parent.parent
        for path in reversed(missing):
            private_dir(path)
        private_dir(root)
        private_dir(root / "swarms")
    try:
        private_dir(temp_dir)
        private_dir(temp_dir / "keys")
        for old_key, known, target in copies:
            pub = old_key.with_name(old_key.name + ".pub")
            if pub.exists():
                public = pub.read_bytes()
            else:
                result = subprocess.run(["ssh-keygen", "-y", "-f", str(old_key)],
                                        capture_output=True, check=False)
                require(result.returncode == 0, str(pub), "ssh-keygen could not derive public key")
                public = result.stdout
            for source, suffix in ((old_key, ""), (known, ".known_hosts")):
                write_file(temp_dir / "keys" / (target.name + suffix), source.read_bytes())
            write_file(temp_dir / "keys" / (target.name + ".pub"), public)
        write_file(temp_dir / "record.toml", record_toml(spec["name"], token, keys, hosts).encode())
        write_file(temp_spec, spec_toml(spec).encode())
        os.rename(temp_dir, swarm_dir)
        renamed = True
        os.rename(temp_spec, spec_path)
    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        temp_spec.unlink(missing_ok=True)
        if renamed:
            shutil.rmtree(swarm_dir, ignore_errors=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-record", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        spec, hosts, keys, copies, spec_path, swarm_dir = convert(args.old_record)
        try:
            tomllib.loads(spec_toml(spec))
            tomllib.loads(record_toml(spec["name"], "-", keys, hosts))
        except ValueError:
            raise ValueError("the converted spec or record does not render as TOML; convert by hand") from None
        if args.dry_run:
            print(f"# spec: {spec_path}\n{spec_toml(spec)}")
            redacted = {key: "<redacted>" for key in keys} if keys else None
            print(f"# record: {swarm_dir / 'record.toml'}\n" +
                  record_toml(spec["name"], "<read from the node when converting>",
                              redacted, hosts))
            for source, _, target in copies:
                print(f"copy {source} -> {target} (+ .pub, .known_hosts)")
            return 0
        token = token_from_node(spec, copies)
        publish(spec, hosts, keys, copies, spec_path, swarm_dir, token)
        print(f"wrote {spec_path}\nwrote {swarm_dir / 'record.toml'}")
        for _, _, target in copies:
            print(f"copied key {target}")
        for command in (f"swarmy swarm show {spec['name']}",
                        f"swarmy --swarm {spec['name']} doctor",
                        f"swarmy swarm use {spec['name']}"):
            print(f"next: {command}")
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(f"convert: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
