#!/usr/bin/env python3
"""Preflight, run, and audit disposable cloud benchmarks. See volume-benchmarks.md."""

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid


sys.modules.setdefault("cloud", sys.modules[__name__])


class AuditError(Exception):
    """Only fixed, credential-free diagnostics may be placed in this exception."""


class ApiError(AuditError):
    def __init__(self, status, reason):
        self.status = status
        self.reason = reason
        super().__init__(f"GCS request failed (HTTP {status})")


def command(args, *, env=None, timeout=600):
    # Never relay cloud CLI errors: they can contain credentials or other resources.
    try:
        result = subprocess.run(args, capture_output=True, text=True, env=env,
                                timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise AuditError("command unavailable or timed out") from error
    if result.returncode:
        raise AuditError("command failed; check the documented permissions")
    return result.stdout


def aws(service, operation, **kwargs):
    args = ["aws", service, operation, "--output", "json", "--no-cli-pager"]
    for key, value in kwargs.items():
        args.append("--" + key.replace("_", "-"))
        if value is not True:
            args.append(str(value))
    try:
        return json.loads(command(args) or "{}")
    except json.JSONDecodeError as error:
        raise AuditError("AWS returned invalid JSON") from error


def gcs(method, path, *, query=None, body=None, media=False):
    token = command(["gcloud", "auth", "print-access-token"]).strip()
    url = "https://storage.googleapis.com/" + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    data = body if media else json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": "Bearer " + token,
        "Content-Type": "application/octet-stream" if media else "application/json",
    })
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read() or "{}")
    except urllib.error.HTTPError as error:
        try:
            reason = json.loads(error.read())["error"]["errors"][0]["reason"]
        except (ValueError, KeyError, IndexError):
            reason = "unknown"
        raise ApiError(error.code, reason) from None
    except (OSError, ValueError) as error:
        raise AuditError("GCS transport or response failure") from error


def save(path, state):
    # State contains resource identifiers, never authentication material.
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as file:
        json.dump(state, file, indent=2)
        temporary = file.name
    os.replace(temporary, path)


def report():
    return {"live_resources": [], "retained_versions": [], "audit_failures": [],
            "evidence": []}


def attempt(result, label, action):
    try:
        return action()
    except AuditError as error:
        result["audit_failures"].append(f"{label}: {error}")
        return None
    except Exception:
        result["audit_failures"].append(f"{label}: unexpected operation or response failure")
        return None


def scoped(state, key):
    if not key.startswith(state["prefix"]):
        raise AuditError("provider returned an object outside the run prefix")
    return key


def s3_list(state, operation):
    markers = {
        "list-objects-v2": (("NextContinuationToken", "continuation_token"),),
        "list-object-versions": (("NextKeyMarker", "key_marker"), ("NextVersionIdMarker", "version_id_marker")),
        "list-multipart-uploads": (("NextKeyMarker", "key_marker"), ("NextUploadIdMarker", "upload_id_marker")),
    }[operation]
    fields = ("Contents", "Versions", "DeleteMarkers", "Uploads")
    result = {field: [] for field in fields}
    arguments = {}
    seen = set()
    while True:
        page = aws("s3api", operation, bucket=state["bucket"], prefix=state["prefix"],
                   no_paginate=True, **arguments)
        for field in fields:
            for item in page.get(field, []):
                scoped(state, item["Key"])
                result[field].append(item)
        if not page.get("IsTruncated"):
            return result
        arguments = {argument: page[key] for key, argument in markers if page.get(key)}
        token = tuple(arguments.items())
        if not arguments or token in seen:
            raise AuditError("incomplete S3 listing or repeated pagination token")
        seen.add(token)


def gcs_path(state):
    return "storage/v1/b/" + state["bucket"]


def gcs_list(state, **query):
    items = []
    query = {"prefix": state["prefix"], **query}
    seen = set()
    while True:
        page = gcs("GET", gcs_path(state) + "/o", query=query)
        for item in page.get("items", []):
            scoped(state, item["name"])
            items.append(item)
        token = page.get("nextPageToken")
        if not token:
            return items
        if token in seen:
            raise AuditError("GCS repeated a pagination token")
        seen.add(token)
        query["pageToken"] = token


def gcs_metadata(state):
    metadata = gcs("GET", gcs_path(state))
    if metadata.get("labels", {}).get("swarmy-bench-run") != state["run_id"]:
        raise AuditError("GCS bucket ownership does not match this run")
    return metadata


def soft_disabled(metadata):
    # Missing policy is not sufficient evidence that creation disabled soft delete.
    return str(metadata.get("softDeletePolicy", {}).get("retentionDurationSeconds")) == "0"


def unchanged_disabled(state, metadata):
    baseline = state.get("gcs_creation", {})
    return (soft_disabled(metadata) and baseline.get("metageneration") == "1"
            and metadata.get("metageneration") == "1"
            and baseline.get("timeCreated") == metadata.get("timeCreated")
            and baseline.get("timeCreated") is not None)


def soft_audit(state, metadata, result):
    try:
        retained = gcs_list(state, softDeleted="true")
    except ApiError as error:
        # GCS rejects this query when disabled. Bucket metadata increments on
        # every policy edit, so generation 1 proves no earlier enabled policy.
        if (error.status == 400 and error.reason in ("invalidArgument", "invalid")
                and unchanged_disabled(state, metadata)):
            result["evidence"].append("GCS soft delete disabled at creation; bucket metadata "
                                      "unchanged (metageneration 1); soft listing unavailable by API design")
            return
        raise
    for item in retained:
        result["retained_versions"].append({
            "kind": "gcs_soft_deleted", "key": item["name"],
            "generation": item["generation"], "bytes": item.get("size"),
            "hard_delete_time": item.get("hardDeleteTime", "unknown"),
        })
    result["evidence"].append("GCS soft-deleted objects: all pages audited")


def preflight(state, path):
    result = report()
    if state["provider"] == "gcp":
        state["bucket_attempted"] = True
        save(path, state)
        # Atomic JSON API equivalent of buckets create --soft-delete-duration=0.
        # The CLI cannot set ownership labels at creation without a second edit.
        gcs("POST", "storage/v1/b", query={"project": state["project"]}, body={
            "name": state["bucket"], "location": state["region"],
            "softDeletePolicy": {"retentionDurationSeconds": "0"},
            "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": True}},
            "labels": {"swarmy-bench-run": state["run_id"]},
        })
        metadata = gcs_metadata(state)
        state["gcs_creation"] = {key: metadata.get(key) for key in
                                 ("metageneration", "timeCreated")}
        save(path, state)
        if not unchanged_disabled(state, metadata):
            result["audit_failures"].append("GCS creation policy is not provably disabled")
        if (metadata.get("retentionPolicy") or metadata.get("defaultEventBasedHold")
                or metadata.get("versioning", {}).get("enabled")):
            result["audit_failures"].append("GCS bucket has retention, holds, or versioning")
        attempt(result, "GCS soft-delete audit", lambda: soft_audit(state, metadata, result))
        versions = attempt(result, "GCS versions audit", lambda: gcs_list(state, versions="true"))
        if versions:
            result["audit_failures"].append("GCS run prefix is not empty")
    else:
        versioning = attempt(result, "s3:GetBucketVersioning",
                             lambda: aws("s3api", "get-bucket-versioning", bucket=state["bucket"]))
        if versioning is not None:
            status = versioning.get("Status", "Unversioned")
            result["evidence"].append("S3 versioning: " + status)
            if status != "Unversioned" and not state["delete_versions"]:
                result["audit_failures"].append("S3 Enabled/Suspended versioning requires --delete-versions")
            if versioning.get("MFADelete") == "Enabled":
                result["audit_failures"].append("S3 MFA Delete is not supported")
        attempt(result, "s3:ListBucket", lambda: s3_list(state, "list-objects-v2"))
        versions = attempt(result, "s3:ListBucketVersions", lambda: s3_list(state, "list-object-versions"))
        if versions and (versions.get("Versions") or versions.get("DeleteMarkers")):
            result["audit_failures"].append("S3 run prefix is not empty")
        attempt(result, "s3:ListBucketMultipartUploads",
                lambda: s3_list(state, "list-multipart-uploads"))
    return result


def probe(state):
    key = state["prefix"] + "preflight-probe"
    # Two writes and an ordinary delete deliberately exercise version retention.
    for data in (b"swarmy storage probe one\n", b"swarmy storage probe two\n"):
        if state["provider"] == "gcp":
            gcs("POST", "upload/" + gcs_path(state) + "/o",
                query={"uploadType": "media", "name": key}, body=data, media=True)
        else:
            with tempfile.NamedTemporaryFile() as file:
                file.write(data)
                file.flush()
                aws("s3api", "put-object", bucket=state["bucket"], key=key, body=file.name)
    if state["provider"] == "gcp":
        gcs("DELETE", gcs_path(state) + "/o/" + urllib.parse.quote(key, safe=""))
    else:
        aws("s3api", "delete-object", bucket=state["bucket"], key=key)


def clean_s3(state, result):
    attempt(result, "s3:GetBucketVersioning",
            lambda: aws("s3api", "get-bucket-versioning", bucket=state["bucket"]))
    versions = attempt(result, "s3:ListBucketVersions before deletion",
                       lambda: s3_list(state, "list-object-versions"))
    if state["delete_versions"] and versions is not None:
        for field in ("Versions", "DeleteMarkers"):
            for item in versions.get(field, []):
                attempt(result, "s3:DeleteObjectVersion", lambda item=item: aws(
                    "s3api", "delete-object", bucket=state["bucket"],
                    key=item["Key"], version_id=item["VersionId"]))
    else:
        current = attempt(result, "S3 live listing before deletion",
                          lambda: s3_list(state, "list-objects-v2"))
        for item in (current or {}).get("Contents", []):
            attempt(result, "s3:DeleteObject", lambda item=item: aws(
                "s3api", "delete-object", bucket=state["bucket"], key=item["Key"]))
    uploads = attempt(result, "S3 multipart listing", lambda: s3_list(state, "list-multipart-uploads"))
    for item in (uploads or {}).get("Uploads", []):
        attempt(result, "s3:AbortMultipartUpload", lambda item=item: aws(
            "s3api", "abort-multipart-upload", bucket=state["bucket"],
            key=item["Key"], upload_id=item["UploadId"]))
    current = attempt(result, "S3 live verification", lambda: s3_list(state, "list-objects-v2"))
    for item in (current or {}).get("Contents", []):
        result["live_resources"].append({"kind": "s3_object", "key": item["Key"]})
    remaining = attempt(result, "S3 multipart verification", lambda: s3_list(state, "list-multipart-uploads"))
    for item in (remaining or {}).get("Uploads", []):
        result["live_resources"].append({"kind": "s3_multipart_upload", "key": item["Key"]})
    versions = attempt(result, "S3 retained-version verification (s3:ListBucketVersions)",
                       lambda: s3_list(state, "list-object-versions"))
    for field in ("Versions", "DeleteMarkers"):
        for item in (versions or {}).get(field, []):
            result["retained_versions"].append({
                "kind": "s3_version" if field == "Versions" else "s3_delete_marker",
                "key": item["Key"], "version_id": item["VersionId"],
                "is_latest": item["IsLatest"], "hard_delete_time": "unknown; S3 does not provide a deadline in version listings",
            })
    if versions is not None:
        result["evidence"].append("S3 versions and delete markers: all pages audited")


def clean_gcs(state, result):
    if not state.get("bucket_attempted"):
        return
    try:
        metadata = gcs_metadata(state)
    except ApiError as error:
        if error.status == 404 and state.get("bucket_deleted"):
            result["evidence"].append("GCS bucket remains absent; prior successful audit recorded in state")
            return
        raise
    versions = attempt(result, "GCS versions before deletion", lambda: gcs_list(state, versions="true"))
    for item in versions or []:
        attempt(result, "GCS generation deletion", lambda item=item: gcs(
            "DELETE", gcs_path(state) + "/o/" + urllib.parse.quote(item["name"], safe=""),
            query={"generation": item["generation"]}))
    current = attempt(result, "GCS live verification", lambda: gcs_list(state))
    live = {item["generation"] for item in current or []}
    for item in current or []:
        result["live_resources"].append({"kind": "gcs_object", "key": item["name"],
                                         "retention_expiration_time": item.get("retentionExpirationTime"),
                                         "temporary_hold": item.get("temporaryHold", False),
                                         "event_based_hold": item.get("eventBasedHold", False)})
    versions = attempt(result, "GCS retained-generation verification", lambda: gcs_list(state, versions="true"))
    for item in versions or []:
        if item["generation"] not in live:
            result["retained_versions"].append({"kind": "gcs_noncurrent", "key": item["name"],
                                                 "generation": item["generation"],
                                                 "hard_delete_time": item.get("hardDeleteTime", "unknown")})
    # Re-read policy after deleting: a change since creation invalidates the proof.
    metadata = gcs_metadata(state)
    attempt(result, "GCS soft-delete verification", lambda: soft_audit(state, metadata, result))
    if not soft_disabled(metadata):
        result["audit_failures"].append("GCS bucket soft delete is enabled; bucket deletion would retain the bucket")
    if not any(result[key] for key in ("live_resources", "retained_versions", "audit_failures")):
        gcs("DELETE", gcs_path(state), query={"ifMetagenerationMatch": metadata["metageneration"]})
        try:
            gcs("GET", gcs_path(state))
        except ApiError as error:
            if error.status != 404:
                raise
            state["bucket_deleted"] = True
            result["evidence"].append("GCS bucket deleted and verified absent after version and soft-delete audits")
            return
        raise AuditError("GCS bucket still exists after deletion")
    result["live_resources"].append({"kind": "gcs_bucket", "name": state["bucket"]})


def cleanup(state, path):
    result = report()
    # Compute termination always runs first, even if storage credentials expired.
    from cloud_vm import terminate
    attempt(result, "machine teardown", lambda: terminate(state, path, result))
    attempt(result, "storage cleanup", lambda: (clean_gcs if state["provider"] == "gcp" else clean_s3)(state, result))
    state["last_cleanup"] = result
    save(path, state)
    return result


def emit(phase, result):
    print(json.dumps({"phase": phase, **result}, sort_keys=True), flush=True)


def failed(result):
    return any(result[key] for key in ("live_resources", "retained_versions", "audit_failures"))


def execute(state, path, workload=None):
    outcome = report()
    try:
        pre = preflight(state, path)
        emit("preflight", pre)
        if failed(pre) and not state["allow_retention"]:
            raise AuditError("preflight refused; no probe writes or machines created")
        if failed(pre):
            outcome["audit_failures"].append("explicit retention allowance used; complete cleanup remains unproven")
        probe(state)
        # Verify delete permissions with real tiny objects before any paid compute.
        if state["provider"] == "aws":
            probe_result = report()
            clean_s3(state, probe_result)
            emit("probe_cleanup", probe_result)
            if failed(probe_result) and not state["allow_retention"]:
                raise AuditError("storage probe cleanup failed; no machines created")
        else:
            probe_result = report()
            versions = attempt(probe_result, "GCS probe versions", lambda: gcs_list(state, versions="true"))
            if versions:
                probe_result["audit_failures"].append("GCS probe generations remain after deletion")
            metadata = gcs_metadata(state)
            attempt(probe_result, "GCS probe soft-delete audit", lambda: soft_audit(state, metadata, probe_result))
            emit("probe_cleanup", probe_result)
            if failed(probe_result) and not state["allow_retention"]:
                raise AuditError("storage probe cleanup failed; no machines created")
        if state["vm"]:
            from cloud_vm import provision
            provision(state, path, workload)
    except (AuditError, KeyboardInterrupt) as error:
        outcome["audit_failures"].append(str(error) if isinstance(error, AuditError) else "run interrupted")
    except Exception:
        outcome["audit_failures"].append("unexpected run failure; cleanup attempted")
    finally:
        result = cleanup(state, path)
        result["audit_failures"].extend(outcome["audit_failures"])
        emit("cleanup", result)
    return 1 if failed(result) else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "cleanup"))
    parser.add_argument("--state", type=Path, required=True, help="private recovery JSON; use a path outside the checkout")
    parser.add_argument("--provider", choices=("gcp", "aws"))
    parser.add_argument("--project", default="swarmy-508717")
    parser.add_argument("--region")
    parser.add_argument("--bucket", default=os.environ.get("SWARMY_BENCH_BUCKET"))
    parser.add_argument("--delete-versions", action="store_true", help="delete S3 versions/markers only under the generated run prefix")
    parser.add_argument("--allow-retention", action="store_true", help="explicitly accept unknown/residual retention; never converts an audit failure to success")
    parser.add_argument("--vm", action="store_true", help="create a paid VM after preflight; omitted for storage-only validation")
    parser.add_argument("--workload", type=Path, help="control-host executable; output suppressed; receives SWARMY_BENCH_STATE")
    parser.add_argument("--ssh-public-key", type=Path)
    parser.add_argument("--nodes", type=int, choices=(1, 2), default=1,
                        help="AWS machines in the same subnet and security group")
    args = parser.parse_args()
    if args.action == "cleanup":
        state = json.loads(args.state.read_text())
        if (not re.fullmatch(r"[0-9a-f]{32}", state["run_id"])
                or state["prefix"] != "swarmy-bench-" + state["run_id"] + "/"
                or state["name"] != "swarmy-bench-" + state["run_id"]
                or state["provider"] not in ("aws", "gcp")
                or (state["provider"] == "gcp" and state["bucket"] != state["name"])):
            parser.error("invalid run ownership in state")
        result = cleanup(state, args.state)
        emit("cleanup", result)
        return int(failed(result))
    if args.state.exists():
        parser.error("state already exists; use cleanup or choose a fresh state file")
    if not args.provider or (args.provider == "aws" and not args.bucket):
        parser.error("run requires --provider and AWS requires --bucket or SWARMY_BENCH_BUCKET")
    if args.vm and (not args.workload or (args.provider == "aws" and not args.ssh_public_key)):
        parser.error("--vm requires --workload, and AWS also requires --ssh-public-key")
    if args.nodes != 1 and (not args.vm or args.provider != "aws"):
        parser.error("--nodes 2 requires --vm --provider aws")
    run_id = uuid.uuid4().hex
    name = "swarmy-bench-" + run_id
    state = dict(run_id=run_id, name=name, prefix=name + "/", provider=args.provider,
                 bucket=name if args.provider == "gcp" else args.bucket,
                 project=args.project, region=args.region or ("us-central1" if args.provider == "gcp" else "us-east-1"),
                 delete_versions=args.delete_versions, allow_retention=args.allow_retention,
                 vm=args.vm, nodes=args.nodes,
                 ssh_public_key=str(args.ssh_public_key.resolve()) if args.ssh_public_key else None)
    save(args.state, state)
    print(json.dumps({"phase": "run", "provider": state["provider"], "bucket": state["bucket"],
                      "prefix": state["prefix"], "retention_allowance": state["allow_retention"]}), flush=True)
    # Convert termination into stack unwinding so the finally block runs.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    return execute(state, args.state, args.workload)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AuditError, OSError, ValueError, KeyError):
        print(json.dumps({"phase": "error", "audit_failures": ["operation or state invalid; cleanup may need retry"]}))
        raise SystemExit(1) from None
