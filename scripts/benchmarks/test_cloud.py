"""Offline provider contract and lifecycle tests; no cloud credentials needed."""

import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import cloud
import cloud_vm


class S3:
    def __init__(self, state, versioned=False, locked=False, denied=()):
        self.state = state
        self.versioned = versioned
        self.locked = locked
        self.denied = denied
        self.versions = []
        self.calls = []
        self.unrelated = {"outside-run/keep": b"untouched"}

    def __call__(self, service, operation, **args):
        self.calls.append((operation, args))
        if operation in self.denied:
            raise cloud.AuditError("permission denied")
        if "key" in args:
            assert args["key"].startswith(self.state["prefix"])
        if "prefix" in args:
            assert args["prefix"] == self.state["prefix"]
        assert args["bucket"] == self.state["bucket"]
        if operation == "get-bucket-versioning":
            return {"Status": "Enabled"} if self.versioned else {}
        if operation == "list-multipart-uploads":
            return {}
        if operation == "list-objects-v2":
            return {"Contents": [{"Key": v["Key"]} for v in self.versions if v["IsLatest"] and not v["marker"]]}
        if operation == "list-object-versions":
            return {field: [copy.copy(v) for v in self.versions if v["marker"] == marker]
                    for field, marker in (("Versions", False), ("DeleteMarkers", True))}
        if operation == "put-object":
            if not self.versioned:
                self.versions.clear()
            for version in self.versions:
                version["IsLatest"] = False
            self.versions.append(dict(Key=args["key"], VersionId=str(len(self.versions)), IsLatest=True, marker=False))
            return {}
        if operation == "delete-object":
            if "version_id" in args:
                if self.locked:
                    raise cloud.AuditError("version retained by object lock")
                self.versions = [v for v in self.versions if v["VersionId"] != args["version_id"]]
            elif self.versioned:
                for version in self.versions:
                    version["IsLatest"] = False
                self.versions.append(dict(Key=args["key"], VersionId=str(len(self.versions)), IsLatest=True, marker=True))
            else:
                self.versions.clear()
            return {}
        raise AssertionError(operation)


class GCS:
    def __init__(self, state):
        self.state = state
        self.metadata = None
        self.objects = []
        self.retained = []
        self.calls = []
        self.block_delete = False

    def __call__(self, method, path, *, query=None, body=None, media=False):
        query = query or {}
        self.calls.append((method, path, query, body))
        if method == "POST" and path == "storage/v1/b":
            assert body["softDeletePolicy"] == {"retentionDurationSeconds": "0"}
            self.metadata = dict(body, metageneration="1", timeCreated="2026-09-15T00:00:00Z")
            return self.metadata
        assert self.state["bucket"] in path
        if self.metadata is None:
            raise cloud.ApiError(404, "notFound")
        if method == "POST":
            assert query["name"].startswith(self.state["prefix"])
            self.objects = [dict(name=query["name"], generation="2", size=str(len(body)))]
            return {}
        if path.endswith("/o"):
            assert query["prefix"] == self.state["prefix"]
            if query.get("softDeleted"):
                if cloud.soft_disabled(self.metadata):
                    raise cloud.ApiError(400, "invalid")
                return {"items": self.retained}
            return {"items": self.objects}
        if method == "GET":
            return copy.deepcopy(self.metadata)
        if method == "DELETE" and "/o/" in path:
            if self.block_delete:
                raise cloud.ApiError(403, "retentionPolicyNotMet")
            if not cloud.soft_disabled(self.metadata):
                self.retained.extend(dict(item, hardDeleteTime="2026-09-22T00:00:00Z") for item in self.objects)
            self.objects.clear()
            return {}
        if method == "DELETE":
            assert not self.objects and not self.retained
            self.metadata = None
            return {}
        raise AssertionError((method, path))


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "run.json"
        run_id = "a" * 32
        name = "swarmy-bench-" + run_id
        self.state = dict(provider="aws", run_id=run_id, name=name, prefix=name + "/",
                          bucket="benchmark-bucket", delete_versions=True, allow_retention=False,
                          vm=False, region="us-east-1", project="test")

    def gcp(self):
        self.state.update(provider="gcp", bucket=self.state["name"])
        return GCS(self.state)

    def execute(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            code = cloud.execute(self.state, self.path)
        return code, [json.loads(line) for line in output.getvalue().splitlines()]

    def test_s3_unversioned_and_versioned_complete_cycles(self):
        for versioned in (False, True):
            with self.subTest(versioned=versioned):
                fake = S3(self.state, versioned=versioned)
                with patch.object(cloud, "aws", fake):
                    code, output = self.execute()
                self.assertEqual(code, 0)
                self.assertFalse(cloud.failed(output[-1]))
                self.assertEqual(fake.versions, [])
                self.assertEqual(fake.unrelated, {"outside-run/keep": b"untouched"})
                if versioned:
                    self.assertEqual(sum("version_id" in args for _, args in fake.calls), 3)

    def test_s3_locked_versions_are_not_hidden_by_empty_live_listing(self):
        fake = S3(self.state, versioned=True, locked=True)
        with patch.object(cloud, "aws", fake):
            code, output = self.execute()
        final = output[-1]
        self.assertEqual(code, 1)
        self.assertFalse(final["live_resources"])
        self.assertEqual(len(final["retained_versions"]), 3)
        self.assertTrue(final["audit_failures"])
        self.assertIn("unknown", final["retained_versions"][0]["hard_delete_time"])

    def test_s3_requires_explicit_version_deletion(self):
        self.state["delete_versions"] = False
        fake = S3(self.state, versioned=True)
        with patch.object(cloud, "aws", fake):
            code, _ = self.execute()
        self.assertEqual(code, 1)
        self.assertFalse(any(op == "put-object" for op, _ in fake.calls))

    def test_unauditable_preflight_blocks_paid_compute_and_writes(self):
        self.state["vm"] = True
        fake = S3(self.state, denied=("get-bucket-versioning", "list-object-versions"))
        with patch.object(cloud, "aws", fake), patch.object(cloud_vm, "provision") as vm:
            code, output = self.execute()
        self.assertEqual(code, 1)
        vm.assert_not_called()
        self.assertFalse(any(op == "put-object" for op, _ in fake.calls))
        self.assertTrue(output[-1]["audit_failures"])

    def test_explicit_allowance_allows_run_but_never_claims_success(self):
        self.state.update(vm=True, allow_retention=True)
        fake = S3(self.state, denied=("get-bucket-versioning", "list-object-versions"))
        with patch.object(cloud, "aws", fake), patch.object(cloud_vm, "provision") as vm:
            code, _ = self.execute()
        vm.assert_called_once()
        self.assertEqual(code, 1)
        self.assertTrue(any(op == "put-object" for op, _ in fake.calls))

    def test_gcs_disabled_from_creation_cycle_and_repeat_cleanup(self):
        fake = self.gcp()
        with patch.object(cloud, "gcs", fake):
            code, output = self.execute()
            repeated = cloud.cleanup(self.state, self.path)
        self.assertEqual(code, 0)
        self.assertFalse(cloud.failed(output[-1]))
        self.assertFalse(cloud.failed(repeated))
        self.assertIsNone(fake.metadata)
        self.assertTrue(any("metageneration 1" in item for item in output[-1]["evidence"]))

    def test_gcs_soft_deleted_generations_have_deadlines(self):
        fake = self.gcp()
        with patch.object(cloud, "gcs", fake):
            cloud.preflight(self.state, self.path)
            fake.metadata.update(metageneration="2", softDeletePolicy={"retentionDurationSeconds": "604800"})
            cloud.probe(self.state)
            result = cloud.cleanup(self.state, self.path)
        self.assertTrue(cloud.failed(result))
        self.assertEqual(result["retained_versions"][0]["hard_delete_time"], "2026-09-22T00:00:00Z")
        self.assertTrue(any(item["kind"] == "gcs_bucket" for item in result["live_resources"]))
        self.assertIsNotNone(fake.metadata)

    def test_gcs_changed_disabled_policy_is_unauditable(self):
        fake = self.gcp()
        with patch.object(cloud, "gcs", fake):
            cloud.preflight(self.state, self.path)
            fake.metadata["metageneration"] = "3"
            result = cloud.cleanup(self.state, self.path)
        self.assertTrue(result["audit_failures"])
        self.assertIsNotNone(fake.metadata)

    def test_gcs_retention_blocks_deletion_and_reports_live_deadline(self):
        fake = self.gcp()
        with patch.object(cloud, "gcs", fake):
            cloud.preflight(self.state, self.path)
            fake.objects = [dict(name=self.state["prefix"] + "held", generation="1",
                                 retentionExpirationTime="2026-09-16T00:00:00Z", temporaryHold=True)]
            fake.block_delete = True
            result = cloud.cleanup(self.state, self.path)
        self.assertEqual(result["live_resources"][0]["retention_expiration_time"], "2026-09-16T00:00:00Z")
        self.assertTrue(result["audit_failures"])

    def test_gcs_ownership_mismatch_never_deletes(self):
        fake = self.gcp()
        with patch.object(cloud, "gcs", fake):
            cloud.preflight(self.state, self.path)
            fake.metadata["labels"] = {}
            result = cloud.cleanup(self.state, self.path)
        self.assertTrue(result["audit_failures"])
        self.assertFalse(any(method == "DELETE" for method, *_ in fake.calls))

    def test_termination_precedes_storage_failure_even_after_provision_failure(self):
        self.state["vm"] = True
        fake = S3(self.state)
        order = []
        def provision(*_):
            order.append("provision")
            raise cloud.AuditError("partial provisioning failure")
        def storage(*_):
            if not order:
                order.append("probe")
                return
            order.append("storage")
            raise RuntimeError("secret must not be printed")
        with patch.object(cloud, "aws", fake), patch.object(cloud_vm, "provision", provision), \
                patch.object(cloud_vm, "terminate", side_effect=lambda *_: order.append("terminate")), \
                patch.object(cloud, "clean_s3", storage):
            code, output = self.execute()
        self.assertEqual(code, 1)
        self.assertEqual(order, ["probe", "provision", "terminate", "storage"])
        self.assertNotIn("secret", json.dumps(output))

    def test_provider_errors_never_relay_credentials(self):
        result = subprocess.CompletedProcess([], 1, "access-key-secret", "Bearer private-key")
        with patch.object(subprocess, "run", return_value=result):
            with self.assertRaises(cloud.AuditError) as error:
                cloud.command(["fake"])
        self.assertNotIn("secret", str(error.exception))
        self.assertNotIn("private-key", str(error.exception))

    def test_out_of_prefix_listing_refuses_all_mutations(self):
        with patch.object(cloud, "aws", return_value={"Versions": [{"Key": "outside-run/keep"}]}):
            with self.assertRaises(cloud.AuditError):
                cloud.s3_list(self.state, "list-object-versions")

    def test_gcs_pagination_including_empty_page(self):
        self.gcp()
        responses = [{"nextPageToken": "second"}, {"items": [{"name": self.state["prefix"] + "last"}]}]
        with patch.object(cloud, "gcs", side_effect=responses) as api:
            self.assertEqual(len(cloud.gcs_list(self.state, softDeleted="true")), 1)
        self.assertEqual(api.call_args.kwargs["query"]["pageToken"], "second")

    def test_truncated_s3_listing_is_an_audit_failure(self):
        with patch.object(cloud, "aws", return_value={"IsTruncated": True}):
            with self.assertRaises(cloud.AuditError):
                cloud.s3_list(self.state, "list-object-versions")

    def test_s3_pagination_consumes_versions_and_markers(self):
        key = self.state["prefix"] + "object"
        pages = [{"IsTruncated": True, "NextKeyMarker": key, "NextVersionIdMarker": "v1",
                  "Versions": [{"Key": key, "VersionId": "v1"}]},
                 {"IsTruncated": False, "DeleteMarkers": [{"Key": key, "VersionId": "v2"}]}]
        with patch.object(cloud, "aws", side_effect=pages) as api:
            result = cloud.s3_list(self.state, "list-object-versions")
        self.assertEqual(len(result["Versions"]), 1)
        self.assertEqual(len(result["DeleteMarkers"]), 1)
        self.assertEqual(api.call_args.kwargs["version_id_marker"], "v1")
        self.assertEqual(api.call_args.kwargs["prefix"], self.state["prefix"])

    def test_aws_termination_deletes_only_discovered_run_resources(self):
        self.state["compute_attempted"] = True
        calls = []
        def api(service, operation, **args):
            calls.append((operation, args))
            if operation == "describe-instances":
                status = "terminated" if any(op == "terminate-instances" for op, _ in calls) else "running"
                return {"Reservations": [{"Instances": [{"InstanceId": "i-run", "State": {"Name": status}}]}]}
            if operation == "describe-key-pairs":
                return {"KeyPairs": [] if any(op == "delete-key-pair" for op, _ in calls)
                        else [{"KeyPairId": "key-run"}]}
            return {"Volumes": []}
        with patch.object(cloud_vm, "aws", api), patch.object(cloud_vm, "command"):
            result = cloud.report()
            cloud_vm.terminate(self.state, self.path, result)
        self.assertFalse(cloud.failed(result))
        self.assertIn(("terminate-instances", {"instance_ids": "i-run", "region": "us-east-1"}), calls)
        self.assertIn(("delete-key-pair", {"key_pair_id": "key-run", "region": "us-east-1"}), calls)
        for operation, args in calls:
            if operation.startswith("describe-"):
                self.assertIn(self.state["name"], args["filters"])
                self.assertIn("codex-launcher", args["filters"])

    def test_gcp_failed_termination_still_verifies_vm_and_disks(self):
        self.gcp()
        self.state["compute_attempted"] = True
        def api(state, *args):
            if args[:2] == ("instances", "delete"):
                raise cloud.AuditError("compute permission denied")
            if args[0] == "disks":
                return [{"name": state["name"] + "-cache"}]
            return [{"name": state["name"], "zone": "projects/test/zones/us-central1-a",
                     "status": "RUNNING", "labels": {"swarmy-bench-run": state["run_id"]}}]
        with patch.object(cloud_vm, "gcloud", api):
            result = cloud.report()
            cloud_vm.terminate(self.state, self.path, result)
        self.assertEqual({item["kind"] for item in result["live_resources"]}, {"gcp_vm", "gcp_disk"})
        self.assertTrue(result["audit_failures"])


if __name__ == "__main__":
    unittest.main()
