"""Paid compute lifecycle, called only after cloud.py's storage preflight."""

import json
import os

from cloud import AuditError, attempt, aws, command, save


def gcloud(state, *args):
    return json.loads(command(["gcloud", "compute", *args,
                              "--project=" + state["project"], "--format=json", "--quiet"]) or "[]")


def gcp_instances(state):
    instances = gcloud(state, "instances", "list", "--filter=name=" + state["name"])
    if any(item.get("labels", {}).get("swarmy-bench-run") != state["run_id"] for item in instances):
        raise AuditError("GCP machine ownership mismatch")
    return instances


def aws_instances(state):
    result = aws("ec2", "describe-instances", filters=json.dumps([
        {"Name": "tag:Name", "Values": [state["name"]]},
        {"Name": "tag:managed-by", "Values": ["codex-launcher"]},
    ]), region=state["region"])
    return [item for group in result["Reservations"] for item in group["Instances"]]


def provision(state, path, workload):
    # Journal intent before creation so cleanup can discover a timed-out request.
    state["compute_attempted"] = True
    save(path, state)
    if state["provider"] == "gcp":
        # Storage preflight has already created the regional bucket. Never
        # silently move compute to another region when capacity is unavailable.
        zones = [zone["name"] for zone in gcloud(
            state, "zones", "list", "--filter=region:" + state["region"])]
        if not zones:
            raise AuditError("no zones found in the bucket region")
        created = False
        for zone in zones:
            try:
                gcloud(state, "instances", "create", state["name"], "--zone=" + zone,
                       "--machine-type=n2-standard-4", "--image-family=ubuntu-2404-lts-amd64",
                       "--image-project=ubuntu-os-cloud", "--boot-disk-size=50GB",
                       "--labels=swarmy-bench-run=" + state["run_id"], "--local-ssd=interface=nvme")
                state["cache_disk"] = "local NVMe SSD"
                created = True
                break
            except AuditError:
                if gcp_instances(state):
                    raise AuditError("GCP create returned ambiguously; terminate before retrying") from None
        if not created:
            zone = zones[0]
            gcloud(state, "instances", "create", state["name"], "--zone=" + zone,
                   "--machine-type=n2-standard-4", "--image-family=ubuntu-2404-lts-amd64",
                   "--image-project=ubuntu-os-cloud", "--boot-disk-size=50GB",
                   "--labels=swarmy-bench-run=" + state["run_id"],
                   "--create-disk=name=" + state["name"] + "-cache,type=pd-ssd,size=500GB,auto-delete=yes")
            state["cache_disk"] = "500 GB pd-ssd (local SSD unavailable)"
        state["zone"] = zone
    else:
        tags = [{"Key": "managed-by", "Value": "codex-launcher"},
                {"Key": "Name", "Value": state["name"]}]
        aws("ec2", "import-key-pair", key_name=state["name"],
            public_key_material="fileb://" + state["ssh_public_key"],
            tag_specifications=json.dumps([{"ResourceType": "key-pair", "Tags": tags}]), region=state["region"])
        ami = aws("ssm", "get-parameter", name="/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
                  region=state["region"])["Parameter"]["Value"]
        if not os.environ.get("SWARMY_BENCH_SUBNET") or not os.environ.get("SWARMY_BENCH_SECURITY_GROUP"):
            raise AuditError("AWS subnet and security group environment settings required")
        count = state.get("nodes", 1)
        created = aws("ec2", "run-instances", image_id=ami, instance_type="m6id.xlarge", count=count,
            client_token=state["run_id"], key_name=state["name"],
            subnet_id=os.environ["SWARMY_BENCH_SUBNET"],
            security_group_ids=os.environ["SWARMY_BENCH_SECURITY_GROUP"],
            block_device_mappings=json.dumps([{"DeviceName": "/dev/sda1", "Ebs": {
                "VolumeSize": 50, "VolumeType": "gp3", "DeleteOnTermination": True}}]),
            tag_specifications=json.dumps([{"ResourceType": kind, "Tags": tags} for kind in ("instance", "volume")]),
            region=state["region"])
        instances = created["Instances"]
        if len(instances) != count:
            raise AuditError("unexpected AWS benchmark machine count")
        state["instances"] = [{"instance_id": item["InstanceId"],
                               "private_ip": item.get("PrivateIpAddress")} for item in instances]
        state["instance_id"] = instances[0]["InstanceId"]
        state["private_ip"] = instances[0].get("PrivateIpAddress")
        state["cache_disk"] = "local NVMe SSD"
    save(path, state)
    print(json.dumps({"phase": "machine", "name": state["name"], "cache_disk": state["cache_disk"]}), flush=True)
    environment = dict(os.environ, SWARMY_BENCH_STATE=str(path.resolve()),
                       SWARMY_S3_BUCKET=state["bucket"] + "/" + state["prefix"].rstrip("/"),
                       SWARMY_S3_REGION=state["region"])
    # Workload does SSH readiness, installation, and volume.py using this state.
    # Capture output to prevent a child command from leaking credentials to logs.
    command([str(workload.resolve())], env=environment, timeout=7200)


def terminate(state, path, result):
    if not state.get("compute_attempted"):
        result["evidence"].append("No cloud VM creation attempted")
        return
    if state["provider"] == "gcp":
        instances = attempt(result, "GCP machine discovery", lambda: gcp_instances(state))
        for item in instances or []:
            zone = item["zone"].rsplit("/", 1)[-1]
            attempt(result, "GCP machine deletion", lambda zone=zone: gcloud(
                state, "instances", "delete", state["name"], "--zone=" + zone))
        remaining = attempt(result, "GCP machine verification", lambda: gcp_instances(state))
        if remaining is not None:
            result["evidence"].append("GCP instances list for run: " + str(len(remaining)) + " remaining")
        for item in remaining or []:
            result["live_resources"].append({"kind": "gcp_vm", "name": state["name"], "status": item["status"]})
        # Exact generated disk names only; attached disks are auto-deleted.
        disks = attempt(result, "GCP disk verification", lambda: gcloud(
            state, "disks", "list", "--filter=name=(" + state["name"] + " " + state["name"] + "-cache)"))
        if disks is not None:
            result["evidence"].append("GCP disks list for run: " + str(len(disks)) + " remaining")
        for item in disks or []:
            result["live_resources"].append({"kind": "gcp_disk", "name": item["name"]})
    else:
        instances = attempt(result, "AWS machine discovery", lambda: aws_instances(state))
        for item in instances or []:
            if item["State"]["Name"] == "terminated":
                continue
            identifier = item["InstanceId"]
            attempt(result, "AWS machine termination", lambda identifier=identifier: aws(
                "ec2", "terminate-instances", instance_ids=identifier, region=state["region"]))
            attempt(result, "AWS termination wait", lambda identifier=identifier: command([
                "aws", "ec2", "wait", "instance-terminated", "--instance-ids", identifier,
                "--region", state["region"], "--no-cli-pager"]))
        remaining = attempt(result, "AWS machine verification", lambda: aws_instances(state))
        if remaining is not None:
            result["evidence"].append("AWS describe-instances for run: " +
                                      ", ".join(item["State"]["Name"] for item in remaining))
        for item in remaining or []:
            if item["State"]["Name"] != "terminated":
                result["live_resources"].append({"kind": "aws_vm", "name": state["name"], "status": item["State"]["Name"]})
        # Only the exact run key is touched, including after partial provisioning.
        keys = attempt(result, "AWS key discovery", lambda: aws("ec2", "describe-key-pairs", filters=json.dumps([
            {"Name": "key-name", "Values": [state["name"]]},
            {"Name": "tag:managed-by", "Values": ["codex-launcher"]},
        ]), region=state["region"]))
        for item in (keys or {}).get("KeyPairs", []):
            attempt(result, "AWS key deletion", lambda item=item: aws(
                "ec2", "delete-key-pair", key_pair_id=item["KeyPairId"], region=state["region"]))
        for operation, field, kind in (("describe-key-pairs", "KeyPairs", "aws_key_pair"),
                                       ("describe-volumes", "Volumes", "aws_volume")):
            remaining = attempt(result, "AWS " + kind + " verification", lambda operation=operation: aws(
                "ec2", operation, filters=json.dumps([
                    {"Name": "tag:Name", "Values": [state["name"]]},
                    {"Name": "tag:managed-by", "Values": ["codex-launcher"]},
                ]), region=state["region"]))
            if remaining is not None:
                result["evidence"].append("AWS " + kind + " for run: " +
                                          str(len(remaining.get(field, []))) + " remaining")
            for _ in (remaining or {}).get(field, []):
                result["live_resources"].append({"kind": kind, "name": state["name"]})
    save(path, state)
