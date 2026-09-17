# Stock Ubuntu cloud volume validation

Run date: 2026-09-15 (UTC).

## Scope and method

This run validates the disk implementation at `ff5832378786802f03a1254226f1d98fab2fd859`
with the benchmark helpers in this pull request. It uses Ubuntu 24.04 LTS,
Rust 1.98.1, the default Cargo debug profile, 256 KiB chunks, NBD, and the
server's default four-chunk readahead. These are development-build measurements,
not production capacity estimates. Each cloud has its own local FoundationDB
and NATS and uses its own remote object storage through `object_store` 0.12.5.
There is no cross-cloud data transfer in this experiment.

The executable procedure is [volume.py](../scripts/benchmarks/volume.py).
[cloud-chunk-read.rs](../crates/swarmy-volume/examples/cloud-chunk-read.rs)
measures the production `ChunkStore::get_chunk` path, including hash verification.

- Shell startup: three fresh volumes per cache condition, timed from immediately
  before `swarmy vol create` through attach, ext4 mount, and a successful command
  in interactive Bash under a controlling terminal (`script` and `chroot`).
  Image construction and teardown are outside the timer. This is a disk-to-shell
  measurement, not a full sandbox scheduler or VM boot measurement.
- Cold shell: stop the previous attachment, delete the local chunk cache, sync,
  and drop the host page cache before each trial. FoundationDB and the cloud
  service remain running; their internal caches are not controlled.
- Warm shell: read the complete base block device once to populate the shared
  chunk cache, then drop the host page cache before each fresh-volume trial.
  This measures reuse of cached base chunks across distinct volumes.
- Single chunk: upload ten distinct, nonzero 256 KiB chunks, construct a separate
  client, and time ten serial remote GETs. No local chunk cache is involved.
  Sample zero includes connection establishment; subsequent samples reuse the
  connection. Cloud-side caching is not controlled.
- Sequential reads: write and flush a 128 MiB file from `/dev/urandom`, detach,
  clone, delete the chunk cache, attach, and mount. Read the file with
  `dd bs=256K iflag=direct`, then repeat with its chunks cached. Mounting and
  checking the persisted compiler happen before timing. Report MiB/s as
  128 divided by elapsed seconds; command startup is included.
- Installation flush: the benchmark recipe is an 8 GiB Noble image containing
  Bash, coreutils, curl, and CA certificates. It deliberately omits
  `build-essential`, which is already present in `images/base-ubuntu`.
  Verify the package is absent, run `apt-get update` and
  `apt-get install -y build-essential`, then time
  `swarmy vol flush VOLUME --mount MOUNT`. The timer includes the filesystem
  freeze, its dirty-page writeback, chunk uploads, manifest publication in
  FoundationDB, and thaw. Background uploading is disabled. Installation and
  package downloads are outside the timer. Verify `gcc` again after cloning
  and attaching from a cleared cache.

## Storage preflight and managed lifecycle

For new runs, start with [cloud.py](../scripts/benchmarks/cloud.py) on the
control host. It performs storage preflight before any VM creation, writes two
tiny probe objects at the same key, deletes that key, and verifies cleanup.
The default is storage-only. `--vm` explicitly enables paid compute through
[cloud_vm.py](../scripts/benchmarks/cloud_vm.py). `volume.py` remains the
measurement program run inside the prepared VM.

Install and authenticate the cloud CLIs using private credential files. Keep
shell tracing disabled. The Python tools use the existing CLI authentication;
they capture provider output and emit only selected resource metadata and fixed
error messages. They never change an existing bucket's policy.

```sh
sudo snap install google-cloud-cli --classic
sudo snap install aws-cli --classic
gcloud auth activate-service-account --key-file ~/.config/swarmy-bench/gcp-service-account.json
gcloud config set project swarmy-508717
set -a
. ~/.config/swarmy-bench/aws.env
set +a

# Choose fresh state paths outside the checkout. The tool creates mode-0600 files.
python3 scripts/benchmarks/cloud.py run --provider gcp \
  --state /tmp/swarmy-gcp-storage.json
python3 scripts/benchmarks/cloud.py run --provider aws --delete-versions \
  --state /tmp/swarmy-aws-storage.json
```

### Storage rules

GCS buckets have generated names and an ownership label set in the same atomic
JSON API creation request as `softDeletePolicy.retentionDurationSeconds=0`.
This is the API equivalent of `gcloud storage buckets create ...
--soft-delete-duration=0`. The CLI cannot set the ownership label during
creation, which would otherwise require a second metadata update. Preflight
reads the effective policy before writing and refuses retention policies,
default holds, or object versioning. Only a bucket owned by this run can be
cleaned. The tool never enables soft delete to perform an audit.

GCS rejects `softDeleted=true` listings when soft delete is disabled. In that
specific case, the tool requires a bucket created with soft delete disabled,
matching creation time, and metageneration still equal to 1. Every bucket
metadata edit increments metageneration, including changing soft delete and
changing it back. This establishes that the new bucket never had an enabled
policy; it is independent of the ordinary object listing. If the metadata has
changed, cleanup reports an audit failure and keeps the bucket for review.
When soft-delete listing is supported, all pages are read and each residual's
`hardDeleteTime` is reported. Existing retained objects cannot be made to expire
sooner by disabling the policy. See the
[object listing API](https://docs.cloud.google.com/storage/docs/json_api/v1/objects/list)
and [soft-delete policy rules](https://docs.cloud.google.com/storage/docs/soft-delete).

S3 uses the existing bucket and a generated `swarmy-bench-<uuid>/` prefix.
Both `Enabled` and `Suspended` versioning require `--delete-versions`;
suspending versioning does not remove its history. Even an unversioned bucket
requires a separate successful version listing. With `--delete-versions`,
cleanup deletes each exact version ID and delete marker, then lists again.
It also aborts incomplete multipart uploads under the run prefix and verifies
that none remain. Every listing follows pagination and validates each returned
key before any deletion. It never deletes the AWS bucket or changes versioning,
Object Lock, lifecycle rules, or retention settings. Failed deletes leave
versions in the residual report. S3 version listings do not include a scheduled
hard-delete deadline, so the report explicitly says the deadline is unknown.
See [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html).

The AWS identity needs these permissions, supplied by the bucket administrator:

- `s3:GetBucketVersioning` on the benchmark bucket.
- `s3:ListBucket` and `s3:ListBucketVersions` on that bucket, restricted using
  `s3:prefix` to the generated run prefix (or the dedicated benchmark prefix
  family for repeated runs).
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, and, for versioned runs,
  `s3:DeleteObjectVersion`, restricted to object ARNs under that prefix.
- `s3:ListBucketMultipartUploads` on the benchmark bucket and
  `s3:AbortMultipartUpload` on object ARNs under the prefix. Requests always
  include the run prefix; no account-wide bucket discovery is used.

No retention-bypass or bucket-policy editing permission is needed. The tiny
probe tests actual deletion before paid compute, including deletion of versions
created by an overwrite and the marker created by an ordinary delete. An Object
Lock or MFA Delete configuration that prevents complete cleanup is refused.

### Reports, recovery, and retention allowance

Stdout is JSON lines with `preflight`, `probe_cleanup`, and `cleanup` phases.
Each report separates:

- `live_resources`: current objects, incomplete uploads, buckets, machines,
  disks, or key pairs still present. Held GCS objects include their retention
  expiration time and hold flags.
- `retained_versions`: GCS soft-deleted or noncurrent generations and S3
  versions/delete markers, with known deadlines or an explicit unknown value.
- `audit_failures`: denied permissions, failed operations, incomplete listings,
  and configurations for which absence cannot be proved.
- `evidence`: the successful independent checks, including the GCS creation
  policy proof when soft-delete listing is unavailable.

Empty arrays mean no residuals were observed in those categories; they never
override `audit_failures`. Exit 0 requires every category to be clear. A
`--allow-retention` flag is an explicit operator acceptance of residual or
unknown retention. It allows a run past failed policy/audit checks and is
recorded in stdout and state, but it never converts an incomplete audit into a
successful run. It does not bypass resource ownership checks or grant cloud
permissions. Do not use it for an ordinary temporary run that needs immediate
complete deletion.

The state file journals resource intent before provisioning. Preserve it until
cleanup is complete; do not edit it or run concurrent writers on the run prefix.
After a crash or timeout, retry:

```sh
python3 scripts/benchmarks/cloud.py cleanup --state /tmp/swarmy-gcp-storage.json
python3 scripts/benchmarks/cloud.py cleanup --state /tmp/swarmy-aws-storage.json
```

Machine termination runs before the storage audit, even after provisioning or
workload failure. SIGTERM and Ctrl-C unwind through cleanup. SIGKILL, host loss,
or expired cloud credentials require a later `cleanup` invocation. Termination
and verification errors remain in the report while independent cleanup steps
continue. GCS bucket deletion uses a metageneration precondition after auditing
all object generations and soft delete. Buckets with residuals remain available
for investigation and deadline-based follow-up.

### Optional VM provisioning

Once storage-only validation passes, add `--vm --workload /absolute/path/to/run`
to the same `run` command. AWS also requires `--ssh-public-key /path/to/key.pub`
and the private `SWARMY_BENCH_SUBNET` and `SWARMY_BENCH_SECURITY_GROUP` environment
settings. The tool imports a generated key-pair name and tags the key pair,
instance, and boot volume with `managed-by=codex-launcher` and the run's Name.
GCP tries N2 with NVMe local SSD in each zone of the bucket region before
falling back to a 500 GB `pd-ssd` in that region; stdout and state record which
cache was used. Set `--region` to choose the colocated bucket and VM placement.
Both providers use the stock Ubuntu 24.04 configuration in the installation
section below and auto-delete attached disks.

The workload is a synchronous control-host executable with a two-hour timeout.
It receives `SWARMY_BENCH_STATE`, `SWARMY_S3_BUCKET` (including the run prefix),
and `SWARMY_S3_REGION` in its environment. State contains the GCP machine name
and zone, or the AWS instance ID and private IP. The executable waits for SSH
and cloud-init, performs the installation below, copies private S3 settings,
runs the tests and `volume.py`, and fetches measurement files. Its stdout and
stderr are captured and discarded to keep credentials out of lifecycle logs;
it must save any desired measurements itself. No SSH private key or HMAC secret
is stored in lifecycle state. HMAC creation is not part of this tool: if the
workload creates an HMAC key for GCS's XML API, it must deactivate and delete
that exact key in its own finally/trap block and verify deletion, as in the
historical procedure below. Existing authentication keys are never modified.

### Storage-only validation of this change

On 2026-09-15 the real GCS create/write/overwrite/delete/verify cycle passed
with zero retained generations and the bucket verified absent. A preliminary
CLI-label attempt failed before bucket creation; a second run discovered GCS's
actual `invalid` error reason for disabled soft delete, refused writes, and its
empty bucket was removed by a successful cleanup retry after fixing the parser.
The final run passed without an allowance. Real AWS preflight exited 1 before
writing or creating machines: the identity denied `s3:GetBucketVersioning`,
`s3:ListBucketVersions`, and `s3:ListBucketMultipartUploads`. The live prefix
listing was empty, but the tool correctly did not certify version cleanup.
No retention allowance was used. No cloud VM or HMAC key was created.

[Storage validation output](benchmarks/storage-preflight.json) records the final
provider reports. The log scan checked the actual local credential values
without printing them; none appeared. GCS mutations were confined to the new
owned buckets. AWS performed read-only, prefix-filtered storage requests and a
bucket versioning check. No existing bucket configuration was changed.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/benchmarks -p 'test_*.py' -v
```

The offline provider tests cover both normal cycles, versioned S3 cleanup,
locked S3 residuals, GCS soft-delete deadlines and holds, changed policies,
ownership mismatch, pagination, credential-safe errors, preflight refusal,
explicit allowance, and compute teardown failures. They do not substitute for
live versioned S3 validation: the current AWS identity cannot authorize that
check. Retention fixtures are simulated so this validation does not deliberately
create another week of retained cloud data. VM provisioning and a full benchmark
run were not exercised in this change, as requested.

## Machines and installation

| Item | Google Cloud | AWS |
| --- | --- | --- |
| Region / zone | `us-central1` / `us-central1-a` | `us-east-1` / `us-east-1a` |
| Machine | `n2-standard-4`, 4 vCPUs, 16 GiB RAM | `m6id.xlarge`, 4 vCPUs, 16 GiB RAM |
| Stock image | `ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20260906` | `ami-025d99823a4caad37`, `ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20260904` |
| Kernel | `7.0.0-1011-gcp` | `7.0.0-1012-aws` |
| Boot disk | 50 GB standard persistent disk | 50 GiB gp3 EBS, delete on termination |
| Cache disk | 500 GB `pd-ssd`, `/dev/sdb` | Local NVMe instance storage, 220.7 GiB, `/dev/nvme0n1` |
| `modprobe nbd nbds_max=32` | Passed on stock kernel | Passed on stock kernel |
| `modprobe ublk_drv` | Passed on stock kernel | Passed on stock kernel |

Both modules loaded before installing build dependencies. No replacement kernel,
extra module package, reboot, or nested virtualization setting was needed.
The repository has an NBD backend; loading `ublk_drv` does not test a ublk backend.

The requested GCP configuration with one NVMe local SSD failed with
`ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS` in `us-central1-a`, `-c`, `-f`, and
`-b`. Each response named `n2-standard-4` and `local-ssd:1` as unavailable.
The same machine type without local SSD succeeded in the original zone. The
fallback is a dedicated SSD persistent disk; its performance must not be
interpreted as local-NVMe performance. `pd-extreme` requires at least 64 N2
vCPUs, and `n2-standard-4` supports no Hyperdisk Extreme volumes; see the
[Extreme PD restrictions](https://docs.cloud.google.com/compute/docs/disks/extreme-persistent-disk#machine_shape_support)
and [N2 disk limits](https://docs.cloud.google.com/compute/docs/general-purpose-machines#n2_supported_disk_types).
Both GCP disks were set to auto-delete. The AWS instance, boot volume, and
imported key pair carried `managed-by=codex-launcher` and `Name` tags at creation
so the supplied policy permitted cleanup.

Installation on each fresh machine:

1. Wait for `cloud-init status --wait`. Record the image, kernel, and `lsblk`.
   Load both modules with the commands above.
2. Run `apt-get update`, then install `build-essential pkg-config clang
   libclang-dev curl rsync fio debootstrap e2fsprogs skopeo umoci jq runc`.
3. Identify the unmounted cache disk with `lsblk`, run `mkfs.ext4 -F` on that
   device, and mount it at `/mnt/bench`. Create `tmp` and `run` below it.
   GCP named the second disk `google-persistent-disk-1`, regardless of its cloud
   resource name; the initial resource-name device lookup failed without
   formatting anything, then setup used the verified `/dev/sdb`.
4. Rsync the checkout, excluding `.git`, `target`, `.dev`, and `.swarmy`.
   AWS SSH uses the private IP and a temporary tagged imported key pair.
   GCP uses `gcloud compute ssh` and `gcloud compute scp`; rsync needs an SSH
   wrapper that passes its remote command with `gcloud ... --command`.
5. Install rustup, source `~/.cargo/env`, and run `rustup show` in the checkout
   to install the pinned toolchain. Run `scripts/install-dev-tools.sh` for
   FoundationDB 7.3.79, NATS 2.14.6, and SeaweedFS 4.47.
6. Set `SWARMY_FDB_LIB_DIR=$HOME/.local/lib` and
   `CARGO_TARGET_DIR=/mnt/bench/target`. Run
   `cargo build --workspace --locked` and compile the test executables as the
   ordinary Ubuntu user. Run `scripts/dev-stack.sh start` and source `.dev/env`.
7. Override the S3 variables from a private environment file. GCP uses its
   temporary bucket, an HMAC key, endpoint `https://storage.googleapis.com`,
   and region `us-central1`. AWS uses endpoint
   `https://s3.us-east-1.amazonaws.com` and region `us-east-1`.
   Both use path-style requests. With this pinned builder, setting the AWS
   bucket string to `BUCKET/RUN_PREFIX` appends the prefix to object URLs;
   the disk paths use object HEAD/GET/PUT, not bucket listing. This isolates
   the run in the pre-existing bucket without modifying production code.
8. Export `TMPDIR=/mnt/bench/tmp`, execute the built test binaries with
   `sudo -E env TMPDIR="$TMPDIR"`, and run the benchmark from `/mnt/bench/run`
   with the built CLI directory on `PATH`. Cache and dirty files live under that directory's
   `.swarmy/volumes`, on the cache disk.

The first test pass used `sudo -E`, which removed `TMPDIR`. The NBD, volume,
and node attachment tests were repeated with the variable explicitly passed
after sudo so their cache fixtures used the intended disk. The offline image
tests do not use a persistent chunk cache. The timed harness always placed its
cache under `/mnt/bench/run`, independently of `TMPDIR`.

## Results

Times with multiple samples are median [minimum, maximum]. These small
samples do not establish tail-latency or capacity guarantees. The cache media
and processors differ between clouds.

| Measurement | Google Cloud | AWS |
| --- | --- | --- |
| Disk to shell, cold cache (s; n=3) | 3.190 [2.786, 5.865] | 1.891 [1.806, 3.344] |
| Disk to shell, warm base (s; n=3) | 1.021 [0.936, 2.563] | 0.717 [0.715, 0.733] |
| First uncached chunk, new connection (ms; n=1) | 60.041 | 38.004 |
| Uncached chunks, reused connection (ms; n=9) | 51.536 [38.798, 104.924] | 26.382 [17.963, 33.647] |
| Sequential cold read (MiB/s; n=1) | 6.57 (19.472 s) | 21.24 (6.027 s) |
| Sequential warm read (MiB/s; n=1) | 350.00 (0.366 s) | 484.24 (0.264 s) |
| Flush after installing build-essential (s; n=1) | 443.670 | 147.469 |

Both machines reached an interactive shell within seconds with the base cached.
Foreground flush after a large package installation took minutes on both clouds;
this is the principal performance issue exposed by the run. The installed
compiler remained available after flush, clone, and reattachment from a cleared
cache on both machines.

| Acceptance coverage | Google Cloud | AWS |
| --- | --- | --- |
| Volume library, manifests, cache, image formats, NBD/fio (21 tests) | Passed | Passed |
| CLI Ubuntu image rebuild and chunk reuse (2 tests) | Passed | Passed |
| CLI durability, clone isolation, crash recovery, writer fencing, history (2 tests) | Passed | Passed |
| Node registration, runc package persistence, restart and crash recovery (1 test) | Passed | Passed |
| Bash normal execution, mid-command node kill, twelve process kills (1 test, 3 scenarios) | Passed | Passed |
| NBD, volume, and node tests repeated with cache-disk fixtures | Passed | Passed |
| Timing harness, including compiler persistence after clone | Passed | Passed |

All 27 distinct tests in this selection passed on each cloud. Root checks ran
with the required environment present; these results are not skipped tests.

Raw samples and test exit codes: [Google Cloud](benchmarks/volume-gcp.json)
and [AWS](benchmarks/volume-aws.json).

## Validation commands

On each VM, with the build environment from the installation steps:

```sh
cargo build --workspace --locked
cargo test --locked -p swarmy-volume -p swarmy-cli --no-run --message-format=json
cargo test --locked -p swarmyd --test node --no-run --message-format=json
cargo build --locked -p swarmy-volume --example cloud-chunk-read
cargo test -p swarmy-chaos --test bash --locked --no-run --message-format=json
```

The JSON compiler artifacts supplied the executable paths to a Python runner.
It executed all `swarmy-volume` tests, the CLI `image` and `vol` tests, and
`swarmyd`'s `node` test as root with `--nocapture --test-threads=1`. The NBD/fio
test uses an in-memory object store. Cloud object-store I/O is exercised by
the CLI, node, and bash tests and the timing harness. The attachment tests
were then repeated with explicit `TMPDIR` as described above.
`SWARMY_TEST_CLI` pointed to `/mnt/bench/target/debug/swarmy`.
The bash test used `SWARMY_TEST_IMAGE=base-ubuntu:test` and `--nocapture`; it
covers normal execution, a node killed after writing, and twelve process kills
with two sessions. The node test had already registered that base image.

Measurements used these commands after the acceptance tests finished:

```sh
sudo -E /mnt/bench/target/debug/examples/cloud-chunk-read
cd /mnt/bench/run
sudo -E env PATH="$PATH" python3 ~/swarmy/scripts/benchmarks/volume.py
```

The chunk probe ran before the additional attachment and bash test pass, with
no other test active. The volume measurements ran after that pass completed.
The benchmark's final clone contained the installed compiler on both machines.

Launcher validation used `.dev/env` from `scripts/dev-stack.sh start`:

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

All three passed. The first backing-stack startup did not become ready within
its deadline; a second `scripts/dev-stack.sh start` succeeded, and the tests
then ran with the services enabled. An earlier run without service variables
also passed with the expected integration-test skips. The launcher's 21 volume
library/integration tests additionally passed as root, including NBD/fio and
the ext4, shell-recipe, and OCI-recipe tests. The Python benchmark completed a
local SeaweedFS smoke run; those timings are excluded from the cloud results.

## Follow-up tasks

[Task #32](https://github.com/pachuc/swarmy/issues/32) records the multi-minute
foreground flush as a performance blocker for promptly acknowledging a large
unstaged tool step. It proposes measuring release builds and background staging,
then using bounded concurrent uploads outside the dirty-store mutex with chunk
generations and the existing fenced manifest commit. Background uploads already
exist; their end-to-end performance was not measured here. Kernel availability
and the tested durability paths did not require a design change.

[Task #33](https://github.com/pachuc/swarmy/issues/33) proposes a storage
retention preflight and complete version-aware cleanup. The default GCS soft
delete policy and the limited AWS audit permissions prevent a complete
immediate-deletion guarantee in this run.

## Cleanup

AWS cleanup completed at `2026-09-15T19:12:30Z`; the empty object listing was
rechecked afterward. The temporary instance, its boot volume, and imported key
pair were deleted. The pre-existing bucket `swarmy-bench-815638500196` remains,
as requested; this run used prefix `swarmy-bench-20260915-183551/`.

Provider verification:

```sh
aws ec2 describe-instances --filters Name=tag:Name,Values=swarmy-bench-20260915-183551 \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name}' --output json
# [{"Id":"i-06974b70859a499d2","State":"terminated"}]
aws ec2 describe-volumes --filters Name=tag:Name,Values=swarmy-bench-20260915-183551 \
  --query 'Volumes[].{Id:VolumeId,State:State}'
# []
aws ec2 describe-key-pairs --filters Name=key-name,Values=swarmy-bench-20260915-183551 \
  --query KeyPairs
# []
aws s3api list-objects-v2 --bucket swarmy-bench-815638500196 \
  --prefix swarmy-bench-20260915-183551/ --max-keys 1 --no-paginate \
  --query '{KeyCount:KeyCount,IsTruncated:IsTruncated}'
# {"KeyCount":0,"IsTruncated":false}
```

`aws s3 rm s3://BUCKET/RUN_PREFIX/ --recursive --only-show-errors` succeeded.
The supplied AWS policy denied `s3:GetBucketVersioning` and
`s3:ListBucketVersions`, so noncurrent object versions could not be audited.
The empty current-object listing is verified; it is not a claim about version
history. No new AWS bucket was created.

GCP cleanup completed at `2026-09-15T19:44:14Z`. The VM, both attached disks,
the live bucket, and the temporary HMAC key were deleted. The recursive storage
delete returned exit 2 without an error diagnostic, so deletion was verified
independently: bucket describe returned HTTP 404, and the resource listings
below were empty. HMAC cleanup was completed separately.

```sh
gcloud compute instances list --filter=name=swarmy-bench-20260915-183550 --format=json
# []
gcloud compute disks list --filter=name~swarmy-bench-20260915-183550 --format=json
# []
gcloud storage buckets list --filter=name=swarmy-bench-20260915-183550 --format=json
# []
gcloud storage buckets describe gs://swarmy-bench-20260915-183550
# HTTP 404: bucket not found
gcloud storage hmac update "$HMAC_ACCESS_ID" --deactivate --quiet
gcloud storage hmac delete "$HMAC_ACCESS_ID" --quiet
gcloud storage hmac describe "$HMAC_ACCESS_ID" --format='value(state)'
# DELETED
```

GCS initially applied a seven-day soft-delete policy. Four checkpoint blobs
removed by the bash tests were retained under that policy, totaling **3,022
bytes**. Their recorded hard-delete deadlines range from
`2026-09-22T19:21:51.680Z` to `2026-09-22T19:23:25.732Z`.
[Exact retained-object metadata](benchmarks/gcp-retained-objects.json) includes
all four paths, generations, sizes, and deadlines. Disabling soft delete does
not shorten existing retention; see [Cloud Storage's policy rules](https://docs.cloud.google.com/storage/docs/soft-delete#soft-delete-policies).

Soft delete was disabled before final cleanup, preventing the live chunk and
manifest data from entering that retention window. Querying retained metadata
required briefly re-enabling the policy, followed by disabling it again and
allowing its propagation before final deletion. Future runs should create the
temporary bucket with `--soft-delete-duration=0` before any tests, and verify
that policy. The four earlier retained objects are an explicit cleanup
exception, not claimed as permanently removed.

## 2026-09-15: instrumented release flush and tool-boundary budget

This run measures the existing serial upload strategy before changing its
concurrency or dirty-store locking. The benchmark now selects binaries explicitly,
measures installation separately from freeze acquisition and frozen publication,
and can run `--background on`, `off`, or `both`. Each sample has a fresh volume,
base image, cache directory, and object prefix. In particular, an earlier package
installation cannot make a later sample look faster through deduplication.
The base is read into the chunk cache before each installation. After publishing,
the harness clones the volume, clears the cache, and checks `gcc --version`.

### Machines and reproduction

| Setting | AWS | Google Cloud |
| --- | --- | --- |
| Machine | `m6id.xlarge`, 4 vCPUs, 16 GiB | `n2-standard-4`, 4 vCPUs, 16 GiB |
| Zone | `us-east-1a` | `us-east1-b` |
| CPU | Xeon Platinum 8375C, 2.90 GHz | Xeon, family 6/model 85, 2.80 GHz |
| Image | Ubuntu 24.04, `ami-025d99823a4caad37` | `ubuntu-2404-lts-amd64`, `ubuntu-os-cloud` |
| Kernel | `7.0.0-1012-aws` | `7.0.0-1011-gcp` |
| Boot disk | 50 GiB gp3, auto-delete | 50 GB persistent disk, auto-delete |
| Cache disk | 220.7 GiB local NVMe, `/dev/nvme0n1` | 375 GiB local NVMe, `/dev/nvme0n1` |
| Object storage location | S3 `us-east-1` | GCS `us-central1`, XML S3-compatible API |

Local SSD capacity failed in `us-central1-a`, `-b`, `-c`, and `-f` with
`ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS`. The next attempt, `us-east1-b`,
succeeded with `--local-ssd interface=nvme`; no persistent-SSD fallback was used.
The GCP VM and bucket are therefore in different regions. These results describe
that placement, not same-region GCS performance. The earlier GCP run also used a
different cache medium. Both stock kernel modules loaded successfully.

Provision with a fresh run name and a temporary SSH key. AWS uses the private
IP and the supplied subnet/security group. Import the public key with
`managed-by=codex-launcher` and `Name` tags, and pass those same tags for both
`instance` and `volume` in `run-instances`. The creation options for this run
were equivalent to:

```sh
ami=$(aws ssm get-parameter \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query Parameter.Value --output text)
aws ec2 run-instances --image-id "$ami" --instance-type m6id.xlarge \
  --subnet-id "$SWARMY_BENCH_SUBNET" --security-group-ids "$SWARMY_BENCH_SECURITY_GROUP" \
  --key-name "$run_name" \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":50,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
  --tag-specifications \
  "ResourceType=instance,Tags=[{Key=managed-by,Value=codex-launcher},{Key=Name,Value=$run_name}]" \
  "ResourceType=volume,Tags=[{Key=managed-by,Value=codex-launcher},{Key=Name,Value=$run_name}]"
gcloud compute instances create "$run_name" --zone "$zone" \
  --machine-type n2-standard-4 --image-family ubuntu-2404-lts-amd64 \
  --image-project ubuntu-os-cloud --boot-disk-size 50GB \
  --local-ssd interface=nvme --metadata-from-file ssh-keys="$ssh_metadata_file"
```

The SSH metadata file contains `ubuntu:` followed by the temporary public key.
Try the central zones listed above, then east/west zones until local SSD is
available; only fall back to an auto-deleting `pd-ssd` if those attempts fail.
Record the successful zone and actual disk before formatting it. The run name
in the evidence below is `swarmy-flush-20260915-205911`.

AWS settings used the existing bucket with run prefix
`swarmy-flush-20260915-205911/`. GCP used a temporary bucket of that name and a
temporary HMAC key. Both clients use path-style requests. The harness adds a
unique `sample-...` suffix to the configured bucket/prefix for each trial.
Create and verify the GCP policy **before** writing any data:

```sh
gcloud storage buckets create gs://swarmy-flush-20260915-205911 \
  --location us-central1 --soft-delete-duration=0
gcloud storage buckets describe gs://swarmy-flush-20260915-205911 --format=json
# soft_delete_policy: {"retentionDurationSeconds": "0"}
```

Create a temporary GCP HMAC key for the S3-compatible API. Save the response in
an owner-readable private file, then use its `metadata.accessId` and `secret`
for `SWARMY_S3_ACCESS_KEY` and `SWARMY_S3_SECRET_KEY` in the private environment:

```sh
(umask 077; gcloud storage hmac create swarmy@swarmy-508717.iam.gserviceaccount.com \
  --format=json > "$private_hmac_file")
```

Reuse the installation procedure above: dependencies, pinned Rust toolchain,
FoundationDB/NATS/SeaweedFS, and the verified unused cache disk mounted at
`/mnt/bench`. Build as the Ubuntu user. This run used:

```sh
export SWARMY_FDB_LIB_DIR="$HOME/.local/lib"
export CARGO_TARGET_DIR=/mnt/bench/target
cargo build --release --workspace --locked
cargo build --release --locked -p swarmy-volume --example cloud-object-requests
scripts/dev-stack.sh start
. .dev/env
. "$HOME/bench.env" # private cloud S3 settings, never checked in
export TMPDIR=/mnt/bench/tmp
/mnt/bench/target/release/examples/cloud-object-requests > "$HOME/requests.jsonl"
mkdir -p /mnt/bench/run/release
cd /mnt/bench/run/release
sudo -E env TMPDIR="$TMPDIR" python3 "$HOME/swarmy/scripts/benchmarks/volume.py" \
  --target-dir /mnt/bench/target --profile release --background both --install-only \
  > "$HOME/release.jsonl"
```

After release measurements finished on AWS, build and run the debug comparison
on the same machine, with the same cache disk and isolated sample prefix:

```sh
cd "$HOME/swarmy"
cargo build --workspace --locked
cd /mnt/bench/run
mkdir debug
cd debug
sudo -E env TMPDIR="$TMPDIR" python3 "$HOME/swarmy/scripts/benchmarks/volume.py" \
  --target-dir /mnt/bench/target --profile debug --background off --install-only \
  > "$HOME/debug.jsonl"
```

### Counter and timing definitions

All durations in CLI JSON are `{secs, nanos}`. `uploads` counts device activity
between entry to freeze acquisition and completion of thaw. `device_total`
includes earlier background activity and foreground reads on that attachment.
Chunks are successful chunk PUTs, not dirty chunk indices: zeros and existing
objects do not count as uploads. Bytes count successful PUT payloads, including
manifest objects. Requests count attempted HEAD/GET/PUT API calls, including
manifest I/O; internal HTTP retries are not visible at this interface.

Lock wait is summed across dirty-store acquisitions by reads, writes, uploaders,
and publication. Several waiting operations can overlap, so this sum can exceed
wall time. The object-store timer sums HEAD/GET/PUT call durations, excludes GET
body consumption, and includes client processing. The residual is local I/O,
hashing/copying, scheduling, freeze/thaw, and FoundationDB work; it is not a direct
CPU measurement. Frozen time starts after the freeze command succeeds and ends
after thaw; freeze acquisition includes kernel writeback. CLI round-trip time
also includes CLI startup and control transport.

### Results and interpretation

Each installation mode has one sample. These runs establish a baseline, not
percentiles or scaling guarantees. The raw measurements, image build records,
request samples, and teardown queries are in
[AWS JSON](benchmarks/volume-flush-20260915-aws.json) and
[GCP JSON](benchmarks/volume-flush-20260915-gcp.json). Installation time excludes
`apt-get update`, which is reported separately. The last timing column adds
update, installation, and CLI flush; it excludes image construction, cache
warming, auxiliary mount setup/teardown, and the subsequent persistence check.

| Cloud / build / background | apt update (s) | Install (s) | Freeze acquisition (s) | Frozen (s) | CLI flush (s) | Update + install + flush (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| AWS / release / off | 0.515 | 11.339 | 0.096 | 140.237 | 140.366 | 152.220 |
| AWS / release / on | 1.617 | 181.526 | 1.568 | 5.174 | 6.776 | 189.919 |
| AWS / debug / off | 1.216 | 11.387 | 0.118 | 139.222 | 139.371 | 151.974 |
| GCP / release / off | 0.715 | 15.296 | 0.111 | 889.606 | 889.749 | 905.760 |
| GCP / release / on | 3.370 | 1233.974 | 9.532 | 22.422 | 31.982 | 1269.326 |

| Cloud / build / background | Scope | Chunks uploaded | Storage requests | Bytes uploaded | Lock wait (s) | Storage call time (s) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| AWS / release / off | flush | 2,238 | 4,485 | 586,940,683 | 0.000041 | 135.082 |
| AWS / release / off | attachment | 2,238 | 4,489 | 586,940,683 | 0.004162 | 135.159 |
| AWS / release / on | flush | 88 | 188 | 23,331,083 | 6.460992 | 6.491 |
| AWS / release / on | attachment | 2,731 | 5,488 | 716,177,675 | 178.760159 | 171.806 |
| AWS / debug / off | flush | 2,239 | 4,487 | 587,202,827 | 0.000304 | 133.879 |
| AWS / debug / off | attachment | 2,239 | 4,491 | 587,202,827 | 0.026461 | 133.990 |
| GCP / release / off | flush | 2,238 | 4,485 | 586,940,683 | 0.000050 | 884.333 |
| GCP / release / off | attachment | 2,238 | 4,489 | 586,940,683 | 0.004620 | 884.659 |
| GCP / release / on | flush | 76 | 164 | 20,185,355 | 31.682961 | 31.572 |
| GCP / release / on | attachment | 3,138 | 6,322 | 822,870,283 | 1256.260064 | 1248.562 |

| Request | AWS (ms) | GCP (ms) |
| --- | ---: | ---: |
| head_missing | 11.355 [8.566, 13.469] | 280.716 [175.582, 292.957] |
| put_256k | 38.673 [29.406, 97.040] | 245.760 [223.870, 288.195] |
| head_existing | 11.887 [9.417, 19.507] | 72.737 [63.439, 85.490] |

Request timings are median [minimum, maximum] over 19 serial requests using a
reused client. Sample zero is retained in the raw data but excluded here; its
missing HEAD took 30.383 ms on AWS and 191.129 ms on GCP, including connection
setup. Each new 256 KiB object requires a missing HEAD and a conditional PUT.
The sums of the medians are 50.028 ms on AWS and 526.476 ms on GCP, implying
serial rates of approximately 5.0 and 0.475 MiB/s before local work and metadata.
This short probe is not a stable latency guarantee: the longer flushes achieved
about 4.0 and 0.63 MiB/s respectively. Request latency varied between the probe
and the sustained upload. GET/manifest calls are also included in flush totals.

**Release does not remove the serial network cost.** AWS uploaded almost the
same amount in release and debug: 2,238 versus 2,239 chunks. Frozen time was
140.237 s versus 139.222 s, a 0.7% difference in the opposite direction to an
assumed release improvement. Storage call time was 135.082 s versus 133.879 s;
subtracting it from server-side elapsed time leaves about 5.25 s versus 5.46 s.
These single samples do not resolve a build-mode effect beyond request
variability. The workspace
already sets `[profile.dev.package."*"].opt-level = 3`, so dependency code,
including blake3, is optimized in both builds.

GCP release/off uploaded exactly the same number of chunks and bytes as AWS
release/off, but froze for 889.606 s, with 884.333 s in storage calls. The
server-side elapsed time outside measured storage calls was about 5.38 s. This
is not a controlled comparison to the earlier 443.670 s GCP debug run: the VM moved from the central region to
the east region to obtain local SSD, while the bucket stayed in the central
region; the cache disk and request latencies also differ. The data does not
support attributing that regression to release mode or to local SSD.

**Background uploading moves work into the installation and can repeat it.**
On AWS it reduced frozen time to 5.174 s and CLI flush to 6.776 s, but installation
rose to 181.526 s from 11.339 s. The full measured update/install/flush sum rose
from 152.220 s to 189.919 s. The attachment uploaded 2,731 chunks instead of 2,238
and accumulated 178.760 s of dirty-lock waiting. The existing uploader keeps the
mutex while waiting on the object store, so a short freeze is not enough to
claim a faster tool boundary. The counter records aggregate waiting, including
background waiters; it cannot by itself assign every second to one caller.

GCP showed the same tradeoff more strongly: frozen time fell to 22.422 s, but
freeze acquisition added 9.532 s and CLI flush took 31.982 s. Installation rose
to 1,233.974 s; the measured update/install/flush sum rose from 905.760 s to
1,269.326 s. The attachment uploaded 3,138 chunks, 900 more than background-off,
and accumulated 1,256.260 s of dirty-lock waiting. Both clouds retained the
installed compiler after flush, clone, cache clearing, and reattachment.

The [proposed budget in the design](DESIGN.md#74-proposed-tool-boundary-latency-budget)
uses the measured request costs, a 32-upload concurrency assumption, conservative
planning throughput, and explicit metadata/freeze allowances. It targets both
unstaged completion latency and avoidance of installation regressions. Neither
parallel throughput nor p95 compliance is established by these measurements;
those are acceptance work for the following tasks. The upload loop in this PR
remains serial and retains its existing locking and durability behavior.

### Validation on the launcher

With the dev stack running and `.dev/env` sourced:

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p swarmy-volume --locked --no-run --message-format=json
cargo test -p swarmy-cli --test vol --locked --no-run --message-format=json
cargo test --workspace --locked --no-run --message-format=json
```

The three CI commands passed; the workspace run reported 173 passing tests.
The JSON compiler artifacts supplied executable paths to a runner which invoked
each volume test binary, the CLI `vol` and `image` binaries, the node test,
and the bash chaos test with
`sudo -E BINARY --nocapture --test-threads=1`. All 28 distinct tests passed as
root, including NBD/fio, publication counters, ext4, shell and OCI recipes,
durability, clone isolation, crash recovery, fencing, history, and CLI JSON
counter/timing assertions. The additional node and bash tests used
`SWARMY_TEST_CLI=/home/ubuntu/workspace/target/debug/swarmy` and
`SWARMY_TEST_IMAGE=base-ubuntu:test`; normal execution, a mid-command node kill,
and twelve process kills all passed. The OCI recipe test initially skipped
because `skopeo` was absent; after installing `skopeo` and `umoci`, all five image tests
were repeated successfully. No NBD device remained attached afterward.

The initial workspace test run failed the existing doctor mock-tools test when
`SWARMY_FDB_LIB_DIR=$HOME/.local/lib` made the compiled tool search path prefer
real tools over its fake HOME. Installing the client library in
`/usr/local/lib`, running `ldconfig`, and rebuilding without that override
resolved the environment conflict. No test, lint, or CI setting was weakened.
Python argument parsing and syntax checks passed; the actual cloud runs exercise
the benchmark and its cleanup paths. No cloud backend or upload strategy changed.

### Cleanup proof

AWS cleanup was verified at `2026-09-15T21:29:21Z`, immediately after its debug
comparison. The instance, boot volume, and imported key pair were removed. The
pre-existing bucket remains. Current objects, versions, and delete markers under
this run's prefix are all absent. Unlike the earlier run, the supplied policy
permitted the version-history audit here.

```sh
aws ec2 describe-instances --filters Name=tag:Name,Values=swarmy-flush-20260915-205911 \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name}' --output json
# [{"Id":"i-0b1dd2396ec6f2bcd","State":"terminated"}]
aws ec2 describe-volumes --filters Name=tag:Name,Values=swarmy-flush-20260915-205911 \
  --query 'Volumes[].{Id:VolumeId,State:State}' --output json
# []
aws ec2 describe-key-pairs --filters Name=key-name,Values=swarmy-flush-20260915-205911 \
  --query KeyPairs --output json
# []
aws s3api get-bucket-versioning --bucket swarmy-bench-815638500196 \
  --query '{Status:Status,MFADelete:MFADelete}' --output json
# {"Status":null,"MFADelete":null}
aws s3api list-object-versions --bucket swarmy-bench-815638500196 \
  --prefix swarmy-flush-20260915-205911/ --max-keys 1 --no-paginate \
  --query '{Versions:length(Versions || `[]`),DeleteMarkers:length(DeleteMarkers || `[]`),IsTruncated:IsTruncated}' \
  --output json
# {"Versions":0,"DeleteMarkers":0,"IsTruncated":false}
aws s3api list-objects-v2 --bucket swarmy-bench-815638500196 \
  --prefix swarmy-flush-20260915-205911/ --max-keys 1 --no-paginate \
  --query '{KeyCount:KeyCount,IsTruncated:IsTruncated}' --output json
# {"KeyCount":0,"IsTruncated":false}
```

Cleanup commands were `aws ec2 terminate-instances`, `aws ec2 delete-key-pair`,
and `aws s3 rm s3://BUCKET/swarmy-flush-20260915-205911/ --recursive --only-show-errors`.
All returned zero, as did the provider verification commands above.

GCP cleanup was verified at `2026-09-15T21:59:35Z`. Instance deletion, recursive
bucket deletion, and HMAC deactivation/deletion all returned zero. Soft delete
was disabled from bucket creation onward, so this run did not create the retained
objects seen in the earlier run. The live bucket no longer exists:

```sh
gcloud compute instances list --filter=name=swarmy-flush-20260915-205911 --format=json
# []
gcloud compute disks list --filter=name~swarmy-flush-20260915-205911 --format=json
# []
gcloud storage buckets list --filter=name=swarmy-flush-20260915-205911 --format=json
# []
gcloud storage buckets describe gs://swarmy-flush-20260915-205911
# not found: 404 (exit 1, as expected)
gcloud storage hmac describe "$HMAC_ACCESS_ID" --format='value(state)'
# DELETED
```

The cleanup commands were:

```sh
gcloud compute instances delete swarmy-flush-20260915-205911 --zone us-east1-b --quiet
gcloud storage rm --recursive gs://swarmy-flush-20260915-205911 --quiet
gcloud storage hmac update "$HMAC_ACCESS_ID" --deactivate --quiet
gcloud storage hmac delete "$HMAC_ACCESS_ID" --quiet
```


## 2026-09-15 Generation-aware background uploads

This run measures the generation-aware uploader in this PR: 256 KiB chunks,
32 concurrent uploads, and a 250 ms quiet period before background staging.
Chunk copies and generation checks hold the dirty-store lock; remote uploads
do not. Flush waits for the active batch and uploads only the unstaged tail.
The single-writer lease and fenced FoundationDB publication are unchanged.

### Controlled release comparison

Both clouds used the same release binaries, built on the Ubuntu 24.04 launcher
with `cargo build --release --workspace --locked` and Rust 1.98.1, then copied
to the benchmark machines. The `swarmy-session` SHA-256 was
`0b0c95045e9aaf4bff1b7840ea78d14a8cb657679aad239f68cba10797cca5a4`.
Each cloud ran two samples per mode on one machine, in off/on/off/on order.
All four samples within each cloud produced the same base-image root hash.
Every sample used a fresh object prefix, the same 8 GiB Noble recipe without
build-essential, and a fully warmed base chunk cache. Image preparation and
teardown are outside the timers. Compiler persistence passed after all eight
flush/clone/cache-clear/reattach trials.

| Placement | AWS | Google Cloud |
| --- | --- | --- |
| Machine | `m6id.xlarge`, 4 vCPUs, 16 GiB | `n2-standard-4`, 4 vCPUs, 16 GiB |
| VM zone / bucket region | `us-east-1a` / `us-east-1` | `us-east1-b` / `us-east1` |
| Cache | 220.7 GiB local NVMe | 375 GiB NVMe local SSD |
| Boot disk | 50 GiB gp3 | 50 GB persistent disk |
| Ubuntu 24.04 image | `ami-025d99823a4caad37` | `ubuntu-2404-noble-amd64-v20260906` |
| Kernel | `7.0.0-1012-aws` | `7.0.0-1011-gcp` |

GCP used the previously available east-region placement and obtained local SSD
in the first requested zone. No persistent-SSD fallback was needed. Its bucket
was created in **us-east1**, with soft delete disabled atomically at creation;
the effective policy was checked before any write. The cloud helper now limits
VM capacity attempts and any fallback to the bucket region, preventing the
cross-region placement in the earlier instrumented run. Both clouds loaded
`nbd` and `ublk_drv` on their stock kernels; these measurements use NBD.

The machines used the installation procedure above. Backing-service binaries
and the FoundationDB client library were copied from the launcher alongside
the checkout and release binaries. Each VM ran its own FoundationDB and NATS.
GCP used a temporary HMAC key with the XML/S3 API and path-style URLs; AWS used
the existing bucket under a generated run prefix. After sourcing the dev stack
and private cloud settings, the command on each machine was:

```sh
sudo -E TMPDIR=/mnt/bench/tmp python3 ../repo/scripts/benchmarks/volume.py \
  --target-dir /mnt/bench/target --profile release --background both \
  --install-only --trials 2
```

Raw samples: [AWS](benchmarks/2026-09-15-generations-aws.jsonl) and
[GCP](benchmarks/2026-09-15-generations-gcp.jsonl). Times below are seconds.
Total is apt update + package installation + CLI flush. Freeze wait includes
kernel writeback; frozen time starts after freeze acquisition and ends after
thaw. Frozen chunks count successful chunk PUTs completed in that window,
including background requests already in flight when the filesystem froze.

| Cloud | Background | Sample | Apt update | Install | CLI flush | Total | Freeze wait | Frozen | Frozen chunks |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| AWS | off | 1 | 1.166 | 9.634 | 9.289 | 20.090 | 0.107 | 9.149 | 2,238 |
| AWS | on | 1 | 1.617 | 13.743 | 0.991 | 16.351 | 0.110 | 0.848 | 187 |
| AWS | off | 2 | 1.617 | 10.537 | 9.845 | 21.998 | 0.108 | 9.704 | 2,238 |
| AWS | on | 2 | 1.617 | 13.842 | 1.243 | 16.702 | 0.118 | 1.091 | 230 |
| GCP | off | 1 | 1.468 | 17.154 | 8.925 | 27.546 | 0.152 | 8.730 | 2,239 |
| GCP | on | 1 | 2.670 | 17.355 | 1.536 | 21.561 | 0.152 | 1.344 | 177 |
| GCP | off | 2 | 1.467 | 17.355 | 8.609 | 27.430 | 0.174 | 8.398 | 2,238 |
| GCP | on | 2 | 1.519 | 19.117 | 1.101 | 21.737 | 0.229 | 0.834 | 149 |

The counters below cover the entire writing attachment, including background
work. Uploaded bytes include manifest objects. Referenced bytes count nonzero
dirty chunk coverage in the published manifest, including deduplicated chunks.
Amplification is successful chunk PUT bytes (`chunks_uploaded * 262144`)
divided by referenced chunk bytes, excluding manifest metadata from both sides.
It can be slightly below one because of deduplication. Lock wait is summed over
all dirty-lock acquisitions, including parallel uploaders waiting to copy local
bytes; it is not the wall time for which sandbox writes were stalled.

| Cloud | Background | Sample | Chunk PUTs | Uploaded bytes | Referenced chunk bytes | Amplification | Lock wait (s) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| AWS | off | 1 | 2,238 | 586,940,683 | 586,940,416 | 0.9996 | 8.770 |
| AWS | on | 1 | 2,466 | 646,709,515 | 586,940,416 | 1.1014 | 12.132 |
| AWS | off | 2 | 2,238 | 586,940,683 | 586,940,416 | 0.9996 | 8.527 |
| AWS | on | 2 | 2,450 | 642,515,211 | 587,202,560 | 1.0938 | 11.247 |
| GCP | off | 1 | 2,239 | 587,202,827 | 587,202,560 | 0.9996 | 11.399 |
| GCP | on | 1 | 2,494 | 654,049,547 | 586,940,416 | 1.1139 | 17.604 |
| GCP | off | 2 | 2,238 | 586,940,683 | 586,940,416 | 0.9996 | 11.273 |
| GCP | on | 2 | 2,529 | 663,224,587 | 586,940,416 | 1.1295 | 18.035 |

**Both measured pairs on both clouds improve total step-plus-flush time.** AWS
averaged 21.044 s off versus 16.526 s on (21.5% lower), while average frozen time
fell from 9.427 s to 0.970 s. Installation rose from 10.086 s to 13.793 s, so the
improvement is smaller than frozen time alone suggests. GCP averaged 27.488 s
off versus 21.649 s on (21.2% lower); average frozen time fell from 8.564 s to
1.089 s, while installation rose from 17.254 s to 18.236 s. Background upload
amplification was 1.094–1.101 on AWS and 1.114–1.130 on GCP. The debounce limits
repeated uploads but does not eliminate them.

The workload changed about 560 MiB of chunk coverage, placing it in the design's
up-to-1-GiB budget: 12 s on AWS and 90 s on GCP. Server-side added latency
(freeze acquisition + publication + thaw) was 9.256–9.812 s off and
0.958–1.209 s on for AWS; GCP was 8.571–8.881 s off and 1.063–1.496 s on.
**The measured workload meets both the unstaged and background latency limits
and the total-time nonregression rule.** Two samples per mode do not establish
p95 compliance or validate the other changed-data rows in the budget. GCP is
now colocated, so its improvement over the earlier cross-region run cannot be
attributed solely to the dirty-lock change.

### Cleanup proof for this run

[Provider queries and lifecycle audits](benchmarks/2026-09-15-generations-cleanup.json)
record full selected outputs and UTC verification times. Both machines are gone;
no benchmark disks, key pairs, objects, retained versions, or incomplete uploads
remain. The existing AWS bucket remains. The temporary GCP bucket and HMAC key
were deleted. The storage-only preflight runs also passed their deletion audits.

An initial GCP setup attempt used
`swarmy-bench-a0029e5a7de34c7cb74f5b20b3def741`. Its disk detector did not recognize
the local SSD model `nvme_card`, so it stopped before formatting or benchmarking.
The lifecycle wrapper deleted that VM, its disk, and its empty bucket; the retry
used the verified local SSD model and completed all four samples. The queries
below include both GCP attempts. No HMAC key was created for the failed attempt.

After fetching results, run objects were bulk-deleted with
`aws s3 rm s3://BUCKET/RUN_PREFIX/ --recursive --only-show-errors` and
`gcloud storage rm --recursive gs://BUCKET/RUN_PREFIX/ --quiet`.
The lifecycle parent was paused briefly during this bulk deletion to avoid
starting its serial per-object deletion loop concurrently, then resumed to
terminate compute and perform its independent version and retention audits.
All bulk-delete and final audit commands returned zero. GCP's bucket metadata
remained at metageneration 1 with soft delete disabled from creation through
deletion. The HMAC key was deactivated, deleted, and independently read back as
`DELETED` before the VM's lifecycle cleanup finished.

AWS verification:

```sh
aws ec2 describe-instances \
  --filters Name=tag:Name,Values=swarmy-bench-8647b4d4bdec46e485a6ac2ab01797a6 \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name}' --output json
# [{"Id":"i-0e5ab9e224d7dfacc","State":"terminated"}]
aws ec2 describe-volumes \
  --filters Name=tag:Name,Values=swarmy-bench-8647b4d4bdec46e485a6ac2ab01797a6 \
  --query 'Volumes[].{Id:VolumeId,State:State}' --output json
# []
aws ec2 describe-key-pairs \
  --filters Name=key-name,Values=swarmy-bench-8647b4d4bdec46e485a6ac2ab01797a6 \
  --query KeyPairs --output json
# []
aws s3api list-objects-v2 --bucket swarmy-bench-815638500196 \
  --prefix swarmy-bench-8647b4d4bdec46e485a6ac2ab01797a6/ --max-keys 1 --no-paginate \
  --query '{KeyCount:KeyCount,IsTruncated:IsTruncated}' --output json
# {"KeyCount":0,"IsTruncated":false}
aws s3api list-object-versions --bucket swarmy-bench-815638500196 \
  --prefix swarmy-bench-8647b4d4bdec46e485a6ac2ab01797a6/ --max-keys 1 --no-paginate \
  --query '{Versions:length(Versions || `[]`),DeleteMarkers:length(DeleteMarkers || `[]`),IsTruncated:IsTruncated}' \
  --output json
# {"Versions":0,"DeleteMarkers":0,"IsTruncated":false}
aws s3api list-multipart-uploads --bucket swarmy-bench-815638500196 \
  --prefix swarmy-bench-8647b4d4bdec46e485a6ac2ab01797a6/ --query Uploads --output json
# null
```

GCP verification:

```sh
gcloud compute instances list \
  --filter='name=(swarmy-bench-a0029e5a7de34c7cb74f5b20b3def741 swarmy-bench-6226f3bce8b447fd9546e8fcaa658823)' --format=json
# []
gcloud compute disks list \
  --filter='name=(swarmy-bench-a0029e5a7de34c7cb74f5b20b3def741 swarmy-bench-6226f3bce8b447fd9546e8fcaa658823)' --format=json
# []
gcloud storage buckets list \
  --filter='name=(swarmy-bench-a0029e5a7de34c7cb74f5b20b3def741 swarmy-bench-6226f3bce8b447fd9546e8fcaa658823)' --format=json
# []
gcloud storage buckets describe gs://swarmy-bench-a0029e5a7de34c7cb74f5b20b3def741 --format=json
# 404 not found (exit 1, expected)
gcloud storage buckets describe gs://swarmy-bench-6226f3bce8b447fd9546e8fcaa658823 --format=json
# 404 not found (exit 1, expected)
```

The HMAC verification used authenticated JSON API GET
`storage/v1/projects/PROJECT/hmacKeys/ACCESS_ID`; only its `state: DELETED` result
is retained in the evidence file. Authentication material is not recorded.

### Launcher validation for this run

With the dev stack running and `.dev/env` sourced, these commands passed:

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --release --workspace --locked
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/benchmarks -p 'test_*.py' -v
cargo test --workspace --locked --no-run --message-format=json
```

The workspace run passed 182 tests; the Python suite passed 19. The JSON compiler
artifacts supplied executable paths to a temporary driver invoked as
`python3 /tmp/swarmy-root-tests.py`. It selected all five swarmy-volume artifacts
(library, device, image, nbd, volume), CLI image and vol, swarmyd node, and chaos
bash. Each ran with `sudo -E BINARY --nocapture --test-threads=1`, after compilation
as the ordinary user. The driver set
`SWARMY_TEST_CLI=/home/ubuntu/workspace/target/debug/swarmy` and
`SWARMY_TEST_IMAGE=base-ubuntu:test`. All 37 root tests passed with no skips,
including OCI image import, ext4/fio/NBD, clone/durability/fencing/history,
node recovery, and bash normal execution, a mid-command node kill, and twelve
seeded process kills. Every NBD device was detached and unmounted afterward.

The new deterministic upload tests block a PUT while overwriting its chunk,
verify the write completes before that PUT is released, discard the old hash,
and read the newer contents through the published manifest. Other tests cover
failed uploads and selective retries, debounce, shared concurrency bounds,
writer-lease revocation during a blocked flush with FoundationDB, and a child
process exiting after chunk and manifest object uploads but before the head
commit. Reopening the local overlay preserves dirty bytes; a fresh attachment
from the authoritative baseline sees only the last publication.

The first workspace run failed with NATS connection refusals because the backing
processes had stopped; restarting the stack and rerunning passed. Root acceptance
was kept separate from the dev lifecycle test that restarts shared services.
Clippy initially required the generation-exhaustion panic to be documented; the
final run passes without lint allowances. No CI, lint, or test requirement was
weakened. The only unverified performance claim is p95 compliance across the full
budget table, which needs more samples and workloads than this installation run.

## 2026-09-16 persistent computers

### Procedure and scope

The persistent chaos scenario is `swarmy-chaos --persistent --image
base-ubuntu:persistent --sessions 2 --gateways 1 --kills 0`. Build without root,
then run the prebuilt executable with `sudo -E`, `--no-start-stack`, and
`--bin-dir` pointing at the built services. `scripts/test-bash.sh` also runs this
scenario. It creates two sessions with one agent identity and sends each tool
request through a scripted inference gateway, scheduler, and worker. It does
not bypass routing by sending tool jobs directly to a node.

For AWS, `cloud.py --vm --nodes 2` provisions two stock Ubuntu 24.04
`m6id.xlarge` machines in the supplied `us-east-1` subnet and security group.
[persistent-cloud.py](../scripts/benchmarks/persistent-cloud.py) installs the
prebuilt debug binaries, loads the stock NBD module, and places working data
and chunk caches on each machine's local NVMe instance store. FoundationDB,
NATS, workers, scheduler, and gateway run on the primary; `swarmyd` runs on both.
The supplied security group admits only SSH between instances. Two private SSH
reverse forwards carry FoundationDB and NATS to the second node; neither
service is exposed publicly and no security-group rules are changed. S3 uses a
fresh prefix in the existing regional bucket. Google Cloud is optional
for this task and is not used in this run.

The test uses a three-second idle window and matching three-second placement
leases to make eviction observable without waiting thirty minutes. The volume
writer lease remains sixty seconds. The managed HTTP server must survive
another session and a wait longer than the idle window. The failure path waits
for both authorities to expire before issuing a new call. Its total recovery
latency therefore includes a different cost from the rehydration measurements.

The remote kill hook starts a checkpoint while a separate process writes and
fsyncs a probe file. It observes that process blocked in the kernel's filesystem
freeze wait, sends `SIGSTOP` to `swarmyd` to hold publication at that point, then
terminates the EC2 instance. The head must still equal the acknowledged
checkpoint from before the fault. The replacement must recover that
checkpoint's files, lose the uncheckpointed marker and HTTP server, move to the
other node with a higher epoch, and deliver exactly one system notice with the
head manifest's exact timestamp. Idle eviction is checked separately, including
writer release, final-checkpoint contents, and the distinct eviction notice.

Rehydration has two cold and two warm samples. Each starts after idle eviction
has released the computer. Cold trials remove the target chunk cache; warm
trials reuse it. Both sync and drop the host page cache before timing. The timer
covers gateway restart, two scripted inference responses, worker routing,
container creation, filesystem access, and return to an idle session. It is a
routed request measurement, not just mount latency. Cloud-side caches and the
FoundationDB cache are not controlled. Two samples do not establish a p95.

The separate [volume workload](../scripts/benchmarks/persistent-volume.py) runs
twice. Each sample overwrites an 8 MiB random file through thirteen explicit
`swarmy vol checkpoint` calls, then measures a checkpoint with a process writing
and fsyncing monotonic timestamps every 10 ms. The reported pause is the largest
observed inter-write gap, including scheduling and fsync costs; raw neighboring
timestamps and observation counts accompany it. The pause checkpoint and final
detach also publish snapshots. Exactly ten manifests must remain retained.
After all writers stop, the isolated collector runs with a one-second grace
window, first dry and then real. Candidate bytes must match deleted bytes.
Every retained manifest is then cloned to a new volume, attached with an empty
chunk cache, mounted, and used to start Bash. Each boot verifies its generation
marker and the SHA-256 digest of its 8 MiB file. This grace setting is specific
to a quiescent test namespace and is not a deployment recommendation.

The reduced CI path in `scripts/chaos-ci.sh` retains seeded service kills and
runs production worker tests for failure/eviction routing and durable notices,
plus the real subprocess registry test. These checks require no root. They
exercise the constituent contracts separately; they do not claim NBD, container,
or physical-machine coverage. The root bash acceptance runs the integrated
persistent scenarios. The CI workflow itself is unchanged.

### Design finding

An initial local trial used a three-second node placement lease with the
worker's default thirty-second grant. The hosting actor's first renewal tried
to shorten the expiry and the store rejected it. The node stopped serving and
the following tool failed after recovery. Matching the durations allowed the
scenario to pass. A follow-up task in the pull request proposes monotonic
hosting renewal across differing durations, with regression tests for both
initial handoff and configuration changes. The store's rejection is correct;
its fencing rule should stay intact.

The cloud collector uncovered a second issue: passing `bucket/run-prefix` as
an S3 bucket produced a listing request to `/bucket/run-prefix?list-type=2`
and S3 returned `404 NoSuchKey`. Individual object reads and writes had worked,
which is why earlier disk benchmarks did not detect this. The blob-store
constructor used by the collector now separates the physical bucket and uses
`PrefixStore` for its namespace. This preserves existing object locations while
making listings and deletions operate on relative chunk paths. A regression
test checks scoped listing, reads, deletion, and preservation of a sibling key.
The follow-up task is to centralize all S3 constructors and expose a validated
prefix explicitly instead of relying on a slash inside the bucket setting.

### Measurements and scenario results

The successful run used two `m6id.xlarge` instances with local NVMe, the stock
Ubuntu 24.04 image, kernel `7.0.0-1012-aws`, four vCPUs and 16 GiB class memory.
The binaries were debug builds from Rust 1.98.1. These are correctness and
small-sample latency measurements, not release-build performance limits.
The run began at 06:00:46 UTC. Full sample values, manifest IDs, generation
hashes, collector counters, and each retained boot are in
[the raw AWS samples](benchmarks/2026-09-16-persistent-aws.json).

| Measurement | Sample 1 | Sample 2 | Count |
| --- | ---: | ---: | ---: |
| Cold routed rehydration, seconds | 2.004998114 | 1.835473571 | 2 |
| Warm routed rehydration, seconds | 0.796242961 | 0.827149000 | 2 |
| Writer maximum gap, milliseconds | 327.948067 | 327.457828 | 2 checkpoints |
| Writer timestamp observations | 179 | 176 | 355 total |
| Checkpoint frozen interval, milliseconds | 304.563129 | 312.518404 | 2 |
| Collector dry-run wall time, seconds | 5.077414065 | 10.912728678 | 2 |
| Collector real-run wall time, seconds | 14.899932086 | 15.085755906 | 2 |
| Collector durable duration, milliseconds | 14871 | 15051 | 2 |
| Collector bytes freed | 118226944 | 50069504 | 168296448 total |
| Collector deleted chunks | 451 | 191 | 642 total |
| Retained snapshots booted after collection | 10 of 10 | 10 of 10 | 20 |

The collector scanned 2,825 and 2,991 objects and marked 19 and 49 manifests.
The second run includes the first run's retained volumes and boot clones.
The first run can also collect chunks orphaned by the interrupted snapshot;
its freed bytes should not be attributed solely to pruning. Both dry runs
freed zero bytes and predicted exactly the real run's deleted byte count.
Each volume published fifteen snapshots and retained ten, pruning the first
five recorded generations. Each of the twenty retained manifests booted from
an empty cache after collection and matched its expected generation and SHA-256.

The raw timestamps around the largest writer gaps, in monotonic nanoseconds,
were `[462085140602, 462096682918, 462109702560, 462437650627, 462449199478,
462460605744]` and `[536664573663, 536676547810, 536687967381, 537015425209,
537026818883, 537038172328]`. The test measured a lightweight timestamp writer
after the 8 MiB file checkpoint; it does not represent installation-heavy
snapshot pauses.

All five requested scenarios passed. Both gateway sessions saw each other's
files and the same running HTTP server. The server prevented idle eviction.
The remote writer then blocked in `percpu_rwsem_wait`, the hosting process was
stopped, and EC2 termination destroyed that machine. The authoritative head
remained `01M2MCW6CEXVDR7VYHAEED8VVG`. Recovery on the other node found the
checkpointed files, lost the uncheckpointed marker and server, and appended
exactly one rebuild notice with snapshot time `2026-09-16T06:01:14.382Z`. The separate
idle-eviction path released its writer, checkpointed its last file, and supplied
the distinct eviction notice on the next routed call. Recovery after the
termination request returned took 56.090676922 seconds, including lease expiry
and the file/process/notice checks; it is separate from the cold/warm timings.

### Reproduction and validation

Install the provider CLI with `sudo snap install aws-cli --classic`, build as
the ordinary user, and invoke the existing lifecycle controller. The workload
installs `runc`, `e2fsprogs`, `debootstrap`, `curl`, `python3`, and `nbd-client` on
the fresh machines and uses their stock NBD module. It does not need a separate
`linux-modules-extra` package. Credentials are loaded from the private file and
transferred only through SSH into private temporary files.

```sh
cargo build --workspace --locked
set -a
. ~/.config/swarmy-bench/aws.env
set +a
export SWARMY_BENCH_SSH_KEY=/tmp/swarmy-persistent-key
export SWARMY_BENCH_RESULTS=/tmp/swarmy-persistent-results-5
python3 scripts/benchmarks/cloud.py run --provider aws --delete-versions \
  --vm --nodes 2 --ssh-public-key /tmp/swarmy-persistent-key.pub \
  --workload scripts/benchmarks/persistent-cloud.py \
  --state /tmp/swarmy-persistent-aws-5.json
```

Generate a temporary SSH key before the run if necessary. The lifecycle
controller imports a separately tagged key pair for each run, journals both
instances, and tears down compute and object storage even if the workload
fails. The workload copies its raw results back before teardown.

With the dev stack running and `.dev/env` sourced, validation passed:

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/benchmarks -p 'test_*.py' -v
scripts/chaos-ci.sh
cargo test --workspace --locked --no-run --message-format=json
cargo test -p swarmy-store --locked \
  s3_namespace_lists_relative_keys_and_keeps_siblings -- --nocapture
```

The workspace suite passed 214 tests with one pre-existing ignored test. The
Python suite passed 21 tests. The reduced chaos command passed its seeded kill
schedule, both worker failure/eviction tests, and the subprocess registry test.
The S3 namespace regression ran against the development object store; the
collector then exercised the same constructor against real S3 on AWS.

Root tests were compiled as the ordinary user. The compiler JSON supplied
paths for `sudo -E BINARY --nocapture --test-threads=1`, with
`SWARMY_TEST_CLI=/home/ubuntu/workspace/target/debug/swarmy` and
`SWARMY_TEST_IMAGE=base-ubuntu:persistent`. All 40 selected tests passed across
swarmy-volume's library/device/image/NBD/volume artifacts, CLI image/vol,
swarmyd node, and chaos bash. OCI import was rerun successfully after installing
`skopeo` and `umoci`. The root chaos acceptance includes the new persistent
scenario, a mid-command node kill, and twelve seeded service kills.

Initial local runs encountered stopped development services, a five-second
readahead test timeout during concurrent compilation, and an intermittent
existing mid-command chaos completion-count assertion. Restarting the services,
rerunning the workspace without concurrent builds, and rerunning the unchanged
root chaos acceptance passed. Clippy's allocation warning in the new regression
test was fixed. No lint, test, or CI requirement was weakened. NBD attachment
and mount audits after root testing were empty.

### Attempts and teardown evidence

The final run passed in 276.442977211 seconds after image construction. Four
earlier attempts are excluded from the final sample table: bundle creation
failed on a directory-mtime change; installation requested an unavailable
kernel modules-extra package; the remote node could not reach FoundationDB
through the SSH-only security group; and the first complete routing run stopped
at the collector's invalid S3 listing request. The bundle procedure, stock NBD
installation, private SSH forwards, and prefix-aware collector fixed those
failures. Attempt four had passed the routing and interrupted-snapshot checks,
but is not counted as a successful full run.

[Raw provider queries](benchmarks/2026-09-16-persistent-cleanup.json) record an
independent audit started at `2026-09-16T06:06:29Z`. Every listed instance is
`terminated`; every run's tagged volumes and key pairs are empty. All six S3
prefixes have zero current objects and no versions, delete markers, or pending
multipart uploads. The existing bucket remains. No Google Cloud machine,
bucket, or HMAC key was created for this task.

| Attempt | Run name suffix (after `swarmy-bench-`) | Terminated instance IDs |
| --- | --- | --- |
| Storage preflight | `45ef9abfc08a421bb0f687ce5c0108d3` | No compute created |
| 1 | `d7d1da0331b24c98972a4b2043f7fb15` | `i-0bd907e08df00102f`, `i-0f2a494a715f9f397` |
| 2 | `4c51eadbbc834e74a49d0bee37c8fb8f` | `i-0b2c57fbf8a5ce0e1`, `i-004183d474de32ce0` |
| 3 | `9eb83331696b4239b45a60cf1a17f16b` | `i-084e8c1ae01dfd5e6`, `i-0451d92f867fe9692` |
| 4 | `7579ebdf38b044e0ad9b9bdbb0dcc12e` | `i-00eb5b17b4b134603`, `i-0d65a10f025afa4f0` |
| 5, successful | `caf60e30a7f74d0cb888f181d1a7751d` | `i-0efeed68b7df7755b`, `i-0972f1ca1a1bd81dd` |

The audit ran these provider queries for every run name above. `IsTruncated`
was false for each empty storage result; the lifecycle controller also audited
all version pages during cleanup.

```sh
run_name=swarmy-bench-caf60e30a7f74d0cb888f181d1a7751d
filters="Name=tag:Name,Values=$run_name Name=tag:managed-by,Values=codex-launcher"
aws ec2 describe-instances --region us-east-1 --filters $filters \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name,Type:InstanceType,Image:ImageId}'
# Both instances: State=terminated (full JSON in the evidence file)
aws ec2 describe-volumes --region us-east-1 --filters $filters \
  --query 'Volumes[].{Id:VolumeId,State:State}'
# []
aws ec2 describe-key-pairs --region us-east-1 --filters $filters \
  --query 'KeyPairs[].KeyPairId'
# []
aws s3api list-objects-v2 --bucket "$SWARMY_BENCH_BUCKET" \
  --prefix "$run_name/" --max-keys 1 --no-paginate \
  --query '{KeyCount:KeyCount,IsTruncated:IsTruncated}'
# {"KeyCount":0,"IsTruncated":false}
aws s3api list-object-versions --bucket "$SWARMY_BENCH_BUCKET" \
  --prefix "$run_name/" --max-keys 1 --no-paginate \
  --query '{Versions:Versions,DeleteMarkers:DeleteMarkers,IsTruncated:IsTruncated}'
# {"Versions":null,"DeleteMarkers":null,"IsTruncated":false}
aws s3api list-multipart-uploads --bucket "$SWARMY_BENCH_BUCKET" \
  --prefix "$run_name/" --max-uploads 1 --no-paginate \
  --query '{Uploads:Uploads,IsTruncated:IsTruncated}'
# {"Uploads":null,"IsTruncated":false}
gcloud compute instances list --project=swarmy-508717 \
  --filter='name=(swarmy-bench-45ef9abfc08a421bb0f687ce5c0108d3 swarmy-bench-d7d1da0331b24c98972a4b2043f7fb15 swarmy-bench-4c51eadbbc834e74a49d0bee37c8fb8f swarmy-bench-9eb83331696b4239b45a60cf1a17f16b swarmy-bench-7579ebdf38b044e0ad9b9bdbb0dcc12e swarmy-bench-caf60e30a7f74d0cb888f181d1a7751d)' \
  --format=json
# []
```

The final lifecycle cleanup also returned `audit_failures: []`,
`live_resources: []`, and `retained_versions: []`. Optional Google Cloud
measurements and statistically meaningful p95 estimates were not attempted.
All required AWS scenarios and the root test plan were verified.

## 2026-09-17 remote node workflow without local root

### Scope and method

Source: `c405fba94d130108077c97b7d1a8cf84e70a229a`, with documentation changes
only. The client was the ordinary Ubuntu user on the launcher:

```text
uid=1000(ubuntu) gid=1000(ubuntu) groups=1000(ubuntu),4(adm),24(cdrom),27(sudo),30(dip),105(lxd)
```

Having sudo available was not used as a substitute for the no-root requirement.
No local sudo command was invoked. The workflow commands ran with a first-in-PATH
`sudo` script that would log an attempt and exit 99; its attempt log was never
created. Provisioning and image construction used sudo over SSH **on the EC2
nodes**, as designed. Rust, compilers, rsync, and a system FoundationDB client
library were already present. `scripts/install-dev-tools.sh` installed tools
under `~/.local`; the optional AWS CLI was unpacked and installed under
`~/.local/aws-cli` and `~/.local/bin`, also without root. The local workspace
build took 9m 31s, outside the `remote up` timer.

AWS credentials were sourced from the existing private file, without printing
or copying it. `[remote]` used the supplied `us-east-1` subnet and security group,
`m6id.xlarge`, 100 GiB gp3, and `managed_by_tag = "codex-launcher"`. The group
already admitted all TCP between members. No network rules were changed.
Both nodes used Ubuntu AMI `ami-025d99823a4caad37` in `us-east-1a` and local NVMe
for computer caches. The client was in the same VPC and used private SSH.
No Google Cloud resources, managed S3 bucket, HMAC key, or objects in the
pre-existing benchmark bucket were created. Object data stayed in the new
node's SeaweedFS stack.

The procedure used no `swarmy vol` commands:

```bash
swarmy remote up no-root-proof
swarmy remote connect no-root-proof
SWARMY_FAKE_SCRIPT="$PWD/.dev/remote-fake.json" \
  SWARMY_FAKE_CALL_LOG=/tmp/swarmy-remote-proof/fake-calls.log \
  swarmy dev up --remote no-root-proof
swarmy doctor --remote no-root-proof
# On the node, through the SSH command printed by up:
cd ~/swarmy
sudo bash -c 'set -a; . /etc/swarmy/node.env; set +a; /usr/local/bin/swarmy image build images/base-ubuntu --tag remote'
# Back on the unprivileged client:
swarmy run --remote no-root-proof --image base-ubuntu:remote \
  'Say ready and wait for my next instruction.'
swarmy chat --remote no-root-proof 01M2PBXXR5TCHDEST70BZ8QCJ0
swarmy remote add-node no-root-proof
```

The real ChatGPT flow is documented in [DEV.md](DEV.md#remote-node-workflow).
There was deliberately no ChatGPT credential in this run. The gateway used the
existing scripted fake provider from `swarmy-gateway`, with `latency_ms = 30`
and an indexed `responses` map in `.dev/remote-fake.json`. A response has the
following format; text responses use `parts: [{"text":{"text":"..."}}]` and
`stop_reason: "end_turn"`:

```json
{
  "parts": [{"tool_call": {
    "call_id": "proof-1",
    "tool": "process_start",
    "input": {"command": "exec python3 -u -m http.server 18765 --bind 127.0.0.1"}
  }}],
  "stop_reason": "tool_calls",
  "usage": {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0,
            "reasoning_output_tokens": 0, "total_tokens": 0}
}
```

The response sequence was ready text; process_start; end-turn text; bash curl
and marker write; process_list; end-turn text; checkpoint; end-turn text. The
bash command fetched `http://127.0.0.1:18765/`, printed
`SERVER_FROM_PREVIOUS_TURN_OK`, and wrote `remote-checkpoint` to
`/root/proof-marker`. The terminal was driven through a PTY, answering cursor
position requests as in `crates/swarmy-cli/tests/session/chat.rs`. Durable
`session show --json` events, not the fake assistant's assertions, established
the results. The driver initially sent the checkpoint prompt while input was
locked; that prompt was not submitted. It was resent after the previous turn
finished, and its tool completion was verified.

### Observed chat and checkpoint

Session `01M2PBXXR5TCHDEST70BZ8QCJ0`, abbreviated from the durable transcript:

```text
User: Start the managed HTTP server in the background.
Tool: process_start {command: exec python3 -u -m http.server 18765 --bind 127.0.0.1}
Result: process_id=01M2PBYNK1XBASKX23F298ZYS4
Agent: HTTP server started; ask me to use it in the next turn.

User: Use the server from the previous turn and write the checkpoint marker.
Tool: bash
Result: exit_code=0, stdout="SERVER_FROM_PREVIOUS_TURN_OK\n"
Tool: process_list
Result: process_id=01M2PBYNK1XBASKX23F298ZYS4, status=running
Agent: The next turn reached the existing background server.

User: Checkpoint the computer.
Tool: checkpoint {}
Result: manifest_id=01M2PC0XQ3E1PSX26KP87AMM7A
Agent: Checkpoint acknowledged.
```

The first node's registration was `01M2PBJBQSSMTZ3Y5XW0HWMRDB`. It hosted the
computer before the second node existed. Doctor passed all eleven checks,
including advertised port preservation and reachability of the remote services.

### Add-node and recovery

`remote add-node no-root-proof` succeeded between `00:25:08Z` and `00:29:12Z`.
Its release build took 148 seconds. Status showed two live registrations:
`01M2PBJBQSSMTZ3Y5XW0HWMRDB` and `01M2PC8W2Z9Z4HRVJ7N913ZTJR`, with heartbeat
ages of three and zero seconds. `remote logs no-root-proof` streamed the first
node's journal; the observer stopped it with a 12-second timeout (exit 124).

The laptop control services were restored after the latency comparison. The
fake gateway was restarted with a two-response script: a read-only bash check
of the marker and server, followed by end-turn text. At `00:30:43Z`, SSH ran
`sudo systemctl kill --signal=SIGKILL swarmyd` on the first node (old PID 13039).
Systemd restarted it as PID 26346. Neither backing services nor EC2 were killed.
The worker logged `waiting for the previous computer's volume writer lease to
expire` between `00:31:09Z` and `00:31:34Z`.

At event 41 the session received this system notice, and event 42 failed the
pending tool with the same text:

```text
Your computer was rebuilt from the snapshot at 2026-09-17T00:24:49.635Z,
which was 409 seconds before the failure. Running processes and file
changes after that snapshot were lost. The interrupted tool call failed;
check external side effects before retrying.
```

The check was deliberately read-only, so it was safe to submit another turn
after this error. The fake gateway was restarted with that check as its first
response and a new call id. The terminal driver and manual validation left more
than a placement lease between recovery and this retry. Event 51 delivered
another notice for the same snapshot, this time saying 474 seconds. Event 52
then completed the retry:

```text
User: Retry the read-only marker and background server checks after the rebuild notice.
Tool: bash
Result: exit_code=0
        stdout="remote-checkpoint\nBACKGROUND_PROCESS_LOST\n"
        manifest_id=01M2PC0XQ3E1PSX26KP87AMM7A
        stderr="curl: (7) Failed to connect to 127.0.0.1 port 18765 ..."
```

The first node's journal proves that the successful retry ran there at epoch 3
at `00:32:44.141360Z` and committed at `00:32:44.232048Z`. The joining node's
journal showed registration but no tool execution. From the worker's preference
for another live node on takeover, the inferred sequence is epoch 2 assigned
to the joining node without materializing a computer, followed by expiry of
that unstarted placement and a second takeover back to the first node. This
was not a demonstrated disk rehydration on the added node. It did verify an
acknowledged checkpoint surviving the hosting process's death, loss of the
background process, and durable rebuild messaging in the resumed chat.

Two notice issues are proposed tasks. First, an unstarted recovery placement
can expire while a user reads the failure notice, producing another apparent
computer failure without another host death. Second, the notice labels the
snapshot age at takeover as time "before the failure": the observed kill was
about 353 seconds after the checkpoint, not 409 or 474. Report the recovery-time
age accurately, and distinguish loss of a resident computer from expiry of a
placement that never booted. The initial local assertion expecting one notice
failed; the final evidence explicitly records both notices rather than claiming
a single-notice acceptance pass.

### Startup and tool timings

| Measurement | Result | Boundary |
| --- | ---: | --- |
| `remote up` | 534.8 s | CLI's own elapsed report, including provisioning and release build |
| Release build on the first node | 439 s | Provisioning script's build timer, included in up |
| `remote connect` | 8.068 s | External monotonic timer around one CLI invocation |

Up ran from `2026-09-17T00:07:59Z` to `00:16:54Z`. Connect ran from
`00:22:41.522045Z` to `00:22:49.590514Z`. Its time includes a failed public-IP
SSH probe before successful private-IP fallback. The CLI emits no start/end
timestamps or elapsed time for connect, so the requested CLI-owned connect
measurement could not be collected. This is a proposed follow-up, not a timer
silently attributed to the CLI.

The latency script issued 21 serial `bash` calls with
`{"command":"printf LATENCY_OK","timeout_ms":120000}`, then ended the turn.
Each completion had exit code zero and exactly `LATENCY_OK` on stdout. The
observer timestamped JSON `tool_call_requested` and `tool_call_completed`
records with `time.monotonic_ns()` and subtracted their arrival times by call
id. The first call, which creates the computer, was excluded; the other twenty
were the warm sample. No inference duration, session startup, image build,
checkpoint, or `swarmy vol` operation is in that interval.

For the tunnel sample, `swarmy --json run --remote no-root-proof --image
base-ubuntu:remote` used the laptop scheduler, worker, and gateway. For the
on-node sample, those local services were stopped with `dev down`; the same
five CLI/service executable files were copied to the first node and their
SHA-256 hashes matched. Scheduler, worker, gateway, CLI, and the monotonic
observer ran there using `.dev/env` and the private service endpoints, with no
SSH in the timed interval. Only the fake script and binaries were copied; no
provider credential was involved. Both samples set scheduler scan/resend
intervals to 50/100 ms and fake latency to zero. The normal chat used defaults.
The second node was provisioning independently during these samples.

| Routed bash call | Mean | Median | Min–max | First call, excluded |
| --- | ---: | ---: | ---: | ---: |
| Client with remote profile | 98.296 ms | 47.639 ms | 36.138–224.022 ms | 153.632 ms |
| Worker and observer on first node | 129.831 ms | 132.956 ms | 93.361–146.193 ms | 161.997 ms |
| Client minus on-node | -31.535 ms | -85.317 ms | not applicable | not applicable |

Warm samples in milliseconds, in execution order:

```text
client: 224.022 185.783 110.000 187.094 192.000 191.539 192.652 164.686
         36.503  36.138  36.658  36.752  43.996  47.494  46.436  44.418
         47.785  48.662  45.935  47.364
node:   136.017 139.093 142.437 142.722 146.193  93.361 137.287 133.672
        137.952 133.006 122.960 132.906 123.103 128.572 124.981 124.032
        125.869 123.346 114.850 134.258
```

These are two deployment-topology samples, not an isolated estimate of SSH
transport overhead or a WAN percentile. Moving control services onto the node
also changes CPU contention, and the client sample has a visible settling
pattern. The negative difference does not mean SSH reduces execution latency.
In addition, the direct FoundationDB connections below mean this is not an
all-traffic-through-the-tunnel measurement. A positive tunnel-only latency
penalty remains unverified.

### FoundationDB reachability finding

The coordinator file remained `dev:dev@127.0.0.1:4500`, and doctor passed.
Nevertheless, `ss -tnp` on the client showed each local control service also
connected directly to the advertised node address. Representative established
connections, with ephemeral ports retained from the observation:

```text
swarmy-worker   127.0.0.1:45408      -> 127.0.0.1:4500
swarmy-worker   172.31.62.91:60864   -> 172.31.59.242:4500
swarmy-worker   127.0.0.1:50880      -> 127.0.0.1:4222
swarmy-scheduler 172.31.62.91:60850  -> 172.31.59.242:4500
swarmy-gateway  172.31.62.91:60868   -> 172.31.59.242:4500
```

Thus this run proves operation as an unprivileged same-VPC client. It does not
prove the intended SSH-only workflow from a laptop with no private route.
Preserving local port 4500 solves the previously documented port assertion but
not advertised-address reachability. DEV.md now makes this prerequisite
explicit. Proposed task: make every FoundationDB endpoint reachable from an
SSH-only client without laptop root, and add acceptance coverage from a client
that cannot route directly to the node's private address. Doctor should detect
this limitation instead of treating coordinator reachability as sufficient.

A process-local negative check confirmed this was a dependency, not an unused
connection. A temporary `LD_PRELOAD` shim intercepted `connect()` and returned
`ENETUNREACH` only for `172.31.59.242:4500`; localhost and SSH connections were
unchanged. It was compiled as the ordinary user with `cc -shared -fPIC -Wall
-Wextra -Werror ... -ldl`, and was not installed or applied to other processes.

```bash
timeout 15 env LD_PRELOAD=/tmp/swarmy-remote-proof/block-private-fdb.so \
  swarmy session list --remote no-root-proof --json
# exit 1, no session rows; stderr ends: Error: FdbError { error_code: 1031 }
env LD_PRELOAD=/tmp/swarmy-remote-proof/block-private-fdb.so \
  swarmy doctor --remote no-root-proof --json
# exit 0, "ok": true
swarmy session list --remote no-root-proof --json
# exit 0, three session rows
```

This simulated loss of the private route requires neither local root nor changes
to security groups. It is not a test from a physically external laptop.

### Validation and cleanup

All three required commands passed on the client as `ubuntu`:

```bash
cargo fmt --all --check
scripts/dev-stack.sh start
source .dev/env
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
scripts/dev-stack.sh stop
```

The temporary local backing stack was used for the workspace integration tests
and stopped before remote connect, so port 4500 was free. Formatting, tests,
and Clippy exited zero (241 tests passed, one ignored, zero failed). The separate root-only volume and sandbox acceptance
programs were not invoked locally; the live remote chat supplied the node
execution exercise. Optional tests without their required environment retain
the repository's normal skip behavior. The real ChatGPT login/session, an
external client with no VPC route, CLI-owned connect timing, and a positive
isolated tunnel-latency penalty remain unverified for the reasons above.

Teardown ran as the ordinary client user:

```text
$ swarmy dev down
services: down; remote stack preserved
$ swarmy remote disconnect no-root-proof
no-root-proof: disconnected
$ swarmy remote down no-root-proof
Terminating i-00f038e52e066a37a
Confirmed i-00f038e52e066a37a is terminated or absent
Deleting key pair swarmy-01M2PC1G1DHJJAQPFFPC1T0W5A
Terminating i-0cd85e43c2a5e0248
Confirmed i-0cd85e43c2a5e0248 is terminated or absent
Deleting key pair swarmy-01M2PB23J85JBXB7ESRJ6C7AT3
Removed remote no-root-proof
```

Independent provider queries at `2026-09-17T00:35:20Z` confirmed termination
and removal of both task-created root volumes and imported keys:

```bash
aws ec2 describe-instances --region us-east-1 \
  --instance-ids i-0cd85e43c2a5e0248 i-00f038e52e066a37a \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name}'
# [{"Id":"i-00f038e52e066a37a","State":"terminated"},
#  {"Id":"i-0cd85e43c2a5e0248","State":"terminated"}]
aws ec2 describe-volumes --region us-east-1 \
  --filters Name=tag:Name,Values=no-root-proof,no-root-proof-2 \
  --query 'Volumes[].VolumeId'
# []
aws ec2 describe-key-pairs --region us-east-1 \
  --filters Name=key-name,Values=swarmy-01M2PB23J85JBXB7ESRJ6C7AT3,swarmy-01M2PC1G1DHJJAQPFFPC1T0W5A \
  --query 'KeyPairs[].KeyName'
# []
```

The deleted EBS volumes were `vol-01c2df92c27efcb5e` and
`vol-0ef8fff9d923e53f4`. Instance-store disks and assigned public IPv4 addresses
were released with their instances. The pre-existing launcher, subnet, security
group, and benchmark bucket were left intact. No Google Cloud resources or
external S3 objects needed cleanup because this workflow created none.

## 2026-09-17 SSH-only remote stack

This run uses loopback advertising and SSH forwarding for FoundationDB, NATS,
and SeaweedFS. Both EC2 nodes are `m6id.xlarge` instances in `us-east-1`, using
Ubuntu 24.04, kernel `7.0.0-1012-aws`, and local NVMe. The ordinary launcher
user is `ubuntu`, UID 1000. Before `remote up`, this rule blocked direct client
connections to all three service ports throughout the private subnet:

```sh
sudo iptables -I OUTPUT -m owner --uid-owner ubuntu -d 172.31.0.0/16 \
  -p tcp -m multiport --dports 4500,4222,8333 -j REJECT
```

SSH port 22 remained available. Direct probes to all three ports on both
`172.31.51.83` and `172.31.61.79` failed, incrementing this rule's packet counter
six times. Cloud provisioning and the client workflow ran as ubuntu. Image
construction and systemd administration ran with sudo over SSH on the nodes;
local privileged acceptance tests ran separately against the local dev stack.

`remote up ssh-proof` reported 525.2 seconds, including a 434-second release
build. The first node's actual fdbserver command used both
`-p 127.0.0.1:4500` and `-l 127.0.0.1:4500`. `remote add-node ssh-proof` installed
a dedicated key and pinned host key, both mode 0600, and registered the second
node through `swarmy-tunnel.service`. Both cluster files contained
`dev:dev@127.0.0.1:4500`. The joiner had no FoundationDB, NATS, or SeaweedFS
server process. Killing its SSH process changed the tunnel PID from 9255 to
9423, with `NRestarts=1`; fdbcli reported the database available before and after.

### Transactions, timing, and recovery

The local dev stack was stopped before connecting. JSON connect reported
7.905577024 seconds total, 7.486774309 seconds of address probing, and
0.413775712 seconds of tunnel startup. A fresh human-output run reported:

```text
# Connected in 7.647s (address probing: 7.234s; tunnel startup: 0.413s; reused: false)
```

The public SSH address did not answer; probing fell back to private SSH.
Reusing that control master took 0.004304175 seconds and reported zero for both
skipped phases. All local service ports were the standard 4500, 4222, and 8333.

The real fdbserver was paused with SIGSTOP while its TCP listener and SSH
control master remained up, then resumed with SIGCONT. Both checks used control
master PID 103901 and the same profile:

| Database process | Doctor exit | SSH check | FoundationDB transaction | NATS round trip |
| --- | ---: | --- | --- | --- |
| Paused | 1 | pass | fail | pass |
| Resumed | 0 | pass | pass | pass |

The failure detail was `FoundationDB transaction failed (exit status: 1); check
the cluster file, advertised address, and tunnel`. The successful detail was
`FoundationDB session read transaction succeeded`. Full reports and timings are
in [the raw results](benchmarks/2026-09-17-ssh-only.json).

The fake provider exercised real tools without a provider credential. A normal
`run` returned `Hello from swarmy!`. With the primary daemon stopped, session
`01M2PG0XPNDWC4WWZECVCKJ2V9` placed agent `01M2PG0XPNNKA86MWG0Q4TJGTJ` on the
joining node. It wrote `/root/ssh-proof-durable`, explicitly checkpointed to
`01M2PG1EKV5S2KJ56J0KC5EAN0`, and then wrote `/root/ssh-proof-transient`.
Both bash calls returned exit code zero; SSH verified both files on the joiner.

The primary daemon was restarted. At `2026-09-17T01:36:24.762892Z`, after
SIGKILL of the joining daemon with automatic restart disabled, the client
requested termination of `i-0d42743c23ae24147`. After the writer lease expired,
a real PTY `chat` resumed the same session through the profile. It displayed
exactly one rebuild notice. The recovery bash call returned exit code zero,
`RECOVERED-FILES`, and the checkpoint manifest ID above. SSH independently
verified the durable marker on the surviving primary and absence of the
uncheckpointed marker. This is cross-node recovery, not a daemon restart on the
same machine. [The recorded tool results and notice](benchmarks/2026-09-17-ssh-only-recovery.json)
retain the exact event contents.

The existing notice's age wording still describes time until takeover as time
before failure: it reported 168 seconds, while the recorded kill was about
83.5 seconds after the snapshot. This task does not change that wording.

### Twenty-call latency comparison

The observer in `scripts/benchmarks/remote-latency.py` repeats the earlier
measurement boundary: match `tool_call_requested` and `tool_call_completed`
JSON records by call ID and subtract their `time.monotonic_ns()` arrival times.
Each run issues 21 serial `printf LATENCY_OK` bash calls. The first call is
excluded; the remaining twenty are the warm sample. Every one of the 42 calls
returned exit code zero and exactly `LATENCY_OK` on stdout. Inference time,
session startup, image building, and checkpoint time are outside the interval.

Both runs used the same five stripped debug CLI/service binaries, verified by
SHA-256, Rust 1.98.1, zero fake-provider delay, and scheduler scan/resend intervals
of 50/100 ms. The first run used the blocked launcher and remote profile. Then
its control services were stopped, and the same scheduler, worker, gateway,
CLI, and observer ran on the primary with loopback service endpoints. The node
services were stopped after that sample. The client block remained installed
through both runs; its counter stayed at the six deliberate direct probes.
The only established client connection to the primary's private IP was SSH.

| Routed bash call | Mean | Median | Min–max | First call, excluded |
| --- | ---: | ---: | ---: | ---: |
| Blocked client through SSH | 110.710 ms | 51.281 ms | 46.323–529.594 ms | 291.837 ms |
| Control services and observer on primary | 116.055 ms | 130.688 ms | 33.093–142.879 ms | 156.410 ms |
| Client minus on-node | -5.345 ms | -79.407 ms | not applicable | not applicable |

[Raw monotonic samples and binary hashes](benchmarks/2026-09-17-ssh-only.json)
include all calls. These are small deployment-topology samples in one VPC.
Moving control services also changes CPU contention. They do not isolate SSH
transport overhead, estimate WAN latency, or show that SSH makes execution
faster. Unlike the earlier private-advertising run, all client database traffic
now uses the tunnel.

To reproduce the sample, select the fake provider with this script and restart
the control services with the scan/resend settings above:

```json
{"latency_ms":0,"request_based":{"steps":22,"tool_steps":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],"bash_command":"printf LATENCY_OK","final_answer":"LATENCY COMPLETE"}}
```

```sh
SWARMY_FAKE_SCRIPT=/tmp/swarmy-latency-fake.json \
SWARMY_SCHEDULER_SCAN_INTERVAL_MS=50 SWARMY_SCHEDULER_RESEND_INTERVAL_MS=100 \
  /tmp/swarmy-latency-bin/swarmy dev up --remote ssh-proof
timeout 120 python3 scripts/benchmarks/remote-latency.py \
  /tmp/swarmy-latency-remote.json /tmp/swarmy-latency-bin/swarmy run \
  --remote ssh-proof --json --image base-ubuntu:ssh-proof \
  'Measure 21 serial bash calls'
```

For the on-node comparison, stop the client control services, copy those same
binaries and fake script to the primary, and start one scheduler, worker, and
gateway there with the same settings. Run the observer there without `--remote`,
then stop those three processes. No provider credential needs to be copied.

### Validation and teardown

With the local dev stack running and `.dev/env` sourced,
`cargo test --workspace --locked` passed 246 tests with one pre-existing ignored
test. `cargo fmt --all --check` and
`cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
The SSH acceptance script passed port collisions, mapping rejection, real NATS
traffic, disconnect cleanup, and fresh/reused connect timing checks.

Tests were compiled as ubuntu with
`cargo test --workspace --locked --no-run --message-format=json`. Running the
selected binaries with `sudo -E BINARY --nocapture --test-threads=1` passed 40
tests across nine artifacts: CLI image/vol, volume library/device/image/NBD/volume,
node, and chaos bash. No root case skipped, including OCI import. The chaos
artifact passed persistent computers, a mid-command node kill, and twelve
seeded randomized kills. No NBD attachment remained afterward. The proof used
the fake provider; a real ChatGPT session and a WAN client were not measured.

Teardown ran `swarmy dev down`, `swarmy remote disconnect ssh-proof`, and
`swarmy remote down ssh-proof`. Independent EC2 SDK queries at
`2026-09-17T01:42:04.244544Z` confirmed:

| Resource | Result |
| --- | --- |
| `i-00abb3ca6ed0c7cd2` | terminated |
| `i-0d42743c23ae24147` | terminated |
| Both task EBS volume IDs | no remaining volumes |
| Both task imported key names | no remaining key pairs |

[The provider audit](benchmarks/2026-09-17-ssh-only-cleanup.json) records the exact
IDs and queried keys. It used `describe_instances(InstanceIds=...)`,
`describe_volumes(Filters=[{"Name":"volume-id","Values":...}])`, and
`describe_key_pairs(Filters=[{"Name":"key-name","Values":...}])` through boto3
in `us-east-1`. No external S3 bucket, object prefix, or GCP resource was created.
The launcher was not a termination target. Remote state, profiles, and keys
were removed by down. The client firewall rule and temporary localhost SSH
authorizations were removed after the audit.

## 2026-09-17 Turn timeline benchmark

`swarmy bench turn` now follows the user message ID across submission, durable
append, scheduler nudges, worker leases, inference, routed bash execution,
committed idle, final text rendering, and input enabling. This run measured the
line client with stdout redirected to a file. It uses the same conversation
transport and line renderer as `swarmy run`, rendering the durable final answer
rather than streaming deltas. It is not a PTY rendering benchmark.

The client, scheduler, worker, and gateway ran as ubuntu on the launcher, an
AWS `m6i.xlarge` with four vCPUs and 15.3 GiB available RAM. Locally, FoundationDB,
NATS, SeaweedFS, and root `swarmyd` also ran there, on its EBS root disk. The remote
run placed those four backing/execution services on `m6id.xlarge`
`i-005be6a555af2b2a8` in `us-east-1a`, with four vCPUs, 16 GiB RAM, a 100 GiB gp3
root disk, and 220.7 GiB instance-store NVMe for computer volumes. Both machines
used Ubuntu 24.04 and kernel `7.0.0-1012-aws`. Services were FoundationDB 7.3.79,
NATS 2.14.6, and SeaweedFS 4.47. The binaries were stripped development builds
from Rust 1.98.1; the same node and client binaries were copied to the remote.
No build or acceptance test ran during the measured turn samples.

The fake script is [turn-fake.json](../scripts/benchmarks/turn-fake.json). Every
user turn selects either one immediate text response or a `printf TURN_TOOL_OK`
bash call followed by the final text. The benchmark validates the final answer,
the number of tool calls, exit status, and tool stdout. Each shape uses one
session and repeats the user turn within that session. One warmup per shape is
recorded but excluded from percentiles. Bash used a 256 MiB ext4 image containing
the launcher's bash, sleep, setsid, and their shared libraries. Image preparation
and session creation are outside the interval; the first computer boot is in
the excluded warmup. The warm computer remains placed between user turns.

All elapsed milestones below start immediately before the client's initial
session read and append. Repeated stages show their last occurrence in a turn;
for example, the last claim is the worker that finishes the turn. Inference
and tool duration rows are sums of matched request intervals, not elapsed
milestones. Rendering can precede committed idle, so these columns are not
additive. End-to-end ends when the client enables input, using its monotonic
clock. Percentiles use nearest rank, without interpolation.

### Default settings

Five measured turns per shape, after one warmup, used the unchanged defaults:
1,000 ms scheduler scans, 5,000 ms nudge resend suppression, and the client's
500 ms catch-up timer. At this sample size p95 is the maximum observed value.

**no_tool**, milliseconds.

| Stage | Local p50 | Local p95 | SSH remote p50 | SSH remote p95 |
| --- | ---: | ---: | ---: | ---: |
| appended | 5.003 | 5.268 | 4.919 | 48.478 |
| nudged | 5589.049 | 5590.860 | 5457.756 | 6081.339 |
| claimed | 5594.892 | 5596.829 | 5468.967 | 6092.089 |
| inference_started | 46.133 | 46.381 | 259.450 | 329.343 |
| inference_finished | 55.370 | 57.373 | 269.732 | 338.248 |
| idle | 5616.737 | 5620.306 | 5506.928 | 6189.309 |
| final_text_rendered | 500.201 | 500.865 | 502.106 | 502.734 |
| input_enabled | 5999.343 | 6000.578 | 5998.844 | 6498.152 |
| end_to_end | 5999.343 | 6000.578 | 5998.844 | 6498.152 |
| inference_total_duration | 9.290 | 10.992 | 9.512 | 10.837 |

**bash**, milliseconds.

| Stage | Local p50 | Local p95 | SSH remote p50 | SSH remote p95 |
| --- | ---: | ---: | ---: | ---: |
| appended | 5.010 | 5.066 | 8.312 | 48.889 |
| nudged | 16589.416 | 16593.199 | 16782.733 | 16892.403 |
| claimed | 16595.594 | 16599.141 | 16790.065 | 16900.589 |
| inference_started | 10634.130 | 11628.889 | 11026.023 | 11858.610 |
| inference_finished | 10645.151 | 11638.159 | 11038.004 | 11912.578 |
| tool_dispatched | 5618.955 | 5624.962 | 5891.079 | 5984.103 |
| tool_completed | 5650.741 | 5658.562 | 5917.678 | 6008.334 |
| idle | 16617.556 | 16621.297 | 16818.246 | 16967.355 |
| final_text_rendered | 11000.334 | 11999.872 | 11143.067 | 12004.017 |
| input_enabled | 16999.834 | 17000.028 | 16998.448 | 17011.471 |
| end_to_end | 16999.834 | 17000.028 | 16998.448 | 17011.471 |
| inference_total_duration | 18.846 | 20.551 | 24.545 | 63.889 |
| tool_total_duration | 31.786 | 33.600 | 23.752 | 26.597 |

### Shorter scheduler timers

Twenty measured turns per shape, after one warmup, used 50 ms scans and
100 ms resend suppression, matching the earlier SSH-only proof. The client
catch-up timer remained 500 ms. These are explicit configuration overrides;
this change does not alter production defaults.

**no_tool**, milliseconds.

| Stage | Local p50 | Local p95 | SSH remote p50 | SSH remote p95 |
| --- | ---: | ---: | ---: | ---: |
| appended | 5.199 | 5.405 | 11.654 | 50.196 |
| nudged | 143.949 | 174.357 | 468.679 | 858.465 |
| claimed | 149.753 | 180.057 | 478.046 | 866.533 |
| inference_started | 46.881 | 48.234 | 92.045 | 179.454 |
| inference_finished | 56.498 | 58.952 | 103.501 | 208.906 |
| idle | 172.719 | 202.179 | 570.536 | 925.728 |
| final_text_rendered | 153.601 | 183.956 | 495.129 | 507.353 |
| input_enabled | 499.660 | 500.592 | 997.135 | 1000.694 |
| end_to_end | 499.660 | 500.592 | 997.135 | 1000.694 |
| inference_total_duration | 9.475 | 11.429 | 11.592 | 34.465 |

**bash**, milliseconds.

| Stage | Local p50 | Local p95 | SSH remote p50 | SSH remote p95 |
| --- | ---: | ---: | ---: | ---: |
| appended | 2.920 | 5.288 | 11.213 | 24.247 |
| nudged | 425.884 | 449.671 | 1921.136 | 2262.872 |
| claimed | 429.295 | 455.329 | 1933.174 | 2276.610 |
| inference_started | 315.268 | 357.457 | 1275.356 | 1653.445 |
| inference_finished | 323.126 | 366.637 | 1292.683 | 1672.706 |
| tool_dispatched | 161.786 | 200.972 | 667.248 | 891.509 |
| tool_completed | 195.433 | 235.094 | 692.246 | 919.934 |
| idle | 443.757 | 476.824 | 1968.034 | 2318.915 |
| final_text_rendered | 433.347 | 459.341 | 1507.688 | 2029.843 |
| input_enabled | 499.581 | 500.558 | 1999.785 | 2524.088 |
| end_to_end | 499.581 | 500.558 | 1999.785 | 2524.088 |
| inference_total_duration | 17.145 | 19.597 | 26.986 | 56.158 |
| tool_total_duration | 30.702 | 35.667 | 24.471 | 29.522 |

### Interpretation and reproduction

The default resend gate dominates: a fresh runnable step within the same turn
waits behind the preceding nudge's five-second suppression window. The bash
shape passes through that window three times, compared with once for text.
The shorter timers remove most of that wait, but client polling and the
remaining scheduling/store work still exceed the proposed budget. Immediate
fake inference is only a small part of end-to-end time. These observations
motivate the per-stage targets in [design section 5.1](DESIGN.md#51-turn-timeline-and-proposed-latency-budget):
under 100 ms locally and under three round trips plus 100 ms remotely. Neither
configuration demonstrates that target. Meeting it needs readiness-driven
scheduling that distinguishes new steps from resends, prompt client idle
notification, and fewer serial remote store round trips.

The remote client used `--remote turn-proof`. The local dev stack was stopped
before connecting so the FoundationDB tunnel could bind port 4500. On the
remote node FoundationDB advertised loopback at port 4500. Throughout remote
measurement, this client rule blocked direct private service traffic:

```sh
sudo iptables -I OUTPUT -m owner --uid-owner ubuntu -d 172.31.0.0/16 \
  -p tcp -m multiport --dports 4500,4222,8333 \
  -m comment --comment swarmy-turn-proof -j REJECT
```

The proof artifact records failed direct probes, firewall counters, SSH
connections, binary hashes, clock synchronization, and resource identifiers.
The launcher and node are in one VPC; this is an SSH-route proof, not a WAN
measurement. A 50-sample TCP connection probe to SSH port 22 measured a median
0.512 ms and p95 1.975 ms; it includes connection setup, not application work.
Only the remote tool-completed milestone and tool interval cross clock domains.
Those use synchronized UTC; all other intervals use a shared host monotonic
clock. Chrony reported microsecond system offsets and sub-millisecond root
uncertainty. Three decimal places in the table do not imply microsecond
cross-host accuracy. Raw records explicitly flag cross-host comparisons.

With the dev stack and a node running, select the fake script in the project
config's `[fake].script` and restart the control services. The script must be
visible to the client and gateway; no real provider credentials are needed.
The measured commands were:

```sh
/tmp/swarmy-turn-bin/swarmy dev up
/tmp/swarmy-turn-bin/swarmy bench turn --turns 5 --image turn-image:bench \
  --output /tmp/turn-local-default.json
SWARMY_SCHEDULER_SCAN_INTERVAL_MS=50 SWARMY_SCHEDULER_RESEND_INTERVAL_MS=100 \
  /tmp/swarmy-turn-bin/swarmy dev up
/tmp/swarmy-turn-bin/swarmy bench turn --turns 20 --image turn-image:bench \
  --output /tmp/turn-local-tuned.json
# Stop the local node and dev stack, then connect the remote profile.
/tmp/swarmy-turn-bin/swarmy remote connect turn-proof
/tmp/swarmy-turn-bin/swarmy dev up --remote turn-proof
/tmp/swarmy-turn-bin/swarmy bench turn --remote turn-proof --turns 5 \
  --image turn-image:bench --output /tmp/turn-remote-default.json
SWARMY_SCHEDULER_SCAN_INTERVAL_MS=50 SWARMY_SCHEDULER_RESEND_INTERVAL_MS=100 \
  /tmp/swarmy-turn-bin/swarmy dev up --remote turn-proof
/tmp/swarmy-turn-bin/swarmy bench turn --remote turn-proof --turns 20 \
  --image turn-image:bench --output /tmp/turn-remote-tuned.json
```

Raw timelines include every warmup and repeated stage:
[local defaults](benchmarks/2026-09-17-turn-local-default.json),
[remote defaults](benchmarks/2026-09-17-turn-remote-default.json),
[local shorter timers](benchmarks/2026-09-17-turn-local-tuned.json), and
[remote shorter timers](benchmarks/2026-09-17-turn-remote-tuned.json).
The [network and teardown evidence](benchmarks/2026-09-17-turn-proof.json)
retains the audit details.

### Validation and cleanup

The required commands passed: `cargo fmt --all --check`,
`cargo test --workspace --locked` (with `.dev/env` sourced; 257 passed and the
existing dedicated-account provider test ignored), and
`cargo clippy --workspace --all-targets --locked -- -D warnings`.
`cargo build --workspace --locked` also passed. The opt-in timeline integration
test ran separately against both measured stacks with
`SWARMY_BENCH_IMAGE=turn-image:bench`; the remote run additionally set
`SWARMY_REMOTE=turn-proof`. Both passed, checking every required stage, turn
identity, tool presence, and percentile output for both shapes.

Privileged acceptance binaries were compiled as ubuntu with
`cargo test --workspace --locked --no-run --message-format=json`, then executed
with `sudo -E <test-binary> --nocapture --test-threads=1`. The volume library,
volume/image/NBD/device integration tests, CLI image/volume tests, and node
lifecycle test passed all 39 tests, exercising actual kernel devices, OCI image
preparation, sandbox persistence, fencing, and cleanup. The bash chaos acceptance
binary ran with `SWARMY_TEST_IMAGE=base-ubuntu:turn-root-tests`: its full repeat
passed all four scenarios, including twelve seeded kills. Its first run failed
in that last scenario at the existing `failure lacks its matching recovery
system message` assertion. The store suppresses a recovery notice for initial
or unstarted placements, while that assertion requires one for every tool
error. These paths are unchanged here; the initial failure is recorded rather
than weakening the check. No NBD or ublk device remained attached after testing.

The dedicated real-account provider test was not run because this task uses the
fake provider and no dedicated provider account was supplied. This run does not
measure WAN latency, cold image creation, or terminal drawing. Cross-host stage
timing depends on clock synchronization; end-to-end does not.

AWS teardown completed and was independently queried at `2026-09-17T09:08:27+00:00`.
The benchmark instance `i-005be6a555af2b2a8` is `terminated`; its tagged
100 GiB root volume query returns `[]`, and the imported key pair
`swarmy-turn-178963` query returns `[]`. The temporary client firewall rule
and SSH tunnel were removed. The launcher and existing network resources
were retained. No GCP resources, external S3 bucket, or external S3 objects
were created; the benchmark's SeaweedFS objects disappeared with the node.
The provider queries and their results were:

```sh
aws ec2 describe-instances --instance-ids i-005be6a555af2b2a8 --query "Reservations[].Instances[].{InstanceId:InstanceId,State:State.Name}" --output json
# [{"InstanceId": "i-005be6a555af2b2a8", "State": "terminated"}]
aws ec2 describe-volumes --filters Name=tag:Name,Values=swarmy-turn-benchmark --query "Volumes[].{VolumeId:VolumeId,State:State}" --output json
# []
aws ec2 describe-key-pairs --filters Name=key-name,Values=swarmy-turn-178963 --query "KeyPairs[].KeyName" --output json
# []
```


## 2026-09-17: Generation boundaries and upload priority

Measured on the launcher Ubuntu 24.04 EC2 host with four CPUs and 15 GiB of
memory. Tests used the pinned Rust 1.98.1 development profile, NBD, ext4,
FoundationDB, NATS, and SeaweedFS. Timing runs ran after compilation stopped.
These are individual host observations, not a fleet percentile or a real-time
guarantee.

Periodic publication now captures dirty chunk generations under the dirty lock
and releases it before uploading or committing metadata. An overwrite preserves
its boundary overlay before changing the live generation. Preserved bytes use
an 8 MiB memory budget and spill to an anonymous file in the dirty directory.
Only generations unchanged since the boundary become clean after publication.
The writer lease, placement fence, and retained-head transaction are unchanged.

Checkpoint syncs buffered writes and uses the same unfrozen block boundary.
Ext4 journal recovery is expected when mounting a retained image.
`swarmy vol flush --freeze` optionally freezes the discovered mount for a clean
filesystem image; `--mount` alone only validates the mount path. Ordinary
snapshots and checkpoints report zero frozen time.

| Root test measurement | Result |
| --- | --- |
| Continuous 4 KiB direct NBD writes across four snapshots | 314 observations; maximum write latency **3.372 ms**, below the 5 ms assertion |
| Timestamp writer with fsync and a 10 ms cadence, during continuous 64 KiB direct rewrites across a preallocated 8 MiB file | 31 observations; largest inter-write gap **28.416 ms**, below the 30 ms assertion |
| Retained ext4 images from the concurrent-write run | All four mounted, matched the known file's SHA-256, and passed `e2fsck -f -n` after journal replay and unmount |
| Node tool call, including a 100 ms sleep | **115.935 ms** without a snapshot; **128.728 ms** during one, a **12.793 ms** difference |
| Snapshot overlapping that tool call | **326.550 ms**, zero frozen time, nine uploads admitted at tool priority |

The direct-write measurement times each `pwrite` through the kernel NBD device.
The filesystem measurement includes its intentional 10 ms sleep, ext4 journal
work, and competing writes. A preliminary buffered 8 MiB overwrite workload
produced a 37.654 ms inter-fsync gap; the steady direct-I/O test preallocates its
file to separate extent allocation and bulk writeback from snapshot contention.
The existing lightweight benchmark below remains the comparison for the old
approximately 330 ms block-level snapshot pause.

The node shares upload admission across all its attachments. While any tool call
is active, it admits at most four uploads and 16 MiB/s of chunk data. Previously
admitted requests drain before new work can use the reduced limit. Prepared
uploads yield the executor, and their dirty reads wait behind foreground mutex
waiters. Flush JSON exposes `upload_concurrency_limit`,
`upload_bytes_per_second` (zero means uncapped), and `tool_priority_uploads`.
The node sample finished after the tool, so its final reported limit returned to
32. The admission test observes the active limit of four and 16 MiB/s directly,
checks the bandwidth duration, and verifies restoration after nested tool guards
are dropped.

The generation tests gate uploads, overwrite 40 boundary chunks, exhaust the
8 MiB copy budget, and verify both preserved manifest bytes and new live bytes.
A separate model checks every chunk hash of four retained 48-chunk snapshots
after concurrent overwrites. Cancellation, commit-time writes, rejected fencing,
and a failed spill copy are covered; a spill failure abandons publication while
allowing the live write to proceed.

The existing `scripts/benchmarks/persistent-volume.py` workload ran twice through
`swarmy-chaos --persistent --measurements`. Each run now keeps a separate 8 MiB
random file changing during thirteen generation checkpoints. It then stops that
writer and uses the unchanged timestamp/fsync loop for the pause measurement.
The checkpointed generation marker and another 8 MiB random payload provide
independent expected hashes for restore checks.

| Existing timestamp benchmark | Run 0 | Run 1 |
| --- | --- | --- |
| Largest inter-write gap | **24.454381 ms** | **30.299136 ms** |
| Timestamp observations | 139 | 111 |
| Retained snapshots | 10 | 10 |
| Retained snapshots booted and hash-verified after collection | 10/10 | 10/10 |
| Unreferenced bytes collected | 287,834,112 | 90,701,824 |

The largest observed gap was **30.30 ms**, compared with the earlier roughly
330 ms pause. Every retained snapshot was cloned, attached with an empty chunk
cache, mounted, and used to start Bash with `chroot`. All twenty boots verified
the expected generation marker and SHA-256 of the full 8 MiB payload. Boot times
ranged from 0.742 to 0.908 seconds. Dry-run candidate bytes matched actual
collection, and restored hashes still matched after deletion.

[Raw measurements](benchmarks/volume-boundary-20260917.json) retain the neighboring
monotonic timestamps around each largest gap, all publication counters, retained
manifest ids, collection results, and every boot time.

Validation commands and results:

- `rustup show`: Rust 1.98.1 installed and selected.
- `scripts/dev-stack.sh start`: FoundationDB, NATS, and SeaweedFS available;
  `.dev/env` was sourced for service and root tests.
- `cargo fmt --all --check`: passed.
- `cargo test --workspace --locked`: passed, both with the dev stack enabled and
  in the normal CI environment where service-dependent tests skip.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo build --workspace --locked`: passed.
- `sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y skopeo umoci`:
  installed the missing OCI test tools.
- `cargo test --workspace --locked --no-run --message-format=json`: passed;
  this supplied the executable paths in `/tmp/all-build.json` without building
  as root. Targeted `--no-run` builds of the volume NBD, node, and chaos Bash
  tests also passed.
- `sudo -E "$(jq -r 'select(.executable != null and .target.name == "nbd") | .executable' /tmp/all-build.json)" --nocapture --test-threads=1`:
  both NBD tests passed, including fio, detach cleanup, and the measurements above.
- `sudo -E "$(jq -r 'select(.executable != null and (.package_id | contains("swarmy-volume")) and .target.name == "image") | .executable' /tmp/all-build.json)" --nocapture`:
  all five image tests passed, including the root ext4, shell, and OCI cases.
- `sudo -E "$(jq -r 'select(.executable != null and .target.name == "node") | .executable' /tmp/all-build.json)" --nocapture`:
  passed in 207.94 seconds, including tool priority, checkpoint, crash recovery,
  lease renewal, takeover fencing, idle eviction, and shutdown recovery.
- `SWARMY_TEST_IMAGE=base-ubuntu:bash-test sudo -E "$(jq -r 'select(.executable != null and .target.name == "bash") | .executable' /tmp/all-build.json)" --nocapture`:
  all four root chaos scenarios passed in 214.84 seconds, including twelve
  scheduled process kills with seed 42.
- With `.dev/env` sourced and `PYTHONDONTWRITEBYTECODE=1`,
  `sudo -E target/debug/swarmy-chaos --no-start-stack --bin-dir /home/ubuntu/workspace/target/debug --persistent --image base-ubuntu:bash-test --sessions 2 --gateways 1 --kills 0 --seed 20260917 --session-timeout-secs 240 --measurements /home/ubuntu/workspace/scripts/benchmarks/persistent-volume.py`:
  passed in 142.64 seconds, including both measured runs and all twenty boots.

The final volume binaries were also executed directly with `.dev/env` sourced:
21 library tests and 16 integration tests passed, including real writer fencing
and object-store tests. The image tests were then rerun as root to exercise
their privileged cases. The root NBD tests above are additional. Root tests keep
Drop guards for unmount and device cleanup; normal CI still skips them with an
explicit message when it lacks root.

## 2026-09-17 Event-driven turn pipeline

This run compares master at `672acea` with the event-driven pipeline in this
change. User appends atomically make the session runnable and publish its nudge.
Gateway and sandbox completions also publish nudges. Publishers share the
partition rule and suppress repeated publications for the same durable head;
a new head is immediately eligible. The scheduler scan and client store poll
both run every five seconds as recovery paths. The client renders committed
assistant events and enables input on the committed idle event. A response with
no tool calls finishes directly in the gateway: it replays the immutable request
history and response through the harness, uploads the snapshot, and commits the
assistant event, snapshot reference, and idle event under the inference claim.
The direct idle commit requires the log head to still match the inference
request; concurrent appends take the worker replay path so the snapshot cannot
skip their events. Responses needing tools still wake the worker. Worker-side idle transitions also
publish their committed idle event.

The worker claims its session, snapshot reference, and turn identity together.
Inference input, its event, the inflight outbox, and lease release share one
transaction; a folded tool result joins that transaction too. Sandbox call
requests and their fenced dispatch also commit together. Idle, its log
event, and the snapshot reference commit together. Initial tool publication
uses the dispatch transaction's checked placement instead of resolving it
again. Node admission reads share a transaction, and execution still checks
both the current placement epoch and tool claim. Worker and gateway heartbeat
renewals begin at the normal renewal deadline, rather than immediately after
claiming.

Both revisions were built with Rust 1.98.1 using
`CARGO_PROFILE_DEV_OPT_LEVEL=3 cargo build --workspace --bins --locked` and
stripped before copying. This optimizes the workspace crates; the repository
already optimizes dependencies in the development profile, except the large
AWS clients. There were no builds or test suites running during these samples.
The fake script was `scripts/benchmarks/turn-fake.json`, with no model delay.
The pinned `turn-image:bench` was a 256 MiB ext4 image containing bash, sleep,
setsid, and their shared libraries. It executes the same
`printf TURN_TOOL_OK` command as the preceding benchmark section.

Each shape uses its own persistent session and excludes one cold warmup.
Before runs measured five turns per shape; after runs measured thirty.
Percentiles use nearest rank. End-to-end is client monotonic time from
submission through input enabled. Repeated milestones retain every raw event;
the tables show the last occurrence, and inference/tool durations sum their
matched spans. Rendering uses the normal line client writing redirected stdout;
separate PTY tests exercise chat input readiness and terminal restoration.

Local services, client, and root sandbox node run on the launcher. The remote
case keeps the client, scheduler, worker, and gateway on the launcher and puts
FoundationDB, NATS, SeaweedFS, and the root sandbox node on an AWS
`m6id.xlarge` in `us-east-1`, with node volume storage on local NVMe; backing
service data stays on the root EBS disk. Baseline samples used instance
`i-0ded0f4e47b5ecab3`. Final samples used a fresh instance of the same type and
configuration, `i-04aec8f7ccb782c82`, after the baseline node was terminated.
For each revision, the same binary hashes run on both hosts. An OUTPUT firewall rule rejects the launcher's
direct private-subnet connections to ports 4500, 4222, and 8333. The remote
profile uses SSH forwarding, including local port 4500 for FoundationDB.

Final sampling follows `sync` and a three-second settling period. The CLI uses
the head from its idle observation for the atomic append, avoiding a separate
read; competing submissions still fail the store's head/state checks. The fake
gateway skips per-delta timer scheduling when the configured delay is zero;
nonzero scripted delays and its durable call log are unchanged.

**End-to-end results (milliseconds).** Both local shapes meet the proposed
under-100 ms p95 budget: 18.196 ms for text and 66.321 ms for bash. This is a
single warm, sequential run with short conversations, not a concurrency or
long-history guarantee. The remote run improves substantially but does **not**
meet its proposed budget. Fifty TCP connections to SSH measured p50 0.214 ms
and p95 0.239 ms; using the latter gives a target below 100.717 ms. That TCP
measurement does not include application processing or SSH channel queuing.
The remote stage timings retain long claim and handoff delays; this run does
not isolate their cause or demonstrate the three-round-trip target.

| Location | Shape | Before p50 | Before p95 | After p50 | After p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| local | no_tool | 5999.637 | 6000.037 | 16.840 | 18.196 |
| local | bash | 17000.388 | 17999.891 | 59.855 | 66.321 |
| remote | no_tool | 5999.895 | 6001.014 | 90.798 | 204.932 |
| remote | bash | 16838.011 | 18007.351 | 348.638 | 535.562 |

**no_tool, after (milliseconds).**

| Stage | Local p50 | Local p95 | SSH remote p50 | SSH remote p95 |
| --- | ---: | ---: | ---: | ---: |
| appended | 1.733 | 1.857 | 5.037 | 7.607 |
| nudged | 1.747 | 1.867 | 5.051 | 7.619 |
| claimed | 3.789 | 4.000 | 11.927 | 68.515 |
| inference_started | 9.712 | 10.364 | 73.787 | 185.243 |
| inference_finished | 12.505 | 13.340 | 77.985 | 189.423 |
| idle | 16.598 | 17.920 | 90.120 | 204.415 |
| final_text_rendered | 16.822 | 18.176 | 90.574 | 204.913 |
| input_enabled | 16.840 | 18.196 | 90.798 | 204.932 |
| inference_total_duration | 2.696 | 3.162 | 4.151 | 4.272 |

**bash, after (milliseconds).**

| Stage | Local p50 | Local p95 | SSH remote p50 | SSH remote p95 |
| --- | ---: | ---: | ---: | ---: |
| appended | 1.818 | 1.936 | 6.250 | 9.830 |
| nudged | 43.539 | 48.951 | 215.290 | 357.139 |
| claimed | 45.855 | 51.612 | 245.772 | 378.221 |
| inference_started | 52.008 | 58.350 | 283.344 | 513.152 |
| inference_finished | 54.978 | 61.278 | 287.632 | 517.380 |
| tool_dispatched | 21.112 | 22.879 | 190.662 | 329.654 |
| tool_completed | 43.082 | 48.435 | 214.759 | 356.693 |
| idle | 59.584 | 66.101 | 348.096 | 534.973 |
| final_text_rendered | 59.833 | 66.296 | 348.618 | 535.537 |
| input_enabled | 59.855 | 66.321 | 348.638 | 535.562 |
| inference_total_duration | 5.767 | 6.157 | 8.414 | 8.598 |
| tool_total_duration | 22.222 | 25.556 | 26.087 | 30.415 |

Raw samples: [local before](benchmarks/2026-09-17-turn-events-local-before.json),
[local after](benchmarks/2026-09-17-turn-events-local-after.json),
[SSH remote before](benchmarks/2026-09-17-turn-events-remote-before.json), and
[SSH remote after](benchmarks/2026-09-17-turn-events-remote-after.json).
The [machine, clock, routing, binary-hash, and teardown proof](benchmarks/2026-09-17-turn-events-proof.json)
includes rejected direct connections on all three service ports, firewall
counters, matching stripped binary hashes, and both `chronyc tracking` outputs.
Both clocks reported normal synchronization with sub-microsecond system offsets
at the recorded check. Cross-host stages use UTC; end-to-end remains entirely
on the client's monotonic clock.

The exact measurement commands, after selecting the corresponding built binary
directory and starting services with the fake script, were:

```sh
/tmp/swarmy-turn-run/before/swarmy bench turn --turns 5 --image turn-image:bench --output /tmp/swarmy-turn-run/local-before.json
/tmp/swarmy-turn-run/after/swarmy bench turn --turns 30 --image turn-image:bench --output /tmp/swarmy-turn-run/local-after.json
/tmp/swarmy-turn-run/before/swarmy bench turn --remote turn-events --turns 5 --image turn-image:bench --output /tmp/swarmy-turn-run/remote-before.json
/tmp/swarmy-turn-run/after/swarmy bench turn --remote turn-events --turns 30 --image turn-image:bench --output /tmp/swarmy-turn-run/remote-after.json
```

Before used the original 1,000 ms scheduler scan, 5,000 ms resend interval, and
500 ms client poll. After used 5,000 ms scan/resend/poll intervals. All services
used `TOKIO_WORKER_THREADS=4`; nodes advertised 16 computer slots for repeat
benchmark sessions. Only one computer ran measured commands at a time.
One early attempt exhausted the original single slot because a previous
placement was retained; another remote setup attempt omitted required capacity
fields. Both stopped during warmup without producing a complete sample file.
The corrected setup drained pending work before measuring.

**Development runs retained for comparison.** These are intermediate revisions
or different build/sampling conditions, not additional samples of the final
revision. The first two rows used the repository's default unoptimized workspace
profile. `local-after-unoptimized` had the initial event path and transaction
batching. The next two rows added optimized builds, removed redundant routing
and renewal work, and combined tool dispatch and result folding. Terminal
inference completion then removed the final worker claim/idle commit pair.
Its first run immediately followed a build and showed stalls across unrelated
stages; `local-terminal-contended` retains it rather than silently dropping it.
The next run drained disk writes and settled before sampling. The next pair removed zero-delay fake timers and the pre-append store read;
the final run above also includes the correctness guard for concurrent log
appends during inference. Both earlier completed runs remain in the archive.

All completed development samples, including warmups, are in this
[gzipped JSON archive](benchmarks/2026-09-17-turn-events-development.json.gz).
The table reports end-to-end p95 in milliseconds.

| Run | Text p95 | Bash p95 |
| --- | ---: | ---: |
| local-before-unoptimized | 6499.312 | 18000.100 |
| remote-before-unoptimized | 6000.733 | 18351.820 |
| local-after-unoptimized | 42.910 | 157.530 |
| local-intermediate-optimized | 36.163 | 128.584 |
| local-batched-intermediate | 42.863 | 118.784 |
| local-terminal-contended | 25.256 | 1483.549 |
| local-terminal-before-zero-delay | 26.418 | 102.799 |
| local-before-final-guard | 22.341 | 97.920 |
| remote-before-final-guard | 212.272 | 505.798 |

**Validation.** With `scripts/dev-stack.sh start` and `source .dev/env`,
`cargo fmt --all --check`, `cargo test --workspace --locked`, and
`cargo clippy --workspace --all-targets --locked -- -D warnings` passed. The
workspace reported 282 passed tests and one intentionally ignored live-provider
account test. `scripts/chaos-ci.sh --bin-dir "$PWD/target/debug"` passed its
reduced chaos and targeted node-loss, eviction, and managed-process checks.

Root test binaries were built as the normal user with
`cargo test --workspace --locked --no-run --message-format=json`, then selected
from that output and executed with `sudo -E <binary> --nocapture --test-threads=1`
and `SWARMY_TEST_IMAGE=base-ubuntu:bash-test`. The selected suites were all
`swarmy-volume` tests, `swarmyd`'s `node`, `swarmy-chaos`'s `bash`, and the CLI's
`image`, `vol`, and `session` (filtered to `root_chat_default_image_executes_pwd`).
All 41 selected tests passed, including four real bash chaos scenarios, the
12-kill seed-42 run, lease and epoch fencing, and NBD/ext4/fio. The OCI recipe
case initially skipped because skopeo was missing; after installing skopeo and
umoci, rerunning `root_oci_recipe_applies_layer_deletions` as root passed.
The prebuilt `turn` integration test also passed against both final benchmark
stacks. No attached NBD devices remained after cleanup.

**Cloud teardown.** Both temporary `m6id.xlarge` instances, their root volumes,
and imported key pairs were tagged `managed-by=codex-launcher` at creation.
The baseline node's final queries at 2026-09-17 10:59:17 UTC returned:

```text
aws ec2 describe-instances --instance-ids i-0ded0f4e47b5ecab3
[{"InstanceId":"i-0ded0f4e47b5ecab3","State":"terminated"}]
aws ec2 describe-volumes --filters Name=tag:Name,Values=swarmy-turn-events-1789638028
[]
aws ec2 describe-key-pairs --filters Name=key-name,Values=swarmy-turn-events-1789638028
[]
```

The final revision's replacement node was also removed. Its queries at
2026-09-17 11:32:53 UTC returned:

```text
aws ec2 describe-instances --instance-ids i-04aec8f7ccb782c82
[{"InstanceId":"i-04aec8f7ccb782c82","State":"terminated"}]
aws ec2 describe-volumes --filters Name=tag:Name,Values=swarmy-turn-events-1789644115
[]
aws ec2 describe-key-pairs --filters Name=key-name,Values=swarmy-turn-events-1789644115
[]
```

The displayed results use the field projections retained in the proof JSON.
The local SSH tunnel and private-route rejection rule were removed and the local
dev stack was restored. This run used the remote node's SeaweedFS, created no
external S3 objects or buckets, and used no Google Cloud resources.

## 2026-09-17 Remote round trips and node services

The warm path was counted before changing it, using the event-driven remote
"after" table above. Counts below cover the client, worker, and gateway;
execution-node transactions remain local to the store. Tool completion there
also drops its preliminary session fetch and overlaps independent fencing reads. Each read transaction
also needs a read version, and each mutation needs a commit acknowledgement.
A read phase means independent reads can be in flight together. These are
logical operations, not a packet capture: FoundationDB may reuse read versions,
cache reads within a transaction, retry, or split a range page.

| Warm operation | Previous transactions | Current transactions | Previous serial read phases | Current serial read phases |
| --- | ---: | ---: | ---: | ---: |
| Client append and wake | 1 | 1 | 3 | 1 |
| Worker claim plus first replay page | 2 | 1 | 5 | 2 |
| Worker inference submission | 1 | 1 | 4 | 1 |
| Gateway inference claim | 1 | 1 | 4 | 1 |
| Gateway header fetch and completion | 2 | 1 | 7 | 1 |
| Worker warm placement lookup | 1 | 0 on cache hit | 1 | 0 |

Text uses one claim, submission, and inference: **7 to 5 transactions**.
A one-bash turn uses three claims, two submissions/inferences, one placement
lookup, and one fenced dispatch: **17 to 11 transactions** on a cache hit.
Dispatch still reads and verifies placement in its commit. Cold placement,
expired cached routes, concurrent notices, oversized event pages, and recovery
can add operations. Snapshot downloads are cached by immutable object key, so
the three worker claims of a bash turn reuse the same snapshot after its first
download. Session image pins are immutable and cached; the resident-computer
path already reads image metadata only when creating a disk or rebuild notice.

NATS retains durable publication acknowledgements: two publications for text
(runnable, inference), six for bash (three runnable, two inference, one tool).
Corresponding work deliveries still use confirmed acknowledgements (two for
text, six for bash, of which one is local to the node). Each delivery also
uses the existing pull consumer. These are not
synchronous tool RPCs: tool results commit on the node and wake the worker.
Independent tool publications now overlap their acknowledgement waits. We do
not count recovery pulls, telemetry, or background scans as per-turn RPCs.

`--services node` moves all worker, gateway, and scheduler store traffic and
NATS handoffs to the node. The laptop still appends a user message, publishes a
nudge, and observes the live durable event sequence through SSH.


**Real-node measurements.** Both modes used the same release binaries and the
same AWS `m6id.xlarge`, `i-0030ed4607356cea9`, in us-east-1, with its local NVMe
cache and a 100 GB root disk. Provisioning used the launcher-tagged credentials
and the supported command:

```sh
swarmy remote up turn-roundtrips --services node --image-recipe images/turn-proof
swarmy remote connect turn-roundtrips --json
swarmy dev up --remote turn-roundtrips
swarmy bench turn --remote turn-roundtrips --turns 30 --image base-ubuntu:turn-roundtrips --output /tmp/swarmy-turn-node-final.json
```

The temporary `images/turn-proof` recipe contained bash, sleep, setsid, their
shared libraries, and a `/bin/sh` link in a 256 MiB ext4 image. Both configurations
used the same registered image and `scripts/benchmarks/turn-fake.json`, including
real `printf TURN_TOOL_OK` bash execution. No provider credential was copied.
The laptop comparison stopped the three node control-plane units, changed the
saved launch setting to `laptop`, and ran `dev up --remote` and the same benchmark
with output `/tmp/swarmy-turn-laptop-final.json`. The setting was restored to
`node` before the final node-mode run. This administrative comparison reused one
machine; changing an existing deployment's mode is not a new CLI operation.

The launcher client and laptop services ran as ubuntu in a network namespace.
Its owner-matched OUTPUT rules rejected direct connections to the node's private
address on 4500, 4222, and 8333; port 22 remained reachable. Only localhost SSH
forwards could reach the backing services. Both hosts reported synchronized
clocks; end-to-end measurements use only the client's monotonic clock. Each
shape had 30 measured turns and one excluded cold warmup. TCP connect to SSH
had p95 0.259 ms over 40 samples, so the three-round-trips-plus-100-ms budget was
**100.776 ms**. This measures a same-region launcher through SSH, not an emulated
cross-continent WAN.

| Configuration | Text p50 ms | Text p95 ms | Bash p50 ms | Bash p95 ms |
| --- | ---: | ---: | ---: | ---: |
| Previous event-driven SSH run (above) | 90.798 | 204.932 | 348.638 | 535.562 |
| Laptop services, final | 24.279 | 26.021 | 74.646 | 81.154 |
| Node services, final | 21.940 | 22.828 | 64.427 | 68.287 |

Both final configurations meet the measured budget for both shapes. The previous
run used a different temporary node, so it is historical context, not a paired
control. Final raw samples, including warmups, are retained in
[the laptop artifact](benchmarks/2026-09-17-roundtrips-laptop-final.json.gz) and
[the node artifact](benchmarks/2026-09-17-roundtrips-node-final.json.gz).

Stage values below are elapsed from submission; inference and tool durations
sum their respective spans. Repeated stage names use their last occurrence.

| Stage | Laptop text p50/p95 ms | Laptop bash p50/p95 ms | Node text p50/p95 ms | Node bash p50/p95 ms |
| --- | ---: | ---: | ---: | ---: |
| appended | 3.027/3.185 | 3.093/3.316 | 3.074/3.327 | 3.052/3.151 |
| claimed | 6.926/7.849 | 57.826/64.314 | 5.947/6.491 | 49.347/53.216 |
| inference_started | 15.459/17.026 | 65.397/71.743 | 12.469/13.399 | 54.730/58.594 |
| inference_finished | 18.266/19.787 | 68.482/74.644 | 16.982/17.869 | 59.336/63.431 |
| inference_total_duration | 2.761/3.543 | 5.987/7.290 | 4.504/4.763 | 9.210/9.512 |
| tool_dispatched | — | 29.950/33.973 | — | 24.852/26.370 |
| tool_completed | — | 53.226/59.666 | — | 46.082/49.806 |
| tool_total_duration | — | 23.235/26.779 | — | 20.944/24.402 |
| idle | 23.744/25.149 | 74.131/80.646 | 21.461/22.183 | 63.989/67.878 |
| final_text_rendered | 24.267/26.008 | 74.633/81.114 | 21.878/22.786 | 64.413/68.272 |
| end_to_end | 24.279/26.021 | 74.646/81.154 | 21.940/22.828 | 64.427/68.287 |

The first development runs still missed budget: laptop text/bash p95 were
161.336/388.484 ms and node text/bash p95 were 33.700/135.678 ms. Their raw
samples are retained as `roundtrips-laptop.json.gz` and `roundtrips-node.json.gz`
with the same date prefix. Opening an exec session on the existing SSH master
removed recurring stalls near 40 ms: a ten-turn diagnostic reached
39.309/107.832 ms (the `roundtrips-laptop-nodelay.json.gz` artifact).
The final connection code reads the cluster file over the forwarding master.
OpenSSH's server transport enables TCP_NODELAY when a session opens; a bare
`-N` master does not take that path. See
[OpenSSH 9.6 packet.c](https://github.com/openssh/openssh-portable/blob/V_9_6_P1/packet.c)
(`ssh_packet_set_interactive`). Final runs used a newly connected master and
also include the tool-completion batching. Existing tunnels should be
disconnected and reconnected after upgrading. No durability acknowledgement,
lease check, timing threshold, or benchmark warmup rule was removed.


**Node-mode acceptance.** `dev up --remote turn-roundtrips` printed
`services: running on node turn-roundtrips; no local services started`.
All three control-plane units and `swarmyd` were active, and the remote credential
file was absent. A real PTY ran `swarmy chat --remote turn-roundtrips`, sent a
bash turn, killed `swarmyd` with
`sudo systemctl kill --kill-whom=main --signal=SIGKILL swarmyd.service`, and sent
another turn. The durable transcript contains one recovery notice at sequence
14, an interrupted call recorded as failed at sequence 15, and a successful
fresh bash call after reopening chat. Recovery waited for the crashed writer's
lease; it did not replay an uncertain tool side effect. This kills the execution
process, not the EC2 host holding the backing store. The
[durable chat transcript](benchmarks/2026-09-17-roundtrips-chat.jsonl) and
[proof artifact](benchmarks/2026-09-17-roundtrips-proof.json) retain the outcomes,
blocked-route checks, matching release binary hashes, and teardown results.
The final credential-exclusion path regression test was added after these
measurements; it changes checkout copying, not the measured turn path.

**Validation.** With `scripts/dev-stack.sh start` and `source .dev/env`:

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
bash -n scripts/remote-services.sh
```

The three CI commands and shell syntax check passed: 302 workspace tests passed
across 56 suites, with one live-provider account test intentionally ignored.
Fake cloud/host tests cover
node services with and without the explicit credential flag, refusal before
resource creation when ChatGPT credentials are not acknowledged, and saved
service mode. A CLI integration test verifies that node mode starts no local
processes without installed service binaries. Routing tests cover cached expiry,
early eviction, renewal, and epoch fencing. Checkout-copy exclusions also cover
symlink targets and paths containing `..`.

Privileged binaries were built without sudo:

```sh
cargo test --workspace --locked --no-run --message-format=json
sudo -E <test-binary> --nocapture --test-threads=1
```

The selected suites were `swarmy-volume`'s library, `image`, and `nbd`,
`swarmyd`'s `node`, `swarmy-chaos`'s `bash`, and the CLI's `image`, `vol`, and
`session` filtered to `root_chat_default_image_executes_pwd`. The bash scenarios
used `SWARMY_TEST_IMAGE=base-ubuntu:roundtrip-root`, built using
`sudo -E target/debug/swarmy image build images/base-ubuntu --tag roundtrip-root`.
OCI, ext4/fio, image builds, node recovery, lease fencing, managed processes,
and all four bash chaos scenarios ran as root. The NBD retained-image test
initially failed its 30 ms writer-gap bound under concurrent load (41.672 ms).
Its isolated rerun passed at 24.267 ms with all four crash images mountable and
filesystem checks passing; the bound was unchanged. An additional node-test
rerun overlapped the CLI integration test restarting the shared dev stack and
failed on a backing-store error. Its isolated final rerun passed all recovery,
takeover, and graceful-shutdown checks in 205.94 seconds. No attached NBD
devices remained after testing.

One additional privileged test remains failing:
`root_volume_durability_clone_crash_fencing_and_history` expects
`stats.frozen > Duration::ZERO` after `vol flush --mount` without `--freeze`.
The existing CLI defaults to an unfrozen block boundary, so that field is zero.
The isolated rerun reproduced the assertion. The test and volume-flush code are
unchanged by this task; this failure is recorded in
[the captured log](benchmarks/2026-09-17-roundtrips-volume-failure.txt).
The other two CLI volume tests passed. The live ChatGPT account test remains
intentionally ignored; no real account credential transfer was performed.

**Cloud teardown.** `swarmy dev down`, `swarmy remote disconnect turn-roundtrips`,
and `swarmy remote down turn-roundtrips` completed. Final AWS queries returned:

```text
aws ec2 describe-instances --instance-ids i-0030ed4607356cea9
[{"InstanceId":"i-0030ed4607356cea9","State":"terminated"}]
aws ec2 describe-volumes --filters Name=volume-id,Values=vol-0f5e6eb21211f2d79
[]
aws ec2 describe-key-pairs --filters Name=key-name,Values=swarmy-01M2QMGDVPV7D6N0DZ4NWC2P4A
[]
gcloud compute instances list --format=json
[]
```

The AWS output uses the field projections in the proof artifact. The only
remaining running AWS instance is the launcher, `i-074ffdebcc7a6968c`.
This run used SeaweedFS on the terminated node and created no external S3
objects, GCP buckets, or HMAC keys. The SSH master, network namespace, route
rejection rules, and NAT rule were removed, and IP forwarding was restored.
