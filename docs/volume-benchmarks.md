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
