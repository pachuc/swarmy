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
GCP tries N2 with NVMe local SSD in the central, east, and west zones before
falling back to a 500 GB `pd-ssd`; stdout and state record which cache was used.
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
