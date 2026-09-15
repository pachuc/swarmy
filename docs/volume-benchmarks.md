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
