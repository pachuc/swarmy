# Virtual disk storage

`ChunkStore` takes an `Arc<dyn object_store::ObjectStore>`. Every disk block is
256 KiB. `put_chunk` hashes the raw block bytes with BLAKE3, performs HEAD, and
returns the hash and whether it uploaded. Conditional creation handles races
between writers of the same content. Errors other than NotFound are propagated.
`get_chunk` checks the decoded size and content hash.

Chunks use `chunks/{hash[0:2]}/{hash}` with lowercase hexadecimal hashes.
Chunk objects hold the raw block bytes; the hash in the object name verifies
them, and raw objects can be read by range and by other tools. Manifest objects
use `swarmy_core::encode` and `decode`, so they carry the storage version. The
all-zero 32-byte hash is reserved:
zero blocks are never uploaded, and reads synthesize zeros without storage I/O.
A nonzero block whose hash equals the sentinel is rejected.

## Manifest format

A leaf contains 4,096 chunk hashes, covering 1 GiB. The final leaf is padded with
zero hashes beyond the disk size. A root contains leaf hashes in disk order.
Leaves and roots have distinct enum tags inside the versioned encoding. Their
addresses are the BLAKE3 hashes of those encoded bytes, stored at
`manifests/{hash}`. This refines the design's manifest object prefix to allow
sharing individual objects between manifests. Manifest ULIDs identify headers
in FoundationDB, independently of these content addresses.

Zero leaves are represented by the zero sentinel and never stored. An entirely
empty root also uses the sentinel, so an empty disk has no object storage cost.
A sparse 32 GiB disk has 32 root entries and stores only its nonzero leaves.

Create a disk with `Manifest::empty(size)`, then pass it or a previously loaded
manifest to `ManifestBuilder::new`. Queue `(block index, chunk hash)` changes
with `set_chunk` and call `build`. The builder loads and encodes only touched
leaves and then writes the root. Unchanged leaves retain their hashes; repeated
updates to a block use the final hash. A no-op update produces no new objects.
`Manifest::load` fetches the root; `chunk_hash` lazily fetches a leaf. Reads verify
content addresses, object types, lengths, and final-leaf padding.

Upload chunks before building the manifest. After `build` succeeds, register its
header under a fresh `ManifestId` using `Store::put_manifest`. Failed builds can
leave unreferenced immutable objects for later garbage collection.

## FoundationDB records

Shared ids, hashes, headers, and volume records live in `swarmy-core`.
`swarmy-store` owns the versioned tuple keys:

- `("manifest", id)` holds `{ size, chunk_size, root_hash }`. An id cannot be
  replaced with a different header.
- `("manifest_parent", id)` records immutable provenance, independent of retention.
- `("volume_snapshots", id)` holds retained manifest ids, newest first.
- `("volume", id)` holds `{ head_manifest, writer_lease, parent }`.
- `("image", name, tag)` maps an image name and string `ImageTag` to a manifest id.
- `("volume_lease_seq", id)` retains the last writer grant sequence even after
  release, preventing an old token from matching a later acquisition.

`create_volume` checks the manifest and creates an unleased volume.
`clone_volume` reads the source volume and writes a new unleased record with
`parent` set to the source. Its transaction does not read the manifest header,
root, leaves, or chunks, so its work is independent of manifest size.

`acquire_writer_lease` accepts caller-supplied time and expiry and succeeds only
when the writer is absent or expired. Every grant increments the fencing
sequence. `release_writer_lease` requires the complete matching, live `Lease`,
as session lease release does. Head advancement checks both the token and the expected previous manifest.

## Local block device

`VolumeDevice::open(chunks, manifest, cache_dir, dirty_dir, readahead_chunks)`
opens a single writer. Reads and writes use 4 KiB alignment and accept at most
32 MiB per request. Trim accepts larger aligned ranges and processes them in
bounded batches. The dirty directory contains a sparse `data` file, a byte
per 4 KiB block in `map`, and the manifest header in `manifest`. Reopening with
the same manifest preserves local writes and trims. An advisory lock prevents
two local writers from opening the same directory. Use a distinct dirty
directory for each volume, including volumes with identical manifests. The
caller remains responsible for the durable volume writer lease.

Reads check dirty blocks first. Clean reads resolve manifest leaves lazily,
verify cached chunks by hash, and fetch missing or corrupt cache entries from
object storage. Cache files are named by content hash and replaced atomically,
so devices can share a cache directory. After contiguous reads, one background
task concurrently fetches the next configured number of chunks (capped at 32). Zero disables
readahead. Cache eviction is later work; background dirty-chunk uploads are
described below.

`stats()` returns `DeviceStats`: cache hits, foreground cold reads, readahead
hits, speculative object fetches, and bytes currently covered by dirty blocks.
Cache counters count chunk lookups, not 4 KiB blocks. A readahead hit is the
first demand lookup of a chunk fetched speculatively by that device and is
also a cache hit. Foreground cold reads plus readahead fetches count object
fetch attempts, including failures. Readahead reduces foreground misses and
waits; it does not reduce the total bytes needed for a complete sequential scan.
Zero chunks never increment object-fetch counters.

`flush()` synchronizes local data, the block map, and directory entries. NBD
flush has this same local durability boundary. It does not upload chunks or
advance a durable manifest. Trim writes dirty zero blocks, retaining the
knowledge that old manifest content has been discarded. It does not reclaim
local disk space yet. Unflushed data has no crash-consistency guarantee.

### NBD interfaces

`NbdServer::bind(path, device)` creates a Unix listener; `run()` negotiates and
serves one connection at a time. Dropping the server removes its socket path.
`nbd::serve_connection` also accepts an existing Tokio Unix stream. The export
name is empty. Fixed-newstyle negotiation supports `EXPORT_NAME`, `GO`, `INFO`,
and `ABORT`, including the optional legacy zero padding. `GO` clients must
request `NBD_INFO_BLOCK_SIZE` because the minimum request alignment is 4 KiB.
Transmission supports simple replies for read, write, flush, trim, and
disconnect. FUA, structured replies, and multiple connections are not advertised.
Malformed bounded writes are consumed before replying with an error; oversized
reads and writes disconnect before allocation.

On Linux, `kernel::Attachment::attach("/dev/nbd0", device).await` negotiates a
socketpair, configures an unused NBD device, and starts `NBD_DO_IT` on a dedicated
thread. It waits for the kernel to publish the capacity before returning.
The caller must keep the Tokio runtime running, unmount, and flush before
`attachment.detach().await`. Detach disconnects, clears queued requests and the socket, and joins the kernel
thread. Kernel requests have a 30-second timeout. Drop also disconnects as a cleanup fallback. Device nodes
such as `/dev/nbd0` remain installed by the kernel; detach removes the active
connection and capacity so the number can be reused. Only the small ioctl
wrapper opts into unsafe code.

### Kernel integration test

The protocol and device tests run without privileges. The kernel test logs a
skip message without root. Build as the ordinary user, then run that executable
with sudo:

```sh
cargo test -p swarmy-volume --test nbd --no-run --message-format=json > /tmp/swarmy-nbd-build.json
sudo -E "$(jq -r 'select(.executable != null and .target.name == "nbd") | .executable' /tmp/swarmy-nbd-build.json)" --nocapture
```

The test needs loaded NBD devices, `mkfs.ext4`, `mount`, `umount`, `sha256sum`,
`fio`, and `dd`. It reserves an unused device, formats a 512 MiB empty volume,
writes 200 MiB of mixed-size files, verifies every SHA-256 after remount and
after reopening the dirty store, runs 4 KiB random writes and reads with fio,
checks detach and reuse, and compares direct sequential reads with readahead
zero and four over an object-backed manifest. A cleanup guard unmounts and
detaches on failure. Statistics and fio summaries are emitted through tracing.

## Durable flush and attachment control

`VolumeWriter` holds the volume id, writer lease, and current manifest id.
`flush(mount)` freezes a known filesystem with `fsfreeze --freeze`, blocks local
writes while it uploads pending chunks and builds a manifest, then records the
header, predecessor link, and volume head in one FoundationDB transaction.
The transaction checks the complete live writer token and expected head on every
attempt. The mount is unfrozen after success or failure, with a Drop fallback.
Without a mount, the write lock captures a single point in the block request
stream; ext4 replays its journal on the next mount.

`background(interval)` uploads pending chunks continuously. It releases the
write lock between chunks. A write invalidates that chunk's uploaded hash, so
flush only uploads chunks that have changed since their background upload.
Errors retain pending changes for retry. Untouched manifest leaves are reused.
Background uploads alone do not advance the durability boundary.

The local overlay remains the read source until detach, even after publication;
`dirty_bytes` measures this local overlay, not the outstanding upload backlog.
Each fresh CLI attachment uses a new dirty directory and the committed head.
After a crash, abandoned local directories can be removed once their server is
dead. They are never replayed by a new CLI attachment. Direct callers of
`VolumeDevice::open` can still explicitly resume local data as before.

The node daemon serves these developer commands directly from the store as
`swarmyd vol` (the `swarmy` client no longer links the store):

```sh
swarmyd vol create base-ubuntu:stable
sudo -E swarmyd vol attach VOLUME --background
# The command prints /dev/nbdX and remains in the foreground.
# In another terminal:
sudo mount /dev/nbdX /mnt/agent
sudo -E swarmyd vol flush VOLUME --mount /mnt/agent
sudo -E swarmyd vol checkpoint VOLUME
sudo -E swarmyd vol snapshot VOLUME
swarmyd vol clone VOLUME
sudo -E swarmyd vol detach VOLUME
swarmyd vol ls
swarmyd vol show VOLUME
```

Every command accepts `--json`. Attach emits one ready record; list emits one
record per volume; show includes retained snapshots, newest first.
Snapshot flushes a live local writer and returns its immutable manifest id.
For an unattached volume it returns the last committed manifest id. Clone
always uses the last committed manifest; snapshot first to include local writes.
Clones begin unleased and share immutable objects, with independent local writes.
Their retained history starts at the cloned head; source retention is independent.

Attach selects an unused `/dev/nbd0` through `/dev/nbd15`, or accepts `--device`.
It renews a 60-second writer lease every 15 seconds. Flush, checkpoint, snapshot, and detach
send requests to `.swarmy/volumes/VOLUME.sock` under the discovered configuration
root. Invoke commands with the same configuration root and node id; root-created
sockets generally require sudo for control commands too. A local lock protects
socket replacement when cleaning up a dead server. Mounts are discovered with
`findmnt`; an explicit `--mount` must match the attached device. Multiple mounts
must be reduced to one before control operations.

Detach unmounts the filesystem, performs a final durable flush, disconnects the
kernel device, and releases the writer lease. SIGINT and SIGTERM use the same
sequence. An unmount or flush error is reported; a failed control request leaves
the foreground server available for retry. SIGKILL loses changes since the last
completed flush. Another node can attach after the old lease expires.

Shared configuration accepts `node_id` or `SWARMY_NODE_ID` (a ULID). Otherwise it
creates and reuses `.swarmy/node-id` under a file lock. Two servers on one host
can represent different nodes by setting different `SWARMY_NODE_ID` values.
The control socket rejects a request from a different node, and FoundationDB
independently fences every durable publication with the full lease token.

### Durability acceptance test

The CLI test uses the real dev stack's FoundationDB and SeaweedFS through
`SWARMY_FDB_CLUSTER_FILE` and `SWARMY_S3_*`. It formats a small ext4 base, starts
separate foreground servers, and tests cross-node recovery, simultaneous clone
writes, SIGKILL rollback, lease rejection, and list/history output. It skips
without root or the required environment. Build without sudo:

```sh
scripts/dev-stack.sh start
source .dev/env
cargo test -p swarmy-cli --test vol --no-run --message-format=json > /tmp/swarmy-vol-build.json
sudo -E "$(jq -r 'select(.executable != null and .target.name == "vol") | .executable' /tmp/swarmy-vol-build.json)" --nocapture
```

The test's cleanup guards unmount and stop servers on failure. The crash test
waits for the real writer lease to expire before reattaching. Unit tests use
`InMemory` object storage to check partial-chunk uploads, background invalidation,
failed-publication retry, trim, and untouched-leaf reuse.

### Shared attachment service

`server::attach` is the shared foreground service used by the CLI and swarmyd.
It accepts `ServerConfig` (node id, local directory, store, and object storage),
a volume id, an optional device path, a background-upload flag, a readiness
callback, and a shutdown future. Readiness follows kernel capacity publication.
`server::control` returns the manifest id after flush or detach. The service
owns the local lock, control socket, overlay, kernel attachment, background
uploader, snapshot loop, and renewal task for its whole lifetime. Callers supply shutdown
policy and presentation; the CLI supplies signals and prints readiness, while
the sandbox runtime mounts the ready device and controls detach itself.

### Periodic snapshots and retention

Every attachment runs a snapshot loop, including when `--background` is absent.
It waits 600 seconds by default between attempts. Only unpublished chunk changes
trigger a flush; already uploaded but unpublished chunks still count as dirty.
An idle attempt neither freezes the filesystem nor accesses FoundationDB.
Each attempt discovers the current mount before freezing it. Periodic publication,
control requests, and detach serialize so publication cannot race unmounting.
Failed publications retain dirty data and retry on the next period.

`swarmyd vol checkpoint VOLUME [--mount PATH]` immediately publishes through the
local control socket and prints the new manifest id. It creates a new snapshot
even on an idle disk. `VolumeWriter::checkpoint` and `server::checkpoint` expose
the same operation to library callers. The existing flush API remains available.

Shared configuration controls the period and retention for CLI and node
attachments. Both values must be positive:

```toml
[volume_snapshots]
period_seconds = 600
retention = 10
```

Environment overrides are `SWARMY_VOLUME_SNAPSHOT_PERIOD_SECONDS` and
`SWARMY_VOLUME_SNAPSHOT_RETENTION`. The shared attachment server loads the policy
when attaching. Direct writers can use `VolumeWriter::with_retention`.

Every successful publication atomically retains the newest ten snapshots by
default, including the new head. Creation and cloning retain their initial head.
Existing volume records without explicit history import the newest retained
entries from their parent links.
Retention removes only per-volume history entries: manifest headers, immutable
parent links, and object data remain for the garbage collector. Parent links do
not keep manifests live and do not define the history shown by the CLI.

`Store::live_manifests` returns the union of all retained snapshots, all attached
heads, and every registered image manifest at one database read version. A writer
lease counts as attached until released, including after expiry. The query
excludes unreferenced headers and pruned ancestors unless another volume or
image retains them. It reads scan pages in one transaction, so FoundationDB's
transaction time and size limits apply. A future collector must also protect
publications concurrent with its sweep; this query alone does not authorize
object deletion.

### Flush measurements

`VolumeDevice::upload_stats()` returns cumulative counters for an attachment.
The device counts successful chunk PUTs, successfully uploaded bytes (including
manifest objects), and attempted object-store HEAD, GET, and PUT calls. Zero
chunks do not issue storage requests; deduplicated chunks issue HEAD without a
PUT. Internal HTTP retries inside `object_store` are not separately counted.
Initial manifest loading before the device opens is outside these counters.

`dirty_lock_wait` is a `Duration` summed across dirty-store lock acquisitions,
including reads, writes, background staging, and publication. It is aggregate
waiting time and can exceed elapsed time when callers overlap.
`object_store_time` sums HEAD/GET/PUT call time; GET body consumption is outside
that timer. It includes client work and network/service latency, so subtracting
it from wall time does not produce a CPU profile.

`VolumeWriter::flush()` returns `FlushResult`. `swarmy --json vol flush` prints
its manifest id, `elapsed`, `freeze_wait`, `frozen`, `uploads`, and `device_total`.
Durations use serde's `{ "secs": ..., "nanos": ... }` representation.
`uploads` is the counter difference from just before freeze acquisition through
thaw; `device_total` includes prior background work. Activity from concurrent
background uploads or readahead in that interval is included. `elapsed` excludes
waiting for another writer flush; CLI round-trip timing includes that wait.
`frozen` measures from freeze command completion through thaw completion and is
zero for an unmounted device. It excludes freeze acquisition and kernel
writeback, which are reported as `freeze_wait`. Successful flushes emit an info
tracing event, and background batches emit a debug event with their counters.

For release and debug installation measurements and serial request latency,
see `scripts/benchmarks/volume.py --help`, the `cloud-object-requests` example,
and the dated procedure in `docs/volume-benchmarks.md`.
