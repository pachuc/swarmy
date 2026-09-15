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
as session lease release does. Future head updates must also check that token;
head advancement, upload flush, and CLI commands are later tasks.

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
readahead. Cache eviction and dirty-chunk uploads are later work.

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
