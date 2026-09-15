# Virtual disk storage

`ChunkStore` takes an `Arc<dyn object_store::ObjectStore>`. Every disk block is
256 KiB. `put_chunk` hashes the raw block bytes with BLAKE3, performs HEAD, and
returns the hash and whether it uploaded. Conditional creation handles races
between writers of the same content. Errors other than NotFound are propagated.
`get_chunk` checks the decoded size and content hash.

Chunks use `chunks/{hash[0:2]}/{hash}` with lowercase hexadecimal hashes.
Payloads use `swarmy_core::encode` and `decode`, including chunk bytes, so every
stored value carries the storage version. The all-zero 32-byte hash is reserved:
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
head advancement, flush, block device serving, and CLI commands are later tasks.
