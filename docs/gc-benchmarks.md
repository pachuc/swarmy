# Local chunk collector measurement

Measured on 2026-09-23 on the task's Ubuntu 24.04 EC2 host: four cores,
15 GiB RAM, FoundationDB and SeaweedFS on the same host. The CLI used the
Cargo debug profile, with dependencies optimized according to the workspace
configuration.

`scripts/benchmarks/gc.sh` creates an isolated metadata directory and S3 bucket,
builds a sparse 512 MiB image, attaches it through `swarmy vol`, and performs
22 rounds of 256 MiB fresh random block writes followed by CLI checkpoints.
Retention is three snapshots. This creates 22,528 unique workload chunks plus
six base-image chunks. The attached writer has no pending writes during
collection. The benchmark uses a one-second grace solely to avoid waiting six
hours for this quiescent fixture. The normal default remains six hours.

The collector uses the default 64 MiB reference filter, sixteen concurrent
prefix listings, pages of 256 candidates, and 32 concurrent object deletes.
The timer and deletion reservations use the same production lease code as CLI
runs.

| Run | Scanned | Candidates | Deleted | Deleted bytes | Collector duration | Process wall time | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Dry run | 22,534 | 19,456 | 0 | 0 | 1.023 s | 1.06 s | 113,896 KiB |
| Collection | 22,534 | 19,456 | 19,456 | 5,100,273,664 | 4.603 s | 4.65 s | 116,392 KiB |
| Immediate second collection | 3,078 | 0 | 0 | 0 | 0.437 s | not measured | not measured |

Deleted bytes are object payload bytes (4.75 GiB). SeaweedFS reclaims underlying
volume-file space through its own compaction. Collection completed within the
initial 120-second lease. The script asserts that
the dry run deletes nothing, the real run deletes the predicted count, and
the following run finds no candidates. All assertions passed. Peak RSS includes
the CLI runtime, FoundationDB client, S3 client, reference filter, and listing
buffers; it is measured for the collector process with `/usr/bin/time -v`.

## Reproduction

```sh
scripts/dev-stack.sh start
source .dev/env
cargo build -p swarmy-cli --bins --locked
scripts/benchmarks/gc.sh
```

The script prints its temporary evidence directory and isolated namespace.
It saves the image, attachment, checkpoint, dry-run, collection, and second-run
JSON plus both timing reports, and detaches its device in an exit trap. It
retains the test namespace and objects for inspection.
