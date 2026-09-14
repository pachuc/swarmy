# Scheduler

The scheduler scans FoundationDB and publishes versioned `swarmy_core::Nudge`
messages on `sched.runnable.{partition}` through `Bus::publish_work`. A nudge
contains only `session_id`; workers must claim a store lease before doing work.
The scheduler also reaps expired leases and serves `sched.wake` over core NATS
request-reply. It has no durable local state.

Start the dev stack, source `.dev/env`, then run:

```sh
SWARMY_SCHEDULER_PARTITIONS=0-255 cargo run --locked -p swarmy-scheduler
```

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `SWARMY_SCHEDULER_PARTITIONS` | `0-255` | Owned partitions, as a range or comma list; mixed ranges also work, such as `0-63,128,200-255`. |
| `SWARMY_SCHEDULER_SCAN_INTERVAL_MS` | `1000` | Positive interval between runnable scans and between lease scans. Both run immediately at startup. |
| `SWARMY_SCHEDULER_RESEND_INTERVAL_MS` | `5000` | Minimum time before a successful nudge is repeated by a runnable scan. |
| `SWARMY_STORE_DIRECTORY` | `swarmy` | FoundationDB directory path; `/` separates components. |
| `SWARMY_BUS_PREFIX` | none | Prefix for NATS subjects and stream names. |
| `RUST_LOG` | `info` | Tracing filter; session events carry a `session_id` field. |

`SWARMY_FDB_CLUSTER_FILE`, `SWARMY_NATS_URL`, and the six `SWARMY_S3_*`
settings documented in [docs/DEV.md](../../docs/DEV.md) are required.

Runnable scans paginate each owned partition and skip entries until `wake_at`.
Lease scans paginate the expiry index and act only on owned partitions. Reaping
checks the full lease again in a transaction, so a renewal or another reaper
winning the race is harmless. A failed scan or nudge is retried on a later pass.
Resends occur on the first scan after their deadline; a large scan can take
longer than the configured interval. Recent nudge timestamps expire from memory.
Restarting a scheduler immediately rediscovers durable runnable entries.

Call `Bus::request_wake(session_id, timeout)` from the CLI or channels service.
`WakeReply::Runnable` means the session was made runnable or was already
runnable; workers can claim it before the caller receives the reply.
Other states are reported as `Unchanged(state)`. Missing sessions return
`NotFound`, and store failures return `Failed(message)`. A transport failure,
absent scheduler, or expired request deadline returns an error naming the
scheduler. Retrying a wake is safe and preserves an existing runnable schedule.

All schedulers in a deployment share the wake request queue. Any instance can
atomically wake an idle session. It nudges immediately when it owns that
partition; otherwise the owning instance's next scan publishes the nudge.
Deploy instances whose partitions collectively cover the sessions in use.
Overlapping ownership can produce duplicate nudges, which workers must tolerate.

The integration tests run the binary as child processes with unique store
directories and bus prefixes. They cover scan latency, resends, future wake
times, pagination, partition isolation, wake replies, expired and live leases,
and recovery after SIGKILL. They skip with a message when the FoundationDB or
NATS environment setting is absent.
