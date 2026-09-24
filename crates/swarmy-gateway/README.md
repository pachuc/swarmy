# Inference gateway

`swarmy-gateway` consumes `WorkQueue::Inference` for one provider class. Workers
publish a versioned `swarmy_llm::InferenceJobRef` after recording the session as
`WaitingInference`. The gateway verifies the deterministic request id, claims
the job, and loads its `Request` from the store before calling the provider.

| Environment variable | Meaning / default |
| --- | --- |
| `SWARMY_PROVIDER` | Required: `chatgpt` or `fake` |
| `SWARMY_GATEWAY_CONCURRENCY` | Maximum concurrent deliveries and provider calls, default `4` |
| `SWARMY_FDB_CLUSTER_FILE` | Required FoundationDB cluster file |
| `SWARMY_STORE_DIRECTORY` | Directory path, separated by `/`, default `swarmy` |
| `SWARMY_NATS_URL` | Required NATS connection URL |
| `SWARMY_BUS_PREFIX` | Optional subject and stream prefix |
| `SWARMY_BUS_ACK_WAIT_MS` | Acknowledgement deadline, default `30000`, minimum `30` |
| `SWARMY_BUS_MAX_DELIVER` | Delivery limit, default `5`; must match existing consumers |
| `SWARMY_S3_*` | Blob settings from [DEV.md](../../docs/DEV.md) |
| `SWARMY_CHATGPT_AUTH` | Required for `chatgpt`: dedicated credential file used by `FileCredentialStore` |
| `SWARMY_FAKE_SCRIPT` | Required for `fake`: JSON script path |
| `SWARMY_FAKE_CALL_LOG` | Required for `fake`: append-only call count file |
| `RUST_LOG` | Tracing filter, default `info` |

A short FoundationDB claim serializes duplicate deliveries across gateway
processes. The existing `Requested` idempotency state means started but not
completed. Both the claim and NATS deadline are renewed every third of the ack
wait. After a crash the claim expires and the unacknowledged job can run again.
Provider errors use exponential negative-acknowledgement backoff starting at
100 ms and capped at 3.2 seconds. The final attempt records `InferenceFailed`.

Every provider delta, including `Completed`, is published on
`LiveFeed::ModelDeltas(session_id)`. Live publication is ephemeral; failed live
publication is logged and durable completion still proceeds. Partial deltas may
repeat after a crash. Consumers treat the final durable event as authoritative.

`Store::complete_inference` atomically writes the response, appends the success
or failure event, clears inflight, the stored gateway request, and the claim,
marks idempotency complete, and indexes the session as Runnable. Large values
are uploaded to content-addressed blobs first. As with other store writes,
failed transactions can leave blobs for later garbage collection. Full
results, including usage and stop reason, are
available through `Store::get_inference_result::<Result<Response, String>>`;
success events also contain the assistant message. Acknowledgement follows the
transaction. Replayed completed jobs are acknowledged without provider calls.

The fake script uses zero-based response turns within each process:

```json
{
  "latency_ms": 2000,
  "fail": false,
  "responses": {
    "0": {
      "parts": [{"text": {"text": "hello"}}],
      "stop_reason": "end_turn",
      "usage": {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "output_tokens": 1,
        "reasoning_output_tokens": 0,
        "total_tokens": 1
      }
    }
  }
}
```

Latency applies before each delta so tests can kill the process after partial
output. `fail: true` fails every call after the configured latency. Missing turns
also fail. The call log is synced before inference starts, and keeps one `call`
line per invocation across restarts. File configuration lives only in this binary;
the reusable fake provider remains in `swarmy-llm`.

Run integration tests with the dev stack:

```sh
scripts/dev-stack.sh start
source .dev/env
cargo test -p swarmy-gateway --locked
```

Tests allocate unique FoundationDB directories and bus prefixes and run the
actual binary as a child. They skip with a printed message when the required
service variables are absent. The crash test sends SIGKILL after a live delta,
then restarts the binary against the same durable state and call log.

For concurrent sessions and gateway restarts, use the request-based script mode:

```json
{
  "latency_ms": 100,
  "request_based": {
    "steps": 3,
    "tool_steps": [0, 1],
    "final_answer": "chaos session complete"
  }
}
```

The zero-based step is the number of assistant messages in the incoming request,
so retries and interleaved sessions select the same response on any process.
Listed steps emit `get_time` with a stable `clock-STEP` call id. Other steps emit
`final_answer` and end the turn. For a turn of exactly `steps` responses, list all
steps before the final one; the current harness ends on a text-only response.
Tool steps must precede the final step, and requests beyond `steps` fail.
Choose either `responses` or `request_based`. Existing turn-keyed scripts,
`fail`, latency, and the synced call log keep their previous behavior.
