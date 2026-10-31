# Node API

The internal node API moves sandbox node store operations to the primary. Its wire contract has no compatibility promise: nodes and services upgrade together. `swarmy-node-protocol` holds JSON types and an HTTP/2 prior-knowledge client without FoundationDB; `swarmy-node-api` authenticates and serves requests against the store.

`POST /node/v1/register` records node resources and returns earlier placements and lease policy. `GET /node/v1/placements` returns the calling node's placements and call occupancy. Each response includes `server_now` from the store clock.

Every request needs `Authorization: Bearer swn.<node>.<secret>`; the node id comes from the token, never from a body. Positive credential verification is cached for ten seconds. Every request also needs `x-swarmy-build` exactly matching the service build; mismatch returns 409. Failed responses have a JSON `Failure` body: 401 unauthorized, 409 build mismatch or typed refusal, 400 invalid request, 503 transient storage failure, 500 internal failure. The client retries connections, timeouts and 503 with bounded exponential backoff until a 30-second deadline, and retries build mismatch every five seconds.

Configure `[node_api] listen` (default `127.0.0.1:8743`), `[node] api_url`, and `[node] token_file`. Run swarmy-core node-credential issue NODE_ID to print a token, or swarmy-core node-credential revoke NODE_ID to revoke it.

## Sync

`POST /node/v1/sync` combines heartbeat, placement and call occupancy, tool-claim and writer renewals, scratch inventory and evictions, and turn metric patches. The server returns index-aligned answers: `Renewed` supplies the new expiry, `Lost` means stop using the fenced token, and `Pending` means keep its existing expiry and retry next cycle. Each store item runs independently under a two-second server deadline. All requested expiries use the server clock; echoed expiries are never trusted. The authenticated node must own each placement, claim placement, and writer lease before a store call can use that token.

The node reports scratch directories and their occupancy. The server decides deleted, moved, and idle eviction verdicts; the node evicts on disk and sends the removed agents in the next sync for row cleanup. Tool and computer metrics are queued without blocking the sync; they are flushed on service shutdown.

## Tool route

`POST /node/v1/tool/route` resolves a tool job's immutable session to its agent. The node caches that agent for its hosting actor. A completed job returns `AlreadyDone`; dispatch to another node returns `PlacedElsewhere`.

## Hosting open

`POST /node/v1/hosting/open` accepts only a live job dispatched to the caller. It repeats safe store steps to claim a placement, bind the volume, open its writer and head, and read the sandbox spec. A stopped epoch is never renewed. The answer includes the server clock and an empty boot set until boot working sets are recorded.

## Hosting ready

`POST /node/v1/hosting/ready` requires the caller's placement, records the sandbox address, and queues boot metric batches. A stale placement is fenced.

## Hosting close

`POST /node/v1/hosting/close` requires ownership of both echoed tokens. It releases the writer before the placement; a writer refusal leaves the placement intact so possible loss can be reported after expiry.

## Volume publish

`POST /node/v1/volume/publish` requires the caller's writer lease and publishes under the server's configured snapshot retention. A stale lease or head is fenced.

## Volume protect

`POST /node/v1/volume/protect` conservatively protects arbitrary reused hashes. One request carries at most 1,024 hashes (400 otherwise); the client splits larger lists into sequential requests and joins the deleting results in order.

## GitHub credential

`POST /node/v1/credentials/github` returns a token only when the agent has a live placement on the calling node according to the server clock. Other placements return `NotYours`; the secret is never logged and its response debug representation is redacted.

## Provisioning

A joining node receives its own token from the primary through the primary shell;
the stack host issues its own token during provisioning. The token is stored in
`/etc/swarmy/node-token` (root-owned, mode 0600) and revoked with the tunnel
key when the node is removed. The SSH tunnel forwards local port 8743 to the
primary's node API. Existing hosts receive credentials during `swarmy swarm
upgrade` in NA11; this provisioning path only covers new joins.
