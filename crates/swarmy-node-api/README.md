# Node API

The internal node API moves sandbox node store operations to the primary. Its wire contract has no compatibility promise: nodes and services upgrade together. `swarmy-node-protocol` holds JSON types and an HTTP/2 prior-knowledge client without FoundationDB; `swarmy-node-api` authenticates and serves requests against the store.

`POST /node/v1/register` records node resources and returns earlier placements and lease policy. `GET /node/v1/placements` returns the calling node's placements and call occupancy. Each response includes `server_now` from the store clock.

Every request needs `Authorization: Bearer swn.<node>.<secret>`; the node id comes from the token, never from a body. Positive credential verification is cached for ten seconds. Every request also needs `x-swarmy-build` exactly matching the service build; mismatch returns 409. Failed responses have a JSON `Failure` body: 401 unauthorized, 409 build mismatch or typed refusal, 400 invalid request, 503 transient storage failure, 500 internal failure. The client retries connections, timeouts and 503 with bounded exponential backoff until a 30-second deadline, and retries build mismatch every five seconds.

Configure `[node_api] listen` (default `127.0.0.1:8743`), `[node] api_url`, and `[node] token_file`. Run swarmy-core node-credential issue NODE_ID to print a token, or swarmy-core node-credential revoke NODE_ID to revoke it.

## Sync

`POST /node/v1/sync` combines heartbeat, placement and call occupancy, tool-claim and writer renewals, scratch inventory and evictions, and turn metric patches. The server returns index-aligned answers: `Renewed` supplies the new expiry, `Lost` means stop using the fenced token, and `Pending` means keep its existing expiry and retry next cycle. Each store item runs independently under a two-second server deadline. All requested expiries use the server clock; echoed expiries are never trusted. The authenticated node must own each placement, claim placement, and writer lease before a store call can use that token.

The node reports scratch directories and their occupancy. The server decides deleted, moved, and idle eviction verdicts; the node evicts on disk and sends the removed agents in the next sync for row cleanup. Tool and computer metrics are queued without blocking the sync; they are flushed on service shutdown.
