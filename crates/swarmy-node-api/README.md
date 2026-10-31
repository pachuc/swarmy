# Node API

The internal node API moves sandbox node store operations to the primary. Its wire contract has no compatibility promise: nodes and services upgrade together. `swarmy-node-protocol` holds JSON types and an HTTP/2 prior-knowledge client without FoundationDB; `swarmy-node-api` authenticates and serves requests against the store.

`POST /node/v1/register` records node resources and returns earlier placements and lease policy. `GET /node/v1/placements` returns the calling node's placements and call occupancy. Each response includes `server_now` from the store clock.

Every request needs `Authorization: Bearer swn.<node>.<secret>`; the node id comes from the token, never from a body. Positive credential verification is cached for ten seconds. Every request also needs `x-swarmy-build` exactly matching the service build; mismatch returns 409. Failed responses have a JSON `Failure` body: 401 unauthorized, 409 build mismatch or typed refusal, 400 invalid request, 503 transient storage failure, 500 internal failure. The client retries connections, timeouts and 503 with bounded exponential backoff until a 30-second deadline, and retries build mismatch every five seconds.

Configure `[node_api] listen` (default `127.0.0.1:8743`), `[node] api_url`, and `[node] token_file`. Use `swarmy-core node-credential issue NODE_ID` to print a token or `swarmy-core node-credential revoke NODE_ID` to revoke it.
