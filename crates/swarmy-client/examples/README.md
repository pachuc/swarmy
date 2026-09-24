# Client examples

Set `SWARMY_API_URL` to the control plane origin (for example,
`http://127.0.0.1:8080`) and `SWARMY_API_TOKEN` to its bearer token.

* `SWARMY_TEST_IMAGE=base-ubuntu:dev cargo run -p swarmy-client --example chat -- "Hello"`
  creates an ephemeral session and prints the first completed assistant response.
  The image must already be registered and the control plane must have a working
  default provider.
* `cargo run -p swarmy-client --example tail` follows the durable log of each
  session returned by the paginated session list, including sessions found by
  later scans. It opens one stream for each group of up to 32 sessions.

The Rust library's `EventStream::next` yields durable events and advances a
cursor only when delivering one. `next_item` additionally yields ephemeral
live token deltas when `token_deltas` is enabled. A cloned subscription handle
can change logs on an open stream; added logs should start from a cursor
chosen by the caller. A disconnected stream replays from its last delivered
cursor, even if it must reconnect to another server.
