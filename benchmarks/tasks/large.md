---
name: api-version-route
scope: large
commit: 8c670aa1b9926cd23badb8a056d2ac68751ad520
expected_duration_band: 35-90 minutes
prompt: |
  Add a read-only GET /v1/version endpoint to crates/swarmy-api returning JSON
  with a `version` string equal to env!("CARGO_PKG_VERSION"). Add a version()
  method to crates/swarmy-client that decodes this response. Add an integration
  test named version_route_round_trip that starts the API against the dev stack,
  invokes the client method, and asserts the value equals the API crate version.
  The test must skip cleanly when required stack variables are absent. Do not
  change other crates. Run scripts/dev-stack.sh start, source .dev/env, then run
  the named integration test. Commit locally; do not push or open a pull request.
pass_criterion: |
  `git diff 8c670aa1b9926cd23badb8a056d2ac68751ad520 --name-only` contains
  only paths in crates/swarmy-api and crates/swarmy-client; GET /v1/version
  returns the expected JSON on a live dev stack; the named test exists and
  `cargo test --locked version_route_round_trip` passes with .dev/env sourced.
---
