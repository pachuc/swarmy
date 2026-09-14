# swarmy-harness

The harness replays core events and returns one action without performing I/O.
Configure `Harness` with a system prompt template, LLM generation settings, and
a `ToolRegistry`. Register `Box::new(GetTime)` to enable the built-in clock tool.
Tool definitions are ordered by name. The system template and ordered core
messages are copied into `swarmy_llm::Request` without text transformations.

Call `Harness::step` with the `SessionRecord`, a decoded `Snapshot`, the ordered
events after that snapshot, and a caller-supplied message ID for folded results.
For a new session, use `Snapshot::default()` and the full log. `SessionRecord`
contains only a snapshot reference, so loading the snapshot is the worker's job.
`Snapshot::replay` creates the next snapshot, including pending inference and
tool work, and supports the core crate's encode/decode functions. The caller
must associate it with the correct session and covered event sequence.

Workers handle the returned actions as follows:

- `BuildInference`: persist `InferenceRequested` and submit the assembled request.
- `DispatchTools`: atomically persist all `ToolCallRequested` events, then execute
  each call through the registry. Unknown names can be recorded as tool errors.
- `Wait`: wait for external work. A partial tool batch never produces a fold.
- `FoldResults`: persist the returned tool-role message as `MessageAppended`.
  The next step builds an inference request containing these results.
- `EndTurn`: finish the turn using the session lifecycle rules.

Use `execution_result` to convert a tool's `Result<String, String>` into the
core `ToolResult` for `ToolCallCompleted`. Success uses the tool name as its
title and an empty metadata map; failure preserves the error text. Replay
also preserves richer completion metadata supplied by workers. Results are
matched by call ID and the tool request ID, then folded in request-event order.
Pass the same message ID when retrying a fold step so replay stays deterministic.

Only `Tool::execute` performs effects. `GetTime` accepts an empty JSON object and
reads `jiff::Timestamp::now()` when its future runs. It needs no sandbox or
runtime-specific facilities. Tests drive futures with Tokio and use the LLM
fake provider; they need no external services or credentials.
