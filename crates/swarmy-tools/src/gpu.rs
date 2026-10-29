use futures::future::BoxFuture;
use serde_json::{Value, json};
use swarmy_harness::Tool;

store_tool!(
    RequestGpu,
    "request_gpu",
    "Ask for a GPU for your computer. mode is shared (the card may also serve other agents' computers) or dedicated (no other GPU computer runs on that machine). reason says why, in 1-500 characters; operators see it next to the machine's cost. Your computer restarts on a node with a GPU: files on your disk are kept, but running processes and host-local scratch such as the cargo target directory and /tmp are lost, and your next tool call may wait several minutes while a GPU machine starts. This call returns at once. It must be the only tool call in its response. Call release_gpu when you no longer need the GPU.",
    json!({"type":"object","properties":{"mode":{"type":"string","enum":["shared","dedicated"]},"reason":{"type":"string","minLength":1,"maxLength":500}},"required":["mode","reason"],"additionalProperties":false})
);
store_tool!(
    ReleaseGpu,
    "release_gpu",
    "Give up your computer's GPU so it restarts on a node without one and the GPU machine can shut down. Files on your disk are kept; running processes and host-local scratch such as the cargo target directory and /tmp are lost. This call returns at once. It must be the only tool call in its response.",
    super::empty_parameters()
);
