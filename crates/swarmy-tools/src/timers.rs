use futures::future::BoxFuture;
use serde_json::{Value, json};
use swarmy_harness::Tool;

store_tool!(
    SetTimer,
    "set_timer",
    "Wake this conversation with a system note at or after the requested time. Supply exactly one of delay_seconds or at (RFC 3339 absolute timestamp). A busy conversation receives the note after its turn ends; if this conversation is closed or summarized away, the note goes to your named agent's main conversation instead. Timers survive summarization and service restarts. At most 32 pending timers per agent; note must be 1-1024 UTF-8 bytes. Returns timer_id and due_at.",
    json!({"type":"object","properties":{
        "delay_seconds":{"type":"integer","minimum":1},
        "at":{"type":"string","format":"date-time"},
        "note":{"type":"string","minLength":1,"maxLength":1024}
    },"required":["note"],"additionalProperties":false})
);
store_tool!(
    ListTimers,
    "list_timers",
    "List this named agent's pending timers with their ids, due times, and notes.",
    super::empty_parameters()
);
store_tool!(
    CancelTimer,
    "cancel_timer",
    "Cancel a pending timer belonging to this named agent. Repeated cancellation is harmless; an already fired timer cannot be cancelled.",
    json!({"type":"object","properties":{"timer_id":{"type":"string"}},"required":["timer_id"],"additionalProperties":false})
);
