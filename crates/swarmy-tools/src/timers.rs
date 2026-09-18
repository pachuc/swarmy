use futures::future::BoxFuture;
use serde_json::{Value, json};
use swarmy_harness::Tool;

macro_rules! timer_tool {
    ($kind:ident, $name:literal, $description:literal, $parameters:expr) => {
        pub struct $kind;
        impl Tool for $kind {
            fn name(&self) -> &'static str {
                $name
            }
            fn description(&self) -> &'static str {
                $description
            }
            fn parameters(&self) -> Value {
                $parameters
            }
            fn execute(&self, _: Value) -> BoxFuture<'_, Result<String, String>> {
                Box::pin(async { Err("timer tools must execute through the agent store".into()) })
            }
        }
    };
}

timer_tool!(
    SetTimer,
    "set_timer",
    "Wake your named agent's main conversation with a system note at or after the requested time. Supply exactly one of delay_seconds or at (RFC 3339 absolute timestamp). A busy conversation receives the note after its turn ends. Timers survive summarization and service restarts. At most 32 pending timers per agent; note must be 1-1024 UTF-8 bytes. Returns timer_id and due_at.",
    json!({"type":"object","properties":{
        "delay_seconds":{"type":"integer","minimum":1},
        "at":{"type":"string","format":"date-time"},
        "note":{"type":"string","minLength":1,"maxLength":1024}
    },"required":["note"],"additionalProperties":false,
    "oneOf":[{"required":["delay_seconds"]},{"required":["at"]}]})
);
timer_tool!(
    ListTimers,
    "list_timers",
    "List this named agent's pending timers with their ids, due times, and notes.",
    super::empty_parameters()
);
timer_tool!(
    CancelTimer,
    "cancel_timer",
    "Cancel a pending timer belonging to this named agent. Repeated cancellation is harmless; an already fired timer cannot be cancelled.",
    json!({"type":"object","properties":{"timer_id":{"type":"string"}},"required":["timer_id"],"additionalProperties":false})
);
