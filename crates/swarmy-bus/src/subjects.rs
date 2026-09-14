//! Subject templates from design section 6.2. Services use typed routes instead.

pub const SCHED_RUNNABLE: &str = "sched.runnable.{partition}";
pub const SCHED_PLACE: &str = "sched.place";
pub const INFER_REQ: &str = "infer.req.{provider_class}";
pub const INFER_LIVE: &str = "infer.live.{session_id}";
pub const TOOL_REMOTE: &str = "tool.remote";
pub const TOOL_NODE: &str = "tool.node.{node_id}";
pub const NODE_HEARTBEAT: &str = "node.heartbeat";
pub const SESSION_EVENTS: &str = "session.events.{session_id}";
pub const CHANNEL_MSG: &str = "channel.msg.{channel_id}";
