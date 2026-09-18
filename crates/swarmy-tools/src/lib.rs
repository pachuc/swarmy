//! Sandbox tools expose schemas here and execute on the assigned node.
use futures::future::BoxFuture;
use serde_json::{Value, json};
use swarmy_harness::{Tool, ToolRegistry};

macro_rules! sandbox_tool {
    ($type:ident, $name:literal, $description:literal, $parameters:expr) => {
        pub struct $type;
        impl Tool for $type {
            fn name(&self) -> &'static str { $name }
            fn description(&self) -> &'static str {
                concat!($description, " Files persist across failures up to the last snapshot, published every ten minutes or when checkpoint is called. Processes do not survive a node failure or an idle eviction.")
            }
            fn parameters(&self) -> Value { $parameters }
            fn sandbox_bound(&self) -> bool { true }
            fn execute(&self, _: Value) -> BoxFuture<'_, Result<String, String>> {
                Box::pin(async { Err(concat!($name, " must execute on a sandbox node").into()) })
            }
        }
    };
}

mod files;
pub use files::{Edit, Glob, Grep, Ls, Read, UpdatePlan, Write};

sandbox_tool!(
    Bash,
    "bash",
    "Run a bash command in your running sandbox with a timeout and return stdout, stderr, and exit status. A timeout stops only this command's process group. This does not take a snapshot.",
    json!({"type":"object", "properties": {
        "command": {"type":"string", "minLength":1},
        "timeout_ms": {"type":"integer", "minimum":1, "maximum":3_600_000, "default":120_000}
    }, "required":["command"], "additionalProperties":false})
);
sandbox_tool!(
    ProcessStart,
    "process_start",
    "Start a background bash command in your running sandbox. Return its process_id and log_path. Output goes to that log and the process continues between tool calls.",
    json!({"type":"object", "properties":{"command":{"type":"string", "minLength":1}}, "required":["command"], "additionalProperties":false})
);
sandbox_tool!(
    ProcessList,
    "process_list",
    "List managed background processes with their command, start time, log path, and running or exited status. Records from an earlier sandbox lifetime report restarted.",
    empty_parameters()
);
sandbox_tool!(
    ProcessLog,
    "process_log",
    "Read the last 64 KiB of a managed process's combined stdout and stderr using its process_id. An id from an earlier sandbox lifetime reports that the sandbox restarted.",
    process_parameters()
);
sandbox_tool!(
    ProcessStop,
    "process_stop",
    "Terminate a managed process group using its process_id. Send TERM, then KILL after a short grace period. An id from an earlier sandbox lifetime reports that the sandbox restarted.",
    process_parameters()
);
sandbox_tool!(
    Checkpoint,
    "checkpoint",
    "Snapshot your sandbox disk now and return the durable manifest_id after publication. This snapshot saves files, but does not save process memory.",
    empty_parameters()
);

fn empty_parameters() -> Value {
    json!({"type":"object", "properties":{}, "additionalProperties":false})
}
fn process_parameters() -> Value {
    json!({"type":"object", "properties":{"process_id":{"type":"string", "description":"The process_id returned by process_start."}}, "required":["process_id"], "additionalProperties":false})
}

pub fn register(tools: &mut ToolRegistry) {
    tools.register(Box::new(Read));
    tools.register(Box::new(Write));
    tools.register(Box::new(Edit));
    tools.register(Box::new(Glob));
    tools.register(Box::new(Grep));
    tools.register(Box::new(Ls));
    tools.register(Box::new(UpdatePlan));
    tools.register(Box::new(Bash));
    tools.register(Box::new(ProcessStart));
    tools.register(Box::new(ProcessList));
    tools.register(Box::new(ProcessLog));
    tools.register(Box::new(ProcessStop));
    tools.register(Box::new(Checkpoint));
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::{BashArguments, decode, encode};

    #[test]
    fn bash_is_remote_and_validates_arguments_without_executing() {
        assert!(Bash.sandbox_bound());
        assert!(futures::executor::block_on(Bash.execute(json!({"command":"exit 0"}))).is_err());
        let arguments: BashArguments =
            serde_json::from_value(json!({"command":"echo hello"})).unwrap();
        assert_eq!(arguments.timeout_ms, 120_000);
        assert!(arguments.valid());
        assert_eq!(
            decode::<BashArguments>(&encode(&arguments).unwrap()).unwrap(),
            arguments
        );
        for input in [
            json!({}),
            json!({"command": 42}),
            json!({"command":"echo", "typo":1}),
        ] {
            assert!(serde_json::from_value::<BashArguments>(input).is_err());
        }
        for timeout_ms in [0, 3_600_001] {
            assert!(
                !BashArguments {
                    command: "echo hello".into(),
                    timeout_ms
                }
                .valid()
            );
        }
    }
}
