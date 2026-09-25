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

mod timers;
pub use timers::{CancelTimer, ListTimers, SetTimer};
mod files;
pub use files::{Edit, Glob, Grep, Ls, Read, UpdatePlan, Write};

sandbox_tool!(
    Bash,
    "bash",
    "Run bash in your sandbox. Wait up to yield_seconds (default 10, clamped to timeout_ms). If still running at yield or full timeout, leave it running as a managed background process and return process_id, log_path, and output so far; timeout does not kill it. Small results retain separate stdout and stderr; truncated results combine them in stdout. The preview budget is output_budget_bytes (default/max 32 KiB): keep the first and last halves with a byte-elision marker, including the marker within the budget. Full output is saved at /home/agent/.swarmy/output/<call id>.log (a unique suffix is added if that path exists). Use process_log, process_stop, or write_stdin afterwards. This does not take a snapshot.",
    json!({"type":"object", "properties": {
        "command": {"type":"string", "minLength":1},
        "timeout_ms": {"type":"integer", "minimum":1, "maximum":3_600_000, "default":120_000},
        "yield_seconds": {"type":"integer", "minimum":0, "maximum":3600, "default":10},
        "output_budget_bytes": {"type":"integer", "minimum":1024, "maximum":32768, "default":32768}
    }, "required":["command"], "additionalProperties":false})
);
sandbox_tool!(
    ProcessStart,
    "process_start",
    "Start a background bash command in your running sandbox. Return its process_id and log_path. Complete output goes to that log and the process continues between tool calls, like bash after its yield or timeout. process_log returns a 32 KiB head-and-tail preview with an elided-byte marker and the full log path. Use write_stdin to feed input.",
    json!({"type":"object", "properties":{"command":{"type":"string", "minLength":1}}, "required":["command"], "additionalProperties":false})
);
sandbox_tool!(
    ProcessList,
    "process_list",
    "List managed processes, including bash commands backgrounded at yield or timeout, with their command, start time, log path, and running or exited status. The list is capped: running processes come first, followed by the 20 most recently started other records, newest first. Pass limit (1-200) to change how many non-running records to include, or all true to include every record. Each command is truncated to 512 characters with a trailing ... marker. Records from an earlier sandbox lifetime report restarted.",
    json!({"type":"object", "properties": {
        "limit": {"type":"integer", "minimum":1, "maximum":200, "default":20, "description":"How many non-running records to include besides running ones."},
        "all": {"type":"boolean", "default":false, "description":"Include every record regardless of the limit."}
    }, "additionalProperties":false})
);
sandbox_tool!(
    ProcessLog,
    "process_log",
    "Read a 32 KiB preview of a managed process's combined stdout and stderr using its process_id (including bash after yield or timeout). Keep the first and last halves with an elided-byte marker; complete output stays in log_path, under /home/agent/.swarmy/output/<call id>.log for bash. An id from an earlier sandbox lifetime reports that the sandbox restarted.",
    process_parameters()
);
sandbox_tool!(
    ProcessStop,
    "process_stop",
    "Terminate a managed process group using its process_id, including bash after yield or timeout. Its full log remains at log_path; process_log reads a 32 KiB head-and-tail preview with an elision marker. Send TERM, then KILL after a short grace period. An id from an earlier sandbox lifetime reports that the sandbox restarted.",
    process_parameters()
);
sandbox_tool!(
    Checkpoint,
    "checkpoint",
    "Snapshot your sandbox disk now and return the durable manifest_id after publication. This snapshot saves files, but does not save process memory.",
    empty_parameters()
);

sandbox_tool!(
    WriteStdin,
    "write_stdin",
    "Write text to a managed process's stdin, including bash after yield or timeout. Include a newline to submit a line. Returns bytes_written; if the pipe is full, only a prefix may be written, so retry the remaining bytes. Output remains in the full log; process_log returns the 32 KiB head-and-tail preview and log_path. Exited processes and earlier sandbox lifetimes are rejected.",
    json!({"type":"object", "properties":{
        "process_id":{"type":"string"}, "text":{"type":"string"}
    }, "required":["process_id", "text"], "additionalProperties":false})
);
sandbox_tool!(
    WebFetch,
    "web_fetch",
    "Fetch an HTTP or HTTPS URL using the sandbox's network. Follow HTTP(S) redirects, convert HTML to text, and pass JSON and plain text through. Limit the response body to 5 MiB and the complete request to 30 seconds.",
    json!({"type":"object", "properties":{"url":{"type":"string", "minLength":1}}, "required":["url"], "additionalProperties":false})
);

sandbox_tool!(
    BrowserNavigate,
    "browser_navigate",
    "Navigate the headed Chromium page to an HTTP or HTTPS URL.",
    json!({"type":"object","properties":{"url":{"type":"string","minLength":1}},"required":["url"],"additionalProperties":false})
);
sandbox_tool!(
    BrowserSnapshot,
    "browser_snapshot",
    "Read the current page's accessibility tree, URL, title, and short element references. Prefer this to a screenshot for interacting with web pages. Refs look like [e3], are stable within a page, and expire on navigation.",
    empty_parameters()
);
sandbox_tool!(
    BrowserClick,
    "browser_click",
    "Click an element from the most recent accessibility snapshot by its short ref.",
    ref_parameters()
);
sandbox_tool!(
    BrowserType,
    "browser_type",
    "Type text into an element from the latest snapshot; optionally submit its form.",
    json!({"type":"object","properties":{"ref":{"type":"string","minLength":1},"text":{"type":"string"},"submit":{"type":"boolean","default":false}},"required":["ref","text"],"additionalProperties":false})
);
sandbox_tool!(
    BrowserSelect,
    "browser_select",
    "Select a value in a dropdown from the latest snapshot.",
    json!({"type":"object","properties":{"ref":{"type":"string","minLength":1},"value":{"type":"string","minLength":1}},"required":["ref","value"],"additionalProperties":false})
);
sandbox_tool!(
    BrowserScroll,
    "browser_scroll",
    "Scroll the page by direction (up, down, left, right) or scroll a snapshot ref into view.",
    json!({"type":"object","properties":{"direction":{"type":"string","enum":["up","down","left","right"]},"ref":{"type":"string","minLength":1}},"additionalProperties":false})
);
sandbox_tool!(
    BrowserScreenshot,
    "browser_screenshot",
    "Capture the browser viewport as a bounded PNG image part. Use browser_snapshot for text and controls.",
    empty_parameters()
);
sandbox_tool!(
    BrowserEvaluate,
    "browser_evaluate",
    "Evaluate JavaScript on the current page and return a bounded JSON result.",
    json!({"type":"object","properties":{"js":{"type":"string","minLength":1}},"required":["js"],"additionalProperties":false})
);
sandbox_tool!(
    ScreenScreenshot,
    "screen_screenshot",
    "Capture the whole virtual display as a bounded PNG image part, including non-browser programs.",
    empty_parameters()
);
sandbox_tool!(
    ScreenWindows,
    "screen_windows",
    "List the virtual display's open windows by title.",
    empty_parameters()
);

fn ref_parameters() -> Value {
    json!({"type":"object","properties":{"ref":{"type":"string","minLength":1}},"required":["ref"],"additionalProperties":false})
}

#[must_use]
pub fn is_display_name(name: &str) -> bool {
    name.starts_with("browser_") || name.starts_with("screen_")
}

pub const DISPLAY_PROMPT: &str = "## Browser and screen tools\nFor browser pages, prefer browser_snapshot and its element refs over screenshots: accessibility text is faster and more reliable for navigation. Use browser_screenshot when visual layout matters. Use screen_screenshot and screen_windows for non-browser applications.";

pub fn register_display(tools: &mut ToolRegistry) {
    tools.register(Box::new(BrowserNavigate));
    tools.register(Box::new(BrowserSnapshot));
    tools.register(Box::new(BrowserClick));
    tools.register(Box::new(BrowserType));
    tools.register(Box::new(BrowserSelect));
    tools.register(Box::new(BrowserScroll));
    tools.register(Box::new(BrowserScreenshot));
    tools.register(Box::new(BrowserEvaluate));
    tools.register(Box::new(ScreenScreenshot));
    tools.register(Box::new(ScreenWindows));
}

fn empty_parameters() -> Value {
    json!({"type":"object", "properties":{}, "additionalProperties":false})
}
fn process_parameters() -> Value {
    json!({"type":"object", "properties":{"process_id":{"type":"string", "description":"The process_id returned by process_start or bash."}}, "required":["process_id"], "additionalProperties":false})
}

pub fn register(tools: &mut ToolRegistry) {
    tools.register(Box::new(SetTimer));
    tools.register(Box::new(ListTimers));
    tools.register(Box::new(CancelTimer));
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
    tools.register(Box::new(WriteStdin));
    tools.register(Box::new(WebFetch));
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::{BashArguments, decode, encode};

    /// Anthropic, Bedrock, xAI, and Azure reject combinators at the top level of a
    /// tool schema, so every registered tool must be a plain object there.
    #[test]
    fn tool_schemas_are_plain_objects_at_the_top_level() {
        let mut registry = ToolRegistry::default();
        register(&mut registry);
        register_display(&mut registry);
        let definitions = registry.definitions();
        assert!(definitions.len() >= 16);
        for tool in definitions {
            assert_eq!(tool.parameters["type"], "object", "{}", tool.name);
            for key in ["oneOf", "anyOf", "allOf", "enum", "const", "not"] {
                assert!(
                    tool.parameters.get(key).is_none(),
                    "{} uses {key} at the top level",
                    tool.name
                );
            }
        }
    }

    #[test]
    fn sandbox_tools_describe_and_validate_new_behavior() {
        use swarmy_core::SandboxArguments;
        for tool in [&Bash as &dyn Tool, &ProcessStart, &ProcessLog, &WriteStdin] {
            assert!(tool.sandbox_bound());
            assert!(tool.description().contains("32 KiB"));
            assert!(tool.description().contains("yield"));
            assert!(tool.description().contains("log"));
        }
        assert!(
            Bash.description()
                .contains("/home/agent/.swarmy/output/<call id>.log")
        );
        assert!(WebFetch.sandbox_bound());
        assert!(SandboxArguments::parse("web_fetch", json!({"url":"http://localhost/"})).is_ok());
        assert!(SandboxArguments::parse("web_fetch", json!({"url":""})).is_err());
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let input = json!({"process_id":id, "text":"hello\n"});
        let parsed = SandboxArguments::parse("write_stdin", input.clone()).unwrap();
        assert_eq!(parsed.name(), "write_stdin");
        assert_eq!(parsed.parameters(), input);
        for arguments in [
            json!({"command":"true", "yield_seconds":-1}),
            json!({"command":"true", "yield_seconds":3601}),
            json!({"command":"true", "output_budget_bytes":0}),
            json!({"command":"true", "output_budget_bytes":32769}),
        ] {
            assert!(SandboxArguments::parse("bash", arguments).is_err());
        }
        assert!(ProcessList.description().contains("capped"));
        assert!(ProcessList.description().contains("limit"));
        let parsed = SandboxArguments::parse("process_list", json!({})).unwrap();
        assert_eq!(parsed.name(), "process_list");
        assert!(parsed.valid());
        assert!(SandboxArguments::parse("process_list", json!({"limit": 5})).is_ok());
        assert!(SandboxArguments::parse("process_list", json!({"all": true})).is_ok());
        for arguments in [
            json!({"limit": 0}),
            json!({"limit": 201}),
            json!({"limit": -1}),
        ] {
            assert!(SandboxArguments::parse("process_list", arguments).is_err());
        }
    }

    #[test]
    fn bash_is_remote_and_validates_arguments_without_executing() {
        assert!(Bash.sandbox_bound());
        assert!(futures::executor::block_on(Bash.execute(json!({"command":"exit 0"}))).is_err());
        let arguments: BashArguments =
            serde_json::from_value(json!({"command":"echo hello"})).unwrap();
        assert_eq!(arguments.timeout_ms, 120_000);
        assert_eq!(arguments.yield_seconds, 10);
        assert_eq!(arguments.output_budget_bytes, 32768);
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
                    timeout_ms,
                    yield_seconds: 10,
                    output_budget_bytes: 32768,
                }
                .valid()
            );
        }
    }
}
