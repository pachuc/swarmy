use futures::future::BoxFuture;
use serde_json::{Value, json};
use swarmy_harness::Tool;

sandbox_tool!(
    Read,
    "read",
    "Read a UTF-8 text file or a PNG, JPEG, GIF, or WebP image (up to 5 MiB). Images are shown to vision-capable models; other models receive the filename and dimensions. Path is relative to the sandbox working directory or absolute. Offset is a 1-indexed line number, default 1; limit defaults to 2000 lines. Output includes line numbers, truncates lines after 2000 characters, and gives the next offset when more remains. Files containing null bytes are rejected.",
    json!({"type":"object", "properties":{
        "path":{"type":"string","minLength":1},
        "offset":{"type":"integer","minimum":1,"default":1},
        "limit":{"type":"integer","minimum":1,"default":2000}
    }, "required":["path"], "additionalProperties":false})
);
sandbox_tool!(
    Write,
    "write",
    "Write UTF-8 content to a file, replacing any existing content and creating missing parent directories. Path may be relative or absolute.",
    json!({"type":"object", "properties":{
        "path":{"type":"string","minLength":1}, "content":{"type":"string"}
    }, "required":["path","content"], "additionalProperties":false})
);
sandbox_tool!(
    Edit,
    "edit",
    "Replace old_string with new_string in a UTF-8 file. First require one exact match, then try matching whole lines ignoring trailing whitespace, then leading and trailing whitespace. Report the matching mode and a unified diff. Ambiguous matches report their line numbers. Set replace_all to replace every exact occurrence; it never uses whitespace fallbacks. old_string must not be empty.",
    json!({"type":"object", "properties":{
        "path":{"type":"string","minLength":1}, "old_string":{"type":"string","minLength":1},
        "new_string":{"type":"string"}, "replace_all":{"type":"boolean","default":false}
    }, "required":["path","old_string","new_string"], "additionalProperties":false})
);
sandbox_tool!(
    Glob,
    "glob",
    "Find files below path (default current directory) using *, ?, character classes, and ** for directory levels. A pattern without slashes matches basenames at any depth. Include hidden and ignored files except .git directories; do not follow directory symlinks. Return at most 100 paths and say when the cap is reached. Use ripgrep when installed, otherwise a Python walk.",
    search_parameters()
);
sandbox_tool!(
    Grep,
    "grep",
    "Search file contents for a regular expression below path (default current directory). Return path, 1-indexed line number, and text for at most 100 matching lines and say when the cap is reached. Include hidden and ignored files except .git directories. Use ripgrep when installed, otherwise Python regular expressions and a directory walk; use common regular expression syntax for portability.",
    search_parameters()
);
sandbox_tool!(
    Ls,
    "ls",
    "List the immediate entries in path (default current directory), sorted by name, including hidden entries. Append a slash to directory names.",
    json!({"type":"object", "properties":{"path":{"type":"string","minLength":1,"default":"."}}, "additionalProperties":false})
);

fn search_parameters() -> Value {
    json!({"type":"object", "properties":{
        "path":{"type":"string","minLength":1,"default":"."}, "pattern":{"type":"string","minLength":1}
    }, "required":["pattern"], "additionalProperties":false})
}

pub struct UpdatePlan;
impl Tool for UpdatePlan {
    fn name(&self) -> &'static str {
        "update_plan"
    }
    fn description(&self) -> &'static str {
        "Replace this session's entire plan with the supplied list and return it. Each step has a nonempty description and a status of pending, in_progress, or completed. At most one step may be in_progress. An empty list clears the plan. This tool stores session data and does not use the sandbox."
    }
    fn parameters(&self) -> Value {
        json!({"type":"object", "properties":{"plan":{"type":"array", "items":{
            "type":"object", "properties":{"step":{"type":"string","minLength":1},
            "status":{"type":"string","enum":["pending","in_progress","completed"]}},
            "required":["step","status"], "additionalProperties":false
        }}}, "required":["plan"], "additionalProperties":false})
    }
    fn execute(&self, _: Value) -> BoxFuture<'_, Result<String, String>> {
        Box::pin(async { Err("update_plan must execute through the session store".into()) })
    }
}
