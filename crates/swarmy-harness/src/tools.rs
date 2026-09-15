use std::collections::BTreeMap;

use futures::future::BoxFuture;
use serde_json::{Value, json};
use swarmy_core::{Part, ToolCallId, ToolResult};
use swarmy_llm::ToolDefinition;

/// Tools expose stable descriptions and schemas for prompt assembly.
/// Only `execute` may perform effects or read the clock.
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters(&self) -> Value;
    fn sandbox_bound(&self) -> bool {
        false
    }
    fn execute(&self, arguments: Value) -> BoxFuture<'_, Result<String, String>>;
}

/// Definitions are sorted by name so registration order cannot change a prompt.
#[derive(Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Box<dyn Tool>>,
}

impl ToolRegistry {
    /// Registers a tool, returning the previous implementation if it was replaced.
    pub fn register(&mut self, tool: Box<dyn Tool>) -> Option<Box<dyn Tool>> {
        self.tools.insert(tool.name().to_owned(), tool)
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&dyn Tool> {
        self.tools.get(name).map(AsRef::as_ref)
    }

    #[must_use]
    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .map(|(name, tool)| ToolDefinition {
                name: name.clone(),
                description: tool.description().to_owned(),
                parameters: tool.parameters(),
            })
            .collect()
    }
}

/// A successful execution uses the tool name as its title and has no metadata.
/// Workers can persist the returned core result in `ToolCallCompleted`.
#[must_use]
pub fn execution_result(tool: &str, result: Result<String, String>) -> ToolResult {
    match result {
        Ok(output) => ToolResult::Completed {
            output,
            title: tool.to_owned(),
            metadata: BTreeMap::new(),
        },
        Err(error) => ToolResult::Error { error },
    }
}

#[must_use]
pub fn result_part(call_id: ToolCallId, result: ToolResult) -> Part {
    Part::ToolResult { call_id, result }
}

/// Reads the worker's clock without a sandbox or network connection.
pub struct GetTime;

impl Tool for GetTime {
    fn name(&self) -> &'static str {
        "get_time"
    }

    fn description(&self) -> &'static str {
        "Return the current time as an RFC 3339 timestamp."
    }

    fn parameters(&self) -> Value {
        json!({"type": "object", "properties": {}, "additionalProperties": false})
    }

    fn execute(&self, arguments: Value) -> BoxFuture<'_, Result<String, String>> {
        Box::pin(async move {
            if !arguments.as_object().is_some_and(serde_json::Map::is_empty) {
                return Err("get_time expects an empty JSON object".to_owned());
            }
            Ok(jiff::Timestamp::now().to_string())
        })
    }
}
