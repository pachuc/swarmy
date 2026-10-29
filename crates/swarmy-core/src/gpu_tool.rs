//! The agent-facing GPU tools: names, arguments, and every text the model sees.
use crate::{EmptyArguments, GpuRequirement};
use serde::Deserialize;

pub const REQUEST_GPU_TOOL: &str = "request_gpu";
pub const RELEASE_GPU_TOOL: &str = "release_gpu";
pub const MAX_GPU_REASON_CHARS: usize = 500;
pub const GPU_REQUEST_NOT_ALLOWED: &str = "This agent is not allowed to change its GPU requirement, so nothing changed. An operator can allow it with: swarmy agent set <agent> --allow-gpu-request true";
pub const GPU_REQUEST_NEEDS_AGENT: &str = "request_gpu and release_gpu work only for named agents. This session's computer belongs to no named agent, so nothing changed.";
pub const GPU_TOOL_NOT_ALONE: &str = "No tool call in this response ran. request_gpu and release_gpu move your computer, so each must be the only tool call in its response. Call it on its own, then repeat the other calls.";
pub const GPU_RELEASED: &str = "Your GPU requirement is now none. This call did not wait for the move: your computer will restart on a node without a GPU when one has room. Files saved on your disk are kept. Running processes and background commands stop, and host-local scratch, including the cargo target directory (/home/agent/.cargo-target) and /tmp, is lost, so the next build starts cold.";
pub const GPU_ALREADY_RELEASED: &str =
    "You have no GPU requirement, so nothing changed and your computer will not move.";

#[must_use]
pub fn is_gpu_tool(name: &str) -> bool {
    matches!(name, REQUEST_GPU_TOOL | RELEASE_GPU_TOOL)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GpuToolCall {
    Request { mode: RequestedMode, reason: String },
    Release,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestGpuArguments {
    mode: RequestedMode,
    reason: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedMode {
    Shared,
    Dedicated,
}

impl From<RequestedMode> for GpuRequirement {
    fn from(mode: RequestedMode) -> Self {
        match mode {
            RequestedMode::Shared => Self::Shared,
            RequestedMode::Dedicated => Self::Dedicated,
        }
    }
}

impl GpuToolCall {
    /// Parse a GPU tool call's arguments; the error text goes to the model.
    /// # Errors
    /// Rejects unknown fields, a mode other than shared or dedicated, a blank
    /// reason or one over 500 characters, and names that are not GPU tools.
    pub fn parse(tool: &str, arguments: serde_json::Value) -> Result<Self, String> {
        match tool {
            REQUEST_GPU_TOOL => {
                let args: RequestGpuArguments = match serde_json::from_value(arguments) {
                    Ok(args) => args,
                    Err(error) => return Err(error.to_string()),
                };
                let reason = args.reason.trim();
                if reason.is_empty() || reason.chars().count() > MAX_GPU_REASON_CHARS {
                    return Err("reason must be 1-500 characters and not blank".into());
                }
                Ok(Self::Request {
                    mode: args.mode,
                    reason: reason.into(),
                })
            }
            RELEASE_GPU_TOOL => match serde_json::from_value::<EmptyArguments>(arguments) {
                Ok(_) => Ok(Self::Release),
                Err(error) => Err(error.to_string()),
            },
            _ => Err("expected request_gpu or release_gpu".into()),
        }
    }
}

fn mode_name(mode: GpuRequirement) -> &'static str {
    match mode {
        GpuRequirement::None => "none",
        GpuRequirement::Shared => "shared",
        GpuRequirement::Dedicated => "dedicated",
    }
}

#[must_use]
pub fn gpu_requested(mode: RequestedMode) -> String {
    let first = match mode {
        RequestedMode::Shared => {
            "Your GPU requirement is now shared: your computer will use a GPU that other computers may also use."
        }
        RequestedMode::Dedicated => {
            "Your GPU requirement is now dedicated: no other GPU computer will run on your computer's machine."
        }
    };
    format!(
        "{first} This call did not wait for the move: your computer will restart on a node with a GPU, and your next tool call that uses the computer may wait for GPU capacity, for several minutes if a GPU machine has to start. Files saved on your disk are kept. Running processes and background commands stop, and host-local scratch, including the cargo target directory (/home/agent/.cargo-target) and /tmp, is lost, so the next build starts cold. Call release_gpu when you no longer need the GPU."
    )
}

#[must_use]
pub fn gpu_already(mode: GpuRequirement) -> String {
    format!(
        "Your GPU requirement is already {}, so nothing changed and your computer will not move.",
        mode_name(mode)
    )
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use serde_json::json;
    #[test]
    fn gpu_tool_arguments_parse_and_reject() {
        assert_eq!(
            GpuToolCall::parse(
                REQUEST_GPU_TOOL,
                json!({"mode":"shared","reason":"  render  "})
            ),
            Ok(GpuToolCall::Request {
                mode: RequestedMode::Shared,
                reason: "render".into()
            })
        );
        assert!(matches!(
            GpuToolCall::parse(REQUEST_GPU_TOOL, json!({"mode":"dedicated","reason":"x"})),
            Ok(GpuToolCall::Request {
                mode: RequestedMode::Dedicated,
                ..
            })
        ));
        for args in [
            json!({"mode":"none","reason":"x"}),
            json!({"mode":"shared"}),
            json!({"mode":"shared","reason":" "}),
            json!({"mode":"shared","reason":"x".repeat(501)}),
            json!({"mode":"shared","reason":"x","extra":1}),
        ] {
            assert!(GpuToolCall::parse(REQUEST_GPU_TOOL, args).is_err());
        }
        assert!(
            GpuToolCall::parse(
                REQUEST_GPU_TOOL,
                json!({"mode":"shared","reason":"x".repeat(500)})
            )
            .is_ok()
        );
        assert_eq!(
            GpuToolCall::parse(RELEASE_GPU_TOOL, json!({})),
            Ok(GpuToolCall::Release)
        );
        assert!(GpuToolCall::parse(RELEASE_GPU_TOOL, json!({"x":1})).is_err());
    }
    #[test]
    fn gpu_tool_texts_explain_the_move() {
        for text in [
            gpu_requested(RequestedMode::Shared),
            gpu_requested(RequestedMode::Dedicated),
            GPU_RELEASED.into(),
        ] {
            for fragment in [
                "did not wait",
                "cargo target directory",
                "/tmp",
                "Running processes",
            ] {
                assert!(text.contains(fragment));
            }
        }
        assert!(gpu_requested(RequestedMode::Shared).contains("several minutes"));
    }
}
