//! Sandbox tools expose schemas here and execute on the assigned node.
use futures::future::BoxFuture;
use serde_json::{Value, json};
use swarmy_harness::Tool;

pub struct Bash;

impl Tool for Bash {
    fn name(&self) -> &'static str {
        "bash"
    }
    fn description(&self) -> &'static str {
        "Run a bash command in the session sandbox and record its disk state."
    }
    fn parameters(&self) -> Value {
        json!({"type":"object", "properties": {
            "command": {"type":"string"},
            "timeout_ms": {"type":"integer", "minimum":1, "maximum":3_600_000, "default":120_000}
        }, "required":["command"], "additionalProperties":false})
    }
    fn sandbox_bound(&self) -> bool {
        true
    }
    fn execute(&self, _: Value) -> BoxFuture<'_, Result<String, String>> {
        Box::pin(async { Err("bash must execute on a sandbox node".into()) })
    }
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
