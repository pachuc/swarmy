use super::*;
use swarmy_core::{AgentRecord, ReasoningEffort};

fn success(output: std::process::Output) -> String {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test]
async fn create_set_and_show_inference_settings_in_text_and_json() {
    run(|fixture| async move {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("prompt.txt");
        let prompt = "  Review this code.\nKeep the trailing newline.\n";
        std::fs::write(&path, prompt).unwrap();
        for json in [false, true] {
            let name = if json { "json-agent" } else { "text-agent" };
            let mut args = vec![
                "agent",
                "create",
                name,
                "--model",
                "agent-model",
                "--effort",
                "high",
            ];
            if json {
                args.extend(["--system-prompt-file", path.to_str().unwrap(), "--json"]);
            } else {
                args.extend(["--system-prompt", prompt]);
            }
            let output = success(fixture.output(&args).await);
            assert_output(
                &output,
                json,
                "Created",
                prompt,
                "agent-model",
                ReasoningEffort::High,
            );
            assert_show(&fixture, name, prompt, "agent-model", ReasoningEffort::High).await;
            for (flag, value, expected_prompt, expected_model, effort) in [
                (
                    "--model",
                    "changed-model",
                    prompt,
                    "changed-model",
                    ReasoningEffort::High,
                ),
                (
                    "--effort",
                    "none",
                    prompt,
                    "changed-model",
                    ReasoningEffort::None,
                ),
                (
                    "--system-prompt",
                    "",
                    "",
                    "changed-model",
                    ReasoningEffort::None,
                ),
                (
                    "--system-prompt-file",
                    path.to_str().unwrap(),
                    prompt,
                    "changed-model",
                    ReasoningEffort::None,
                ),
            ] {
                let mut args = vec!["agent", "set", name, flag, value];
                if json {
                    args.push("--json");
                }
                let output = success(fixture.output(&args).await);
                assert_output(
                    &output,
                    json,
                    "Updated",
                    expected_prompt,
                    expected_model,
                    effort,
                );
                assert_show(&fixture, name, expected_prompt, expected_model, effort).await;
            }
        }
        assert_default_output(&fixture).await;
    })
    .await;
}

fn assert_output(
    text: &str,
    json: bool,
    verb: &str,
    prompt: &str,
    model: &str,
    effort: ReasoningEffort,
) {
    if json {
        let agent: AgentRecord = serde_json::from_str(text).unwrap();
        assert_eq!(agent.system_prompt.as_deref(), Some(prompt));
        assert_eq!(agent.model.as_deref(), Some(model));
        assert_eq!(agent.reasoning_effort, Some(effort));
    } else {
        assert!(text.contains(&format!("{verb} agent")));
        for expected in [
            format!("system_prompt={prompt}"),
            format!("model={model}"),
            format!("reasoning_effort={effort}"),
        ] {
            assert!(text.contains(&expected), "missing {expected}: {text}");
        }
    }
}

async fn assert_show(
    fixture: &Fixture,
    name: &str,
    prompt: &str,
    model: &str,
    effort: ReasoningEffort,
) {
    let text = success(fixture.output(&["agent", "show", name]).await);
    for expected in [
        format!("system_prompt={prompt}"),
        format!("model={model}"),
        format!("reasoning_effort={effort}"),
    ] {
        assert!(text.contains(&expected), "missing {expected}: {text}");
    }
    let text = success(fixture.output(&["agent", "show", name, "--json"]).await);
    assert_output(&text, true, "", prompt, model, effort);
}

#[tokio::test]
async fn invalid_agent_settings_do_not_change_records() {
    run(|fixture| async move {
        let agent: AgentRecord = serde_json::from_str(&success(
            fixture
                .output(&["agent", "create", "original", "--json"])
                .await,
        ))
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing.txt");
        let invalid_utf8 = directory.path().join("invalid.txt");
        std::fs::write(&invalid_utf8, [0xff]).unwrap();
        for command in ["create", "set"] {
            let name = if command == "create" {
                "invalid"
            } else {
                "original"
            };
            for flags in [
                vec!["--effort", "unknown"],
                vec!["--effort", "HIGH"],
                vec![
                    "--system-prompt",
                    "inline",
                    "--system-prompt-file",
                    missing.to_str().unwrap(),
                ],
                vec!["--system-prompt-file", missing.to_str().unwrap()],
                vec!["--system-prompt-file", invalid_utf8.to_str().unwrap()],
            ] {
                let mut args = vec!["agent", command, name, "--model", "should-not-stick"];
                args.extend(flags);
                let output = fixture.output(&args).await;
                assert!(!output.status.success(), "accepted {args:?}");
            }
        }
        assert!(
            !fixture
                .output(&["agent", "set", "original"])
                .await
                .status
                .success()
        );
        assert!(
            !fixture
                .output(&["agent", "set", "missing", "--model", "new"])
                .await
                .status
                .success()
        );
        assert_eq!(
            fixture.store.get_agent(agent.agent_id).await.unwrap(),
            Some(agent)
        );
        assert!(
            fixture
                .store
                .get_agent_by_name("invalid")
                .await
                .unwrap()
                .is_none()
        );
    })
    .await;
}

async fn assert_default_output(fixture: &Fixture) {
    let created: AgentRecord = serde_json::from_str(&success(
        fixture
            .output(&["agent", "create", "defaults", "--json"])
            .await,
    ))
    .unwrap();
    assert!(
        created.system_prompt.is_none()
            && created.model.is_none()
            && created.reasoning_effort.is_none()
    );
    let text = success(fixture.output(&["agent", "show", "defaults"]).await);
    for field in ["system_prompt", "model", "reasoning_effort"] {
        assert!(text.contains(&format!("{field}=(stack default)")));
    }
    let shown: serde_json::Value = serde_json::from_str(&success(
        fixture
            .output(&["agent", "show", "defaults", "--json"])
            .await,
    ))
    .unwrap();
    for field in ["system_prompt", "model", "reasoning_effort"] {
        assert!(shown[field].is_null());
    }
}
