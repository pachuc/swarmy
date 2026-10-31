//! Desktop acceptance: a scripted agent logs into an app in its sandbox, changes a setting,
//! and takes screenshots. The harness checks the app's record and the image bytes.
use std::{path::Path, process::Command, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use swarmy_core::{Event, ToolResult};
use swarmy_store::blob::{BlobStore, ObjectBlobStore};
use tokio::time::Instant;

use crate::Fixture;

type ToolCompletions<'a> = Vec<(&'a swarmy_core::ToolCallId, &'a ToolResult)>;

const APP: &str = include_str!("../../../images/base-desktop/tests/desktop_check_app.py");
const DIR: &str = "/home/agent/desktop-check";
const PORT: u16 = 8765;
const USER: &str = "desk";
const PASSWORD: &str = "desk-pass-7341";
const DONE: &str = "Logged in, saved Theme = Dark, and took screenshots. CODE: unknown (a scripted agent cannot read pixels).";
const MOVES: [&str; 17] = [
    "write",
    "bash",
    "browser_navigate",
    "browser_type",
    "browser_type",
    "browser_click",
    "bash",
    "browser_navigate",
    "browser_select",
    "browser_click",
    "bash",
    "browser_navigate",
    "browser_screenshot",
    "screen_windows",
    "screen_screenshot",
    "bash",
    "bash",
];
const APP_START: usize = 1;
const VIEW_WAIT: usize = 6;
const SAVE_WAIT: usize = 10;
const SETTINGS_SNAPSHOT: usize = 11;
const BROWSER_IMAGE: usize = 12;
const WINDOWS: usize = 13;
const SCREEN_IMAGE: usize = 14;
const MEMORY_PEAK: usize = 15;
const RENDERER: usize = 16;

pub(crate) fn script() -> serde_json::Map<String, Value> {
    let url = format!("http://127.0.0.1:{PORT}");
    let start = format!(
        "cd {DIR} && python3 app.py --background --port {PORT} --record record.json --log app.log --username {USER} --password {PASSWORD}"
    );
    let inputs = vec![
        json!({"path":format!("{DIR}/app.py"),"content":APP}),
        json!({"command":start,"yield_seconds":60}),
        json!({"url":format!("{url}/")}),
        json!({"ref":"{{ref:textbox \"Username\"}}","text":USER,"submit":false}),
        json!({"ref":"{{ref:textbox \"Password\"}}","text":PASSWORD,"submit":false}),
        json!({"ref":"{{ref:button \"Log in\"}}"}),
        json!({"command":format!("curl -fsS --max-time 30 '{url}/wait?event=settings_view&timeout=25'")}),
        json!({"url":format!("{url}/settings")}),
        json!({"ref":"{{ref:combobox \"Theme\"}}","value":"dark"}),
        json!({"ref":"{{ref:button \"Save\"}}"}),
        json!({"command":format!("curl -fsS --max-time 30 '{url}/wait?event=setting_saved&timeout=25'")}),
        json!({"url":format!("{url}/settings")}),
        json!({}),
        json!({}),
        json!({}),
        json!({"command":"cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo unavailable"}),
        json!({"command":"cat /run/swarmy-display/renderer && glxinfo -B | grep 'OpenGL renderer string' && vulkaninfo --summary 2>/dev/null | grep -E 'deviceType|driverID' && fc-list ':lang=zh-cn' family"}),
    ];
    let mut responses: serde_json::Map<_, _> = inputs
        .into_iter()
        .enumerate()
        .map(|(i, input)| {
            (
                i.to_string(),
                crate::scripted_tool("desktop", i, MOVES[i], &input),
            )
        })
        .collect();
    responses.insert(MOVES.len().to_string(), crate::scripted_text(DONE));
    responses
}

pub(crate) fn prompt() -> String {
    format!(
        "This is a scripted desktop check. Write and start the app, log in at http://127.0.0.1:{PORT}/ as {USER} with {PASSWORD}, save Dark, and take browser and screen screenshots."
    )
}

pub(crate) async fn exercise(f: &mut Fixture, output: Option<&Path>) -> Result<()> {
    let agent = f
        .store
        .create_agent(
            "desktop-proof",
            "chaos:test",
            "Desktop proof",
            jiff::Timestamp::now(),
            None,
        )
        .await?;
    let (session, _) = f
        .store
        .open_main_session(agent.agent_id, jiff::Timestamp::now())
        .await?;
    f.sessions.push(session);
    let started = Instant::now();
    crate::send_user(&f.store, &f.bus, session, &prompt()).await?;
    let events = crate::wait_idle(f, session, Duration::from_secs(300), "desktop session").await?;
    verify(
        f,
        agent.agent_id,
        &events,
        output,
        started.elapsed().as_secs_f64(),
    )
    .await
}

async fn verify(
    f: &Fixture,
    agent: swarmy_core::AgentId,
    events: &[Event],
    output: Option<&Path>,
    elapsed: f64,
) -> Result<()> {
    verify_advertised(f, events).await?;
    let (names, results) = verify_tools(events)?;
    let (record, call_log) = verify_record(f, agent, &results)?;
    let destination = output.map_or_else(
        || std::env::temp_dir().join(format!("swarmy-desktop-evidence-{}", f.prefix)),
        Path::to_path_buf,
    );
    std::fs::create_dir_all(&destination)?;
    std::fs::write(
        destination.join("record.json"),
        serde_json::to_vec_pretty(&record)?,
    )?;
    let code = record["code"]
        .as_str()
        .context("desktop record has no code")?;
    let images = verify_images(&results, &call_log, &destination, code).await?;
    let peak: swarmy_core::BashResult = match results[MEMORY_PEAK].1 {
        ToolResult::Completed { output, .. } => serde_json::from_str(output)?,
        ToolResult::Error { .. } => unreachable!("checked above"),
    };
    std::fs::write(
        destination.join("summary.json"),
        serde_json::to_vec_pretty(&json!({
            "tools":names,"images":images,"memory_peak":peak.stdout.trim(),"elapsed_seconds":elapsed
        }))?,
    )?;
    tracing::info!(directory = %destination.display(), "desktop proof passed");
    Ok(())
}

async fn verify_advertised(f: &Fixture, events: &[Event]) -> Result<()> {
    let request_id = events
        .iter()
        .find_map(|event| match event {
            Event::InferenceRequested { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .context("desktop inference request missing")?;
    let job = f
        .store
        .get_inference_input::<swarmy_llm::InferenceJob>(request_id)
        .await?
        .context("desktop inference input missing")?;
    for required in [
        "browser_navigate",
        "browser_screenshot",
        "screen_screenshot",
        "screen_windows",
    ] {
        ensure!(
            job.request.tools.iter().any(|tool| tool.name == required),
            "desktop inference did not advertise {required}"
        );
    }
    ensure!(
        job.request
            .system_prompt
            .contains(swarmy_tools::DISPLAY_PROMPT),
        "desktop inference missing display tool prompt"
    );
    Ok(())
}

fn verify_tools(events: &[Event]) -> Result<(Vec<&str>, ToolCompletions<'_>)> {
    let expected = MOVES;
    let calls: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::ToolCallRequested { call, .. } => Some(call),
            _ => None,
        })
        .collect();
    let names: Vec<_> = calls.iter().map(|call| call.tool.as_str()).collect();
    let failure = events.iter().rev().find_map(|event| match event {
        Event::InferenceFailed { error, .. } => Some(error.as_str()),
        _ => None,
    });
    ensure!(
        names == expected,
        "desktop tool sequence differs: {names:?}; last inference error: {failure:?}"
    );
    let results: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::ToolCallCompleted {
                call_id, result, ..
            } => Some((call_id, result)),
            _ => None,
        })
        .collect();
    ensure!(
        results.len() == expected.len(),
        "desktop tool completions missing: {}",
        results.len()
    );
    for (index, (_, result)) in results.iter().enumerate() {
        let ToolResult::Completed { output, .. } = result else {
            anyhow::bail!("desktop tool {} failed: {result:?}", expected[index]);
        };
        if [APP_START, VIEW_WAIT, SAVE_WAIT, MEMORY_PEAK, RENDERER].contains(&index) {
            let bash: swarmy_core::BashResult = serde_json::from_str(output)
                .with_context(|| format!("desktop bash result at step {index} invalid"))?;
            ensure!(
                bash.exit_code == 0,
                "desktop bash step {index} exited {}: {}",
                bash.exit_code,
                bash.stderr
            );
            if index == APP_START {
                ensure!(
                    bash.stdout.contains("listening"),
                    "desktop app did not report listening"
                );
            }
            if index == RENDERER {
                verify_software_renderer(&bash.stdout)?;
                verify_cjk_font(&bash.stdout)?;
            }
        }
        if index == SETTINGS_SNAPSHOT {
            ensure!(
                output.contains("Theme saved: Dark") && output.contains("Verification code"),
                "desktop saved settings absent from browser output"
            );
        }
        if index == WINDOWS {
            ensure!(
                output.contains("Settings"),
                "desktop screen window title missing"
            );
        }
    }
    Ok((names, results))
}

/// The suite node has no card, so the display must be the software one:
/// Xvfb with Mesa's llvmpipe for OpenGL and lavapipe for Vulkan.
fn verify_software_renderer(stdout: &str) -> Result<()> {
    ensure!(
        stdout.lines().next() == Some("software"),
        "desktop renderer is not software: {stdout}"
    );
    ensure!(
        stdout.contains("OpenGL renderer string: llvmpipe"),
        "desktop OpenGL is not llvmpipe: {stdout}"
    );
    ensure!(
        stdout.contains("DRIVER_ID_MESA_LLVMPIPE")
            && stdout.contains("PHYSICAL_DEVICE_TYPE_CPU")
            && !stdout.contains("PHYSICAL_DEVICE_TYPE_DISCRETE_GPU"),
        "desktop Vulkan is not lavapipe alone: {stdout}"
    );
    Ok(())
}

/// Screenshots of Chinese, Japanese or Korean pages need a CJK font; without
/// one the text draws as empty boxes.
fn verify_cjk_font(stdout: &str) -> Result<()> {
    ensure!(
        stdout.contains("WenQuanYi Micro Hei"),
        "desktop image has no CJK font: {stdout}"
    );
    Ok(())
}

fn verify_record(
    f: &Fixture,
    agent: swarmy_core::AgentId,
    results: &ToolCompletions<'_>,
) -> Result<(Value, String)> {
    let record_path = f.files.path().join(format!(
        ".swarmy/node/bundles/{agent}/rootfs{DIR}/record.json"
    ));
    let record: Value = serde_json::from_slice(&std::fs::read(record_path)?)?;
    ensure!(
        record["login"]["ok"] == true && record["login"]["user"] == USER,
        "desktop app login record missing"
    );
    ensure!(
        record["failed_logins"] == 0
            && record["setting"]["theme"] == "dark"
            && record["setting"]["saved"] == true
            && record["settings_views"].as_u64().unwrap_or(0) >= 1,
        "desktop app setting record invalid: {record}"
    );
    let code = record["code"]
        .as_str()
        .context("desktop record has no code")?;
    let call_log = std::fs::read_to_string(f.files.path().join("calls"))?;
    ensure!(
        !call_log.contains(code),
        "desktop code leaked into provider request text"
    );
    for (_, result) in results {
        if let ToolResult::Completed { output, .. } = result {
            ensure!(!output.contains(code), "desktop code leaked into tool text");
        }
    }
    Ok((record, call_log))
}

async fn verify_images(
    results: &ToolCompletions<'_>,
    call_log: &str,
    destination: &Path,
    code: &str,
) -> Result<Vec<Value>> {
    let blobs = ObjectBlobStore::from_env()?;
    let mut images = Vec::new();
    for (index, filename, kind) in [
        (BROWSER_IMAGE, "browser-screenshot.png", "browser"),
        (SCREEN_IMAGE, "screen-screenshot.png", "screen"),
    ] {
        let ToolResult::Completed { metadata, .. } = results[index].1 else {
            unreachable!("checked above")
        };
        ensure!(
            metadata.get("image_media_type").and_then(Value::as_str) == Some("image/png"),
            "desktop step {index} has no PNG media type"
        );
        ensure!(
            !metadata.contains_key("image_base64"),
            "desktop image remains inline"
        );
        let key = metadata
            .get("image_object_key")
            .and_then(Value::as_str)
            .context("desktop screenshot blob key missing")?;
        let bytes = blobs.get(key).await?;
        let path = destination.join(filename);
        std::fs::write(&path, &bytes)?;
        let checker = crate::config::repo().join("images/base-desktop/tests/png_check.py");
        let png_result = Command::new("python3")
            .arg(checker)
            .arg(&path)
            .args(["--kind", kind, "--expect-code", code])
            .output()?;
        ensure!(
            png_result.status.success(),
            "desktop PNG {filename} failed: {}",
            String::from_utf8_lossy(&png_result.stderr)
        );
        let pixels: Value = serde_json::from_slice(&png_result.stdout)?;
        let reached = image_reached_provider(call_log, key, &bytes);
        ensure!(
            reached,
            "desktop {filename} bytes did not reach provider as image part"
        );
        images.push(
            json!({"file":filename,"width":pixels["width"],"height":pixels["height"],
            "bytes":bytes.len(),"sha256":sha256(&path)?}),
        );
    }
    Ok(images)
}

fn image_reached_provider(call_log: &str, key: &str, bytes: &[u8]) -> bool {
    call_log.lines().any(|line| {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return false;
        };
        value["messages"].as_array().is_some_and(|messages| {
            messages.iter().any(|message| {
                message["role"] == "user"
                    && message["parts"].as_array().is_some_and(|parts| {
                        parts
                            .iter()
                            .any(|part| image_matches(&part["image"], key, bytes))
                    })
            })
        })
    })
}

fn image_matches(image: &Value, key: &str, bytes: &[u8]) -> bool {
    image["object_key"] == key
        && image["bytes"].as_array().is_some_and(|array| {
            array.len() == bytes.len()
                && array
                    .iter()
                    .zip(bytes)
                    .all(|(value, byte)| value.as_u64() == Some(u64::from(*byte)))
        })
}

fn sha256(path: &Path) -> Result<String> {
    let output = Command::new("sha256sum").arg(path).output()?;
    ensure!(output.status.success(), "sha256sum failed");
    Ok(String::from_utf8(output.stdout)?
        .split_whitespace()
        .next()
        .context("sha256sum output missing")?
        .to_owned())
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use futures::StreamExt;
    use swarmy_core::{Message, MessageId, MessageRole, Part};
    use swarmy_llm::{Delta, GenerationSettings, Provider, Request};

    #[test]
    fn software_renderer_check_accepts_llvmpipe_and_lavapipe_only() {
        let good = "software\nOpenGL renderer string: llvmpipe (LLVM 17.0.6, 256 bits)\n\tdeviceType         = PHYSICAL_DEVICE_TYPE_CPU\n\tdriverID           = DRIVER_ID_MESA_LLVMPIPE\n";
        verify_software_renderer(good).unwrap();
        let nvidia = good.replacen("software", "nvidia", 1);
        assert!(verify_software_renderer(&nvidia).is_err());
        let card = format!("{good}\tdeviceType         = PHYSICAL_DEVICE_TYPE_DISCRETE_GPU\n");
        assert!(verify_software_renderer(&card).is_err());
        let hardware_gl = good.replace("llvmpipe (LLVM", "NVIDIA L4/PCIe/SSE2 (");
        assert!(verify_software_renderer(&hardware_gl).is_err());
    }

    #[test]
    fn cjk_font_check_needs_wenquanyi() {
        assert!(
            verify_cjk_font("software\nWenQuanYi Micro Hei,文泉驛微米黑,文泉驿微米黑\n").is_ok()
        );
        assert!(verify_cjk_font("software\nOpenGL renderer string: llvmpipe\n").is_err());
    }

    #[test]
    fn inference_failure_is_visible_in_sequence_error() {
        let session = swarmy_core::SessionId::from_ulid(ulid::Ulid::generate());
        let failed = Event::InferenceFailed {
            seq: 1,
            request_id: swarmy_core::RequestId::for_step(session, 1),
            error: "unknown accessibility ref".into(),
            retryable: false,
            retry_at: None,
            failure_kind: swarmy_core::FailureKind::Provider,
        };
        let error = verify_tools(&[failed]).unwrap_err().to_string();
        assert!(error.contains("unknown accessibility ref"));
    }

    #[tokio::test]
    async fn scripted_desktop_moves_resolve_refs_against_snapshots() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join("script"),
            serde_json::to_vec(&json!({"responses":script()})).unwrap(),
        )
        .unwrap();
        let fake = swarmy_llm::fake::FileFake::from_files(
            &temp.path().join("script"),
            &temp.path().join("calls"),
        )
        .unwrap();
        let mut messages = Vec::new();
        let login = "URL: http://127.0.0.1:8765/\nTitle: Login\n[e1] RootWebArea \"Login\"\n  [e4] textbox \"Username\"\n  [e5] textbox \"Password\"\n  [e6] button \"Log in\"";
        let settings = "URL: http://127.0.0.1:8765/settings\nTitle: Settings\n[e1] RootWebArea \"Settings\"\n  [e8] combobox \"Theme\"\n  [e9] button \"Save\"";
        let expected = MOVES;
        for (i, expected_name) in expected
            .iter()
            .enumerate()
            .chain(std::iter::once((MOVES.len(), &"")))
        {
            if i == 3 || i == 8 {
                messages.push(Message {
                    id: MessageId::from_ulid(ulid::Ulid::generate()),
                    role: MessageRole::Tool,
                    parts: vec![Part::ToolResult {
                        call_id: swarmy_core::ToolCallId("snapshot".into()),
                        result: ToolResult::Completed {
                            output: if i == 3 { login } else { settings }.into(),
                            title: String::new(),
                            metadata: std::collections::BTreeMap::default(),
                        },
                    }],
                });
            }
            let request = Request {
                system_prompt: String::new(),
                messages: messages.clone(),
                tools: vec![],
                settings: GenerationSettings::default(),
                no_cache: false,
            };
            let deltas: Vec<_> = fake.request(request).collect().await;
            let Delta::Completed(response) = deltas.last().unwrap().as_ref().unwrap() else {
                panic!("missing completion")
            };
            if i == MOVES.len() {
                assert!(matches!(&response.parts[0], Part::Text { text } if text == DONE));
            } else {
                let Part::ToolCall { tool, input, .. } = &response.parts[0] else {
                    panic!("missing tool call")
                };
                assert_eq!(tool, expected_name);
                let reference = match i {
                    3 => Some("e4"),
                    4 => Some("e5"),
                    5 => Some("e6"),
                    8 => Some("e8"),
                    9 => Some("e9"),
                    _ => None,
                };
                if let Some(reference) = reference {
                    assert_eq!(input["ref"], reference);
                }
            }
        }
    }
}
