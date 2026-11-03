#![deny(clippy::disallowed_methods)]
use super::{exec_guard, helper_timeout_error};
use swarmy_core::{NodeId, ignore_best_effort};

#[test]
fn prefetch_fields_report_only_prefetched_boots() {
    let mut stats = swarmy_volume::DeviceStats::default();
    assert_eq!(super::prefetch_fields(&stats), (None, None));
    stats.boot_prefetch_chunks = 3;
    stats.boot_prefetch_bytes = 786_432;
    stats.boot_prefetch_ms = Some(1200);
    assert_eq!(
        super::prefetch_fields(&stats),
        (Some(786_432), Some(1200.0))
    );
    stats.boot_prefetch_chunks = 0;
    stats.boot_prefetch_bytes = 0;
    stats.boot_prefetch_ms = Some(5);
    assert_eq!(super::prefetch_fields(&stats), (Some(0), Some(5.0)));
    stats.boot_prefetch_chunks = 2;
    stats.boot_prefetch_ms = None;
    assert_eq!(super::prefetch_fields(&stats), (Some(0), None));
}

#[test]
fn bash_guard_reserves_startup_after_default_yield() {
    let args = swarmy_core::SandboxArguments::Bash(swarmy_core::BashArguments {
        command: "true".into(),
        timeout_ms: 120_000,
        yield_seconds: 10,
        output_budget_bytes: 32_768,
    });
    assert_eq!(exec_guard(&args), std::time::Duration::from_secs(70));
}

#[test]
fn timed_out_helper_names_its_process() {
    let error = helper_timeout_error("bash", 70, Some("process-123"), "partial", "warning");
    assert!(error.contains("process process-123; use process_log or process_list"));
    assert!(error.contains("within 70 s"));
    assert!(error.contains("stdout: partial stderr: warning"));
}

struct Processes(tempfile::TempDir);

impl Processes {
    fn call(&self, action: &str, id: &str, command: &str, epoch: u64) -> serde_json::Value {
        self.call_with_options(action, id, command, epoch, None)
    }

    fn call_with_options(
        &self,
        action: &str,
        id: &str,
        command: &str,
        epoch: u64,
        options: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let script = include_str!("processes.py").replace(
            "Path('/var/lib/swarmy/processes')",
            &format!("Path({})", serde_json::json!(self.0.path())),
        );
        let mut args = vec![
            "-c".to_owned(),
            script,
            action.to_owned(),
            epoch.to_string(),
            id.to_owned(),
            command.to_owned(),
        ];
        if let Some(options) = options {
            args.push(options.to_string());
        }
        let output = std::process::Command::new("python3")
            .args(&args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn list(&self, options: serde_json::Value, epoch: u64) -> Vec<serde_json::Value> {
        let value = self.call_with_options("process_list", "", "", epoch, Some(options));
        value.as_array().cloned().unwrap_or_default()
    }

    async fn start_exited(&self, command: &str, epoch: u64) -> String {
        let id = swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
        self.call("process_start", &id, command, epoch);
        let exit = self.0.path().join(&id).join("exit.json");
        swarmy_testkit::eventually(
            "process did not exit",
            std::time::Duration::from_secs(5),
            async || exit.exists().then_some(()),
        )
        .await;
        id
    }
}

impl Drop for Processes {
    fn drop(&mut self) {
        for entry in std::fs::read_dir(self.0.path()).unwrap().flatten() {
            if let Ok(bytes) = std::fs::read(entry.path().join("record.json")) {
                let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                ignore_best_effort(
                    std::process::Command::new("kill")
                        .args(["-KILL", "--", &format!("-{}", record["pid"])])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status(),
                    "kill stale sandbox process",
                );
            }
        }
    }
}

#[test]
fn process_records_outlive_the_launching_exec_and_reject_stale_pids() {
    let processes = Processes(tempfile::tempdir().unwrap());
    let id = swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
    processes.call("process_start", &id, "echo ready; sleep 300", 1);
    let listed = processes.call("process_list", "", "", 1);
    assert_eq!(listed[0]["status"], "running");
    let restarted = processes.call("process_list", "", "", 2);
    assert_eq!(restarted[0]["status"], "restarted");
    let path = processes.0.path().join(&id).join("record.json");
    let mut record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let ticks = record["start_ticks"].clone();
    record["start_ticks"] = serde_json::json!("impossible start time");
    std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
    assert_eq!(
        processes.call("process_list", "", "", 1)[0]["status"],
        "exited"
    );
    processes.call("process_stop", &id, "", 1);
    record["start_ticks"] = ticks;
    std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
    assert_eq!(
        processes.call("process_list", "", "", 1)[0]["status"],
        "running"
    );
    processes.call("process_stop", &id, "", 1);
    assert_eq!(
        processes.call("process_list", "", "", 1)[0]["status"],
        "exited"
    );
}

async fn seed_exited(processes: &Processes, count: usize, epoch: u64) -> Vec<String> {
    let mut ids = Vec::with_capacity(count);
    for index in 0..count {
        ids.push(
            processes
                .start_exited(&format!("true # {index}"), epoch)
                .await,
        );
    }
    ids
}

fn assert_newest_first(records: &[serde_json::Value]) {
    // Running records lead; within each group the order is newest first.
    let mut seen_exited = false;
    for window in records.windows(2) {
        let running = window[0]["status"] == "running";
        assert!(
            !(running && seen_exited),
            "running process listed after an exited one"
        );
        seen_exited |= !running;
        if window[0]["status"] == window[1]["status"] {
            let first = window[0]["started_at"].as_f64().unwrap_or_default();
            let second = window[1]["started_at"].as_f64().unwrap_or_default();
            assert!(first >= second, "process list is not newest first");
        }
    }
}

#[tokio::test]
async fn process_list_is_bounded_newest_first_with_overrides() {
    let processes = Processes(tempfile::tempdir().unwrap());
    let exited = seed_exited(&processes, 30, 1).await;
    let running = swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
    processes.call("process_start", &running, "sleep 300", 1);
    let default = processes.list(serde_json::json!({}), 1);
    assert_eq!(default.len(), 21);
    assert_newest_first(&default);
    assert!(default.iter().any(|item| item["process_id"] == running));
    assert_eq!(default[0]["process_id"], running);
    let newest_exited: Vec<&String> = exited.iter().rev().take(20).collect();
    for id in &newest_exited {
        assert!(default.iter().any(|item| item["process_id"] == **id));
    }
    let oldest = &exited[0];
    assert!(!default.iter().any(|item| item["process_id"] == *oldest));
    let limited = processes.list(serde_json::json!({"limit": 5}), 1);
    assert_eq!(limited.len(), 6);
    assert_newest_first(&limited);
    let all = processes.list(serde_json::json!({"all": true}), 1);
    assert_eq!(all.len(), 31);
    assert_newest_first(&all);
}

#[test]
fn process_list_truncates_long_commands() {
    let processes = Processes(tempfile::tempdir().unwrap());
    let long = "x".repeat(2000);
    let id = swarmy_core::ProcessId::from_ulid(ulid::Ulid::generate()).to_string();
    processes.call("process_start", &id, &long, 1);
    let listed = processes.list(serde_json::json!({"all": true}), 1);
    let record = listed
        .iter()
        .find(|item| item["process_id"] == id)
        .expect("long command missing");
    let command = record["command"].as_str().unwrap_or_default();
    assert_eq!(command.len(), 512);
    assert!(command.ends_with("..."));
}

#[tokio::test]
async fn failed_call_is_released_for_redelivery() {
    let Some(stack) = swarmy_testkit::Stack::load("toolserver") else {
        return;
    };
    let mut guard = swarmy_testkit::StackGuard::new(&stack);
    let bus = swarmy_bus::Bus::connect(
        &stack.nats_url,
        swarmy_bus::Config {
            prefix: Some(swarmy_bus::SubjectToken::new(&stack.prefix).unwrap()),
            ..swarmy_bus::Config::default()
        },
    )
    .await
    .unwrap();
    let node = NodeId::from_ulid(ulid::Ulid::generate());
    let queue = swarmy_bus::WorkQueue::NodeTools(node);
    bus.setup(std::slice::from_ref(&queue)).await.unwrap();
    let session = swarmy_core::SessionId::from_ulid(ulid::Ulid::generate());
    let job = swarmy_core::ToolJob {
        session_id: session,
        request_id: swarmy_core::RequestId::for_step(session, 1),
        call_id: swarmy_core::ToolCallId("bash".into()),
        step: 1,
        arguments: swarmy_core::SandboxArguments::Bash(swarmy_core::BashArguments {
            command: "true".into(),
            timeout_ms: 120_000,
            yield_seconds: 10,
            output_budget_bytes: 32_768,
        }),
    };
    bus.publish_work(&queue, &job).await.unwrap();
    let mut messages = bus.consume::<swarmy_core::ToolJob>(&queue).await.unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.value.request_id, job.request_id);
    super::settle(&first, Err(anyhow::anyhow!("injected store failure"))).await;
    let second = tokio::time::timeout(std::time::Duration::from_secs(10), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.value.request_id, job.request_id);
    super::settle(&second, Ok(())).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(3), messages.next())
            .await
            .is_err()
    );
    guard.cleanup().await;
}
