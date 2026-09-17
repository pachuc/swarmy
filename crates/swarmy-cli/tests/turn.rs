use std::process::Command;

#[test]
fn configured_stack_records_every_stage_for_both_turn_shapes() {
    let Ok(image) = std::env::var("SWARMY_BENCH_IMAGE") else {
        eprintln!(
            "skipping turn benchmark integration: SWARMY_BENCH_IMAGE is unset; requires the turn fake script and a running node"
        );
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("turns.json");
    let output = Command::new(env!("CARGO_BIN_EXE_swarmy"))
        .args([
            "bench",
            "turn",
            "--turns",
            "1",
            "--image",
            &image,
            "--timeout-secs",
            "20",
            "--output",
        ])
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let samples: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(samples.len(), 4);
    for sample in samples {
        let events = sample["events"].as_array().unwrap();
        for stage in [
            "submitted",
            "appended",
            "nudged",
            "claimed",
            "inference_started",
            "inference_finished",
            "idle",
            "final_text_rendered",
            "input_enabled",
        ] {
            assert!(
                events.iter().any(|event| event["stage"] == stage),
                "missing {stage}"
            );
        }
        for stage in ["tool_dispatched", "tool_completed"] {
            assert_eq!(
                events.iter().any(|event| event["stage"] == stage),
                sample["shape"] == "bash"
            );
        }
        assert!(
            events
                .iter()
                .all(|event| event["turn_id"] == sample["turn_id"])
        );
    }
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("p50") && stdout.contains("p95") && stdout.contains("end_to_end"));
}
