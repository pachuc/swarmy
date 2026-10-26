#![deny(clippy::disallowed_methods)]
use super::*;
use crate::provision::{test_support::Fixture, tests::FakeHost};
use swarmy_config::Settings;

fn settings() -> Settings {
    let mut settings = Settings::default();
    settings.selection.provider = "fake".into();
    settings
}
fn request<'a>(f: &'a Fixture, settings: &'a Settings, count: u32) -> ScaleRequest<'a> {
    ScaleRequest {
        registry: &f.registry,
        spec: &f.spec,
        group: "workers",
        count,
        drain_timeout: Duration::from_secs(600),
        force: false,
        machines: Some(f.machines()),
        settings,
        cache: f.temp.path(),
    }
}
#[tokio::test]
async fn grow_provisions_only_the_new_machines() {
    let f = Fixture::new();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let previous = host.calls().len();
    f.workers(3);
    let settings = settings();
    let report = scale_swarm_with(&host, request(&f, &settings, 3))
        .await
        .unwrap();
    assert_eq!(report.added, ["fleet-workers-2", "fleet-workers-3"]);
    assert_eq!(
        host.calls()[previous..]
            .iter()
            .filter(|c| c.starts_with("ready "))
            .count(),
        2
    );
    assert_eq!(
        f.registry.read_spec("fleet").unwrap().unwrap().groups["workers"].count,
        Some(3)
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        3
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(f.temp.path().join("applied-workers-2.json")).unwrap()
        )
        .unwrap()["machine_count"],
        3
    );
}
#[tokio::test]
async fn shrink_drains_highest_first_then_applies_the_lower_count() {
    let f = Fixture::new();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    f.workers(3);
    let settings = settings();
    scale_swarm_with(&host, request(&f, &settings, 3))
        .await
        .unwrap();
    let mut spec = f.registry.read_spec("fleet").unwrap().unwrap();
    let previous = host.calls().len();
    f.workers(1);
    let report = scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            ..request(&f, &settings, 1)
        },
    )
    .await
    .unwrap();
    assert_eq!(report.removed, ["fleet-workers-3", "fleet-workers-2"]);
    assert_eq!(
        &host.calls()[previous..],
        [
            "drain fleet-workers-3 timeout=600 force=false",
            "drain fleet-workers-2 timeout=600 force=false",
            "revoke fleet-workers-3 primary=fleet-control-1",
            "revoke fleet-workers-2 primary=fleet-control-1",
        ]
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        1
    );
    spec = f.registry.read_spec("fleet").unwrap().unwrap();
    assert_eq!(spec.groups["workers"].count, Some(1));
}
#[tokio::test]
async fn busy_node_restarts_drained_nodes_and_removes_nothing() {
    let f = Fixture::new();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    f.workers(3);
    let settings = settings();
    scale_swarm_with(&host, request(&f, &settings, 3))
        .await
        .unwrap();
    let spec = f.registry.read_spec("fleet").unwrap().unwrap();
    host.drain_results.lock().unwrap().insert(
        "fleet-workers-2".into(),
        Ok(ssh::Drain {
            stopped: false,
            busy: vec!["agent-a".into()],
            unreleased: vec![],
        }),
    );
    let error = scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            ..request(&f, &settings, 1)
        },
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("still has running sandbox commands after 600s")
    );
    assert!(host.calls().contains(&"start fleet-workers-3".into()));
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        3
    );
}
#[test]
fn refusals_are_ordered() {
    let f = Fixture::new();
    assert!(
        check_scalable(&f.spec, "missing")
            .unwrap_err()
            .to_string()
            .contains("has no group")
    );
    assert!(
        check_scalable(&f.spec, "control")
            .unwrap_err()
            .to_string()
            .contains("runs the stack")
    );
    assert_eq!(
        check_scalable(&f.spec, "workers").unwrap(),
        GroupKind::Module
    );
}
