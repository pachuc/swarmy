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
    assert_eq!((report.from, report.to), (1, 3));
    assert!(report.removed.is_empty());
    assert!(report.interrupted.is_empty());
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
    assert_eq!((report.from, report.to), (3, 1));
    assert!(report.added.is_empty());
    assert!(report.interrupted.is_empty());
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
    assert!(error.to_string().contains("were started again"));
    assert!(error.to_string().contains("--force"));
    assert!(host.calls().contains(&"start fleet-workers-3".into()));
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        3
    );
}
#[tokio::test]
async fn failed_restart_is_not_reported_as_restarted() {
    let f = Fixture::new();
    let host = FakeHost::new();
    let settings = settings();
    let spec = grown_three(&f, &host, &settings).await;
    f.workers(1);
    *host.fail_start.lock().unwrap() = Some("fleet-workers-3".into());
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
            .contains("fleet-workers-3 could not be restarted")
    );
    assert!(!error.to_string().contains("were started again"));
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
    let mut spec = f.spec;
    spec.groups
        .insert("services".into(), spec.groups["control"].clone());
    spec.placement.services = "services".into();
    assert!(
        check_scalable(&spec, "services")
            .unwrap_err()
            .to_string()
            .contains("runs the services")
    );
    spec.groups
        .insert("unused".into(), spec.groups["control"].clone());
    assert!(
        check_scalable(&spec, "unused")
            .unwrap_err()
            .to_string()
            .contains("not a node group")
    );
    spec.groups.get_mut("workers").unwrap().module = Module::Local;
    assert!(
        check_scalable(&spec, "workers")
            .unwrap_err()
            .to_string()
            .contains("runs on this machine")
    );
}

async fn grown_three(f: &Fixture, host: &FakeHost, settings: &Settings) -> SwarmSpec {
    f.up(host).await.unwrap();
    f.workers(3);
    scale_swarm_with(host, request(f, settings, 3))
        .await
        .unwrap();
    f.registry.read_spec("fleet").unwrap().unwrap()
}
#[tokio::test]
async fn force_interrupts_and_reports() {
    let f = Fixture::new();
    let host = FakeHost::new();
    let settings = settings();
    let spec = grown_three(&f, &host, &settings).await;
    f.workers(1);
    host.drain_results.lock().unwrap().insert(
        "fleet-workers-2".into(),
        Ok(ssh::Drain {
            stopped: true,
            busy: vec!["agent-a".into()],
            unreleased: vec![],
        }),
    );
    let report = scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            force: true,
            ..request(&f, &settings, 1)
        },
    )
    .await
    .unwrap();
    assert_eq!(report.interrupted, ["agent-a"]);
    assert!(
        host.calls()
            .contains(&"drain fleet-workers-2 timeout=600 force=true".into())
    );
}
#[tokio::test]
async fn unreachable_node_needs_force() {
    let f = Fixture::new();
    let host = FakeHost::new();
    let settings = settings();
    let spec = grown_three(&f, &host, &settings).await;
    f.workers(1);
    host.drain_results
        .lock()
        .unwrap()
        .insert("fleet-workers-3".into(), Err("unreachable".into()));
    let error = scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            ..request(&f, &settings, 1)
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("--force"));
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        3
    );
    let report = scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            force: true,
            ..request(&f, &settings, 1)
        },
    )
    .await
    .unwrap();
    assert_eq!(report.removed, ["fleet-workers-3", "fleet-workers-2"]);
}
#[tokio::test]
async fn unreleased_computers_warn_and_continue() {
    let f = Fixture::new();
    let host = FakeHost::new();
    let settings = settings();
    let spec = grown_three(&f, &host, &settings).await;
    f.workers(1);
    host.drain_results.lock().unwrap().insert(
        "fleet-workers-2".into(),
        Ok(ssh::Drain {
            stopped: true,
            busy: vec![],
            unreleased: vec!["agent-b".into()],
        }),
    );
    let report = scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            ..request(&f, &settings, 1)
        },
    )
    .await
    .unwrap();
    assert_eq!(report.unreleased, ["agent-b"]);
}
#[tokio::test]
async fn failed_grow_apply_keeps_the_record_and_resumes() {
    let f = Fixture::new();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    f.workers(2);
    let settings = settings();
    std::fs::write(f.temp.path().join("fail-apply-workers"), "").unwrap();
    assert!(
        scale_swarm_with(&host, request(&f, &settings, 2))
            .await
            .is_err()
    );
    assert_eq!(
        f.registry.read_spec("fleet").unwrap().unwrap().groups["workers"].count,
        Some(2)
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        1
    );
    assert!(
        f.registry
            .swarm_dir("fleet")
            .unwrap()
            .join("tofu/workers/terraform.tfstate")
            .exists()
    );
    std::fs::remove_file(f.temp.path().join("fail-apply-workers")).unwrap();
    let spec = f.registry.read_spec("fleet").unwrap().unwrap();
    let spec_before_retry = std::fs::read(f.registry.spec_path("fleet").unwrap()).unwrap();
    let report = scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            ..request(&f, &settings, 2)
        },
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read(f.registry.spec_path("fleet").unwrap()).unwrap(),
        spec_before_retry
    );
    assert_eq!(report.added, ["fleet-workers-2"]);
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        2
    );
    assert_eq!(
        f.registry.read_spec("fleet").unwrap().unwrap().groups["workers"].count,
        Some(2)
    );
}
#[tokio::test]
async fn failed_shrink_apply_is_finished_by_rerun() {
    let f = Fixture::new();
    let host = FakeHost::new();
    let settings = settings();
    f.up(&host).await.unwrap();
    f.workers(2);
    scale_swarm_with(&host, request(&f, &settings, 2))
        .await
        .unwrap();
    f.workers(1);
    let spec = f.registry.read_spec("fleet").unwrap().unwrap();
    std::fs::write(f.temp.path().join("fail-apply-workers"), "").unwrap();
    assert!(
        scale_swarm_with(
            &host,
            ScaleRequest {
                spec: &spec,
                ..request(&f, &settings, 1)
            }
        )
        .await
        .is_err()
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        1
    );
    std::fs::remove_file(f.temp.path().join("fail-apply-workers")).unwrap();
    let spec = f.registry.read_spec("fleet").unwrap().unwrap();
    let previous = host.calls().len();
    scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            ..request(&f, &settings, 1)
        },
    )
    .await
    .unwrap();
    assert!(
        !host.calls()[previous..]
            .iter()
            .any(|c| c.starts_with("drain "))
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        1
    );
    assert_eq!(
        f.registry.read_spec("fleet").unwrap().unwrap().groups["workers"].count,
        Some(1)
    );
}
#[tokio::test]
async fn unprovisioned_victims_are_not_drained() {
    let f = Fixture::new();
    let host = FakeHost::new();
    let settings = settings();
    f.up(&host).await.unwrap();
    f.workers(2);
    scale_swarm_with(&host, request(&f, &settings, 2))
        .await
        .unwrap();
    let spec = f.registry.read_spec("fleet").unwrap().unwrap();
    let mut record = f.registry.read_record("fleet").unwrap().unwrap();
    record.hosts.get_mut("workers").unwrap()[1].provisioned = false;
    f.registry.write_record(&record).unwrap();
    f.workers(1);
    let previous = host.calls().len();
    scale_swarm_with(
        &host,
        ScaleRequest {
            spec: &spec,
            ..request(&f, &settings, 1)
        },
    )
    .await
    .unwrap();
    assert!(
        !host.calls()[previous..]
            .iter()
            .any(|c| c.starts_with("drain "))
    );
    assert!(
        !host.calls()[previous..]
            .iter()
            .any(|c| c.starts_with("revoke "))
    );
}

#[tokio::test]
async fn existing_group_shrink_drains_decommissions_and_truncates() {
    let mut f = Fixture::new();
    f.all_existing_workers(3);
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let settings = settings();
    let prev = host.calls().len();
    let report = scale_swarm_with(&host, request(&f, &settings, 1))
        .await
        .unwrap();
    assert_eq!(report.removed, ["fleet-workers-3", "fleet-workers-2"]);
    let calls = &host.calls()[prev..];
    assert_eq!(
        calls,
        [
            "drain fleet-workers-3 timeout=600 force=false",
            "drain fleet-workers-2 timeout=600 force=false",
            "revoke fleet-workers-3 primary=fleet-control-1",
            "revoke fleet-workers-2 primary=fleet-control-1",
            "decommission fleet-workers-3",
            "decommission fleet-workers-2"
        ]
    );
    assert_eq!(
        f.registry.read_spec("fleet").unwrap().unwrap().groups["workers"]
            .hosts
            .len(),
        1
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"].len(),
        1
    );
}
#[tokio::test]
async fn existing_group_decommission_failure_keeps_failed_host() {
    let mut f = Fixture::new();
    f.all_existing_workers(3);
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    *host.fail_decommission.lock().unwrap() = Some("fleet-workers-2".into());
    let settings = settings();
    let error = scale_swarm_with(&host, request(&f, &settings, 1))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("stopped at host fleet-workers-2")
    );
    assert_eq!(
        f.registry.read_spec("fleet").unwrap().unwrap().groups["workers"]
            .hosts
            .len(),
        2
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"]
            .iter()
            .map(|h| h.name.as_str())
            .collect::<Vec<_>>(),
        ["fleet-workers-1", "fleet-workers-2"]
    );
    assert_eq!(
        f.registry.read_spec("fleet").unwrap().unwrap().groups["workers"].hosts[1].address,
        "203.0.113.22"
    );
}
#[tokio::test]
async fn existing_group_growth_refuses_without_changes() {
    let mut f = Fixture::new();
    f.all_existing_workers(2);
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let before_spec = std::fs::read(f.registry.spec_path("fleet").unwrap()).unwrap();
    let before_record = f.registry.read_record("fleet").unwrap().unwrap();
    let before_log = f.log();
    let before_calls = host.calls();
    let settings = settings();
    let error = scale_swarm_with(&host, request(&f, &settings, 3))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("swarmy swarm node add"));
    assert_eq!(
        std::fs::read(f.registry.spec_path("fleet").unwrap()).unwrap(),
        before_spec
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"],
        before_record.hosts["workers"]
    );
    assert_eq!(f.log(), before_log);
    assert_eq!(host.calls(), before_calls);
}

#[tokio::test]
async fn refusals_change_nothing() {
    let f = Fixture::new();
    let host = FakeHost::new();
    let settings = settings();
    let spec_path = f.registry.spec_path("fleet").unwrap();
    let before = std::fs::read(&spec_path).unwrap();
    let no_record = scale_swarm_with(&host, request(&f, &settings, 2))
        .await
        .unwrap_err();
    assert!(no_record.to_string().contains("is not up"));
    f.up(&host).await.unwrap();
    let previous_calls = host.calls().len();
    let previous_log = f.log();
    for group in ["missing", "control"] {
        assert!(
            scale_swarm_with(
                &host,
                ScaleRequest {
                    group,
                    ..request(&f, &settings, 2)
                }
            )
            .await
            .is_err()
        );
    }
    assert_eq!(before, std::fs::read(&spec_path).unwrap());
    assert_eq!(previous_calls, host.calls().len());
    assert_eq!(previous_log, f.log());
}
#[tokio::test]
async fn lowering_count_by_hand_points_at_scale() {
    let f = Fixture::new();
    let host = FakeHost::new();
    let settings = settings();
    f.up(&host).await.unwrap();
    f.workers(2);
    scale_swarm_with(&host, request(&f, &settings, 2))
        .await
        .unwrap();
    let mut spec = f.registry.read_spec("fleet").unwrap().unwrap();
    spec.groups.get_mut("workers").unwrap().count = Some(1);
    f.registry.write_spec(&spec).unwrap();
    let error = crate::provision::provision_swarm_with(
        &host,
        UpRequest {
            registry: &f.registry,
            spec: &spec,
            sources: vec![],
            settings: &settings,
            copy_credential: false,
            recipe: None,
            cache: f.temp.path(),
            bucket_keys: None,
            machines: Some(f.machines()),
        },
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("swarmy swarm scale fleet workers 1")
    );
}

#[tokio::test]
async fn module_node_group_cannot_scale_to_zero() {
    let f = Fixture::new();
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let settings = settings();
    assert!(
        scale_swarm_with(&host, request(&f, &settings, 0))
            .await
            .unwrap_err()
            .to_string()
            .contains("not elastic, so its count must be between 1 and 50")
    );
}

#[tokio::test]
async fn existing_group_cannot_scale_to_zero() {
    let mut f = Fixture::new();
    f.all_existing_workers(2);
    let host = FakeHost::new();
    f.up(&host).await.unwrap();
    let before_spec = std::fs::read(f.registry.spec_path("fleet").unwrap()).unwrap();
    let before_record = f.registry.read_record("fleet").unwrap().unwrap();
    let before_log = f.log();
    let before_calls = host.calls();
    let settings = settings();
    let error = scale_swarm_with(&host, request(&f, &settings, 0))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not elastic, so its count must be between 1 and 50")
    );
    assert_eq!(
        std::fs::read(f.registry.spec_path("fleet").unwrap()).unwrap(),
        before_spec
    );
    assert_eq!(
        f.registry.read_record("fleet").unwrap().unwrap().hosts["workers"],
        before_record.hosts["workers"]
    );
    assert_eq!(f.log(), before_log);
    assert_eq!(host.calls(), before_calls);
}
