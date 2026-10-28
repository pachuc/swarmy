#![deny(clippy::disallowed_methods)]
use super::{CacheAffinity, RouteSnapshot, RouteStepStatus};
use jiff::Timestamp;
use std::time::Duration;

fn snapshot() -> RouteSnapshot {
    RouteSnapshot {
        name: Some("ab".into()),
        steps: ["a", "b"]
            .map(|entry| RouteStepStatus {
                provider: "fake".into(),
                label: Some(entry.into()),
                model: None,
                open_until: None,
                reason: None,
            })
            .into(),
        skipped: vec![],
        affinity: Some(CacheAffinity {
            provider: "fake".into(),
            entry: "b".into(),
            model: "test-model".into(),
            route: Some("ab".into()),
            completed_at: Timestamp::from_second(1_700_000_000).unwrap(),
        }),
    }
}
fn at(secs: u64) -> Timestamp {
    snapshot()
        .affinity
        .unwrap()
        .completed_at
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}
fn pick(s: &RouteSnapshot, from: u32, window: u64, now: Timestamp) -> Option<usize> {
    s.affine_step(from, Duration::from_secs(window), "test-model", now)
}
#[test]
fn affine_step_keeps_warm_closed_entry() {
    assert_eq!(pick(&snapshot(), 0, 300, at(60)), Some(1));
}
#[test]
fn affine_step_lapses_at_window_end() {
    assert_eq!(pick(&snapshot(), 0, 300, at(299)), Some(1));
    assert_eq!(pick(&snapshot(), 0, 300, at(300)), None);
}
#[test]
fn affine_step_zero_window_disables() {
    assert_eq!(pick(&snapshot(), 0, 0, at(1)), None);
}
#[test]
fn affine_step_skips_open_breaker() {
    let mut s = snapshot();
    s.steps[1].open_until = Some(at(100));
    assert_eq!(pick(&s, 0, 300, at(1)), None);
}
#[test]
fn affine_step_never_leaves_route() {
    let mut s = snapshot();
    s.steps[1].label = Some("c".into());
    assert_eq!(pick(&s, 0, 300, at(1)), None);
}
#[test]
fn affine_step_yields_to_route_change() {
    let mut s = snapshot();
    s.name = Some("ba".into());
    assert_eq!(pick(&s, 0, 300, at(1)), None);
    s.name = None;
    assert_eq!(pick(&s, 0, 300, at(1)), None);
    s.affinity.as_mut().unwrap().route = None;
    assert_eq!(pick(&s, 0, 300, at(1)), Some(1));
}
#[test]
fn affine_step_waits_for_chain_start() {
    assert_eq!(pick(&snapshot(), 1, 300, at(1)), None);
}
#[test]
fn affine_step_matches_model_and_step_override() {
    let mut s = snapshot();
    s.steps[0].label = Some("b".into());
    s.steps[0].model = Some("m1".into());
    s.steps[1].model = Some("m2".into());
    s.affinity.as_mut().unwrap().model = "m2".into();
    assert_eq!(pick(&s, 0, 300, at(1)), Some(1));
    s.steps[0].label = Some("a".into());
    s.steps[1].model = None;
    s.affinity.as_mut().unwrap().model = "test-model".into();
    assert_eq!(pick(&s, 0, 300, at(1)), Some(1));
    s.affinity.as_mut().unwrap().model = "other".into();
    assert_eq!(pick(&s, 0, 300, at(1)), None);
}
#[test]
fn affine_step_without_affinity_is_none() {
    let mut s = snapshot();
    s.affinity = None;
    assert_eq!(pick(&s, 0, 300, at(1)), None);
}
