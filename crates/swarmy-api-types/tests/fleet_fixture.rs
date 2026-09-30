#![deny(clippy::disallowed_methods)]
//! The fleet driver's fake CLI output must match the typed API shapes.
//! This test loads the JSON sample the Python stub clones so a renamed
//! field fails here instead of silently drifting in the fake.
use swarmy_api_types::{Agent, Session};

const SAMPLE: &str = include_str!("../../../scripts/fleet/fixtures/typed_sample.json");

#[test]
fn fleet_fixture_matches_typed_structs() {
    let value: serde_json::Value =
        serde_json::from_str(SAMPLE).expect("fleet fixture is valid JSON");
    let agent: Agent =
        serde_json::from_value(value["agent_show"].clone()).expect("agent_show matches api::Agent");
    assert_eq!(agent.id, "01AGENT");
    assert_eq!(agent.name, "worker-1");
    assert_eq!(agent.main_session_id.as_deref(), Some("01AAAA"));
    assert_eq!(
        agent
            .usage
            .as_ref()
            .map(|usage| usage.cost_dollars.as_str()),
        Some("1.25")
    );
    // Detail keys stay absent on summary rows: list output omits them
    // instead of printing nulls or zeros.
    let agent_value = serde_json::to_value(&agent).expect("agent serializes");
    for key in [
        "node_id",
        "session_count",
        "sessions",
        "placement",
        "sandbox_state",
    ] {
        assert!(agent_value.get(key).is_none(), "summary Agent omits {key}");
    }

    let listed: Session = serde_json::from_value(value["session_ls_item"].clone())
        .expect("session_ls_item matches api::Session");
    assert_eq!(listed.id, "01AAAA");
    assert_eq!(listed.state, swarmy_api_types::SessionState::Sleeping);
    assert_eq!(listed.agent_name.as_deref(), Some("worker-1"));
    assert!(listed.state_since.is_some());
    // The fleet driver follows this link to the successor session.
    assert_eq!(listed.next_session.as_deref(), Some("01BBBB"));
    let listed_value = serde_json::to_value(listed).expect("session serializes");
    for key in ["id", "state", "state_since", "agent_name", "next_session"] {
        assert!(
            listed_value.get(key).is_some(),
            "typed Session keeps key {key}"
        );
    }

    let shown: Session = serde_json::from_value(value["session_show_item"].clone())
        .expect("session_show_item matches api::Session");
    let waiting = shown.waiting.clone().expect("show fixture carries waiting");
    assert_eq!(waiting.reasons, vec!["429 rate limited".to_owned()]);
    let round_trip: Session =
        serde_json::from_value(serde_json::to_value(&shown).expect("show serializes"))
            .expect("show round trips");
    assert_eq!(round_trip, shown);
}
