//! The fleet driver's fake CLI output must match the typed API shapes.
//! This test loads the JSON sample the Python stub clones so a renamed
//! field fails here instead of silently drifting in the fake.
use swarmy_api_types::{Agent, Session};

const SAMPLE: &str = include_str!("../../../scripts/fleet/fixtures/typed_sample.json");

#[test]
fn fleet_fixture_matches_typed_structs() {
    let value: serde_json::Value =
        serde_json::from_str(SAMPLE).expect("fleet fixture is valid JSON");
    let agent: Agent = serde_json::from_value(value["agent_show"].clone())
        .expect("agent_show matches api::Agent");
    assert_eq!(agent.id, "01AGENT");
    assert_eq!(agent.name, "worker-1");
    assert_eq!(agent.main_session_id.as_deref(), Some("01AAAA"));
    assert_eq!(
        agent.usage.as_ref().map(|usage| usage.cost_dollars.as_str()),
        Some("1.25")
    );

    let listed: Session = serde_json::from_value(value["session_ls_item"].clone())
        .expect("session_ls_item matches api::Session");
    assert_eq!(listed.id, "01AAAA");
    assert_eq!(listed.state, swarmy_api_types::SessionState::Sleeping);
    assert_eq!(listed.agent_name.as_deref(), Some("worker-1"));
    assert!(listed.state_since.is_some());
    let listed_value = serde_json::to_value(&listed).expect("session serializes");
    for key in ["id", "state", "state_since", "agent_name"] {
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
