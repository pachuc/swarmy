use std::collections::HashSet;

use anyhow::{Result, ensure};
use swarmy_core::{
    Event, MessageRole, Part, RequestId, SessionId, SessionRecord, SessionState, ToolResult,
};

pub const ANSWER: &str = "chaos session complete";

/// Check prefixes too: a duplicate must fail even if the broken worker then hangs.
pub fn log(id: SessionId, events: &[Event]) -> Result<usize> {
    let mut requests = HashSet::new();
    let mut steps = HashSet::new();
    let mut pending = None;
    for (index, event) in events.iter().enumerate() {
        ensure!(
            event.seq() == u64::try_from(index)? + 1,
            "session {id}: noncontiguous or duplicated sequence at {event:?}"
        );
        match event {
            Event::InferenceRequested {
                request_id, step, ..
            } => {
                ensure!(
                    requests.insert(*request_id) && steps.insert(*step) && pending.is_none(),
                    "session {id}: duplicate request for one step at {event:?}; pending={pending:?}"
                );
                ensure!(
                    *request_id == RequestId::for_step(id, *step),
                    "session {id}: invalid request id at {event:?}"
                );
                pending = Some(*request_id);
            }
            Event::InferenceCompleted { request_id, .. } => {
                ensure!(
                    pending.take() == Some(*request_id),
                    "session {id}: completion without its unique request at {event:?}"
                );
            }
            Event::InferenceFailed { .. } => {
                anyhow::bail!("session {id}: inference failed: {event:?}")
            }
            _ => {}
        }
    }
    Ok(requests.len())
}

pub fn finished(session: &SessionRecord, events: &[Event], expected_steps: usize) -> Result<()> {
    let id = session.session_id;
    ensure!(
        log(id, events)? == expected_steps,
        "session {id}: expected {expected_steps} request steps"
    );
    ensure!(
        session.state == SessionState::Idle,
        "session {id}: expected Idle, got {:?}",
        session.state
    );
    ensure!(
        events.last().map(Event::seq) == Some(session.head_seq),
        "session {id}: log head mismatch"
    );
    let last = events.iter().rev().find_map(|event| match event {
        Event::InferenceCompleted { message, .. } => Some(message),
        _ => None,
    });
    ensure!(
        last.is_some_and(|message| message.role == MessageRole::Assistant
            && message.parts
                == [Part::Text {
                    text: ANSWER.into()
                }]),
        "session {id}: unexpected final answer: {last:?}"
    );
    tool_results(id, events, expected_steps - 1)
}

fn tool_results(id: SessionId, events: &[Event], expected: usize) -> Result<()> {
    let mut tools = 0;
    let mut writes = 0;
    let mut recovery = None;
    for event in events {
        if let Event::MessageAppended { message, .. } = event
            && message.role == MessageRole::System
            && let [Part::Text { text }] = message.parts.as_slice()
        {
            if text.starts_with("Your computer recovery began at ") {
                // These short runs never reach the periodic checkpoint interval.
                writes = 0;
                recovery = Some(text);
            } else if text.starts_with("Your computer was evicted while idle. ") {
                recovery = Some(text);
            }
        }
        if let Event::ToolCallCompleted { result, .. } = event {
            let ToolResult::Completed {
                output,
                title,
                metadata,
            } = result
            else {
                let ToolResult::Error { error } = result else {
                    unreachable!()
                };
                ensure!(
                    recovery == Some(error),
                    "session {id}: failure lacks its matching recovery system message: {result:?}"
                );
                tools += 1;
                continue;
            };
            if title == "bash" {
                ensure!(
                    metadata["exit_code"] == 0 && metadata["stderr"] == "",
                    "session {id}: bash failed: {result:?}"
                );
                ensure!(
                    metadata["stdout"] == "swarmy\n".repeat(writes + 1),
                    "session {id}: disk contains missing or duplicated writes: {output:?}"
                );
                ensure!(
                    serde_json::from_str::<serde_json::Value>(output)?
                        == serde_json::json!(metadata),
                    "session {id}: provider output lost tool metadata"
                );
                ensure!(
                    metadata.contains_key("manifest_id"),
                    "session {id}: manifest missing"
                );
                writes += 1;
            } else {
                output.parse::<jiff::Timestamp>()?;
            }
            tools += 1;
        }
    }
    ensure!(
        tools == expected,
        "session {id}: expected {expected} tool results, got {tools}"
    );
    Ok(())
}

pub fn calls(actual: usize, steps: usize, gateway_kills: usize) -> Result<()> {
    ensure!(
        (steps..=steps + gateway_kills).contains(&actual),
        "provider call count {actual} outside {steps}..={} ({gateway_kills} gateway kills)",
        steps + gateway_kills
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use swarmy_core::{Message, MessageId};
    use ulid::Ulid;

    #[test]
    fn duplicate_requests_fail_even_with_new_ids_or_after_completion() {
        let id = SessionId::from_ulid(Ulid::generate());
        let request = |seq, step| Event::InferenceRequested {
            seq,
            step,
            request_id: RequestId::for_step(id, step),
        };
        for events in [
            vec![request(1, 1), request(2, 1)],
            vec![request(1, 1), request(2, 2)],
        ] {
            assert!(
                log(id, &events)
                    .unwrap_err()
                    .to_string()
                    .contains("duplicate request")
            );
        }
        let completed = Event::InferenceCompleted {
            seq: 2,
            request_id: RequestId::for_step(id, 1),
            message: Message {
                id: MessageId::from_ulid(Ulid::generate()),
                role: MessageRole::Assistant,
                parts: Vec::new(),
            },
        };
        assert!(log(id, &[request(1, 1), completed.clone(), request(3, 1)]).is_err());
        assert_eq!(
            log(id, &[request(1, 1), completed, request(3, 3)]).unwrap(),
            2
        );
        assert!(log(id, &[request(2, 2)]).is_err());
        assert!(log(id, &[request(1, 1), request(1, 1)]).is_err());
    }

    #[test]
    fn final_check_rejects_wrong_answers_missing_steps_and_non_idle_sessions() {
        let id = SessionId::from_ulid(Ulid::generate());
        let mut session = SessionRecord {
            session_id: id,
            agent_id: swarmy_core::AgentId::from_ulid(Ulid::generate()),
            state: SessionState::Idle,
            head_seq: 2,
            snapshot_ref: None,
        };
        let mut events = vec![
            Event::InferenceRequested {
                seq: 1,
                step: 1,
                request_id: RequestId::for_step(id, 1),
            },
            Event::InferenceCompleted {
                seq: 2,
                request_id: RequestId::for_step(id, 1),
                message: Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: MessageRole::Assistant,
                    parts: vec![Part::Text {
                        text: ANSWER.into(),
                    }],
                },
            },
        ];
        finished(&session, &events, 1).unwrap();
        assert!(finished(&session, &events, 2).is_err());
        session.state = SessionState::Runnable;
        assert!(finished(&session, &events, 1).is_err());
        session.state = SessionState::Idle;
        if let Event::InferenceCompleted { message, .. } = &mut events[1] {
            message.parts = vec![Part::Text {
                text: "wrong".into(),
            }];
        }
        assert!(finished(&session, &events, 1).is_err());
    }

    #[test]
    fn charge_bounds_include_only_gateway_kills() {
        assert!(calls(100, 100, 3).is_ok());
        assert!(calls(103, 100, 3).is_ok());
        assert!(calls(99, 100, 3).is_err());
        assert!(calls(104, 100, 3).is_err());
    }
}
