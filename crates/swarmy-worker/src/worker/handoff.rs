//! Move a named session to a new model through a neutral summary.

use super::{
    Event, HeldLease, MessageId, Result, SessionRecord, Snapshot, Ulid, Worker,
    inference::{Compaction, PreparedInference},
    summarize::{SUMMARY_RESERVE_TOKENS, summary_prompt, summary_request_with_prompt},
};
use swarmy_llm::handoff::{ModelHandoff, Reasoning};

pub(super) struct HandoffTarget<'a> {
    pub(super) provider: &'a str,
    pub(super) model: &'a str,
    pub(super) route: Option<&'a str>,
    pub(super) route_step: u32,
}
const HANDOFF_DEFAULT_CONTEXT_TOKENS: u64 = 128_000;
const HANDOFF_PROMPT_TOKENS: u64 = 2_048;
const HANDOFF_CHARS_PER_TOKEN: u64 = 3;
const HANDOFF_REASON_CHARS: usize = 200;
const ROUTE_MOVED: &str = "the route moved to another step";

pub(super) fn detect_model_handoff(
    events: &[Event],
    target: &HandoffTarget<'_>,
) -> Option<ModelHandoff> {
    // A rejected checkpoint has a completion but never became the session's
    // history. Its request follows the handoff-attempt notice atomically.
    let handoff_requests: Vec<_> = events
        .windows(2)
        .filter_map(|pair| match pair {
            [
                Event::MessageAppended { message, .. },
                Event::InferenceRequested { request_id, .. },
            ] if message.parts.iter().any(|part| {
                matches!(
                    part,
                    swarmy_core::Part::Notice {
                        kind: swarmy_core::NoticeKind::ModelHandoff,
                        ..
                    }
                )
            }) =>
            {
                Some(*request_id)
            }
            _ => None,
        })
        .collect();
    let pos = events.iter().rposition(|event| {
        matches!(event,
        Event::InferenceCompleted { request_id, .. } if !handoff_requests.contains(request_id))
    })?;
    let Event::InferenceCompleted { completion, .. } = &events[pos] else {
        return None;
    };
    if completion.provider.is_empty()
        || completion.model.is_empty()
        || (completion.provider == target.provider && completion.model == target.model)
        || (completion.route_step == Some(target.route_step)
            && completion.route.as_deref() == target.route)
    {
        return None;
    }
    let reason = events[pos + 1..]
        .iter()
        .rev()
        .find_map(|event| match event {
            Event::InferenceFailed { error, .. } => Some(
                error
                    .lines()
                    .next()
                    .unwrap_or(ROUTE_MOVED)
                    .chars()
                    .take(HANDOFF_REASON_CHARS)
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_else(|| ROUTE_MOVED.to_owned());
    Some(ModelHandoff {
        from_provider: completion.provider.clone(),
        from_model: completion.model.clone(),
        to_provider: target.provider.to_owned(),
        to_model: target.model.to_owned(),
        reason,
    })
}

pub(super) fn handoff_budget_chars(context_tokens: Option<u64>) -> usize {
    let tokens = context_tokens
        .filter(|t| *t > 0)
        .unwrap_or(HANDOFF_DEFAULT_CONTEXT_TOKENS);
    usize::try_from(
        tokens
            .saturating_sub(SUMMARY_RESERVE_TOKENS)
            .saturating_sub(HANDOFF_PROMPT_TOKENS)
            .saturating_mul(HANDOFF_CHARS_PER_TOKEN),
    )
    .unwrap_or(usize::MAX)
}

fn handoff_notice_text(handoff: &ModelHandoff, successor: bool) -> String {
    let mut text = format!(
        "Model handoff from {}/{} to {}/{}: {}",
        handoff.from_provider,
        handoff.from_model,
        handoff.to_provider,
        handoff.to_model,
        handoff.reason
    );
    if successor {
        text.push_str("\nThe earlier conversation on the previous model was summarized as plain text above. Continue the task on the new model from that summary.");
    }
    text
}

pub(super) fn handoff_notice(handoff: &ModelHandoff, successor: bool) -> swarmy_core::Message {
    swarmy_core::Message {
        id: MessageId::from_ulid(Ulid::generate()),
        role: swarmy_core::MessageRole::System,
        parts: vec![swarmy_core::Part::Notice {
            kind: swarmy_core::NoticeKind::ModelHandoff,
            text: handoff_notice_text(handoff, successor),
        }],
    }
}

impl Worker {
    pub(super) async fn issue_handoff_summary(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        history: (&Snapshot, &[Event]),
        handoff: ModelHandoff,
        settings: swarmy_llm::GenerationSettings,
    ) -> Result<bool> {
        let messages = self.compaction_history(history.0, history.1, false).await?;
        if messages.is_empty() {
            return Ok(false);
        }
        let window = self
            .config
            .model_context_window_tokens
            .or_else(|| {
                self.config
                    .catalog
                    .model(&handoff.to_provider, &handoff.to_model)
                    .map(|m| m.limit.context)
            })
            .filter(|t| *t > 0);
        let budget = handoff_budget_chars(window);
        if budget == 0 {
            tracing::warn!(session_id = %session.session_id, context_tokens = ?window,
                "handoff context window leaves no room for conversation history");
        }
        let prompt = summary_prompt(&messages, Reasoning::Drop, Some(budget));
        let mut request =
            summary_request_with_prompt(&self.config, &handoff.to_provider, prompt, settings);
        let mut preceding = Vec::new();
        let attempt = self
            .prepare_request(session, &mut request, &mut preceding, true)
            .await?;
        let mut handoff = handoff;
        handoff.to_provider.clone_from(&attempt.provider);
        handoff.to_model.clone_from(&request.settings.model);
        tracing::warn!(session_id = %session.session_id, from = %format!("{}/{}", handoff.from_provider, handoff.from_model), to = %format!("{}/{}", handoff.to_provider, handoff.to_model), reason = %handoff.reason, "route changed model; handing the conversation over through a plain-text summary");
        // Keep this notice immediately before the request even if preparation
        // emitted an effort-clamp notice, so rejected checkpoints stay identifiable.
        preceding.push(Event::MessageAppended {
            seq: 0,
            message: handoff_notice(&handoff, false),
        });
        self.submit_prepared(
            session,
            lease,
            PreparedInference {
                request,
                preceding,
                attempt,
            },
            false,
            Some(Compaction {
                cut: messages.len(),
                recovery: false,
                handoff: Some(handoff),
            }),
        )
        .await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    fn completion(provider: &str, model: &str) -> Event {
        let session = swarmy_core::SessionId::from_ulid(Ulid::generate());
        Event::InferenceCompleted {
            seq: 1,
            request_id: swarmy_core::RequestId::for_step(session, 1),
            completion: swarmy_core::InferenceCompletion {
                message: swarmy_core::Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: swarmy_core::MessageRole::Assistant,
                    parts: Vec::new(),
                },
                provider: provider.into(),
                model: model.into(),
                effort_used: None,
                usage: swarmy_core::TokenUsage::default(),
                cost_micros: 0,
                effort_requested: None,
                effort_clamped: false,
                entry: None,
                route: Some("ab".into()),
                route_step: Some(0),
            },
        }
    }
    fn target<'a>(provider: &'a str, model: &'a str, route_step: u32) -> HandoffTarget<'a> {
        HandoffTarget {
            provider,
            model,
            route: Some("ab"),
            route_step,
        }
    }
    #[test]
    fn different_model_after_a_route_move_hands_off() {
        let session = swarmy_core::SessionId::from_ulid(Ulid::generate());
        let events = [
            completion("fake-a", "fake-model"),
            Event::InferenceFailed {
                seq: 2,
                request_id: swarmy_core::RequestId::for_step(session, 2),
                error: "429 quota reached\nmore detail".into(),
                retryable: true,
                retry_at: None,
                failure_kind: swarmy_core::FailureKind::Unknown,
            },
        ];
        assert_eq!(
            detect_model_handoff(&events, &target("fake-a", "fake-other", 1)),
            Some(ModelHandoff {
                from_provider: "fake-a".into(),
                from_model: "fake-model".into(),
                to_provider: "fake-a".into(),
                to_model: "fake-other".into(),
                reason: "429 quota reached".into(),
            })
        );
    }
    #[test]
    fn same_model_on_another_key_does_not_hand_off() {
        assert_eq!(
            detect_model_handoff(
                &[completion("fake-a", "fake-model")],
                &target("fake-a", "fake-model", 1)
            ),
            None
        );
    }
    #[test]
    fn provider_change_with_the_same_model_hands_off() {
        assert!(
            detect_model_handoff(
                &[completion("fake-a", "fake-model")],
                &target("fake-b", "fake-model", 1)
            )
            .is_some()
        );
    }
    #[test]
    fn model_change_without_a_route_move_does_not_hand_off() {
        assert_eq!(
            detect_model_handoff(
                &[completion("fake-a", "fake-model")],
                &target("fake-a", "fake-other", 0)
            ),
            None
        );
    }
    #[test]
    fn missing_or_unattributed_completion_does_not_hand_off() {
        assert_eq!(
            detect_model_handoff(&[], &target("fake-a", "fake-other", 1)),
            None
        );
        assert_eq!(
            detect_model_handoff(
                &[completion("", "fake-model")],
                &target("fake-a", "fake-other", 1)
            ),
            None
        );
    }
    #[test]
    fn breaker_skip_without_failure_names_the_route_move() {
        let handoff = detect_model_handoff(
            &[completion("fake-a", "fake-model")],
            &target("fake-a", "fake-other", 1),
        )
        .unwrap();
        assert_eq!(handoff.reason, ROUTE_MOVED);
    }
    #[test]
    fn reason_is_capped() {
        let session = swarmy_core::SessionId::from_ulid(Ulid::generate());
        let events = [
            completion("fake-a", "fake-model"),
            Event::InferenceFailed {
                seq: 2,
                request_id: swarmy_core::RequestId::for_step(session, 2),
                error: "x".repeat(500),
                retryable: true,
                retry_at: None,
                failure_kind: swarmy_core::FailureKind::Unknown,
            },
        ];
        assert_eq!(
            detect_model_handoff(&events, &target("fake-a", "fake-other", 1))
                .unwrap()
                .reason
                .len(),
            200
        );
    }
    #[test]
    fn rejected_handoff_summary_does_not_mask_the_model_change() {
        let session = swarmy_core::SessionId::from_ulid(Ulid::generate());
        let request_id = swarmy_core::RequestId::for_step(session, 2);
        let Event::InferenceCompleted {
            completion: mut summary,
            ..
        } = completion("fake-a", "fake-other")
        else {
            unreachable!()
        };
        summary.route_step = Some(1);
        let handoff = ModelHandoff {
            from_provider: "fake-a".into(),
            from_model: "fake-model".into(),
            to_provider: "fake-a".into(),
            to_model: "fake-other".into(),
            reason: "quota".into(),
        };
        let events = [
            completion("fake-a", "fake-model"),
            Event::MessageAppended {
                seq: 2,
                message: handoff_notice(&handoff, false),
            },
            Event::InferenceRequested {
                seq: 3,
                request_id,
                step: 3,
            },
            Event::InferenceCompleted {
                seq: 4,
                request_id,
                completion: summary,
            },
        ];
        assert_eq!(
            detect_model_handoff(&events, &target("fake-a", "fake-other", 1)),
            Some(ModelHandoff {
                reason: ROUTE_MOVED.into(),
                ..handoff
            })
        );
    }
    #[test]
    fn budget_leaves_room_for_the_summary() {
        assert_eq!(
            handoff_budget_chars(Some(30_000)),
            (30_000 - 16_384 - 2_048) * 3
        );
        assert_eq!(handoff_budget_chars(None), handoff_budget_chars(Some(0)));
        assert_eq!(handoff_budget_chars(Some(1_000)), 0);
    }
    #[test]
    fn notice_names_both_models() {
        let handoff = ModelHandoff {
            from_provider: "fake-a".into(),
            from_model: "old".into(),
            to_provider: "fake-b".into(),
            to_model: "new".into(),
            reason: "quota".into(),
        };
        let first = "Model handoff from fake-a/old to fake-b/new: quota";
        for (successor, expected_lines) in [(false, 1), (true, 2)] {
            let notice = handoff_notice(&handoff, successor);
            assert_eq!(notice.role, swarmy_core::MessageRole::System);
            assert!(matches!(&notice.parts[0], swarmy_core::Part::Notice {
                kind: swarmy_core::NoticeKind::ModelHandoff, text
            } if text.starts_with(first) && text.lines().count() == expected_lines));
        }
    }
}
