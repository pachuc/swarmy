use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{AgentId, LeaseOwnerId, SessionId};

/// Durable session states from the design's step lifecycle.
///
/// Variant order is part of storage version 1. Append new variants; never reorder them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Idle,
    Runnable,
    Leased,
    WaitingInference,
    WaitingTools,
    Sleeping,
    Completed,
}

/// A named session shares the named agent's computer across conversations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    #[default]
    Ephemeral,
    Named {
        agent_id: AgentId,
    },
}

/// Whether a state change is allowed, independently of leases, timers, or I/O.
///
/// Wakeups and completed external work make a session runnable. Only a leased
/// step can dispatch work, end a turn, sleep, or complete a session. The
/// scheduler can also park a runnable session behind an entry breaker or
/// end a marked turn. An expired lease returns to runnable so another worker
/// can retry the step. Sleeping sessions wake to runnable on a timer or
/// message, or go idle when an inference wait is interrupted. Completed is terminal and
/// moving to the same state is not a transition.
///
/// Callers must separately check lease ownership and whether external work or
/// timers have completed; this function only checks the state pair.
#[must_use]
pub const fn can_transition(from: SessionState, to: SessionState) -> bool {
    use SessionState::{
        Completed, Idle, Leased, Runnable, Sleeping, WaitingInference, WaitingTools,
    };

    matches!(
        (from, to),
        (Idle | WaitingInference | WaitingTools | Sleeping, Runnable)
            | (Runnable, Leased | Sleeping | Idle)
            | (Sleeping, Idle)
            | (
                Leased,
                Runnable | WaitingInference | WaitingTools | Idle | Sleeping | Completed
            )
    )
}

/// The object containing a snapshot and the inclusive event sequence it covers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub object_key: String,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    #[serde(default)]
    pub interrupt_requested: bool,
    #[serde(default)]
    pub kind: SessionKind,
    #[serde(default)]
    pub computer_deleted: bool,
    #[serde(default)]
    pub plan: Vec<crate::PlanStep>,
    pub session_id: SessionId,
    pub agent_id: AgentId,
    pub state: SessionState,
    /// Zero for an empty log; appended events start at one.
    pub head_seq: u64,
    /// Absent until the first snapshot is written.
    pub snapshot_ref: Option<SnapshotRef>,
    #[serde(default)]
    pub inference: crate::InferenceSelection,
    /// Session route override. `None` inherits the agent, then the swarm default.
    #[serde(default)]
    pub route: Option<String>,
    /// Position in the resolved route for the current attempt chain. The
    /// worker advances it past retryable failures and resets it when the
    /// chain is exhausted or the turn succeeds.
    #[serde(default)]
    pub route_step: u32,
}

impl SessionRecord {
    /// Whether resolving this session's route needs a store read. Named
    /// sessions always resolve, since the agent's assignment, provider, and
    /// model may have changed. Ephemeral sessions resolve only when a route
    /// is assigned to the session or the swarm; otherwise the worker uses
    /// the implicit single-step chain and the gateway pool picks the entry,
    /// so an unrouted ephemeral inference costs no route transaction.
    #[must_use]
    pub fn needs_route_snapshot(&self, default_route: Option<&str>) -> bool {
        !matches!(self.kind, SessionKind::Ephemeral)
            || self.route.is_some()
            || default_route.is_some()
    }
}

/// Lease times are supplied by callers; this crate never reads the clock.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub owner: LeaseOwnerId,
    pub expires_at: Timestamp,
    /// The step sequence used to derive its deterministic request id.
    pub seq: u64,
}

/// Opening context preserved when a main conversation is summarized.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ConversationSummary {
    pub goals: String,
    pub state_of_work: String,
    pub open_questions: String,
    pub facts_to_keep: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::tests::assert_round_trip;
    use ulid::Ulid;

    #[test]
    fn all_state_pairs_follow_the_lifecycle() {
        use SessionState::*;

        let states = [
            Idle,
            Runnable,
            Leased,
            WaitingInference,
            WaitingTools,
            Sleeping,
            Completed,
        ];
        // Rows and columns use the order above. This checks all 49 pairs,
        // including every self-transition and every transition out of Completed.
        let allowed = [
            [false, true, false, false, false, false, false],
            [true, false, true, false, false, true, false],
            [true, true, false, true, true, true, true],
            [false, true, false, false, false, false, false],
            [false, true, false, false, false, false, false],
            [true, true, false, false, false, false, false],
            [false, false, false, false, false, false, false],
        ];
        for (row, from) in states.iter().enumerate() {
            assert_round_trip(from);
            for (column, to) in states.iter().enumerate() {
                assert_eq!(
                    can_transition(*from, *to),
                    allowed[row][column],
                    "{from:?} -> {to:?}"
                );
            }
        }
    }

    #[test]
    fn records_round_trip() {
        let mut session = SessionRecord {
            interrupt_requested: false,
            session_id: SessionId::from_ulid(Ulid::from_parts(1, 2)),
            agent_id: AgentId::from_ulid(Ulid::from_parts(1, 3)),
            state: SessionState::Idle,
            head_seq: 0,
            snapshot_ref: None,
            inference: crate::InferenceSelection::default(),
            kind: SessionKind::Ephemeral,
            computer_deleted: false,
            plan: Vec::new(),
            route: None,
            route_step: 0,
        };
        assert_round_trip(&session);
        let mut old = serde_json::to_value(&session).unwrap();
        old.as_object_mut().unwrap().remove("inference");
        old.as_object_mut().unwrap().remove("interrupt_requested");
        assert_eq!(
            serde_json::from_value::<SessionRecord>(old).unwrap(),
            session
        );
        session.kind = SessionKind::Named {
            agent_id: session.agent_id,
        };
        session.computer_deleted = true;
        assert_round_trip(&session);
        session.head_seq = u64::MAX;
        session.snapshot_ref = Some(SnapshotRef {
            object_key: "snapshots/session/42".into(),
            seq: 42,
        });
        assert_round_trip(&session);
        for expires_at in [
            "2026-09-14T12:34:56.123456789Z",
            "1969-12-31T23:59:59.999999999Z",
        ] {
            assert_round_trip(&Lease {
                owner: LeaseOwnerId::from_ulid(Ulid::from_parts(1, 4)),
                expires_at: expires_at.parse().unwrap(),
                seq: u64::MAX,
            });
        }
    }

    #[test]
    fn unrouted_ephemeral_sessions_need_no_route_snapshot() {
        let agent = AgentId::from_ulid(Ulid::from_parts(2, 3));
        let mut session = SessionRecord {
            interrupt_requested: false,
            session_id: SessionId::from_ulid(Ulid::from_parts(1, 2)),
            agent_id: agent,
            state: SessionState::Idle,
            head_seq: 0,
            snapshot_ref: None,
            inference: crate::InferenceSelection::default(),
            kind: SessionKind::Ephemeral,
            computer_deleted: false,
            plan: Vec::new(),
            route: None,
            route_step: 0,
        };
        assert!(!session.needs_route_snapshot(None));
        // Any route assignment forces a resolution read.
        session.route = Some("fallback".into());
        assert!(session.needs_route_snapshot(None));
        session.route = None;
        assert!(session.needs_route_snapshot(Some("fallback")));
        // Named sessions always resolve against the agent record.
        session.kind = SessionKind::Named { agent_id: agent };
        assert!(session.needs_route_snapshot(None));
    }
}
