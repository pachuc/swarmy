//! One node API sync renews every locally held lease.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use swarmy_core::{
    AgentId, Lease, LeaseOwnerId, PlacedToolClaim, PlacementRecord, SessionId, VolumeId,
};
use swarmy_node_protocol::{
    ClientError, HostedPlacement, LeaseLengths, NodeAdvert, Renewal, SyncRequest, SyncResponse,
    WriterToken,
};
use swarmy_volume::{LeaseStatus, WriterLease};
use tokio::{sync::watch, time::Instant};

#[async_trait::async_trait]
pub(crate) trait SyncTransport: Send + Sync {
    async fn sync(
        &self,
        request: &SyncRequest,
        deadline: Instant,
    ) -> Result<SyncResponse, ClientError>;
}
#[async_trait::async_trait]
impl SyncTransport for swarmy_node_protocol::NodeClient {
    async fn sync(
        &self,
        request: &SyncRequest,
        deadline: Instant,
    ) -> Result<SyncResponse, ClientError> {
        self.sync(request, deadline).await
    }
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Answered {
    pub(crate) sent: Instant,
    pub(crate) server_now: jiff::Timestamp,
}
pub(crate) fn margin(length: Duration) -> Duration {
    length / 10
}
pub(crate) fn local_deadline(
    answered: Answered,
    expires_at: jiff::Timestamp,
    margin: Duration,
) -> Instant {
    let remaining: Duration = expires_at
        .duration_since(answered.server_now)
        .try_into()
        .unwrap_or_default();
    answered.sent + remaining.saturating_sub(margin)
}
pub(crate) fn sync_period(leases: &LeaseLengths, heartbeat: Duration) -> Duration {
    [
        leases.sync_interval,
        heartbeat,
        leases.placement / 3,
        leases.tool_claim / 3,
        leases.writer / 3,
    ]
    .into_iter()
    .min()
    .unwrap_or_default()
    .max(Duration::from_millis(100))
}
#[derive(Debug, Default)]
pub(crate) struct Occupancy {
    pub(crate) holder: Option<SessionId>,
    pub(crate) queued: u64,
}
#[derive(Clone, Debug)]
pub(crate) enum PlacementState {
    Live {
        placement: PlacementRecord,
        deadline: Instant,
        release_requested: bool,
    },
    Lost(String),
}
#[derive(Clone)]
pub(crate) struct PlacementHold(watch::Receiver<PlacementState>);
impl PlacementHold {
    pub(crate) fn current(&self) -> Option<PlacementRecord> {
        match &*self.0.borrow() {
            PlacementState::Live {
                placement,
                deadline,
                ..
            } if *deadline > Instant::now() => Some(placement.clone()),
            _ => None,
        }
    }
    pub(crate) fn deadline(&self) -> Option<Instant> {
        match &*self.0.borrow() {
            PlacementState::Live { deadline, .. } if *deadline > Instant::now() => Some(*deadline),
            _ => None,
        }
    }
    pub(crate) fn release_requested(&self) -> bool {
        matches!(
            &*self.0.borrow(),
            PlacementState::Live {
                release_requested: true,
                ..
            }
        )
    }
    pub(crate) async fn lost(&mut self) -> String {
        loop {
            let state = self.0.borrow_and_update().clone();
            match state {
                PlacementState::Lost(reason) => return reason,
                PlacementState::Live { deadline, .. } => {
                    tokio::select! { changed = self.0.changed() => { if changed.is_err() { return "replaced".into(); } }, () = tokio::time::sleep_until(deadline) => return "deadline passed before renewal".into() }
                }
            }
        }
    }
    pub(crate) async fn released(&mut self) {
        loop {
            let state = self.0.borrow_and_update().clone();
            if matches!(
                state,
                PlacementState::Live {
                    release_requested: true,
                    ..
                }
            ) {
                return;
            }
            if self.0.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) enum ClaimState {
    Live { deadline: Instant },
    Lost(String),
}
pub(crate) struct ClaimHold(watch::Receiver<ClaimState>);
impl ClaimHold {
    pub(crate) async fn lost(&mut self) -> String {
        loop {
            let state = self.0.borrow_and_update().clone();
            match state {
                ClaimState::Lost(reason) => return reason,
                ClaimState::Live { deadline } => {
                    tokio::select! { changed = self.0.changed() => { if changed.is_err() { return "replaced".into(); } }, () = tokio::time::sleep_until(deadline) => return "deadline passed before renewal".into() }
                }
            }
        }
    }
}
struct PlacementEntry {
    record: PlacementRecord,
    deadline: Instant,
    release: bool,
    occupancy: Arc<Mutex<Occupancy>>,
    sender: watch::Sender<PlacementState>,
}
struct ClaimEntry {
    claim: PlacedToolClaim,
    deadline: Instant,
    sender: watch::Sender<ClaimState>,
}
struct WriterEntry {
    lease: Lease,
    deadline: Instant,
    sender: watch::Sender<LeaseStatus>,
}
#[derive(Default)]
struct Tokens {
    placements: BTreeMap<AgentId, PlacementEntry>,
    claims: BTreeMap<LeaseOwnerId, ClaimEntry>,
    writers: BTreeMap<VolumeId, WriterEntry>,
}
struct Inner {
    transport: Arc<dyn SyncTransport>,
    advert: NodeAdvert,
    leases: LeaseLengths,
    period: Duration,
    tokens: Mutex<Tokens>,
}
#[derive(Clone)]
pub(crate) struct Keeper {
    inner: Arc<Inner>,
}
impl Keeper {
    pub(crate) fn new(
        transport: Arc<dyn SyncTransport>,
        advert: NodeAdvert,
        leases: LeaseLengths,
        heartbeat: Duration,
    ) -> Self {
        let period = sync_period(&leases, heartbeat);
        Self {
            inner: Arc::new(Inner {
                transport,
                advert,
                leases,
                period,
                tokens: Mutex::new(Tokens::default()),
            }),
        }
    }
    pub(crate) fn leases(&self) -> &LeaseLengths {
        &self.inner.leases
    }
    pub(crate) fn hold_placement(
        &self,
        placement: PlacementRecord,
        answered: Answered,
        occupancy: Arc<Mutex<Occupancy>>,
    ) -> PlacementHold {
        let deadline = local_deadline(
            answered,
            placement.expires_at,
            margin(self.leases().placement),
        );
        let (sender, receiver) = watch::channel(PlacementState::Live {
            placement: placement.clone(),
            deadline,
            release_requested: false,
        });
        self.inner
            .tokens
            .lock()
            .expect("keeper lock poisoned")
            .placements
            .insert(
                placement.agent_id,
                PlacementEntry {
                    record: placement,
                    deadline,
                    release: false,
                    occupancy,
                    sender,
                },
            );
        PlacementHold(receiver)
    }
    pub(crate) fn hold_writer(
        &self,
        volume: VolumeId,
        lease: Lease,
        answered: Answered,
    ) -> WriterLease {
        let deadline = local_deadline(answered, lease.expires_at, margin(self.leases().writer));
        let (sender, receiver) = WriterLease::channel(lease.clone(), deadline);
        self.inner
            .tokens
            .lock()
            .expect("keeper lock poisoned")
            .writers
            .insert(
                volume,
                WriterEntry {
                    lease,
                    deadline,
                    sender,
                },
            );
        receiver
    }
    pub(crate) fn hold_claim(&self, claim: PlacedToolClaim, deadline: Instant) -> ClaimHold {
        let (sender, receiver) = watch::channel(ClaimState::Live { deadline });
        self.inner
            .tokens
            .lock()
            .expect("keeper lock poisoned")
            .claims
            .insert(
                claim.owner,
                ClaimEntry {
                    claim,
                    deadline,
                    sender,
                },
            );
        ClaimHold(receiver)
    }
    pub(crate) fn writer_lease(&self, volume: VolumeId) -> Option<Lease> {
        self.inner
            .tokens
            .lock()
            .expect("keeper lock poisoned")
            .writers
            .get(&volume)
            .filter(|e| e.deadline > Instant::now())
            .map(|e| e.lease.clone())
    }
    fn request(&self) -> (SyncRequest, Instant) {
        let mut tokens = self.inner.tokens.lock().expect("keeper lock poisoned");
        let now = Instant::now();
        tokens.placements.retain(|_, e| {
            if e.deadline <= now {
                e.sender.send_replace(PlacementState::Lost(
                    "deadline passed before renewal".into(),
                ));
                false
            } else {
                !e.sender.is_closed()
            }
        });
        tokens.claims.retain(|_, e| {
            if e.deadline <= now {
                e.sender
                    .send_replace(ClaimState::Lost("deadline passed before renewal".into()));
                false
            } else {
                !e.sender.is_closed()
            }
        });
        tokens.writers.retain(|_, e| {
            if e.deadline <= now {
                e.sender.send_replace(LeaseStatus::Lost(Arc::new(
                    swarmy_volume::MetadataError::Fenced(swarmy_core::FenceKind::VolumeLease),
                )));
                false
            } else {
                !e.sender.is_closed()
            }
        });
        let deadline = tokens
            .placements
            .values()
            .map(|e| e.deadline)
            .chain(tokens.claims.values().map(|e| e.deadline))
            .chain(tokens.writers.values().map(|e| e.deadline))
            .min()
            .unwrap_or(now + self.inner.period);
        let request = SyncRequest {
            node: self.inner.advert.clone(),
            placements: tokens
                .placements
                .values()
                .map(|e| {
                    let occupancy = e.occupancy.lock().expect("occupancy lock poisoned");
                    HostedPlacement {
                        placement: e.record.clone(),
                        holder_session_id: occupancy.holder,
                        queued_calls: occupancy.queued,
                    }
                })
                .collect(),
            claims: tokens.claims.values().map(|e| e.claim.clone()).collect(),
            writers: tokens
                .writers
                .iter()
                .map(|(volume, e)| WriterToken {
                    volume: *volume,
                    lease: e.lease.clone(),
                })
                .collect(),
            scratch: vec![],
            scratch_evicted: vec![],
            metrics: vec![],
        };
        (request, deadline)
    }
    pub(crate) async fn sync_once(&self) -> Result<(), ClientError> {
        let (request, deadline) = self.request();
        let sent = Instant::now();
        let answer = self.inner.transport.sync(&request, deadline).await?;
        if !answer.heartbeat_recorded {
            tracing::warn!("node sync did not record heartbeat");
        }
        self.apply(&request, answer, sent);
        Ok(())
    }
    fn apply(&self, request: &SyncRequest, answer: SyncResponse, sent: Instant) {
        let answered = Answered {
            sent,
            server_now: answer.server_now,
        };
        let mut tokens = self.inner.tokens.lock().expect("keeper lock poisoned");
        for (sent, renewal) in request.placements.iter().zip(answer.placements) {
            let id = sent.placement.agent_id;
            let Some(e) = tokens.placements.get_mut(&id) else {
                continue;
            };
            if e.record.epoch != sent.placement.epoch {
                continue;
            }
            if e.deadline <= Instant::now() {
                e.sender.send_replace(PlacementState::Lost(
                    "deadline passed before renewal".into(),
                ));
                tokens.placements.remove(&id);
                continue;
            }
            match renewal {
                Renewal::Renewed(next) => {
                    e.record = next.placement;
                    e.release |= next.release_requested;
                    e.deadline = local_deadline(
                        answered,
                        e.record.expires_at,
                        margin(self.leases().placement),
                    );
                    e.sender.send_replace(PlacementState::Live {
                        placement: e.record.clone(),
                        deadline: e.deadline,
                        release_requested: e.release,
                    });
                }
                Renewal::Lost(refusal) => {
                    e.sender
                        .send_replace(PlacementState::Lost(refusal.to_string()));
                    tokens.placements.remove(&id);
                }
                Renewal::Pending(_) => {}
            }
        }
        for (sent, renewal) in request.claims.iter().zip(answer.claims) {
            let Some(e) = tokens.claims.get_mut(&sent.owner) else {
                continue;
            };
            if e.claim.job.request_id != sent.job.request_id {
                continue;
            }
            if e.deadline <= Instant::now() {
                e.sender
                    .send_replace(ClaimState::Lost("deadline passed before renewal".into()));
                tokens.claims.remove(&sent.owner);
                continue;
            }
            match renewal {
                Renewal::Renewed(expiry) => {
                    e.deadline = local_deadline(answered, expiry, margin(self.leases().tool_claim));
                    e.sender.send_replace(ClaimState::Live {
                        deadline: e.deadline,
                    });
                }
                Renewal::Lost(refusal) => {
                    e.sender.send_replace(ClaimState::Lost(refusal.to_string()));
                    tokens.claims.remove(&sent.owner);
                }
                Renewal::Pending(_) => {}
            }
        }
        self.apply_writers(&mut tokens, request, answer.writers, answered);
    }
    fn apply_writers(
        &self,
        tokens: &mut Tokens,
        request: &SyncRequest,
        renewals: Vec<Renewal<Lease>>,
        answered: Answered,
    ) {
        for (sent, renewal) in request.writers.iter().zip(renewals) {
            let Some(e) = tokens.writers.get_mut(&sent.volume) else {
                continue;
            };
            if e.lease.owner != sent.lease.owner || e.lease.seq != sent.lease.seq {
                continue;
            }
            if e.deadline <= Instant::now() {
                e.sender.send_replace(LeaseStatus::Lost(Arc::new(
                    swarmy_volume::MetadataError::Fenced(swarmy_core::FenceKind::VolumeLease),
                )));
                tokens.writers.remove(&sent.volume);
                continue;
            }
            match renewal {
                Renewal::Renewed(lease) => {
                    e.deadline =
                        local_deadline(answered, lease.expires_at, margin(self.leases().writer));
                    e.lease = lease.clone();
                    e.sender.send_replace(LeaseStatus::Held {
                        lease,
                        deadline: e.deadline,
                    });
                }
                Renewal::Lost(refusal) => {
                    e.sender.send_replace(LeaseStatus::Lost(Arc::new(
                        crate::api_metadata::fenced_or_failed(refusal),
                    )));
                    tokens.writers.remove(&sent.volume);
                }
                Renewal::Pending(_) => {}
            }
        }
    }
    pub(crate) async fn run(&self, mut stop: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.inner.period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = interval.tick() => { if let Err(error) = self.sync_once().await { tracing::warn!(error = %swarmy_core::error_chain(&error), "node sync failed; leases keep their local deadlines"); } }, changed = stop.changed() => { if changed.is_err() || *stop.borrow() { break; } } }
        }
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::collections::VecDeque;
    use swarmy_core::{GpuRequirement, NodeCapacity, NodeId, NodeRole, PlacementChangeReason};
    use swarmy_node_protocol::{FenceKind, NodeRefusal, PlacementRenewal};
    struct Script {
        requests: Mutex<Vec<SyncRequest>>,
        answers: Mutex<VecDeque<Result<SyncResponse, ClientError>>>,
    }
    #[async_trait::async_trait]
    impl SyncTransport for Script {
        async fn sync(
            &self,
            request: &SyncRequest,
            _deadline: Instant,
        ) -> Result<SyncResponse, ClientError> {
            self.requests.lock().unwrap().push(request.clone());
            self.answers.lock().unwrap().pop_front().unwrap()
        }
    }
    fn leases() -> LeaseLengths {
        LeaseLengths {
            placement: Duration::from_secs(30),
            tool_claim: Duration::from_secs(30),
            writer: Duration::from_secs(60),
            sync_interval: Duration::from_secs(5),
        }
    }
    fn fixture() -> (Keeper, Arc<Script>) {
        let script = Arc::new(Script {
            requests: Mutex::new(Vec::new()),
            answers: Mutex::new(VecDeque::new()),
        });
        let advert = NodeAdvert {
            roles: vec![NodeRole::Sandbox],
            capacity: NodeCapacity {
                cpu_millis: 1000,
                memory_bytes: 1000,
                disk_bytes: 1000,
                sandboxes: 1,
            },
            gpu: None,
        };
        (
            Keeper::new(script.clone(), advert, leases(), Duration::from_secs(5)),
            script,
        )
    }
    fn placement(now: jiff::Timestamp) -> PlacementRecord {
        PlacementRecord {
            agent_id: AgentId::from_ulid(ulid::Ulid::generate()),
            node_id: NodeId::from_ulid(ulid::Ulid::generate()),
            epoch: 1,
            expires_at: now.checked_add(Duration::from_secs(30)).unwrap(),
            last_change_reason: PlacementChangeReason::Initial,
            last_changed_at: now,
            gpu: GpuRequirement::None,
        }
    }
    fn answer(now: jiff::Timestamp, placement: Vec<Renewal<PlacementRenewal>>) -> SyncResponse {
        SyncResponse {
            server_now: now,
            heartbeat_recorded: true,
            placements: placement,
            claims: vec![],
            writers: vec![],
            scratch: vec![],
        }
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_comes_from_server_time_only() {
        let server_now = "2001-01-01T00:00:00Z".parse().unwrap();
        let sent = Instant::now();
        let answered = Answered { sent, server_now };
        let expires = server_now.checked_add(Duration::from_secs(30)).unwrap();
        assert_eq!(
            local_deadline(answered, expires, Duration::from_secs(3)),
            sent + Duration::from_secs(27)
        );
        assert_eq!(
            local_deadline(
                answered,
                server_now.checked_sub(Duration::from_secs(1)).unwrap(),
                Duration::from_secs(3)
            ),
            sent
        );
    }
    #[test]
    fn sync_period_follows_the_shortest_lease() {
        let mut l = leases();
        assert_eq!(
            sync_period(&l, Duration::from_secs(5)),
            Duration::from_secs(5)
        );
        l.placement = Duration::from_secs(3);
        assert_eq!(
            sync_period(&l, Duration::from_secs(5)),
            Duration::from_secs(1)
        );
        assert_eq!(
            sync_period(&l, Duration::from_millis(100)),
            Duration::from_millis(100)
        );
        l.placement = Duration::from_millis(150);
        assert_eq!(
            sync_period(&l, Duration::from_secs(5)),
            Duration::from_millis(100)
        );
    }
    #[tokio::test(start_paused = true)]
    async fn refusal_is_final_and_dropped_holders_are_pruned() {
        let (keeper, script) = fixture();
        let now = "2001-01-01T00:00:00Z".parse().unwrap();
        let placement = placement(now);
        let mut hold = keeper.hold_placement(
            placement,
            Answered {
                sent: Instant::now(),
                server_now: now,
            },
            Arc::default(),
        );
        script.answers.lock().unwrap().push_back(Ok(answer(
            now,
            vec![Renewal::Lost(NodeRefusal::Fenced(FenceKind::Placement))],
        )));
        keeper.sync_once().await.unwrap();
        assert!(hold.lost().await.contains("fenced"));
        script
            .answers
            .lock()
            .unwrap()
            .push_back(Ok(answer(now, vec![])));
        keeper.sync_once().await.unwrap();
        assert_eq!(script.requests.lock().unwrap()[0].placements.len(), 1);
        assert!(script.requests.lock().unwrap()[1].placements.is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn renewal_reports_release_and_occupancy() {
        let (keeper, script) = fixture();
        let now = "2001-01-01T00:00:00Z".parse().unwrap();
        let placement = placement(now);
        let occupancy = Arc::new(Mutex::new(Occupancy {
            holder: None,
            queued: 2,
        }));
        let mut hold = keeper.hold_placement(
            placement.clone(),
            Answered {
                sent: Instant::now(),
                server_now: now,
            },
            occupancy,
        );
        script.answers.lock().unwrap().push_back(Ok(answer(
            now,
            vec![Renewal::Renewed(PlacementRenewal {
                placement,
                release_requested: true,
            })],
        )));
        keeper.sync_once().await.unwrap();
        assert_eq!(
            script.requests.lock().unwrap()[0].placements[0].queued_calls,
            2
        );
        assert!(hold.release_requested());
        hold.released().await;
    }
}
