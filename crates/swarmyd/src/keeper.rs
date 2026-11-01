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
        for (sent, renewal) in request.writers.iter().zip(answer.writers) {
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
