//! Inference routes: named failover chains over auth entries.
//!
//! A route is an ordered list of steps, each naming a provider and an entry
//! label (or `*` for every entry of that provider in creation order) with an
//! optional model override. Sessions resolve session override, then agent,
//! then swarm default, then an implicit route of the selected provider's
//! entries, so configurations without routes behave exactly as before: the
//! wildcard expansion keeps the gateway pool's ready-first order.
use jiff::Timestamp;
use std::collections::HashMap;
use swarmy_core::{
    AgentId, AgentRecord, CredentialScope, ExpandedRouteStep, Lease, RouteRecord, SessionId,
    SessionKind,
};

use crate::{
    CredentialKey, InferenceFailureWait, InferenceWait, Result, Store, StoreError,
    credentials::{decode_entry, entry_ready},
    inference_wait::Breaker,
    read, scan, write,
};

/// One stored entry label with its readiness hint, in creation order.
#[derive(Clone, Debug)]
pub struct PoolEntry {
    pub label: String,
    pub ready: bool,
}

/// What one atomic failover step decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailoverAction {
    /// The session advanced to another step; the worker continues the turn.
    AdvanceTo(u32),
    /// The session parked until a retry; the worker releases its lease.
    Park,
    /// The failure was already handled by an earlier attempt; the worker
    /// continues without advancing or parking, so a resumed worker never
    /// moves a second step for the same failure.
    AlreadyHandled,
}

/// The outcome of one atomic failover step with the resolved route name and
/// the reasons for named steps skipped as missing or unready, so the worker
/// can warn when an assigned route fell back to the implicit chain.
#[derive(Clone, Debug)]
pub struct FailoverOutcome {
    pub action: FailoverAction,
    pub route: Option<String>,
    pub skipped: Vec<String>,
}

/// Reasons recorded when a failover moves past steps: the failure itself,
/// the named steps skipped as missing or unready, and the open breakers
/// between the failed step and the target. A wrap to a recovered earlier
/// step skips every open step after the failure; a forward move skips the
/// open steps between the two.
#[must_use]
fn failover_reasons(snapshot: &RouteSnapshot, from: u32, target: u32, error: &str) -> Vec<String> {
    let mut reasons = vec![error.to_owned()];
    reasons.extend(snapshot.skipped.iter().cloned());
    let end = if target > from {
        usize::try_from(target).unwrap_or(usize::MAX)
    } else {
        snapshot.steps.len()
    };
    for skipped in from.saturating_add(1)..u32::try_from(end).unwrap_or(u32::MAX) {
        if let Some(reason) = snapshot
            .steps
            .get(usize::try_from(skipped).unwrap_or(usize::MAX))
            .and_then(|step| step.reason.clone())
        {
            reasons.push(reason);
        }
    }
    reasons
}

/// One expanded step with its live breaker state, resolved in a single
/// transaction so scheduler and worker agree on the same chain.
#[derive(Clone, Debug)]
pub struct RouteStepStatus {
    pub provider: String,
    pub label: Option<String>,
    pub model: Option<String>,
    pub open_until: Option<Timestamp>,
    pub reason: Option<String>,
}

/// The resolved chain for one session: the named route, if any, the
/// expanded steps with their breaker records, and the reasons for named
/// steps skipped as missing or unready.
#[derive(Clone, Debug)]
pub struct RouteSnapshot {
    pub name: Option<String>,
    pub steps: Vec<RouteStepStatus>,
    pub skipped: Vec<String>,
}

/// One expansion without breaker states, shared by the worker's single
/// transaction and the scheduler's per-tick caches so both agree on the
/// same chain.
#[derive(Clone, Debug)]
pub struct ExpandedChain {
    pub name: Option<String>,
    pub steps: Vec<ExpandedRouteStep>,
    pub skipped: Vec<String>,
}

fn skipped_step_reason(provider: &str, label: &str) -> String {
    format!("{provider}/{label} names an entry with no ready credential; trying the next step")
}

/// Labels a route step may use from one provider's pool, mirroring the
/// gateway pool: ready entries in creation order, or every entry when none
/// is ready. The fallback keeps implicit chains working exactly as before
/// when every stored entry is unready.
fn usable_labels(pool: &[PoolEntry]) -> Vec<String> {
    let ready: Vec<String> = pool
        .iter()
        .filter(|entry| entry.ready)
        .map(|entry| entry.label.clone())
        .collect();
    if ready.is_empty() {
        pool.iter().map(|entry| entry.label.clone()).collect()
    } else {
        ready
    }
}

impl RouteSnapshot {
    /// First usable step at or after the session's attempt position, wrapping
    /// to the first usable step from the start when every later step is open.
    /// A recovered earlier step serves the next attempt instead of parking
    /// behind a later open breaker; a position past the last step (a route
    /// that shrank mid-turn) still resolves through the wrap. `None` means
    /// every step is open and the worker parks until the earliest retry.
    #[must_use]
    pub fn pick(&self, from: u32) -> Option<usize> {
        let start = usize::try_from(from)
            .unwrap_or(usize::MAX)
            .min(self.steps.len());
        self.steps
            .iter()
            .enumerate()
            .skip(start)
            .find(|(_, step)| step.open_until.is_none())
            .map(|(index, _)| index)
            .or_else(|| {
                self.steps
                    .iter()
                    .enumerate()
                    .take(start)
                    .find(|(_, step)| step.open_until.is_none())
                    .map(|(index, _)| index)
            })
    }

    /// First usable step at or after the session's attempt position, wrapping
    /// to a recovered earlier step when every later step is open, and falling
    /// back to the earliest retry when every step is open. The worker
    /// submits there so the gateway's probe discipline records the outcome
    /// for the next attempt instead of spinning locally.
    #[must_use]
    pub fn pick_or_earliest(&self, from: u32) -> usize {
        if let Some(index) = self.pick(from) {
            return index;
        }
        self.steps
            .iter()
            .enumerate()
            .filter_map(|(index, step)| step.open_until.map(|until| (until, index)))
            .min_by_key(|(until, _)| *until)
            .map_or(0, |(_, index)| index)
    }

    /// Earliest retry across every step for an exhausted chain.
    #[must_use]
    pub fn earliest(&self) -> Option<(Timestamp, String)> {
        let mut earliest: Option<(Timestamp, String)> = None;
        for step in &self.steps {
            let Some(until) = step.open_until else {
                continue;
            };
            let reason = step
                .reason
                .clone()
                .unwrap_or_else(|| "provider temporarily unavailable".into());
            if earliest.as_ref().is_none_or(|(at, _)| until < *at) {
                earliest = Some((until, reason));
            }
        }
        earliest
    }
}

impl Store {
    pub(crate) fn route_key(&self, name: &str) -> Vec<u8> {
        self.root.pack(&("route", name))
    }

    /// Store a named failover chain, replacing any previous steps.
    /// # Errors
    /// Returns invalid routes or storage failures.
    pub async fn put_route(&self, name: &str, steps: &[swarmy_core::RouteStep]) -> Result<()> {
        swarmy_core::route::validate(name, steps).map_err(StoreError::InvalidRoute)?;
        let record = RouteRecord {
            name: name.to_owned(),
            steps: steps.to_vec(),
            updated_at: Timestamp::now(),
        };
        self.transaction(|trx| {
            let record = &record;
            async move { write(&trx, &self.route_key(name), record) }
        })
        .await
    }

    /// Read one named route.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn get_route(&self, name: &str) -> Result<Option<RouteRecord>> {
        self.transaction(|trx| async move { read(&trx, &self.route_key(name)).await })
            .await
    }

    /// List every named route in name order.
    /// # Errors
    /// Returns storage or decoding failures.
    pub async fn list_routes(&self) -> Result<Vec<RouteRecord>> {
        let space = self.root.subspace(&("route",));
        let (mut begin, end) = space.range();
        let mut routes = Vec::new();
        loop {
            let rows = self
                .transaction(|trx| {
                    let range = (begin.clone(), end.clone());
                    async move { scan(&trx, range, crate::MAX_SCAN_LIMIT).await }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            for (key, value) in rows {
                let (name,): (String,) = space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                let mut record: RouteRecord = swarmy_core::decode(&value)?;
                if record.name != name {
                    return Err(StoreError::Corrupt);
                }
                record.name = name;
                routes.push(record);
                begin = key;
                begin.push(0);
            }
        }
        Ok(routes)
    }

    /// Delete a named route. Sessions assigned to it fall back to the next
    /// resolution level on their following turn.
    /// # Errors
    /// Returns storage failures.
    pub async fn delete_route(&self, name: &str) -> Result<bool> {
        self.transaction(|trx| async move {
            let key = self.route_key(name);
            let existed = trx.get(&key, false).await?.is_some();
            trx.clear(&key);
            Ok(existed)
        })
        .await
    }

    /// Entry labels for one provider in creation order, without decrypting.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn route_entry_labels(&self, provider: &str) -> Result<Vec<String>> {
        self.transaction(|trx| async move { self.route_entry_labels_in(&trx, provider).await })
            .await
    }

    async fn route_entry_labels_in(
        &self,
        trx: &foundationdb::Transaction,
        provider: &str,
    ) -> Result<Vec<String>> {
        let space = self.root.subspace(&(
            "credential_entry",
            CredentialScope::Cluster.to_string(),
            provider,
        ));
        let (mut begin, end) = space.range();
        let mut entries = Vec::new();
        loop {
            let rows = scan(trx, (begin.clone(), end.clone()), crate::MAX_SCAN_LIMIT).await?;
            let complete = rows.len() < crate::MAX_SCAN_LIMIT;
            for (key, value) in rows {
                let (label,): (String,) = space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                let entry = decode_entry(&value)?;
                entries.push((entry.created_at, label));
                begin = key;
                begin.push(0);
            }
            if complete {
                break;
            }
        }
        entries.sort();
        Ok(entries.into_iter().map(|(_, label)| label).collect())
    }

    /// Entry labels with readiness hints, without decrypting. Callers apply
    /// the gateway pool rule themselves: ready entries in creation order,
    /// or every entry when none is ready.
    async fn pool_labels_in(
        &self,
        trx: &foundationdb::Transaction,
        provider: &str,
        now: Timestamp,
    ) -> Result<Vec<PoolEntry>> {
        let space = self.root.subspace(&(
            "credential_entry",
            CredentialScope::Cluster.to_string(),
            provider,
        ));
        let (mut begin, end) = space.range();
        let mut entries: Vec<(Timestamp, PoolEntry)> = Vec::new();
        loop {
            let rows = scan(trx, (begin.clone(), end.clone()), crate::MAX_SCAN_LIMIT).await?;
            let complete = rows.len() < crate::MAX_SCAN_LIMIT;
            for (key, value) in rows {
                let (label,): (String,) = space.unpack(&key).map_err(|_| StoreError::Corrupt)?;
                let entry = decode_entry(&value)?;
                entries.push((
                    entry.created_at,
                    PoolEntry {
                        label,
                        ready: entry_ready(entry.needs_login, entry.expires_at, now),
                    },
                ));
                begin = key;
                begin.push(0);
            }
            if complete {
                break;
            }
        }
        // Creation order is the failover order; labels break ties for
        // entries written in the same transaction.
        entries.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.label.cmp(&right.1.label)));
        Ok(entries.into_iter().map(|(_, entry)| entry).collect())
    }

    /// Resolve one session's failover chain in a single transaction: session
    /// override, then agent, then swarm default, then the implicit route of
    /// the selected provider's entries in creation order. A named route that
    /// was deleted or never existed falls back to the implicit route so a
    /// typo cannot wedge an agent's turns.
    /// # Errors
    /// Returns database or decoding errors.
    #[allow(clippy::too_many_arguments)]
    pub async fn route_snapshot(
        &self,
        agent: AgentId,
        session_route: Option<&str>,
        session_provider: Option<&str>,
        default_route: Option<&str>,
        default_provider: &str,
        now: Timestamp,
    ) -> Result<RouteSnapshot> {
        self.agent_and_route_snapshot(
            agent,
            session_route,
            session_provider,
            default_route,
            default_provider,
            now,
        )
        .await
        .map(|(_, snapshot)| snapshot)
    }

    /// Read the agent and resolve its session's failover chain in one
    /// transaction, so an inference costs no more store transactions than
    /// before routes: the worker resolves the agent once for both its
    /// inference settings and its route.
    /// # Errors
    /// Returns database or decoding errors.
    #[allow(clippy::too_many_arguments)]
    pub async fn agent_and_route_snapshot(
        &self,
        agent: AgentId,
        session_route: Option<&str>,
        session_provider: Option<&str>,
        default_route: Option<&str>,
        default_provider: &str,
        now: Timestamp,
    ) -> Result<(Option<AgentRecord>, RouteSnapshot)> {
        self.transaction(|trx| async move {
            let agent = self.read_agent(&trx, agent).await?;
            let (agent_route, agent_provider) = agent.as_ref().map_or((None, None), |record| {
                (record.route.clone(), record.provider.clone())
            });
            let snapshot = self
                .route_snapshot_in(
                    &trx,
                    session_route,
                    agent_route.as_deref(),
                    session_provider,
                    agent_provider.as_deref(),
                    default_route,
                    default_provider,
                    now,
                )
                .await?;
            Ok((agent, snapshot))
        })
        .await
    }

    /// Entry pools for several providers in one transaction, with readiness
    /// hints in creation order. The scheduler fills its per-tick cache with
    /// one call instead of one scan per session.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn route_pools(
        &self,
        providers: &[String],
        now: Timestamp,
    ) -> Result<HashMap<String, Vec<PoolEntry>>> {
        self.transaction(|trx| async move {
            let mut pools = HashMap::with_capacity(providers.len());
            for provider in providers {
                pools.insert(
                    provider.clone(),
                    self.pool_labels_in(&trx, provider, now).await?,
                );
            }
            Ok(pools)
        })
        .await
    }

    /// Breaker states for several route steps in one transaction. Each entry
    /// is the open retry time, if the breaker is open, with its reason. The
    /// scheduler fills its per-tick cache with one call per distinct step.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn breaker_states(
        &self,
        steps: &[(String, Option<String>)],
        now: Timestamp,
    ) -> Result<Vec<(Option<Timestamp>, Option<String>)>> {
        self.transaction(|trx| async move {
            let mut states = Vec::with_capacity(steps.len());
            for (provider, label) in steps {
                let key = CredentialKey::for_label(provider, label.clone());
                let breaker: Option<Breaker> = read(&trx, &self.breaker_key(&key)).await?;
                states.push(crate::inference_wait::open_state(breaker.as_ref(), now));
            }
            Ok(states)
        })
        .await
    }

    /// Expand a named route against entry pools without touching breaker
    /// states, so the worker's transaction and the scheduler's tick cache
    /// resolve the same chain. A named step whose label is missing, expired,
    /// or otherwise not ready is skipped with a recorded reason, so the
    /// turn fails over to the next step instead of failing on a dead entry;
    /// a provider with no stored entries keeps its explicit steps for
    /// environment, ambient, and scripted keys. Wildcards and the implicit
    /// fallback keep the gateway pool order: ready entries in creation
    /// order, or every entry when none is ready. A named route that selects
    /// nothing usable expands to nothing and the caller falls back to the
    /// implicit route.
    #[must_use]
    pub fn expand_chain(
        record: Option<&RouteRecord>,
        pools: &HashMap<String, Vec<PoolEntry>>,
        fallback_provider: &str,
    ) -> ExpandedChain {
        let mut expanded = Vec::new();
        let mut skipped = Vec::new();
        if let Some(record) = record {
            for step in &record.steps {
                if step.entry == swarmy_core::ANY_ENTRY {
                    match pools.get(&step.provider) {
                        Some(pool) if !pool.is_empty() => {
                            for label in usable_labels(pool) {
                                expanded.push(ExpandedRouteStep {
                                    provider: step.provider.clone(),
                                    label: Some(label),
                                    model: step.model.clone(),
                                });
                            }
                        }
                        _ => {
                            expanded.push(ExpandedRouteStep {
                                provider: step.provider.clone(),
                                label: None,
                                model: step.model.clone(),
                            });
                        }
                    }
                    continue;
                }
                // A stored but unready entry skips exactly like a missing
                // label; only a provider with no stored entries at all keeps
                // the step for keys the store cannot see.
                match pools.get(&step.provider) {
                    Some(pool)
                        if !pool.is_empty()
                            && !pool
                                .iter()
                                .any(|entry| entry.ready && entry.label == step.entry) =>
                    {
                        skipped.push(skipped_step_reason(&step.provider, &step.entry));
                    }
                    _ => {
                        expanded.push(ExpandedRouteStep {
                            provider: step.provider.clone(),
                            label: Some(step.entry.clone()),
                            model: step.model.clone(),
                        });
                    }
                }
            }
        }
        // A named route that is missing or selects nothing usable falls back
        // to the implicit route below, so a typo cannot wedge an agent's turns.
        let resolved = if expanded.is_empty() {
            None
        } else {
            record.map(|record| record.name.clone())
        };
        if expanded.is_empty() {
            match pools.get(fallback_provider) {
                Some(pool) if !pool.is_empty() => {
                    for label in usable_labels(pool) {
                        expanded.push(ExpandedRouteStep {
                            provider: fallback_provider.to_owned(),
                            label: Some(label),
                            model: None,
                        });
                    }
                }
                _ => {
                    expanded.push(ExpandedRouteStep {
                        provider: fallback_provider.to_owned(),
                        label: None,
                        model: None,
                    });
                }
            }
        }
        ExpandedChain {
            name: resolved,
            steps: expanded,
            skipped,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn route_snapshot_in(
        &self,
        trx: &foundationdb::Transaction,
        session_route: Option<&str>,
        agent_route: Option<&str>,
        session_provider: Option<&str>,
        agent_provider: Option<&str>,
        default_route: Option<&str>,
        default_provider: &str,
        now: Timestamp,
    ) -> Result<RouteSnapshot> {
        let name = session_route
            .or(agent_route)
            .or(default_route)
            .map(str::to_owned);
        let record = match name.as_deref() {
            Some(name) => read::<RouteRecord>(trx, &self.route_key(name)).await?,
            None => None,
        };
        let provider = session_provider
            .or(agent_provider)
            .unwrap_or(default_provider);
        // One pool scan per distinct provider in the same transaction; entry
        // scans dominate snapshot cost, so named routes share them here.
        let mut providers: Vec<&str> = record
            .as_ref()
            .map(|record| {
                record
                    .steps
                    .iter()
                    .map(|step| step.provider.as_str())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        providers.push(provider);
        providers.sort_unstable();
        providers.dedup();
        let mut pools = HashMap::with_capacity(providers.len());
        for provider in providers {
            pools.insert(
                provider.to_owned(),
                self.pool_labels_in(trx, provider, now).await?,
            );
        }
        let chain = Self::expand_chain(record.as_ref(), &pools, provider);
        let mut steps = Vec::with_capacity(chain.steps.len());
        for step in chain.steps {
            let key = CredentialKey::for_label(&step.provider, step.label.clone());
            let breaker: Option<Breaker> = read(trx, &self.breaker_key(&key)).await?;
            let (open_until, reason) = crate::inference_wait::open_state(breaker.as_ref(), now);
            steps.push(RouteStepStatus {
                provider: step.provider,
                label: step.label,
                model: step.model,
                reason,
                open_until,
            });
        }
        Ok(RouteSnapshot {
            name: chain.name,
            steps,
            skipped: chain.skipped,
        })
    }

    /// Assign a session route override, resetting its attempt chain. A named
    /// route must exist; `None` returns the session to agent and swarm defaults.
    /// # Errors
    /// Returns missing routes, missing sessions, or storage failures.
    pub async fn set_session_route(&self, id: SessionId, route: Option<&str>) -> Result<()> {
        if let Some(name) = route
            && self.get_route(name).await?.is_none()
        {
            return Err(StoreError::RouteMissing);
        }
        self.transaction(|trx| async move {
            self.session(&trx, id).await?;
            write(&trx, &self.session_route_key(id), &route.map(str::to_owned))?;
            write(&trx, &self.session_route_step_key(id), &0_u32)?;
            Ok(())
        })
        .await
    }

    /// Persist one route step with the failure marked handled and the reasons
    /// merged into the session's wait history for the atomic failover.
    async fn write_route_step_in(
        &self,
        trx: &foundationdb::Transaction,
        id: SessionId,
        step: u32,
        seq: u64,
        reasons: &[String],
        now: Timestamp,
    ) -> Result<()> {
        write(trx, &self.session_route_step_key(id), &step)?;
        if !reasons.is_empty() {
            let wait_key = self.wait_key(id);
            let mut wait = read::<InferenceWait>(trx, &wait_key)
                .await?
                .unwrap_or(InferenceWait {
                    since: now,
                    wake_at: now,
                    last_failure_seq: 0,
                    reasons: Vec::new(),
                    attempts: 0,
                });
            if wait.last_failure_seq != seq {
                wait.attempts = wait.attempts.saturating_add(1);
            }
            wait.last_failure_seq = seq;
            for reason in reasons {
                let summary: String = reason.chars().take(256).collect();
                if !wait.reasons.contains(&summary) && wait.reasons.len() < 32 {
                    wait.reasons.push(summary);
                }
            }
            write(trx, &wait_key, &wait)?;
        }
        Ok(())
    }

    /// Failures from `fail_unserved` carry this prefix. The provider may
    /// appear on the next gateway advertisement, so the session waits out
    /// the gateway interval on its current step instead of consuming a
    /// route step.
    #[must_use]
    pub fn is_unserved_error(error: &str) -> bool {
        error.starts_with("no gateway serves provider ")
    }

    /// Resolve one retryable failure against the session's route and either
    /// advance to the next usable step or park the exhausted chain, in a
    /// single transaction: the snapshot, the step move, and the wait write
    /// commit together, so the failure path costs one transaction like the
    /// pre-routes park. A failure already recorded as handled returns
    /// without reading pools or writing, so a worker that restarts or loses
    /// its lease after the advance never moves a second step for the same
    /// failure or parks the session with the successor's request in flight.
    /// # Errors
    /// Rejects stale leases or returns storage failures.
    #[allow(clippy::too_many_arguments)]
    pub async fn failover_route_step(
        &self,
        id: SessionId,
        lease: &Lease,
        seq: u64,
        error: &str,
        retry_at: Timestamp,
        route_step: u32,
        session_route: Option<&str>,
        session_provider: Option<&str>,
        default_route: Option<&str>,
        default_provider: &str,
        now: Timestamp,
        max_wait: std::time::Duration,
    ) -> Result<FailoverOutcome> {
        self.transaction(|trx| async move {
            self.check_worker_lease(&trx, id, lease, now).await?;
            let stored = self.session(&trx, id).await?;
            if let Some(wait) = read::<InferenceWait>(&trx, &self.wait_key(id)).await?
                && wait.last_failure_seq == seq
            {
                return Ok(FailoverOutcome {
                    action: FailoverAction::AlreadyHandled,
                    route: None,
                    skipped: Vec::new(),
                });
            }
            // Ephemeral sessions carry no agent record, so only named
            // sessions read one here; the snapshot covers both either way.
            let (agent_route, agent_provider) = if matches!(
                self.session_kind(&trx, id).await?,
                SessionKind::Named { .. }
            ) {
                self.read_agent(&trx, stored.agent_id)
                    .await?
                    .map_or((None, None), |record| (record.route, record.provider))
            } else {
                (None, None)
            };
            let snapshot = self
                .route_snapshot_in(
                    &trx,
                    session_route,
                    agent_route.as_deref(),
                    session_provider,
                    agent_provider.as_deref(),
                    default_route,
                    default_provider,
                    now,
                )
                .await?;
            let route = snapshot.name.clone();
            if Self::is_unserved_error(error) {
                self.park_leased_in(
                    &trx,
                    id,
                    stored,
                    &InferenceFailureWait {
                        seq,
                        reason: error,
                        wake_at: retry_at,
                    },
                    now,
                    max_wait,
                )
                .await?;
                return Ok(FailoverOutcome {
                    action: FailoverAction::Park,
                    route,
                    skipped: snapshot.skipped.clone(),
                });
            }
            if let Some(target) = snapshot.pick(route_step.saturating_add(1)) {
                let target = u32::try_from(target).unwrap_or(u32::MAX);
                if target != route_step {
                    let reasons = failover_reasons(&snapshot, route_step, target, error);
                    self.write_route_step_in(&trx, id, target, seq, &reasons, now)
                        .await?;
                    return Ok(FailoverOutcome {
                        action: FailoverAction::AdvanceTo(target),
                        route,
                        skipped: snapshot.skipped.clone(),
                    });
                }
                // The failed step still reads as closed: the breaker write
                // has not propagated to this snapshot, so parking until the
                // failure's retry time is safer than retrying the same step
                // in a tight loop.
            }
            let earliest = Self::earliest_retry(&snapshot.steps, retry_at);
            self.park_leased_in(
                &trx,
                id,
                stored,
                &InferenceFailureWait {
                    seq,
                    reason: error,
                    wake_at: earliest,
                },
                now,
                max_wait,
            )
            .await?;
            write(&trx, &self.session_route_step_key(id), &0_u32)?;
            Ok(FailoverOutcome {
                action: FailoverAction::Park,
                route,
                skipped: snapshot.skipped.clone(),
            })
        })
        .await
    }

    /// Earliest retry across one snapshot's steps for an exhausted chain,
    /// bounded by the failure's own retry time. The snapshot may predate the
    /// failure's breaker write and name a later retry than the failure
    /// itself carries, so the minimum of the two wakes the session as soon
    /// as any step could serve it.
    #[must_use]
    pub fn earliest_retry(steps: &[RouteStepStatus], fallback: Timestamp) -> Timestamp {
        steps
            .iter()
            .filter_map(|step| step.open_until)
            .chain(std::iter::once(fallback))
            .min()
            .unwrap_or(fallback)
    }
}
