//! Inference routes: named failover chains over auth entries.
//!
//! A route is an ordered list of steps, each naming a provider and an entry
//! label (or `*` for every entry of that provider in creation order) with an
//! optional model override. Sessions resolve session override, then agent,
//! then swarm default, then an implicit route of the selected provider's
//! entries, so configurations without routes behave exactly as before: the
//! wildcard expansion keeps the gateway pool's ready-first order.
use jiff::Timestamp;
use swarmy_core::{AgentId, CredentialScope, ExpandedRouteStep, Lease, RouteRecord, SessionId};

use crate::{
    CredentialKey, InferenceFailureWait, InferenceWait, Result, Store, StoreError,
    credentials::{decode_entry, entry_ready},
    inference_wait::Breaker,
    read, scan, write,
};

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

/// The resolved chain for one session: the named route, if any, and the
/// expanded steps with their breaker records.
#[derive(Clone, Debug)]
pub struct RouteSnapshot {
    pub name: Option<String>,
    pub steps: Vec<RouteStepStatus>,
}

impl RouteSnapshot {
    /// First usable step at or after the session's attempt position, so a
    /// retryable failure moves strictly forward along the chain. A position
    /// past the last step (a route that shrank mid-turn) has no pick; the
    /// worker falls back to the earliest retry and the scheduler parks.
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
    }

    /// First usable step at or after the session's attempt position, falling
    /// back to the earliest retry when every later step is open. The worker
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

    /// Readiness hints without decrypting, mirroring the gateway pool: ready
    /// entries when any is ready, otherwise every entry.
    async fn pool_labels_in(
        &self,
        trx: &foundationdb::Transaction,
        provider: &str,
        now: Timestamp,
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
                entries.push((
                    entry.created_at,
                    label,
                    entry_ready(entry.needs_login, entry.expires_at, now),
                ));
                begin = key;
                begin.push(0);
            }
            if complete {
                break;
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        let any_ready = entries.iter().any(|(_, _, ready)| *ready);
        Ok(entries
            .into_iter()
            .filter(|(_, _, ready)| *ready || !any_ready)
            .map(|(_, label, _)| label)
            .collect())
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
        self.transaction(|trx| async move {
            let agent = self.read_agent(&trx, agent).await?;
            let (agent_route, agent_provider) = agent.as_ref().map_or((None, None), |record| {
                (record.route.clone(), record.provider.clone())
            });
            self.route_snapshot_in(
                &trx,
                session_route,
                agent_route.as_deref(),
                session_provider,
                agent_provider.as_deref(),
                default_route,
                default_provider,
                now,
            )
            .await
        })
        .await
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
        let mut expanded = Vec::new();
        if let Some(name) = name.as_deref()
            && let Some(record) = read::<RouteRecord>(trx, &self.route_key(name)).await?
        {
            for step in &record.steps {
                self.expand_step(trx, step, now, &mut expanded).await?;
            }
        }
        // A named route that is missing or selects nothing usable falls back
        // to the implicit route below, so a typo cannot wedge an agent's turns.
        let resolved = if expanded.is_empty() {
            None
        } else {
            name.clone()
        };
        if expanded.is_empty() {
            let provider = session_provider
                .or(agent_provider)
                .unwrap_or(default_provider);
            for label in self.pool_labels_in(trx, provider, now).await? {
                expanded.push(ExpandedRouteStep {
                    provider: provider.to_owned(),
                    label: Some(label),
                    model: None,
                });
            }
            if expanded.is_empty() {
                expanded.push(ExpandedRouteStep {
                    provider: provider.to_owned(),
                    label: None,
                    model: None,
                });
            }
        }
        let mut steps = Vec::with_capacity(expanded.len());
        for step in expanded {
            let key = CredentialKey::for_label(&step.provider, step.label.clone());
            let breaker: Option<Breaker> = read(trx, &self.breaker_key(&key)).await?;
            let open_until = breaker.as_ref().and_then(|record| {
                if record.open_until > now {
                    Some(record.open_until)
                } else if record.probe_until.is_some_and(|until| until > now) {
                    now.checked_add(std::time::Duration::from_secs(1)).ok()
                } else {
                    None
                }
            });
            steps.push(RouteStepStatus {
                provider: step.provider,
                label: step.label,
                model: step.model,
                reason: breaker
                    .filter(|_| open_until.is_some())
                    .map(|record| record.reason),
                open_until,
            });
        }
        Ok(RouteSnapshot {
            name: resolved,
            steps,
        })
    }

    async fn expand_step(
        &self,
        trx: &foundationdb::Transaction,
        step: &swarmy_core::RouteStep,
        now: Timestamp,
        expanded: &mut Vec<ExpandedRouteStep>,
    ) -> Result<()> {
        if step.entry == swarmy_core::ANY_ENTRY {
            // The wildcard keeps the gateway pool order: ready entries in
            // creation order, or every entry when none is ready.
            for label in self.pool_labels_in(trx, &step.provider, now).await? {
                expanded.push(ExpandedRouteStep {
                    provider: step.provider.clone(),
                    label: Some(label),
                    model: step.model.clone(),
                });
            }
            if expanded.is_empty()
                || expanded
                    .last()
                    .is_none_or(|last| last.provider != step.provider)
            {
                expanded.push(ExpandedRouteStep {
                    provider: step.provider.clone(),
                    label: None,
                    model: step.model.clone(),
                });
            }
            return Ok(());
        }
        expanded.push(ExpandedRouteStep {
            provider: step.provider.clone(),
            label: Some(step.entry.clone()),
            model: step.model.clone(),
        });
        Ok(())
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

    /// Move the session to another route step for the next attempt, recording
    /// the skipped steps' reasons with the session's wait history. The caller
    /// holds the step lease; the lease check serializes concurrent moves, so
    /// the value may restart at zero when the route shrank mid-turn.
    /// # Errors
    /// Rejects stale leases or storage failures.
    pub async fn set_session_route_step(
        &self,
        id: SessionId,
        lease: &Lease,
        step: u32,
        reasons: &[String],
        now: Timestamp,
    ) -> Result<()> {
        self.transaction(|trx| async move {
            self.check_worker_lease(&trx, id, lease, now).await?;
            let key = self.session_route_step_key(id);
            let current: u32 = read(&trx, &key).await?.unwrap_or(0);
            if step == current && reasons.is_empty() {
                return Ok(());
            }
            write(&trx, &key, &step)?;
            if !reasons.is_empty() {
                let wait_key = self.wait_key(id);
                let mut wait =
                    read::<InferenceWait>(&trx, &wait_key)
                        .await?
                        .unwrap_or(InferenceWait {
                            since: now,
                            wake_at: now,
                            last_failure_seq: 0,
                            reasons: Vec::new(),
                            attempts: 0,
                        });
                for reason in reasons {
                    let summary: String = reason.chars().take(256).collect();
                    if !wait.reasons.contains(&summary) && wait.reasons.len() < 32 {
                        wait.reasons.push(summary);
                    }
                }
                write(&trx, &wait_key, &wait)?;
            }
            Ok(())
        })
        .await
    }

    /// Park an exhausted route until the earliest retry among its steps and
    /// restart the attempt chain, so the next wake probes from the first step.
    /// # Errors
    /// Rejects stale leases or returns storage failures.
    pub async fn park_exhausted_route(
        &self,
        id: SessionId,
        lease: &Lease,
        failure: &InferenceFailureWait<'_>,
        earliest: Timestamp,
        now: Timestamp,
        max_wait: std::time::Duration,
    ) -> Result<bool> {
        let parked = self
            .park_inference(
                id,
                lease,
                &InferenceFailureWait {
                    seq: failure.seq,
                    reason: failure.reason,
                    wake_at: earliest,
                },
                now,
                max_wait,
            )
            .await?;
        if parked {
            self.transaction(|trx| async move {
                write(&trx, &self.session_route_step_key(id), &0_u32)?;
                Ok(())
            })
            .await?;
        }
        Ok(parked)
    }

    /// Earliest retry across one snapshot's steps for an exhausted chain.
    /// Steps without an open breaker need no wait; callers only park when no
    /// step is usable, so an empty result falls back to the failure's time.
    #[must_use]
    pub fn earliest_retry(steps: &[RouteStepStatus], fallback: Timestamp) -> Timestamp {
        steps
            .iter()
            .filter_map(|step| step.open_until)
            .min()
            .unwrap_or(fallback)
    }
}
