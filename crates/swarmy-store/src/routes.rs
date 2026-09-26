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
};

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

    /// Entry pools for several providers in one transaction, in creation
    /// order with the gateway pool's ready-first rule. The scheduler fills
    /// its per-tick cache with one call instead of one scan per session.
    /// # Errors
    /// Returns database or decoding errors.
    pub async fn route_pools(
        &self,
        providers: &[String],
        now: Timestamp,
    ) -> Result<HashMap<String, Vec<String>>> {
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

    /// Expand a named route against ready-first entry pools without touching
    /// breaker states, so the worker's transaction and the scheduler's tick
    /// cache resolve the same chain. Steps naming a missing or unready entry
    /// are skipped with a recorded reason; a provider with no stored entries
    /// keeps its explicit steps for environment, ambient, and scripted keys.
    /// A named route that selects nothing usable expands to nothing and the
    /// caller falls back to the implicit route.
    #[must_use]
    pub fn expand_chain(
        record: Option<&RouteRecord>,
        pools: &HashMap<String, Vec<String>>,
        fallback_provider: &str,
    ) -> ExpandedChain {
        let mut expanded = Vec::new();
        let mut skipped = Vec::new();
        if let Some(record) = record {
            for step in &record.steps {
                if step.entry == swarmy_core::ANY_ENTRY {
                    match pools.get(&step.provider) {
                        Some(pool) if !pool.is_empty() => {
                            // The wildcard keeps the gateway pool order: ready
                            // entries in creation order, or every entry when
                            // none is ready.
                            for label in pool {
                                expanded.push(ExpandedRouteStep {
                                    provider: step.provider.clone(),
                                    label: Some(label.clone()),
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
                let usable = pools
                    .get(&step.provider)
                    .is_none_or(|pool| pool.is_empty() || pool.contains(&step.entry));
                if usable {
                    expanded.push(ExpandedRouteStep {
                        provider: step.provider.clone(),
                        label: Some(step.entry.clone()),
                        model: step.model.clone(),
                    });
                } else {
                    skipped.push(skipped_step_reason(&step.provider, &step.entry));
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
                    for label in pool {
                        expanded.push(ExpandedRouteStep {
                            provider: fallback_provider.to_owned(),
                            label: Some(label.clone()),
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

    /// Move the session to another route step for the next attempt, recording
    /// the failure as handled with the skipped steps' reasons in the session's
    /// wait history. Marking the failure handled keeps a worker that restarts
    /// or loses its lease after the advance from seeing the same failure
    /// again and advancing a second step. The caller holds the step lease;
    /// the lease check serializes concurrent moves, so the value may restart
    /// at zero when the route shrank mid-turn.
    /// # Errors
    /// Rejects stale leases or storage failures.
    pub async fn set_session_route_step(
        &self,
        id: SessionId,
        lease: &Lease,
        step: u32,
        seq: u64,
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
