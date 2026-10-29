//! Durable elastic machine records are the cost source for rented machines.
//! Cost is derived at read time, not accumulated by a background writer.
//!
//! Transitions: Requested -> Creating -> Provisioning (Created), Provisioning
//! -> Provisioning (Provisioned) -> Joined, Provisioning or Joined -> Draining,
//! Requested/Draining/Failed/Orphaned -> Destroyed. Requested through Draining
//! may fail; Creating through Failed (except Requested) may become Orphaned.
//! Destroyed is terminal.

use crate::{AgentId, MachineId, NodeId, SessionId};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MachineState {
    Requested,
    Creating,
    Provisioning,
    Joined,
    Draining,
    Destroyed,
    Failed,
    Orphaned,
}
impl MachineState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Creating => "creating",
            Self::Provisioning => "provisioning",
            Self::Joined => "joined",
            Self::Draining => "draining",
            Self::Destroyed => "destroyed",
            Self::Failed => "failed",
            Self::Orphaned => "orphaned",
        }
    }
}
impl std::fmt::Display for MachineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingGranularity {
    #[default]
    PerStartedHour,
    PerStartedMinute,
}
impl BillingGranularity {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PerStartedHour => "per_started_hour",
            Self::PerStartedMinute => "per_started_minute",
        }
    }
    const fn unit_nanos(self) -> i128 {
        match self {
            Self::PerStartedHour => 3_600_000_000_000,
            Self::PerStartedMinute => 60_000_000_000,
        }
    }
    const fn units_per_hour(self) -> u128 {
        match self {
            Self::PerStartedHour => 1,
            Self::PerStartedMinute => 60,
        }
    }
}
/// Why a machine was requested. The fleet driver puts the tasky id in `reason`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineCause {
    pub agent: Option<AgentId>,
    pub session: Option<SessionId>,
    pub reason: String,
}
/// Input for a new machine record; the store stamps `requested_at`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewMachine {
    pub machine_id: MachineId,
    pub group: String,
    /// OpenTofu module name from the group spec.
    pub module: String,
    pub size: String,
    pub location: String,
    pub cause: MachineCause,
    /// Millionths of `currency`, copied from the group at request time.
    pub hourly_price_micros: u64,
    pub currency: String,
    pub billing: BillingGranularity,
}
impl NewMachine {
    /// The record in state Requested with every later timestamp unset.
    #[must_use]
    pub fn record(&self, requested_at: Timestamp) -> MachineRecord {
        MachineRecord {
            machine_id: self.machine_id,
            group: self.group.clone(),
            module: self.module.clone(),
            size: self.size.clone(),
            location: self.location.clone(),
            address: None,
            node_id: None,
            state: MachineState::Requested,
            requested_at,
            created_at: None,
            provisioned_at: None,
            joined_at: None,
            drain_requested_at: None,
            destroyed_at: None,
            cause: self.cause.clone(),
            hourly_price_micros: self.hourly_price_micros,
            currency: self.currency.clone(),
            billing: self.billing,
            failure: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineRecord {
    pub machine_id: MachineId,
    pub group: String,
    pub module: String,
    pub size: String,
    pub location: String,
    pub address: Option<String>,
    pub node_id: Option<NodeId>,
    pub state: MachineState,
    pub requested_at: Timestamp,
    pub created_at: Option<Timestamp>,
    pub provisioned_at: Option<Timestamp>,
    pub joined_at: Option<Timestamp>,
    pub drain_requested_at: Option<Timestamp>,
    pub destroyed_at: Option<Timestamp>,
    pub cause: MachineCause,
    pub hourly_price_micros: u64,
    pub currency: String,
    pub billing: BillingGranularity,
    pub failure: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MachineTransition {
    Creating,
    Created { address: String },
    Provisioned,
    Joined { node_id: NodeId },
    DrainRequested,
    Destroyed,
    Failed { detail: String },
    Orphaned { detail: String },
}
impl MachineTransition {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Created { .. } => "created",
            Self::Provisioned => "provisioned",
            Self::Joined { .. } => "joined",
            Self::DrainRequested => "drain_requested",
            Self::Destroyed => "destroyed",
            Self::Failed { .. } => "failed",
            Self::Orphaned { .. } => "orphaned",
        }
    }
}
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[error("a {from} machine cannot take the {transition} transition")]
pub struct InvalidMachineTransition {
    pub from: MachineState,
    pub transition: &'static str,
}
/// One agent's use of one machine. `to` is None while it remains placed there.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineAssignment {
    pub machine: MachineId,
    pub agent: AgentId,
    pub from: Timestamp,
    pub to: Option<Timestamp>,
}
fn units_started_before(created: Timestamp, at: Timestamp, unit_nanos: i128) -> u128 {
    let elapsed = at.as_nanosecond().saturating_sub(created.as_nanosecond());
    if elapsed <= 0 {
        return 0;
    }
    u128::try_from(elapsed.saturating_add(unit_nanos - 1) / unit_nanos).unwrap_or(u128::MAX)
}
impl MachineRecord {
    /// Apply a transition; rejection leaves the record unchanged.
    /// # Errors
    /// Returns `InvalidMachineTransition` when the state does not allow it.
    pub fn apply(
        &mut self,
        transition: &MachineTransition,
        at: Timestamp,
    ) -> Result<(), InvalidMachineTransition> {
        use MachineState as S;
        match (self.state, transition) {
            (S::Requested, MachineTransition::Creating) => self.state = S::Creating,
            (S::Creating, MachineTransition::Created { address }) => {
                self.created_at = Some(at);
                self.address = Some(address.clone());
                self.state = S::Provisioning;
            }
            (S::Provisioning, MachineTransition::Provisioned) if self.provisioned_at.is_none() => {
                self.provisioned_at = Some(at)
            }
            (S::Provisioning, MachineTransition::Joined { node_id })
                if self.provisioned_at.is_some() =>
            {
                self.joined_at = Some(at);
                self.node_id = Some(*node_id);
                self.state = S::Joined;
            }
            (S::Provisioning | S::Joined, MachineTransition::DrainRequested) => {
                self.drain_requested_at = Some(at);
                self.state = S::Draining;
            }
            (
                S::Requested | S::Draining | S::Failed | S::Orphaned,
                MachineTransition::Destroyed,
            ) => {
                self.destroyed_at = Some(at);
                self.state = S::Destroyed;
            }
            (
                S::Requested | S::Creating | S::Provisioning | S::Joined | S::Draining,
                MachineTransition::Failed { detail },
            ) => {
                self.failure = Some(detail.clone());
                self.state = S::Failed;
            }
            (
                S::Creating | S::Provisioning | S::Joined | S::Draining | S::Failed,
                MachineTransition::Orphaned { detail },
            ) => {
                self.failure = Some(detail.clone());
                self.state = S::Orphaned;
            }
            _ => {
                return Err(InvalidMachineTransition {
                    from: self.state,
                    transition: transition.name(),
                });
            }
        }
        Ok(())
    }
    /// Whether the machine may still cost money or is still in progress.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state != MachineState::Destroyed
            && (self.state != MachineState::Failed || self.created_at.is_some())
    }
    fn billed_units(&self, now: Timestamp) -> Option<(Timestamp, u128)> {
        let created = self.created_at?;
        let end = self.destroyed_at.unwrap_or(now);
        Some((
            created,
            units_started_before(created, end, self.billing.unit_nanos()).max(1),
        ))
    }
    fn price_of_units(&self, units: u128) -> u64 {
        let per_hour = self.billing.units_per_hour();
        let micros = units
            .saturating_mul(u128::from(self.hourly_price_micros))
            .saturating_add(per_hour / 2)
            / per_hour;
        u64::try_from(micros).unwrap_or(u64::MAX)
    }
    /// Billed cost from creation to destruction, or to `now` while running.
    #[must_use]
    pub fn cost_micros(&self, now: Timestamp) -> u64 {
        self.billed_units(now)
            .map_or(0, |(_, units)| self.price_of_units(units))
    }
    /// Cost of billing units whose starts lie in `[from, to)`.
    #[must_use]
    pub fn cost_micros_between(&self, from: Timestamp, to: Timestamp, now: Timestamp) -> u64 {
        let Some((created, billed)) = self.billed_units(now) else {
            return 0;
        };
        if to <= from {
            return 0;
        }
        let unit = self.billing.unit_nanos();
        let start = units_started_before(created, from, unit).min(billed);
        let end = units_started_before(created, to, unit).min(billed);
        self.price_of_units(end)
            .saturating_sub(self.price_of_units(start))
    }
}
