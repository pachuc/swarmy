//! Turn parked GPU capacity waits and idle GPU machines into a desired count.
//! Only desired is written here; reconcile applies it with creation and drain limits.
use jiff::Timestamp;
use std::time::Duration;
use swarmy_core::{ElasticGroupRecord, GpuRequirement, MachineCause, MachineState};
use swarmy_store::{GpuCapacityWait, MachineFilter, Store};

/// Time a joined machine must have no GPU computer before removal.
pub const IDLE_TIMEOUT: Duration = Duration::from_mins(30);

/// One active machine of the group, as autoscale sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineLoad {
    pub number: u32,
    pub state: MachineState,
    /// Placements of every kind on the node.
    pub placements: u32,
    pub gpu: Vec<GpuRequirement>,
    /// Join time or end of the latest assignment interval.
    pub idle_since: Option<Timestamp>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Keep,
    Raise { desired: u32, cause: MachineCause },
    Lower { desired: u32, cause: MachineCause },
}
fn busy(machines: &[MachineLoad]) -> u32 {
    u32::try_from(machines.iter().filter(|m| !m.gpu.is_empty()).count()).unwrap_or(u32::MAX)
}
/// Busy machines plus machines for unmet waits, before the max cap.
#[must_use]
pub fn machines_needed(sandboxes: u32, machines: &[MachineLoad], waits: &[GpuCapacityWait]) -> u32 {
    let k = sandboxes.max(1);
    let free = machines
        .iter()
        .filter(|m| !m.gpu.is_empty() && !m.gpu.contains(&GpuRequirement::Dedicated))
        .map(|m| k.saturating_sub(m.placements))
        .fold(0u32, u32::saturating_add);
    let dedicated = u32::try_from(
        waits
            .iter()
            .filter(|w| !w.placed && w.requirement == GpuRequirement::Dedicated)
            .count(),
    )
    .unwrap_or(u32::MAX);
    let shared = u32::try_from(
        waits
            .iter()
            .filter(|w| !w.placed && w.requirement == GpuRequirement::Shared)
            .count(),
    )
    .unwrap_or(u32::MAX);
    busy(machines)
        .saturating_add(dedicated)
        .saturating_add(shared.saturating_sub(free).div_ceil(k))
}
/// Choose one change in desired from the current snapshot.
#[must_use]
pub fn decide(
    group: &ElasticGroupRecord,
    machines: &[MachineLoad],
    waits: &[GpuCapacityWait],
    now: Timestamp,
) -> Decision {
    let unmet = waits.iter().filter(|w| !w.placed).count();
    let target =
        machines_needed(group.definition.sandboxes, machines, waits).min(group.definition.max);
    if target > group.desired
        && let Some(first) = waits
            .iter()
            .filter(|w| !w.placed)
            .min_by_key(|w| (w.since, w.session_id))
    {
        return Decision::Raise {
            desired: target,
            cause: MachineCause {
                agent: Some(first.agent_id),
                session: Some(first.session_id),
                reason: format!(
                    "{} (autoscale: {} session(s) waiting for GPU capacity)",
                    first_reason(first),
                    unmet
                ),
            },
        };
    }
    if !waits.is_empty()
        || (!group.settled() && !group.desired_cause.reason.starts_with("autoscale:"))
    {
        return Decision::Keep;
    }
    let mut n = group.desired;
    let floor = busy(machines);
    while n > floor {
        let removable = machines
            .iter()
            .find(|m| m.number == n)
            .is_none_or(|m| match m.state {
                MachineState::Failed => true,
                MachineState::Joined => {
                    m.gpu.is_empty()
                        && m.idle_since.is_some_and(|since| {
                            since.checked_add(IDLE_TIMEOUT).is_ok_and(|at| at <= now)
                        })
                }
                _ => false,
            });
        if !removable {
            break;
        }
        n -= 1;
    }
    if n == group.desired {
        Decision::Keep
    } else {
        Decision::Lower {
            desired: n,
            cause: MachineCause::from_reason(format!(
                "autoscale: lowered from {} to {n}: no session waits for GPU capacity and the removed machines had no GPU computer for 30 minutes or were never created",
                group.desired
            )),
        }
    }
}
/// Read demand and load, then adjust the sole autoscaled group's desired.
/// # Errors
/// Store failures.
pub async fn run(store: &Store) -> anyhow::Result<Option<Decision>> {
    let mut groups = store
        .list_elastic_groups()
        .await?
        .into_iter()
        .filter(|g| g.definition.autoscale);
    let Some(record) = groups.next() else {
        return Ok(None);
    };
    if groups.next().is_some() {
        tracing::warn!("multiple autoscaled groups; using the first");
    }
    let waits = store.gpu_capacity_waits().await?;
    let machines = load(store, &record).await?;
    let decision = decide(&record, &machines, &waits, store.now());
    let unmet = waits.iter().filter(|w| !w.placed).count();
    let placed_waits = waits.len() - unmet;
    let busy = busy(&machines);
    let needed = machines_needed(record.definition.sandboxes, &machines, &waits);
    let max = record.definition.max;
    let capped = needed > max;
    let group = &record.definition.group;
    match &decision {
        Decision::Raise { desired, cause } | Decision::Lower { desired, cause } => {
            store.set_elastic_desired(group, *desired, cause).await?;
            let message = if matches!(decision, Decision::Raise { .. }) {
                "autoscale raised desired"
            } else {
                "autoscale lowered desired"
            };
            tracing::info!(group, from = record.desired, to = desired, waiting = unmet, placed_waits, busy, max, capped, reason = %cause.reason, "{message}");
        }
        Decision::Keep => {
            tracing::debug!(group, from = record.desired, to = record.desired, waiting = unmet, placed_waits, busy, max, capped, reason = %record.desired_cause.reason, "autoscale kept desired");
        }
    }
    Ok(Some(decision))
}
async fn load(store: &Store, group: &ElasticGroupRecord) -> anyhow::Result<Vec<MachineLoad>> {
    let mut loads = Vec::new();
    for record in store
        .list_machines(Some(&group.definition.group), MachineFilter::Active)
        .await?
    {
        let Some(number) = crate::reconcile::number(&group.definition, &record.name) else {
            continue;
        };
        let mut placements = 0u32;
        let mut gpu = Vec::new();
        let node = record.machine_id.joined_node_id();
        let mut after = None;
        loop {
            let page = store
                .list_by_node(node, after, swarmy_store::MAX_SCAN_LIMIT)
                .await?;
            for placement in &page {
                placements = placements.saturating_add(1);
                if placement.gpu.needs_card() {
                    gpu.push(placement.gpu);
                }
            }
            if page.len() < swarmy_store::MAX_SCAN_LIMIT {
                break;
            }
            after = page.last().map(|p| p.agent_id);
        }
        let idle_since = if record.state == MachineState::Joined {
            store
                .machine_assignments(record.machine_id)
                .await?
                .into_iter()
                .filter_map(|a| a.to)
                .chain(record.joined_at)
                .max()
        } else {
            None
        };
        loads.push(MachineLoad {
            number,
            state: record.state,
            placements,
            gpu,
            idle_since,
        });
    }
    Ok(loads)
}
fn first_reason(wait: &GpuCapacityWait) -> String {
    if let Some(request) = &wait.gpu_request {
        return request.reason.clone();
    }
    let mode = match wait.requirement {
        GpuRequirement::Shared => "shared",
        GpuRequirement::Dedicated => "dedicated",
        GpuRequirement::None => unreachable!("GPU waits require a card"),
    };
    format!(
        "agent {} needs a {mode} GPU",
        wait.agent_name
            .clone()
            .unwrap_or_else(|| wait.agent_id.to_string())
    )
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_core::{AgentId, ElasticGroupDefinition, GpuRequest, HourlyPrice, SessionId};
    use ulid::Ulid;

    fn at(minutes: i64) -> Timestamp {
        Timestamp::from_second(minutes * 60).unwrap()
    }
    fn group(desired: u32, max: u32, sandboxes: u32) -> ElasticGroupRecord {
        ElasticGroupRecord {
            definition: ElasticGroupDefinition {
                swarm: "fleet".into(),
                group: "gpu".into(),
                module: "scaleway".into(),
                size: "L4-1-24G".into(),
                location: "test".into(),
                sandboxes,
                local_storage: "dir:/tmp".into(),
                max,
                hourly_price: HourlyPrice::parse("1", "EUR").unwrap(),
                primary_address: std::net::Ipv4Addr::LOCALHOST,
                service_user: "swarmy".into(),
                autoscale: true,
            },
            desired,
            desired_cause: MachineCause::from_reason("manual"),
            generation: 0,
            applied: 0,
            reconciled_generation: 0,
            updated_at: at(0),
        }
    }
    fn wait(requirement: GpuRequirement) -> GpuCapacityWait {
        GpuCapacityWait {
            session_id: SessionId::from_ulid(Ulid::generate()),
            agent_id: AgentId::from_ulid(Ulid::generate()),
            agent_name: Some("w1".into()),
            requirement,
            since: at(0),
            gpu_request: None,
            placed: false,
        }
    }
    fn machine(
        number: u32,
        state: MachineState,
        gpu: Vec<GpuRequirement>,
        placements: u32,
        idle_since: Option<Timestamp>,
    ) -> MachineLoad {
        MachineLoad {
            number,
            state,
            placements,
            gpu,
            idle_since,
        }
    }
    #[test]
    fn one_shared_wait_raises_to_one_with_its_cause() {
        let mut w = wait(GpuRequirement::Shared);
        w.gpu_request = Some(GpuRequest {
            reason: "tasky 01AAA: render".into(),
            requested_at: at(0),
            session_id: w.session_id,
        });
        let Decision::Raise { desired, cause } = decide(&group(0, 1, 1), &[], &[w.clone()], at(31))
        else {
            panic!("expected raise")
        };
        assert_eq!(desired, 1);
        assert_eq!(cause.agent, Some(w.agent_id));
        assert_eq!(cause.session, Some(w.session_id));
        assert_eq!(
            cause.reason,
            "tasky 01AAA: render (autoscale: 1 session(s) waiting for GPU capacity)"
        );
    }
    #[test]
    fn shared_waits_pack_by_sandboxes_after_free_room() {
        let m = machine(
            1,
            MachineState::Joined,
            vec![GpuRequirement::Shared],
            1,
            Some(at(0)),
        );
        let waits = vec![wait(GpuRequirement::Shared); 3];
        assert!(matches!(
            decide(&group(1, 5, 2), &[m], &waits, at(0)),
            Decision::Raise { desired: 2, .. }
        ));
    }
    #[test]
    fn dedicated_waits_need_a_machine_each_and_cap_at_max() {
        let m = machine(
            1,
            MachineState::Joined,
            vec![GpuRequirement::Dedicated],
            1,
            Some(at(0)),
        );
        let waits = [
            wait(GpuRequirement::Shared),
            wait(GpuRequirement::Dedicated),
            wait(GpuRequirement::Dedicated),
        ];
        assert_eq!(machines_needed(4, std::slice::from_ref(&m), &waits), 4);
        assert!(matches!(
            decide(&group(1, 3, 4), &[m], &waits, at(0)),
            Decision::Raise { desired: 3, .. }
        ));
    }
    #[test]
    fn waiting_without_gpu_request_names_the_agent() {
        let Decision::Raise { cause, .. } = decide(
            &group(0, 1, 1),
            &[],
            &[wait(GpuRequirement::Dedicated)],
            at(0),
        ) else {
            panic!("expected raise")
        };
        assert!(cause.reason.starts_with("agent w1 needs a dedicated GPU"));
    }
    #[test]
    fn placed_waits_block_lowering_but_are_not_demand() {
        let m = machine(1, MachineState::Joined, vec![], 0, Some(at(0)));
        let mut w = wait(GpuRequirement::Shared);
        w.placed = true;
        assert_eq!(
            machines_needed(1, std::slice::from_ref(&m), std::slice::from_ref(&w)),
            0
        );
        assert_eq!(decide(&group(1, 1, 1), &[m], &[w], at(31)), Decision::Keep);
    }
    #[test]
    fn idle_top_machines_lower_down_to_the_first_busy_or_recent_one() {
        let one = machine(
            1,
            MachineState::Joined,
            vec![GpuRequirement::Shared],
            1,
            Some(at(0)),
        );
        let old = machine(2, MachineState::Joined, vec![], 0, Some(at(0)));
        assert!(matches!(
            decide(&group(3, 3, 1), &[one.clone(), old], &[], at(31)),
            Decision::Lower { desired: 1, .. }
        ));
        let recent = machine(2, MachineState::Joined, vec![], 0, Some(at(2)));
        assert!(matches!(
            decide(&group(3, 3, 1), &[one, recent], &[], at(31)),
            Decision::Lower { desired: 2, .. }
        ));
    }
    #[test]
    fn an_idle_machine_below_a_busy_one_stays() {
        let idle = machine(1, MachineState::Joined, vec![], 0, Some(at(0)));
        let busy = machine(
            2,
            MachineState::Joined,
            vec![GpuRequirement::Shared],
            1,
            Some(at(0)),
        );
        assert_eq!(
            decide(&group(2, 2, 1), &[idle, busy], &[], at(31)),
            Decision::Keep
        );
    }
    #[test]
    fn joining_machines_are_never_lowered() {
        let joining = machine(1, MachineState::Provisioning, vec![], 0, None);
        assert_eq!(
            decide(&group(1, 1, 1), &[joining], &[], at(31)),
            Decision::Keep
        );
        let failed = machine(1, MachineState::Failed, vec![], 0, None);
        assert!(matches!(
            decide(&group(1, 1, 1), &[failed], &[], at(31)),
            Decision::Lower { desired: 0, .. }
        ));
    }
    #[test]
    fn a_new_machine_is_not_bought_twice() {
        let joining = machine(1, MachineState::Provisioning, vec![], 0, None);
        assert_eq!(
            decide(
                &group(1, 3, 1),
                &[joining],
                &[wait(GpuRequirement::Shared)],
                at(0)
            ),
            Decision::Keep
        );
    }
}
