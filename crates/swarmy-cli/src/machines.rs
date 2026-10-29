//! Read-only elastic machine listings and cost output.
use anyhow::{Context, Result};
use swarmy_api_types::{Machine, MachineList};
use swarmy_client::api_client::call as request;

pub(crate) async fn list(all: bool, json: bool) -> Result<()> {
    let (client, endpoint) = crate::target::connect().await?;
    let list = request(&endpoint, client.machines(all, None)).await?;
    if json {
        println!("{}", serde_json::to_string(&list)?);
    } else if list.machines.is_empty() {
        println!(
            "{}",
            if all {
                "no machines"
            } else {
                "no active machines; pass --all to include destroyed ones"
            }
        );
    } else {
        println!("{}", render_table(&list)?);
    }
    Ok(())
}
fn row(machine: &Machine, as_of: jiff::Timestamp) -> Result<Vec<String>> {
    let requested = machine
        .requested_at
        .parse::<jiff::Timestamp>()
        .with_context(|| format!("invalid requested_at for machine {}", machine.id))?;
    let seconds = as_of.as_second().saturating_sub(requested.as_second());
    let minutes = seconds.max(0) / 60;
    let age = format!(
        "{:#}",
        jiff::SignedDuration::from_secs(minutes.saturating_mul(60))
    );
    let agents = machine
        .current_agents
        .iter()
        .map(|agent| agent.agent_name.as_deref().unwrap_or(&agent.agent_id))
        .collect::<Vec<_>>()
        .join(",");
    Ok(vec![
        machine.id.clone(),
        machine.group.clone(),
        machine.size.clone(),
        machine.location.clone(),
        machine.state.as_str().to_owned(),
        age,
        machine.node_id.clone().unwrap_or_else(|| "-".into()),
        format!("{} {}", machine.currency, machine.cost),
        if agents.is_empty() {
            "-".into()
        } else {
            agents
        },
        machine.cause.reason.clone(),
    ])
}
/// Render aligned columns, preserving the reason as the unpadded last column.
pub(crate) fn render_table(list: &MachineList) -> Result<String> {
    let as_of = list
        .as_of
        .parse::<jiff::Timestamp>()
        .context("invalid machine list as_of")?;
    let mut rows = vec![
        [
            "ID", "GROUP", "SIZE", "LOCATION", "STATE", "AGE", "NODE", "COST", "AGENTS", "REASON",
        ]
        .map(str::to_owned)
        .to_vec(),
    ];
    for machine in &list.machines {
        rows.push(row(machine, as_of)?);
    }
    let widths: Vec<_> = (0..10)
        .map(|i| {
            rows.iter()
                .map(|row| row[i].chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    Ok(rows
        .into_iter()
        .map(|cells| {
            cells
                .iter()
                .enumerate()
                .map(|(i, cell)| {
                    if i == 9 {
                        cell.clone()
                    } else {
                        format!("{cell:width$}", width = widths[i])
                    }
                })
                .collect::<Vec<_>>()
                .join("  ")
                .trim_end()
                .to_owned()
        })
        .collect::<Vec<_>>()
        .join("\n"))
}
/// Render machines that accrued cost in the requested window.
#[must_use]
pub(crate) fn render_cost_section(list: &MachineList) -> Option<String> {
    if !list
        .machines
        .iter()
        .any(|machine| machine.window_cost_micros.unwrap_or(0) > 0)
    {
        return None;
    }
    let mut lines = vec![format!(
        "machines from {} to {}:",
        list.from.as_deref()?,
        list.to.as_deref()?
    )];
    for machine in &list.machines {
        if machine.window_cost_micros.unwrap_or(0) > 0 {
            lines.push(format!(
                "  {} {} {} {} cost={} {}",
                machine.id,
                machine.group,
                machine.size,
                machine.state.as_str(),
                machine.currency,
                machine.window_cost.as_deref().unwrap_or("0.0000")
            ));
        }
    }
    for total in &list.totals {
        if total.cost_micros > 0 {
            lines.push(format!("  total cost={} {}", total.currency, total.cost));
        }
    }
    Some(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use clap::Parser;
    fn list() -> MachineList {
        let first: Machine = serde_json::from_value(serde_json::json!({
            "id":"01JC0000000000000000000001", "group":"gpu", "module":"scaleway", "size":"L4-1-24G", "location":"fr-par-2", "address":null, "node_id":"node-1", "state":"joined", "requested_at":"2026-10-29T10:55:00Z", "created_at":null, "provisioned_at":null, "joined_at":null, "drain_requested_at":null, "destroyed_at":null,
            "cause":{"agent_id":null,"agent_name":null,"session_id":null,"reason":"fleet task 01M63CEJ needs a GPU"}, "hourly_price_micros":750_000, "currency":"EUR", "billing":"per_started_hour", "failure":null, "cost_micros":1_500_000, "cost":"1.5000", "window_cost_micros":750_000, "window_cost":"0.7500", "current_agents":[{"agent_id":"agent-1","agent_name":"worker-3","since":"2026-10-29T10:55:00Z"}]
        })).unwrap();
        let mut second = first.clone();
        second.id = "01JC0000000000000000000002".into();
        second.location = "pl-waw-2".into();
        second.state = swarmy_api_types::MachineState::Requested;
        second.requested_at = "2026-10-29T11:59:30Z".into();
        second.node_id = None;
        second.cost_micros = 0;
        second.cost = "0.0000".into();
        second.window_cost_micros = Some(0);
        second.window_cost = Some("0.0000".into());
        second.current_agents.clear();
        second.cause.reason = "autoscale: 1 session waiting".into();
        MachineList {
            as_of: "2026-10-29T12:00:00Z".into(),
            from: Some("2026-10-29T10:00:00Z".into()),
            to: Some("2026-10-29T12:00:00Z".into()),
            machines: vec![first, second],
            totals: vec![swarmy_api_types::MachineCostTotal {
                currency: "EUR".into(),
                cost_micros: 750_000,
                cost: "0.7500".into(),
            }],
        }
    }
    #[test]
    fn machines_table_renders_aligned_columns() {
        let expected = "ID                          GROUP  SIZE      LOCATION  STATE      AGE    NODE    COST        AGENTS    REASON\n01JC0000000000000000000001  gpu    L4-1-24G  fr-par-2  joined     1h 5m  node-1  EUR 1.5000  worker-3  fleet task 01M63CEJ needs a GPU\n01JC0000000000000000000002  gpu    L4-1-24G  pl-waw-2  requested  0s     -       EUR 0.0000  -         autoscale: 1 session waiting";
        assert_eq!(render_table(&list()).unwrap(), expected);
    }
    #[test]
    fn cost_section_lists_only_machines_with_window_cost() {
        let list = list();
        assert_eq!(
            render_cost_section(&list).unwrap(),
            "machines from 2026-10-29T10:00:00Z to 2026-10-29T12:00:00Z:\n  01JC0000000000000000000001 gpu L4-1-24G joined cost=EUR 0.7500\n  total cost=EUR 0.7500"
        );
        let mut empty = list;
        empty.machines.clear();
        assert_eq!(render_cost_section(&empty), None);
    }
    #[test]
    fn machines_clap_accepts_all() {
        assert!(crate::Cli::try_parse_from(["swarmy", "swarm", "machines", "--all"]).is_ok());
    }
}
