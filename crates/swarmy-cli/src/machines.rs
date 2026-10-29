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
