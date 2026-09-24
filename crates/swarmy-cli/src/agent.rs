use anyhow::Result;
use swarmy_core::{AgentId, AgentRecord};
use swarmy_store::Store;

pub async fn resolve(store: &Store, name: &str) -> Result<AgentRecord> {
    // Prefer a literal name, including names that happen to parse as a ULID.
    if let Some(agent) = store.get_agent_by_name(name).await? {
        return Ok(agent);
    }
    if let Ok(id) = name.parse::<ulid::Ulid>()
        && let Some(agent) = store.get_agent(AgentId::from_ulid(id)).await?
    {
        return Ok(agent);
    }
    anyhow::bail!("agent {name} not found")
}
