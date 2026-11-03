//! The sandbox node agent. Durable actions go through the primary node API.
fn main() -> anyhow::Result<()> {
    swarmyd::role::run(std::env::args_os().skip(1).collect())
}
