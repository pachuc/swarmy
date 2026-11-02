//! What a computer needs from a node, and the texts shown while none can take it.
use crate::{GpuRequirement, NodeRecord, SandboxRequirements};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Whether `node` can host a computer with this GPU requirement. Every node
/// satisfies `None`; GPU modes need a node advertising at least one card.
#[must_use]
pub fn node_satisfies(node: &NodeRecord, requirement: GpuRequirement) -> bool {
    !requirement.needs_card() || node.gpu_count() > 0
}

const MIB: u64 = 1024 * 1024;

/// What one empty machine offers a computer after the system reserve.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MachineOffer {
    pub memory_mib: u64,
    pub cards: bool,
}
impl MachineOffer {
    #[must_use]
    pub fn of_node(node: &NodeRecord) -> Self {
        Self {
            memory_mib: node.capacity.memory_bytes / MIB,
            cards: node.gpu_count() > 0,
        }
    }
}
/// Whether an empty machine can hold this computer.
#[must_use]
pub fn machine_holds(offer: MachineOffer, requirements: SandboxRequirements) -> bool {
    (offer.cards || !requirements.gpu.needs_card()) && requirements.memory_mib <= offer.memory_mib
}
/// Memory needed beyond what the largest suitable machine offers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shortfall {
    pub needed_mib: u64,
    pub largest_mib: u64,
}
impl std::fmt::Display for Shortfall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "this computer needs {} MiB of memory, but the largest machine that can take it offers {} MiB; lower the agent's memory with swarmy agent set NAME --memory MIB, or give the group a machine size with more memory",
            self.needed_mib, self.largest_mib
        )
    }
}
/// Report memory only if some machine meets the card requirement but none fits.
#[must_use]
pub fn shortfall(offers: &[MachineOffer], requirements: SandboxRequirements) -> Option<Shortfall> {
    let largest_mib = offers
        .iter()
        .filter(|offer| offer.cards || !requirements.gpu.needs_card())
        .map(|offer| offer.memory_mib)
        .max()?;
    (largest_mib < requirements.memory_mib).then_some(Shortfall {
        needed_mib: requirements.memory_mib,
        largest_mib,
    })
}

/// Short reason shown in session views while a session waits for capacity.
#[must_use]
pub fn capacity_wait_reason(requirement: GpuRequirement, shortfall: Option<Shortfall>) -> String {
    let reason: String = match requirement {
        GpuRequirement::None => {
            "waiting for computer capacity: no live node has room for this computer".into()
        }
        GpuRequirement::Shared => {
            "waiting for GPU capacity: no live node with a shared GPU can host this computer".into()
        }
        GpuRequirement::Dedicated => {
            "waiting for GPU capacity: no live node with a dedicated GPU can host this computer"
                .into()
        }
    };
    shortfall.map_or_else(|| reason.clone(), |s| format!("{reason}; {s}"))
}

/// The conversation notice appended when a session starts waiting for capacity.
#[must_use]
pub fn capacity_wait_notice(
    requirement: GpuRequirement,
    shortfall: Option<Shortfall>,
    max_wait: Duration,
) -> String {
    let reason = capacity_wait_reason(requirement, shortfall);
    let secs = max_wait.as_secs();
    format!(
        "Swarmy is {reason}. The tool call will run when a node can take the computer, which can take several minutes while a new machine starts. If no node can take it within {secs} seconds, the call fails without running."
    )
}

/// The tool error recorded when a capacity wait reaches its maximum.
#[must_use]
pub fn capacity_wait_exceeded(
    requirement: GpuRequirement,
    shortfall: Option<Shortfall>,
    max_wait: Duration,
) -> String {
    let reason = capacity_wait_reason(requirement, shortfall);
    let secs = max_wait.as_secs();
    let advice = if shortfall.is_some() {
        "Lower the agent's memory or use a machine size with more memory."
    } else {
        match requirement {
            GpuRequirement::None => "Try again later.",
            GpuRequirement::Shared | GpuRequirement::Dedicated => {
                "Try again later, or change the agent's GPU requirement."
            }
        }
    };
    format!(
        "The tool call did not run: swarmy was {reason} for {secs} seconds and gave up. {advice}"
    )
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn gpu_wait_texts_say_waiting_for_gpu_capacity() {
        let wait = Duration::from_secs(3600);
        for text in [
            capacity_wait_reason(GpuRequirement::Shared, None),
            capacity_wait_notice(GpuRequirement::Shared, None, wait),
            capacity_wait_exceeded(GpuRequirement::Shared, None, wait),
        ] {
            assert!(text.contains("waiting for GPU capacity"));
            assert!(text.contains("shared"));
        }
        assert!(capacity_wait_notice(GpuRequirement::Shared, None, wait).contains("3600 seconds"));
        assert!(capacity_wait_exceeded(GpuRequirement::Shared, None, wait).contains("did not run"));
    }
    #[test]
    fn cpu_wait_texts_say_computer_capacity() {
        let wait = Duration::from_secs(3600);
        for text in [
            capacity_wait_reason(GpuRequirement::None, None),
            capacity_wait_notice(GpuRequirement::None, None, wait),
            capacity_wait_exceeded(GpuRequirement::None, None, wait),
        ] {
            assert!(text.contains("waiting for computer capacity"));
            assert!(!text.contains("GPU"));
        }
    }
}
