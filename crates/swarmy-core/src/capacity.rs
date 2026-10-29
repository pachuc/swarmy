//! What a computer needs from a node, and the texts shown while none can take it.
use crate::{GpuRequirement, NodeRecord};
use std::time::Duration;

/// Whether `node` can host a computer with this GPU requirement. Every node
/// satisfies `None`, and callers rely on that. Nodes do not advertise cards yet.
#[must_use]
pub fn node_satisfies(_node: &NodeRecord, requirement: GpuRequirement) -> bool {
    requirement == GpuRequirement::None
}

/// Short reason shown in session views while a session waits for capacity.
#[must_use]
pub fn capacity_wait_reason(requirement: GpuRequirement) -> String {
    match requirement {
        GpuRequirement::None => "waiting for computer capacity: no live node has room for this computer".into(),
        GpuRequirement::Shared => "waiting for GPU capacity: no live node with a shared GPU can host this computer".into(),
        GpuRequirement::Dedicated => "waiting for GPU capacity: no live node with a dedicated GPU can host this computer".into(),
    }
}

/// The conversation notice appended when a session starts waiting for capacity.
#[must_use]
pub fn capacity_wait_notice(requirement: GpuRequirement, max_wait: Duration) -> String {
    let reason = capacity_wait_reason(requirement);
    let secs = max_wait.as_secs();
    format!("Swarmy is {reason}. The tool call will run when a node can take the computer, which can take several minutes while a new machine starts. If no node can take it within {secs} seconds, the call fails without running.")
}

/// The tool error recorded when a capacity wait reaches its maximum.
#[must_use]
pub fn capacity_wait_exceeded(requirement: GpuRequirement, max_wait: Duration) -> String {
    let reason = capacity_wait_reason(requirement);
    let secs = max_wait.as_secs();
    format!("The tool call did not run: swarmy was {reason} for {secs} seconds and gave up. Try again later, or change the agent's GPU requirement.")
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn gpu_wait_texts_say_waiting_for_gpu_capacity() {
        let wait = Duration::from_secs(3600);
        for text in [capacity_wait_reason(GpuRequirement::Shared), capacity_wait_notice(GpuRequirement::Shared, wait), capacity_wait_exceeded(GpuRequirement::Shared, wait)] {
            assert!(text.contains("waiting for GPU capacity"));
            assert!(text.contains("shared"));
        }
        assert!(capacity_wait_notice(GpuRequirement::Shared, wait).contains("3600 seconds"));
        assert!(capacity_wait_exceeded(GpuRequirement::Shared, wait).contains("did not run"));
    }
    #[test]
    fn cpu_wait_texts_say_computer_capacity() {
        let wait = Duration::from_secs(3600);
        for text in [capacity_wait_reason(GpuRequirement::None), capacity_wait_notice(GpuRequirement::None, wait), capacity_wait_exceeded(GpuRequirement::None, wait)] {
            assert!(text.contains("waiting for computer capacity"));
            assert!(!text.contains("GPU"));
        }
    }
}
