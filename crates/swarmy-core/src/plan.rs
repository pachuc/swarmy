use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    pub step: String,
    pub status: PlanStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdatePlanArguments {
    pub plan: Vec<PlanStep>,
}

impl UpdatePlanArguments {
    /// # Errors
    /// Rejects malformed steps, unknown statuses, and multiple active steps.
    pub fn parse(value: serde_json::Value) -> Result<Self, String> {
        let arguments: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        arguments.validate()?;
        Ok(arguments)
    }

    /// # Errors
    /// Rejects empty step descriptions and more than one step in progress.
    pub fn validate(&self) -> Result<(), String> {
        if self.plan.iter().any(|step| step.step.trim().is_empty()) {
            return Err("plan steps must have a nonempty description".into());
        }
        if self
            .plan
            .iter()
            .filter(|step| step.status == PlanStatus::InProgress)
            .count()
            > 1
        {
            return Err("at most one plan step may be in_progress".into());
        }
        Ok(())
    }
}
