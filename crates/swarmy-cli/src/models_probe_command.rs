use swarmy_core::ReasoningEffort;

#[derive(clap::Args)]
pub struct Args {
    /// Exact PROVIDER/MODEL from the configured catalog
    pub model: String,
    #[arg(long)]
    pub effort: Option<ReasoningEffort>,
    /// Require a `get_time` call and send its result back before the answer
    #[arg(long)]
    pub tools: bool,
    /// Credential entry label to probe on the control plane (default when absent).
    #[arg(long)]
    pub label: Option<String>,
}
