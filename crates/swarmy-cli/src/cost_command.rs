/// Show billed token and cost totals from the metering rollups.
///
/// Without a key filter the series aggregates every key in the dimension.
/// `swarmy cost --agent coder --group week --since 3mo` shows what the
/// `coder` agent cost each week this quarter.
#[derive(clap::Args)]
pub struct Args {
    /// Rollup dimension to read: session, agent, provider, entry, kind, or model
    #[arg(long, value_enum)]
    pub by: Option<CostDimension>,
    /// Calendar grouping for the rows
    #[arg(long, default_value = "day", value_enum)]
    pub group: UsageGroup,
    /// Range start: an absolute date or timestamp, a relative span like 7d, 3mo, or 1y,
    /// or a calendar word like month or 2months for the start of this or last month
    #[arg(long)]
    pub since: Option<String>,
    /// Range end: an absolute date or timestamp, a relative span, a calendar word, or now
    #[arg(long)]
    pub until: Option<String>,
    /// Restrict the series to one agent (name or id)
    #[arg(long)]
    pub agent: Option<String>,
    /// Restrict the series to one session id
    #[arg(long)]
    pub session: Option<String>,
    /// Restrict the series to one provider
    #[arg(long)]
    pub provider: Option<String>,
    /// Restrict the series to one auth entry in PROVIDER/LABEL form
    #[arg(long)]
    pub entry: Option<String>,
    /// Restrict the series to one entry kind
    #[arg(long)]
    pub kind: Option<String>,
    /// Restrict the series to one model
    #[arg(long)]
    pub model: Option<String>,
}

/// Rollup dimension for `swarmy cost --by`, parsed once by clap.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum CostDimension {
    Session,
    Agent,
    Provider,
    Entry,
    Kind,
    Model,
}

impl CostDimension {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Agent => "agent",
            Self::Provider => "provider",
            Self::Entry => "entry",
            Self::Kind => "kind",
            Self::Model => "model",
        }
    }
}

/// Calendar grouping for usage rows, parsed once by clap.
#[derive(Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum UsageGroup {
    #[default]
    Day,
    Week,
    Month,
    Year,
}

impl UsageGroup {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Year => "year",
        }
    }
}

impl std::fmt::Display for UsageGroup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn cost_flags_parse_without_connecting_to_the_stack() {
        assert!(crate::Cli::try_parse_from(["swarmy", "cost"]).is_ok());
        assert!(
            crate::Cli::try_parse_from([
                "swarmy", "cost", "--by", "agent", "--group", "month", "--since", "1y"
            ])
            .is_ok()
        );
        assert!(
            crate::Cli::try_parse_from([
                "swarmy", "cost", "--agent", "coder", "--group", "week", "--since", "3mo"
            ])
            .is_ok()
        );
        assert!(crate::Cli::try_parse_from(["swarmy", "cost", "--by", "team"]).is_err());
        assert!(crate::Cli::try_parse_from(["swarmy", "cost", "--group", "hour"]).is_err());
    }
}
