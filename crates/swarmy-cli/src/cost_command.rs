/// Show billed token and cost totals from the metering rollups.
///
/// Without a key filter the series aggregates every key in the dimension.
/// `swarmy cost --agent coder --group week --since 3mo` shows what the
/// `coder` agent cost each week this quarter.
#[derive(clap::Args)]
pub struct Args {
    /// Rollup dimension to read: session, agent, provider, entry, kind, or model
    #[arg(long, value_parser = ["session", "agent", "provider", "entry", "kind", "model"])]
    pub by: Option<String>,
    /// Calendar grouping for the rows
    #[arg(long, default_value = "day", value_parser = ["day", "week", "month", "year"])]
    pub group: String,
    /// Range start: an absolute date or timestamp, or a relative span like 7d, 3mo, or 1y
    #[arg(long)]
    pub since: Option<String>,
    /// Range end: an absolute date or timestamp, a relative span, or now
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
