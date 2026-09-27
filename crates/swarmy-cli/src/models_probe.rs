use crate::models_probe_command::Args;
use anyhow::Context;

pub async fn run(args: Args, json: bool) -> anyhow::Result<()> {
    anyhow::ensure!(
        !args.tools,
        "tool-call probes are not supported by the API probe route"
    );
    let (provider, model) = args
        .model
        .split_once('/')
        .context("expected PROVIDER/MODEL")?;
    let (client, endpoint) = crate::api_client::connect()?;
    server_probe(
        &client,
        &endpoint,
        provider,
        model,
        args.effort,
        args.label,
        json,
    )
    .await
}

async fn server_probe(
    client: &swarmy_client::Client,
    endpoint: &str,
    provider_id: &str,
    model_id: &str,
    effort: Option<swarmy_core::ReasoningEffort>,
    label: Option<String>,
    json: bool,
) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let effort = effort
        .map(|effort| {
            serde_json::to_value(effort)
                .ok()
                .and_then(|value| serde_json::from_value(value).ok())
                .context("encoding probe effort")
        })
        .transpose()?;
    // A live inference round trip can take minutes on a loaded provider.
    // The client error is matched before the endpoint wrapper so a missing
    // credential still names its recovery command.
    let answer = tokio::time::timeout(
        std::time::Duration::from_secs(300),
        client.probe_model(&swarmy_api_types::ProbeModel {
            provider: provider_id.into(),
            model: model_id.into(),
            label,
            effort,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("API at {endpoint}: request timed out"))?
    .map_err(|error| {
        // The server's fixed `credential_unavailable` code carries the
        // resolver detail in its message; name the recovery command here so
        // the hint survives without baking it into the API code.
        if let swarmy_client::Error::Api { body, .. } = &error
            && body.code == "credential_unavailable"
        {
            return anyhow::anyhow!(
                "resolve {provider_id}: {}; use swarmy auth set {provider_id} --from-env or swarmy auth login {provider_id}",
                body.message
            );
        }
        crate::api_client::api_error(&error, endpoint)
    })?;
    let dollars = {
        let units = answer.cost_micros / 100 + u64::from(answer.cost_micros % 100 >= 50);
        format!("{}.{:04}", units / 10_000, units % 10_000)
    };
    if json {
        println!(
            "{}",
            serde_json::json!({"event":"probe_summary", "provider":answer.provider, "model":answer.model,
            "usage":answer.usage, "cost_micros":answer.cost_micros, "effort":answer.effort, "elapsed_seconds":started.elapsed().as_secs_f64()})
        );
    } else {
        println!("\nUsage: {}", serde_json::to_string(&answer.usage)?);
        println!(
            "Cost: ${} ({} micros; catalog estimate)\nEffort used: {}\nElapsed: {:.3}s",
            dollars,
            answer.cost_micros,
            serde_json::to_value(&answer.effort)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| "unknown".into()),
            started.elapsed().as_secs_f64()
        );
    }
    Ok(())
}
