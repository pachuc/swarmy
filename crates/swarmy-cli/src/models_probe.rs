use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, ensure};
use futures_util::StreamExt as _;
use swarmy_core::{Message, MessageId, MessageRole, Part, ToolResult, UsageTotals};
use swarmy_llm::{
    ClientAuth, Delta, GenerationSettings, Provider, Request, Response, StopReason, ToolDefinition,
};

use crate::models_probe_command::Args;

pub async fn run(args: Args, json: bool) -> anyhow::Result<()> {
    let started = Instant::now();
    let settings = swarmy_config::Settings::load()?.settings;
    let catalog = settings.catalog()?;
    let (provider_id, model_id) = args
        .model
        .split_once('/')
        .context("expected PROVIDER/MODEL")?;
    let provider = catalog
        .provider(provider_id)
        .with_context(|| format!("unknown provider: {provider_id}"))?;
    let model = catalog
        .model(provider_id, model_id)
        .with_context(|| format!("unknown model: {}", args.model))?;
    // The control plane holds cluster credentials from `swarmy auth set`, so
    // prefer a server-side probe there. Scripted providers and tool-call
    // probes need local files and streaming, and run locally; without a
    // configured API the local file and environment path is the fallback.
    if provider.api != swarmy_llm::catalog::Api::Fake
        && !args.tools
        && let Ok((client, endpoint)) = crate::api_client::connect()
    {
        return server_probe(
            &client,
            &endpoint,
            provider_id,
            &model.id,
            args.effort,
            args.label,
            json,
        )
        .await;
    }
    let auth = if provider.api == swarmy_llm::catalog::Api::Fake {
        ClientAuth::Scripted(Arc::new(swarmy_llm::fake::FileFake::from_files(
            std::path::Path::new(&settings.fake.script),
            std::path::Path::new(&settings.fake.call_log),
        )?))
    } else {
        let resolver =
            swarmy_llm::auth::Resolver::new(crate::provider_runtime::auth_store(&settings))?;
        swarmy_llm::auth::resolve(provider_id, &resolver).await
            .with_context(|| format!("resolve {provider_id}; use swarmy auth set {provider_id} --from-env or swarmy auth login {provider_id}"))?.auth
    };
    let client = swarmy_llm::client_for(provider, model, auth).with_context(|| {
        format!("build {provider_id} client; configure host credentials or use swarmy auth set {provider_id} / swarmy auth login {provider_id}")
    })?;
    let effort = model
        .clamp_effort(args.effort.unwrap_or(swarmy_core::ReasoningEffort::None))
        .0;
    let mut request = request(&model.id, effort, args.tools);
    let mut totals = UsageTotals::default();
    let first = stream(client.as_ref(), request.clone(), json).await?;
    totals.add(
        &first.usage,
        swarmy_llm::cost::cost_micros(&model.cost, &first.usage),
    );
    let answer = if args.tools {
        tool_result(&mut request, first)?;
        let answer = stream(client.as_ref(), request, json).await?;
        totals.add(
            &answer.usage,
            swarmy_llm::cost::cost_micros(&model.cost, &answer.usage),
        );
        answer
    } else {
        first
    };
    ensure!(
        answer.stop_reason == StopReason::EndTurn,
        "provider ended probe with {:?}",
        answer.stop_reason
    );
    ensure!(
        answer
            .parts
            .iter()
            .any(|part| matches!(part, Part::Text { text } if !text.trim().is_empty())),
        "provider returned no answer"
    );
    if json {
        println!(
            "{}",
            serde_json::json!({"event":"probe_summary", "provider":provider_id, "model":model.id,
            "usage":totals.usage, "cost_micros":totals.cost_micros, "effort":effort, "elapsed_seconds":started.elapsed().as_secs_f64()})
        );
    } else {
        println!("\nUsage: {}", serde_json::to_string(&totals.usage)?);
        println!(
            "Cost: ${} ({} micros; catalog estimate)\nEffort used: {effort}\nElapsed: {:.3}s",
            totals.dollars(),
            totals.cost_micros,
            started.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

fn request(model: &str, effort: swarmy_core::ReasoningEffort, tools: bool) -> Request {
    Request {
        system_prompt: "Follow the user's instructions precisely.".into(),
        messages: vec![message(
            MessageRole::User,
            vec![Part::Text {
                text: if tools {
                    "Call get_time exactly once, then reply with the single word ready."
                } else {
                    "Reply with the single word ready."
                }
                .into(),
            }],
        )],
        tools: if tools {
            vec![ToolDefinition {
                name: "get_time".into(),
                description: "Return the current UTC time.".into(),
                parameters: serde_json::json!({"type":"object","properties":{},"required":[],"additionalProperties":false}),
            }]
        } else {
            vec![]
        },
        settings: GenerationSettings {
            model: model.into(),
            reasoning_effort: Some(effort),
            ..GenerationSettings::default()
        },
    }
}

fn tool_result(request: &mut Request, first: Response) -> anyhow::Result<()> {
    ensure!(
        first.stop_reason == StopReason::ToolCalls,
        "provider did not request get_time"
    );
    let calls: Vec<_> = first
        .parts
        .iter()
        .filter_map(|part| match part {
            Part::ToolCall {
                call_id,
                tool,
                input,
            } => Some((call_id, tool, input)),
            _ => None,
        })
        .collect();
    ensure!(
        calls.len() == 1 && calls[0].1 == "get_time" && calls[0].2 == &serde_json::json!({}),
        "provider must call get_time exactly once with no arguments"
    );
    let result = Part::ToolResult {
        call_id: calls[0].0.clone(),
        result: ToolResult::Completed {
            output: jiff::Timestamp::now().to_string(),
            title: "Current UTC time".into(),
            metadata: BTreeMap::new(),
        },
    };
    request
        .messages
        .push(message(MessageRole::Assistant, first.parts));
    request
        .messages
        .push(message(MessageRole::Tool, vec![result]));

    Ok(())
}

fn message(role: MessageRole, parts: Vec<Part>) -> Message {
    Message {
        id: MessageId::from_ulid(ulid::Ulid::generate()),
        role,
        parts,
    }
}

async fn stream(client: &dyn Provider, request: Request, json: bool) -> anyhow::Result<Response> {
    let mut stream = client.request(request);
    let mut rendered = BTreeSet::new();
    while let Some(delta) = stream.next().await {
        let delta = delta?;
        if json {
            println!("{}", serde_json::json!({"event":"delta", "delta":delta}));
        } else {
            match &delta {
                Delta::Text { output_index, text } | Delta::Reasoning { output_index, text } => {
                    rendered.insert(*output_index);
                    print!("{text}");
                }
                Delta::PartDone {
                    output_index,
                    part: Part::Text { text } | Part::Reasoning { text, .. },
                } if rendered.insert(*output_index) => print!("{text}"),
                Delta::ToolArguments { arguments, .. } => print!("{arguments}"),
                Delta::PartDone {
                    part: Part::ToolCall { tool, input, .. },
                    ..
                } => println!("\nTool: {tool} {input}"),
                _ => (),
            }
            std::io::stdout().flush()?;
        }
        if let Delta::Completed(response) = delta {
            return Ok(response);
        }
    }
    anyhow::bail!("provider stream ended without a completion")
}

/// Probe through the control plane so `swarmy auth set` credentials work
/// without local files. The summary matches the local probe output.
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
