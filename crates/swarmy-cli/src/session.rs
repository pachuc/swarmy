use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use swarmy_core::{MessageId, SessionId};
use swarmy_store::MAX_SCAN_LIMIT;

pub use crate::session_command::Command;

pub async fn inspect(command: Command, json: bool) -> Result<()> {
    let store = store().await?;
    match command {
        Command::Interrupt { session_id } => {
            let id = SessionId::from_ulid(session_id);
            let result = store.interrupt_session(id).await?;
            if result == swarmy_store::InterruptResult::Finished {
                let session = store
                    .fetch_session(id)
                    .await?
                    .context("session not found")?;
                if let Some(event) = store.read_events(id, session.head_seq - 1, 1).await?.pop()
                    && let Ok(bus) = bus().await
                {
                    let _ = bus
                        .publish_live(swarmy_bus::LiveFeed::SessionEvents(id), &event)
                        .await;
                }
            }
            let (status, message) = match result {
                swarmy_store::InterruptResult::Finished => {
                    ("finished", format!("Interrupted session {id}"))
                }
                swarmy_store::InterruptResult::Requested => {
                    ("requested", format!("Interrupt requested for session {id}"))
                }
            };
            crate::vol::output(
                &serde_json::json!({"event": "session_interrupt", "session_id": id, "result": status}),
                &message,
                json,
            )?;
        }
        Command::Close { session_id } => {
            let id = SessionId::from_ulid(session_id);
            store.close_session(id, jiff::Timestamp::now()).await?;
            crate::vol::output(
                &serde_json::json!({"event": "session_closed", "session_id": id}),
                &format!("Closed session {id}"),
                json,
            )?;
        }
        Command::Show { .. } | Command::List => unreachable!("session reads use the API"),
        Command::Metrics { session_id } => {
            show_metrics(&store, SessionId::from_ulid(session_id), json).await?;
        }
    }
    Ok(())
}

async fn show_metrics(store: &swarmy_store::Store, id: SessionId, json: bool) -> Result<()> {
    anyhow::ensure!(
        store.fetch_session(id).await?.is_some(),
        "session not found"
    );
    let mut after = None;
    let mut rows = Vec::new();
    loop {
        let page = store.list_turn_metrics(id, after, MAX_SCAN_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page
            .last()
            .and_then(|row| row.turn_id.parse::<ulid::Ulid>().ok())
            .map(MessageId::from_ulid);
        rows.extend(page);
    }
    if json {
        println!("{}", serde_json::to_string(&rows)?);
    } else {
        for row in rows {
            let ms = |value: Option<f64>| value.map_or_else(|| "-".into(), |ms| format!("{ms:.1}"));
            println!(
                "{} append_to_first_token_ms={} inference_ms={} append_to_idle_ms={} tools={} error={}",
                row.turn_id,
                ms(row.append_to_first_token_ms),
                ms(row.inference_duration_ms),
                ms(row.append_to_idle_ms),
                row.tools.len(),
                row.error.as_deref().unwrap_or("-"),
            );
        }
    }
    Ok(())
}

/// Open the cluster store for the database commands that still run here.
/// Conversation commands (`run`, `chat`, `bench`) talk to the control-plane
/// API instead, so this helper serves only interrupt, close, volumes, images,
/// and the volume server.
pub async fn store() -> Result<swarmy_store::Store> {
    let settings = swarmy_config::Settings::load()?.settings;
    let cluster = settings.fdb_cluster_file;
    let directory: Vec<_> = settings
        .store_directory
        .split('/')
        .map(str::to_owned)
        .collect();
    Ok(swarmy_store::Store::open(
        Some(&cluster),
        Some(&directory),
        Arc::new(swarmy_store::blob::ObjectBlobStore::from_env()?),
    )
    .await?)
}

const WAKE_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) async fn bus() -> Result<swarmy_bus::Bus> {
    let settings = swarmy_config::Settings::load()?.settings;
    let url = settings.nats_url;
    let config = swarmy_bus::Config {
        prefix: if settings.bus_prefix.is_empty() {
            None
        } else {
            Some(swarmy_bus::SubjectToken::new(settings.bus_prefix)?)
        },
        ack_wait: Duration::from_millis(settings.bus_ack_wait_ms),
        max_deliver: settings.bus_max_deliver,
    };
    let bus = tokio::time::timeout(WAKE_TIMEOUT, swarmy_bus::Bus::connect(&url, config))
        .await
        .context("cannot reach scheduler: NATS connection timed out")?
        .context("cannot reach scheduler over NATS")?;
    Ok(bus)
}
