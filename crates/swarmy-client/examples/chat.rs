//! Run with `SWARMY_API_URL`, `SWARMY_API_TOKEN`, and `SWARMY_TEST_IMAGE` set.
use swarmy_api_types::{AppendMessage, CreateSession, Cursor, EventPayload, LogId, Subscription};
use swarmy_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("SWARMY_API_URL")?;
    let token = std::env::var("SWARMY_API_TOKEN")?;
    let image = std::env::var("SWARMY_TEST_IMAGE")?;
    let (name, tag) = image
        .split_once(':')
        .ok_or("expected SWARMY_TEST_IMAGE=NAME:TAG")?;
    let client = Client::new(&url, token)?;
    let session = client
        .create_session(&CreateSession {
            idempotency_key: ulid::Ulid::generate().to_string(),
            agent_id: None,
            new: false,
            image: Some(swarmy_api_types::ImageRef {
                name: name.into(),
                tag: tag.into(),
            }),
            provider: None,
            model: None,
            effort: None,
        })
        .await?;
    let mut stream = client.stream(Subscription {
        cursors: vec![Cursor {
            log_id: LogId::Session(session.id.clone()),
            sequence: session.head_sequence,
        }],
        token_deltas: false,
    });
    let text = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    client
        .append_message(
            &session.id,
            &AppendMessage {
                idempotency_key: ulid::Ulid::generate().to_string(),
                expected_head: session.head_sequence,
                text: if text.is_empty() {
                    "Hello".into()
                } else {
                    text
                },
            },
        )
        .await?;
    loop {
        let event = stream.next().await?;
        if let EventPayload::StoreRecord { record } = event.payload
            && let Some(message) = record
                .get("InferenceCompleted")
                .and_then(|value| value.get("message"))
        {
            if let Some(parts) = message.get("parts").and_then(serde_json::Value::as_array) {
                for part in parts {
                    if let Some(text) = part
                        .get("text")
                        .and_then(|part| part.get("text"))
                        .and_then(serde_json::Value::as_str)
                    {
                        print!("{text}");
                    }
                }
                println!();
            }
            break;
        }
    }
    Ok(())
}
