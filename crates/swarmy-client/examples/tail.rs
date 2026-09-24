//! Tail sessions already present in a swarm. Newly created sessions are picked up on the next scan.
use std::{collections::HashSet, time::Duration};
use swarmy_api_types::{Cursor, LogId, Subscription};
use swarmy_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(
        &std::env::var("SWARMY_API_URL")?,
        std::env::var("SWARMY_API_TOKEN")?,
    )?;
    let mut known = HashSet::new();
    let mut stream = None;
    loop {
        let mut after = None;
        let mut cursors = Vec::new();
        loop {
            let page = client.sessions(after.as_deref(), 100).await?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|session| session.id.clone());
            for session in page {
                if known.insert(session.id.clone()) {
                    cursors.push(Cursor {
                        log_id: LogId::Session(session.id),
                        sequence: 0,
                    });
                }
            }
        }
        if !cursors.is_empty() {
            // The server currently allows up to 32 logs per connection.
            for chunk in cursors.chunks(32) {
                let subscription = Subscription {
                    cursors: chunk.to_vec(),
                    token_deltas: false,
                };
                let client = client.clone();
                tokio::spawn(async move {
                    let mut stream = client.stream(subscription);
                    loop {
                        match stream.next().await {
                            Ok(event) => println!(
                                "{} {:?} {:?}",
                                event.sequence, event.log_id, event.payload
                            ),
                            Err(error) => {
                                eprintln!("stream: {error}");
                                break;
                            }
                        }
                    }
                });
            }
            stream = Some(());
        }
        if stream.is_none() {
            eprintln!("waiting for sessions");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
