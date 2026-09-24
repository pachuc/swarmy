//! Tail every session with one stream, rotating groups of 32 when necessary.
use std::{collections::HashSet, time::Duration};
use swarmy_api_types::{Cursor, LogId, Subscription};
use swarmy_client::Client;

async fn discover(
    client: &Client,
    known: &mut HashSet<String>,
    cursors: &mut Vec<Cursor>,
) -> Result<(), swarmy_client::Error> {
    let mut after = None;
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
    Ok(())
}
fn group(cursors: &[Cursor], start: usize) -> Subscription {
    Subscription {
        cursors: cursors
            .iter()
            .cycle()
            .skip(start)
            .take(cursors.len().min(32))
            .cloned()
            .collect(),
        token_deltas: false,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(
        &std::env::var("SWARMY_API_URL")?,
        std::env::var("SWARMY_API_TOKEN")?,
    )?;
    let mut known = HashSet::new();
    let mut cursors = Vec::new();
    discover(&client, &mut known, &mut cursors).await?;
    while cursors.is_empty() {
        eprintln!("waiting for sessions");
        tokio::time::sleep(Duration::from_secs(5)).await;
        discover(&client, &mut known, &mut cursors).await?;
    }
    let mut stream = client.stream(group(&cursors, 0));
    let handle = stream.subscription_handle();
    let mut offset = 0;
    loop {
        tokio::select! {
            result = stream.next() => match result {
                Ok(event) => {
                    if let Some(cursor) = cursors.iter_mut().find(|c| c.log_id == event.log_id) {
                        cursor.sequence = event.sequence;
                    }
                    println!("{} {:?} {:?}", event.sequence, event.log_id, event.payload);
                }
                Err(error) => { eprintln!("stream: {error}; restarting"); stream.restart().await; }
            },
            () = tokio::time::sleep(Duration::from_secs(5)) => {
                discover(&client, &mut known, &mut cursors).await?;
                offset = (offset + 32) % cursors.len();
                handle.set(group(&cursors, offset));
            }
        }
    }
}
