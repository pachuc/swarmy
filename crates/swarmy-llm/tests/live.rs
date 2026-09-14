//! Manual test only. Never falls back to a Codex or launcher credential path.
use futures::TryStreamExt;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};
use swarmy_core::{Message, MessageId, MessageRole, Part};
use swarmy_llm::{
    Delta, GenerationSettings, Provider, Request, auth::FileCredentialStore,
    chatgpt::ChatGptProvider,
};

#[tokio::test]
#[ignore = "requires a dedicated swarmy auth login and explicitly selected models; spends subscription quota"]
async fn dedicated_chatgpt_account_models() {
    let Some(path) = std::env::var_os("SWARMY_CHATGPT_AUTH") else {
        return;
    };
    let models = std::env::var("SWARMY_CHATGPT_MODELS")
        .expect("set SWARMY_CHATGPT_MODELS to comma-separated model ids to probe");
    let report = std::env::var_os("SWARMY_CHATGPT_REPORT").map_or_else(
        || PathBuf::from("/tmp/swarmy-chatgpt-models.json"),
        PathBuf::from,
    );
    let provider = ChatGptProvider::new(Arc::new(FileCredentialStore::new(path))).unwrap();
    let mut results = Vec::new();
    for model in models.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let request = Request {
            system_prompt: "Answer briefly.".into(),
            messages: vec![Message {
                id: MessageId::from_ulid(ulid::Ulid::nil()),
                role: MessageRole::User,
                parts: vec![Part::Text {
                    text: "Say hello.".into(),
                }],
            }],
            tools: vec![],
            settings: GenerationSettings {
                model: model.into(),
                ..Default::default()
            },
        };
        let result = provider.request(request).try_collect::<Vec<_>>().await;
        let entry = match result {
            Ok(events) => {
                json!({"model": model, "usable": matches!(events.last(), Some(Delta::Completed(_)))})
            }
            Err(error) => json!({"model": model, "usable": false, "error": error.to_string()}),
        };
        results.push(entry);
    }
    tokio::fs::write(
        report,
        serde_json::to_vec_pretty(
            &json!({"tested_at": jiff::Timestamp::now().to_string(), "models": results}),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(
        results.iter().any(|result| result["usable"] == true),
        "no tested model succeeded; inspect the report"
    );
}
