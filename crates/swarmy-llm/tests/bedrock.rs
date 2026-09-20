use futures::TryStreamExt as _;
use swarmy_core::{Message, MessageId, MessageRole, Part};
use swarmy_llm::{ClientAuth, Delta, GenerationSettings, Request, catalog::Catalog, client_for};

#[tokio::test]
async fn live_bedrock_turn() {
    let Ok(id) = std::env::var("SWARMY_BEDROCK_TEST_MODEL") else {
        return;
    };
    let provider = Catalog::get().provider("amazon-bedrock").unwrap();
    let mut model = provider.models.get(&id).cloned().unwrap_or_else(|| {
        let mut model = provider.models.values().next().unwrap().clone();
        model.id.clone_from(&id);
        model.name.clone_from(&id);
        model.reasoning = None;
        model
    });
    model.limit.output = Some(1024);
    let request = Request {
        system_prompt: "Reply briefly.".into(),
        messages: vec![Message {
            id: MessageId::from_ulid(ulid::Ulid::nil()),
            role: MessageRole::User,
            parts: vec![Part::Text {
                text: "Say hello.".into(),
            }],
        }],
        tools: Vec::new(),
        settings: GenerationSettings {
            model: id,
            max_output_tokens: Some(1024),
            ..Default::default()
        },
    };
    let client = client_for(provider, &model, ClientAuth::Ambient).unwrap();
    let deltas = client
        .request(request)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(
        matches!(deltas.last(), Some(Delta::Completed(response)) if !response.parts.is_empty() && response.usage.total_tokens > 0)
    );
}
