use serde::{Deserialize, Serialize};
use swarmy_llm::catalog::Catalog;

#[derive(Serialize, Deserialize)]
pub struct ProviderRow {
    pub provider: String,
    pub credential: String,
    pub status: String,
    pub store: String,
    pub gateway: String,
    pub gateway_reason: String,
}

pub fn local(catalog: &Catalog, store: &str) -> Vec<ProviderRow> {
    catalog
        .providers()
        .map(|provider| {
            let keys: &[&str] = match provider.id.as_str() {
                "anthropic" => &["ANTHROPIC_API_KEY"],
                "openai" => &["OPENAI_API_KEY"],
                "xai" => &["XAI_API_KEY"],
                "meta" => &["META_MODEL_API_KEY"],
                "openrouter" => &["OPENROUTER_API_KEY"],
                "azure" => &["AZURE_API_KEY", "AZURE_OPENAI_API_KEY"],
                "google" => &[
                    "GEMINI_API_KEY",
                    "GOOGLE_API_KEY",
                    "GOOGLE_GENERATIVE_AI_API_KEY",
                ],
                "amazon-bedrock" => &["AWS_BEARER_TOKEN_BEDROCK"],
                _ => &[],
            };
            let credential = if provider.id == "fake" {
                "not required"
            } else if keys.iter().any(|key| present(key)) {
                "environment"
            } else if ambient(&provider.id) {
                "ambient"
            } else {
                "none"
            };
            ProviderRow {
                provider: provider.id.clone(),
                credential: credential.into(),
                status: match credential {
                    "none" => "no credential",
                    "not required" => "ready",
                    _ => "unverified",
                }
                .into(),
                store: store.into(),
                gateway: "unknown".into(),
                gateway_reason: String::new(),
            }
        })
        .collect()
}

fn present(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

fn home_file(path: &str) -> bool {
    std::env::var_os("HOME").is_some_and(|home| std::path::Path::new(&home).join(path).is_file())
}

fn ambient(provider: &str) -> bool {
    match provider {
        "amazon-bedrock" => {
            (present("AWS_ACCESS_KEY_ID") && present("AWS_SECRET_ACCESS_KEY"))
                || [
                    "AWS_PROFILE",
                    "AWS_WEB_IDENTITY_TOKEN_FILE",
                    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                    "AWS_SHARED_CREDENTIALS_FILE",
                    "AWS_CONFIG_FILE",
                ]
                .iter()
                .any(|key| present(key))
                || home_file(".aws/credentials")
                || home_file(".aws/config")
        }
        "google-vertex" | "google-vertex-anthropic" => {
            present("GOOGLE_APPLICATION_CREDENTIALS")
                || home_file(".config/gcloud/application_default_credentials.json")
        }
        _ => false,
    }
}
