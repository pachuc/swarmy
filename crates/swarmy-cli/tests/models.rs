use serde_json::Value;
use std::{
    fs,
    process::{Command, Output},
};

struct Fixture {
    dir: tempfile::TempDir,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    server: Option<std::thread::JoinHandle<()>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}
#[derive(serde::Deserialize)]
struct Search {
    q: Option<String>,
    provider: Option<String>,
    reasoning: Option<bool>,
}
async fn models(
    axum::extract::State(catalog): axum::extract::State<swarmy_llm::catalog::Catalog>,
    axum::extract::Query(search): axum::extract::Query<Search>,
) -> axum::Json<Vec<Value>> {
    let rows = catalog
        .find(search.q.as_deref().unwrap_or(""))
        .into_iter()
        .filter(|(provider, model)| {
            search.provider.as_ref().is_none_or(|id| id == &provider.id)
                && (!search.reasoning.unwrap_or(false)
                    || model
                        .supported_efforts()
                        .iter()
                        .any(|effort| *effort != swarmy_core::ReasoningEffort::None))
        })
        .map(|(provider, model)| {
            let mut row = serde_json::to_value(model).unwrap();
            row["key"] = serde_json::json!(format!("{}/{}", provider.id, model.id));
            row["provider"] = serde_json::json!(provider.id);
            row["effective_api"] = serde_json::json!(model.api.unwrap_or(provider.api));
            row["effective_base_url"] =
                serde_json::json!(model.base_url.as_deref().unwrap_or(&provider.base_url));
            row["supported_efforts"] = serde_json::json!(model.supported_efforts());
            row
        })
        .collect();
    axum::Json(rows)
}
async fn providers(
    axum::extract::State(catalog): axum::extract::State<swarmy_llm::catalog::Catalog>,
) -> axum::Json<Vec<Value>> {
    axum::Json(
        catalog
            .providers()
            .map(|provider| {
                serde_json::json!({"id":provider.id,"api":provider.api,
        "auth_kinds":provider.auth_kinds,"env_keys":provider.env_keys,"credential":"unknown"})
            })
            .collect(),
    )
}

impl Fixture {
    fn new(config: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(directory.path().join(".swarmy")).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let config = format!("{config}\n[api]\nurl = '{endpoint}'\ntoken = 'fixture-token'\n");
        let config_path = directory.path().join(".swarmy/config.toml");
        fs::write(&config_path, config).unwrap();
        let catalog = swarmy_config::Settings::read(&config_path)
            .unwrap()
            .catalog()
            .unwrap();
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let (ready, started) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let app = axum::Router::new()
                    .route("/v1/cli/models", axum::routing::get(models))
                    .route("/v1/cli/providers", axum::routing::get(providers))
                    .with_state(catalog);
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                ready.send(()).unwrap();
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
        });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        Self {
            dir: directory,
            shutdown: Some(shutdown),
            server: Some(server),
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarmy"));
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                command.env_remove(key);
            }
        }
        command
            .current_dir(self.dir.path())
            .env_remove("OPENAI_API_KEY")
            .env("HOME", self.dir.path())
            .env("XDG_CONFIG_HOME", self.dir.path())
            .args(args)
            .output()
            .unwrap()
    }

    fn success(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    fn json(&self, args: &[&str]) -> Value {
        serde_json::from_str(&self.success(args)).unwrap()
    }
}

#[test]
fn lists_sorted_snapshot_models_as_json() {
    let fixture = Fixture::new("");
    let rows = fixture.json(&["models", "ls", "--json"]);
    let rows = rows.as_array().unwrap();
    assert!(rows.iter().any(|row| row["key"] == "openai/gpt-5.5"));
    let keys: Vec<_> = rows
        .iter()
        .map(|row| {
            (
                row["provider"].as_str().unwrap(),
                row["id"].as_str().unwrap(),
            )
        })
        .collect();
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    for row in rows {
        assert!(row["name"].is_string());
        assert!(row["limit"]["context"].is_number());
        assert!(row["cost"]["input"].is_number());
        assert!(row["supported_efforts"].is_array());
    }
}

#[test]
fn show_resolves_model_ids_with_slashes_and_includes_compat() {
    let fixture = Fixture::new("");
    let output = fixture.success(&["models", "show", "openrouter/anthropic/claude-sonnet-4.6"]);
    let model: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(model["id"], "anthropic/claude-sonnet-4.6");
    assert_eq!(model["provider"], "openrouter");
    assert_eq!(model["effective_api"], "AnthropicMessages");
    assert!(model["compat"].is_object());
}

#[test]
fn search_and_lookup_errors_are_clear() {
    let fixture = Fixture::new("");
    for (args, message) in [
        (
            vec!["models", "search", "nonexistent"],
            "no models found matching \"nonexistent\"",
        ),
        (
            vec!["models", "ls", "--provider", "missing"],
            "unknown provider: missing",
        ),
        (
            vec!["models", "show", "openai/missing"],
            "unknown model: openai/missing",
        ),
        (vec!["models", "show", "missing"], "expected PROVIDER/MODEL"),
    ] {
        let output = fixture.run(&args);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(message));
    }
}

#[test]
fn commands_share_configured_catalog_and_filters() {
    let fixture = Fixture::new(
        r#"
[custom_providers.private]
api = "OpenAiCompletions"
base_url = "http://localhost:8000/v1"
[[models]]
provider = "private"
id = "team/reasoner"
reasoning = ["low", "high"]
[[models]]
provider = "private"
id = "ordinary"
[[models]]
provider = "openai"
id = "gpt-5.5"
context_window = 42
"#,
    );
    let rows = fixture.json(&[
        "models",
        "ls",
        "--provider",
        "private",
        "--reasoning",
        "--json",
    ]);
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["id"], "team/reasoner");
    assert_eq!(rows[0]["effective_api"], "OpenAiCompletions");
    assert_eq!(rows[0]["effective_base_url"], "http://localhost:8000/v1");
    let shown = fixture.json(&["models", "show", "private/team/reasoner", "--json"]);
    assert_eq!(shown, rows[0]);
    let rows = fixture.json(&["models", "search", "PRIVATE/", "--json"]);
    assert_eq!(rows.as_array().unwrap().len(), 2);
    let model = fixture.json(&["models", "show", "openai/gpt-5.5", "--json"]);
    assert_eq!(model["limit"]["context"], 42);
    let providers = fixture.json(&["models", "providers", "--json"]);
    let private = providers
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "private")
        .unwrap();
    assert_eq!(private["api"], "OpenAiCompletions");
    assert_eq!(private["credential"], "unknown");
}

#[test]
fn terminal_tables_fit_eighty_columns() {
    let fixture = Fixture::new("");
    for args in [
        vec!["models", "providers"],
        vec!["models", "ls", "--provider", "anthropic"],
    ] {
        let output = fixture.success(&args);
        assert!(!output.is_empty());
        assert!(
            output
                .lines()
                .all(|line| unicode_width::UnicodeWidthStr::width(line) <= 80)
        );
    }
}

#[test]
fn probe_streams_fake_and_completes_tool_round_trip() {
    let fixture = Fixture::new(
        "model = 'scripted'\n[fake]\nscript = 'script.json'\ncall_log = 'calls.jsonl'",
    );
    let script = fixture.dir.path().join("script.json");
    fs::write(
        &script,
        r#"{"request_based":{"steps":1,"tool_steps":[],"final_answer":"ready"}}"#,
    )
    .unwrap();
    let text = fixture.success(&["models", "probe", "fake/scripted", "--effort", "high"]);
    for expected in ["ready", "Usage:", "Cost:", "Effort used: high", "Elapsed:"] {
        assert!(text.contains(expected), "{text}");
    }
    let output = fixture.run(&["models", "probe", "fake/scripted", "--tools"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("did not request get_time"));
    fs::write(
        &script,
        r#"{"request_based":{"steps":2,"tool_steps":[0],"final_answer":"ready"}}"#,
    )
    .unwrap();
    let text = fixture.success(&["models", "probe", "fake/scripted", "--tools", "--json"]);
    let rows: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.last().unwrap()["event"], "probe_summary");
    assert_eq!(rows.last().unwrap()["cost_micros"], 0);
    assert_eq!(
        rows.iter()
            .filter(|row| row["delta"].get("Completed").is_some())
            .count(),
        2
    );
    fs::write(&script, r#"{"fail":true}"#).unwrap();
    let output = fixture.run(&["models", "probe", "fake/scripted"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("scripted provider failure"));
}

#[test]
fn probe_missing_credential_names_auth_set() {
    let fixture = Fixture::new("");
    let output = fixture.run(&["models", "probe", "openai/gpt-5.5"]);
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("swarmy auth set openai"), "{error}");
}
