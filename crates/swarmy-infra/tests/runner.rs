#![deny(clippy::disallowed_methods)]
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use swarmy_infra::{
    CredentialEnv, Error, ModuleFile, OutputError, PlanOutcome, Tofu, VARIABLES_FILE, parse_outputs,
};
const SECRET: &str = "hcloud-test-token-5f2b9c";
const OUTPUTS: &str = r#"{"machines":{"value":[{"name":"worker","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":null,"ssh_user":"root"}]}}"#;
fn fake_tofu(fixture: &Path, behaviour: &str, outputs: &str) -> PathBuf {
    let script = fixture.join("tofu");
    let record = fixture.join("record");
    let json = fixture.join("outputs.json");
    std::fs::write(&json, outputs).unwrap();
    let body = format!(
        r#"#!/bin/sh
{{
  printf 'cwd=%s\n' "$(pwd -P)"
  for a in "$@"; do printf 'arg=%s\n' "$a"; done
  env | sed 's/^/env=/'
  printf 'end\n'
}} >> '{}'
{}
"#,
        record.display(),
        behaviour.replace("{outputs}", &json.display().to_string())
    );
    std::fs::write(&script, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    script
}
const DEFAULT: &str = r#"case "$1" in
output) cat '{outputs}' ;;
apply) echo 'state' > terraform.tfstate ;;
plan) exit 2 ;;
esac"#;
#[derive(Debug)]
struct Invocation {
    cwd: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
}
fn invocations(path: &Path) -> Vec<Invocation> {
    let mut all = Vec::new();
    let mut current = None;
    for line in std::fs::read_to_string(path).unwrap().lines() {
        if let Some(cwd) = line.strip_prefix("cwd=") {
            current = Some(Invocation {
                cwd: cwd.into(),
                args: Vec::new(),
                env: BTreeMap::new(),
            });
        } else if let Some(arg) = line.strip_prefix("arg=") {
            current.as_mut().unwrap().args.push(arg.into());
        } else if let Some(env) = line.strip_prefix("env=") {
            if let Some((name, value)) = env.split_once('=') {
                current
                    .as_mut()
                    .unwrap()
                    .env
                    .insert(name.into(), value.into());
            }
        } else if line == "end" {
            all.push(current.take().unwrap());
        }
    }
    all
}
fn variables() -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({"name":"fleet","count":2})
        .as_object()
        .unwrap()
        .clone()
}
fn module() -> [ModuleFile<'static>; 2] {
    [
        ModuleFile {
            path: "main.tf",
            contents: b"resource \"null_resource\" \"x\" {}",
        },
        ModuleFile {
            path: "outputs.tf",
            contents: b"output \"machines\" {}",
        },
    ]
}
fn assert_environment(call: &Invocation, cache: &Path) {
    assert_eq!(call.env.get("HCLOUD_TOKEN").unwrap(), SECRET);
    assert_eq!(call.env.get("TF_IN_AUTOMATION").unwrap(), "1");
    assert_eq!(call.env.get("TF_INPUT").unwrap(), "0");
    assert_eq!(
        call.env.get("TF_PLUGIN_CACHE_DIR").unwrap(),
        &cache.join("tofu/plugins").to_string_lossy()
    );
    assert!(!call.env.contains_key("CARGO_MANIFEST_DIR"));
    for key in call.env.keys() {
        assert!(
            !key.starts_with("CARGO")
                && !key.starts_with("RUST")
                && !key.starts_with("TF_CLI_ARGS")
        );
        assert!(
            [
                "PATH",
                "HOME",
                "TMPDIR",
                "LANG",
                "LC_ALL",
                "SSL_CERT_FILE",
                "SSL_CERT_DIR",
                "HTTPS_PROXY",
                "https_proxy",
                "HTTP_PROXY",
                "http_proxy",
                "NO_PROXY",
                "no_proxy",
                "TF_IN_AUTOMATION",
                "TF_INPUT",
                "TF_PLUGIN_CACHE_DIR",
                "HCLOUD_TOKEN",
                "PWD",
                "OLDPWD",
                "SHLVL",
                "_"
            ]
            .contains(&key.as_str()),
            "{key}"
        );
    }
}
#[tokio::test]
async fn argument_sequence_and_environment_passing() {
    assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_some());
    let fixture = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let tofu = Tofu::from_binary(fake_tofu(fixture.path(), DEFAULT, OUTPUTS), cache.path());
    let workspace = tofu.workspace(
        work.path(),
        CredentialEnv::new().with("HCLOUD_TOKEN", SECRET),
    );
    workspace.prepare(&module(), &variables()).await.unwrap();
    workspace.init().await.unwrap();
    assert_eq!(workspace.plan().await.unwrap(), PlanOutcome::Changes);
    workspace.apply().await.unwrap();
    assert_eq!(
        workspace.outputs().await.unwrap(),
        parse_outputs(OUTPUTS).unwrap()
    );
    workspace.destroy().await.unwrap();
    let calls = invocations(&fixture.path().join("record"));
    assert_eq!(calls.len(), 5);
    let expected = [
        vec!["init", "-input=false", "-no-color"],
        vec![
            "plan",
            "-input=false",
            "-no-color",
            "-detailed-exitcode",
            "-lock-timeout=120s",
            "-var-file=swarmy.tfvars.json",
        ],
        vec![
            "apply",
            "-input=false",
            "-no-color",
            "-auto-approve",
            "-lock-timeout=120s",
            "-var-file=swarmy.tfvars.json",
        ],
        vec!["output", "-no-color", "-json"],
        vec![
            "destroy",
            "-input=false",
            "-no-color",
            "-auto-approve",
            "-lock-timeout=120s",
            "-var-file=swarmy.tfvars.json",
        ],
    ];
    for (call, args) in calls.iter().zip(expected) {
        assert_eq!(call.args, args);
        assert_eq!(
            call.cwd,
            work.path().canonicalize().unwrap().to_string_lossy()
        );
        assert_environment(call, cache.path());
    }
    assert!(cache.path().join("tofu/plugins").is_dir());
    let plan0 = Tofu::from_binary(
        fake_tofu(
            fixture.path(),
            "case \"$1\" in plan) exit 0;; esac",
            OUTPUTS,
        ),
        cache.path(),
    );
    assert_eq!(
        plan0
            .workspace(work.path(), CredentialEnv::new())
            .plan()
            .await
            .unwrap(),
        PlanOutcome::NoChanges
    );
}
fn walk(dir: &Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path);
        } else if path.is_file() {
            assert!(
                !std::fs::read(path)
                    .unwrap()
                    .windows(SECRET.len())
                    .any(|w| w == SECRET.as_bytes())
            );
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn no_credential_in_files_or_logs() {
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };
    #[derive(Clone)]
    struct Writer(Arc<Mutex<Vec<u8>>>);
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Writer {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let captured = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(Writer(captured.clone()))
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let fixture = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let tofu = Tofu::from_binary(fake_tofu(fixture.path(), DEFAULT, OUTPUTS), cache.path());
    let workspace = tofu.workspace(
        work.path(),
        CredentialEnv::new().with("HCLOUD_TOKEN", SECRET),
    );
    workspace.prepare(&module(), &variables()).await.unwrap();
    workspace.init().await.unwrap();
    workspace.plan().await.unwrap();
    workspace.apply().await.unwrap();
    workspace.outputs().await.unwrap();
    workspace.destroy().await.unwrap();
    walk(work.path());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(work.path().join(VARIABLES_FILE)).unwrap()
        )
        .unwrap(),
        serde_json::Value::Object(variables())
    );
    let log = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    assert!(
        !log.is_empty() && !log.contains(SECRET),
        "log len={} redacted={}",
        log.len(),
        log.replace(SECRET, "<redacted>")
    );
    assert!(!format!("{workspace:?}").contains(SECRET));
    assert!(!format!("{:?}", CredentialEnv::new().with("HCLOUD_TOKEN", SECRET)).contains(SECRET));
}
#[tokio::test]
async fn failure_is_structured_and_redacted() {
    let fixture = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let tofu = Tofu::from_binary(
        fake_tofu(
            fixture.path(),
            "case \"$1\" in apply) echo 'Error: boom' >&2; echo \"$HCLOUD_TOKEN\" >&2; exit 1;; esac",
            OUTPUTS,
        ),
        cache.path(),
    );
    let error = tofu
        .workspace(
            work.path(),
            CredentialEnv::new().with("HCLOUD_TOKEN", SECRET),
        )
        .apply()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Failed {
            command: "apply",
            ..
        }
    ));
    let display = error.to_string();
    assert!(
        display.contains("Error: boom")
            && display.contains("<redacted>")
            && display.contains(&work.path().display().to_string())
    );
    assert!(!display.contains(SECRET) && !format!("{error:?}").contains(SECRET));
}
#[tokio::test]
async fn secret_and_bad_paths() {
    let fixture = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let tofu = Tofu::from_binary(fake_tofu(fixture.path(), DEFAULT, OUTPUTS), cache.path());
    let workspace = tofu.workspace(
        work.path(),
        CredentialEnv::new().with("HCLOUD_TOKEN", SECRET),
    );
    let mut vars = variables();
    vars.insert("secret".into(), SECRET.into());
    assert!(
        matches!(workspace.prepare(&module(), &vars).await, Err(Error::SecretInFiles { name }) if name == "HCLOUD_TOKEN")
    );
    assert!(!work.path().join(VARIABLES_FILE).exists() && !work.path().join("main.tf").exists());
    for path in ["../x.tf", "/abs.tf", VARIABLES_FILE] {
        assert!(matches!(
            workspace
                .prepare(
                    &[ModuleFile {
                        path,
                        contents: b""
                    }],
                    &variables()
                )
                .await,
            Err(Error::ModulePath(_))
        ));
    }
    assert!(matches!(
        workspace
            .prepare(
                &[ModuleFile {
                    path: "main.tf",
                    contents: SECRET.as_bytes()
                }],
                &variables()
            )
            .await,
        Err(Error::SecretInFiles { .. })
    ));
}
#[tokio::test]
async fn missing_binary_and_bad_output() {
    let fixture = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let binary = fixture.path().join("nope");
    let tofu = Tofu::from_binary(&binary, cache.path());
    assert!(
        matches!(tofu.workspace(work.path(), CredentialEnv::new()).init().await, Err(Error::Spawn { binary: found, .. }) if found == binary)
    );
    let tofu = Tofu::from_binary(fake_tofu(fixture.path(), DEFAULT, "{}"), cache.path());
    assert!(matches!(
        tofu.workspace(work.path(), CredentialEnv::new())
            .outputs()
            .await,
        Err(Error::Output(OutputError::Missing("machines")))
    ));
}
#[tokio::test]
async fn stale_module_files_removed() {
    let fixture = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let tofu = Tofu::from_binary(fake_tofu(fixture.path(), DEFAULT, OUTPUTS), cache.path());
    let workspace = tofu.workspace(work.path(), CredentialEnv::new());
    workspace
        .prepare(
            &[ModuleFile {
                path: "old.tf",
                contents: b"old",
            }],
            &variables(),
        )
        .await
        .unwrap();
    std::fs::write(work.path().join("terraform.tfstate"), "state").unwrap();
    std::fs::create_dir(work.path().join(".terraform")).unwrap();
    workspace
        .prepare(
            &[ModuleFile {
                path: "main.tf",
                contents: b"new",
            }],
            &variables(),
        )
        .await
        .unwrap();
    assert!(!work.path().join("old.tf").exists());
    assert!(work.path().join("main.tf").is_file());
    assert!(work.path().join("terraform.tfstate").is_file());
    assert!(work.path().join(".terraform").is_dir());
}

#[tokio::test]
async fn plan_text_redacts_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let script = fake_tofu(
        temp.path(),
        "if [ \"$1\" = plan ]; then echo 'hcloud-test-token-5f2b9c'; exit 2; fi",
        OUTPUTS,
    );
    let tofu = Tofu::from_binary(script, &temp.path().join("cache"));
    let workspace = tofu.workspace(
        temp.path().join("work"),
        CredentialEnv::new().with("HCLOUD_TOKEN", SECRET),
    );
    workspace.prepare(&module(), &variables()).await.unwrap();
    let (outcome, text) = workspace.plan_text().await.unwrap();
    assert_eq!(outcome, PlanOutcome::Changes);
    assert!(text.contains("<redacted>"));
    assert!(!text.contains(SECRET));
}

#[tokio::test]
async fn set_variables_refuses_credentials_and_keeps_module_files() {
    let temp = tempfile::tempdir().unwrap();
    let tofu = Tofu::from_binary(temp.path().join("missing"), temp.path());
    let work = temp.path().join("work");
    let workspace = tofu.workspace(&work, CredentialEnv::new().with("HCLOUD_TOKEN", SECRET));
    workspace.prepare(&module(), &variables()).await.unwrap();
    let changed = serde_json::json!({"count":3}).as_object().unwrap().clone();
    workspace.set_variables(&changed).await.unwrap();
    assert!(work.join("main.tf").is_file());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(work.join(VARIABLES_FILE)).unwrap()
        )
        .unwrap(),
        serde_json::json!({"count":3})
    );
    let secret = serde_json::json!({"secret":SECRET})
        .as_object()
        .unwrap()
        .clone();
    assert!(matches!(
        workspace.set_variables(&secret).await,
        Err(Error::SecretInFiles { .. })
    ));
    assert!(work.join("main.tf").is_file());
}
