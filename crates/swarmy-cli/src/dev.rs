use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use clap::Subcommand;
use fs2::FileExt;
use swarmy_config::Settings;

#[derive(Subcommand)]
pub(crate) enum Command {
    /// Start the backing stack and supervised services in the background
    Up {
        /// Run this swarmy-core instead of the matching release
        #[arg(long, env = "SWARMY_CORE_BIN", value_name = "PATH")]
        core_bin: Option<PathBuf>,
        /// Do not download and register the release base image when no default image is configured
        #[arg(long)]
        no_image: bool,
    },
    /// Stop the services, supervisor, and backing stack, preserving data
    Down,
    /// Show process ids and uptime
    Status,
    /// Follow all service logs, or one service's log
    Logs {
        #[arg(value_enum)]
        service: Option<crate::local::Service>,
    },
    /// Install `FoundationDB`, NATS, and `SeaweedFS` for the local stack (Linux x86-64)
    InstallTools {
        /// Install root: tools go to PREFIX/bin and PREFIX/lib (default ~/.local)
        #[arg(long)]
        prefix: Option<PathBuf>,
    },
}

struct Layout {
    /// File dev up creates or updates.
    config: PathBuf,
    /// Anchor for relative paths and services' cwd: the project, the
    /// checkout, or the user config directory.
    root: PathBuf,
    /// Supervised service state, `<state_dir>/dev`: pids, logs, lock.
    state: PathBuf,
    /// Backing stack directory passed as `SWARMY_DEV_DIR`.
    stack: PathBuf,
    /// HOME, XDG and dev variables this layout was resolved from.
    environment: BTreeMap<String, String>,
}

impl Layout {
    fn discover() -> Result<Self> {
        let loaded = Settings::load_base()?;
        let cwd = std::env::current_dir()?;
        let environment = [
            "HOME",
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_STATE_HOME",
            "XDG_CACHE_HOME",
            "SWARMY_DEV_DIR",
            "SWARMY_STATE_DIR",
        ]
        .into_iter()
        .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
        .collect();
        Self::resolve(loaded, &cwd, &environment)
    }

    fn resolve(
        loaded: swarmy_config::Loaded,
        cwd: &Path,
        environment: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let repo = cwd
            .ancestors()
            .find(|dir| dir.join("scripts/dev-stack.sh").is_file())
            .map(Path::canonicalize)
            .transpose()?;
        let found = loaded.path.is_some();
        let project = loaded
            .path
            .as_deref()
            .and_then(swarmy_config::project_root)
            .is_some();
        let dirs = || swarmy_config::paths::Dirs::from_env(environment);
        let (config, root) = if let Some(path) = loaded.path {
            (path, loaded.root)
        } else if let Some(repo) = &repo {
            (repo.join(".swarmy/config.toml"), repo.clone())
        } else {
            let config = Settings::user_config_path(environment).context(
                "set HOME (or XDG_CONFIG_HOME) to an absolute path, or run swarmy dev inside a checkout or a directory with .swarmy/config.toml",
            )?;
            let root = config
                .parent()
                .context("the user config path has no directory")?
                .to_owned();
            (config, root)
        };
        let state_dir = if found || environment.contains_key("SWARMY_STATE_DIR") {
            loaded.settings.state_dir
        } else if repo.is_some() {
            root.join(".swarmy")
        } else {
            dirs()?.state
        };
        let state_dir = if state_dir.is_absolute() {
            state_dir
        } else {
            root.join(state_dir)
        };
        let stack = match environment
            .get("SWARMY_DEV_DIR")
            .filter(|dir| !dir.is_empty())
        {
            Some(dir) => {
                let path = PathBuf::from(dir);
                if path.is_absolute() {
                    path
                } else {
                    cwd.join(path)
                }
            }
            None => match &repo {
                Some(repo) => repo.join(".dev"),
                None if project => state_dir.join("stack"),
                None => dirs()?.data.join("stack"),
            },
        };
        Ok(Self {
            config,
            root,
            state: state_dir.join("dev"),
            stack,
            environment: environment.clone(),
        })
    }

    fn lock(&self) -> Result<File> {
        fs::create_dir_all(&self.state)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(self.state.join("lock"))?;
        file.try_lock_exclusive()
            .context("another swarmy dev up/down is in progress")?;
        Ok(file)
    }
}

pub(crate) async fn run(command: Command, json: bool, allow_version_mismatch: bool) -> Result<()> {
    // One match owns every variant. The supervisor is spawned without flags
    // by `dev up`, so it never takes the human-output path; every other arm
    // rejects `--json` and discovers the layout through one helper.
    match command {
        Command::Up { core_bin, no_image } => {
            let layout = local(json)?;
            let _lock = layout.lock()?;
            up(&layout, allow_version_mismatch, core_bin, no_image).await
        }
        Command::Down => {
            let layout = local(json)?;
            let _lock = layout.lock()?;
            crate::local::stop_services(&layout.state).await?;
            crate::local::stack(&layout.stack, "stop").await?;
            println!("services and stack: down");
            Ok(())
        }
        Command::Status => {
            let layout = local(json)?;
            crate::local::status(&layout.stack, &layout.state).await
        }
        Command::InstallTools { prefix } => {
            reject_json(json)?;
            install_tools(prefix.as_deref()).await
        }
        Command::Logs { service } => crate::local::logs(&local(json)?.state, service).await,
    }
}

/// Reject `--json` and discover the dev layout for human-output `dev`
/// commands. `dev` manages local processes with progress lines, so it has no
/// JSON rendering; the explicit rejection keeps `--json dev` from silently
/// printing text.
fn reject_json(json: bool) -> Result<()> {
    if json {
        bail!("--json is not supported for dev commands; run without it for human-readable output");
    }
    Ok(())
}

fn local(json: bool) -> Result<Layout> {
    reject_json(json)?;
    Layout::discover()
}

async fn up(
    layout: &Layout,
    allow_version_mismatch: bool,
    core_bin: Option<PathBuf>,
    no_image: bool,
) -> Result<()> {
    crate::local::ensure_keyring()?;
    let core = crate::core_command::resolve_core(core_bin, None).await?;
    crate::core_command::check_versions(&core, allow_version_mismatch).await?;
    fs::create_dir_all(layout.state.join("logs"))?;
    let survivors = crate::local::survivors(&layout.state);
    if !survivors.is_empty() {
        println!("replacing recorded processes: {}", survivors.join(", "));
    }
    crate::local::stop_services(&layout.state).await?;
    crate::local::ensure_stack(&layout.stack).await?;
    let settings = prepare_settings(layout)?;
    let mut pid = crate::local::launch(&layout.state, &layout.root, &core.path, &settings).await?;
    let api_url = crate::local::api_url(&settings);
    crate::swarm_command::register_dev(
        swarmy_config::paths::Dirs::from_env(&layout.environment)?,
        &api_url,
        &settings.api.token,
    )?;
    if !no_image {
        match settings.selection.default_image.as_deref() {
            None => match crate::image::pull_release_image(
                Some((&api_url, &settings.api.token)),
                &crate::image::image_cache()?,
                swarmy_version::RELEASE_TAG,
                swarmy_cloud::release::Target::host()?,
                false,
            )
            .await
            {
                Ok(Some(_)) => {
                    let image = crate::image::release_image(swarmy_version::RELEASE_TAG);
                    crate::image::set_default_image(&layout.config, &image)?;
                    println!("default image: {image}; restarting services");
                    crate::local::stop_services(&layout.state).await?;
                    let settings = Settings::load()?.settings;
                    pid = crate::local::launch(&layout.state, &layout.root, &core.path, &settings)
                        .await?;
                }
                Ok(None) => println!(
                    "default image: none (development build); build one from a checkout with swarmy image build images/base-ubuntu and set selection.default_image in {}",
                    layout.config.display()
                ),
                Err(error) => eprintln!(
                    "warning: could not register the release base image: {error:#}\nRetry with swarmy image pull base-ubuntu --set-default, then swarmy dev up"
                ),
            },
            Some(default) if crate::image::other_release_image(default) => println!(
                "note: default image {default} is from another release; run swarmy image pull base-ubuntu --set-default and swarmy dev up to use {}",
                crate::image::release_image(swarmy_version::RELEASE_TAG)
            ),
            Some(_) => {}
        }
    }
    crate::local::print_started(&layout.stack, &layout.state, pid)?;
    Ok(())
}

fn prepare_settings(layout: &Layout) -> Result<Settings> {
    let mut settings = if layout.config.exists() {
        Settings::read(&layout.config)?
    } else {
        Settings::default()
    };
    let exports = swarmy_config::parse_exports(&fs::read_to_string(layout.stack.join("env"))?)?;
    settings.apply_environment(&exports)?;
    settings.fill_default_locations(
        &layout.root,
        swarmy_config::project_root(&layout.config).is_some(),
        &layout.environment,
    );
    // The fake provider's files sit beside the service state, <state_dir>/dev.
    let defaults = swarmy_config::Fake::default();
    if settings.fake.script == defaults.script {
        settings.fake.script = layout.state.join("fake.json");
    }
    if settings.fake.call_log == defaults.call_log {
        settings.fake.call_log = layout.state.join("calls.log");
    }
    settings.resolve_paths(&layout.root);
    if settings.api.token.is_empty() {
        // A usable API must reject unauthenticated traffic, including on loopback.
        settings.api.token = ulid::Ulid::generate().to_string();
    }
    if let Some(parent) = layout.config.parent() {
        fs::create_dir_all(parent)?;
    }
    swarmy_config::write_private(&layout.config, settings.to_toml()?.as_bytes())?;
    // Load again so environment overrides win, without persisting those overrides.
    settings = Settings::load()?.settings;
    crate::local::prepare_fake(&settings)?;
    Ok(settings)
}

/// Run the embedded installer with bash, inheriting stdio so download progress shows.
async fn install_tools(prefix: Option<&Path>) -> Result<()> {
    let mut command = crate::local::embedded(
        swarmy_cloud::node_files::INSTALL_DEV_TOOLS,
        "swarmy dev install-tools",
    );
    if let Some(prefix) = prefix {
        command.arg("--prefix").arg(prefix);
    }
    let status = command
        .status()
        .await
        .context("cannot run bash for the backing tool installer")?;
    ensure!(
        status.success(),
        "backing tool installation failed ({status})"
    );
    Ok(())
}

#[cfg(test)]
mod layout_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    fn loaded(path: Option<PathBuf>, root: &Path, state_dir: PathBuf) -> swarmy_config::Loaded {
        let settings = Settings {
            state_dir,
            ..Settings::default()
        };
        swarmy_config::Loaded {
            path,
            root: root.to_owned(),
            settings,
        }
    }
    #[test]
    fn checkout_without_config_keeps_repo_files() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        fs::create_dir_all(repo.join("scripts")).unwrap();
        fs::write(repo.join("scripts/dev-stack.sh"), "").unwrap();
        let cwd = repo.join("crates/x");
        fs::create_dir_all(&cwd).unwrap();
        let layout = Layout::resolve(
            loaded(None, &cwd, cwd.join(".swarmy")),
            &cwd,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(layout.config, repo.join(".swarmy/config.toml"));
        assert_eq!(layout.root, repo);
        assert_eq!(layout.state, repo.join(".swarmy/dev"));
        assert_eq!(layout.stack, repo.join(".dev"));
    }
    #[test]
    fn no_checkout_uses_xdg_directories() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("cwd");
        let home = temp.path().join("home");
        let mut env = BTreeMap::from([("HOME".into(), home.display().to_string())]);
        let make = |env: &BTreeMap<_, _>| {
            Layout::resolve(loaded(None, &cwd, cwd.join(".swarmy")), &cwd, env).unwrap()
        };
        let layout = make(&env);
        assert_eq!(layout.config, home.join(".config/swarmy/config.toml"));
        assert_eq!(layout.root, home.join(".config/swarmy"));
        assert_eq!(layout.state, home.join(".local/state/swarmy/dev"));
        assert_eq!(layout.stack, home.join(".local/share/swarmy/stack"));
        env.insert(
            "XDG_CONFIG_HOME".into(),
            temp.path().join("xdg").display().to_string(),
        );
        for (key, dir) in [
            ("XDG_DATA_HOME", "data"),
            ("XDG_STATE_HOME", "state"),
            ("XDG_CACHE_HOME", "cache"),
        ] {
            env.insert(key.into(), temp.path().join(dir).display().to_string());
        }
        let layout = make(&env);
        assert_eq!(layout.config, temp.path().join("xdg/swarmy/config.toml"));
        assert_eq!(layout.root, temp.path().join("xdg/swarmy"));
        assert_eq!(layout.state, temp.path().join("state/swarmy/dev"));
        assert_eq!(layout.stack, temp.path().join("data/swarmy/stack"));
    }
    #[test]
    fn discovered_config_wins_and_supplies_state_dir() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path().join("cwd");
        let root = temp.path().join("project");
        let state = temp.path().join("custom");
        let layout = Layout::resolve(
            loaded(Some(root.join(".swarmy/config.toml")), &root, state.clone()),
            &cwd,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(layout.root, root);
        assert_eq!(layout.state, state.join("dev"));
        assert_eq!(layout.stack, state.join("stack"));
    }
    #[test]
    fn user_config_keeps_the_stack_in_the_data_dir() {
        let temp = tempfile::tempdir().unwrap();
        let xdg = temp.path().join("config");
        let root = xdg.join("swarmy");
        let custom = temp.path().join("custom");
        let env = BTreeMap::from([
            ("XDG_CONFIG_HOME".into(), xdg.display().to_string()),
            (
                "XDG_DATA_HOME".into(),
                temp.path().join("data").display().to_string(),
            ),
            (
                "XDG_STATE_HOME".into(),
                temp.path().join("state").display().to_string(),
            ),
            (
                "XDG_CACHE_HOME".into(),
                temp.path().join("cache").display().to_string(),
            ),
        ]);
        let layout = Layout::resolve(
            loaded(Some(root.join("config.toml")), &root, custom.clone()),
            temp.path(),
            &env,
        )
        .unwrap();
        assert_eq!(layout.root, root);
        assert_eq!(layout.state, custom.join("dev"));
        assert_eq!(layout.stack, temp.path().join("data/swarmy/stack"));
    }
    #[test]
    fn state_dir_env_applies_without_a_file() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        let state = cwd.join("other");
        let env = BTreeMap::from([
            ("HOME".into(), cwd.display().to_string()),
            ("SWARMY_STATE_DIR".into(), state.display().to_string()),
        ]);
        assert_eq!(
            Layout::resolve(loaded(None, cwd, state.clone()), cwd, &env)
                .unwrap()
                .state,
            state.join("dev")
        );
    }
    #[test]
    fn dev_dir_override_is_absolute() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        let env = BTreeMap::from([
            ("HOME".into(), cwd.display().to_string()),
            ("SWARMY_DEV_DIR".into(), "elsewhere".into()),
        ]);
        assert_eq!(
            Layout::resolve(loaded(None, cwd, cwd.join(".swarmy")), cwd, &env)
                .unwrap()
                .stack,
            cwd.join("elsewhere")
        );
    }
    #[test]
    fn empty_dev_dir_uses_default_stack() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        let env = BTreeMap::from([
            ("HOME".into(), cwd.display().to_string()),
            ("SWARMY_DEV_DIR".into(), String::new()),
        ]);
        assert_eq!(
            Layout::resolve(loaded(None, cwd, cwd.join(".swarmy")), cwd, &env)
                .unwrap()
                .stack,
            cwd.join(".local/share/swarmy/stack")
        );
    }
    #[test]
    fn no_home_no_checkout_no_config_errors() {
        let temp = tempfile::tempdir().unwrap();
        let cwd = temp.path();
        let error = Layout::resolve(
            loaded(None, cwd, cwd.join(".swarmy")),
            cwd,
            &BTreeMap::new(),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("HOME"));
    }
}
