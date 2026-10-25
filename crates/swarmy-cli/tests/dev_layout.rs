#![cfg(target_os = "linux")]
#![deny(clippy::disallowed_methods)]
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use swarmy_testkit::FakeStack;
const CLI: &str = env!("CARGO_BIN_EXE_swarmy");
struct Fixture {
    root: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        for dir in ["home", "config", "bin", "a", "b", "c"] {
            fs::create_dir(root.path().join(dir)).unwrap();
        }
        let core = root.path().join("bin/swarmy-core");
        fs::write(&core, format!("#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'swarmy-core {}'; exit 0; fi\ncase \"$1\" in scheduler) echo 'scheduler started';; worker) echo 'worker ready';; gateway) echo 'gateway ready';; api) echo 'api ready';; esac\nexec /bin/sleep 600\n", swarmy_version::IDENTITY)).unwrap();
        fs::set_permissions(core, fs::Permissions::from_mode(0o755)).unwrap();
        Self { root }
    }
    fn path(&self, path: &str) -> PathBuf {
        self.root.path().join(path)
    }
    fn run(&self, cwd: &Path, args: &[&str], stack: Option<&Path>) -> Output {
        let mut cmd = Command::new(CLI);
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("SWARMY_") {
                cmd.env_remove(key);
            }
        }
        if let Some(stack) = stack {
            cmd.env("SWARMY_DEV_DIR", stack);
        }
        cmd.args(args)
            .current_dir(cwd)
            .env("HOME", self.path("home"))
            .env("XDG_CONFIG_HOME", self.path("config"))
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.path("bin").display()),
            )
            .output()
            .unwrap()
    }
    fn ok(&self, cwd: &Path, args: &[&str], stack: Option<&Path>) -> String {
        let out = self.run(cwd, args, stack);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.run(&self.path("a"), &["dev", "down"], None);
    }
}
#[test]
fn dev_up_status_down_work_without_a_checkout() {
    let f = Fixture::new();
    let mut stack = FakeStack::start(&f.path("home/.swarmy/stack"));
    let up = f.ok(&f.path("a"), &["dev", "up"], None);
    assert!(
        up.contains(&format!(
            "stack: ready (fdb={}, nats={}, seaweed={})",
            stack.pid("fdb"),
            stack.pid("nats"),
            stack.pid("seaweed")
        )),
        "{up}"
    );
    assert!(up.contains("api: ready"));
    let config = f.path("config/swarmy/config.toml");
    let settings = swarmy_config::Settings::read(&config).unwrap();
    assert_eq!(settings.state_dir, f.path("home/.swarmy"));
    assert_eq!(
        settings.store.cluster_file,
        f.path("home/.swarmy/stack/fdb.cluster")
    );
    assert!(!settings.api.token.is_empty());
    assert!(f.path("home/.swarmy/dev/supervisor.pid").exists());
    assert!(!f.path("a/.swarmy").exists() && !f.path("a/.dev").exists());
    let status = f.ok(&f.path("b"), &["dev", "status"], None);
    assert!(
        status.contains(&format!("stack: fdb pid {}", stack.pid("fdb"))),
        "{status}"
    );
    assert!(status.contains("api: up"), "{status}");
    let down = f.ok(&f.path("c"), &["dev", "down"], None);
    assert!(down.contains("services and stack: down"), "{down}");
    assert!(!stack.running());
    assert!(!f.path("home/.swarmy/stack/fdb.pid").exists());
    assert!(!f.path("home/.swarmy/dev/supervisor.pid").exists());
}
#[test]
fn dev_up_in_a_checkout_keeps_repo_files() {
    let f = Fixture::new();
    let repo = f.path("repo");
    fs::create_dir_all(repo.join("scripts")).unwrap();
    fs::create_dir_all(repo.join("sub")).unwrap();
    fs::write(
        repo.join("scripts/dev-stack.sh"),
        format!("#!/bin/sh\ntouch {}\n", repo.join("marker-ran").display()),
    )
    .unwrap();
    let mut stack = FakeStack::start(&repo.join(".dev"));
    f.ok(&repo.join("sub"), &["dev", "up"], None);
    assert!(repo.join(".swarmy/config.toml").exists());
    assert!(repo.join(".swarmy/dev/supervisor.pid").exists());
    assert!(!f.path("config/swarmy").exists());
    assert!(!repo.join("marker-ran").exists());
    f.ok(&repo.join("sub"), &["dev", "down"], None);
    assert!(!stack.running());
}
#[test]
fn dev_dir_env_attaches_to_an_existing_stack() {
    let f = Fixture::new();
    let stack = FakeStack::start(&f.path("elsewhere"));
    let status = f.ok(&f.path("a"), &["dev", "status"], Some(stack.dir()));
    assert!(
        status.contains(&format!("stack: fdb pid {}", stack.pid("fdb"))),
        "{status}"
    );
}
