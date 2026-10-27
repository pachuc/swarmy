use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use swarmy_testkit::release::{fake_release, host_target, serve_files};
pub(crate) struct Fixture {
    pub(crate) root: tempfile::TempDir,
    release_url: String,
    pub(crate) key: PathBuf,
}
impl Fixture {
    pub(crate) fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path();
        fs::create_dir_all(path.join("bin")).unwrap();
        fs::create_dir_all(path.join("config/swarmy")).unwrap();
        fs::write(
            path.join("config/swarmy/config.toml"),
            "[selection]\nprovider = 'fake'\n",
        )
        .unwrap();
        let key = path.join("bootstrap");
        assert!(
            Command::new("ssh-keygen")
                .args(["-q", "-t", "ed25519", "-N", "", "-f"])
                .arg(&key)
                .status()
                .unwrap()
                .success()
        );
        for program in ["ssh", "rsync", "ssh-keygen"] {
            std::os::unix::fs::symlink(
                Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures"))
                    .join(format!("fake-host-{program}")),
                path.join("bin").join(program),
            )
            .unwrap();
        }
        let addr = serve_files(fake_release(
            swarmy_version::RELEASE_TAG,
            &host_target(),
            "#!/bin/sh\n",
        ));
        Self {
            root,
            release_url: format!("http://{addr}"),
            key,
        }
    }
    pub(crate) fn path(&self) -> &Path {
        self.root.path()
    }
    pub(crate) fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_swarmy"))
            .env_clear()
            .current_dir(self.path())
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.path().join("bin").display()),
            )
            .env("HOME", self.path().join("home"))
            .env("XDG_CONFIG_HOME", self.path().join("config"))
            .env("XDG_DATA_HOME", self.path().join("data"))
            .env("XDG_STATE_HOME", self.path().join("state"))
            .env("XDG_CACHE_HOME", self.path().join("cache"))
            .env("SWARMY_RELEASE_BASE_URL", &self.release_url)
            .env("SWARMY_TOFU_BIN", self.path().join("bin/tofu"))
            .env("RUST_LOG", "trace")
            .args(args)
            .output()
            .unwrap()
    }
    pub(crate) fn ok(&self, args: &[&str]) -> Output {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
    pub(crate) fn create(&self) {
        self.ok(&[
            "swarm",
            "create",
            "box",
            "--host",
            "203.0.113.10",
            "--ssh-key",
            self.key.to_str().unwrap(),
            "--local-storage",
            "dir:/srv/swarmy-local",
        ]);
    }
}
