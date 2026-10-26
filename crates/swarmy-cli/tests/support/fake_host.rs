use std::{
    fs,
    os::unix::fs::PermissionsExt,
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
        let ssh = format!(
            r#"#!/bin/bash
last="${{@: -1}}"
printf '%s\n' "$last" >> '{}/ssh-commands'
case "$last" in
  true) exit 0 ;;
  'bash -s') file='{}/piped-'$(ls '{}/piped-'* 2>/dev/null | wc -l); cat > "$file";
    if grep -q '^list_installed_control_units$' "$file"; then printf 'swarmy-api.service\n'; else printf '/home/fake\n'; fi ;;
  'uname -m') /usr/bin/uname -m ;;
  *'swarmy-core --version'*) printf 'swarmy-core test\n' ;;
  *'journalctl --unit swarmyd --follow --no-pager --lines 100'*) if [ -e '{}/fail-journal' ]; then echo journal-failed >&2; exit 23; fi; echo remote-output ;;
  *'remote-upgrade.sh'*) if [ -e '{}/fail-upgrade-primary' ] && [[ " $* " == *203.0.113.10* ]]; then echo upgrade-failed >&2; exit 23; fi; printf '{{"node":"","changed":[],"restarted":[],"elapsed_seconds":0}}\n' ;;
  'cat ~swarmy/swarmy/.dev/fdb.cluster') printf 'test:test@127.0.0.1:4500\n' ;;
  *'cat ~swarmy/swarmy/.swarmy/tunnel-key.pub') printf 'ssh-ed25519 AAAATEST test\n' ;;
  'cat /etc/ssh/ssh_host_ed25519_key.pub') printf 'ssh-ed25519 AAAATEST test\n' ;;
  'cat ~swarmy/swarmy/.swarmy/config.toml') printf '[api]\ntoken = "node-token"\n' ;;
  'sudo -n sh -c '*) cat >/dev/null ;;
  *) echo remote-output ;;
esac
"#,
            path.display(),
            path.display(),
            path.display(),
            path.display(),
            path.display()
        );
        fs::write(path.join("bin/ssh"), ssh).unwrap();
        fs::write(path.join("bin/rsync"), "#!/bin/bash\nexit 0\n").unwrap();
        for program in ["ssh", "rsync"] {
            fs::set_permissions(
                path.join("bin").join(program),
                fs::Permissions::from_mode(0o755),
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
