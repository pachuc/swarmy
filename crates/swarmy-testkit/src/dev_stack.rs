//! A port-free backing stack fixture for CLI tests on Linux.
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

/// Three sleep children with real Linux process identities and a stack environment.
pub struct FakeStack {
    dir: PathBuf,
    children: Vec<(&'static str, Child)>,
}

impl FakeStack {
    /// Start the fixture at `dir`, without binding any ports.
    ///
    /// # Panics
    /// Panics if a child cannot start or its Linux process identity cannot be recorded.
    #[must_use]
    pub fn start(dir: &Path) -> Self {
        fs::create_dir_all(dir).expect("create fake stack directory");
        let mut children = Vec::new();
        for name in ["fdb", "nats", "seaweed"] {
            let child = Command::new("/bin/sleep")
                .arg("600")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn fake stack child");
            let pid = child.id();
            let stat = fs::read_to_string(format!("/proc/{pid}/stat")).expect("read process stat");
            let (_, fields) = stat.rsplit_once(") ").expect("process stat has command");
            let start_time = fields
                .split_whitespace()
                .nth(19)
                .expect("process stat has start time");
            fs::write(
                dir.join(format!("{name}.pid")),
                format!("{pid} {start_time}\n"),
            )
            .expect("write stack pid");
            children.push((name, child));
        }
        fs::write(dir.join("fdb.cluster"), "dev:dev@127.0.0.1:4500\n").expect("write cluster file");
        fs::write(
            dir.join("env"),
            format!(
                "export SWARMY_FDB_CLUSTER_FILE={}\n",
                dir.join("fdb.cluster").display()
            ),
        )
        .expect("write stack environment");
        Self {
            dir: dir.to_owned(),
            children,
        }
    }

    /// The stack directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The PID of a named backing child.
    ///
    /// # Panics
    /// Panics if `name` is not one of the three backing services.
    #[must_use]
    pub fn pid(&self, name: &str) -> u32 {
        self.children
            .iter()
            .find(|(entry, _)| *entry == name)
            .expect("named fake stack service exists")
            .1
            .id()
    }

    /// True while every recorded process is alive; reaps stopped children.
    ///
    /// # Panics
    /// Panics if checking a child process fails.
    pub fn running(&mut self) -> bool {
        self.children
            .iter_mut()
            .all(|(_, child)| child.try_wait().expect("wait on stack child").is_none())
    }
}

impl Drop for FakeStack {
    fn drop(&mut self) {
        for (_, child) in &mut self.children {
            if child.try_wait().expect("wait on stack child").is_none() {
                child.kill().expect("kill remaining stack child");
            }
            child.wait().expect("reap stack child");
        }
    }
}
