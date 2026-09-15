use std::{fs, path::Path, process::Stdio, time::Duration};

use anyhow::{Result, ensure};
use tokio::{
    process::Command,
    time::{Instant, sleep},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Identity {
    pub pid: u32,
    pub start: u64,
}

impl Identity {
    pub fn current(pid: u32) -> Option<Self> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        Self::parse(pid, &stat)
    }

    fn parse(pid: u32, content: &str) -> Option<Self> {
        // The executable name may contain spaces and parentheses.
        let (_, fields) = content.rsplit_once(") ")?;
        let mut fields = fields.split_whitespace();
        if matches!(fields.next()?, "Z" | "X") {
            return None;
        }
        let start = fields.nth(18)?.parse().ok()?;
        Some(Self { pid, start })
    }

    pub fn read(path: &Path) -> Option<Self> {
        let content = fs::read_to_string(path).ok()?;
        let mut words = content.split_whitespace();
        let recorded = Self {
            pid: words.next()?.parse().ok()?,
            start: words.next()?.parse().ok()?,
        };
        (Self::current(recorded.pid) == Some(recorded)).then_some(recorded)
    }

    pub fn save(self, path: &Path) -> Result<()> {
        super::write_private(path, &format!("{} {}\n", self.pid, self.start))
    }

    pub async fn signal(self, signal: &str) -> Result<()> {
        if Self::current(self.pid) == Some(self) {
            let status = Command::new("kill")
                .args([signal, &self.pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?;
            ensure!(
                status.success() || Self::current(self.pid) != Some(self),
                "cannot signal pid {}",
                self.pid
            );
        }
        Ok(())
    }
}

pub async fn stop(path: &Path, grace: Duration) -> Result<()> {
    if let Some(identity) = Identity::read(path) {
        identity.signal("-INT").await?;
        let deadline = Instant::now() + grace;
        while Identity::current(identity.pid) == Some(identity) {
            if Instant::now() >= deadline {
                identity.signal("-KILL").await?;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while Identity::current(identity.pid) == Some(identity) {
            ensure!(
                Instant::now() < deadline,
                "pid {} did not stop; retained {}",
                identity.pid,
                path.display()
            );
            sleep(Duration::from_millis(50)).await;
        }
    }
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub async fn clock_ticks() -> Result<u64> {
    let output = Command::new("getconf").arg("CLK_TCK").output().await?;
    let ticks = String::from_utf8(output.stdout)?.trim().parse()?;
    ensure!(ticks > 0, "invalid CLK_TCK");
    Ok(ticks)
}

pub fn uptime(identity: Identity, ticks: u64) -> Result<u64> {
    // Some container runtimes virtualize /proc/uptime separately from process
    // start times. Read the kernel clock that also timestamps /proc/PID/stat.
    let uptime =
        u64::try_from(rustix::time::clock_gettime(rustix::time::ClockId::Boottime).tv_sec)?;
    Ok(uptime.saturating_sub(identity.start / ticks))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_checks_start_time_and_ignores_zombies() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let identity = Identity::current(std::process::id()).unwrap();
        identity.save(file.path()).unwrap();
        assert!(Identity::read(file.path()) == Some(identity));
        Identity {
            start: identity.start + 1,
            ..identity
        }
        .save(file.path())
        .unwrap();
        assert!(Identity::read(file.path()).is_none());
        let fields = format!("42 (a name (with) spaces) S {} 123 0", "0 ".repeat(18));
        assert_eq!(Identity::parse(42, &fields).unwrap().start, 123);
        assert!(Identity::parse(42, &fields.replace(") S ", ") Z ")).is_none());
    }
}
