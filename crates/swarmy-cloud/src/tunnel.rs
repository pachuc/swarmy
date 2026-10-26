//! One SSH control master per swarm, with a private record, lock, and log in its directory.
//! `FoundationDB` prefers its advertised port: two services-local swarms cannot both
//! have a usable FDB forward. Fully remote swarms have no such limit.
//! Health proves the local API listener is up, not that the remote API answers.
use crate::{Error, Result, ssh};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    net::{Ipv4Addr, TcpListener},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use swarmy_config::{RemoteNode, RemotePorts};
use swarmy_core::ignore_best_effort;
use tokio::{
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout},
};

pub const REMOTE_API_PORT: u16 = 8742;
pub const RECORD_FILE: &str = "tunnel.json";
pub const LOCK_FILE: &str = "tunnel.lock";
pub const LOG_FILE: &str = "ssh.log";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Service {
    Api,
    FoundationDb,
    Nats,
    ObjectStore,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    Remote,
    LocalServices { object_store: bool },
}
impl Placement {
    #[must_use]
    pub fn services(self) -> Vec<Service> {
        match self {
            Self::Remote => vec![Service::Api],
            Self::LocalServices { object_store } => {
                let mut services = vec![Service::FoundationDb, Service::Nats];
                if object_store {
                    services.push(Service::ObjectStore);
                }
                services
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshTarget {
    pub addresses: Vec<String>,
    pub user: String,
    pub key: PathBuf,
    pub known_hosts: PathBuf,
}
impl SshTarget {
    #[must_use]
    pub fn for_node(node: &RemoteNode) -> Self {
        Self {
            addresses: vec![node.public_ip.clone(), node.private_ip.clone()],
            user: node.ssh_user.clone(),
            key: node.key_path.clone(),
            known_hosts: node.key_path.with_extension("known_hosts"),
        }
    }
}
pub struct Request<'a> {
    pub dir: &'a Path,
    pub target: SshTarget,
    pub placement: Placement,
    pub remote_ports: RemotePorts,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Forward {
    pub service: Service,
    pub local: u16,
    pub remote: u16,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub socket: PathBuf,
    pub pid: u32,
    pub address: String,
    pub addresses: Vec<String>,
    pub forwards: Vec<Forward>,
}
impl Record {
    #[must_use]
    pub fn local_port(&self, service: Service) -> Option<u16> {
        self.forwards
            .iter()
            .find(|forward| forward.service == service)
            .map(|forward| forward.local)
    }
    #[must_use]
    pub fn api_url(&self) -> Option<String> {
        self.local_port(Service::Api)
            .map(|port| format!("http://127.0.0.1:{port}"))
    }
    /// # Errors
    /// Reports unreadable or invalid records.
    pub fn read(dir: &Path) -> Result<Option<Self>> {
        match fs::read(dir.join(RECORD_FILE)) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}
pub struct Opened {
    pub record: Record,
    pub reused: bool,
    pub probe: Duration,
    pub startup: Duration,
}
pub struct Tunnels {
    program: OsString,
}
impl Tunnels {
    #[must_use]
    pub fn ssh() -> Self {
        Self::with_program("ssh")
    }
    #[must_use]
    pub fn with_program(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
        }
    }
    /// # Errors
    /// Reports lock, probe, startup, or I/O failures.
    pub async fn open(&self, request: &Request<'_>) -> Result<Opened> {
        let _lock = lock(request.dir).await?;
        let current = match Record::read(request.dir) {
            Ok(record) => record,
            Err(error) => {
                tracing::debug!(error = %swarmy_core::error_chain(&error), "invalid tunnel record");
                None
            }
        };
        if let Some(record) = &current {
            let matching = record.addresses == request.target.addresses
                && record
                    .forwards
                    .iter()
                    .map(|forward| (forward.service, forward.remote))
                    .collect::<Vec<_>>()
                    == wanted(request);
            if matching && self.healthy(record).await {
                return Ok(Opened {
                    record: record.clone(),
                    reused: true,
                    probe: Duration::ZERO,
                    startup: Duration::ZERO,
                });
            }
        }
        self.clear(request.dir, current.as_ref()).await;
        let options = ssh::options(
            &request.target.user,
            &request.target.key,
            &request.target.known_hosts,
        )?;
        let probing = Instant::now();
        let address =
            ssh::first_reachable(&self.program, &options, &request.target.addresses).await?;
        let probe = probing.elapsed();
        let (record, startup) = self.start(request, &options, address).await?;
        if let Err(error) = swarmy_config::write_private(
            &request.dir.join(RECORD_FILE),
            &serde_json::to_vec_pretty(&record)?,
        ) {
            self.clear(request.dir, Some(&record)).await;
            return Err(error.into());
        }
        Ok(Opened {
            record,
            reused: false,
            probe,
            startup,
        })
    }
    /// # Errors
    /// Reports an unreadable record.
    pub async fn status(&self, dir: &Path) -> Result<Option<Record>> {
        let record = Record::read(dir)?;
        match record {
            Some(record) if self.healthy(&record).await => Ok(Some(record)),
            _ => Ok(None),
        }
    }
    /// # Errors
    /// Reports lock, live-master exit, or I/O failures.
    pub async fn disconnect(&self, dir: &Path) -> Result<bool> {
        let _lock = lock(dir).await?;
        let Some(record) = Record::read(dir)? else {
            return Ok(false);
        };
        if record.socket.exists() {
            let output = ssh::control_socket(&self.program, &record.socket, "exit").await?;
            Error::ensure(
                output.status.success(),
                format!(
                    "cannot stop tunnel: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            )?;
        }
        self.clear(dir, Some(&record)).await;
        Ok(true)
    }
    async fn healthy(&self, record: &Record) -> bool {
        if !ssh::control_socket(&self.program, &record.socket, "check")
            .await
            .is_ok_and(|output| output.status.success())
        {
            return false;
        }
        match record.local_port(Service::Api) {
            Some(port) => timeout(
                Duration::from_secs(2),
                TcpStream::connect((Ipv4Addr::LOCALHOST, port)),
            )
            .await
            .is_ok_and(|result| result.is_ok()),
            None => true,
        }
    }
    async fn clear(&self, dir: &Path, record: Option<&Record>) {
        if let Some(record) = record {
            if record.socket.exists() {
                ignore_best_effort(
                    ssh::control_socket(&self.program, &record.socket, "exit").await,
                    "exit stale SSH master",
                );
            }
            ignore_best_effort(fs::remove_file(&record.socket), "remove control socket");
            if let Some(parent) = record.socket.parent() {
                ignore_best_effort(fs::remove_dir(parent), "remove control directory");
            }
        }
        if dir.join(RECORD_FILE).exists() {
            ignore_best_effort(
                fs::remove_file(dir.join(RECORD_FILE)),
                "remove tunnel record",
            );
        }
    }
    async fn start(
        &self,
        request: &Request<'_>,
        options: &[String],
        address: String,
    ) -> Result<(Record, Duration)> {
        let socket_dir = tempfile::Builder::new()
            .prefix("swarmy-ssh-")
            .tempdir_in("/tmp")?;
        let socket = socket_dir.path().join("control");
        let reservations = wanted(request)
            .into_iter()
            .map(|(service, remote)| {
                let listener = bind_local(service, remote)?;
                Ok((
                    Forward {
                        service,
                        local: listener.local_addr()?.port(),
                        remote,
                    },
                    listener,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let forwards = reservations
            .iter()
            .map(|(forward, _)| forward.clone())
            .collect::<Vec<_>>();
        let log_path = request.dir.join(LOG_FILE);
        swarmy_config::write_private(&log_path, b"")?;
        let mut command = Command::new(&self.program);
        command
            .args(master_args(options, &socket, &forwards, &address))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(OpenOptions::new().append(true).open(&log_path)?)
            .process_group(0)
            .kill_on_drop(false);
        drop(reservations);
        let mut starting = StartingTunnel {
            child: command.spawn().map_err(Error::ssh("start SSH tunnel"))?,
            published: false,
        };
        let record = Record {
            socket,
            pid: starting
                .child
                .id()
                .ok_or_else(|| Error::other("SSH exited before recording pid"))?,
            address,
            addresses: request.target.addresses.clone(),
            forwards,
        };
        let began = Instant::now();
        let result = timeout(Duration::from_secs(15), async {
            loop {
                if let Some(exit) = starting.child.try_wait()? {
                    return Err(Error::other(format!(
                        "SSH exited ({exit}); inspect {}",
                        log_path.display()
                    )));
                }
                if self.healthy(&record).await {
                    return Ok(());
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|source| {
            Error::context(
                source,
                format!(
                    "SSH tunnel startup timed out; inspect {}",
                    log_path.display()
                ),
            )
        })?;
        result?;
        starting.published = true;
        let _socket_dir = socket_dir.keep();
        Ok((record, began.elapsed()))
    }
}
struct StartingTunnel {
    child: Child,
    published: bool,
}
impl Drop for StartingTunnel {
    fn drop(&mut self) {
        if !self.published {
            ignore_best_effort(self.child.start_kill(), "kill failed SSH master");
        }
    }
}
async fn lock(dir: &Path) -> Result<File> {
    swarmy_config::create_private_dir(dir)?;
    let path = dir.join(LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(&path)?;
    let start = Instant::now();
    loop {
        match file.try_lock_exclusive() {
            Ok(()) => return Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if start.elapsed() >= Duration::from_secs(30) {
                    return Err(Error::other(format!(
                        "timed out waiting for {}",
                        path.display()
                    )));
                }
                sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}
fn wanted(request: &Request<'_>) -> Vec<(Service, u16)> {
    request
        .placement
        .services()
        .into_iter()
        .map(|service| {
            (
                service,
                match service {
                    Service::Api => REMOTE_API_PORT,
                    Service::FoundationDb => request.remote_ports.fdb,
                    Service::Nats => request.remote_ports.nats,
                    Service::ObjectStore => request.remote_ports.s3,
                },
            )
        })
        .collect()
}
fn bind_local(service: Service, remote: u16) -> Result<TcpListener> {
    Error::ensure(remote != 0, "remote ports must be nonzero")?;
    let preferred = if service == Service::FoundationDb {
        remote
    } else {
        0
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, preferred))
        .or_else(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)))?;
    if service == Service::FoundationDb && listener.local_addr()?.port() != remote {
        tracing::warn!(
            "FoundationDB coordinator port is occupied; forwarded port will be unusable"
        );
    }
    Ok(listener)
}
fn master_args(
    options: &[String],
    socket: &Path,
    forwards: &[Forward],
    address: &str,
) -> Vec<String> {
    let mut args = options.to_vec();
    args.extend([
        "-N".into(),
        "-M".into(),
        "-S".into(),
        socket.to_string_lossy().into_owned(),
        "-o".into(),
        "ControlPersist=no".into(),
        "-o".into(),
        "ExitOnForwardFailure=yes".into(),
    ]);
    for forward in forwards {
        args.extend([
            "-L".into(),
            format!("127.0.0.1:{}:127.0.0.1:{}", forward.local, forward.remote),
        ]);
    }
    args.push(address.into());
    args
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    struct Fixture {
        root: tempfile::TempDir,
        program: PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let bin = root.path().join("bin");
            fs::create_dir(&bin).unwrap();
            let program = bin.join("ssh");
            std::os::unix::fs::symlink(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-ssh"),
                &program,
            )
            .unwrap();
            Self { root, program }
        }
        fn request(dir: &Path, placement: Placement) -> Request<'_> {
            Request {
                dir,
                target: SshTarget {
                    addresses: vec!["127.0.0.1".into()],
                    user: "test".into(),
                    key: "key".into(),
                    known_hosts: "hosts".into(),
                },
                placement,
                remote_ports: RemotePorts::default(),
            }
        }
        fn masters(&self) -> usize {
            fs::read_to_string(self.program.parent().unwrap().join("ssh-masters"))
                .unwrap()
                .lines()
                .count()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            for entry in fs::read_dir(self.root.path()).unwrap().flatten() {
                if let Ok(Some(record)) = Record::read(&entry.path()) {
                    ignore_best_effort(
                        std::process::Command::new("kill")
                            .arg(record.pid.to_string())
                            .status(),
                        "kill test SSH master",
                    );
                }
            }
        }
    }
    #[test]
    fn placement_selects_forwards() {
        assert_eq!(Placement::Remote.services(), vec![Service::Api]);
        assert_eq!(
            Placement::LocalServices { object_store: true }.services(),
            vec![Service::FoundationDb, Service::Nats, Service::ObjectStore]
        );
        assert_eq!(
            Placement::LocalServices {
                object_store: false
            }
            .services(),
            vec![Service::FoundationDb, Service::Nats]
        );
    }
    #[test]
    fn master_args_forward_loopback_with_batch_mode_and_exit_on_failure() {
        let options = ssh::options("test", Path::new("key"), Path::new("hosts")).unwrap();
        let args = master_args(
            &options,
            Path::new("socket"),
            &[Forward {
                service: Service::Api,
                local: 40123,
                remote: REMOTE_API_PORT,
            }],
            "127.0.0.1",
        );
        for flag in [
            "-N",
            "-M",
            "-S",
            "BatchMode=yes",
            "ControlPersist=no",
            "ExitOnForwardFailure=yes",
            "127.0.0.1:40123:127.0.0.1:8742",
        ] {
            assert!(args.contains(&flag.to_owned()));
        }
        assert_eq!(args.last().unwrap(), "127.0.0.1");
    }
    #[tokio::test]
    async fn open_forwards_only_the_api_on_a_free_port_and_records_it() {
        let fixture = Fixture::new();
        let tunnels = Tunnels::with_program(fixture.program.clone());
        let dir = fixture.root.path().join("one");
        let request = Fixture::request(&dir, Placement::Remote);
        let opened = tunnels.open(&request).await.unwrap();
        assert!(!opened.reused);
        let port = opened.record.local_port(Service::Api).unwrap();
        assert_ne!(port, 0);
        assert_eq!(
            opened.record.forwards,
            vec![Forward {
                service: Service::Api,
                local: port,
                remote: REMOTE_API_PORT
            }]
        );
        assert!(
            TcpStream::connect((Ipv4Addr::LOCALHOST, port))
                .await
                .is_ok()
        );
        assert_eq!(
            Record::read(request.dir).unwrap(),
            Some(opened.record.clone())
        );
        assert_eq!(
            fs::metadata(request.dir.join(RECORD_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(request.dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            opened.record.api_url(),
            Some(format!("http://127.0.0.1:{port}"))
        );
        assert_eq!(fixture.masters(), 1);
        tunnels.disconnect(request.dir).await.unwrap();
    }
    #[tokio::test]
    async fn healthy_tunnel_is_reused_and_disconnect_is_idempotent() {
        let fixture = Fixture::new();
        let tunnels = Tunnels::with_program(fixture.program.clone());
        let dir = fixture.root.path().join("one");
        let request = Fixture::request(&dir, Placement::Remote);
        let first = tunnels.open(&request).await.unwrap();
        let second = tunnels.open(&request).await.unwrap();
        assert!(second.reused);
        assert_eq!(first.record, second.record);
        assert_eq!(second.probe, Duration::ZERO);
        assert_eq!(second.startup, Duration::ZERO);
        assert_eq!(fixture.masters(), 1);
        assert_eq!(
            tunnels.status(request.dir).await.unwrap(),
            Some(first.record.clone())
        );
        assert!(tunnels.disconnect(request.dir).await.unwrap());
        assert!(!request.dir.join(RECORD_FILE).exists());
        assert!(!first.record.socket.exists());
        assert!(!first.record.socket.parent().unwrap().exists());
        let port = first.record.local_port(Service::Api).unwrap();
        swarmy_testkit::eventually("API forward closed", Duration::from_secs(5), || async {
            TcpStream::connect((Ipv4Addr::LOCALHOST, port))
                .await
                .is_err()
                .then_some(())
        })
        .await;
        assert!(!tunnels.disconnect(request.dir).await.unwrap());
        assert!(tunnels.status(request.dir).await.unwrap().is_none());
    }
    #[tokio::test]
    async fn concurrent_opens_start_one_tunnel() {
        let fixture = Fixture::new();
        let tunnels = Tunnels::with_program(fixture.program.clone());
        let dir = fixture.root.path().join("one");
        let request = Fixture::request(&dir, Placement::Remote);
        let (first, second) = tokio::join!(tunnels.open(&request), tunnels.open(&request));
        let first = first.unwrap();
        let second = second.unwrap();
        assert_ne!(first.reused, second.reused);
        assert_eq!(first.record, second.record);
        assert_eq!(fixture.masters(), 1);
        tunnels.disconnect(request.dir).await.unwrap();
    }
    #[test]
    fn foundationdb_falls_back_when_its_port_is_taken() {
        let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = occupied.local_addr().unwrap().port();
        assert_ne!(
            bind_local(Service::FoundationDb, port)
                .unwrap()
                .local_addr()
                .unwrap()
                .port(),
            port
        );
    }
    #[tokio::test]
    async fn failed_start_publishes_nothing() {
        let fixture = Fixture::new();
        fs::write(fixture.program.parent().unwrap().join("fail-start"), b"").unwrap();
        let tunnels = Tunnels::with_program(fixture.program.clone());
        let dir = fixture.root.path().join("one");
        let request = Fixture::request(&dir, Placement::Remote);
        let error = tunnels.open(&request).await.err().unwrap();
        assert!(error.to_string().contains("ssh.log"));
        assert!(!request.dir.join(RECORD_FILE).exists());
        assert!(
            fs::read_to_string(request.dir.join(LOG_FILE))
                .unwrap()
                .contains("boom")
        );
    }
}

#[cfg(test)]
mod extra_tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;

    struct Setup {
        root: tempfile::TempDir,
        program: PathBuf,
        records: Vec<PathBuf>,
    }
    impl Setup {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let program = root.path().join("ssh");
            std::os::unix::fs::symlink(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-ssh"),
                &program,
            )
            .unwrap();
            Self {
                root,
                program,
                records: Vec::new(),
            }
        }
        fn dir(&mut self, name: &str) -> PathBuf {
            let dir = self.root.path().join(name);
            self.records.push(dir.clone());
            dir
        }
        fn request(dir: &Path, placement: Placement) -> Request<'_> {
            Request {
                dir,
                placement,
                target: SshTarget {
                    addresses: vec!["127.0.0.1".into()],
                    user: "test".into(),
                    key: "key".into(),
                    known_hosts: "hosts".into(),
                },
                remote_ports: RemotePorts::default(),
            }
        }
        fn masters(&self) -> usize {
            fs::read_to_string(self.root.path().join("ssh-masters"))
                .unwrap()
                .lines()
                .count()
        }
    }
    impl Drop for Setup {
        fn drop(&mut self) {
            for dir in &self.records {
                if let Ok(Some(record)) = Record::read(dir) {
                    ignore_best_effort(
                        std::process::Command::new("kill")
                            .arg(record.pid.to_string())
                            .status(),
                        "kill test SSH master",
                    );
                }
            }
        }
    }
    #[tokio::test]
    async fn local_services_forward_the_backing_stack() {
        let mut setup = Setup::new();
        let tunnels = Tunnels::with_program(setup.program.clone());
        let dir = setup.dir("with_bucket_forward");
        let reserved = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let fdb = reserved.local_addr().unwrap().port();
        drop(reserved);
        let mut request = Setup::request(&dir, Placement::LocalServices { object_store: true });
        request.remote_ports = RemotePorts {
            fdb,
            nats: 14222,
            s3: 18333,
        };
        let record = tunnels.open(&request).await.unwrap().record;
        assert_eq!(
            record
                .forwards
                .iter()
                .map(|f| f.service)
                .collect::<Vec<_>>(),
            vec![Service::FoundationDb, Service::Nats, Service::ObjectStore]
        );
        assert_eq!(record.local_port(Service::FoundationDb), Some(fdb));
        assert!(record.forwards.iter().all(|forward| forward.local != 0));
        assert!(record.local_port(Service::Api).is_none());
        tunnels.disconnect(&dir).await.unwrap();
        let dir = setup.dir("without_bucket_forward");
        let request = Setup::request(
            &dir,
            Placement::LocalServices {
                object_store: false,
            },
        );
        let record = tunnels.open(&request).await.unwrap().record;
        assert!(record.local_port(Service::ObjectStore).is_none());
        tunnels.disconnect(&dir).await.unwrap();
    }
    #[tokio::test]
    async fn stale_socket_is_replaced() {
        let mut setup = Setup::new();
        let tunnels = Tunnels::with_program(setup.program.clone());
        let dir = setup.dir("one");
        swarmy_config::create_private_dir(&dir).unwrap();
        let missing = setup.root.path().join("missing");
        let stale = Record {
            socket: missing.clone(),
            pid: 0,
            address: "127.0.0.1".into(),
            addresses: vec!["127.0.0.1".into()],
            forwards: vec![Forward {
                service: Service::Api,
                local: 1,
                remote: REMOTE_API_PORT,
            }],
        };
        swarmy_config::write_private(&dir.join(RECORD_FILE), &serde_json::to_vec(&stale).unwrap())
            .unwrap();
        let request = Setup::request(&dir, Placement::Remote);
        let first = tunnels.open(&request).await.unwrap();
        assert!(!first.reused);
        assert_ne!(first.record.socket, missing);
        let local = Setup::request(
            &dir,
            Placement::LocalServices {
                object_store: false,
            },
        );
        let second = tunnels.open(&local).await.unwrap();
        assert!(!second.reused);
        assert_eq!(setup.masters(), 2);
        assert!(!first.record.socket.exists());
        tunnels.disconnect(&dir).await.unwrap();
    }
    #[tokio::test]
    async fn two_swarms_connect_at_once_on_different_ports() {
        let mut setup = Setup::new();
        let tunnels = Tunnels::with_program(setup.program.clone());
        let a = setup.dir("a");
        let b = setup.dir("b");
        let first = Setup::request(&a, Placement::Remote);
        let second = Setup::request(&b, Placement::Remote);
        let (one, two) = tokio::join!(tunnels.open(&first), tunnels.open(&second));
        let one = one.unwrap().record;
        let two = two.unwrap().record;
        assert_ne!(one.local_port(Service::Api), two.local_port(Service::Api));
        for (dir, record) in [(&a, &one), (&b, &two)] {
            assert!(
                TcpStream::connect((
                    Ipv4Addr::LOCALHOST,
                    record.local_port(Service::Api).unwrap()
                ))
                .await
                .is_ok()
            );
            assert!(tunnels.status(dir).await.unwrap().is_some());
            tunnels.disconnect(dir).await.unwrap();
        }
    }
}
