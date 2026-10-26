//! One SSH control master per swarm, with a private record, lock, and log in its directory.
//! FoundationDB prefers its advertised port: two services-local swarms cannot both
//! have a usable FDB forward. Fully remote swarms have no such limit.
//! Health proves the local API listener is up, not that the remote API answers.
use crate::{Error, Result, ssh};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    ffi::{OsStr, OsString},
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
        swarmy_config::write_private(
            &request.dir.join(RECORD_FILE),
            &serde_json::to_vec_pretty(&record)?,
        )?;
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
