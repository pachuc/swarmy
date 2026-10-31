//! Select a client API from the swarm registry and its recorded endpoint or tunnel.
use crate::{Client, Error};

/// The selected API, either directly reachable or requiring an SSH tunnel.
pub enum Api {
    Direct {
        url: String,
        token: String,
    },
    Tunnel {
        swarm: String,
        dir: std::path::PathBuf,
        host: Box<swarmy_config::swarm::RecordedHost>,
        token: String,
    },
}
impl std::fmt::Debug for Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Direct { url, .. } => f
                .debug_struct("Direct")
                .field("url", url)
                .finish_non_exhaustive(),
            Self::Tunnel {
                swarm, dir, host, ..
            } => f
                .debug_struct("Tunnel")
                .field("swarm", swarm)
                .field("dir", dir)
                .field("host", host)
                .finish_non_exhaustive(),
        }
    }
}
/// # Errors
/// Returns target resolution and selection errors.
pub fn resolve(flag: Option<&str>) -> Result<Api, Error> {
    let (registry, target) = swarmy_config::swarm::resolve_process(flag)?;
    select_api(&registry, target)
}
/// # Errors
/// Returns errors for incomplete swarm records and missing services hosts.
pub fn select_api(
    registry: &swarmy_config::swarm::Registry,
    target: swarmy_config::swarm::Target,
) -> Result<Api, Error> {
    use swarmy_config::swarm::Target;
    let swarm = match target {
        Target::Endpoint { url, token } => return Ok(Api::Direct { url, token }),
        Target::Swarm(swarm) => swarm,
    };
    let name = swarm.name;
    let (url, token) = swarm.record.as_ref().map_or((None, None), |record| {
        (
            record.api.url.as_ref().filter(|s| !s.is_empty()).cloned(),
            record.api.token.as_ref().filter(|s| !s.is_empty()).cloned(),
        )
    });
    if let (Some(url), Some(token)) = (&url, &token) {
        return Ok(Api::Direct {
            url: url.clone(),
            token: token.clone(),
        });
    }
    let Some(spec) = swarm.spec else {
        return Err(Error::SwarmIncomplete { swarm: name });
    };
    if url.is_some() {
        return Err(Error::SwarmNotUp { swarm: name });
    }
    let Some(token) = token else {
        return Err(Error::SwarmNotUp { swarm: name });
    };
    let group = spec.placement.services;
    let host = swarm
        .record
        .as_ref()
        .and_then(|record| record.hosts.get(&group))
        .and_then(|hosts| hosts.first())
        .ok_or_else(|| Error::SwarmNoHost {
            swarm: name.clone(),
            group: group.clone(),
        })?;
    Ok(Api::Tunnel {
        dir: registry.swarm_dir(&name)?.join("tunnel"),
        swarm: name,
        host: Box::new(host.clone()),
        token,
    })
}
/// Connect to an explicit API.
/// # Errors
/// Returns `MissingToken` or `InvalidEndpoint` for invalid credentials or URL.
pub fn connect_to(url: &str, token: String) -> Result<(Client, String), Error> {
    if token.is_empty() {
        return Err(Error::MissingToken);
    }
    let client = Client::new(url, token).map_err(|error| match error {
        Error::Url(source) => Error::InvalidEndpoint {
            endpoint: url.into(),
            source,
        },
        error => error,
    })?;
    Ok((client, url.into()))
}
/// Resolve and connect when the API is directly reachable.
/// # Errors
/// Returns resolution, connection, or `NeedsTunnel` errors.
pub fn connect(flag: Option<&str>) -> Result<(Client, String), Error> {
    match resolve(flag)? {
        Api::Direct { url, token } => connect_to(&url, token),
        Api::Tunnel { swarm, .. } => Err(Error::NeedsTunnel { swarm }),
    }
}

/// Include the endpoint in an API failure.
#[must_use]
pub fn api_error(error: Error, endpoint: &str) -> Error {
    Error::Endpoint {
        endpoint: endpoint.into(),
        source: Box::new(error),
    }
}

/// Apply the standard API request timeout.
///
/// # Errors
/// Fails if the request times out or the API rejects it.
pub async fn call<T>(
    endpoint: &str,
    future: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    crate::timed_call(future)
        .await
        .map_err(|error| api_error(error, endpoint))
}

/// Call the API with an explicit timeout. Image uploads use
/// [`crate::upload_timeout`], sized from the body on disk, because
/// the server chunks and stores the whole image before answering.
/// # Errors
/// Fails if the request times out or the API rejects it.
pub async fn call_with_timeout<T>(
    endpoint: &str,
    timeout: std::time::Duration,
    future: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| api_error(Error::Timeout, endpoint))?
        .map_err(|error| api_error(error, endpoint))
}

/// Upload a file with a timeout proportional to its size on disk.
/// # Errors
/// Fails if the file cannot be inspected or the request fails.
pub async fn call_upload<T>(
    endpoint: &str,
    file: &std::path::Path,
    future: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    let size = std::fs::metadata(file)
        .map_err(|source| Error::UploadSize {
            path: file.into(),
            source,
        })?
        .len();
    call_with_timeout(endpoint, crate::upload_timeout(size), future).await
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use swarmy_config::{
        paths::Dirs,
        swarm::{Registry, SwarmApi, SwarmRecord, SwarmSpec},
    };
    fn registry() -> (tempfile::TempDir, Registry) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let registry = Registry::new(Dirs {
            config: root.join("config"),
            data: root.join("data"),
            state: root.join("state"),
            cache: root.join("cache"),
        });
        (tmp, registry)
    }
    fn spec(name: &str) -> SwarmSpec {
        SwarmSpec::parse(&format!("name = '{name}'\nlifecycle = 'persistent'\n[groups.box]\nmodule = 'existing'\n[[groups.box.hosts]]\naddress = '203.0.113.10'\nssh_user = 'root'\nssh_key = '/keys/box'\n[placement]\nstack = 'box'\nservices = 'box'\nnodes = ['box']\n")).unwrap()
    }
    fn record(name: &str, url: Option<&str>, token: Option<&str>, host: bool) -> SwarmRecord {
        SwarmRecord {
            bucket_keys: None,
            name: name.into(),
            created_at: "2026-10-26T00:00:00Z".parse().unwrap(),
            api: SwarmApi {
                url: url.map(str::to_owned),
                token: token.map(str::to_owned),
            },
            hosts: if host {
                [(
                    "box".into(),
                    vec![swarmy_config::swarm::RecordedHost {
                        provisioned: false,
                        name: "box".into(),
                        public_address: "203.0.113.10".into(),
                        private_address: None,
                        ssh_user: "root".into(),
                        ssh_key: "/keys/box".into(),
                    }],
                )]
                .into()
            } else {
                std::collections::BTreeMap::default()
            },
            tofu_state: None,
            tofu_bucket: None,
            tofu_bucket_endpoint: None,
            tofu_bucket_region: None,
            tofu_networks: BTreeMap::new(),
        }
    }
    #[test]
    fn every_selection_rule() {
        let (_tmp, r) = registry();
        let endpoint = select_api(
            &r,
            swarmy_config::swarm::Target::Endpoint {
                url: "https://example.test".into(),
                token: "secret".into(),
            },
        )
        .unwrap();
        assert!(matches!(endpoint, Api::Direct { .. }));
        assert!(!format!("{endpoint:?}").contains("secret"));
        r.attach("attached", "https://example.test", "secret")
            .unwrap();
        assert!(matches!(
            select_api(
                &r,
                swarmy_config::swarm::Target::Swarm(Box::new(r.read("attached").unwrap()))
            )
            .unwrap(),
            Api::Direct { .. }
        ));
        assert!(matches!(
            connect_to("http://localhost", String::new()),
            Err(Error::MissingToken)
        ));
        for (name, url, token, host) in [
            (
                "direct",
                Some("https://example.test"),
                Some("secret"),
                false,
            ),
            ("tunnel", None, Some("secret"), true),
            ("no-record", None, None, false),
            ("no-token", None, None, true),
            ("url-no-token", Some("https://example.test"), None, true),
            ("no-host", None, Some("secret"), false),
        ] {
            r.write_spec(&spec(name)).unwrap();
            if name != "no-record" {
                r.write_record(&record(name, url, token, host)).unwrap();
            }
            let result = select_api(
                &r,
                swarmy_config::swarm::Target::Swarm(Box::new(r.read(name).unwrap())),
            );
            match name {
                "direct" => assert!(matches!(result.unwrap(), Api::Direct { .. })),
                "tunnel" => {
                    let api = result.unwrap();
                    assert!(!format!("{api:?}").contains("secret"));
                    if let Api::Tunnel { host, dir, .. } = api {
                        assert_eq!(host.public_address, "203.0.113.10");
                        assert_eq!(dir, r.swarm_dir(name).unwrap().join("tunnel"));
                    } else {
                        panic!("expected tunnel");
                    }
                }
                "no-host" => assert!(matches!(result, Err(Error::SwarmNoHost { .. }))),
                _ => assert!(matches!(result, Err(Error::SwarmNotUp { .. }))),
            }
        }
        r.write_record(&record("incomplete", None, Some("secret"), false))
            .unwrap();
        assert!(matches!(
            select_api(
                &r,
                swarmy_config::swarm::Target::Swarm(Box::new(r.read("incomplete").unwrap()))
            ),
            Err(Error::SwarmIncomplete { .. })
        ));
    }
}
