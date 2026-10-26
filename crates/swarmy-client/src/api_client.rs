//! Resolve one API endpoint for local or selected remote commands.
use crate::{Client, Error};

/// The selected API, either directly reachable or requiring an SSH tunnel.
pub enum Api {
    Direct { url: String, token: String },
    Tunnel { swarm: String, dir: std::path::PathBuf, host: Box<swarmy_config::swarm::RecordedHost>, token: String },
}
impl std::fmt::Debug for Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Direct { url, .. } => f.debug_struct("Direct").field("url", url).finish_non_exhaustive(),
            Self::Tunnel { swarm, dir, host, .. } => f.debug_struct("Tunnel").field("swarm", swarm).field("dir", dir).field("host", host).finish_non_exhaustive(),
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
pub fn select_api(registry: &swarmy_config::swarm::Registry, target: swarmy_config::swarm::Target) -> Result<Api, Error> {
    use swarmy_config::swarm::Target;
    let Target::Swarm(swarm) = target else {
        let Target::Endpoint { url, token } = target else { unreachable!() };
        return Ok(Api::Direct { url, token });
    };
    let name = swarm.name;
    let (url, token) = swarm.record.as_ref().map_or((None, None), |record| (
        record.api.url.as_ref().filter(|s| !s.is_empty()).cloned(),
        record.api.token.as_ref().filter(|s| !s.is_empty()).cloned(),
    ));
    if let (Some(url), Some(token)) = (&url, &token) {
        return Ok(Api::Direct { url: url.clone(), token: token.clone() });
    }
    let Some(spec) = swarm.spec else { return Err(Error::SwarmIncomplete { swarm: name }); };
    if url.is_some() || token.is_none() {
        return Err(Error::SwarmNotUp { swarm: name });
    }
    let group = spec.placement.services;
    let host = swarm.record.as_ref().and_then(|record| record.hosts.get(&group)).and_then(|hosts| hosts.first()).ok_or_else(|| Error::SwarmNoHost { swarm: name.clone(), group: group.clone() })?;
    Ok(Api::Tunnel { dir: registry.swarm_dir(&name)?.join("tunnel"), swarm: name, host: Box::new(host.clone()), token: token.expect("checked above") })
}
/// Connect to an explicit API.
/// # Errors
/// Returns `MissingToken` or `InvalidEndpoint` for invalid credentials or URL.
pub fn connect_to(url: &str, token: String) -> Result<(Client, String), Error> {
    if token.is_empty() { return Err(Error::MissingToken); }
    let client = Client::new(url, token).map_err(|error| match error {
        Error::Url(source) => Error::InvalidEndpoint { endpoint: url.into(), source },
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
