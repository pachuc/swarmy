//! Precedence for choosing a registered swarm or an unregistered API.
use super::{Registry, Swarm};
use crate::Error;
use std::{collections::BTreeMap, fmt};
pub enum Target {
    Swarm(Box<Swarm>),
    Endpoint { url: String, token: String },
}
impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Swarm(s) => f.debug_tuple("Swarm").field(&s.spec.name).finish(),
            Self::Endpoint { url, .. } => f
                .debug_struct("Endpoint")
                .field("url", url)
                .field("token", &"..redacted..")
                .finish(),
        }
    }
}
/// # Errors
/// Returns `UnknownSwarm`, `StaleCurrent`, `IncompleteEndpoint`, `NoSwarm`, or registry errors.
pub fn resolve(
    registry: &Registry,
    flag: Option<&str>,
    environment: &BTreeMap<String, String>,
) -> Result<Target, Error> {
    if let Some(name) = flag {
        return Ok(Target::Swarm(Box::new(registry.read(name)?)));
    }
    let get = |key: &str| environment.get(key).filter(|v| !v.is_empty());
    if let Some(name) = get("SWARMY_SWARM") {
        return Ok(Target::Swarm(Box::new(registry.read(name)?)));
    }
    if let Some(url) = get("SWARMY_API_URL") {
        return Ok(Target::Endpoint {
            url: url.clone(),
            token: get("SWARMY_API_TOKEN")
                .ok_or(Error::IncompleteEndpoint)?
                .clone(),
        });
    }
    // A token without a URL belongs to the API service itself; ignore it here.
    if let Some(name) = registry.current()? {
        return registry
            .read(&name)
            .map(|s| Target::Swarm(Box::new(s)))
            .map_err(|e| {
                if matches!(e, Error::UnknownSwarm(_)) {
                    Error::StaleCurrent(name)
                } else {
                    e
                }
            });
    }
    Err(Error::NoSwarm)
}
