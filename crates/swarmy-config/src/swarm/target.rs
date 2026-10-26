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
            Self::Swarm(s) => f.debug_tuple("Swarm").field(&s.name).finish(),
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
/// Resolve a client target from the process environment and registry.
/// # Errors
/// Returns resolution, environment, or directory errors.
pub fn resolve_process(flag: Option<&str>) -> Result<(Registry, Target), Error> {
    let environment = crate::process_environment(|key| {
        matches!(key, "HOME" | "XDG_CONFIG_HOME" | "XDG_DATA_HOME" | "XDG_STATE_HOME"
            | "XDG_CACHE_HOME" | "SWARMY_SWARM" | "SWARMY_API_URL" | "SWARMY_API_TOKEN")
    })?;
    let registry = Registry::new(crate::paths::Dirs::from_env(&environment)?);
    let target = resolve(&registry, flag, &environment)?;
    Ok((registry, target))
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    use crate::swarm::registry::tests::{setup, spec};
    fn assert_redacted(registry: &Registry, env: &BTreeMap<String, String>) {
        assert!(!format!("{:?}", resolve(registry, None, env).unwrap()).contains("secret"));
    }
    fn assert_no_swarm_hints() {
        for hint in ["swarmy swarm ls", "swarmy swarm use", "swarmy swarm create"] {
            assert!(Error::NoSwarm.to_string().contains(hint));
        }
    }
    #[test]
    fn precedence_and_errors() {
        let (_temp, registry) = setup();
        for name in ["flag", "env", "cur"] {
            registry.write_spec(&spec(name)).unwrap();
        }
        registry.set_current("cur").unwrap();
        let mut env = BTreeMap::from([
            ("SWARMY_SWARM".into(), "env".into()),
            ("SWARMY_API_URL".into(), "http://localhost".into()),
            ("SWARMY_API_TOKEN".into(), "secret".into()),
        ]);
        assert!(
            matches!(resolve(&registry,Some("flag"),&env).unwrap(),Target::Swarm(s) if s.name=="flag")
        );
        assert!(
            matches!(resolve(&registry,None,&env).unwrap(),Target::Swarm(s) if s.name=="env")
        );
        env.insert("SWARMY_SWARM".into(), String::new());
        assert!(
            matches!(resolve(&registry,None,&env).unwrap(),Target::Endpoint{url,token} if url=="http://localhost" && token=="secret")
        );
        assert_redacted(&registry, &env);
        env.insert("SWARMY_API_TOKEN".into(), String::new());
        assert!(matches!(
            resolve(&registry, None, &env),
            Err(Error::IncompleteEndpoint)
        ));
        env.remove("SWARMY_API_TOKEN");
        assert!(matches!(
            resolve(&registry, None, &env),
            Err(Error::IncompleteEndpoint)
        ));
        env.insert("SWARMY_API_URL".into(), String::new());
        env.insert("SWARMY_API_TOKEN".into(), "secret".into());
        assert!(
            matches!(resolve(&registry,None,&env).unwrap(),Target::Swarm(s) if s.name=="cur")
        );
        assert!(matches!(
            resolve(&registry, Some("missing"), &env),
            Err(Error::UnknownSwarm(_))
        ));
        registry.clear_current().unwrap();
        assert!(matches!(
            resolve(&registry, None, &env),
            Err(Error::NoSwarm)
        ));
        assert_no_swarm_hints();
        crate::write_private(&registry.dirs().current(), b"missing\n").unwrap();
        assert!(matches!(
            resolve(&registry, None, &env),
            Err(Error::StaleCurrent(_))
        ));
    }
}
