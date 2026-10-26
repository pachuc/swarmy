//! `tofu output -json` prints one key per root output, and each value is an object `{ "sensitive": bool, "type": <type>, "value": <value> }`. Swarmy reads only `value` and ignores `sensitive` and `type`. A module must declare these root outputs:
//!
//! ```json
//! {
//!   "machines": {
//!     "sensitive": false,
//!     "type": ["list", ["object", {"name": "string", "public_ipv4": "string", "public_ipv6": "string", "private_ip": "string", "ssh_user": "string"}]],
//!     "value": [
//!       {
//!         "name": "fleet-workers-1",
//!         "public_ipv4": "203.0.113.10",
//!         "public_ipv6": "2001:db8::1",
//!         "private_ip": "10.0.1.2",
//!         "ssh_user": "root"
//!       }
//!     ]
//!   },
//!   "bucket": {
//!     "sensitive": false,
//!     "type": ["object", {"endpoint": "string", "name": "string", "region": "string"}],
//!     "value": { "endpoint": "https://objects.example.com", "name": "fleet-data", "region": "fsn1" }
//!   }
//! }
//! ```
//!
//! Rules:
//! - `machines` is required. It is a list (possibly empty when count is 0) of objects with exactly these five keys; unknown keys are rejected.
//! - `name` must be non-empty and unique.
//! - `public_ipv4`, `public_ipv6` and `private_ip` are address strings, or `null` when absent. Never use `""`.
//! - At least one of `public_ipv4` and `public_ipv6` must be non-null, because swarmy connects over SSH to the public address.
//! - `ssh_user` must be non-empty.
//! - `bucket` is optional. A group that creates no bucket either omits it or sets its value to `null`. When present it has exactly `endpoint` (an `http` or `https` URL), `name` (non-empty) and `region` (non-empty).
//! - `network_id` is optional, opaque to swarmy, and passed to later groups of the same module.
//! - Other outputs are ignored.
use std::{
    collections::{BTreeMap, HashSet},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};
// Unlike serde's default Option handling, missing address keys violate the contract.
fn present_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    serde::Deserialize::deserialize(deserializer)
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Machine {
    pub name: String,
    #[serde(deserialize_with = "present_option")]
    pub public_ipv4: Option<Ipv4Addr>,
    #[serde(deserialize_with = "present_option")]
    pub public_ipv6: Option<Ipv6Addr>,
    #[serde(deserialize_with = "present_option")]
    pub private_ip: Option<IpAddr>,
    pub ssh_user: String,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bucket {
    pub endpoint: String,
    pub name: String,
    pub region: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleOutputs {
    pub machines: Vec<Machine>,
    pub bucket: Option<Bucket>,
    pub network_id: Option<String>,
}
#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    #[error("not valid JSON")]
    Json(#[source] serde_json::Error),
    #[error("the module has no {0} output")]
    Missing(&'static str),
    #[error("output {output} does not match the module contract")]
    Shape {
        output: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("output {output}: {reason}")]
    Invalid {
        output: &'static str,
        reason: String,
    },
}
#[derive(serde::Deserialize)]
struct RawOutput {
    value: serde_json::Value,
}
/// Parse and validate module outputs.
/// # Errors
/// Rejects malformed or incomplete output contracts.
pub fn parse_outputs(json: &str) -> Result<ModuleOutputs, OutputError> {
    let mut raw: BTreeMap<String, RawOutput> =
        serde_json::from_str(json).map_err(OutputError::Json)?;
    let machines = raw
        .remove("machines")
        .ok_or(OutputError::Missing("machines"))?;
    let machines: Vec<Machine> =
        serde_json::from_value(machines.value).map_err(|source| OutputError::Shape {
            output: "machines",
            source,
        })?;
    let bucket: Option<Bucket> = raw
        .remove("bucket")
        .map(|b| {
            serde_json::from_value(b.value).map_err(|source| OutputError::Shape {
                output: "bucket",
                source,
            })
        })
        .transpose()?
        .flatten();
    let network_id: Option<String> = raw
        .remove("network_id")
        .map(|n| {
            serde_json::from_value(n.value).map_err(|source| OutputError::Shape {
                output: "network_id",
                source,
            })
        })
        .transpose()?
        .flatten()
        .filter(|id: &String| !id.is_empty());
    let mut names = HashSet::new();
    for machine in &machines {
        let reason = if machine.name.is_empty() {
            Some("machine name is empty".into())
        } else if !names.insert(&machine.name) {
            Some(format!("duplicate machine name {}", machine.name))
        } else if machine.public_ipv4.is_none() && machine.public_ipv6.is_none() {
            Some(format!("machine {} has no public address", machine.name))
        } else if machine.ssh_user.is_empty() {
            Some(format!("machine {} has empty ssh_user", machine.name))
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(OutputError::Invalid {
                output: "machines",
                reason,
            });
        }
    }
    if let Some(bucket) = &bucket {
        let valid_endpoint = url::Url::parse(&bucket.endpoint)
            .is_ok_and(|url| matches!(url.scheme(), "http" | "https"));
        let reason = if bucket.name.is_empty() {
            Some("bucket name is empty")
        } else if bucket.region.is_empty() {
            Some("bucket region is empty")
        } else if !valid_endpoint {
            Some("bucket endpoint must be an http or https URL")
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(OutputError::Invalid {
                output: "bucket",
                reason: reason.into(),
            });
        }
    }
    Ok(ModuleOutputs {
        machines,
        bucket,
        network_id,
    })
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    const VALID: &str = r#"{"machines":{"sensitive":false,"type":[],"value":[{"name":"fleet-workers-1","public_ipv4":"203.0.113.10","public_ipv6":"2001:db8::1","private_ip":"10.0.1.2","ssh_user":"root"}]},"bucket":{"value":{"endpoint":"https://objects.example.com","name":"fleet-data","region":"fsn1"}}}"#;
    #[test]
    fn valid() {
        let outputs = parse_outputs(VALID).unwrap();
        assert_eq!(outputs.machines[0].name, "fleet-workers-1");
        assert_eq!(
            outputs.machines[0].public_ipv4.unwrap().to_string(),
            "203.0.113.10"
        );
        assert_eq!(
            outputs.machines[0].public_ipv6.unwrap().to_string(),
            "2001:db8::1"
        );
        assert_eq!(
            outputs.machines[0].private_ip.unwrap().to_string(),
            "10.0.1.2"
        );
        assert_eq!(outputs.machines[0].ssh_user, "root");
        assert_eq!(outputs.bucket.unwrap().name, "fleet-data");
    }
    #[test]
    fn optional_network_id() {
        let base = r#"{"machines":{"value":[]}}"#;
        assert_eq!(parse_outputs(base).unwrap().network_id, None);
        for (value, expected) in [(r#""4711""#, Some("4711")), ("null", None), (r#""""#, None)] {
            let json = format!(r#"{{"machines":{{"value":[]}},"network_id":{{"value":{value}}}}}"#);
            assert_eq!(
                parse_outputs(&json).unwrap().network_id.as_deref(),
                expected
            );
        }
        assert!(matches!(
            parse_outputs(r#"{"machines":{"value":[]},"network_id":{"value":12}}"#),
            Err(OutputError::Shape {
                output: "network_id",
                ..
            })
        ));
    }
    #[test]
    fn optional_and_invalid() {
        for json in [
            r#"{"machines":{"value":[]}}"#,
            r#"{"machines":{"value":[]},"bucket":{"value":null}}"#,
        ] {
            assert_eq!(parse_outputs(json).unwrap().bucket, None);
        }
        assert!(matches!(
            parse_outputs("{}"),
            Err(OutputError::Missing("machines"))
        ));
        assert!(matches!(parse_outputs("oops"), Err(OutputError::Json(_))));
        let unknown = VALID.replace(
            "\"name\":\"fleet-workers-1\",",
            "\"extra\":1,\"name\":\"fleet-workers-1\",",
        );
        assert!(matches!(
            parse_outputs(&unknown),
            Err(OutputError::Shape { .. })
        ));
        let missing_ip = VALID.replace("\"private_ip\":\"10.0.1.2\",", "");
        assert!(matches!(
            parse_outputs(&missing_ip),
            Err(OutputError::Shape { .. })
        ));
        let bad_ip = VALID.replace("203.0.113.10", "bad");
        assert!(matches!(
            parse_outputs(&bad_ip),
            Err(OutputError::Shape { .. })
        ));
        for (from, to) in [
            ("fleet-workers-1", ""),
            ("root", ""),
            ("203.0.113.10", ""),
            ("https://objects.example.com", "ftp://x"),
            ("https://objects.example.com", "not a url"),
        ] {
            let json = VALID.replace(from, to);
            assert!(parse_outputs(&json).is_err());
        }
        let machine = r#"{"name":"x","public_ipv4":"203.0.113.10","public_ipv6":null,"private_ip":null,"ssh_user":"root"}"#;
        assert!(matches!(
            parse_outputs(&format!(
                r#"{{"machines":{{"value":[{machine},{machine}]}}}}"#
            )),
            Err(OutputError::Invalid { .. })
        ));
        assert!(matches!(
            parse_outputs(
                r#"{"machines":{"value":[{"name":"x","public_ipv4":null,"public_ipv6":null,"private_ip":null,"ssh_user":"root"}]}}"#
            ),
            Err(OutputError::Invalid { .. })
        ));
    }
}
