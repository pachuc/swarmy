use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{Error, Settings};

/// An exact object namespace, with no empty or relative path segments.
/// Empty selects the whole bucket. Parsing never trims or encodes the value.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectPrefix(String);

impl ObjectPrefix {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ObjectPrefix {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || {
            Error::S3Namespace(
                "prefix must have no leading/trailing slash, empty or dot segments, or control characters",
            )
        };
        if value.is_empty() {
            return Ok(Self(String::new()));
        }
        if value.starts_with('/') || value.ends_with('/') {
            return Err(invalid());
        }
        if value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
            || value.chars().any(char::is_control)
        {
            return Err(invalid());
        }
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for ObjectPrefix {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ObjectPrefix> for String {
    fn from(value: ObjectPrefix) -> Self {
        value.0
    }
}

impl Settings {
    /// Split the configured bucket and prefix without building a client.
    /// A legacy `bucket/prefix` keeps its exact object locations; combining
    /// it with an explicit prefix is rejected as ambiguous.
    /// # Errors
    /// Rejects empty buckets and invalid or ambiguous namespaces.
    pub fn s3_namespace(&self) -> Result<(&str, ObjectPrefix), Error> {
        let (bucket, prefix) = if let Some((bucket, prefix)) = self.s3_bucket.split_once('/') {
            if !self.s3_prefix.as_str().is_empty() {
                return Err(Error::S3Namespace(
                    "set either a legacy bucket/prefix or s3_prefix, not both",
                ));
            }
            if prefix.is_empty() {
                return Err(Error::S3Namespace(
                    "legacy bucket/prefix has an empty prefix",
                ));
            }
            (bucket, prefix.parse()?)
        } else {
            (self.s3_bucket.as_str(), self.s3_prefix.clone())
        };
        if bucket.is_empty() {
            return Err(Error::S3Namespace("bucket must not be empty"));
        }
        Ok((bucket, prefix))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn prefixes_are_validated_without_normalization() {
        for value in ["", "run", "runs/nested", "spaces and %2F/雪"] {
            assert_eq!(value.parse::<ObjectPrefix>().unwrap().as_str(), value);
        }
        for value in [
            "/", "/run", "run/", "a//b", ".", "..", "a/./b", "a/../b", "a\n",
        ] {
            assert!(value.parse::<ObjectPrefix>().is_err(), "{value:?}");
            let encoded = toml::to_string(&BTreeMap::from([("s3_prefix", value)])).unwrap();
            assert!(toml::from_str::<Settings>(&encoded).is_err());
            assert!(
                Settings::default()
                    .apply_environment(&BTreeMap::from([("SWARMY_S3_PREFIX".into(), value.into())]))
                    .is_err()
            );
        }
    }

    #[test]
    fn prefix_round_trips_and_environment_overrides_file() {
        let mut settings: Settings = toml::from_str("s3_prefix = 'file/nested'").unwrap();
        settings
            .apply_environment(&BTreeMap::from([(
                "SWARMY_S3_PREFIX".into(),
                "env/nested".into(),
            )]))
            .unwrap();
        assert_eq!(settings.s3_prefix.as_str(), "env/nested");
        let decoded: Settings = toml::from_str(&settings.to_toml().unwrap()).unwrap();
        assert_eq!(decoded.s3_prefix, settings.s3_prefix);
        let mut exported = Settings::default();
        exported.apply_environment(&settings.environment()).unwrap();
        assert_eq!(exported.s3_prefix, settings.s3_prefix);
        exported
            .apply_environment(&BTreeMap::from([(
                "SWARMY_S3_PREFIX".into(),
                String::new(),
            )]))
            .unwrap();
        assert_eq!(exported.s3_prefix, ObjectPrefix::default());
    }

    #[test]
    fn legacy_namespace_is_exact_and_ambiguity_is_rejected() {
        let mut settings = Settings {
            s3_bucket: "bucket/run/nested".into(),
            ..Settings::default()
        };
        let (bucket, prefix) = settings.s3_namespace().unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(prefix.as_str(), "run/nested");
        settings.s3_prefix = "explicit".parse().unwrap();
        assert!(settings.s3_namespace().is_err());
        settings.s3_prefix = ObjectPrefix::default();
        for value in [
            "",
            "/run",
            "bucket/",
            "bucket/a//b",
            "bucket/a/../b",
            "bucket/a/",
        ] {
            settings.s3_bucket = value.into();
            assert!(settings.s3_namespace().is_err(), "{value}");
        }
    }
}
