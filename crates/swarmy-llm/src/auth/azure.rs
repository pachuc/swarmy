use super::{Login, LoginUi};
use crate::Error;
use async_trait::async_trait;
use jiff::{Timestamp, civil::DateTime, tz::TimeZone};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
use swarmy_core::CredentialKind;

const DEFAULT_SCOPE: &str = "https://cognitiveservices.azure.com/.default";

pub struct AzureLogin {
    resource: String,
    scope: String,
}

impl AzureLogin {
    #[must_use]
    pub fn new(resource: &str, scope: Option<&str>) -> Self {
        Self {
            resource: resource.into(),
            scope: scope.unwrap_or(DEFAULT_SCOPE).into(),
        }
    }

    async fn token(resource: &str, scope: &str) -> Result<CredentialKind, Error> {
        if resource.trim().is_empty() {
            return Err(Error::Credentials("Azure login requires --resource NAME"));
        }
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new("az")
                .args([
                    "account",
                    "get-access-token",
                    "--scope",
                    scope,
                    "--output",
                    "json",
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| Error::NeedsLogin("azure".into()))?
        .map_err(|_| Error::NeedsLogin("azure".into()))?;
        if !output.status.success() {
            return Err(Error::NeedsLogin("azure".into()));
        }
        let value: Value = serde_json::from_slice(&output.stdout)
            .map_err(|_| Error::Credentials("invalid Azure CLI JSON"))?;
        let access = value["accessToken"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Credentials("Azure CLI returned no access token"))?;
        let expires_at = expiry(&value)?;
        Ok(CredentialKind::OAuth {
            access: access.into(),
            refresh: String::new(),
            expires_at,
            extra: BTreeMap::from([
                ("resource_name".into(), resource.into()),
                ("scope".into(), scope.into()),
                ("token_source".into(), "azure_cli".into()),
            ]),
        })
    }
}

fn expiry(value: &Value) -> Result<Timestamp, Error> {
    // New Azure CLI versions provide an unambiguous UTC epoch as well as local time.
    if let Some(seconds) = value["expires_on"].as_i64() {
        return Timestamp::from_second(seconds)
            .map_err(|_| Error::Credentials("invalid Azure expiry"));
    }
    let text = value["expiresOn"]
        .as_str()
        .ok_or(Error::Credentials("missing Azure expiresOn"))?;
    if let Ok(timestamp) = text.parse::<Timestamp>() {
        return Ok(timestamp);
    }
    text.parse::<DateTime>()
        .ok()
        .and_then(|date| date.to_zoned(TimeZone::system()).ok())
        .map(|date| date.timestamp())
        .ok_or(Error::Credentials("invalid Azure expiresOn"))
}

#[async_trait]
impl Login for AzureLogin {
    fn provider(&self) -> &'static str {
        "azure"
    }
    async fn login(&self, _ui: &dyn LoginUi) -> Result<CredentialKind, Error> {
        Self::token(&self.resource, &self.scope).await
    }
    async fn refresh(&self, record: &CredentialKind) -> Result<Option<CredentialKind>, Error> {
        let CredentialKind::OAuth { extra, .. } = record else {
            return Ok(None);
        };
        Ok(Some(
            Self::token(
                extra.get("resource_name").map_or("", String::as_str),
                extra.get("scope").map_or(DEFAULT_SCOPE, String::as_str),
            )
            .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn epoch_avoids_local_daylight_saving_ambiguity() {
        let value =
            serde_json::json!({"expires_on": 1_800_000_000, "expiresOn": "invalid local time"});
        assert_eq!(expiry(&value).unwrap().as_second(), 1_800_000_000);
        assert!(expiry(&serde_json::json!({"expiresOn":"invalid"})).is_err());
    }
}
