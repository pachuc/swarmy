use serde::{Deserialize, Serialize};
/// Request the token for an agent currently placed on this node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GithubTokenRequest {
    pub agent: swarmy_core::AgentId,
}
/// Secret answer; debug output must never include the credential.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GithubTokenResponse {
    pub server_now: jiff::Timestamp,
    pub token: Option<String>,
}
impl std::fmt::Debug for GithubTokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubTokenResponse")
            .field("server_now", &self.server_now)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}
#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::*;
    #[test]
    fn debug_redacts_token() {
        let value = GithubTokenResponse {
            server_now: jiff::Timestamp::UNIX_EPOCH,
            token: Some("ghp_secret".into()),
        };
        assert!(!format!("{value:?}").contains("ghp_secret"));
        assert_eq!(
            serde_json::from_str::<GithubTokenResponse>(&serde_json::to_string(&value).unwrap())
                .unwrap(),
            value
        );
    }
}
