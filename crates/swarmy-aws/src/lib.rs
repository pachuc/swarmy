//! The HTTP client every AWS SDK client in the workspace uses.
//!
//! The SDKs' own client features build either the aws-lc crypto backend,
//! which the workspace never compiles, or the deprecated hyper 0.14 and
//! rustls 0.21 stack. This client is hyper 1 with rustls 0.23 on ring, the
//! same TLS stack reqwest uses.

use aws_smithy_http_client::{Builder, tls};
use aws_smithy_runtime_api::client::http::SharedHttpClient;

/// Build the HTTPS client for an AWS SDK configuration.
///
/// Pass it to `aws_config::ConfigLoader::http_client` or to a service config
/// builder's `http_client`; service configs built from the resulting
/// `SdkConfig` inherit it. It trusts the platform's native root certificates
/// and, like the SDKs' previous client, ignores proxy environment variables.
#[must_use]
pub fn https_client() -> SharedHttpClient {
    Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https()
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use aws_smithy_runtime_api::client::http::HttpClient as _;

    #[test]
    fn client_is_hyper_1() {
        let metadata = super::https_client()
            .connector_metadata()
            .expect("the hyper client reports connector metadata");
        assert_eq!(metadata.name(), "hyper");
        assert_eq!(metadata.version().as_deref(), Some("1.x"));
    }
}
