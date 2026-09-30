//! Resolve one bucket description from CLI flags, environment, and config.
//!
//! Static keys enter through `--s3-secret-file` (a 0600 file),
//! `--s3-secret-stdin` (one line), or `AWS_SECRET_ACCESS_KEY`, with the access
//! key from `--s3-access-key` or `AWS_ACCESS_KEY_ID`. The secret value never
//! appears on a command line, in a log, or in `remote status` output.
use std::{
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use swarmy_config::{BucketCredentials, BucketSpec};

use crate::Result;

pub(crate) struct BucketOptions {
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub prefix: Option<String>,
    pub access_key: Option<String>,
    pub secret_file: Option<PathBuf>,
    pub secret_stdin: bool,
    /// Secret line already read from stdin by the caller.
    pub stdin_secret: Option<String>,
    /// Values of `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
    pub env_access_key: Option<String>,
    pub env_secret_key: Option<String>,
}

/// Resolve the remote's bucket description. `existing` is the configured
/// `[remote.bucket]` value, if any; flags override it field by field. A
/// repeat `up` without flags reproduces the saved description, so the
/// idempotent-reuse check in `up` keeps working.
///
/// # Errors
///
/// Rejects bucket options without a bucket name, invalid prefixes and names,
/// unreadable or wrongly permissioned secret files, and static keys without
/// an endpoint, access key, or secret.
pub(crate) fn resolve(
    existing: Option<BucketSpec>,
    options: &BucketOptions,
) -> Result<Option<BucketSpec>> {
    let mut spec = match (&options.bucket, existing) {
        (Some(name), _) => BucketSpec {
            bucket: name.clone(),
            ..BucketSpec::default()
        },
        (None, Some(spec)) => spec,
        (None, None) => {
            crate::Error::ensure(
                options.endpoint.is_none()
                    && options.region.is_none()
                    && options.prefix.is_none()
                    && options.access_key.is_none()
                    && options.secret_file.is_none()
                    && !options.secret_stdin
                    && options.stdin_secret.is_none(),
                "bucket --s3-* options need --bucket NAME",
            )?;
            return Ok(None);
        }
    };
    if let Some(endpoint) = &options.endpoint {
        spec.endpoint.clone_from(endpoint);
    }
    if let Some(region) = &options.region {
        spec.region.clone_from(region);
    }
    if let Some(prefix) = &options.prefix {
        spec.prefix = prefix
            .parse()
            .map_err(|source| crate::Error::context(source, "invalid --s3-prefix"))?;
    }
    // An explicit empty endpoint selects AWS S3 with the instance role.
    if options.endpoint.as_deref() == Some("") {
        spec.credentials = BucketCredentials::InstanceRole;
    }
    // Ambient `AWS_*` keys belong to the laptop identity and must never
    // become node credentials on their own. They apply only when the bucket
    // is static by endpoint, by flags, or by its saved description.
    let explicit_secret =
        options.secret_file.is_some() || options.secret_stdin || options.stdin_secret.is_some();
    if !spec.endpoint.is_empty()
        || options.access_key.is_some()
        || explicit_secret
        || spec.needs_static_keys()
    {
        // Static keys need all three coordinates; anything less is a typo.
        // Saved keys survive when no flag replaces them, so a repeat `up`
        // without flags reproduces the saved description.
        let saved = match &spec.credentials {
            BucketCredentials::StaticKeys {
                access_key,
                secret_key,
            } => (access_key.clone(), secret_key.clone()),
            BucketCredentials::InstanceRole => (String::new(), String::new()),
        };
        let access_key = options
            .access_key
            .clone()
            .or_else(|| options.env_access_key.clone().filter(|key| !key.is_empty()))
            .unwrap_or(saved.0);
        let secret_key = secret_from(options)?.unwrap_or(saved.1);
        crate::Error::ensure(
            !spec.endpoint.is_empty(),
            "static S3 keys need --s3-endpoint URL",
        )?;
        crate::Error::ensure(
            !access_key.is_empty(),
            "static S3 keys need --s3-access-key or AWS_ACCESS_KEY_ID",
        )?;
        crate::Error::ensure(
            !secret_key.is_empty(),
            "static S3 keys need --s3-secret-file, --s3-secret-stdin, or AWS_SECRET_ACCESS_KEY",
        )?;
        spec.credentials = BucketCredentials::StaticKeys {
            access_key,
            secret_key,
        };
    }
    spec.validate_name()?;
    Ok(Some(spec))
}

fn secret_from(options: &BucketOptions) -> Result<Option<String>> {
    if let Some(path) = &options.secret_file {
        return Ok(Some(read_secret_file(path)?));
    }
    if options.secret_stdin {
        let Some(secret) = &options.stdin_secret else {
            return Err(crate::Error::other(
                "--s3-secret-stdin needs one secret line on stdin",
            ));
        };
        crate::Error::ensure(!secret.is_empty(), "stdin secret must not be empty")?;
        return Ok(Some(secret.clone()));
    }
    if let Some(secret) = &options.env_secret_key
        && !secret.is_empty()
    {
        return Ok(Some(secret.clone()));
    }
    Ok(None)
}

/// Read one secret line from a file only the owner can read. The value never
/// appears in an error message.
///
/// # Errors
///
/// Rejects missing files, group- or world-accessible permissions, and empty
/// or multi-line contents.
pub(crate) fn read_secret_file(path: &Path) -> Result<String> {
    let mode = std::fs::metadata(path)?.permissions().mode();
    // No group or other permission bits: the low six mode bits must be zero.
    crate::Error::ensure(
        mode & 0o077 == 0,
        format!(
            "secret file {} must not be readable by group or others (chmod 600)",
            path.display()
        ),
    )?;
    let secret = std::fs::read_to_string(path)?;
    let secret = secret.strip_suffix('\n').unwrap_or(&secret);
    let secret = secret.strip_suffix('\r').unwrap_or(secret);
    crate::Error::ensure(
        !secret.is_empty() && !secret.contains('\n'),
        "secret file must hold exactly one non-empty line",
    )?;
    Ok(secret.to_owned())
}

/// Read one secret line from stdin without echoing or logging it.
///
/// # Errors
///
/// Reports I/O failures and empty input.
pub(crate) fn read_secret_stdin() -> Result<String> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let secret = input.strip_suffix('\n').unwrap_or(&input);
    let secret = secret.strip_suffix('\r').unwrap_or(secret);
    crate::Error::ensure(!secret.is_empty(), "stdin secret must not be empty")?;
    Ok(secret.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_options() -> BucketOptions {
        BucketOptions {
            bucket: None,
            endpoint: None,
            region: None,
            prefix: None,
            access_key: None,
            secret_file: None,
            secret_stdin: false,
            stdin_secret: None,
            env_access_key: None,
            env_secret_key: None,
        }
    }

    fn secret_file(contents: &str) -> (tempfile::NamedTempFile, PathBuf) {
        use std::io::Write;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o600))
            .unwrap();
        let path = file.path().to_owned();
        (file, path)
    }

    #[test]
    fn no_bucket_without_options_stays_local() {
        assert!(resolve(None, &empty_options()).unwrap().is_none());
        let mut options = empty_options();
        options.endpoint = Some("https://objects.example.invalid".into());
        assert!(resolve(None, &options).is_err());
    }

    #[test]
    fn bucket_name_alone_selects_the_instance_role() {
        let mut options = empty_options();
        options.bucket = Some("test-bucket".into());
        let spec = resolve(None, &options).unwrap().unwrap();
        assert!(spec.is_aws());
        assert_eq!(spec.bucket, "test-bucket");
    }

    #[test]
    fn flags_override_the_saved_description_field_by_field() {
        let saved = BucketSpec {
            endpoint: "https://objects.example.invalid".into(),
            region: "eu-west-1".into(),
            bucket: "test-bucket".into(),
            prefix: "runs/old".parse().unwrap(),
            credentials: BucketCredentials::StaticKeys {
                access_key: "old-access".into(),
                secret_key: "old-secret".into(),
            },
            ..BucketSpec::default()
        };
        // A repeat run without flags reproduces the saved description.
        assert_eq!(
            resolve(Some(saved.clone()), &empty_options()).unwrap(),
            Some(saved.clone())
        );
        let mut options = empty_options();
        options.prefix = Some("runs/new".into());
        let spec = resolve(Some(saved), &options).unwrap().unwrap();
        assert_eq!(spec.prefix.as_str(), "runs/new");
        assert_eq!(
            spec.credentials,
            BucketCredentials::StaticKeys {
                access_key: "old-access".into(),
                secret_key: "old-secret".into(),
            }
        );
    }

    #[test]
    fn static_keys_come_from_file_stdin_or_environment() {
        let (_file, path) = secret_file("file-secret\n");
        let mut options = empty_options();
        options.bucket = Some("test-bucket".into());
        options.endpoint = Some("https://objects.example.invalid".into());
        options.access_key = Some("test-access".into());
        options.secret_file = Some(path);
        let spec = resolve(None, &options).unwrap().unwrap();
        assert_eq!(
            spec.credentials,
            BucketCredentials::StaticKeys {
                access_key: "test-access".into(),
                secret_key: "file-secret".into(),
            }
        );
        let mut options = empty_options();
        options.bucket = Some("test-bucket".into());
        options.endpoint = Some("https://objects.example.invalid".into());
        options.secret_stdin = true;
        options.stdin_secret = Some("stdin-secret".into());
        options.env_access_key = Some("env-access".into());
        let spec = resolve(None, &options).unwrap().unwrap();
        assert_eq!(
            spec.credentials,
            BucketCredentials::StaticKeys {
                access_key: "env-access".into(),
                secret_key: "stdin-secret".into(),
            }
        );
        // Partial static coordinates are rejected, never defaulted.
        let mut missing_secret = empty_options();
        missing_secret.bucket = Some("test-bucket".into());
        missing_secret.endpoint = Some("https://objects.example.invalid".into());
        missing_secret.access_key = Some("test-access".into());
        assert!(resolve(None, &missing_secret).is_err());
        let mut missing_endpoint = empty_options();
        missing_endpoint.bucket = Some("test-bucket".into());
        missing_endpoint.access_key = Some("test-access".into());
        missing_endpoint.env_secret_key = Some("env-secret".into());
        assert!(resolve(None, &missing_endpoint).is_err());
    }

    #[test]
    fn ambient_laptop_keys_never_become_node_credentials() {
        // An operator with AWS keys exported for the provisioning identity
        // runs plain AWS bucket remotes; the env keys must not convert the
        // bucket to static or fail the run.
        let mut options = empty_options();
        options.bucket = Some("test-bucket".into());
        options.env_access_key = Some("laptop-access".into());
        options.env_secret_key = Some("laptop-secret".into());
        let spec = resolve(None, &options).unwrap().unwrap();
        assert!(spec.is_aws());
        // The same holds when the saved description is already AWS.
        let spec = resolve(Some(spec), &options).unwrap().unwrap();
        assert!(spec.is_aws());
        // An explicit empty endpoint switches a static bucket back to AWS.
        let mut back = empty_options();
        back.endpoint = Some(String::new());
        let spec = resolve(
            Some(BucketSpec {
                endpoint: "https://objects.example.invalid".into(),
                region: "eu-west-1".into(),
                bucket: "test-bucket".into(),
                credentials: BucketCredentials::StaticKeys {
                    access_key: "old-access".into(),
                    secret_key: "old-secret".into(),
                },
                ..BucketSpec::default()
            }),
            &back,
        )
        .unwrap()
        .unwrap();
        assert!(spec.is_aws());
    }

    #[test]
    fn secret_files_need_private_permissions_and_one_line() {
        let (file, path) = secret_file("line-secret\n");
        assert_eq!(read_secret_file(&path).unwrap(), "line-secret");
        file.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o644))
            .unwrap();
        let error = read_secret_file(&path).unwrap_err().to_string();
        assert!(error.contains("chmod 600"), "{error}");
        assert!(!error.contains("line-secret"), "{error}");
        let (_file, empty) = secret_file("\n");
        assert!(read_secret_file(&empty).is_err());
        let (_file, two) = secret_file("one\ntwo\n");
        assert!(read_secret_file(&two).is_err());
    }

    #[test]
    fn invalid_names_and_prefixes_are_rejected() {
        let mut options = empty_options();
        options.bucket = Some("bad/bucket".into());
        assert!(resolve(None, &options).is_err());
        let mut options = empty_options();
        options.bucket = Some("test-bucket".into());
        options.prefix = Some("/lead".into());
        assert!(resolve(None, &options).is_err());
    }
}
