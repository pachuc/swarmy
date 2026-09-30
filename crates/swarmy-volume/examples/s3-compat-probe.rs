//! Compatibility probe for any S3-compatible object storage.
//!
//! The operator runs this against a candidate bucket before the fleet depends
//! on it, and the same command verifies the development stack. It exercises
//! every S3 call swarmy makes: bucket existence through the provisioning
//! client's `HeadBucket`, then PUT, create-only PUT (`If-None-Match: *`) on a
//! new and an existing key, GET, HEAD, prefixed listing with pagination, and
//! DELETE through the same object client the volume and blob stores use
//! (`swarmy_store::objects::from_settings`). Swarmy never issues ranged GETs:
//! chunks, manifests, and blobs are always read whole, so ranges are not
//! probed. Each check prints one JSON line naming only the check, whether it
//! passed, and a detail free of key material; the endpoint, bucket, prefix,
//! and keys never appear, so the output is safe to paste into a pull request.
//!
//! All probe objects live under one `probe-<ulid>/` prefix and are deleted at
//! the end. Example against the development stack (coordinates from the
//! sourced `.dev/env`) or against a candidate bucket with static keys:
//!
//! ```sh
//! scripts/dev-stack.sh start && source .dev/env
//! cargo run --locked -p swarmy-volume --example s3-compat-probe
//! cargo run --locked -p swarmy-volume --example s3-compat-probe -- \
//!   --endpoint https://objects.example.invalid --region eu-west-1 \
//!   --bucket NAME --access-key KEY --secret-file ~/.swarmy/demo-s3-secret
//! ```
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use clap::Parser;
use futures::TryStreamExt;
use object_store::{ObjectStore, PutMode, path::Path};

/// Probe an S3-compatible bucket with static keys.
#[derive(Parser)]
#[command(name = "s3-compat-probe", version)]
struct Args {
    /// Bucket endpoint URL. Falls back to `SWARMY_S3_ENDPOINT`.
    #[arg(long)]
    endpoint: Option<String>,
    /// Bucket region. Falls back to `SWARMY_S3_REGION`, then `us-east-1`.
    #[arg(long)]
    region: Option<String>,
    /// Bucket name. Falls back to `SWARMY_S3_BUCKET`.
    #[arg(long)]
    bucket: Option<String>,
    /// Static access key. Falls back to `SWARMY_S3_ACCESS_KEY`, then
    /// `AWS_ACCESS_KEY_ID`.
    #[arg(long, name = "access-key")]
    access_key: Option<String>,
    /// File holding the static secret key on one line. The file must be
    /// readable only by its owner. Takes precedence over `--secret-stdin`.
    #[arg(long, name = "secret-file")]
    secret_file: Option<PathBuf>,
    /// Read the static secret key as one line on stdin instead of a file.
    #[arg(long, name = "secret-stdin")]
    secret_stdin: bool,
    /// Expect create-only PUTs to be enforced. Pass `--conditional-create=false`
    /// when the bucket description sets `conditional_create = false`, in which
    /// case the second create-only PUT is expected to overwrite.
    #[arg(long, name = "conditional-create", action = clap::ArgAction::Set, default_value_t = true)]
    conditional_create: bool,
}

/// Resolved bucket coordinates with the secret kept out of every report.
struct Coordinates {
    endpoint: String,
    region: String,
    bucket: String,
    access_key: String,
    secret: String,
    conditional_create: bool,
}

impl Coordinates {
    /// Key material that must never appear in output, even inside errors.
    fn secrets(&self) -> [&str; 2] {
        [&self.access_key, &self.secret]
    }
}

fn first_present(names: &[&str]) -> Option<String> {
    names
        .iter()
        .filter_map(|name| std::env::var(name).ok())
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
}

fn required(flag: Option<String>, names: &[&str], what: &str) -> Result<String, String> {
    flag.filter(|value| !value.trim().is_empty())
        .or_else(|| first_present(names))
        .ok_or_else(|| format!("missing {what}; pass the flag or set one of {}", names.join(", ")))
}

/// Read the secret key without ever printing it, from a file, stdin, or the
/// environment, in the same precedence `remote up` accepts.
fn resolve_secret(args: &Args) -> Result<String, String> {
    if let Some(path) = &args.secret_file {
        let metadata = std::fs::metadata(path)
            .map_err(|_| "cannot read the secret file".to_owned())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err("the secret file must be readable only by its owner".to_owned());
            }
        }
        let _ = metadata;
        let content = std::fs::read_to_string(path)
            .map_err(|_| "cannot read the secret file".to_owned())?;
        let secret = content.lines().next().unwrap_or_default().trim().to_owned();
        return if secret.is_empty() {
            Err("the secret file holds no key".to_owned())
        } else {
            Ok(secret)
        };
    }
    if args.secret_stdin {
        let mut line = String::new();
        std::io::BufRead::read_line(&mut std::io::stdin().lock(), &mut line)
            .map_err(|_| "cannot read the secret on stdin".to_owned())?;
        let secret = line.trim().to_owned();
        return if secret.is_empty() {
            Err("no secret key arrived on stdin".to_owned())
        } else {
            Ok(secret)
        };
    }
    first_present(&["SWARMY_S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"])
        .ok_or_else(|| {
            "missing secret key; pass --secret-file, --secret-stdin, or set SWARMY_S3_SECRET_KEY"
                .to_owned()
        })
}

fn resolve(args: &Args) -> Result<Coordinates, String> {
    Ok(Coordinates {
        endpoint: required(
            args.endpoint.clone(),
            &["SWARMY_S3_ENDPOINT"],
            "an endpoint URL",
        )?,
        region: args
            .region
            .clone()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| first_present(&["SWARMY_S3_REGION"]))
            .unwrap_or_else(|| "us-east-1".to_owned()),
        bucket: required(args.bucket.clone(), &["SWARMY_S3_BUCKET"], "a bucket name")?,
        access_key: required(
            args.access_key.clone(),
            &["SWARMY_S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"],
            "an access key",
        )?,
        secret: resolve_secret(args)?,
        conditional_create: args.conditional_create,
    })
}

/// Build the production object client from the probe coordinates, including
/// the plain-PUT fallback wrapper when `conditional_create` is off.
fn object_client(coordinates: &Coordinates) -> Result<Arc<dyn ObjectStore>, String> {
    let mut settings = swarmy_config::Settings::default();
    settings.s3.endpoint = coordinates.endpoint.clone();
    settings.s3.region = coordinates.region.clone();
    settings.s3.bucket = coordinates.bucket.clone();
    settings.s3.access_key = coordinates.access_key.clone();
    settings.s3.secret_key = coordinates.secret.clone();
    settings.s3.prefix = swarmy_config::ObjectPrefix::default();
    settings.s3.conditional_create = coordinates.conditional_create;
    swarmy_store::objects::from_settings(&settings)
        .map_err(|error| format!("invalid bucket coordinates: {error}"))
}

/// Build the provisioning-style bucket client: static keys, path-style
/// requests, the same shape `remote up` uses for `HeadBucket`.
fn bucket_client(coordinates: &Coordinates) -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(coordinates.region.clone()))
        .endpoint_url(coordinates.endpoint.clone())
        .credentials_provider(Credentials::new(
            &coordinates.access_key,
            &coordinates.secret,
            None,
            None,
            "s3-compat-probe",
        ))
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

fn service_code<E>(error: &aws_sdk_s3::error::SdkError<E>) -> String
where
    E: ProvideErrorMetadata + std::fmt::Debug + Send + Sync + 'static,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .unwrap_or("unclassified")
        .to_owned()
}

/// Short object-store failure names; paths stay out so key material cannot leak.
fn object_code(error: &object_store::Error) -> String {
    match error {
        object_store::Error::NotFound { .. } => "not_found".to_owned(),
        object_store::Error::AlreadyExists { .. } => "already_exists".to_owned(),
        object_store::Error::NotSupported { .. } => "not_supported".to_owned(),
        object_store::Error::NotImplemented { .. } => "not_implemented".to_owned(),
        object_store::Error::InvalidPath { .. } => "invalid_path".to_owned(),
        _ => "request_failed".to_owned(),
    }
}

/// Replace any key material accidentally carried inside an error with a marker.
fn redact(detail: &str, secrets: &[&str]) -> String {
    let mut clean = detail.to_owned();
    for secret in secrets {
        if !secret.is_empty() {
            clean = clean.replace(secret, "[redacted]");
        }
    }
    clean
}

struct Outcome {
    name: &'static str,
    result: Result<String, String>,
}

fn report(outcome: &Outcome, secrets: &[&str]) -> bool {
    let ok = outcome.result.is_ok();
    let detail = match &outcome.result {
        Ok(detail) => detail.clone(),
        Err(detail) => detail.clone(),
    };
    println!(
        "{}",
        serde_json::json!({
            "check": outcome.name,
            "ok": ok,
            "detail": redact(&detail, secrets),
        })
    );
    ok
}

fn payload() -> bytes::Bytes {
    bytes::Bytes::from((0..8192_u32).map(|index| (index % 251) as u8).collect::<Vec<_>>())
}

async fn check_bucket_exists(client: &aws_sdk_s3::Client, bucket: &str) -> Outcome {
    let name = "bucket_exists";
    let result = match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => Ok("bucket is reachable".to_owned()),
        Err(error) => Err(format!("head_bucket {}", service_code(&error))),
    };
    Outcome { name, result }
}

async fn check_put(store: &dyn ObjectStore, path: &Path, bytes: &bytes::Bytes) -> Outcome {
    let name = "put";
    let result = match store.put(path, bytes.clone().into()).await {
        Ok(_) => Ok(format!("{} bytes written", bytes.len())),
        Err(error) => Err(format!("put {}", object_code(&error))),
    };
    Outcome { name, result }
}

async fn check_create_new(store: &dyn ObjectStore, path: &Path, bytes: &bytes::Bytes) -> Outcome {
    let name = "create_new";
    let result = match store
        .put_opts(path, bytes.clone().into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(format!("{} bytes created", bytes.len())),
        Err(error) => Err(format!("create-only put {}", object_code(&error))),
    };
    Outcome { name, result }
}

async fn check_create_existing(
    store: &dyn ObjectStore,
    path: &Path,
    bytes: &bytes::Bytes,
    conditional_create: bool,
) -> Outcome {
    let name = "create_existing";
    let result = match store
        .put_opts(path, bytes.clone().into(), PutMode::Create.into())
        .await
    {
        Ok(()) if conditional_create => {
            Err("create-only put overwrote the existing key; the provider ignores If-None-Match: set conditional_create = false".to_owned())
        }
        Ok(()) => Ok("overwrote the existing key with the fallback plain PUT".to_owned()),
        Err(object_store::Error::AlreadyExists { .. }) if conditional_create => {
            Ok("existing key rejected with AlreadyExists".to_owned())
        }
        Err(error) => Err(format!("second create-only put {}", object_code(&error))),
    };
    Outcome { name, result }
}

async fn check_get(store: &dyn ObjectStore, path: &Path, bytes: &bytes::Bytes) -> Outcome {
    let name = "get";
    let result = match store.get(path).await {
        Ok(object) => match object.bytes().await {
            Ok(actual) if actual == *bytes => Ok(format!("{} bytes verified", bytes.len())),
            Ok(_) => Err("get returned different bytes".to_owned()),
            Err(error) => Err(format!("get body {}", object_code(&error))),
        },
        Err(error) => Err(format!("get {}", object_code(&error))),
    };
    Outcome { name, result }
}

async fn check_head(store: &dyn ObjectStore, path: &Path, expected: usize) -> Outcome {
    let name = "head";
    let result = match store.head(path).await {
        Ok(meta) if meta.size == expected => Ok(format!("size {expected} reported")),
        Ok(meta) => Err(format!("head reported size {}", meta.size)),
        Err(error) => Err(format!("head {}", object_code(&error))),
    };
    Outcome { name, result }
}

/// Write numbered list keys, then read them back through one prefixed listing.
async fn check_list_prefix(
    store: &dyn ObjectStore,
    prefix: &Path,
    count: usize,
    bytes: &bytes::Bytes,
) -> Outcome {
    let name = "list_prefix";
    for index in 0..count {
        let path = Path::from(format!("{prefix}/key-{index:02}"));
        if let Err(error) = store.put(&path, bytes.clone().into()).await {
            return Outcome {
                name,
                result: Err(format!("list fixture put {}", object_code(&error))),
            };
        }
    }
    let result = match store.list(Some(prefix)).try_collect::<Vec<_>>().await {
        Ok(metas) => {
            let mut names: Vec<_> = metas
                .iter()
                .map(|meta| meta.location.filename().unwrap_or_default())
                .collect();
            names.sort_unstable();
            let expected: Vec<_> =
                (0..count).map(|index| format!("key-{index:02}")).collect();
            if names.iter().map(ToString::to_string).collect::<Vec<_>>() == expected {
                Ok(format!("{count} keys listed under the prefix"))
            } else {
                Err(format!("prefix listing returned {} keys", names.len()))
            }
        }
        Err(error) => Err(format!("list {}", object_code(&error))),
    };
    Outcome { name, result }
}

/// Page through the same list keys two at a time with continuation tokens, so
/// multi-page responses are exercised even for a handful of keys.
async fn check_list_pagination(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    count: usize,
) -> Outcome {
    let name = "list_pagination";
    let mut keys = HashSet::new();
    let mut pages = 0_usize;
    let mut token: Option<String> = None;
    let result = loop {
        let request = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .max_keys(2)
            .set_continuation_token(token);
        match request.send().await {
            Ok(page) => {
                pages += 1;
                for object in page.contents() {
                    if let Some(key) = object.key() {
                        keys.insert(key.to_owned());
                    }
                }
                if page.is_truncated() == Some(true) {
                    token = page.next_continuation_token().map(str::to_owned);
                } else {
                    break if keys.len() == count && pages >= 3 {
                        Ok(format!("{count} keys across {pages} pages"))
                    } else {
                        Err(format!(
                            "paginated listing saw {} keys in {pages} pages",
                            keys.len()
                        ))
                    };
                }
            }
            Err(error) => break Err(format!("list_objects_v2 {}", service_code(&error))),
        }
    };
    Outcome { name, result }
}

/// Delete every probe object, then confirm the run prefix is empty.
async fn check_delete(store: &dyn ObjectStore, paths: &[Path]) -> Outcome {
    let name = "delete";
    for path in paths {
        if let Err(error) = store.delete(path).await {
            return Outcome {
                name,
                result: Err(format!("delete {}", object_code(&error))),
            };
        }
    }
    if paths.is_empty() {
        return Outcome {
            name,
            result: Ok("nothing to remove".to_owned()),
        };
    }
    let run = run_prefix(&paths[0]);
    let result = match store.list(Some(&run)).try_collect::<Vec<_>>().await {
        Ok(metas) if metas.is_empty() => Ok(format!("{} objects removed", paths.len())),
        Ok(metas) => Err(format!("{} objects remain", metas.len())),
        Err(error) => Err(format!("delete verification list {}", object_code(&error))),
    };
    Outcome { name, result }
}

/// The run prefix is the first segment of any probe path.
fn run_prefix(path: &Path) -> Path {
    let segment = path.as_ref().split('/').next().unwrap_or_default();
    Path::from(segment)
}

async fn run_probe(
    objects: &dyn ObjectStore,
    buckets: &aws_sdk_s3::Client,
    bucket: &str,
    conditional_create: bool,
) -> Vec<Outcome> {
    let run = format!("probe-{}", ulid::Ulid::new());
    let bytes = payload();
    let put_path = Path::from(format!("{run}/put"));
    let create_path = Path::from(format!("{run}/create"));
    let list_prefix = Path::from(format!("{run}/list"));
    let list_count = 6_usize;

    let mut outcomes = Vec::with_capacity(9);
    outcomes.push(check_bucket_exists(buckets, bucket).await);
    outcomes.push(check_put(objects, &put_path, &bytes).await);
    outcomes.push(check_create_new(objects, &create_path, &bytes).await);
    outcomes.push(check_create_existing(objects, &create_path, &bytes, conditional_create).await);
    outcomes.push(check_get(objects, &put_path, &bytes).await);
    outcomes.push(check_head(objects, &put_path, bytes.len()).await);
    outcomes.push(check_list_prefix(objects, &list_prefix, list_count, &bytes).await);
    outcomes.push(
        check_list_pagination(buckets, bucket, &format!("{run}/list/"), list_count).await,
    );
    let mut paths = vec![put_path, create_path];
    for index in 0..list_count {
        paths.push(Path::from(format!("{run}/list/key-{index:02}")));
    }
    outcomes.push(check_delete(objects, &paths).await);
    outcomes
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let coordinates = match resolve(&args) {
        Ok(coordinates) => coordinates,
        Err(detail) => {
            println!(
                "{}",
                serde_json::json!({"check": "arguments", "ok": false, "detail": detail})
            );
            std::process::exit(2);
        }
    };
    let secrets: Vec<&str> = coordinates.secrets().to_vec();
    let objects = match object_client(&coordinates) {
        Ok(objects) => objects,
        Err(detail) => {
            println!(
                "{}",
                serde_json::json!({"check": "arguments", "ok": false, "detail": detail})
            );
            std::process::exit(2);
        }
    };
    let buckets = bucket_client(&coordinates);
    let outcomes = run_probe(
        &objects,
        &buckets,
        &coordinates.bucket,
        coordinates.conditional_create,
    )
    .await;
    let mut passed = 0_usize;
    for outcome in &outcomes {
        if report(outcome, &secrets) {
            passed += 1;
        }
    }
    let failed = outcomes.len() - passed;
    println!(
        "{}",
        serde_json::json!({"check": "probe", "ok": failed == 0, "passed": passed, "failed": failed})
    );
    if failed > 0 {
        std::process::exit(1);
    }
}
