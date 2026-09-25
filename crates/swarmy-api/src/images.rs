//! Image publication. Clients build ext4 files locally and stream the raw
//! bytes; the control plane chunks them, uploads with the same GC protection
//! as a local build, and registers the image. A client machine never needs
//! object store credentials.
use super::{ApiResult, AppState, error, storage, volume};
use axum::{
    Json,
    body::Body,
    extract::{Query, State},
    http::StatusCode,
};
use futures::StreamExt as _;
use serde::Deserialize;
use swarmy_api_types as api;
use swarmy_core::{ImageTag, ManifestId};
use tokio::io::AsyncWriteExt;

#[derive(Deserialize)]
pub struct UploadQuery {
    name: String,
    tag: String,
    idempotency_key: String,
    scratch: Option<String>,
    memory_mib: Option<u64>,
    display: Option<bool>,
}

/// Largest sandbox memory a published image may request. Real nodes size
/// sandboxes in MiB; anything above a TiB is a malformed recipe, not a real
/// machine.
const MAX_MEMORY_MIB: u64 = 1024 * 1024;

struct ValidatedUpload {
    name: String,
    tag: String,
    idempotency_key: String,
    scratch: Vec<String>,
    memory_mib: Option<u64>,
    display: bool,
}

fn invalid(message: &str) -> (StatusCode, Json<api::ApiError>) {
    (
        StatusCode::BAD_REQUEST,
        Json(api::ApiError {
            code: "invalid_request".into(),
            message: message.into(),
            provider_text: None,
        }),
    )
}

/// Check the name, tag, idempotency key, and image requirements before
/// reading the body, mirroring the client-side `Recipe::load` checks so a
/// malformed recipe fails fast instead of after a multi-gigabyte upload.
fn validate(query: &UploadQuery) -> Result<ValidatedUpload, (StatusCode, Json<api::ApiError>)> {
    swarmy_image::validate_label(&query.name).map_err(|_| invalid("invalid image name"))?;
    swarmy_image::validate_label(&query.tag).map_err(|_| invalid("invalid image tag"))?;
    if query.idempotency_key.is_empty() || query.idempotency_key.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_idempotency_key"));
    }
    if let Some(memory) = query.memory_mib
        && (memory == 0 || memory > MAX_MEMORY_MIB)
    {
        return Err(invalid("sandbox memory_mib must be within 1-1048576 MiB"));
    }
    let scratch: Vec<String> = query
        .scratch
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .collect();
    for (index, path) in scratch.iter().enumerate() {
        let path = std::path::Path::new(path);
        if !path.is_absolute()
            || path.components().any(|part| {
                !matches!(
                    part,
                    std::path::Component::RootDir | std::path::Component::Normal(_)
                )
            })
            || path == std::path::Path::new("/")
            || scratch[..index].iter().any(|earlier| {
                path.starts_with(earlier) || std::path::Path::new(earlier).starts_with(path)
            })
        {
            return Err(invalid(
                "scratch paths must be distinct, absolute, and non-overlapping",
            ));
        }
    }
    Ok(ValidatedUpload {
        name: query.name.clone(),
        tag: query.tag.clone(),
        idempotency_key: query.idempotency_key.clone(),
        scratch,
        memory_mib: query.memory_mib,
        display: query.display.unwrap_or(false),
    })
}

/// Consume a streamed body without storing it. Rejected uploads still drain
/// the body before responding: answering while the client is still writing
/// tears down the connection and surfaces as a transport error client-side.
async fn drain(body: Body) {
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        if chunk.is_err() {
            break;
        }
    }
}

/// Stream an image to the control plane for chunking and registration.
///
/// # Errors
///
/// Returns a 4xx response for an invalid name, tag, idempotency key, image
/// requirement, or oversized body, and a 5xx response when the store or the
/// object upload fails.
pub async fn upload(
    State(state): State<AppState>,
    Query(query): Query<UploadQuery>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> ApiResult<api::ImageUpload> {
    let validated = match validate(&query) {
        Ok(validated) => validated,
        Err(error) => {
            drain(body).await;
            return Err(error);
        }
    };
    // A well-behaved client sends `Content-Length`; reject an oversized
    // upload before spooling gigabytes the server would only delete.
    if let Some(length) = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.parse::<u64>().ok())
        && length > state.upload_max_bytes
    {
        drain(body).await;
        return Err(oversized(state.upload_max_bytes));
    }
    let replay_key = format!("images:upload:{}", validated.idempotency_key);
    // A retried upload observes the completed response without re-spooling
    // the image to disk. The body is still drained so the client finishes
    // writing before the replayed answer. The lock is held only for the
    // replay check and response store; chunk publication below must not
    // block other control-plane mutations.
    {
        let _guard = state.mutation_guard().await;
        if let Some(value) = state.store.api_replay(&replay_key).await.map_err(storage)? {
            drain(body).await;
            return serde_json::from_value(value)
                .map(Json)
                .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt_replay"));
        }
    }
    let (_spool, path, _size) = spool(&state, body).await?;
    let store = state.store.clone();
    let objects = state.objects.clone();
    let built = swarmy_volume::image::upload_image_protected(&path, objects, store.clone())
        .await
        .map_err(|failure| match failure {
            swarmy_volume::image::ImageError::Volume(inner) => volume(inner),
            _ => error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"),
        })?;
    let manifest_id = ManifestId::from_ulid(ulid::Ulid::generate());
    store
        .put_manifest(manifest_id, &built.header)
        .await
        .map_err(storage)?;
    store
        .put_image_with_requirements(
            &validated.name,
            &ImageTag(validated.tag.clone()),
            manifest_id,
            &validated.scratch,
            validated.memory_mib,
            validated.display,
        )
        .await
        .map_err(storage)?;
    let response = api::ImageUpload {
        name: validated.name.clone(),
        tag: validated.tag.clone(),
        manifest_id: manifest_id.to_string(),
        header: serde_json::to_value(&built.header)
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?,
        size: built.header.size,
        chunks_total: built.chunks_total,
        chunks_stored: built.chunks_stored,
        chunks_uploaded: built.chunks_uploaded,
    };
    {
        let _guard = state.mutation_guard().await;
        let value = serde_json::to_value(&response)
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "encoding_error"))?;
        state
            .store
            .put_api_replay(&replay_key, value)
            .await
            .map_err(storage)?;
    }
    Ok(Json(response))
}

/// Stream the body into a spool file under the control node's data
/// directory, enforcing the configured maximum size. The returned directory
/// guard deletes the spool on every path; oversized and empty bodies drain
/// first so the client finishes writing before the rejection.
async fn spool(
    state: &AppState,
    body: Body,
) -> Result<(tempfile::TempDir, std::path::PathBuf, u64), (StatusCode, Json<api::ApiError>)> {
    let max_bytes = state.upload_max_bytes;
    if let Err(create_error) = std::fs::create_dir_all(&state.upload_dir) {
        tracing::warn!(error = %create_error, "upload spool directory unavailable");
        drain(body).await;
        return Err(error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"));
    }
    // `/tmp` is often a small tmpfs that cannot hold a multi-gigabyte image.
    let directory = tempfile::Builder::new()
        .prefix("swarmy-upload-")
        .tempdir_in(&state.upload_dir)
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    let path = directory.path().join("disk.ext4");
    let mut file = tokio::fs::File::create(&path)
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    let mut stream = body.into_data_stream();
    let mut size: u64 = 0;
    let mut too_large = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_request"))?;
        if too_large {
            continue;
        }
        size = size.saturating_add(chunk.len() as u64);
        if size > max_bytes {
            // Keep draining so the client finishes writing before the 413;
            // the partial spool is deleted with the directory guard.
            too_large = true;
            continue;
        }
        if chunk.is_empty() {
            continue;
        }
        file.write_all(&chunk)
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    }
    if too_large {
        return Err(oversized(max_bytes));
    }
    if size == 0 {
        return Err(invalid("uploaded image is empty"));
    }
    file.flush()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    drop(file);
    Ok((directory, path, size))
}

/// Reject an upload over the configured limit with a 413 before spooling or
/// after draining an oversized stream.
fn oversized(max_bytes: u64) -> (StatusCode, Json<api::ApiError>) {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        Json(api::ApiError {
            code: "image_too_large".into(),
            message: format!("uploaded image exceeds the {max_bytes} byte limit"),
            provider_text: None,
        }),
    )
}

/// Delete spool directories a crashed upload left behind. Each upload spools
/// under `swarmy-upload-*` inside the configured directory with a guard that
/// deletes it on every path, so anything present at startup is orphaned.
pub fn sweep_stale_uploads(upload_dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(upload_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("swarmy-upload-") {
            continue;
        }
        if let Err(error) = std::fs::remove_dir_all(entry.path()) {
            tracing::warn!(path = %entry.path().display(), %error, "stale upload not swept");
        }
    }
}
