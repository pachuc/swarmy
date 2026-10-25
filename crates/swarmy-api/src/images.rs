//! Image publication. Clients build ext4 files locally and stream the data extents
//! of the sparse file. The control plane chunks them, uploads with the same GC
//! protection as a local build, and registers the image. A client machine never needs
//! object store credentials.
use super::idempotency::{IdempotencyKey, record_replay, replayed};
use super::{ApiFailure, ApiResult, AppState, storage, volume};
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
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[derive(Deserialize)]
pub(crate) struct UploadQuery {
    name: String,
    tag: String,
    size: u64,
    idempotency_key: String,
    scratch: Option<String>,
    memory_mib: Option<u64>,
    display: Option<bool>,
}

/// Largest sandbox memory a published image may request. Real nodes size
/// sandboxes in MiB; anything above a TiB is a malformed recipe, not a real
/// machine.
const MAX_MEMORY_MIB: u64 = 1024 * 1024;
/// Bound the chunker walk now that upload limits count only data extents.
const MAX_IMAGE_SIZE: u64 = 1 << 40;
const EXTENT_HEADER_BYTES: usize = 16;

struct ValidatedUpload {
    size: u64,
    name: String,
    tag: String,
    idempotency_key: IdempotencyKey,
    scratch: Vec<String>,
    memory_mib: Option<u64>,
    display: bool,
}

fn invalid(message: &str) -> ApiFailure {
    ApiFailure::new(StatusCode::BAD_REQUEST, "invalid_request", message)
}

/// Check the name, tag, idempotency key, and image requirements before
/// reading the body, mirroring the client-side `Recipe::load` checks so a
/// malformed recipe fails fast instead of after a multi-gigabyte upload.
fn validate(query: &UploadQuery) -> Result<ValidatedUpload, ApiFailure> {
    if query.size == 0
        || !query
            .size
            .is_multiple_of(u64::from(swarmy_core::CHUNK_SIZE))
        || query.size > MAX_IMAGE_SIZE
    {
        return Err(invalid(
            "image size must be a positive multiple of the chunk size, at most 1 TiB",
        ));
    }
    swarmy_image::validate_label(&query.name).map_err(|_| invalid("invalid image name"))?;
    swarmy_image::validate_label(&query.tag).map_err(|_| invalid("invalid image tag"))?;
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
        size: query.size,
        name: query.name.clone(),
        tag: query.tag.clone(),
        idempotency_key: IdempotencyKey::parse(&query.idempotency_key)?,
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
pub(crate) async fn upload(
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
    // upload body (data plus framing, not holes) before spooling. The body
    // is dropped without draining so an oversized stream stops early instead
    // of uploading fully before the rejection.
    if let Some(length) = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.parse::<u64>().ok())
        && length > state.upload_max_bytes
    {
        return Err(oversized(state.upload_max_bytes));
    }
    let replay_key = validated.idempotency_key.scoped("images:upload");
    // A retried upload observes the completed response without re-spooling
    // the image to disk. The body is still drained so the client finishes
    // writing before the replayed answer. The lock is held only for the
    // replay check and response store; chunk publication below must not
    // block other control-plane mutations.
    {
        let _guard = state.mutation_guard().await;
        if let Some(done) = replayed::<api::ImageUpload>(&state, &replay_key).await? {
            drain(body).await;
            return Ok(Json(done));
        }
    }
    let (_spool, path) = spool(&state, validated.size, body).await?;
    let store = state.store.clone();
    let objects = state.objects.clone();
    let built = swarmy_volume::image::upload_image_protected(&path, objects, store.clone())
        .await
        .map_err(|failure| match failure {
            swarmy_volume::image::ImageError::Volume(inner) => volume(inner),
            other => ApiFailure::caused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_error",
                "image upload failed",
                &other,
            ),
        })?;
    let manifest_id = ManifestId::from_ulid(ulid::Ulid::generate());
    store
        .put_manifest(manifest_id, &built.header)
        .await
        .map_err(storage)?;
    store
        .put_image(
            &validated.name,
            &ImageTag(validated.tag.clone()),
            manifest_id,
            Some(swarmy_store::PutImageOptions {
                scratch: validated.scratch.clone(),
                memory_mib: validated.memory_mib,
                display: validated.display,
            }),
        )
        .await
        .map_err(storage)?;
    let response = api::ImageUpload {
        name: validated.name.clone(),
        tag: validated.tag.clone(),
        manifest_id: manifest_id.to_string(),
        header: super::views::image_header(&built.header),
        size: built.header.size,
        chunks_total: built.chunks_total,
        chunks_stored: built.chunks_stored,
        chunks_uploaded: built.chunks_uploaded,
    };
    {
        let _guard = state.mutation_guard().await;
        record_replay(&state, &replay_key, &response).await?;
    }
    Ok(Json(response))
}

/// Decode an extent header, rejecting overlap, empty ranges and size overflow.
fn parse_extent(header: &[u8], next_free: u64, size: u64) -> Result<(u64, u64), ApiFailure> {
    let offset = u64::from_le_bytes(header[..8].try_into().expect("eight offset bytes"));
    let length = u64::from_le_bytes(header[8..16].try_into().expect("eight length bytes"));
    if length == 0 || offset < next_free || offset.checked_add(length).is_none_or(|end| end > size)
    {
        return Err(invalid(
            "image extents must be non-empty, ascending, and inside the image size",
        ));
    }
    Ok((offset, length))
}

fn spool_failed(cause: &std::io::Error) -> ApiFailure {
    ApiFailure::caused(
        StatusCode::INTERNAL_SERVER_ERROR,
        "storage_error",
        "upload spool write failed",
        cause,
    )
}

/// Stream extent records into a sparse spool file under the control node's
/// data directory. The limit counts data plus framing, not holes. The directory
/// guard deletes the spool on every path; an oversized stream stops early.
async fn spool(
    state: &AppState,
    size: u64,
    body: Body,
) -> Result<(tempfile::TempDir, std::path::PathBuf), ApiFailure> {
    let max_bytes = state.upload_max_bytes;
    if let Err(create_error) = std::fs::create_dir_all(&state.upload_dir) {
        drain(body).await;
        return Err(ApiFailure::caused(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_error",
            "upload spool directory unavailable",
            &create_error,
        ));
    }
    // `/tmp` is often a small tmpfs that cannot hold a multi-gigabyte image.
    let directory = tempfile::Builder::new()
        .prefix("swarmy-upload-")
        .tempdir_in(&state.upload_dir)
        .map_err(|cause| spool_failed(&cause))?;
    let path = directory.path().join("disk.ext4");
    let mut file = tokio::fs::File::create(&path)
        .await
        .map_err(|cause| spool_failed(&cause))?;
    file.set_len(size)
        .await
        .map_err(|cause| spool_failed(&cause))?;
    let mut stream = body.into_data_stream();
    let mut header = Vec::with_capacity(EXTENT_HEADER_BYTES);
    let mut remaining: u64 = 0;
    let mut next_free: u64 = 0;
    let mut received: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| invalid("upload body could not be read"))?;
        received = received.saturating_add(chunk.len() as u64);
        if received > max_bytes {
            return Err(oversized(max_bytes));
        }
        let mut bytes: &[u8] = &chunk;
        while !bytes.is_empty() {
            if remaining == 0 {
                let take = (EXTENT_HEADER_BYTES - header.len()).min(bytes.len());
                header.extend_from_slice(&bytes[..take]);
                bytes = &bytes[take..];
                if header.len() == EXTENT_HEADER_BYTES {
                    let (offset, length) = parse_extent(&header, next_free, size)?;
                    file.seek(std::io::SeekFrom::Start(offset))
                        .await
                        .map_err(|cause| spool_failed(&cause))?;
                    remaining = length;
                    next_free = offset + length;
                    header.clear();
                }
            } else {
                let take = usize::try_from(remaining)
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                file.write_all(&bytes[..take])
                    .await
                    .map_err(|cause| spool_failed(&cause))?;
                remaining -= take as u64;
                bytes = &bytes[take..];
            }
        }
    }
    if remaining != 0 || !header.is_empty() {
        return Err(invalid("upload body ended inside an extent"));
    }
    file.flush().await.map_err(|cause| spool_failed(&cause))?;
    drop(file);
    Ok((directory, path))
}

/// Reject a body over the configured extent-byte limit with a 413. The header
/// and spool paths both stop without draining the remainder.
fn oversized(max_bytes: u64) -> ApiFailure {
    ApiFailure::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "image_too_large",
        format!("uploaded image exceeds the {max_bytes} byte limit"),
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
            tracing::warn!(path = %entry.path().display(), error = %swarmy_core::error_chain(&error), "stale upload not swept");
        }
    }
}

#[cfg(test)]
mod tests {
    #![deny(clippy::disallowed_methods)]
    use super::parse_extent;

    #[test]
    fn parse_extent_validates_bounds_and_order() {
        let header =
            |offset: u64, length: u64| [offset.to_le_bytes(), length.to_le_bytes()].concat();
        assert_eq!(parse_extent(&header(5, 3), 0, 8).unwrap(), (5, 3));
        assert_eq!(parse_extent(&header(8, 1), 8, 9).unwrap(), (8, 1));
        for (offset, length, next, size) in [
            (0, 0, 0, 8),
            (2, 3, 3, 8),
            (8, 1, 0, 8),
            (u64::MAX, 2, 0, u64::MAX),
        ] {
            assert!(parse_extent(&header(offset, length), next, size).is_err());
        }
    }
}
