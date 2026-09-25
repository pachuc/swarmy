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

pub async fn upload(
    State(state): State<AppState>,
    Query(query): Query<UploadQuery>,
    body: Body,
) -> ApiResult<api::ImageUpload> {
    let store = state.store.clone();
    let objects = state.objects.clone();
    // Always consume the streamed body before responding, even for replayed
    // or rejected uploads: answering while the client is still writing tears
    // down the connection and surfaces as a transport error client-side.
    let directory = tempfile::Builder::new()
        .prefix("swarmy-upload-")
        .tempdir()
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    let path = directory.path().join("disk.ext4");
    let mut file = tokio::fs::File::create(&path)
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    let mut stream = body.into_data_stream();
    let mut received = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid_request"))?;
        received = received || !chunk.is_empty();
        file.write_all(&chunk)
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    }
    if !received {
        return Err(invalid("uploaded image is empty"));
    }
    swarmy_image::validate_label(&query.name).map_err(|_| invalid("invalid image name"))?;
    swarmy_image::validate_label(&query.tag).map_err(|_| invalid("invalid image tag"))?;
    if query.idempotency_key.is_empty() || query.idempotency_key.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid_idempotency_key"));
    }
    if query.memory_mib.is_some_and(|memory| memory == 0) {
        return Err(invalid("sandbox memory_mib must be positive"));
    }
    let scratch: Vec<String> = query
        .scratch
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .collect();
    let display = query.display.unwrap_or(false);
    let replay_key = format!("images:upload:{}", query.idempotency_key);
    // A retried upload observes the completed response. The lock is held
    // only for the replay check and response store; chunk publication below
    // must not block other control-plane mutations.
    {
        let _guard = state.mutation_guard().await;
        if let Some(value) = state.store.api_replay(&replay_key).await.map_err(storage)? {
            return serde_json::from_value(value)
                .map(Json)
                .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "corrupt_replay"));
        }
    }
    file.flush()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "storage_error"))?;
    drop(file);
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
            &query.name,
            &ImageTag(query.tag.clone()),
            manifest_id,
            &scratch,
            query.memory_mib,
            display,
        )
        .await
        .map_err(storage)?;
    let response = api::ImageUpload {
        name: query.name.clone(),
        tag: query.tag.clone(),
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
