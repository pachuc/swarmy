//! Decode failures keep their codec cause instead of collapsing to a bare
//! corruption report, and validation rejections name their reason.
use crate::{DomainError, StorageError, StoreError};

fn causes(error: &StoreError) -> Vec<String> {
    let mut chain = Vec::new();
    let mut current: Option<&dyn std::error::Error> = Some(error);
    while let Some(next) = current {
        chain.push(next.to_string());
        current = next.source();
    }
    chain
}

#[test]
fn tuple_unpack_failures_keep_their_cause() {
    let error = StoreError::from(foundationdb::tuple::PackError::MissingBytes);
    assert!(matches!(
        error,
        StoreError::Storage(StorageError::Decode(_))
    ));
    let chain = causes(&error);
    assert_eq!(chain[0], "stored key or blob is corrupt");
    assert!(
        chain.iter().any(|cause| cause.contains("missing bytes")),
        "unexpected chain: {chain:?}"
    );
}

#[test]
fn json_and_slice_and_id_failures_keep_their_cause() {
    let json: serde_json::Error =
        serde_json::from_str::<serde_json::Value>("{oops").expect_err("invalid JSON");
    let chain = causes(&StoreError::from(json));
    assert!(chain.iter().any(|cause| cause.contains("key must be a string")));

    let slice = <[u8; 16]>::try_from([0; 4].as_slice()).expect_err("short slice");
    let chain = causes(&StoreError::from(slice));
    assert!(chain[0].contains("corrupt"));

    let id = "nope".parse::<ulid::Ulid>().expect_err("bad id");
    let chain = causes(&StoreError::from(id));
    assert!(chain.iter().any(|cause| cause.contains("invalid length")));
}

#[test]
fn validation_rejections_name_their_reason() {
    let tool = StoreError::Domain(DomainError::InvalidToolCall(
        "unknown tool frobnicate".into(),
    ));
    assert_eq!(
        tool.to_string(),
        "invalid tool call: unknown tool frobnicate"
    );
    let session = StoreError::Domain(DomainError::InvalidSessionRecord(
        "plan steps must have a nonempty description".into(),
    ));
    assert_eq!(
        session.to_string(),
        "invalid session record: plan steps must have a nonempty description"
    );
    let randomness = StoreError::Storage(StorageError::Randomness(
        std::io::Error::new(std::io::ErrorKind::Other, "entropy pool is empty").into(),
    ));
    assert_eq!(randomness.to_string(), "keyring randomness unavailable");
}
