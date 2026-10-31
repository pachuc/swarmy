#![deny(clippy::disallowed_methods)]
use super::*;

#[test]
fn inline_values_match_by_content() {
    let stored = stored_encoding(&"small".to_owned()).unwrap();
    assert!(stored.1.is_none());
    assert!(stored_matches(&stored.0, &"small".to_owned()).unwrap());
    assert!(!stored_matches(&stored.0, &"other".to_owned()).unwrap());
}

#[test]
fn blob_values_match_by_content_address_without_a_download() {
    let large = "x".repeat(INLINE_LIMIT + 1);
    let (stored, blob) = stored_encoding(&large).unwrap();
    let (key, bytes) = blob.unwrap();
    assert_eq!(key, blob::blob_key(&bytes));
    assert!(stored_matches(&stored, &large).unwrap());
    let mut other = large;
    other.replace_range(0..1, "y");
    assert!(!stored_matches(&stored, &other).unwrap());
}

#[test]
fn inline_preparation_refuses_values_that_would_need_an_upload() {
    assert!(prepare_inline(&"short".to_owned()).is_ok());
    assert!(matches!(
        prepare_inline(&"x".repeat(INLINE_LIMIT + 1)),
        Err(StoreError::Storage(StorageError::TooLarge))
    ));
}
