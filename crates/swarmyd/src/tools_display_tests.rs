#![deny(clippy::disallowed_methods)]
use super::display_result;
use swarmy_core::ToolResult;

#[tokio::test]
async fn sandbox_supplied_image_key_is_removed() {
    let mut result = ToolResult::Completed {
        output: String::new(),
        title: String::new(),
        metadata: std::collections::BTreeMap::from([(
            "image_object_key".into(),
            serde_json::json!("blobs/untrusted"),
        )]),
    };
    super::extract_image_blob(&object_store::memory::InMemory::new(), &mut result)
        .await
        .unwrap();
    let ToolResult::Completed { metadata, .. } = result else {
        panic!("completed result changed")
    };
    assert!(!metadata.contains_key("image_object_key"));
}

#[test]
fn screenshot_payload_becomes_image_metadata() {
    let result = display_result(
        "browser_screenshot",
        serde_json::json!({
            "output": "PNG screenshot", "image_base64": "iVBORw0KGgo="
        }),
    )
    .unwrap();
    let ToolResult::Completed {
        output, metadata, ..
    } = result
    else {
        panic!("expected screenshot result");
    };
    assert_eq!(output, "PNG screenshot");
    assert_eq!(metadata["image_media_type"], "image/png");
    assert_eq!(metadata["image_base64"], "iVBORw0KGgo=");
}
