use super::*;
use swarmy_core::{NodeId, NodeToken};

#[tokio::test]
async fn node_credentials_issue_replace_and_revoke() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let node = NodeId::from_ulid(Ulid::generate());
    let first = test.store.issue_node_credential(node).await.unwrap();
    assert_eq!(first.node(), node);
    assert!(test.store.verify_node_credential(&first).await.unwrap());
    assert!(
        test.store
            .verify_node_credential(&NodeToken::parse(&first.reveal()).unwrap())
            .await
            .unwrap()
    );
    assert!(
        !test
            .store
            .verify_node_credential(&NodeToken::new(node, [1; 32]))
            .await
            .unwrap()
    );
    let second = test.store.issue_node_credential(node).await.unwrap();
    assert!(!test.store.verify_node_credential(&first).await.unwrap());
    assert!(test.store.verify_node_credential(&second).await.unwrap());
    assert!(test.store.revoke_node_credential(node).await.unwrap());
    assert!(!test.store.verify_node_credential(&second).await.unwrap());
    assert!(!test.store.revoke_node_credential(node).await.unwrap());
    assert!(
        !test
            .store
            .verify_node_credential(&NodeToken::new(
                NodeId::from_ulid(Ulid::generate()),
                [2; 32]
            ))
            .await
            .unwrap()
    );
}
