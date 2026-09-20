use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use foundationdb::{Database, tuple::Subspace};
use jiff::Timestamp;
use swarmy_config::Keyring;
use swarmy_core::{
    CredentialKind, CredentialRecord, CredentialScope, CredentialStatus, Lease, LeaseOwnerId,
    encode,
};
use swarmy_store::{Store, StoreError, blob::MemoryBlobStore, credentials::CredentialStore};

static NETWORK: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

const SCOPE: CredentialScope = CredentialScope::Cluster;

struct Fixture {
    credentials: CredentialStore,
    db: Arc<Database>,
    root: Subspace,
}
impl Fixture {
    fn new() -> Option<Self> {
        let Ok(cluster) = std::env::var("SWARMY_FDB_CLUSTER_FILE") else {
            eprintln!("skipping credentials integration: SWARMY_FDB_CLUSTER_FILE unset");
            return None;
        };
        NETWORK.get_or_init(swarmy_store::boot);
        let db = Arc::new(Database::new(Some(&cluster)).unwrap());
        let root =
            Subspace::all().subspace(&("credential-tests", ulid::Ulid::generate().to_string()));
        let store = Store::with_subspace(
            db.clone(),
            root.clone(),
            Arc::new(MemoryBlobStore::default()),
        );
        Some(Self {
            credentials: store.credentials(Keyring::from_bytes([7; 32])),
            db,
            root,
        })
    }
}
fn oauth(access: &str) -> CredentialRecord {
    CredentialRecord {
        kind: CredentialKind::OAuth {
            access: access.into(),
            refresh: "refresh".into(),
            expires_at: Timestamp::from_second(1).unwrap(),
            extra: std::collections::BTreeMap::new(),
        },
        updated_at: Timestamp::now(),
    }
}
fn access(record: &CredentialRecord) -> &str {
    let CredentialKind::OAuth { access, .. } = &record.kind else {
        panic!("expected OAuth")
    };
    access
}

#[tokio::test]
async fn put_get_list_delete_and_wrong_key() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.credentials
        .put_credential(SCOPE, "openai", &oauth("secret"))
        .await
        .unwrap();
    let actual = f
        .credentials
        .get_credential(SCOPE, "openai")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(access(&actual), "secret");
    let listed = f.credentials.list_credentials(SCOPE).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].provider, "openai");
    assert_eq!(listed[0].status, CredentialStatus::Expired);
    let wrong = Store::with_subspace(
        f.db.clone(),
        f.root.clone(),
        Arc::new(MemoryBlobStore::default()),
    )
    .credentials(Keyring::from_bytes([8; 32]));
    assert!(matches!(
        wrong.get_credential(SCOPE, "openai").await,
        Err(StoreError::Keyring)
    ));
    f.credentials
        .delete_credential(SCOPE, "openai")
        .await
        .unwrap();
    assert!(
        f.credentials
            .get_credential(SCOPE, "openai")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.credentials
            .list_credentials(SCOPE)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn simultaneous_refresh_invokes_one_function() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.credentials
        .put_credential(SCOPE, "chatgpt", &oauth("old"))
        .await
        .unwrap();
    let calls = AtomicUsize::new(0);
    let refresh = |_: CredentialRecord| async {
        calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok(oauth("new"))
    };
    let (a, b) = tokio::join!(
        f.credentials
            .refresh_with_lease(SCOPE, "chatgpt", Duration::from_secs(2), refresh),
        f.credentials
            .refresh_with_lease(SCOPE, "chatgpt", Duration::from_secs(2), refresh),
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(a == b);
    assert_eq!(access(&a), "new");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        f.credentials
            .get_credential(SCOPE, "chatgpt")
            .await
            .unwrap()
            .unwrap()
            == a
    );
}

#[tokio::test]
async fn dead_owner_expires_and_stale_write_is_fenced() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.credentials
        .put_credential(SCOPE, "chatgpt", &oauth("old"))
        .await
        .unwrap();
    let trx = f.db.create_trx().unwrap();
    let lease = Lease {
        owner: LeaseOwnerId::from_ulid(ulid::Ulid::generate()),
        expires_at: Timestamp::now()
            .checked_add(Duration::from_millis(100))
            .unwrap(),
        seq: 0,
    };
    trx.set(
        &f.root.pack(&("credential_lease", "cluster", "chatgpt")),
        &encode(&lease).unwrap(),
    );
    trx.commit().await.unwrap();
    let actual = f
        .credentials
        .refresh_with_lease(SCOPE, "chatgpt", Duration::from_secs(2), |_| async {
            Ok(oauth("recovered"))
        })
        .await
        .unwrap();
    assert_eq!(access(&actual), "recovered");
    let result = f
        .credentials
        .refresh_with_lease(SCOPE, "chatgpt", Duration::from_secs(2), |_| async {
            f.credentials
                .put_credential(SCOPE, "chatgpt", &oauth("explicit replacement"))
                .await?;
            Ok(oauth("stale"))
        })
        .await;
    assert!(matches!(result, Err(StoreError::LeaseMismatch)));
    assert_eq!(
        access(
            &f.credentials
                .get_credential(SCOPE, "chatgpt")
                .await
                .unwrap()
                .unwrap()
        ),
        "explicit replacement"
    );
}

#[tokio::test]
async fn refresh_failure_keeps_tokens_and_requires_login() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.credentials
        .put_credential(SCOPE, "chatgpt", &oauth("old"))
        .await
        .unwrap();
    assert!(matches!(
        f.credentials
            .refresh_with_lease(SCOPE, "chatgpt", Duration::from_secs(2), |_| async {
                Err(StoreError::CredentialRefresh)
            })
            .await,
        Err(StoreError::CredentialRefresh)
    ));
    let current = f
        .credentials
        .get_credential(SCOPE, "chatgpt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(access(&current), "old");
    assert_eq!(
        current.status(Timestamp::now()),
        CredentialStatus::NeedsLogin
    );
    assert!(matches!(
        f.credentials
            .refresh_with_lease(SCOPE, "chatgpt", Duration::from_secs(2), |_| async {
                panic!("must not retry a failed rotation")
            })
            .await,
        Err(StoreError::CredentialRefresh)
    ));
}

#[tokio::test]
async fn refresh_cannot_write_after_expiry_or_resurrect_deleted_credentials() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.credentials
        .put_credential(SCOPE, "chatgpt", &oauth("old"))
        .await
        .unwrap();
    let result = f
        .credentials
        .refresh_with_lease(SCOPE, "chatgpt", Duration::from_millis(50), |_| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(oauth("late"))
        })
        .await;
    assert!(matches!(result, Err(StoreError::LeaseMismatch)));
    assert_eq!(
        access(
            &f.credentials
                .get_credential(SCOPE, "chatgpt")
                .await
                .unwrap()
                .unwrap()
        ),
        "old"
    );
    let result = f
        .credentials
        .refresh_with_lease(SCOPE, "chatgpt", Duration::from_secs(2), |_| async {
            f.credentials.delete_credential(SCOPE, "chatgpt").await?;
            Ok(oauth("deleted"))
        })
        .await;
    assert!(matches!(result, Err(StoreError::LeaseMismatch)));
    assert!(
        f.credentials
            .get_credential(SCOPE, "chatgpt")
            .await
            .unwrap()
            .is_none()
    );
}
