#![deny(clippy::disallowed_methods)]
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use foundationdb::{Database, tuple::Subspace};
use jiff::Timestamp;
use swarmy_config::Keyring;
use swarmy_core::{
    CredentialEntryKind, CredentialKind, CredentialRecord, CredentialScope, CredentialStatus,
    Lease, LeaseOwnerId, encode,
};
use swarmy_store::{
    CredentialKey, Store, StoreError, blob::MemoryBlobStore, credentials::CredentialStore,
};

const SCOPE: CredentialScope = CredentialScope::Cluster;

struct Fixture {
    credentials: CredentialStore,
    db: Arc<Database>,
    root: Subspace,
    // Held for its Drop: removes the test subspace even on panic.
    _guard: swarmy_testkit::StackGuard,
}
impl Fixture {
    fn new() -> Option<Self> {
        let stack = swarmy_testkit::Stack::load("credentials")?;
        let db = Arc::new(Database::new(Some(&stack.cluster)).unwrap());
        let root = Subspace::all().subspace(&(stack.prefix.clone(),));
        let store = Store::with_subspace(
            db.clone(),
            root.clone(),
            Arc::new(MemoryBlobStore::default()),
        );
        Some(Self {
            credentials: store.credentials(Keyring::from_bytes([7; 32])),
            db,
            root,
            _guard: swarmy_testkit::StackGuard::new(&stack),
        })
    }
}
fn oauth(access: &str) -> CredentialRecord {
    CredentialRecord {
        bookkeeping: swarmy_core::CredentialBookkeeping::default(),
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
        .put_entry(SCOPE, "openai", "default", &oauth("secret"))
        .await
        .unwrap();
    let actual = f
        .credentials
        .get_entry(SCOPE, "openai", "default")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(access(&actual), "secret");
    let listed = f.credentials.list_entries(SCOPE).await.unwrap();
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
        wrong.get_entry(SCOPE, "openai", "default").await,
        Err(StoreError::Storage(swarmy_store::StorageError::Keyring))
    ));
    f.credentials
        .delete_entry(SCOPE, "openai", "default")
        .await
        .unwrap();
    assert!(
        f.credentials
            .get_entry(SCOPE, "openai", "default")
            .await
            .unwrap()
            .is_none()
    );
    assert!(f.credentials.list_entries(SCOPE).await.unwrap().is_empty());
}

#[tokio::test]
async fn touch_records_last_use_without_changing_the_secret() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.credentials
        .put_entry(SCOPE, "openai", "ready", &api_key("live"))
        .await
        .unwrap();
    // Touching records last use without changing the encrypted credential: a
    // subsequent read returns the same secret, while listings show the use.
    let before = f
        .credentials
        .get_entry(SCOPE, "openai", "ready")
        .await
        .unwrap()
        .unwrap();
    assert!(
        f.credentials
            .list_entries(SCOPE)
            .await
            .unwrap()
            .iter()
            .find(|entry| entry.label == "ready")
            .unwrap()
            .last_used_at
            .is_none()
    );
    f.credentials
        .touch_entry(SCOPE, "openai", "ready")
        .await
        .unwrap();
    let after = f
        .credentials
        .get_entry(SCOPE, "openai", "ready")
        .await
        .unwrap()
        .unwrap();
    assert!(before == after);
    assert!(
        f.credentials
            .list_entries(SCOPE)
            .await
            .unwrap()
            .iter()
            .find(|entry| entry.label == "ready")
            .unwrap()
            .last_used_at
            .is_some()
    );
    assert!(matches!(
        f.credentials.touch_entry(SCOPE, "openai", "missing").await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::CredentialMissing
        ))
    ));
}

#[tokio::test]
async fn simultaneous_refresh_invokes_one_function() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.credentials
        .put_entry(SCOPE, "chatgpt", "default", &oauth("old"))
        .await
        .unwrap();
    let calls = AtomicUsize::new(0);
    let refresh = |_: CredentialRecord| async {
        calls.fetch_add(1, Ordering::SeqCst);
        // The refresh must genuinely take time: the second concurrent
        // refresh may only proceed after the first releases the lease.
        #[expect(
            clippy::disallowed_methods,
            reason = "slow refresh work is the lease contention under test"
        )]
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok(oauth("new"))
    };
    let (a, b) = tokio::join!(
        f.credentials.refresh_entry_with_lease(
            SCOPE,
            "chatgpt",
            "default",
            Duration::from_secs(2),
            refresh
        ),
        f.credentials.refresh_entry_with_lease(
            SCOPE,
            "chatgpt",
            "default",
            Duration::from_secs(2),
            refresh
        ),
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(a == b);
    assert_eq!(access(&a), "new");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        f.credentials
            .get_entry(SCOPE, "chatgpt", "default")
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
        .put_entry(SCOPE, "chatgpt", "default", &oauth("old"))
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
        &f.root
            .pack(&("credential_entry_lease", "cluster", "chatgpt", "default")),
        &encode(&lease).unwrap(),
    );
    trx.commit().await.unwrap();
    let actual = f
        .credentials
        .refresh_entry_with_lease(
            SCOPE,
            "chatgpt",
            "default",
            Duration::from_secs(2),
            |_| async { Ok(oauth("recovered")) },
        )
        .await
        .unwrap();
    assert_eq!(access(&actual), "recovered");
    let result = f
        .credentials
        .refresh_entry_with_lease(
            SCOPE,
            "chatgpt",
            "default",
            Duration::from_secs(2),
            |_| async {
                f.credentials
                    .put_entry(SCOPE, "chatgpt", "default", &oauth("explicit replacement"))
                    .await?;
                Ok(oauth("stale"))
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(StoreError::Fence(
            swarmy_store::FenceError::CredentialRefreshMismatch
        ))
    ));
    assert_eq!(
        access(
            &f.credentials
                .get_entry(SCOPE, "chatgpt", "default")
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
        .put_entry(SCOPE, "chatgpt", "default", &oauth("old"))
        .await
        .unwrap();
    assert!(matches!(
        f.credentials
            .refresh_entry_with_lease(
                SCOPE,
                "chatgpt",
                "default",
                Duration::from_secs(2),
                |_| async {
                    Err(StoreError::Domain(
                        swarmy_store::DomainError::CredentialRefresh,
                    ))
                }
            )
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::CredentialRefresh
        ))
    ));
    let current = f
        .credentials
        .get_entry(SCOPE, "chatgpt", "default")
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
            .refresh_entry_with_lease(
                SCOPE,
                "chatgpt",
                "default",
                Duration::from_secs(2),
                |_| async { panic!("must not retry a failed rotation") }
            )
            .await,
        Err(StoreError::Domain(
            swarmy_store::DomainError::CredentialRefresh
        ))
    ));
}

#[tokio::test]
async fn refresh_cannot_write_after_expiry_or_resurrect_deleted_credentials() {
    let Some(f) = Fixture::new() else {
        return;
    };
    f.credentials
        .put_entry(SCOPE, "chatgpt", "default", &oauth("old"))
        .await
        .unwrap();
    let result = f
        .credentials
        .refresh_entry_with_lease(
            SCOPE,
            "chatgpt",
            "default",
            Duration::from_millis(50),
            |_| async {
                // The refresh must outlast the 50 ms lease so the fencing
                // path triggers mid-refresh.
                #[expect(
                    clippy::disallowed_methods,
                    reason = "slow refresh outlasting the lease is the fencing under test"
                )]
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(oauth("late"))
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(StoreError::Fence(
            swarmy_store::FenceError::CredentialRefreshMismatch
        ))
    ));
    assert_eq!(
        access(
            &f.credentials
                .get_entry(SCOPE, "chatgpt", "default")
                .await
                .unwrap()
                .unwrap()
        ),
        "old"
    );
    let result = f
        .credentials
        .refresh_entry_with_lease(
            SCOPE,
            "chatgpt",
            "default",
            Duration::from_secs(2),
            |_| async {
                f.credentials
                    .delete_entry(SCOPE, "chatgpt", "default")
                    .await?;
                Ok(oauth("deleted"))
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(StoreError::Fence(
            swarmy_store::FenceError::CredentialRefreshMismatch
        ))
    ));
    assert!(
        f.credentials
            .get_entry(SCOPE, "chatgpt", "default")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn labelled_entries_refresh_and_remove_independently() {
    let Some(f) = Fixture::new() else { return };
    f.credentials
        .put_entry(SCOPE, "openai", "default", &oauth("legacy"))
        .await
        .unwrap();
    let first = f.credentials.list_entries(SCOPE).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].label, "default");
    assert_eq!(first[0].kind, CredentialEntryKind::Subscription);
    assert_eq!(f.credentials.list_entries(SCOPE).await.unwrap().len(), 1);
    f.credentials
        .put_entry(SCOPE, "openai", "second", &oauth("second"))
        .await
        .unwrap();
    assert_eq!(f.credentials.list_entries(SCOPE).await.unwrap().len(), 2);
    assert_eq!(
        access(
            &f.credentials
                .first_entry(SCOPE, "openai")
                .await
                .unwrap()
                .unwrap()
                .1
        ),
        "legacy"
    );
    let refreshed = f
        .credentials
        .refresh_entry_with_lease(
            SCOPE,
            "openai",
            "second",
            Duration::from_secs(2),
            |_| async { Ok(oauth("rotated")) },
        )
        .await
        .unwrap();
    assert_eq!(access(&refreshed), "rotated");
    assert_eq!(
        access(
            &f.credentials
                .get_entry(SCOPE, "openai", "default")
                .await
                .unwrap()
                .unwrap()
        ),
        "legacy"
    );
    f.credentials
        .delete_entry(SCOPE, "openai", "second")
        .await
        .unwrap();
    assert!(
        f.credentials
            .get_entry(SCOPE, "openai", "second")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(f.credentials.list_entries(SCOPE).await.unwrap().len(), 1);
}

#[tokio::test]
async fn first_entry_prefers_the_oldest_ready_entry() {
    let Some(f) = Fixture::new() else { return };
    f.credentials
        .put_entry(SCOPE, "openai", "default", &oauth("expired"))
        .await
        .unwrap();
    let ready = CredentialRecord {
        bookkeeping: swarmy_core::CredentialBookkeeping::default(),
        kind: CredentialKind::ApiKey {
            key: "live".into(),
            extra: std::collections::BTreeMap::default(),
        },
        updated_at: Timestamp::now(),
    };
    f.credentials
        .put_entry(SCOPE, "openai", "second", &ready)
        .await
        .unwrap();
    // The older default is expired, so the newer ready entry serves.
    let (label, record) = f
        .credentials
        .first_entry(SCOPE, "openai")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(label, "second");
    assert!(record == ready);
    assert!(
        f.credentials
            .first_entry(SCOPE, "missing")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn replacing_one_entry_fences_its_refresh_without_touching_another() {
    let Some(f) = Fixture::new() else { return };
    f.credentials
        .put_entry(SCOPE, "openai", "primary", &oauth("first"))
        .await
        .unwrap();
    f.credentials
        .put_entry(SCOPE, "openai", "backup", &oauth("backup"))
        .await
        .unwrap();
    let entered = tokio::sync::Notify::new();
    let proceed = tokio::sync::Notify::new();
    let refresh = f.credentials.refresh_entry_with_lease(
        SCOPE,
        "openai",
        "primary",
        Duration::from_secs(3),
        |_| async {
            entered.notify_one();
            proceed.notified().await;
            Ok(oauth("stale"))
        },
    );
    let replace = async {
        entered.notified().await;
        f.credentials
            .put_entry(SCOPE, "openai", "primary", &oauth("replaced"))
            .await
            .unwrap();
        proceed.notify_one();
    };
    let (result, ()) = tokio::join!(refresh, replace);
    assert!(matches!(
        result,
        Err(StoreError::Fence(
            swarmy_store::FenceError::CredentialRefreshMismatch
        ))
    ));
    assert_eq!(
        access(
            &f.credentials
                .get_entry(SCOPE, "openai", "primary")
                .await
                .unwrap()
                .unwrap()
        ),
        "replaced"
    );
    assert_eq!(
        access(
            &f.credentials
                .get_entry(SCOPE, "openai", "backup")
                .await
                .unwrap()
                .unwrap()
        ),
        "backup"
    );
}

#[tokio::test]
async fn default_entry_lists_once() {
    let Some(f) = Fixture::new() else { return };
    let record = CredentialRecord {
        bookkeeping: swarmy_core::CredentialBookkeeping::default(),
        kind: CredentialKind::ApiKey {
            key: "synthetic".into(),
            extra: std::collections::BTreeMap::default(),
        },
        updated_at: Timestamp::now(),
    };
    f.credentials
        .put_entry(SCOPE, "openai", "default", &record)
        .await
        .unwrap();
    let first = f.credentials.list_entries(SCOPE).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].label, "default");
    assert_eq!(first[0].kind, CredentialEntryKind::ApiKey);
    let second = f.credentials.list_entries(SCOPE).await.unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].created_at, first[0].created_at);
    assert!(
        f.credentials
            .get_entry(SCOPE, "openai", "default")
            .await
            .unwrap()
            .unwrap()
            == record
    );
}

#[tokio::test]
async fn entry_labels_list_without_decrypting() {
    let Some(f) = Fixture::new() else {
        return;
    };
    let store = Store::with_subspace(
        f.db.clone(),
        f.root.clone(),
        Arc::new(MemoryBlobStore::default()),
    );
    assert!(
        store
            .credential_entry_labels(SCOPE, "openai")
            .await
            .unwrap()
            .is_empty()
    );
    f.credentials
        .put_entry(SCOPE, "openai", "backup", &oauth("backup"))
        .await
        .unwrap();
    f.credentials
        .put_entry(SCOPE, "openai", "primary", &oauth("primary"))
        .await
        .unwrap();
    assert_eq!(
        store
            .credential_entry_labels(SCOPE, "openai")
            .await
            .unwrap(),
        ["backup", "primary"]
    );
    // A default entry lists alongside the other labels.
    f.credentials
        .put_entry(SCOPE, "anthropic", "default", &oauth("primary"))
        .await
        .unwrap();
    assert_eq!(
        store
            .credential_entry_labels(SCOPE, "anthropic")
            .await
            .unwrap(),
        ["default"]
    );
}

fn api_key(key: &str) -> CredentialRecord {
    CredentialRecord {
        bookkeeping: swarmy_core::CredentialBookkeeping::default(),
        kind: CredentialKind::ApiKey {
            key: key.into(),
            extra: std::collections::BTreeMap::default(),
        },
        updated_at: Timestamp::now(),
    }
}

fn login_needed() -> CredentialRecord {
    let mut record = api_key("");
    record.bookkeeping.needs_login = true;
    record
}

#[tokio::test]
async fn breaker_snapshot_matches_the_gateway_pool_without_decrypting() {
    let Some(f) = Fixture::new() else {
        return;
    };
    let store = Store::with_subspace(
        f.db.clone(),
        f.root.clone(),
        Arc::new(MemoryBlobStore::default()),
    );
    // Without stored entries the provider shares one unlabeled record.
    let empty = store
        .breaker_snapshot(SCOPE, "openai", Timestamp::now())
        .await
        .unwrap();
    assert_eq!(empty.len(), 1);
    assert_eq!(empty[0].key, CredentialKey::provider("openai"));
    assert!(empty[0].open_until.is_none());
    assert!(empty[0].reason.is_none());

    f.credentials
        .put_entry(SCOPE, "openai", "ready", &api_key("live"))
        .await
        .unwrap();
    f.credentials
        .put_entry(SCOPE, "openai", "login", &login_needed())
        .await
        .unwrap();
    f.credentials
        .put_entry(SCOPE, "openai", "expired", &oauth("stale"))
        .await
        .unwrap();
    // While a ready entry exists, entries needing login or past expiry are
    // not candidates, matching the gateway pool.
    let pool = store
        .breaker_snapshot(SCOPE, "openai", Timestamp::now())
        .await
        .unwrap();
    assert_eq!(pool.len(), 1);
    assert_eq!(pool[0].key, CredentialKey::entry("openai", "ready"));
    assert!(pool[0].open_until.is_none());

    // An open breaker attaches its retry time and entry-named reason.
    let until = Timestamp::now()
        .checked_add(Duration::from_secs(60))
        .unwrap();
    store
        .entry_failure(
            &CredentialKey::entry("openai", "ready"),
            until,
            "openai/ready: slow down",
        )
        .await
        .unwrap();
    let parked = store
        .breaker_snapshot(SCOPE, "openai", Timestamp::now())
        .await
        .unwrap();
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].open_until, Some(until));
    assert_eq!(parked[0].reason.as_deref(), Some("openai/ready: slow down"));

    // With no ready entry every stored entry is a candidate, so a provider
    // with no usable key still parks behind its breaker.
    let fallback = store
        .breaker_snapshot(SCOPE, "anthropic", Timestamp::now())
        .await
        .unwrap();
    assert_eq!(fallback.len(), 1);
    f.credentials
        .put_entry(SCOPE, "anthropic", "login", &login_needed())
        .await
        .unwrap();
    f.credentials
        .put_entry(SCOPE, "anthropic", "expired", &oauth("stale"))
        .await
        .unwrap();
    let mut labels: Vec<String> = store
        .breaker_snapshot(SCOPE, "anthropic", Timestamp::now())
        .await
        .unwrap()
        .into_iter()
        .map(|candidate| candidate.key.label.unwrap())
        .collect();
    labels.sort();
    assert_eq!(labels, ["expired", "login"]);

    // A stored default entry is the pool candidate.
    f.credentials
        .put_entry(SCOPE, "xai", "default", &api_key("live"))
        .await
        .unwrap();
    let pool = store
        .breaker_snapshot(SCOPE, "xai", Timestamp::now())
        .await
        .unwrap();
    assert_eq!(pool.len(), 1);
    assert_eq!(pool[0].key, CredentialKey::entry("xai", "default"));
}
