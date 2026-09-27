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
use swarmy_store::{
    CredentialKey, Store, StoreError,
    blob::MemoryBlobStore,
    credentials::{CredentialStore, LegacyMigration},
};

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
        Err(StoreError::Keyring)
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
    assert!(matches!(result, Err(StoreError::LeaseMismatch)));
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
                |_| async { Err(StoreError::CredentialRefresh) }
            )
            .await,
        Err(StoreError::CredentialRefresh)
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
        Err(StoreError::CredentialRefresh)
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
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(oauth("late"))
            },
        )
        .await;
    assert!(matches!(result, Err(StoreError::LeaseMismatch)));
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
    assert!(matches!(result, Err(StoreError::LeaseMismatch)));
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
    assert_eq!(first[0].kind, "subscription");
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
    assert!(matches!(result, Err(StoreError::LeaseMismatch)));
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
    assert_eq!(first[0].kind, "api-key");
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
        kind: CredentialKind::ApiKey {
            key: key.into(),
            extra: std::collections::BTreeMap::default(),
        },
        updated_at: Timestamp::now(),
    }
}

fn login_needed() -> CredentialRecord {
    let mut record = api_key("");
    let CredentialKind::ApiKey { extra, .. } = &mut record.kind else {
        unreachable!("api_key builds ApiKey records");
    };
    extra.insert("needs_login".into(), "true".into());
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

/// Encrypt a record the way the retired `put_credential` writer on master
/// did: the plaintext record sealed under the legacy associated data
/// `encode(&(scope, provider))`, with the provider string (not an entry
/// identity) as the identity. This pins the wire format independently of
/// the crate's current entry helpers, so the boot migration stays
/// compatible with rows written before the old API was retired.
fn encrypt_legacy_row(
    keyring: &Keyring,
    scope: CredentialScope,
    provider: &str,
    record: &CredentialRecord,
) -> Vec<u8> {
    use chacha20poly1305::{
        XChaCha20Poly1305, XNonce,
        aead::{Aead, KeyInit, Payload},
    };
    use rand::TryRngCore;
    let plaintext = encode(record).unwrap();
    let mut nonce = [0; 24];
    rand::rngs::OsRng.try_fill_bytes(&mut nonce).unwrap();
    let associated = encode(&(scope, provider)).unwrap();
    let ciphertext = XChaCha20Poly1305::new(keyring.key().into())
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &associated,
            },
        )
        .unwrap();
    let mut bytes = nonce.to_vec();
    bytes.extend(ciphertext);
    bytes
}

async fn write_legacy_row(fixture: &Fixture, provider: &str, record: &CredentialRecord) {
    let key = fixture
        .root
        .pack(&("credential", SCOPE.to_string(), provider));
    let lease_key = fixture
        .root
        .pack(&("credential_lease", SCOPE.to_string(), provider));
    let bytes = encrypt_legacy_row(&Keyring::from_bytes([7; 32]), SCOPE, provider, record);
    fixture
        .db
        .run(|trx, _| async move {
            trx.set(&key, &bytes);
            trx.set(&lease_key, b"stale-lease");
            Ok(())
        })
        .await
        .unwrap();
}

async fn raw_row(fixture: &Fixture, table: &str, provider: &str) -> Option<Vec<u8>> {
    let key = fixture.root.pack(&(table, SCOPE.to_string(), provider));
    fixture
        .db
        .run(|trx, _| async move { Ok(trx.get(&key, false).await?.map(|value| value.to_vec())) })
        .await
        .unwrap()
}

#[tokio::test]
async fn boot_migration_moves_a_legacy_row_to_the_default_entry() {
    let Some(fixture) = Fixture::new() else {
        return;
    };
    let record = api_key("legacy-secret");
    write_legacy_row(&fixture, "openai", &record).await;
    let outcome = fixture
        .credentials
        .migrate_legacy_credentials()
        .await
        .unwrap();
    assert_eq!(outcome.written, 1);
    assert_eq!(outcome.cleared, 0);
    let entry = fixture
        .credentials
        .get_entry(SCOPE, "openai", "default")
        .await
        .unwrap()
        .unwrap();
    assert!(entry == record);
    assert!(raw_row(&fixture, "credential", "openai").await.is_none());
    assert!(
        raw_row(&fixture, "credential_lease", "openai")
            .await
            .is_none()
    );
    let again = fixture
        .credentials
        .migrate_legacy_credentials()
        .await
        .unwrap();
    assert!(again == LegacyMigration::default());
}

#[tokio::test]
async fn boot_migration_clears_but_does_not_overwrite_an_existing_default_entry() {
    let Some(fixture) = Fixture::new() else {
        return;
    };
    let current = api_key("current-secret");
    fixture
        .credentials
        .put_entry(SCOPE, "anthropic", "default", &current)
        .await
        .unwrap();
    write_legacy_row(&fixture, "anthropic", &api_key("stale-secret")).await;
    let outcome = fixture
        .credentials
        .migrate_legacy_credentials()
        .await
        .unwrap();
    assert_eq!(outcome.written, 0);
    assert_eq!(outcome.cleared, 1);
    let entry = fixture
        .credentials
        .get_entry(SCOPE, "anthropic", "default")
        .await
        .unwrap()
        .unwrap();
    assert!(entry == current);
    assert!(raw_row(&fixture, "credential", "anthropic").await.is_none());
}
