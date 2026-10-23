//! PostgreSQL integration coverage for the database-backed asset cache.

use std::{
    collections::HashMap,
    sync::Arc,
    sync::Mutex,
    time::{Duration, Instant},
};

use chrono::Utc;
use sqlx::{PgPool, Row};
use uuid::Uuid;
use werrss::{
    archive::asset_store::{
        AssetBlob, AssetBlobKey, AssetBlobMetadata, AssetBlobStore, AssetBlobStoreError,
        AssetCachePolicy, AssetInput, AssetRead, AssetRefreshPolicy, AssetRepairPolicy,
    },
    domain::{
        article::{ArticleObservationVersion, NewArticle},
        source::{NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
    },
    persistence::{
        repositories::{
            article_repository::ArticleTransactionRepository,
            asset_repository::{
                ArticleRefreshOutcome, AssetRepairEnqueueResult, AssetRepairRestoreResult,
                AssetRepositoryError, AssetTransactionRepository, PostgresAssetStore,
            },
            job_repository::{JobLease, JobQueue, PostgresJobRepository},
            opendal_blob_store::OpenDalFsBlobStore,
            source_repository::SourceTransactionRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

const FIRST_BYTES: &[u8] = b"first-image";
const SECOND_BYTES: &[u8] = b"second-img";

#[derive(Clone, Default)]
struct MemoryBlobStore {
    entries: Arc<Mutex<HashMap<String, MemoryBlobEntry>>>,
    failed_deletes: Arc<Mutex<usize>>,
    failed_puts_after_write: Arc<Mutex<usize>>,
}

impl MemoryBlobStore {
    fn fail_next_deletes(&self, count: usize) {
        *self.failed_deletes.lock().unwrap() = count;
    }

    fn fail_next_puts_after_write(&self, count: usize) {
        *self.failed_puts_after_write.lock().unwrap() = count;
    }
}

#[derive(Clone)]
struct MemoryBlobEntry {
    blob: AssetBlob,
    present: bool,
}

#[async_trait::async_trait]
impl AssetBlobStore for MemoryBlobStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn read(&self, key: &AssetBlobKey) -> Result<Option<AssetBlob>, AssetBlobStoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .get(key.as_str())
            .filter(|entry| entry.present)
            .map(|entry| entry.blob.clone()))
    }

    async fn stat(
        &self,
        key: &AssetBlobKey,
    ) -> Result<Option<AssetBlobMetadata>, AssetBlobStoreError> {
        self.entries
            .lock()
            .unwrap()
            .get(key.as_str())
            .map(|entry| {
                AssetBlobMetadata::new(
                    entry.blob.checksum().to_owned(),
                    entry.blob.media_type().to_owned(),
                    entry.blob.byte_size(),
                    entry.present,
                )
            })
            .transpose()
    }

    async fn put(&self, key: &AssetBlobKey, blob: &AssetBlob) -> Result<(), AssetBlobStoreError> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get_mut(key.as_str()) {
            if entry.blob != *blob {
                return Err(AssetBlobStoreError::Conflict);
            }
            entry.present = true;
        } else {
            entries.insert(
                key.as_str().to_owned(),
                MemoryBlobEntry {
                    blob: blob.clone(),
                    present: true,
                },
            );
        }
        drop(entries);
        let mut failed_puts = self.failed_puts_after_write.lock().unwrap();
        if *failed_puts > 0 {
            *failed_puts -= 1;
            return Err(AssetBlobStoreError::Backend(
                "injected ambiguous put failure".to_owned(),
            ));
        }
        Ok(())
    }

    async fn delete(&self, key: &AssetBlobKey) -> Result<(), AssetBlobStoreError> {
        let mut failed_deletes = self.failed_deletes.lock().unwrap();
        if *failed_deletes > 0 {
            *failed_deletes -= 1;
            return Err(AssetBlobStoreError::Backend(
                "injected delete failure".to_owned(),
            ));
        }
        if let Some(entry) = self.entries.lock().unwrap().get_mut(key.as_str()) {
            entry.present = false;
        }
        Ok(())
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_unit_of_work_reserves_capacity_before_callers_lock_articles(pool: PgPool) {
    let input = asset_input("https://cdn.example/capacity-order.png", FIRST_BYTES, 0);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();

    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT pg_try_advisory_xact_lock(hashtextextended('asset-capacity', 0))",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "asset preparation must reserve capacity before a caller acquires source/article rows"
    );

    unit_of_work.commit().await.unwrap();
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT pg_try_advisory_xact_lock(hashtextextended('asset-capacity', 0))",
    )
    .fetch_one(&pool)
    .await
    .unwrap());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_repository_routes_binary_operations_through_an_injected_backend(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["injected-backend"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input("https://cdn.example/injected-backend.png", FIRST_BYTES, 0);

    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "injected-backend", std::slice::from_ref(&input))
        .await
        .unwrap()
        .remove(0);
    unit_of_work.commit().await.unwrap();

    let key = AssetBlobKey::for_blob_id(stored.blob_id());
    assert_eq!(
        backend.read(&key).await.unwrap().unwrap().bytes(),
        FIRST_BYTES
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT data IS NULL FROM asset_blobs WHERE id = $1")
            .bind(stored.blob_id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "an injected backend should own bytes instead of duplicating them in PostgreSQL"
    );

    let read = PostgresAssetStore::with_blob_store(pool, policy, backend)
        .read_and_touch(stored.id())
        .await
        .unwrap();
    assert!(matches!(
        read,
        Some(AssetRead::Available { ref bytes, .. }) if bytes == FIRST_BYTES
    ));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn dropping_external_asset_unit_cleans_implicit_rollback_allocations(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["implicit-rollback"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input("https://cdn.example/implicit-rollback.png", FIRST_BYTES, 0);
    let key;
    {
        let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
        let mut unit_of_work = factory
            .begin_with_assets(std::slice::from_ref(&input))
            .await
            .unwrap();
        let stored = unit_of_work
            .assets(policy)
            .store_for_article(source_id, "implicit-rollback", std::slice::from_ref(&input))
            .await
            .unwrap()
            .remove(0);
        key = AssetBlobKey::for_blob_id(stored.blob_id());
        assert!(backend.read(&key).await.unwrap().is_some());
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(backend.read(&key).await.unwrap().is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_rollback_keeps_evicted_bytes_and_cleans_new_allocations(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["rollback-first", "rollback-second"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let first = asset_input("https://cdn.example/rollback-first.png", FIRST_BYTES, 0);
    let second = asset_input("https://cdn.example/rollback-second.png", SECOND_BYTES, 0);
    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());

    let mut first_work = factory
        .begin_with_assets(std::slice::from_ref(&first))
        .await
        .unwrap();
    let first_stored = first_work
        .assets(policy)
        .store_for_article(source_id, "rollback-first", std::slice::from_ref(&first))
        .await
        .unwrap()
        .remove(0);
    first_work.commit().await.unwrap();

    let first_key = AssetBlobKey::for_blob_id(first_stored.blob_id());
    let mut second_work = factory
        .begin_with_assets(std::slice::from_ref(&second))
        .await
        .unwrap();
    let second_stored = second_work
        .assets(policy)
        .store_for_article(source_id, "rollback-second", std::slice::from_ref(&second))
        .await
        .unwrap()
        .remove(0);
    backend.fail_next_deletes(1);
    second_work.rollback().await.unwrap();

    assert_eq!(
        backend.read(&first_key).await.unwrap().unwrap().bytes(),
        FIRST_BYTES
    );
    assert!(backend
        .read(&AssetBlobKey::for_blob_id(second_stored.blob_id()))
        .await
        .unwrap()
        .is_none());
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT fetch_status = 'available' FROM asset_records WHERE id = $1"
    )
    .bind(first_stored.id())
    .fetch_one(&pool)
    .await
    .unwrap());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn external_backend_capacity_includes_blobs_from_prior_asset_views(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["capacity-first", "capacity-second"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let first = asset_input("https://cdn.example/capacity-first.png", FIRST_BYTES, 0);
    let second = asset_input("https://cdn.example/capacity-second.png", SECOND_BYTES, 0);
    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
    let mut unit_of_work = factory
        .begin_with_assets(&[first.clone(), second.clone()])
        .await
        .unwrap();

    let first_stored = unit_of_work
        .assets(policy)
        .replace_for_article(source_id, "capacity-first", std::slice::from_ref(&first))
        .await
        .unwrap()
        .remove(0);
    let second_stored = unit_of_work
        .assets(policy)
        .replace_for_article(source_id, "capacity-second", std::slice::from_ref(&second))
        .await
        .unwrap()
        .remove(0);
    unit_of_work.commit().await.unwrap();

    assert!(backend
        .read(&AssetBlobKey::for_blob_id(first_stored.blob_id()))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        backend
            .read(&AssetBlobKey::for_blob_id(second_stored.blob_id()))
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        SECOND_BYTES
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(SUM(byte_size), 0)::BIGINT FROM asset_blobs WHERE data IS NULL"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        21
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn queued_external_blob_is_not_reused_by_asset_deduplication(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["queued-reuse"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input("https://cdn.example/queued-reuse.png", FIRST_BYTES, 0);
    let old = store_asset_with_backend(
        &pool,
        policy,
        source_id,
        "queued-reuse",
        input.clone(),
        backend.clone(),
    )
    .await;
    sqlx::query(
        "INSERT INTO asset_blob_deletion_queue (blob_id) VALUES ($1)
         ON CONFLICT (blob_id) DO NOTHING",
    )
    .bind(old.blob_id())
    .execute(&pool)
    .await
    .unwrap();

    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let replacement = unit_of_work
        .assets(policy)
        .replace_for_article(source_id, "queued-reuse", std::slice::from_ref(&input))
        .await
        .unwrap()
        .remove(0);
    assert_ne!(replacement.blob_id(), old.blob_id());
    unit_of_work.commit().await.unwrap();

    assert!(backend
        .read(&AssetBlobKey::for_blob_id(old.blob_id()))
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        backend
            .read(&AssetBlobKey::for_blob_id(replacement.blob_id()))
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        FIRST_BYTES
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn failed_post_commit_eviction_stays_queued_and_hidden_from_readers(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["queued-first", "queued-second"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let first = asset_input("https://cdn.example/queued-first.png", FIRST_BYTES, 0);
    let second = asset_input("https://cdn.example/queued-second.png", SECOND_BYTES, 0);
    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
    let mut unit_of_work = factory
        .begin_with_assets(&[first.clone(), second.clone()])
        .await
        .unwrap();
    let first_stored = unit_of_work
        .assets(policy)
        .replace_for_article(source_id, "queued-first", std::slice::from_ref(&first))
        .await
        .unwrap()
        .remove(0);
    let second_stored = unit_of_work
        .assets(policy)
        .replace_for_article(source_id, "queued-second", std::slice::from_ref(&second))
        .await
        .unwrap()
        .remove(0);
    backend.fail_next_deletes(1);
    assert!(unit_of_work.commit().await.is_err());

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_blob_deletion_queue")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(matches!(
        PostgresAssetStore::with_blob_store(pool.clone(), policy, backend.clone())
            .read_and_touch(first_stored.id())
            .await
            .unwrap(),
        Some(AssetRead::Missing { .. })
    ));

    PostgresAssetStore::with_blob_store(
        pool.clone(),
        test_policy(0, Duration::from_secs(30), 1024),
        backend.clone(),
    )
    .maintenance()
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_blob_deletion_queue")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(backend
        .read(&AssetBlobKey::for_blob_id(first_stored.blob_id()))
        .await
        .unwrap()
        .is_none());
    assert!(backend
        .read(&AssetBlobKey::for_blob_id(second_stored.blob_id()))
        .await
        .unwrap()
        .is_some());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn failed_savepoint_cleanup_is_durable_for_external_blobs(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["cleanup-queue"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input(
        "https://cdn.example/cleanup-queue.png",
        FIRST_BYTES,
        u32::MAX,
    );
    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    backend.fail_next_deletes(2);

    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "cleanup-queue", std::slice::from_ref(&input))
        .await;
    assert!(result.is_err());
    assert!(unit_of_work.commit().await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_blob_cleanup_queue")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );

    PostgresAssetStore::with_blob_store(pool.clone(), policy, backend.clone())
        .maintenance()
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_blob_cleanup_queue")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn ambiguous_external_put_is_tracked_and_cleaned_by_the_savepoint(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["ambiguous-put"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    backend.fail_next_puts_after_write(1);
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input(
        "https://cdn.example/ambiguous-put.png",
        FIRST_BYTES,
        u32::MAX,
    );
    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();

    assert!(unit_of_work
        .assets(policy)
        .store_for_article(source_id, "ambiguous-put", std::slice::from_ref(&input))
        .await
        .is_err());
    unit_of_work.commit().await.unwrap();

    assert!(backend
        .entries
        .lock()
        .unwrap()
        .values()
        .all(|entry| !entry.present));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn ambiguous_external_repair_put_is_cleaned_after_transaction_rollback(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["ambiguous-repair-put"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input(
        "https://cdn.example/ambiguous-repair-put.png",
        FIRST_BYTES,
        0,
    );
    let stored = store_asset_with_backend(
        &pool,
        policy,
        source_id,
        "ambiguous-repair-put",
        input.clone(),
        backend.clone(),
    )
    .await;
    backend
        .delete(&AssetBlobKey::for_blob_id(stored.blob_id()))
        .await
        .unwrap();

    let store = PostgresAssetStore::with_blob_store(pool.clone(), policy, backend.clone());
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair should be admitted, got {other:?}"),
    };
    let lease = claim_repair_job(&pool).await;
    backend.fail_next_puts_after_write(1);

    assert!(store
        .restore_missing_asset(&lease, stored.id(), &input)
        .await
        .is_err());
    assert!(backend
        .entries
        .lock()
        .unwrap()
        .values()
        .all(|entry| !entry.present));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_blob_cleanup_queue")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    finish_repair_job(&pool, job_id).await;
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn failed_later_asset_view_does_not_clean_prior_allocations(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["prior-allocation", "failed-allocation"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let first = asset_input("https://cdn.example/prior-allocation.png", FIRST_BYTES, 0);
    let failed = asset_input(
        "https://cdn.example/failed-allocation.png",
        SECOND_BYTES,
        u32::MAX,
    );
    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
    let mut unit_of_work = factory
        .begin_with_assets(&[first.clone(), failed.clone()])
        .await
        .unwrap();
    let first_stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "prior-allocation", std::slice::from_ref(&first))
        .await
        .unwrap()
        .remove(0);
    assert!(unit_of_work
        .assets(policy)
        .store_for_article(
            source_id,
            "failed-allocation",
            std::slice::from_ref(&failed)
        )
        .await
        .is_err());
    unit_of_work.commit().await.unwrap();

    assert!(backend
        .read(&AssetBlobKey::for_blob_id(first_stored.blob_id()))
        .await
        .unwrap()
        .is_some());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_retains_orphan_metadata_when_external_delete_fails(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["orphan-delete-retry"]).await;
    let backend = Arc::new(MemoryBlobStore::default());
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset_with_backend(
        &pool,
        policy,
        source_id,
        "orphan-delete-retry",
        asset_input(
            "https://cdn.example/orphan-delete-retry.png",
            FIRST_BYTES,
            0,
        ),
        backend.clone(),
    )
    .await;
    let key = AssetBlobKey::for_blob_id(stored.blob_id());
    sqlx::query("DELETE FROM article_assets WHERE asset_record_id = $1")
        .bind(stored.id())
        .execute(&pool)
        .await
        .unwrap();
    backend.fail_next_deletes(1);

    let store = PostgresAssetStore::with_blob_store(pool.clone(), policy, backend.clone());
    assert!(store.maintenance().await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM asset_blobs WHERE id = $1")
            .bind(stored.blob_id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(backend.read(&key).await.unwrap().is_some());
    assert_eq!(store.maintenance().await.unwrap().orphan_blobs, 1);
    assert!(backend.read(&key).await.unwrap().is_none());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn stores_asset_bytes_and_updates_last_accessed_on_read(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["read-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "read-article",
        asset_input("https://cdn.example/read.png", FIRST_BYTES, 0),
    )
    .await;

    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '1 hour' WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let read = PostgresAssetStore::new(pool.clone(), policy)
        .read_and_touch(stored.id())
        .await
        .unwrap()
        .expect("referenced asset should be readable");
    let AssetRead::Available {
        media_type,
        checksum,
        bytes,
    } = read
    else {
        panic!("stored asset should have binary data");
    };
    assert_eq!(media_type, "image/png");
    assert_eq!(checksum, stored.checksum());
    assert_eq!(bytes, FIRST_BYTES);

    let recently_accessed: bool = sqlx::query_scalar(
        "SELECT last_accessed_at > clock_timestamp() - interval '1 minute' FROM asset_blobs WHERE id = $1",
    )
    .bind(stored.blob_id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(recently_accessed, "a successful read should touch the blob");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_read_waits_for_eviction_lock_and_keeps_bytes_available(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["read-lock-article"]).await;
    let policy = test_policy(0, Duration::from_secs(1), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "read-lock-article",
        asset_input("https://cdn.example/read-lock.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query(
        "UPDATE asset_blobs
         SET last_accessed_at = clock_timestamp() - interval '1 day'
         WHERE id = $1",
    )
    .bind(stored.blob_id())
    .execute(&pool)
    .await
    .unwrap();

    let mut lock_connection = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *lock_connection)
        .await
        .unwrap();

    let asset_id = stored.id();
    let read_task = tokio::spawn({
        let pool = pool.clone();
        async move {
            PostgresAssetStore::new(pool, policy)
                .read_and_touch(asset_id)
                .await
        }
    });
    wait_for_capacity_waiter(&pool).await;
    assert!(
        !read_task.is_finished(),
        "asset read must not finish while maintenance holds the capacity lock"
    );

    sqlx::query("SELECT pg_advisory_unlock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *lock_connection)
        .await
        .unwrap();
    let read = tokio::time::timeout(Duration::from_secs(2), read_task)
        .await
        .expect("asset read should finish after the capacity lock is released")
        .unwrap()
        .unwrap()
        .expect("referenced asset should be readable");
    assert!(matches!(
        read,
        AssetRead::Available { ref bytes, .. } if bytes == FIRST_BYTES
    ));

    let maintenance = PostgresAssetStore::new(pool.clone(), policy)
        .maintenance()
        .await
        .unwrap();
    assert_eq!(
        maintenance.stale_blobs, 0,
        "a read that waited for the eviction decision must refresh last_accessed_at"
    );
    assert!(matches!(
        PostgresAssetStore::new(pool, policy)
            .read_and_touch(asset_id)
            .await
            .unwrap(),
        Some(AssetRead::Available { .. })
    ));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_write_rejects_provider_signed_urls(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["signed-url-policy"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let signed = asset_input(
        "https://cdn.example/signed.png?X-Amz-Signature=secret",
        FIRST_BYTES,
        0,
    );
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&signed))
        .await
        .unwrap();
    assert_eq!(
        unit_of_work
            .assets(policy)
            .store_for_article(source_id, "signed-url-policy", &[signed])
            .await
            .unwrap_err(),
        AssetRepositoryError::InvalidUrl
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_write_preserves_ordinary_query_url_identity(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["query-url-policy"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input(
        "https://cdn.example/image.png?width=640&fit=cover",
        FIRST_BYTES,
        0,
    );
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "query-url-policy", &[input])
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    let source_url: String =
        sqlx::query_scalar("SELECT source_url FROM asset_records WHERE id = $1")
            .bind(stored[0].id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        source_url,
        "https://cdn.example/image.png?width=640&fit=cover"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn confirmed_expiry_deduplication_leaves_other_generation_unreserved(pool: PgPool) {
    let (first_asset_id, first_lease) = admit_refresh_job(&pool, "shared-refresh-article").await;
    let source_id = SourceId::from_uuid(
        Uuid::parse_str(first_lease.job.payload()["source_id"].as_str().unwrap()).unwrap(),
    );
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let second = store_asset(
        &pool,
        policy,
        source_id,
        "shared-refresh-article",
        asset_input("https://cdn.example/second-expired.png", SECOND_BYTES, 1),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(second.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresAssetStore::new(pool.clone(), policy);
    assert!(matches!(
        store.enqueue_repair(second.id()).await.unwrap(),
        AssetRepairEnqueueResult::Enqueued { .. }
    ));
    let repair = claim_repair_job(&pool).await;
    assert_eq!(store.finish_confirmed_expiry(&repair, second.id()).await.unwrap(),
        werrss::persistence::repositories::asset_repository::AssetRefreshAdmissionResult::NoEligibleArticle);
    assert_eq!(
        sqlx::query_as::<_, (String, i64, Option<Uuid>)>(
            "SELECT e.state, l.attempts, s.active_chain_id
         FROM asset_refresh_generation_entries e
         JOIN asset_record_refresh_state s USING (asset_record_id)
         JOIN asset_article_refresh_lineage l USING (source_id, review_id)
         WHERE e.asset_record_id = $1",
        )
        .bind(second.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        ("unseen".to_owned(), 1, None)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT active_chain_id FROM asset_record_refresh_state WHERE asset_record_id = $1",
        )
        .bind(first_asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        Some(Uuid::parse_str(first_lease.job.payload()["chain_id"].as_str().unwrap()).unwrap())
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn complete_normal_acquisition_reconciles_other_generation_entries(pool: PgPool) {
    let (asset_id, lease) = admit_refresh_job(&pool, "reconciled-article").await;
    let source_id = SourceId::from_uuid(
        Uuid::parse_str(lease.job.payload()["source_id"].as_str().unwrap()).unwrap(),
    );
    let other_asset_id = Uuid::new_v4();
    sqlx::query("INSERT INTO asset_record_refresh_state (asset_record_id, requested_generation, served_generation) VALUES ($1, 1, 1)")
        .bind(other_asset_id).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO asset_refresh_generation_entries (asset_record_id, generation, ordinal, source_id, review_id) VALUES ($1, 1, 1, $2, 'reconciled-article')")
        .bind(other_asset_id).bind(source_id.as_uuid()).execute(&pool).await.unwrap();
    let factory = UnitOfWorkFactory::new(pool.clone());
    // Match the production source-sync/backfill path, which reserves the
    // capacity and recovery-admission locks before touching these rows.
    let mut uow = factory
        .begin_article_acquisition_with_assets(&[])
        .await
        .unwrap();
    uow.assets(test_policy(0, Duration::from_secs(30), 1024))
        .reset_article_refresh_lineage(source_id, "reconciled-article")
        .await
        .unwrap();
    uow.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM asset_refresh_generation_entries WHERE asset_record_id = $1"
        )
        .bind(other_asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "done"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM asset_refresh_generation_entries WHERE asset_record_id = $1"
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "in_flight"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT completed_generation FROM asset_record_refresh_state WHERE asset_record_id = $1"
        )
        .bind(other_asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn periodic_pending_pass_resumes_due_generation_without_cache_miss(pool: PgPool) {
    let (asset_id, lease) = admit_refresh_job(&pool, "periodic-resume").await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut uow = factory.begin().await.unwrap();
    uow.assets(policy)
        .finish_article_refresh(
            &lease,
            ArticleRefreshOutcome::Inconclusive,
            AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    uow.commit().await.unwrap();
    sqlx::query("UPDATE asset_article_refresh_lineage SET next_allowed_at = clock_timestamp() - interval '1 second'")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE asset_refresh_generation_entries SET not_before = clock_timestamp() - interval '1 second' WHERE state = 'cooling'")
        .execute(&pool).await.unwrap();
    let store = PostgresAssetStore::new(pool.clone(), policy);
    store.resume_pending_refreshes().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_repair_jobs r JOIN jobs j ON j.id = r.job_id
         WHERE r.expired_asset_record_id = $1 AND j.status = 'queued'",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn repeated_cache_misses_coalesce_one_active_chain_follow_up(pool: PgPool) {
    let (asset_id, _lease) = admit_refresh_job(&pool, "coalesced-active-follow-up").await;
    let store =
        PostgresAssetStore::new(pool.clone(), test_policy(0, Duration::from_secs(30), 1024))
            .with_refresh_policy(AssetRefreshPolicy::default());

    assert_eq!(
        store.enqueue_repair(asset_id).await.unwrap(),
        AssetRepairEnqueueResult::Retryable
    );
    assert_eq!(
        store.enqueue_repair(asset_id).await.unwrap(),
        AssetRepairEnqueueResult::Retryable
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT requested_generation FROM asset_record_refresh_state
             WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        2,
        "repeated misses during one active generation should request only one follow-up"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn pending_generation_marks_entries_done_when_live_relationship_was_removed(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let review_id = "stale-refresh-snapshot";
    create_source_and_articles(&pool, source_id, &[review_id]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        cache_policy,
        source_id,
        review_id,
        asset_input("https://cdn.example/stale-snapshot.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO asset_record_refresh_state
             (asset_record_id, requested_generation, completed_generation,
              served_generation, active_chain_id)
         VALUES ($1, 1, 0, 1, NULL)",
    )
    .bind(stored.id())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id, state)
         VALUES ($1, 1, 1, $2, $3, 'unseen')",
    )
    .bind(stored.id())
    .bind(source_id.as_uuid())
    .bind(review_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM article_assets WHERE asset_record_id = $1")
        .bind(stored.id())
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), cache_policy)
        .with_refresh_policy(AssetRefreshPolicy::default());
    assert_eq!(
        store.enqueue_repair(stored.id()).await.unwrap(),
        AssetRepairEnqueueResult::Retryable
    );
    assert_eq!(
        sqlx::query_as::<_, (String, i64)>(
            "SELECT e.state, s.completed_generation
             FROM asset_refresh_generation_entries e
             JOIN asset_record_refresh_state s USING (asset_record_id)
             WHERE e.asset_record_id = $1 AND e.generation = 1",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        ("done".to_owned(), 1)
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn pending_refresh_cache_miss_does_not_increment_requested_generation(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["pending-refresh-cooling"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "pending-refresh-cooling",
        asset_input(
            "https://cdn.example/pending-refresh-cooling.png",
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO asset_record_refresh_state
             (asset_record_id, requested_generation, completed_generation,
              served_generation, active_chain_id)
         VALUES ($1, 2, 0, 1, NULL)",
    )
    .bind(stored.id())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id,
              state, not_before)
         VALUES ($1, 1, 1, $2, 'pending-refresh-cooling', 'cooling',
                 clock_timestamp() + interval '1 hour')",
    )
    .bind(stored.id())
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy)
        .with_refresh_policy(AssetRefreshPolicy::default());
    assert_eq!(
        store.enqueue_repair(stored.id()).await.unwrap(),
        AssetRepairEnqueueResult::Retryable
    );
    let state = sqlx::query_as::<_, (i64, i64)>(
        "SELECT requested_generation, served_generation
         FROM asset_record_refresh_state WHERE asset_record_id = $1",
    )
    .bind(stored.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, (2, 1));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_repair_jobs arj
             JOIN jobs j ON j.id = arj.job_id
             WHERE arj.asset_record_id = $1
               AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "a cooling pending snapshot must not admit a repair job"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn pending_refresh_cache_miss_resumes_existing_snapshot_without_counter_increment(
    pool: PgPool,
) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["pending-refresh-resume"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "pending-refresh-resume",
        asset_input(
            "https://cdn.example/pending-refresh-resume.png",
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO asset_record_refresh_state
             (asset_record_id, requested_generation, completed_generation,
              served_generation, active_chain_id)
         VALUES ($1, 2, 0, 1, NULL)",
    )
    .bind(stored.id())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id, state)
         VALUES ($1, 1, 1, $2, 'pending-refresh-resume', 'unseen')",
    )
    .bind(stored.id())
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy)
        .with_refresh_policy(AssetRefreshPolicy::default());
    let result = store.enqueue_repair(stored.id()).await.unwrap();
    let job_id = match result {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("pending snapshot should resume, got {other:?}"),
    };
    let state = sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>)>(
        "SELECT requested_generation, completed_generation, served_generation, active_chain_id
         FROM asset_record_refresh_state WHERE asset_record_id = $1",
    )
    .bind(stored.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state.0, 2);
    assert_eq!(state.1, 0);
    assert_eq!(state.2, 1);
    assert!(state.3.is_some());
    let queued = sqlx::query_as::<_, (i64, Uuid)>(
        "SELECT arj.chain_ordinal, arj.chain_id
         FROM asset_repair_jobs arj JOIN jobs j ON j.id = arj.job_id
         WHERE j.id = $1 AND j.job_type = 'asset_refresh' AND j.status = 'queued'",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(queued.0, 1);
    assert_eq!(Some(queued.1), state.3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation = 1",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
        "resume must not append snapshot entries"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn pending_refresh_cache_miss_resumes_a_due_cooling_entry(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let review_id = "pending-refresh-cooling-resume";
    create_source_and_articles(&pool, source_id, &[review_id]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        cache_policy,
        source_id,
        review_id,
        asset_input(
            "https://cdn.example/pending-cooling-resume.png",
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, next_allowed_at, exhausted, last_outcome)
         VALUES ($1, $2, 1, clock_timestamp() - interval '1 second', FALSE, 'inconclusive')",
    )
    .bind(source_id.as_uuid())
    .bind(review_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_record_refresh_state
             (asset_record_id, requested_generation, completed_generation,
              served_generation, active_chain_id)
         VALUES ($1, 1, 0, 1, NULL)",
    )
    .bind(stored.id())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id, state, not_before)
         VALUES ($1, 1, 1, $2, $3, 'cooling', clock_timestamp() - interval '1 second')",
    )
    .bind(stored.id())
    .bind(source_id.as_uuid())
    .bind(review_id)
    .execute(&pool)
    .await
    .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), cache_policy)
        .with_refresh_policy(AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap());
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("due cooling entry should resume from its snapshot: {other:?}"),
    };

    let job = sqlx::query_as::<_, (String, i64, i64, i64, i64)>(
        "SELECT j.job_type, arj.chain_ordinal, ars.served_generation,
                ars.requested_generation, ars.completed_generation
         FROM jobs j JOIN asset_repair_jobs arj ON arj.job_id = j.id
         JOIN asset_record_refresh_state ars ON ars.asset_record_id = $2
         WHERE j.id = $1",
    )
    .bind(job_id)
    .bind(stored.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(job, ("asset_refresh".to_owned(), 1, 1, 1, 0));
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT active_chain_id IS NOT NULL FROM asset_record_refresh_state WHERE asset_record_id = $1",
    ).bind(stored.id()).fetch_one(&pool).await.unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT attempts FROM asset_article_refresh_lineage WHERE source_id = $1 AND review_id = $2",
        ).bind(source_id.as_uuid()).bind(review_id).fetch_one(&pool).await.unwrap(),
        2,
        "resuming a due entry reserves one additional bounded attempt"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_refresh_generation_entries WHERE asset_record_id = $1",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
        "resumption must not create a new generation snapshot"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn unresolvable_refresh_metadata_succeeds_only_the_claimed_job(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["stale-refresh-metadata"]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        cache_policy,
        source_id,
        "stale-refresh-metadata",
        asset_input(
            "https://cdn.example/stale-refresh-metadata.png",
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresAssetStore::new(pool.clone(), cache_policy)
        .with_repair_policy(AssetRepairPolicy::new(2, 2, Duration::from_secs(60), 3).unwrap())
        .with_refresh_policy(AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap());
    let repair_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("public repair should be admitted, got {other:?}"),
    };
    let repair_lease = claim_repair_job(&pool).await;
    assert_eq!(repair_lease.job.id(), repair_id);
    let refresh_id = match store
        .finish_confirmed_expiry(&repair_lease, stored.id())
        .await
        .unwrap()
    {
        werrss::persistence::repositories::asset_repository::AssetRefreshAdmissionResult::Enqueued {
            job_id,
            ..
        } => job_id,
        other => panic!("confirmed expiry should create refresh job, got {other:?}"),
    };
    let refresh_lease = PostgresJobRepository::new(pool.clone())
        .claim_next(
            "asset-refresh-test-worker",
            Utc::now(),
            chrono::Duration::minutes(5),
            &[werrss::domain::job::JobType::AssetRefresh],
        )
        .await
        .unwrap()
        .expect("refresh job should be claimable");
    assert_eq!(refresh_lease.job.id(), refresh_id);
    sqlx::query("DELETE FROM asset_repair_jobs WHERE job_id = $1")
        .bind(refresh_id)
        .execute(&pool)
        .await
        .unwrap();
    let chain_before = sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>)>(
        "SELECT requested_generation, completed_generation, served_generation, active_chain_id
         FROM asset_record_refresh_state WHERE asset_record_id = $1",
    )
    .bind(stored.id())
    .fetch_one(&pool)
    .await
    .unwrap();

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .assets(cache_policy)
        .complete_unresolvable_article_refresh(&refresh_lease)
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(refresh_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "succeeded"
    );
    let chain_after = sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>)>(
        "SELECT requested_generation, completed_generation, served_generation, active_chain_id
         FROM asset_record_refresh_state WHERE asset_record_id = $1",
    )
    .bind(stored.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(chain_after, chain_before);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(repair_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "succeeded",
        "terminalizing stale refresh metadata must not modify the prior repair job"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn malformed_refresh_payload_terminalizes_its_current_snapshot_entry(pool: PgPool) {
    let (asset_id, old_lease) = admit_refresh_job(&pool, "malformed-refresh-payload").await;
    sqlx::query(
        "UPDATE jobs
         SET status = 'queued', lease_owner = NULL, lease_token = NULL,
             lease_until = NULL, heartbeat_at = NULL, payload_json = '{}'::jsonb
         WHERE id = $1",
    )
    .bind(old_lease.job.id())
    .execute(&pool)
    .await
    .unwrap();
    let lease = PostgresJobRepository::new(pool.clone())
        .claim_next(
            "malformed-refresh-worker",
            Utc::now(),
            chrono::Duration::minutes(5),
            &[werrss::domain::job::JobType::AssetRefresh],
        )
        .await
        .unwrap()
        .expect("malformed refresh job should remain claimable");

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .assets(test_policy(0, Duration::from_secs(30), 1024))
        .complete_unresolvable_article_refresh(&lease)
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation = 1 AND ordinal = 1",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "done"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<Uuid>)>(
            "SELECT completed_generation, active_chain_id
             FROM asset_record_refresh_state WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        (1, None)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(lease.job.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "succeeded"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn superseded_snapshot_entry_cannot_finish_a_refresh(pool: PgPool) {
    let (asset_id, lease) = admit_refresh_job(&pool, "superseded-entry").await;
    sqlx::query(
        "UPDATE asset_refresh_generation_entries
         SET state = 'done', not_before = NULL
         WHERE asset_record_id = $1 AND generation = 1 AND ordinal = 1",
    )
    .bind(asset_id)
    .execute(&pool)
    .await
    .unwrap();

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    let result = unit_of_work
        .assets(test_policy(0, Duration::from_secs(30), 1024))
        .finish_article_refresh(
            &lease,
            ArticleRefreshOutcome::Succeeded,
            AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap(),
        )
        .await;
    assert!(
        matches!(result, Err(AssetRepositoryError::InvalidMetadata)),
        "a snapshot entry retired by reconciliation must not be finalized by its old owner: {result:?}"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn ordinary_completion_reports_entries_retired_by_the_selector(pool: PgPool) {
    let (asset_id, lease) = admit_refresh_job(&pool, "selector-retired-entry").await;
    let source_id: Uuid = lease.job.payload()["source_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id, state)
         VALUES ($1, 1, 2, $2, 'vanished-article', 'unseen')",
    )
    .bind(asset_id)
    .bind(source_id)
    .execute(&pool)
    .await
    .unwrap();

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    let finish = unit_of_work
        .assets(test_policy(0, Duration::from_secs(30), 1024))
        .finish_article_refresh(
            &lease,
            ArticleRefreshOutcome::Deleted,
            AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        finish.generation_completed,
        "completion must be measured after the selector retires stale entries"
    );
    assert!(finish.next_job_id.is_none());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn cache_miss_returns_retryable_while_article_lineage_is_cooling(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["cooling-lineage-miss"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "cooling-lineage-miss",
        asset_input(
            "https://cdn.example/cooling-lineage-miss.png",
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, next_allowed_at, exhausted, last_outcome)
         VALUES ($1, 'cooling-lineage-miss', 1,
                 clock_timestamp() + interval '10 minutes', FALSE, 'inconclusive')",
    )
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    assert_eq!(
        store.enqueue_repair(stored.id()).await.unwrap(),
        AssetRepairEnqueueResult::Retryable,
        "a cooling article lineage keeps the miss retryable instead of admitting a repair"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn cache_miss_is_exhausted_once_every_article_lineage_is_exhausted(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["exhausted-lineage-miss"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "exhausted-lineage-miss",
        asset_input(
            "https://cdn.example/exhausted-lineage-miss.png",
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, next_allowed_at, exhausted, last_outcome)
         VALUES ($1, 'exhausted-lineage-miss', 3, NULL, TRUE, 'exhausted')",
    )
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    assert_eq!(
        store.enqueue_repair(stored.id()).await.unwrap(),
        AssetRepairEnqueueResult::Exhausted,
        "no live article may be refreshed, so the miss is terminal"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn normal_acquisition_completes_only_the_generations_it_superseded(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["reconciled-target", "unrelated-target"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let affected = store_asset(
        &pool,
        policy,
        source_id,
        "reconciled-target",
        asset_input("https://cdn.example/reconciled-target.png", FIRST_BYTES, 0),
    )
    .await;
    let unrelated = store_asset(
        &pool,
        policy,
        source_id,
        "unrelated-target",
        asset_input("https://cdn.example/unrelated-target.png", SECOND_BYTES, 0),
    )
    .await;
    for (asset_id, review_id, state, not_before) in [
        (
            affected.id(),
            "reconciled-target",
            "unseen",
            None::<chrono::DateTime<Utc>>,
        ),
        (unrelated.id(), "unrelated-target", "done", None),
    ] {
        sqlx::query(
            "INSERT INTO asset_record_refresh_state
                 (asset_record_id, requested_generation, completed_generation, served_generation)
             VALUES ($1, 1, 0, 1)",
        )
        .bind(asset_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO asset_refresh_generation_entries
                 (asset_record_id, generation, ordinal, source_id, review_id, state, not_before)
             VALUES ($1, 1, 1, $2, $3, $4, $5)",
        )
        .bind(asset_id)
        .bind(source_id.as_uuid())
        .bind(review_id)
        .bind(state)
        .bind(not_before)
        .execute(&pool)
        .await
        .unwrap();
    }

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .assets(policy)
        .reset_article_refresh_lineage(source_id, "reconciled-target")
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT completed_generation FROM asset_record_refresh_state
             WHERE asset_record_id = $1",
        )
        .bind(affected.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
        "the superseded generation should complete"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT completed_generation FROM asset_record_refresh_state
             WHERE asset_record_id = $1",
        )
        .bind(unrelated.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "an unrelated generation must not be advanced by another article's reset"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn periodic_pass_skips_a_pending_generation_with_an_active_repair(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let review_id = "repair-owns-the-slot";
    create_source_and_articles(&pool, source_id, &[review_id]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        review_id,
        asset_input(
            "https://cdn.example/repair-owns-the-slot.png",
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresAssetStore::new(pool.clone(), policy)
        .with_refresh_policy(AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap());
    assert!(matches!(
        store.enqueue_repair(stored.id()).await.unwrap(),
        AssetRepairEnqueueResult::Enqueued { .. }
    ));
    sqlx::query(
        "INSERT INTO asset_record_refresh_state
             (asset_record_id, requested_generation, completed_generation, served_generation)
         VALUES ($1, 1, 0, 1)",
    )
    .bind(stored.id())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id, state)
         VALUES ($1, 1, 1, $2, $3, 'unseen')",
    )
    .bind(stored.id())
    .bind(source_id.as_uuid())
    .bind(review_id)
    .execute(&pool)
    .await
    .unwrap();

    store.resume_pending_refreshes().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation = 1 AND ordinal = 1",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        "unseen",
        "an active repair keeps the recovery slot, so the refresh entry stays pending"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_repair_jobs r JOIN jobs j ON j.id = r.job_id
             WHERE r.expired_asset_record_id = $1",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "no refresh job may be admitted while the repair is active"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn stale_refresh_lease_cannot_finish_or_mutate_chain_state(pool: PgPool) {
    let (asset_id, lease) = admit_refresh_job(&pool, "stale-refresh-lease").await;
    let replacement_token = Uuid::new_v4();
    sqlx::query("UPDATE jobs SET lease_token = $2 WHERE id = $1")
        .bind(lease.job.id())
        .bind(replacement_token)
        .execute(&pool)
        .await
        .unwrap();

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    assert!(matches!(
        unit_of_work
            .assets(test_policy(0, Duration::from_secs(30), 1024))
            .finish_article_refresh(
                &lease,
                ArticleRefreshOutcome::Inconclusive,
                AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap(),
            )
            .await,
        Err(AssetRepositoryError::LeaseLost)
    ));
    drop(unit_of_work);

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(lease.job.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "running"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation = 1 AND ordinal = 1",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "in_flight"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT attempts FROM asset_article_refresh_lineage
             WHERE source_id = $1 AND review_id = $2",
        )
        .bind(
            lease.job.payload()["source_id"]
                .as_str()
                .unwrap()
                .parse::<Uuid>()
                .unwrap()
        )
        .bind("stale-refresh-lease")
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT active_chain_id FROM asset_record_refresh_state WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        Some(
            lease.job.payload()["chain_id"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap()
        )
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn ordinary_refresh_completion_does_not_resnapshot_same_generation(pool: PgPool) {
    let (asset_id, lease) = admit_refresh_job(&pool, "ordinary-refresh-completion").await;

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    let finish = unit_of_work
        .assets(test_policy(0, Duration::from_secs(30), 1024))
        .finish_article_refresh(
            &lease,
            ArticleRefreshOutcome::Deleted,
            AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap(),
        )
        .await;
    assert!(
        finish.is_ok(),
        "ordinary completion should not resnapshot: {finish:?}"
    );
    let finish = finish.unwrap();
    assert!(finish.generation_completed);
    assert_eq!(finish.next_job_id, None);
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_as::<_, (String, i64, i64, i64, Option<Uuid>)>(
            "SELECT j.status, ars.requested_generation, ars.completed_generation,
                    ars.served_generation, ars.active_chain_id
             FROM jobs j CROSS JOIN asset_record_refresh_state ars
             WHERE j.id = $1 AND ars.asset_record_id = $2",
        )
        .bind(lease.job.id())
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        ("succeeded".to_owned(), 1, 1, 1, None)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_refresh_generation_entries WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
        "ordinary completion must retain the immutable generation-1 snapshot only"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn refresh_completion_starts_one_coalesced_follow_up_generation(pool: PgPool) {
    let (asset_id, lease) = admit_refresh_job(&pool, "coalesced-refresh-follow-up").await;
    sqlx::query(
        "UPDATE asset_record_refresh_state
         SET requested_generation = requested_generation + 1
         WHERE asset_record_id = $1",
    )
    .bind(asset_id)
    .execute(&pool)
    .await
    .unwrap();

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    let finish = unit_of_work
        .assets(test_policy(0, Duration::from_secs(30), 1024))
        .finish_article_refresh(
            &lease,
            ArticleRefreshOutcome::Superseded,
            AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    assert!(finish.generation_completed);
    let follow_up_job_id = finish
        .next_job_id
        .expect("new request should start a follow-up");
    unit_of_work.commit().await.unwrap();

    let state = sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>)>(
        "SELECT requested_generation, completed_generation, served_generation, active_chain_id
         FROM asset_record_refresh_state WHERE asset_record_id = $1",
    )
    .bind(asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state.0, 2);
    assert_eq!(state.1, 1);
    assert_eq!(state.2, 2);
    assert!(state.3.is_some());
    assert_eq!(
        sqlx::query_as::<_, (String, i64, Uuid)>(
            "SELECT j.status, arj.chain_ordinal, arj.chain_id
             FROM jobs j JOIN asset_repair_jobs arj ON arj.job_id = j.id
             WHERE j.id = $1",
        )
        .bind(follow_up_job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        ("queued".to_owned(), 1, state.3.unwrap())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation IN (1, 2)",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        2,
        "coalesced follow-up should snapshot live article relationships once"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(lease.job.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "succeeded",
        "the current job's dedupe slot must be released before the same article is re-admitted"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn follow_up_generation_stays_pending_while_its_article_is_cooling(pool: PgPool) {
    let (asset_id, lease) = admit_refresh_job(&pool, "cooling-follow-up").await;
    sqlx::query(
        "UPDATE asset_article_refresh_lineage
         SET next_allowed_at = clock_timestamp() + interval '1 hour'
         WHERE source_id = $1 AND review_id = 'cooling-follow-up'",
    )
    .bind(
        lease.job.payload()["source_id"]
            .as_str()
            .unwrap()
            .parse::<Uuid>()
            .unwrap(),
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE asset_record_refresh_state
         SET requested_generation = 2 WHERE asset_record_id = $1",
    )
    .bind(asset_id)
    .execute(&pool)
    .await
    .unwrap();

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    let finish = unit_of_work
        .assets(test_policy(0, Duration::from_secs(30), 1024))
        .finish_article_refresh(
            &lease,
            ArticleRefreshOutcome::Superseded,
            AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap(),
        )
        .await
        .unwrap();
    assert!(finish.generation_completed);
    assert_eq!(finish.next_job_id, None);
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>)>(
            "SELECT requested_generation, completed_generation, served_generation, active_chain_id
             FROM asset_record_refresh_state WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        (2, 1, 2, None),
        "a cooling follow-up snapshot must remain pending for cache-miss resumption"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation = 2",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "unseen"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn refresh_chain_advances_articles_in_snapshot_order_and_completes_once(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["chain-alpha", "chain-beta"]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input("https://cdn.example/shared-chain.png", FIRST_BYTES, 0);
    let first = store_asset(&pool, cache_policy, source_id, "chain-alpha", input.clone()).await;
    let second = store_asset(&pool, cache_policy, source_id, "chain-beta", input).await;
    assert_eq!(first.id(), second.id());
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(first.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let refresh_policy = AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap();
    let store = PostgresAssetStore::new(pool.clone(), cache_policy)
        .with_repair_policy(AssetRepairPolicy::new(3, 3, Duration::from_secs(60), 3).unwrap())
        .with_refresh_policy(refresh_policy);
    let repair_id = match store.enqueue_repair(first.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair should be admitted: {other:?}"),
    };
    let repair_lease = claim_repair_job(&pool).await;
    assert_eq!(repair_lease.job.id(), repair_id);
    let first_refresh_id = match store.finish_confirmed_expiry(&repair_lease, first.id()).await.unwrap() {
        werrss::persistence::repositories::asset_repository::AssetRefreshAdmissionResult::Enqueued { job_id, ordinal: 1, .. } => job_id,
        other => panic!("first snapshot article should be admitted: {other:?}"),
    };
    let jobs = PostgresJobRepository::new(pool.clone());
    let first_refresh = jobs
        .claim_next(
            "ordered-refresh-worker",
            Utc::now(),
            chrono::Duration::minutes(5),
            &[werrss::domain::job::JobType::AssetRefresh],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_refresh.job.id(), first_refresh_id);
    assert_eq!(first_refresh.job.payload()["review_id"], "chain-alpha");

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut first_uow = factory.begin().await.unwrap();
    let first_finish = first_uow
        .assets(cache_policy)
        .finish_article_refresh(
            &first_refresh,
            ArticleRefreshOutcome::Succeeded,
            refresh_policy,
        )
        .await
        .unwrap();
    assert!(!first_finish.generation_completed);
    let second_refresh_id = first_finish
        .next_job_id
        .expect("the next article should inherit the chain slot");
    first_uow.commit().await.unwrap();

    let second_refresh = jobs
        .claim_next(
            "ordered-refresh-worker",
            Utc::now(),
            chrono::Duration::minutes(5),
            &[werrss::domain::job::JobType::AssetRefresh],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_refresh.job.id(), second_refresh_id);
    assert_eq!(second_refresh.job.payload()["review_id"], "chain-beta");
    assert_eq!(second_refresh.job.payload()["ordinal"], 2);
    let mut second_uow = factory.begin().await.unwrap();
    let second_finish = second_uow
        .assets(cache_policy)
        .finish_article_refresh(
            &second_refresh,
            ArticleRefreshOutcome::Succeeded,
            refresh_policy,
        )
        .await
        .unwrap();
    assert!(second_finish.generation_completed);
    assert_eq!(second_finish.next_job_id, None);
    second_uow.commit().await.unwrap();

    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>)>(
            "SELECT requested_generation, completed_generation, served_generation, active_chain_id
         FROM asset_record_refresh_state WHERE asset_record_id = $1",
        )
        .bind(first.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        (1, 1, 1, None)
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn refresh_chain_slot_handoff_never_exceeds_a_global_limit_of_one(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["limit-alpha", "limit-beta"]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input("https://cdn.example/limit-shared.png", FIRST_BYTES, 0);
    let first = store_asset(&pool, cache_policy, source_id, "limit-alpha", input.clone()).await;
    let second = store_asset(&pool, cache_policy, source_id, "limit-beta", input).await;
    assert_eq!(first.id(), second.id());
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(first.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let refresh_policy = AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap();
    let store = PostgresAssetStore::new(pool.clone(), cache_policy)
        .with_repair_policy(AssetRepairPolicy::new(1, 1, Duration::from_secs(60), 1).unwrap())
        .with_refresh_policy(refresh_policy);
    let active_recovery_jobs = || {
        let pool = pool.clone();
        let asset_id = first.id();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM asset_repair_jobs arj
                 JOIN jobs j ON j.id = arj.job_id
                 WHERE arj.expired_asset_record_id = $1
                   AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
            )
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let repair_id = match store.enqueue_repair(first.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair should be admitted at capacity one: {other:?}"),
    };
    let repair_lease = claim_repair_job(&pool).await;
    assert_eq!(repair_lease.job.id(), repair_id);
    let first_refresh_id = match store.finish_confirmed_expiry(&repair_lease, first.id()).await.unwrap() {
        werrss::persistence::repositories::asset_repository::AssetRefreshAdmissionResult::Enqueued {
            job_id,
            ordinal: 1,
            ..
        } => job_id,
        other => panic!("the repair slot should transfer to the first snapshot article: {other:?}"),
    };
    assert_eq!(
        active_recovery_jobs().await,
        1,
        "the transferred slot must not create a parallel recovery job"
    );
    let jobs = PostgresJobRepository::new(pool.clone());
    let first_refresh = jobs
        .claim_next(
            "limit-refresh-worker",
            Utc::now(),
            chrono::Duration::minutes(5),
            &[werrss::domain::job::JobType::AssetRefresh],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_refresh.job.id(), first_refresh_id);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut first_uow = factory.begin().await.unwrap();
    let first_finish = first_uow
        .assets(cache_policy)
        .finish_article_refresh(
            &first_refresh,
            ArticleRefreshOutcome::Succeeded,
            refresh_policy,
        )
        .await
        .unwrap();
    let second_refresh_id = first_finish
        .next_job_id
        .expect("capacity one still transfers the slot to the next snapshot article");
    first_uow.commit().await.unwrap();
    assert_eq!(
        active_recovery_jobs().await,
        1,
        "refresh-to-refresh handoff must keep exactly one active recovery job"
    );
    let second_refresh = jobs
        .claim_next(
            "limit-refresh-worker",
            Utc::now(),
            chrono::Duration::minutes(5),
            &[werrss::domain::job::JobType::AssetRefresh],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_refresh.job.id(), second_refresh_id);
    let mut second_uow = factory.begin().await.unwrap();
    let second_finish = second_uow
        .assets(cache_policy)
        .finish_article_refresh(
            &second_refresh,
            ArticleRefreshOutcome::Succeeded,
            refresh_policy,
        )
        .await
        .unwrap();
    assert!(second_finish.generation_completed);
    assert_eq!(second_finish.next_job_id, None);
    second_uow.commit().await.unwrap();
    assert_eq!(active_recovery_jobs().await, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn stale_repair_lease_cannot_refund_or_admit_refresh(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["stale-expiry-lease"]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        cache_policy,
        source_id,
        "stale-expiry-lease",
        asset_input("https://cdn.example/stale-expiry-lease.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresAssetStore::new(pool.clone(), cache_policy);
    let repair_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("public repair should be admitted: {other:?}"),
    };
    let lease = claim_repair_job(&pool).await;
    assert_eq!(lease.job.id(), repair_id);
    sqlx::query("UPDATE jobs SET lease_token = $2 WHERE id = $1")
        .bind(repair_id)
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        store.finish_confirmed_expiry(&lease, stored.id()).await,
        Err(AssetRepositoryError::LeaseLost)
    ));
    assert!(!sqlx::query_scalar::<_, bool>(
        "SELECT expiry_refunded FROM asset_repair_jobs WHERE job_id = $1",
    )
    .bind(repair_id)
    .fetch_one(&pool)
    .await
    .unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE job_type = 'asset_refresh'",)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT requested_generation FROM asset_record_refresh_state WHERE asset_record_id = $1",
        )
        .bind(stored.id())
        .fetch_optional(&pool)
        .await
        .unwrap()
        .unwrap_or(0),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn confirmed_expiry_completes_a_generation_with_only_exhausted_articles(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let review_id = "already-exhausted-refresh";
    create_source_and_articles(&pool, source_id, &[review_id]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        cache_policy,
        source_id,
        review_id,
        asset_input("https://cdn.example/exhausted-refresh.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let refresh_policy = AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap();
    let store =
        PostgresAssetStore::new(pool.clone(), cache_policy).with_refresh_policy(refresh_policy);
    let repair_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair should be admitted: {other:?}"),
    };
    let lease = claim_repair_job(&pool).await;
    assert_eq!(lease.job.id(), repair_id);
    // The article exhausts its refresh budget while the repair is in flight, so
    // the confirmed expiry snapshots it but admits no refresh for it.
    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, exhausted, last_outcome)
         VALUES ($1, $2, 3, TRUE, 'exhausted')",
    )
    .bind(source_id.as_uuid())
    .bind(review_id)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        store.finish_confirmed_expiry(&lease, stored.id()).await.unwrap(),
        werrss::persistence::repositories::asset_repository::AssetRefreshAdmissionResult::NoEligibleArticle
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>)>(
            "SELECT requested_generation, completed_generation, served_generation, active_chain_id
             FROM asset_record_refresh_state WHERE asset_record_id = $1",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        (1, 1, 1, None),
        "a snapshot with no eligible entries must be terminal rather than pending forever"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation = 1 AND ordinal = 1",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        "done"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn confirmed_public_expiry_refunds_repair_and_transfers_its_slot_to_refresh(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["confirmed-expiry"]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        cache_policy,
        source_id,
        "confirmed-expiry",
        asset_input("https://cdn.example/confirmed-expiry.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let repair_policy = AssetRepairPolicy::new(2, 2, Duration::from_secs(60), 3).unwrap();
    let refresh_policy = AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap();
    let store = PostgresAssetStore::new(pool.clone(), cache_policy)
        .with_repair_policy(repair_policy)
        .with_refresh_policy(refresh_policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("public repair should be admitted, got {other:?}"),
    };
    let lease = claim_repair_job(&pool).await;
    assert_eq!(lease.job.id(), job_id);

    let admission = store
        .finish_confirmed_expiry(&lease, stored.id())
        .await
        .unwrap();
    let refresh_job_id = match admission {
        werrss::persistence::repositories::asset_repository::AssetRefreshAdmissionResult::Enqueued {
            job_id,
            generation: 1,
            ordinal: 1,
            ..
        } => job_id,
        other => panic!("confirmed expiry should admit an article refresh, got {other:?}"),
    };
    let repair = sqlx::query_as::<_, (String, bool, Option<Uuid>)>(
        "SELECT j.status, arj.expiry_refunded, arj.asset_record_id
         FROM jobs j JOIN asset_repair_jobs arj ON arj.job_id = j.id
         WHERE j.id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(repair, ("succeeded".to_owned(), true, Some(stored.id())));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT admitted_attempts FROM asset_repair_states WHERE asset_record_id = $1",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0,
        "confirmed expiry must refund the public repair reservation once"
    );
    let refresh = sqlx::query_as::<_, (String, Option<Uuid>, Uuid, i64)>(
        "SELECT j.status, arj.asset_record_id, arj.chain_id, arj.chain_ordinal
         FROM jobs j JOIN asset_repair_jobs arj ON arj.job_id = j.id
         WHERE j.id = $1",
    )
    .bind(refresh_job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(refresh.0, "queued");
    assert_eq!(refresh.1, None);
    assert_eq!(refresh.3, 1);
    let refresh_state = sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>)>(
        "SELECT requested_generation, completed_generation, served_generation, active_chain_id
         FROM asset_record_refresh_state WHERE asset_record_id = $1",
    )
    .bind(stored.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(refresh_state, (1, 0, 1, Some(refresh.2)));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation = 1 AND state = 'in_flight'",
        )
        .bind(stored.id())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn repair_admission_is_deduplicated_for_one_missing_asset(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["repair-dedupe"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "repair-dedupe",
        asset_input("https://cdn.example/repair-dedupe.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    let first = store.enqueue_repair(stored.id()).await.unwrap();
    let second = store.enqueue_repair(stored.id()).await.unwrap();
    let first_id = match first {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("first admission should enqueue, got {other:?}"),
    };
    assert_eq!(
        second,
        AssetRepairEnqueueResult::AlreadyActive { job_id: first_id }
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE job_type = 'asset_repair'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn repair_admission_enforces_the_cluster_pending_cap(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["repair-cap-one", "repair-cap-two"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "repair-cap-one",
        asset_input("https://cdn.example/repair-cap-one.png", FIRST_BYTES, 0),
    )
    .await;
    let second = store_asset(
        &pool,
        policy,
        source_id,
        "repair-cap-two",
        asset_input("https://cdn.example/repair-cap-two.png", SECOND_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL")
        .execute(&pool)
        .await
        .unwrap();
    let repair_policy = AssetRepairPolicy::new(1, 1, Duration::from_secs(60), 3).unwrap();
    let store = PostgresAssetStore::new(pool.clone(), policy).with_repair_policy(repair_policy);
    assert!(matches!(
        store.enqueue_repair(first.id()).await.unwrap(),
        AssetRepairEnqueueResult::Enqueued { .. }
    ));
    assert_eq!(
        store.enqueue_repair(second.id()).await.unwrap(),
        AssetRepairEnqueueResult::CapacityFull
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM jobs WHERE job_type = 'asset_repair' AND status IN ('queued', 'running', 'retry_wait', 'deferred')",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn repair_admission_honors_backoff_and_exhausts_the_attempt_budget(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["repair-budget"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "repair-budget",
        asset_input("https://cdn.example/repair-budget.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let repair_policy = AssetRepairPolicy::new(10, 10, Duration::from_secs(60), 2).unwrap();
    let store = PostgresAssetStore::new(pool.clone(), policy).with_repair_policy(repair_policy);
    let first_job = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("first admission should enqueue, got {other:?}"),
    };
    let first_lease = claim_repair_job(&pool).await;
    assert!(!store
        .record_repair_failure(&first_lease, stored.id(), "upstream unavailable")
        .await
        .unwrap());
    finish_repair_job(&pool, first_job).await;
    assert_eq!(
        store.enqueue_repair(stored.id()).await.unwrap(),
        AssetRepairEnqueueResult::Backoff
    );

    sqlx::query(
        "UPDATE asset_repair_states
         SET next_allowed_at = clock_timestamp() - interval '1 second'
         WHERE asset_record_id = $1",
    )
    .bind(stored.id())
    .execute(&pool)
    .await
    .unwrap();
    let second_job = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("second admission should enqueue after backoff, got {other:?}"),
    };
    let second_lease = claim_repair_job(&pool).await;
    assert!(store
        .record_repair_failure(&second_lease, stored.id(), "upstream still unavailable")
        .await
        .unwrap());
    finish_repair_job(&pool, second_job).await;
    assert_eq!(
        store.enqueue_repair(stored.id()).await.unwrap(),
        AssetRepairEnqueueResult::Exhausted
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_keeps_orphan_metadata_until_repair_job_is_terminal(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["repair-orphan"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "repair-orphan",
        asset_input("https://cdn.example/repair-orphan.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresAssetStore::new(pool.clone(), policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair should be admitted, got {other:?}"),
    };

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .assets(policy)
        .clear_for_article(source_id, "repair-orphan")
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    assert_eq!(store.maintenance().await.unwrap().orphan_records, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );

    finish_repair_job(&pool, job_id).await;
    assert_eq!(store.maintenance().await.unwrap().orphan_records, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn repair_admission_serializes_with_orphan_cleanup(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["repair-admission-race"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "repair-admission-race",
        asset_input(
            "https://cdn.example/repair-admission-race.png",
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    // Keep the repair transaction between its asset-row lock and commit. This
    // lets the test remove the article relationship and start maintenance at
    // exactly the point where the old lock order could delete the new job.
    let mut trigger_blocker = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(2147483643::bigint)")
        .execute(&mut *trigger_blocker)
        .await
        .unwrap();
    sqlx::query(
        "CREATE FUNCTION test_hold_asset_repair_job_insert() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_lock(2147483642::bigint);
             PERFORM pg_advisory_lock(2147483643::bigint);
             PERFORM pg_advisory_unlock(2147483643::bigint);
             PERFORM pg_advisory_unlock(2147483642::bigint);
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_hold_asset_repair_job_insert
         AFTER INSERT ON jobs
         FOR EACH ROW EXECUTE FUNCTION test_hold_asset_repair_job_insert()",
    )
    .execute(&pool)
    .await
    .unwrap();

    let asset_id = stored.id();
    let enqueue = tokio::spawn({
        let pool = pool.clone();
        async move {
            PostgresAssetStore::new(pool, policy)
                .enqueue_repair(asset_id)
                .await
        }
    });
    wait_for_repair_insert_trigger(&pool).await;

    // The relationship can disappear while admission is in progress. The
    // active job must still protect the stable asset metadata from cleanup.
    sqlx::query("DELETE FROM article_assets WHERE asset_record_id = $1")
        .bind(asset_id)
        .execute(&pool)
        .await
        .unwrap();

    let maintenance = tokio::spawn({
        let pool = pool.clone();
        async move { PostgresAssetStore::new(pool, policy).maintenance().await }
    });
    wait_for_capacity_waiter(&pool).await;

    sqlx::query("SELECT pg_advisory_unlock(2147483643::bigint)")
        .execute(&mut *trigger_blocker)
        .await
        .unwrap();

    let enqueue_result = tokio::time::timeout(Duration::from_secs(5), enqueue)
        .await
        .expect("repair admission should finish after the trigger is released")
        .unwrap()
        .unwrap();
    let job_id = match enqueue_result {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair admission should enqueue one job, got {other:?}"),
    };
    let maintenance_result = tokio::time::timeout(Duration::from_secs(5), maintenance)
        .await
        .expect("maintenance should finish after repair admission")
        .unwrap()
        .unwrap();

    assert_eq!(maintenance_result.orphan_records, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "a newly admitted repair must preserve the stable asset record"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)
             FROM asset_repair_jobs AS arj
             JOIN jobs AS j ON j.id = arj.job_id
             WHERE arj.asset_record_id = $1
               AND j.id = $2
               AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
        )
        .bind(asset_id)
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
        "maintenance must not cascade-delete the admitted repair job"
    );

    sqlx::query("DROP TRIGGER test_hold_asset_repair_job_insert ON jobs")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION test_hold_asset_repair_job_insert()")
        .execute(&pool)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn repair_restores_bytes_without_changing_the_stable_asset_id(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["repair-restore"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input("https://cdn.example/repair-restore.png", FIRST_BYTES, 0);
    let stored = store_asset(&pool, policy, source_id, "repair-restore", input.clone()).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresAssetStore::new(pool.clone(), policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair should be admitted, got {other:?}"),
    };
    let lease = claim_repair_job(&pool).await;
    assert_eq!(
        store
            .repair_target(stored.id())
            .await
            .unwrap()
            .unwrap()
            .source_url,
        input.source_url
    );
    assert_eq!(
        store
            .restore_missing_asset(&lease, stored.id(), &input)
            .await
            .unwrap(),
        AssetRepairRestoreResult::Restored
    );
    let restored_blob_id =
        sqlx::query_scalar::<_, Uuid>("SELECT blob_id FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    let metadata = store
        .blob_store()
        .stat(&AssetBlobKey::for_blob_id(restored_blob_id))
        .await
        .unwrap()
        .expect("repair restoration should retain the blob metadata");
    assert!(metadata.is_present());
    assert_eq!(metadata.byte_size(), FIRST_BYTES.len() as u64);
    finish_repair_job(&pool, job_id).await;
    assert!(matches!(
        store.read_and_touch(stored.id()).await.unwrap(),
        Some(AssetRead::Available { ref bytes, .. }) if bytes == FIRST_BYTES
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_preflight_and_repair_restore_share_capacity_first_lock_order(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["lock-order"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input("https://cdn.example/lock-order.png", FIRST_BYTES, 0);
    let stored = store_asset(&pool, policy, source_id, "lock-order", input.clone()).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair should be admitted, got {other:?}"),
    };
    let lease = claim_repair_job(&pool).await;

    // Queue both operations behind one capacity lock. Before source preflight
    // acquired this lock, it could hold the checksum lock while waiting for
    // capacity, forming a cycle with repair restore.
    let mut capacity_blocker = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *capacity_blocker)
        .await
        .unwrap();

    let repair_task = tokio::spawn({
        let store = PostgresAssetStore::new(pool.clone(), policy);
        let lease = lease.clone();
        let input = input.clone();
        async move {
            store
                .restore_missing_asset(&lease, stored.id(), &input)
                .await?;
            store.complete_repair(&lease, stored.id()).await
        }
    });
    wait_for_capacity_waiters(&pool, 1).await;

    let source_task = tokio::spawn({
        let pool = pool.clone();
        let input = input.clone();
        async move {
            let factory = UnitOfWorkFactory::new(pool);
            let mut unit_of_work = factory
                .begin_with_assets(std::slice::from_ref(&input))
                .await
                .map_err(|error| error.to_string())?;
            unit_of_work
                .assets(policy)
                .store_for_article(source_id, "lock-order", &[input])
                .await
                .map_err(|error| error.to_string())?;
            unit_of_work
                .commit()
                .await
                .map_err(|error| error.to_string())
        }
    });
    wait_for_capacity_waiters(&pool, 2).await;

    sqlx::query("SELECT pg_advisory_unlock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *capacity_blocker)
        .await
        .unwrap();

    let (repair_result, source_result) = tokio::time::timeout(Duration::from_secs(5), async {
        (
            repair_task.await.expect("repair task should not panic"),
            source_task.await.expect("source task should not panic"),
        )
    })
    .await
    .expect("repair and source persistence should not deadlock");
    repair_result.expect("repair restore should complete");
    source_result.expect("source asset persistence should complete");
    finish_repair_job(&pool, job_id).await;
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn empty_asset_replacement_does_not_deadlock_with_nonempty_preflight(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(
        &pool,
        source_id,
        &["empty-replacement", "nonempty-preflight"],
    )
    .await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let input = asset_input("https://cdn.example/nonempty-preflight.png", FIRST_BYTES, 0);

    // The empty transaction owns the source row without reserving the asset
    // capacity lock, matching the source-sync path after all asset fetches
    // failed.
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut empty_unit_of_work = factory.begin().await.unwrap();
    empty_unit_of_work
        .source()
        .find_for_update(source_id)
        .await
        .unwrap();

    // The non-empty transaction reserves capacity during preflight and then
    // waits for the source row. Before the fix, the empty replacement waited
    // for capacity here while holding the source row, forming a cycle.
    let nonempty_task = tokio::spawn({
        let pool = pool.clone();
        async move {
            let factory = UnitOfWorkFactory::new(pool);
            let mut unit_of_work = factory
                .begin_with_assets(std::slice::from_ref(&input))
                .await
                .unwrap();
            unit_of_work
                .source()
                .find_for_update(source_id)
                .await
                .unwrap();
            unit_of_work.commit().await.unwrap();
        }
    });
    wait_for_capacity_holder(&pool).await;

    let replacement = tokio::time::timeout(
        Duration::from_secs(2),
        empty_unit_of_work
            .assets(policy)
            .replace_for_article(source_id, "empty-replacement", &[]),
    )
    .await;
    if replacement.is_err() {
        nonempty_task.abort();
        let _ = nonempty_task.await;
        drop(empty_unit_of_work);
        panic!("empty asset replacement deadlocked behind non-empty preflight");
    }
    assert!(replacement.unwrap().unwrap().is_empty());
    empty_unit_of_work.commit().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), nonempty_task)
        .await
        .expect("non-empty preflight should finish after the source row is released")
        .expect("non-empty preflight task should not panic");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn reuses_one_record_when_the_same_url_is_seen_again(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["url-first", "url-second"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "url-first",
        asset_input("https://cdn.example/shared.png", FIRST_BYTES, 0),
    )
    .await;
    let second = store_asset(
        &pool,
        policy,
        source_id,
        "url-second",
        asset_input("https://cdn.example/shared.png", FIRST_BYTES, 0),
    )
    .await;

    assert_eq!(second.id(), first.id());
    assert_eq!(second.blob_id(), first.blob_id());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM article_assets")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn deduplicates_same_bytes_within_one_asset_batch(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["batch-dedup"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let inputs = vec![
        asset_input("https://cdn.example/batch-one.png", FIRST_BYTES, 0),
        asset_input("https://cdn.example/batch-two.png", FIRST_BYTES, 1),
    ];

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin_with_assets(&inputs).await.unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "batch-dedup", &inputs)
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].blob_id(), stored[1].blob_id());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn replacing_a_full_article_observation_removes_stale_asset_links(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["replace-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "replace-article",
        asset_input("https://cdn.example/old.png", FIRST_BYTES, 0),
    )
    .await;

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    let replacement = unit_of_work
        .assets(policy)
        .replace_for_article(source_id, "replace-article", &[])
        .await
        .unwrap();
    assert!(replacement.is_empty());
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM article_assets
             WHERE source_id = $1 AND review_id = 'replace-article'",
        )
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "relationship replacement must not delete repair metadata"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn clearing_article_asset_links_keeps_repair_metadata(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["clear-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "clear-article",
        asset_input("https://cdn.example/clear.png", FIRST_BYTES, 0),
    )
    .await;

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .assets(policy)
        .clear_for_article(source_id, "clear-article")
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM article_assets
             WHERE source_id = $1 AND review_id = 'clear-article'",
        )
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "clearing a relationship must not delete repair metadata"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn failed_article_asset_replacement_preserves_existing_asset_link(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["failed-replacement"]).await;
    let seed_policy = test_policy(0, Duration::from_secs(30), 1024);
    let existing = store_asset(
        &pool,
        seed_policy,
        source_id,
        "failed-replacement",
        asset_input("https://cdn.example/existing.png", FIRST_BYTES, 0),
    )
    .await;

    let replacement_policy = test_policy(5, Duration::from_secs(30), 1024);
    let replacement = asset_input("https://cdn.example/replacement.png", SECOND_BYTES, 0);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&replacement))
        .await
        .unwrap();
    let result = unit_of_work
        .assets(replacement_policy)
        .replace_for_article(
            source_id,
            "failed-replacement",
            std::slice::from_ref(&replacement),
        )
        .await;
    assert_eq!(
        result,
        Err(AssetRepositoryError::CapacityExceeded {
            requested_bytes: SECOND_BYTES.len() as u64,
            max_bytes: 5,
        })
    );
    unit_of_work.commit().await.unwrap();

    let attached_id: Uuid = sqlx::query_scalar(
        "SELECT asset_record_id
         FROM article_assets
         WHERE source_id = $1 AND review_id = 'failed-replacement'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(attached_id, existing.id());
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>("SELECT data FROM asset_blobs WHERE id = $1")
            .bind(existing.blob_id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        FIRST_BYTES
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn same_byte_reuse_waits_for_the_capacity_decision_lock(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["lock-first", "lock-second"]).await;
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "lock-first",
        asset_input("https://cdn.example/locked.png", FIRST_BYTES, 0),
    )
    .await;

    let mut lock_connection = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *lock_connection)
        .await
        .unwrap();

    let task = tokio::spawn({
        let pool = pool.clone();
        async move {
            store_asset(
                &pool,
                policy,
                source_id,
                "lock-second",
                asset_input("https://cdn.example/locked.png", FIRST_BYTES, 0),
            )
            .await
        }
    });
    let lock_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let waiting_for_capacity: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks AS locks
                 JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
                 WHERE locks.locktype = 'advisory'
                   AND NOT locks.granted
                   AND locks.objsubid = 1
                   AND activity.datname = current_database()
                   AND locks.classid = (
                       (hashtextextended('asset-capacity', 0) >> 32)
                       & 4294967295
                   )::oid
                   AND locks.objid = (
                       hashtextextended('asset-capacity', 0) & 4294967295
                   )::oid
             )",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if waiting_for_capacity {
            break;
        }
        assert!(
            !task.is_finished(),
            "same-byte reuse completed without joining the capacity lock"
        );
        assert!(
            Instant::now() < lock_deadline,
            "reuse did not request the lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    sqlx::query("SELECT pg_advisory_unlock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *lock_connection)
        .await
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("same-byte reuse should finish after the lock is released")
        .unwrap();
    assert_eq!(second.id(), first.id());
    assert_eq!(second.blob_id(), first.blob_id());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn shares_one_blob_when_different_urls_have_identical_bytes(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["bytes-first", "bytes-second"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "bytes-first",
        asset_input("https://cdn.example/one.png", FIRST_BYTES, 0),
    )
    .await;
    let second = store_asset(
        &pool,
        policy,
        source_id,
        "bytes-second",
        asset_input("https://cdn.example/two.png", FIRST_BYTES, 0),
    )
    .await;

    assert_ne!(
        second.id(),
        first.id(),
        "different URLs need separate records"
    );
    assert_eq!(second.blob_id(), first.blob_id());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn creates_a_new_url_version_when_the_bytes_change(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["version-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "version-article",
        asset_input("https://cdn.example/version.png", FIRST_BYTES, 0),
    )
    .await;
    let second = store_asset(
        &pool,
        policy,
        source_id,
        "version-article",
        asset_input("https://cdn.example/version.png", SECOND_BYTES, 0),
    )
    .await;

    assert_ne!(second.id(), first.id());
    assert_ne!(second.blob_id(), first.blob_id());
    let versions = sqlx::query(
        "SELECT version FROM asset_records WHERE source_url = 'https://cdn.example/version.png' ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        versions
            .iter()
            .map(|row| row.get::<i64, _>("version"))
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let attached_id: Uuid = sqlx::query_scalar(
        "SELECT asset_record_id FROM article_assets WHERE source_id = $1 AND review_id = 'version-article'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(attached_id, second.id());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn evicts_oldest_binary_bytes_but_retains_url_metadata(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["evict-first", "evict-second"]).await;
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "evict-first",
        asset_input("https://cdn.example/evict-first.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '1 hour' WHERE id = $1")
        .bind(first.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let second = store_asset(
        &pool,
        policy,
        source_id,
        "evict-second",
        asset_input("https://cdn.example/evict-second.png", SECOND_BYTES, 0),
    )
    .await;

    assert_eq!(
        PostgresAssetStore::new(pool.clone(), policy)
            .read_and_touch(first.id())
            .await
            .unwrap(),
        Some(AssetRead::Missing {
            source_url: "https://cdn.example/evict-first.png".parse().unwrap(),
        })
    );
    assert_eq!(
        PostgresAssetStore::new(pool.clone(), policy)
            .read_and_touch(second.id())
            .await
            .unwrap()
            .and_then(|read| match read {
                AssetRead::Available { bytes, .. } => Some(bytes),
                AssetRead::Missing { .. } => None,
            }),
        Some(SECOND_BYTES.to_vec())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2,
        "eviction must not remove URL/version metadata"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT fetch_status FROM asset_records WHERE id = $1",)
            .bind(first.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "missing"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn serializes_concurrent_capacity_decisions(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["concurrent-first", "concurrent-second"]).await;
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = store_asset_after_barrier(
        &pool,
        policy,
        source_id,
        "concurrent-first",
        asset_input("https://cdn.example/concurrent-first.png", FIRST_BYTES, 0),
        barrier.clone(),
    );
    let second = store_asset_after_barrier(
        &pool,
        policy,
        source_id,
        "concurrent-second",
        asset_input("https://cdn.example/concurrent-second.png", SECOND_BYTES, 0),
        barrier,
    );
    let (_first, _second) = tokio::join!(first, second);

    let present_bytes: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(byte_size), 0)::bigint FROM asset_blobs WHERE data IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        present_bytes <= 15,
        "cache bytes must remain within the cap"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn serializes_multi_asset_batches_before_acquiring_url_locks(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["batch-first", "batch-second"]).await;
    let policy = test_policy(1024, Duration::from_secs(30), 1024);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first_batch = store_assets_after_barrier(
        &pool,
        policy,
        source_id,
        "batch-first",
        vec![
            asset_input("https://cdn.example/batch-one.png", FIRST_BYTES, 0),
            asset_input("https://cdn.example/batch-two.png", SECOND_BYTES, 1),
        ],
        barrier.clone(),
    );
    let second_batch = store_assets_after_barrier(
        &pool,
        policy,
        source_id,
        "batch-second",
        vec![
            asset_input("https://cdn.example/batch-two.png", SECOND_BYTES, 0),
            asset_input("https://cdn.example/batch-one.png", FIRST_BYTES, 1),
        ],
        barrier,
    );

    let (first_batch, second_batch) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first_batch, second_batch)
    })
    .await
    .expect("reverse-order asset batches must not deadlock");
    assert_eq!(first_batch.len(), 2);
    assert_eq!(second_batch.len(), 2);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_evicts_idle_bytes_but_retains_the_repair_record(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["stale-article"]).await;
    let policy = test_policy(0, Duration::from_secs(1), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "stale-article",
        asset_input("https://cdn.example/stale.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '1 day' WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    let result = store.maintenance().await.unwrap();

    assert_eq!(result.stale_blobs, 1);
    assert_eq!(
        store.read_and_touch(stored.id()).await.unwrap(),
        Some(AssetRead::Missing {
            source_url: "https://cdn.example/stale.png".parse().unwrap(),
        })
    );
    let metadata = store
        .blob_store()
        .stat(&AssetBlobKey::for_blob_id(stored.blob_id()))
        .await
        .unwrap()
        .expect("age eviction should retain blob metadata");
    assert!(!metadata.is_present());
    assert_eq!(metadata.byte_size(), FIRST_BYTES.len() as u64);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT fetch_status FROM asset_records WHERE id = $1",)
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "missing"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_deletes_a_truncated_external_blob(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["truncated-article"]).await;
    let root = std::env::temp_dir().join(format!("werrss-assets-{}", Uuid::new_v4()));
    std::fs::create_dir_all(root.join("blobs")).unwrap();
    let root_string = root.to_string_lossy();
    let backend = Arc::new(OpenDalFsBlobStore::new(root_string.as_ref()).unwrap());
    let policy = test_policy(0, Duration::from_secs(1), 1024);
    let input = asset_input("https://cdn.example/truncated.png", FIRST_BYTES, 0);
    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "truncated-article", &[input])
        .await
        .unwrap()
        .remove(0);
    unit_of_work.commit().await.unwrap();
    let key = AssetBlobKey::for_blob_id(stored.blob_id());
    std::fs::write(root.join(key.as_str()), b"short").unwrap();
    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '1 day' WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let result = PostgresAssetStore::with_blob_store(pool.clone(), policy, backend.clone())
        .maintenance()
        .await
        .unwrap();

    assert_eq!(result.stale_blobs, 1);
    assert!(!root.join(key.as_str()).exists());
    assert!(!root
        .join(format!("{}.metadata.json", key.as_str()))
        .exists());
    let _ = std::fs::remove_dir_all(root);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn size_limit_evicts_truncated_external_bytes_by_their_stored_size(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(
        &pool,
        source_id,
        &[
            "truncated-size-article",
            "intact-size-article-one",
            "intact-size-article-two",
        ],
    )
    .await;
    let root = std::env::temp_dir().join(format!("werrss-assets-{}", Uuid::new_v4()));
    std::fs::create_dir_all(root.join("blobs")).unwrap();
    let root_string = root.to_string_lossy();
    let backend: Arc<dyn AssetBlobStore> =
        Arc::new(OpenDalFsBlobStore::new(root_string.as_ref()).unwrap());
    let store_policy = test_policy(100, Duration::from_secs(30), 1024);
    let partial = store_asset_with_backend(
        &pool,
        store_policy,
        source_id,
        "truncated-size-article",
        asset_input("https://cdn.example/truncated-size.png", FIRST_BYTES, 0),
        backend.clone(),
    )
    .await;
    let intact_one = store_asset_with_backend(
        &pool,
        store_policy,
        source_id,
        "intact-size-article-one",
        asset_input("https://cdn.example/intact-size-one.png", SECOND_BYTES, 0),
        backend.clone(),
    )
    .await;
    let intact_two = store_asset_with_backend(
        &pool,
        store_policy,
        source_id,
        "intact-size-article-two",
        asset_input("https://cdn.example/intact-size-two.png", b"third-image", 0),
        backend.clone(),
    )
    .await;
    let partial_key = AssetBlobKey::for_blob_id(partial.blob_id());
    let intact_one_key = AssetBlobKey::for_blob_id(intact_one.blob_id());
    let intact_two_key = AssetBlobKey::for_blob_id(intact_two.blob_id());
    std::fs::write(root.join(partial_key.as_str()), b"short").unwrap();
    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '3 hours' WHERE id = $1")
        .bind(partial.blob_id())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '2 hours' WHERE id = $1")
        .bind(intact_one.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let limit_policy = test_policy(20, Duration::ZERO, 1024);
    let result = PostgresAssetStore::with_blob_store(pool.clone(), limit_policy, backend)
        .maintenance()
        .await
        .unwrap();

    assert_eq!(result.size_evicted_blobs, 2);
    assert!(!root.join(partial_key.as_str()).exists());
    assert!(!root.join(intact_one_key.as_str()).exists());
    assert!(root.join(intact_two_key.as_str()).exists());
    let _ = std::fs::remove_dir_all(root);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_and_asset_reuse_follow_the_same_lock_order(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(
        &pool,
        source_id,
        &["maintenance-article", "maintenance-writer"],
    )
    .await;
    let policy = test_policy(1024, Duration::from_secs(1), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "maintenance-article",
        asset_input("https://cdn.example/maintenance.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query(
        "UPDATE asset_blobs
         SET last_accessed_at = clock_timestamp() - interval '1 day'
         WHERE id = $1",
    )
    .bind(stored.blob_id())
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE FUNCTION test_hold_asset_blob_update() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_lock(2147483645::bigint);
             PERFORM pg_sleep(1);
             PERFORM pg_advisory_unlock(2147483645::bigint);
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_hold_asset_blob_update
         BEFORE UPDATE OF data ON asset_blobs
         FOR EACH ROW EXECUTE FUNCTION test_hold_asset_blob_update()",
    )
    .execute(&pool)
    .await
    .unwrap();

    let maintenance = tokio::spawn({
        let pool = pool.clone();
        async move { PostgresAssetStore::new(pool, policy).maintenance().await }
    });
    wait_for_asset_update_trigger(&pool).await;
    let writer = tokio::spawn({
        let pool = pool.clone();
        async move {
            store_asset(
                &pool,
                policy,
                source_id,
                "maintenance-writer",
                asset_input("https://cdn.example/maintenance.png", FIRST_BYTES, 0),
            )
            .await
        }
    });

    let (maintenance, writer) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(maintenance, writer)
    })
    .await
    .expect("maintenance and asset reuse must not deadlock");
    assert!(
        maintenance
            .expect("maintenance task should not panic")
            .is_ok(),
        "maintenance should complete successfully"
    );
    writer.expect("asset writer task should not panic");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_removes_records_and_blobs_after_article_deletion(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["orphan-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    store_asset(
        &pool,
        policy,
        source_id,
        "orphan-article",
        asset_input("https://cdn.example/orphan.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("DELETE FROM articles WHERE source_id = $1 AND review_id = 'orphan-article'")
        .bind(source_id.as_uuid())
        .execute(&pool)
        .await
        .unwrap();

    let result = PostgresAssetStore::new(pool.clone(), policy)
        .maintenance()
        .await
        .unwrap();

    assert_eq!(result.orphan_records, 1);
    assert_eq!(result.orphan_blobs, 1);
    assert!(result.changed());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rejects_an_overlarge_asset_without_writing_cache_rows(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["too-large"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 4);
    let input = asset_input("https://cdn.example/too-large.png", FIRST_BYTES, 0);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "too-large", std::slice::from_ref(&input))
        .await;
    assert_eq!(
        result,
        Err(AssetRepositoryError::AssetTooLarge {
            bytes: FIRST_BYTES.len() as u64,
            max_bytes: 4,
        })
    );
    drop(unit_of_work);

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rejects_asset_bytes_mutated_after_preflight_without_writing_cache_rows(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["mutated-input"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let mut input = asset_input("https://cdn.example/mutated.png", FIRST_BYTES, 0);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();

    input.bytes = b"mutated-after-preflight".to_vec();
    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "mutated-input", std::slice::from_ref(&input))
        .await;

    assert_eq!(result, Err(AssetRepositoryError::ChecksumMismatch));
    drop(unit_of_work);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a mutated input must not persist bytes under a stale checksum"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rolls_back_a_partial_asset_batch_when_a_later_asset_cannot_fit(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["partial-batch"]).await;
    let policy = test_policy(2, Duration::from_secs(30), 1024);
    let inputs = vec![
        asset_input("https://cdn.example/partial-first.png", b"a", 0),
        asset_input("https://cdn.example/partial-second.png", b"bbb", 1),
    ];
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin_with_assets(&inputs).await.unwrap();
    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "partial-batch", &inputs)
        .await;
    assert!(matches!(
        result,
        Err(AssetRepositoryError::CapacityExceeded {
            requested_bytes: 3,
            max_bytes: 2,
        })
    ));
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a failed batch must not leave a binary blob behind"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a failed batch must not leave URL metadata behind"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM article_assets")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a failed batch must not leave an article relationship behind"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rejects_a_batch_that_would_evict_its_own_previous_asset(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["same-batch-capacity"]).await;
    let policy = test_policy(5, Duration::from_secs(30), 1024);
    let inputs = vec![
        asset_input("https://cdn.example/same-batch-first.png", b"one", 0),
        asset_input("https://cdn.example/same-batch-second.png", b"two", 1),
    ];
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin_with_assets(&inputs).await.unwrap();
    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "same-batch-capacity", &inputs)
        .await;
    assert_eq!(
        result,
        Err(AssetRepositoryError::CapacityExceeded {
            requested_bytes: 3,
            max_bytes: 5,
        })
    );
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a rejected batch must not leave a blob that was evicted during insertion"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a rejected batch must not leave URL metadata"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM article_assets")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a rejected batch must not leave article relationships"
    );
}

fn test_policy(
    max_cache_size_bytes: u64,
    max_age: Duration,
    max_asset_size_bytes: u64,
) -> AssetCachePolicy {
    AssetCachePolicy::new(
        max_cache_size_bytes,
        max_age,
        max_asset_size_bytes,
        10,
        1024 * 1024,
        Duration::from_secs(10),
        Duration::from_secs(2),
        2,
    )
    .unwrap()
}

fn asset_input(url: &str, bytes: &[u8], occurrence: u32) -> AssetInput {
    AssetInput::new(
        url.parse().unwrap(),
        url.parse().unwrap(),
        "image/png".to_owned(),
        bytes.to_vec(),
        occurrence,
        "https://mp.weixin.qq.com/s/article".parse().unwrap(),
        Some("https://mp.weixin.qq.com".to_owned()),
        Some("asset-test".to_owned()),
    )
}

async fn finish_repair_job(pool: &PgPool, job_id: Uuid) {
    sqlx::query(
        "UPDATE jobs
         SET status = 'failed', failure_count = max_attempts,
             lease_owner = NULL, lease_token = NULL, lease_until = NULL,
             heartbeat_at = NULL,
             finished_at = clock_timestamp(), updated_at = clock_timestamp()
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .unwrap();
}

async fn claim_repair_job(pool: &PgPool) -> JobLease {
    PostgresJobRepository::new(pool.clone())
        .claim_next(
            "asset-repair-test-worker",
            Utc::now(),
            chrono::Duration::minutes(5),
            &[werrss::domain::job::JobType::AssetRepair],
        )
        .await
        .unwrap()
        .expect("repair job should be claimable")
}

async fn admit_refresh_job(pool: &PgPool, review_id: &str) -> (Uuid, JobLease) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(pool, source_id, &[review_id]).await;
    let cache_policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        pool,
        cache_policy,
        source_id,
        review_id,
        asset_input(
            &format!("https://cdn.example/{review_id}.png"),
            FIRST_BYTES,
            0,
        ),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(pool)
        .await
        .unwrap();
    let store = PostgresAssetStore::new(pool.clone(), cache_policy)
        .with_repair_policy(AssetRepairPolicy::new(2, 2, Duration::from_secs(60), 3).unwrap())
        .with_refresh_policy(AssetRefreshPolicy::new(3, Duration::from_secs(60)).unwrap());
    let repair_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("public repair should be admitted, got {other:?}"),
    };
    let repair_lease = claim_repair_job(pool).await;
    assert_eq!(repair_lease.job.id(), repair_id);
    assert!(matches!(
        store
            .finish_confirmed_expiry(&repair_lease, stored.id())
            .await
            .unwrap(),
        werrss::persistence::repositories::asset_repository::AssetRefreshAdmissionResult::Enqueued {
            generation: 1,
            ordinal: 1,
            ..
        }
    ));
    let refresh_lease = PostgresJobRepository::new(pool.clone())
        .claim_next(
            "asset-refresh-test-worker",
            Utc::now(),
            chrono::Duration::minutes(5),
            &[werrss::domain::job::JobType::AssetRefresh],
        )
        .await
        .unwrap()
        .expect("refresh job should be claimable");
    (stored.id(), refresh_lease)
}

async fn store_asset(
    pool: &PgPool,
    policy: AssetCachePolicy,
    source_id: SourceId,
    review_id: &str,
    input: AssetInput,
) -> werrss::archive::asset_store::StoredAsset {
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, review_id, &[input])
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
    stored.into_iter().next().unwrap()
}

async fn store_asset_with_backend(
    pool: &PgPool,
    policy: AssetCachePolicy,
    source_id: SourceId,
    review_id: &str,
    input: AssetInput,
    backend: Arc<dyn AssetBlobStore>,
) -> werrss::archive::asset_store::StoredAsset {
    let factory = UnitOfWorkFactory::with_blob_store(pool.clone(), backend);
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, review_id, &[input])
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
    stored.into_iter().next().unwrap()
}

async fn store_asset_after_barrier(
    pool: &PgPool,
    policy: AssetCachePolicy,
    source_id: SourceId,
    review_id: &str,
    input: AssetInput,
    barrier: Arc<tokio::sync::Barrier>,
) -> werrss::archive::asset_store::StoredAsset {
    let factory = UnitOfWorkFactory::new(pool.clone());
    barrier.wait().await;
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, review_id, &[input])
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
    stored.into_iter().next().unwrap()
}

async fn store_assets_after_barrier(
    pool: &PgPool,
    policy: AssetCachePolicy,
    source_id: SourceId,
    review_id: &str,
    inputs: Vec<AssetInput>,
    barrier: Arc<tokio::sync::Barrier>,
) -> Vec<werrss::archive::asset_store::StoredAsset> {
    let factory = UnitOfWorkFactory::new(pool.clone());
    barrier.wait().await;
    let mut unit_of_work = factory.begin_with_assets(&inputs).await.unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, review_id, &inputs)
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
    stored
}

async fn wait_for_asset_update_trigger(pool: &PgPool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let held: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND granted
                   AND objsubid = 1
                   AND classid = 0::oid
                   AND objid = 2147483645::oid
             )",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if held {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "maintenance did not reach the asset update trigger"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_capacity_waiter(pool: &PgPool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks AS locks
                 JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
                 WHERE locks.locktype = 'advisory'
                   AND NOT locks.granted
                   AND locks.objsubid = 1
                   AND activity.datname = current_database()
                   AND locks.classid = (
                       (hashtextextended('asset-capacity', 0) >> 32)
                       & 4294967295
                   )::oid
                   AND locks.objid = (
                       hashtextextended('asset-capacity', 0) & 4294967295
                   )::oid
             )",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "asset read did not request the capacity lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_capacity_holder(pool: &PgPool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let held: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks AS locks
                 JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
                 WHERE locks.locktype = 'advisory'
                   AND locks.granted
                   AND locks.objsubid = 1
                   AND activity.datname = current_database()
                   AND locks.classid = (
                       (hashtextextended('asset-capacity', 0) >> 32)
                       & 4294967295
                   )::oid
                   AND locks.objid = (
                       hashtextextended('asset-capacity', 0) & 4294967295
                   )::oid
             )",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if held {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "non-empty preflight did not acquire the capacity lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_capacity_waiters(pool: &PgPool, minimum: i64) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*)::bigint
             FROM pg_locks AS locks
             JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
             WHERE locks.locktype = 'advisory'
               AND NOT locks.granted
               AND locks.objsubid = 1
               AND activity.datname = current_database()
               AND locks.classid = (
                   (hashtextextended('asset-capacity', 0) >> 32)
                   & 4294967295
               )::oid
               AND locks.objid = (
                   hashtextextended('asset-capacity', 0) & 4294967295
               )::oid",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting >= minimum {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected at least {minimum} asset capacity waiters, found {waiting}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_repair_insert_trigger(pool: &PgPool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let held: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND granted
                   AND objsubid = 1
                   AND classid = 0::oid
                   AND objid = 2147483642::oid
             )",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if held {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "repair admission did not reach the synchronization trigger"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn create_source_and_articles(pool: &PgPool, source_id: SourceId, review_ids: &[&str]) {
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .source()
        .insert(NewSource {
            id: source_id,
            book_id: format!("asset-book-{source_id}"),
            display_name: "Asset test source".to_owned(),
            article_url: Some("https://mp.weixin.qq.com/s/source".parse().unwrap()),
            enabled: true,
            sync_interval: chrono::Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: Utc::now(),
            priority: 0,
            max_attempts: 3,
        })
        .await
        .unwrap();

    for (index, review_id) in review_ids.iter().enumerate() {
        let article_url =
            VerifiedWechatArticleUrl::parse(&format!("https://mp.weixin.qq.com/s/{review_id}"))
                .unwrap();
        unit_of_work
            .articles()
            .upsert(NewArticle {
                source_id,
                review_id: (*review_id).to_owned(),
                title: format!("Asset test article {index}"),
                author: None,
                summary: None,
                cover_url: None,
                original_url: Some(article_url),
                published_at: Utc::now(),
                content_html: "<p>asset test article</p>".to_owned(),
                content_hash: Some(format!("asset-test-hash-{index}")),
                observation_version: ArticleObservationVersion::from_u64(index as u64 + 1),
                fetched_at: Utc::now(),
            })
            .await
            .unwrap();
    }
    unit_of_work.commit().await.unwrap();
}
