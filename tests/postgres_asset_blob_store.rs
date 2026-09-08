//! Integration coverage for the backend-neutral PostgreSQL blob adapter.

use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;
use werrss::{
    archive::asset_store::{AssetBlob, AssetBlobKey, AssetBlobStore, AssetBlobStoreError},
    persistence::repositories::asset_repository::PostgresAssetBlobStore,
};

const LEGACY_BYTES: &[u8] = b"legacy-image-bytes";

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_reads_existing_rows_without_a_storage_key_migration(pool: PgPool) {
    let blob_id = insert_legacy_blob(&pool, LEGACY_BYTES, "image/png").await;
    let store = PostgresAssetBlobStore::new(pool);

    let blob = store
        .read(&AssetBlobKey::for_blob_id(blob_id))
        .await
        .unwrap()
        .expect("legacy binary row should remain readable");

    assert_eq!(blob.bytes(), LEGACY_BYTES);
    assert_eq!(blob.media_type(), "image/png");
    assert_eq!(blob.byte_size(), LEGACY_BYTES.len() as u64);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_accepts_the_legacy_bare_uuid_key(pool: PgPool) {
    let blob_id = insert_legacy_blob(&pool, LEGACY_BYTES, "image/png").await;
    let store = PostgresAssetBlobStore::new(pool);
    let legacy_key = AssetBlobKey::new(blob_id.to_string()).unwrap();

    let blob = store
        .read(&legacy_key)
        .await
        .unwrap()
        .expect("legacy bare UUID key should remain readable");

    assert_eq!(blob.bytes(), LEGACY_BYTES);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_reports_metadata_for_an_existing_row(pool: PgPool) {
    let blob_id = insert_legacy_blob(&pool, LEGACY_BYTES, "image/png").await;
    let store = PostgresAssetBlobStore::new(pool);

    let metadata = store
        .stat(&AssetBlobKey::for_blob_id(blob_id))
        .await
        .unwrap()
        .expect("legacy metadata row should exist");

    assert!(metadata.is_present());
    assert_eq!(metadata.media_type(), "image/png");
    assert_eq!(metadata.byte_size(), LEGACY_BYTES.len() as u64);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_put_is_idempotent_for_the_same_legacy_key(pool: PgPool) {
    let blob_id = Uuid::new_v4();
    let key = AssetBlobKey::for_blob_id(blob_id);
    let blob = test_blob(LEGACY_BYTES, "image/png");
    let store = PostgresAssetBlobStore::new(pool.clone());

    store.put(&key, &blob).await.unwrap();
    store.put(&key, &blob).await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs WHERE id = $1")
            .bind(blob_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(store.read(&key).await.unwrap().unwrap(), blob);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_serializes_concurrent_same_key_writes(pool: PgPool) {
    let blob_id = Uuid::new_v4();
    let key = AssetBlobKey::for_blob_id(blob_id);
    let blob = test_blob(LEGACY_BYTES, "image/png");
    let store = PostgresAssetBlobStore::new(pool.clone());

    let (first, second) = tokio::join!(store.put(&key, &blob), store.put(&key, &blob));

    assert!(first.is_ok());
    assert!(second.is_ok());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs WHERE id = $1")
            .bind(blob_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_rejects_a_conflicting_rewrite(pool: PgPool) {
    let blob_id = Uuid::new_v4();
    let key = AssetBlobKey::for_blob_id(blob_id);
    let original = test_blob(LEGACY_BYTES, "image/png");
    let replacement = test_blob(b"different-image-bytes", "image/png");
    let store = PostgresAssetBlobStore::new(pool);

    store.put(&key, &original).await.unwrap();

    assert_eq!(
        store.put(&key, &replacement).await,
        Err(AssetBlobStoreError::Conflict)
    );
    assert_eq!(store.read(&key).await.unwrap().unwrap(), original);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_treats_an_absent_key_as_a_retryable_miss(pool: PgPool) {
    let store = PostgresAssetBlobStore::new(pool);
    let key = AssetBlobKey::for_blob_id(Uuid::new_v4());

    assert!(store.read(&key).await.unwrap().is_none());
    assert!(store.stat(&key).await.unwrap().is_none());
    store.delete(&key).await.unwrap();
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_delete_retains_metadata_for_repair(pool: PgPool) {
    let blob_id = Uuid::new_v4();
    let key = AssetBlobKey::for_blob_id(blob_id);
    let blob = test_blob(LEGACY_BYTES, "image/png");
    let store = PostgresAssetBlobStore::new(pool.clone());

    store.put(&key, &blob).await.unwrap();
    store.delete(&key).await.unwrap();
    store.delete(&key).await.unwrap();

    assert!(store.read(&key).await.unwrap().is_none());
    let metadata = store
        .stat(&key)
        .await
        .unwrap()
        .expect("delete should retain metadata");
    assert!(!metadata.is_present());
    assert_eq!(metadata.checksum(), blob.checksum());
    assert_eq!(metadata.byte_size(), blob.byte_size());
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT data IS NULL FROM asset_blobs WHERE id = $1",)
            .bind(blob_id)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_restores_evicted_bytes_without_replacing_the_row(pool: PgPool) {
    let blob_id = insert_legacy_blob(&pool, LEGACY_BYTES, "image/png").await;
    let key = AssetBlobKey::for_blob_id(blob_id);
    let blob = test_blob(LEGACY_BYTES, "image/png");
    let store = PostgresAssetBlobStore::new(pool.clone());

    store.delete(&key).await.unwrap();
    store.put(&key, &blob).await.unwrap();

    assert_eq!(store.read(&key).await.unwrap().unwrap(), blob);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs WHERE id = $1")
            .bind(blob_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_blob_store_rejects_corrupt_existing_bytes(pool: PgPool) {
    let blob_id = insert_legacy_blob(&pool, LEGACY_BYTES, "image/png").await;
    let wrong_checksum = format!("{:x}", Sha256::digest(b"different-bytes"));
    sqlx::query("UPDATE asset_blobs SET checksum = $2 WHERE id = $1")
        .bind(blob_id)
        .bind(wrong_checksum)
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresAssetBlobStore::new(pool);

    assert_eq!(
        store.read(&AssetBlobKey::for_blob_id(blob_id)).await,
        Err(AssetBlobStoreError::ChecksumMismatch)
    );
}

async fn insert_legacy_blob(pool: &PgPool, bytes: &[u8], media_type: &str) -> Uuid {
    let blob_id = Uuid::new_v4();
    let checksum = format!("{:x}", Sha256::digest(bytes));
    sqlx::query(
        "INSERT INTO asset_blobs
                (id, checksum_algorithm, checksum, byte_size, media_type, data)
         VALUES ($1, 'sha256', $2, $3, $4, $5)",
    )
    .bind(blob_id)
    .bind(checksum)
    .bind(i64::try_from(bytes.len()).unwrap())
    .bind(media_type)
    .bind(bytes)
    .execute(pool)
    .await
    .unwrap();
    blob_id
}

fn test_blob(bytes: &[u8], media_type: &str) -> AssetBlob {
    AssetBlob::new(
        bytes.to_vec(),
        media_type.to_owned(),
        format!("{:x}", Sha256::digest(bytes)),
    )
    .unwrap()
}
