//! PostgreSQL persistence for archived article assets.
//!
//! Metadata and binary bytes deliberately have different lifetimes. Eviction
//! clears `asset_blobs.data` while retaining the URL/version row and the
//! article relationship, so the stable public asset ID can report a repairable
//! miss later. Metadata and article attachment are transaction-scoped; binary
//! operations go through the injected blob port and compare raw bytes after
//! using SHA-256 and size as candidate keys.

use std::{
    any::Any,
    collections::{BTreeSet, HashMap},
    fmt,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use serde_json::json;
use sqlx::{Acquire, PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::archive::asset_store::{
    AssetBlob, AssetBlobKey, AssetBlobMetadata, AssetBlobStore, AssetBlobStoreError,
    AssetCachePolicy, AssetInput, AssetMaintenanceResult, AssetRead, AssetRefreshPolicy,
    AssetRepairPolicy, AssetRepairTarget, StoredAsset,
};
use crate::domain::job::{JobType, NewJob};
use crate::domain::url_policy::has_provider_signature;

use super::job_repository::{
    JobLease, JobRepositoryError, JobRepositoryTransaction, PostgresJobTransaction,
};

// Keep one queue-level retry independent from the per-asset admission budget:
// an expired lease must be claimable by another worker even when the public
// request has admitted only one repair attempt.
const REPAIR_JOB_MAX_ATTEMPTS: u32 = 2;

/// Errors returned while storing or reading archived assets.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AssetRepositoryError {
    /// A source or article identity was empty or nil.
    #[error("asset article identity is invalid")]
    InvalidArticleIdentity,
    /// A URL supplied by an acquisition adapter was not an approved HTTP URL.
    #[error("asset URL is not a safe HTTP or HTTPS URL")]
    InvalidUrl,
    /// Only validated image media types are eligible for this first backend.
    #[error("asset media type is not a supported image type")]
    InvalidMediaType,
    /// The response body was empty.
    #[error("asset response body is empty")]
    EmptyBody,
    /// One binary exceeds the configured per-asset limit.
    #[error("asset is {bytes} bytes, above the configured {max_bytes}-byte limit")]
    AssetTooLarge {
        /// Received body size.
        bytes: u64,
        /// Configured maximum body size.
        max_bytes: u64,
    },
    /// A batch exceeds the configured per-article count limit.
    #[error("article asset count exceeds the configured limit of {max_count}")]
    TooManyAssets {
        /// Configured maximum count.
        max_count: u32,
    },
    /// The aggregate byte limit cannot admit the new body.
    #[error("asset cache cannot admit {requested_bytes} bytes within its {max_bytes}-byte limit")]
    CapacityExceeded {
        /// Body size that was requested.
        requested_bytes: u64,
        /// Configured aggregate limit.
        max_bytes: u64,
    },
    /// A stored URL or request context could not be decoded safely.
    #[error("asset metadata is invalid")]
    InvalidMetadata,
    /// The public asset input was mutated after its checksum was computed.
    #[error("asset checksum does not match the response body")]
    ChecksumMismatch,
    /// Asset writes must be preceded by a preflight before article locks are held.
    #[error("asset inputs were not preflighted before persistence")]
    PreparationRequired,
    /// The worker lease no longer authorizes an asset-repair mutation.
    #[error("asset repair job lease is no longer live")]
    LeaseLost,
    /// The database operation failed.
    #[error("asset repository storage error: {0}")]
    Storage(String),
    /// The binary storage adapter rejected or could not complete an operation.
    #[error("asset blob storage error: {0}")]
    BlobStore(#[from] AssetBlobStoreError),
}

/// Result of admitting a public cache-miss repair request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetRepairEnqueueResult {
    /// A new durable repair job was admitted.
    Enqueued { job_id: Uuid },
    /// An active repair already exists for this stable asset record.
    AlreadyActive { job_id: Uuid },
    /// The record is not referenced by an article or does not exist.
    NotFound,
    /// Another request restored the bytes before admission completed.
    AlreadyAvailable,
    /// The cluster-wide or public repair cap is currently full.
    CapacityFull,
    /// A previous failed attempt is still in its backoff window.
    Backoff,
    /// The bounded repair-attempt budget is exhausted.
    Exhausted,
    /// A refresh chain is active or pending without an eligible snapshot entry.
    Retryable,
}

/// Immutable target persisted on one admitted whole-article refresh job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArticleRefreshTarget {
    /// Article source identity.
    pub source_id: crate::domain::source::SourceId,
    /// Article provider identity.
    pub review_id: String,
    /// Asset record whose expiry triggered this generation.
    pub expired_asset_record_id: Uuid,
    /// Active serial chain identity.
    pub chain_id: Uuid,
    /// Entry ordinal, also the active chain's forward cursor.
    pub chain_ordinal: i64,
    /// Generation captured by this immutable snapshot.
    pub served_generation: i64,
}

/// Terminal result for a whole-article refresh attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArticleRefreshOutcome {
    /// Full article and all required assets were acquired and committed.
    Succeeded,
    /// The page or one or more required assets were incomplete.
    Inconclusive,
    /// A newer observation superseded this refresh.
    Superseded,
    /// The article was deleted before this refresh committed.
    Deleted,
}

/// Result of a terminal refresh transition and serial successor admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshFinishResult {
    /// Successor refresh job admitted into the same capacity slot, if any.
    pub next_job_id: Option<Uuid>,
    /// Whether this generation no longer has unfinished entries.
    pub generation_completed: bool,
}

/// Result of routing a confirmed asset expiry through the refresh snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetRefreshAdmissionResult {
    /// No article from the selected snapshot is eligible now.
    NoEligibleArticle,
    /// One refresh was admitted using the triggering job's capacity slot.
    Enqueued {
        /// Inserted durable job.
        job_id: Uuid,
        /// Serial chain identity.
        chain_id: Uuid,
        /// Snapshot generation.
        generation: i64,
        /// Selected article ordinal.
        ordinal: i64,
    },
    /// This repair job had already been completed or refunded.
    AlreadyHandled,
}

/// Result of restoring bytes into an existing stable asset record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetRepairRestoreResult {
    /// The missing record now points at available bytes.
    Restored,
    /// The record was already available, so the operation was idempotent.
    AlreadyAvailable,
    /// The record is no longer referenced or no longer exists.
    NotFound,
}

/// Transaction-scoped asset persistence operations.
#[async_trait::async_trait]
pub trait AssetTransactionRepository {
    /// Adds validated bodies, deduplicates them, and attaches them to one
    /// already-persisted article.
    async fn store_for_article(
        &mut self,
        source_id: crate::domain::source::SourceId,
        review_id: &str,
        inputs: &[AssetInput],
    ) -> Result<Vec<StoredAsset>, AssetRepositoryError>;

    /// Removes all article-to-asset relationships without deleting asset
    /// metadata or binary blobs. Callers use this when an article is
    /// intentionally persisted with external URLs after a cache failure.
    async fn clear_for_article(
        &mut self,
        source_id: crate::domain::source::SourceId,
        review_id: &str,
    ) -> Result<(), AssetRepositoryError>;

    /// Replaces all asset relationships for one full article observation.
    ///
    /// The relationship rows are cleared before the new batch is stored so a
    /// successful full observation cannot retain links from an older version.
    async fn replace_for_article(
        &mut self,
        source_id: crate::domain::source::SourceId,
        review_id: &str,
        inputs: &[AssetInput],
    ) -> Result<Vec<StoredAsset>, AssetRepositoryError>;

    /// Locks and returns the persisted target for a live, fenced refresh job.
    async fn begin_article_refresh(
        &mut self,
        lease: &JobLease,
    ) -> Result<Option<ArticleRefreshTarget>, AssetRepositoryError>;

    /// Applies one refresh outcome and admits a serial successor when eligible.
    async fn finish_article_refresh(
        &mut self,
        lease: &JobLease,
        outcome: ArticleRefreshOutcome,
        policy: AssetRefreshPolicy,
    ) -> Result<RefreshFinishResult, AssetRepositoryError>;

    /// Fenced-succeeds a refresh job whose persisted target cannot be validated.
    async fn complete_unresolvable_article_refresh(
        &mut self,
        lease: &JobLease,
    ) -> Result<(), AssetRepositoryError>;

    /// Clears article refresh lineage after a complete usable normal acquisition.
    ///
    /// The caller's unit of work must already hold the asset-capacity and
    /// recovery-admission locks (for example
    /// `UnitOfWorkFactory::begin_article_acquisition_with_assets`) because this
    /// method reconciles generations and may admit a successor refresh job.
    async fn reset_article_refresh_lineage(
        &mut self,
        source_id: crate::domain::source::SourceId,
        review_id: &str,
    ) -> Result<(), AssetRepositoryError>;
}

/// PostgreSQL implementation of the backend-neutral binary storage port.
///
/// Existing rows use their `asset_blobs.id` UUID as the legacy storage key.
/// This adapter deliberately requires no schema migration: a future backend
/// can use the same opaque `blobs/{uuid}` key convention while old database
/// rows continue to be readable.
#[derive(Clone)]
pub struct PostgresAssetBlobStore {
    pool: PgPool,
}

impl fmt::Debug for PostgresAssetBlobStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAssetBlobStore")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresAssetBlobStore {
    /// Creates a database blob adapter over an existing PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Reads a legacy database blob inside an existing repository transaction.
    async fn read_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        key: &AssetBlobKey,
    ) -> Result<Option<AssetBlob>, AssetBlobStoreError> {
        let blob_id = legacy_blob_id(key)?;
        let row = sqlx::query(
            "SELECT checksum, media_type, byte_size, data
             FROM asset_blobs
             WHERE id = $1",
        )
        .bind(blob_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(blob_backend_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let checksum = row
            .try_get::<String, _>("checksum")
            .map_err(blob_backend_error)?;
        let media_type = row
            .try_get::<String, _>("media_type")
            .map_err(blob_backend_error)?;
        let byte_size = row
            .try_get::<i64, _>("byte_size")
            .map_err(blob_backend_error)?;
        let Some(bytes) = row
            .try_get::<Option<Vec<u8>>, _>("data")
            .map_err(blob_backend_error)?
        else {
            return Ok(None);
        };
        let expected_size =
            u64::try_from(byte_size).map_err(|_| AssetBlobStoreError::InvalidMetadata)?;
        if expected_size != bytes.len() as u64 {
            return Err(AssetBlobStoreError::InvalidMetadata);
        }
        Ok(Some(AssetBlob::new(bytes, media_type, checksum)?))
    }

    /// Reads blob metadata inside an existing repository transaction.
    async fn stat_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        key: &AssetBlobKey,
    ) -> Result<Option<AssetBlobMetadata>, AssetBlobStoreError> {
        let blob_id = legacy_blob_id(key)?;
        let row = sqlx::query(
            "SELECT checksum, media_type, byte_size, data IS NOT NULL AS present
             FROM asset_blobs
             WHERE id = $1",
        )
        .bind(blob_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(blob_backend_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(blob_metadata_from_row(&row)?))
    }

    /// Writes a blob inside the caller's existing PostgreSQL transaction.
    async fn put_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        key: &AssetBlobKey,
        blob: &AssetBlob,
    ) -> Result<(), AssetBlobStoreError> {
        let blob_id = legacy_blob_id(key)?;
        let byte_size =
            i64::try_from(blob.byte_size()).map_err(|_| AssetBlobStoreError::SizeOverflow)?;
        let inserted = sqlx::query(
            "INSERT INTO asset_blobs
                    (id, checksum_algorithm, checksum, byte_size, media_type, data)
             VALUES ($1, 'sha256', $2, $3, $4, $5)
             ON CONFLICT (id) DO NOTHING
             RETURNING id",
        )
        .bind(blob_id)
        .bind(blob.checksum())
        .bind(byte_size)
        .bind(blob.media_type())
        .bind(blob.bytes())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(blob_backend_error)?;
        if inserted.is_some() {
            return Ok(());
        }

        let existing = sqlx::query(
            "SELECT checksum, media_type, byte_size, data
             FROM asset_blobs
             WHERE id = $1
             FOR UPDATE",
        )
        .bind(blob_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(blob_backend_error)?;

        if let Some(existing) = existing {
            let existing_checksum = existing
                .try_get::<String, _>("checksum")
                .map_err(blob_backend_error)?;
            let existing_media_type = existing
                .try_get::<String, _>("media_type")
                .map_err(blob_backend_error)?;
            let existing_size = existing
                .try_get::<i64, _>("byte_size")
                .map_err(blob_backend_error)?;
            let existing_data = existing
                .try_get::<Option<Vec<u8>>, _>("data")
                .map_err(blob_backend_error)?;
            if existing_checksum != blob.checksum()
                || existing_media_type != blob.media_type()
                || existing_size != byte_size
                || existing_data
                    .as_deref()
                    .is_some_and(|data| data != blob.bytes())
            {
                return Err(AssetBlobStoreError::Conflict);
            }
            if existing_data.is_none() {
                sqlx::query(
                    "UPDATE asset_blobs
                     SET data = $2, last_fetched_at = clock_timestamp(),
                         last_accessed_at = clock_timestamp()
                     WHERE id = $1",
                )
                .bind(blob_id)
                .bind(blob.bytes())
                .execute(&mut **transaction)
                .await
                .map_err(blob_backend_error)?;
            }
            return Ok(());
        }

        Err(AssetBlobStoreError::Backend(
            "asset blob disappeared after a conflicting insert".to_owned(),
        ))
    }

    /// Clears binary data inside an existing repository transaction while
    /// retaining the metadata row for repair and legacy compatibility.
    async fn delete_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        key: &AssetBlobKey,
    ) -> Result<(), AssetBlobStoreError> {
        let blob_id = legacy_blob_id(key)?;
        sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
            .bind(blob_id)
            .execute(&mut **transaction)
            .await
            .map_err(blob_backend_error)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AssetBlobStore for PostgresAssetBlobStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read(&self, key: &AssetBlobKey) -> Result<Option<AssetBlob>, AssetBlobStoreError> {
        let mut transaction = self.pool.begin().await.map_err(blob_backend_error)?;
        let result = Self::read_in_transaction(&mut transaction, key).await?;
        transaction.commit().await.map_err(blob_backend_error)?;
        Ok(result)
    }

    async fn stat(
        &self,
        key: &AssetBlobKey,
    ) -> Result<Option<AssetBlobMetadata>, AssetBlobStoreError> {
        let mut transaction = self.pool.begin().await.map_err(blob_backend_error)?;
        let result = Self::stat_in_transaction(&mut transaction, key).await?;
        transaction.commit().await.map_err(blob_backend_error)?;
        Ok(result)
    }

    async fn put(&self, key: &AssetBlobKey, blob: &AssetBlob) -> Result<(), AssetBlobStoreError> {
        let mut transaction = self.pool.begin().await.map_err(blob_backend_error)?;
        Self::put_in_transaction(&mut transaction, key, blob).await?;
        transaction.commit().await.map_err(blob_backend_error)?;
        Ok(())
    }

    async fn delete(&self, key: &AssetBlobKey) -> Result<(), AssetBlobStoreError> {
        let mut transaction = self.pool.begin().await.map_err(blob_backend_error)?;
        Self::delete_in_transaction(&mut transaction, key).await?;
        transaction.commit().await.map_err(blob_backend_error)?;
        Ok(())
    }
}

fn legacy_blob_id(key: &AssetBlobKey) -> Result<Uuid, AssetBlobStoreError> {
    let value = key.as_str().strip_prefix("blobs/").unwrap_or(key.as_str());
    Uuid::parse_str(value).map_err(|_| AssetBlobStoreError::InvalidMetadata)
}

fn blob_metadata_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<AssetBlobMetadata, AssetBlobStoreError> {
    let checksum = row
        .try_get::<String, _>("checksum")
        .map_err(blob_backend_error)?;
    let media_type = row
        .try_get::<String, _>("media_type")
        .map_err(blob_backend_error)?;
    let byte_size = row
        .try_get::<i64, _>("byte_size")
        .map_err(blob_backend_error)?;
    let byte_size = u64::try_from(byte_size).map_err(|_| AssetBlobStoreError::InvalidMetadata)?;
    let present = row
        .try_get::<bool, _>("present")
        .map_err(blob_backend_error)?;
    AssetBlobMetadata::new(checksum, media_type, byte_size, present)
}

fn blob_backend_error(error: impl fmt::Display) -> AssetBlobStoreError {
    AssetBlobStoreError::Backend(error.to_string())
}

// The injected port remains backend-neutral, but the built-in PostgreSQL
// implementation must participate in the repository transaction. Falling
// back to the port methods keeps filesystem/object-store adapters unchanged.
async fn blob_read_in_transaction(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    key: &AssetBlobKey,
) -> Result<Option<AssetBlob>, AssetRepositoryError> {
    if blob_store.as_any().is::<PostgresAssetBlobStore>() {
        return Ok(PostgresAssetBlobStore::read_in_transaction(transaction, key).await?);
    }
    Ok(blob_store.read(key).await?)
}

async fn blob_stat_in_transaction(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    key: &AssetBlobKey,
) -> Result<Option<AssetBlobMetadata>, AssetRepositoryError> {
    if blob_store.as_any().is::<PostgresAssetBlobStore>() {
        return Ok(PostgresAssetBlobStore::stat_in_transaction(transaction, key).await?);
    }
    Ok(blob_store.stat(key).await?)
}

async fn blob_put_in_transaction(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    key: &AssetBlobKey,
    blob: &AssetBlob,
) -> Result<(), AssetRepositoryError> {
    if blob_store.as_any().is::<PostgresAssetBlobStore>() {
        PostgresAssetBlobStore::put_in_transaction(transaction, key, blob).await?;
        return Ok(());
    }
    blob_store.put(key, blob).await?;
    Ok(())
}

async fn blob_delete_in_transaction(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    key: &AssetBlobKey,
) -> Result<(), AssetRepositoryError> {
    if blob_store.as_any().is::<PostgresAssetBlobStore>() {
        PostgresAssetBlobStore::delete_in_transaction(transaction, key).await?;
    } else {
        blob_store.delete(key).await?;
    }
    Ok(())
}

/// Pool-backed asset reads and cache maintenance.
#[derive(Clone)]
pub struct PostgresAssetStore {
    pool: PgPool,
    blob_store: Arc<dyn AssetBlobStore>,
    policy: AssetCachePolicy,
    repair_policy: AssetRepairPolicy,
    refresh_policy: AssetRefreshPolicy,
}

impl fmt::Debug for PostgresAssetStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAssetStore")
            .field("pool", &"<postgres pool>")
            .field("policy", &self.policy)
            .field("repair_policy", &self.repair_policy)
            .field("refresh_policy", &self.refresh_policy)
            .finish()
    }
}

impl PostgresAssetStore {
    /// Creates a database-backed asset store.
    pub fn new(pool: PgPool, policy: AssetCachePolicy) -> Self {
        Self::with_blob_store(
            pool.clone(),
            policy,
            Arc::new(PostgresAssetBlobStore::new(pool)),
        )
    }

    /// Creates a PostgreSQL metadata store over an injected binary backend.
    ///
    /// The metadata, relationship, deduplication, and lease records remain in
    /// PostgreSQL. The injected port owns only binary bytes, which lets a
    /// filesystem or object-store adapter be selected without changing the
    /// repository's public asset operations.
    pub fn with_blob_store(
        pool: PgPool,
        policy: AssetCachePolicy,
        blob_store: Arc<dyn AssetBlobStore>,
    ) -> Self {
        Self {
            blob_store,
            pool,
            policy,
            repair_policy: AssetRepairPolicy::default(),
            refresh_policy: AssetRefreshPolicy::default(),
        }
    }

    /// Returns the injected common blob-storage port.
    pub fn blob_store(&self) -> &dyn AssetBlobStore {
        self.blob_store.as_ref()
    }

    /// Applies the durable admission policy used for public cache misses.
    pub const fn with_repair_policy(mut self, repair_policy: AssetRepairPolicy) -> Self {
        self.repair_policy = repair_policy;
        self
    }

    /// Returns the policy used by this store.
    pub const fn policy(&self) -> AssetCachePolicy {
        self.policy
    }

    /// Returns the repair admission policy used by this store.
    pub const fn repair_policy(&self) -> AssetRepairPolicy {
        self.repair_policy
    }

    /// Applies the bounded article-lineage policy for refresh admission.
    pub const fn with_refresh_policy(mut self, refresh_policy: AssetRefreshPolicy) -> Self {
        self.refresh_policy = refresh_policy;
        self
    }

    /// Returns the article-lineage refresh policy used by this store.
    pub const fn refresh_policy(&self) -> AssetRefreshPolicy {
        self.refresh_policy
    }

    /// Reads one referenced asset and touches its last-accessed timestamp.
    ///
    /// A missing blob returns [`AssetRead::Missing`] without attempting any
    /// network work. The URL remains available to a future repair worker.
    pub async fn read_and_touch(
        &self,
        asset_id: Uuid,
    ) -> Result<Option<AssetRead>, AssetRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        // Serialize the read snapshot and access-time update with eviction.
        // This lock must be acquired before selecting the bytes; taking it
        // after the SELECT would still allow maintenance to evict a blob
        // while the response is being prepared.
        lock_asset_capacity(&mut transaction).await?;
        let row = sqlx::query(
            "SELECT r.source_url, r.blob_id,
                    EXISTS (
                        SELECT 1 FROM asset_blob_deletion_queue AS q
                        WHERE q.blob_id = r.blob_id
                    ) AS deletion_pending
             FROM asset_records AS r
             WHERE r.id = $1
               AND EXISTS (
                   SELECT 1
                   FROM article_assets AS aa
                   WHERE aa.asset_record_id = r.id
               )",
        )
        .bind(asset_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;

        let Some(row) = row else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(None);
        };

        let source_url = parse_url(
            &row.try_get::<String, _>("source_url")
                .map_err(storage_error)?,
        )?;
        let blob_id = row
            .try_get::<Option<Uuid>, _>("blob_id")
            .map_err(storage_error)?;
        let deletion_pending = row
            .try_get::<bool, _>("deletion_pending")
            .map_err(storage_error)?;
        let blob = match blob_id {
            Some(blob_id) if !deletion_pending => {
                blob_read_in_transaction(
                    self.blob_store.as_ref(),
                    &mut transaction,
                    &AssetBlobKey::for_blob_id(blob_id),
                )
                .await?
            }
            Some(_) => None,
            None => None,
        };

        let result = match blob {
            Some(blob) => {
                sqlx::query(
                    "UPDATE asset_blobs
                     SET last_accessed_at = clock_timestamp()
                     WHERE id = (
                         SELECT blob_id FROM asset_records WHERE id = $1
                     )",
                )
                .bind(asset_id)
                .execute(&mut *transaction)
                .await
                .map_err(storage_error)?;
                Some(AssetRead::Available {
                    media_type: blob.media_type().to_owned(),
                    checksum: blob.checksum().to_owned(),
                    bytes: blob.bytes().to_vec(),
                })
            }
            _ => Some(AssetRead::Missing { source_url }),
        };
        transaction.commit().await.map_err(storage_error)?;
        Ok(result)
    }

    /// Returns the upstream request context for a referenced missing asset.
    pub async fn repair_target(
        &self,
        asset_id: Uuid,
    ) -> Result<Option<AssetRepairTarget>, AssetRepositoryError> {
        let row = sqlx::query(
            "SELECT r.source_url, r.blob_id, aa.referer_url, aa.origin, aa.user_agent,
                    ars.next_allowed_at, COALESCE(ars.exhausted, FALSE) AS exhausted
             FROM asset_records AS r
             JOIN article_assets AS aa ON aa.asset_record_id = r.id
             LEFT JOIN asset_repair_states AS ars ON ars.asset_record_id = r.id
             WHERE r.id = $1
               AND NOT EXISTS (
                   SELECT 1 FROM asset_blob_deletion_queue AS q
                   WHERE q.blob_id = r.blob_id
               )
             ORDER BY aa.source_id, aa.review_id, aa.occurrence
             LIMIT 1",
        )
        .bind(asset_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        if let Some(blob_id) = row
            .try_get::<Option<Uuid>, _>("blob_id")
            .map_err(storage_error)?
        {
            if self
                .blob_store
                .stat(&AssetBlobKey::for_blob_id(blob_id))
                .await?
                .is_some_and(|metadata| metadata.is_present())
            {
                return Ok(None);
            }
        }
        Ok(Some(AssetRepairTarget {
            asset_id,
            source_url: parse_url(
                &row.try_get::<String, _>("source_url")
                    .map_err(storage_error)?,
            )?,
            referer_url: parse_url(
                &row.try_get::<String, _>("referer_url")
                    .map_err(storage_error)?,
            )?,
            origin: row.try_get("origin").map_err(storage_error)?,
            user_agent: row.try_get("user_agent").map_err(storage_error)?,
            next_allowed_at: row.try_get("next_allowed_at").map_err(storage_error)?,
            exhausted: row.try_get("exhausted").map_err(storage_error)?,
        }))
    }

    /// Admits one public repair job without doing network work in the request.
    ///
    /// The admission lock, active-job lookup, capacity check, state update,
    /// and job insert are one transaction. This makes repeated requests from
    /// several API replicas converge on one durable job and prevents a burst
    /// of cache misses from bypassing the cluster cap.
    pub async fn enqueue_repair(
        &self,
        asset_id: Uuid,
    ) -> Result<AssetRepairEnqueueResult, AssetRepositoryError> {
        let mut transaction = PostgresJobTransaction::begin(&self.pool)
            .await
            .map_err(storage_error)?;
        let result = enqueue_repair_in_transaction(
            &mut transaction,
            self.blob_store.as_ref(),
            self.repair_policy,
            self.refresh_policy,
            asset_id,
        )
        .await?;
        transaction.commit_inner().await.map_err(storage_error)?;
        Ok(result)
    }

    /// Atomically refunds confirmed public expiry and transfers the repair slot
    /// to one eligible whole-article refresh job.
    pub async fn finish_confirmed_expiry(
        &self,
        lease: &JobLease,
        asset_id: Uuid,
    ) -> Result<AssetRefreshAdmissionResult, AssetRepositoryError> {
        if lease.job.job_type() != JobType::AssetRepair {
            return Err(AssetRepositoryError::InvalidMetadata);
        }
        let owner = lease
            .job
            .lease_owner()
            .ok_or(AssetRepositoryError::LeaseLost)?;
        let mut transaction = PostgresJobTransaction::begin(&self.pool)
            .await
            .map_err(storage_error)?;
        {
            let sql_transaction = transaction
                .transaction_mut()
                .map_err(job_transaction_error)?;
            lock_asset_capacity(sql_transaction).await?;
            lock_asset_recovery_admission(sql_transaction).await?;
            lock_live_repair_lease(sql_transaction, lease).await?;
        }
        let repair = sqlx::query(
            "SELECT admission_class, asset_record_id, expired_asset_record_id, expiry_refunded
             FROM asset_repair_jobs WHERE job_id = $1 FOR UPDATE",
        )
        .bind(lease.job.id())
        .fetch_optional(
            &mut **transaction
                .transaction_mut()
                .map_err(job_transaction_error)?,
        )
        .await
        .map_err(storage_error)?
        .ok_or(AssetRepositoryError::InvalidMetadata)?;
        let admission_class: String = repair.try_get("admission_class").map_err(storage_error)?;
        let repair_asset_id: Option<Uuid> =
            repair.try_get("asset_record_id").map_err(storage_error)?;
        if repair_asset_id.is_some_and(|current| current != asset_id) {
            return Err(AssetRepositoryError::InvalidMetadata);
        }
        if admission_class == "public_repair"
            && !repair
                .try_get::<bool, _>("expiry_refunded")
                .map_err(storage_error)?
        {
            let cooldown = i64::try_from(self.repair_policy.requeue_after().as_secs())
                .map_err(|_| AssetRepositoryError::InvalidMetadata)?;
            let sql_transaction = transaction
                .transaction_mut()
                .map_err(job_transaction_error)?;
            sqlx::query(
                "UPDATE asset_repair_states
                 SET admitted_attempts = GREATEST(admitted_attempts - 1, 0),
                     next_allowed_at = clock_timestamp()
                         + make_interval(secs => $2::double precision),
                     exhausted = FALSE, last_error = NULL,
                     updated_at = clock_timestamp()
                 WHERE asset_record_id = $1",
            )
            .bind(asset_id)
            .bind(cooldown)
            .execute(&mut **sql_transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query(
                "UPDATE asset_repair_jobs SET expiry_refunded = TRUE
                 WHERE job_id = $1 AND admission_class = 'public_repair'
                   AND expiry_refunded = FALSE",
            )
            .bind(lease.job.id())
            .execute(&mut **sql_transaction)
            .await
            .map_err(storage_error)?;
        }
        let result = route_confirmed_expiry(
            &mut transaction,
            self.blob_store.as_ref(),
            asset_id,
            self.repair_policy,
            self.refresh_policy,
        )
        .await?;
        JobRepositoryTransaction::succeed(
            &mut transaction,
            lease.job.id(),
            owner,
            lease.token,
            Utc::now(),
        )
        .await
        .map_err(job_transaction_error)?;
        transaction.commit_inner().await.map_err(storage_error)?;
        Ok(result)
    }

    /// Restores bytes into the existing asset record, preserving its public ID.
    pub async fn restore_missing_asset(
        &self,
        lease: &JobLease,
        asset_id: Uuid,
        input: &AssetInput,
    ) -> Result<AssetRepairRestoreResult, AssetRepositoryError> {
        validate_input(input, self.policy)?;
        let source_url = normalize_url(input.source_url.clone())?;
        let final_url = normalize_url(input.final_url.clone())?;
        let checksum = input.checksum().to_owned();
        let byte_size = input_byte_size(input)?;
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        lock_asset_capacity(&mut transaction).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-url:' || $1, 0))")
            .bind(source_url.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-checksum:' || $1, 0))")
            .bind(&checksum)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;

        let record = sqlx::query(
            "SELECT r.source_url, r.blob_id
             FROM asset_records AS r
             WHERE r.id = $1
               AND EXISTS (
                   SELECT 1 FROM article_assets AS aa
                   WHERE aa.asset_record_id = r.id
               )
             FOR UPDATE OF r",
        )
        .bind(asset_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some(record) = record else {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(AssetRepairRestoreResult::NotFound);
        };
        let record_source_url = normalize_url(
            Url::parse(
                &record
                    .try_get::<String, _>("source_url")
                    .map_err(storage_error)?,
            )
            .map_err(|_| AssetRepositoryError::InvalidMetadata)?,
        )?;
        if record_source_url != source_url {
            return Err(AssetRepositoryError::InvalidMetadata);
        }
        if let Some(blob_id) = record
            .try_get::<Option<Uuid>, _>("blob_id")
            .map_err(storage_error)?
        {
            if !blob_deletion_pending(&mut transaction, blob_id).await?
                && blob_stat_in_transaction(
                    self.blob_store.as_ref(),
                    &mut transaction,
                    &AssetBlobKey::for_blob_id(blob_id),
                )
                .await?
                .is_some_and(|metadata| metadata.is_present())
            {
                transaction.commit().await.map_err(storage_error)?;
                return Ok(AssetRepairRestoreResult::AlreadyAvailable);
            }
        }

        let candidates = sqlx::query(
            "SELECT id
             FROM asset_blobs
             WHERE checksum = $1 AND byte_size = $2
               AND NOT EXISTS (
                   SELECT 1 FROM asset_blob_deletion_queue AS q
                   WHERE q.blob_id = asset_blobs.id
               )
             ORDER BY id
             FOR UPDATE",
        )
        .bind(&checksum)
        .bind(byte_size)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let mut matching = None;
        for row in candidates {
            let candidate_id = row.try_get::<Uuid, _>("id").map_err(storage_error)?;
            let Some(candidate) = blob_read_in_transaction(
                self.blob_store.as_ref(),
                &mut transaction,
                &AssetBlobKey::for_blob_id(candidate_id),
            )
            .await?
            else {
                continue;
            };
            if candidate.bytes() == input.bytes {
                matching = Some(candidate_id);
                break;
            }
        }
        let mutation_state = Arc::new(Mutex::new(AssetMutationState::default()));
        let (blob_id, newly_written) = if let Some(matching) = matching {
            (matching, false)
        } else {
            let mut context = AssetWriteContext::new(
                self.blob_store.as_ref(),
                self.policy,
                None,
                &mutation_state,
            );
            let inserted = insert_blob(
                &mut transaction,
                &mut context,
                input,
                &checksum,
                byte_size,
                &[],
            )
            .await;
            let blob_id = match inserted {
                Ok((blob_id, _)) => blob_id,
                Err(error) => {
                    let blob_ids = mutation_state.lock().unwrap().newly_written.clone();
                    let rollback = transaction.rollback().await;
                    let cleanup = cleanup_blob_ids_after_rollback(
                        &self.pool,
                        self.blob_store.as_ref(),
                        &blob_ids,
                    )
                    .await;
                    if let Err(cleanup_error) = cleanup {
                        return Err(AssetRepositoryError::Storage(format!(
                            "{error}; external blob cleanup failed: {cleanup_error}"
                        )));
                    }
                    if let Err(rollback_error) = rollback {
                        return Err(AssetRepositoryError::Storage(format!(
                            "{error}; restore transaction rollback failed: {rollback_error}"
                        )));
                    }
                    return Err(error);
                }
            };
            (blob_id, true)
        };
        // Hold the job row lock through commit so lease recovery cannot grant
        // the same job to another worker while this mutation is being fenced.
        let result = async {
            lock_live_repair_lease(&mut transaction, lease).await?;
            sqlx::query(
                "UPDATE asset_records
             SET final_url = $2, blob_id = $3, fetch_status = 'available',
                 last_error = NULL, updated_at = clock_timestamp()
             WHERE id = $1",
            )
            .bind(asset_id)
            .bind(final_url.as_str())
            .bind(blob_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            sqlx::query(
                "UPDATE asset_blobs
             SET last_fetched_at = clock_timestamp(), last_accessed_at = clock_timestamp()
             WHERE id = $1",
            )
            .bind(blob_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            Ok::<_, AssetRepositoryError>(())
        }
        .await;
        if let Err(error) = result {
            let blob_ids = if newly_written {
                vec![blob_id]
            } else {
                Vec::new()
            };
            let rollback = transaction.rollback().await;
            let cleanup =
                cleanup_blob_ids_after_rollback(&self.pool, self.blob_store.as_ref(), &blob_ids)
                    .await;
            if let Err(cleanup_error) = cleanup {
                return Err(AssetRepositoryError::Storage(format!(
                    "{error}; external blob cleanup failed: {cleanup_error}"
                )));
            }
            if let Err(rollback_error) = rollback {
                return Err(AssetRepositoryError::Storage(format!(
                    "{error}; restore transaction rollback failed: {rollback_error}"
                )));
            }
            return Err(error);
        }
        // A commit error is ambiguous: PostgreSQL may have committed the
        // metadata, so never delete the external object here.
        transaction.commit().await.map_err(storage_error)?;
        Ok(AssetRepairRestoreResult::Restored)
    }

    /// Records a failed admitted repair and opens a bounded backoff window.
    /// Returns whether the per-record repair circuit is exhausted.
    pub async fn record_repair_failure(
        &self,
        lease: &JobLease,
        asset_id: Uuid,
        error: &str,
    ) -> Result<bool, AssetRepositoryError> {
        let seconds = i64::try_from(self.repair_policy.requeue_after().as_secs())
            .map_err(|_| AssetRepositoryError::Storage("repair delay is too large".to_owned()))?;
        let safe_error: String = error.chars().take(512).collect();
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0))")
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        // Keep repair-state mutations in the same lock order as admission:
        // admission lock first, then the fenced job row.
        lock_live_repair_lease(&mut transaction, lease).await?;
        let exhausted = sqlx::query_scalar::<_, bool>(
            "UPDATE asset_repair_states
             SET next_allowed_at = clock_timestamp()
                     + make_interval(secs => $2::double precision),
                 exhausted = admitted_attempts >= $3,
                 last_error = $4,
                 updated_at = clock_timestamp()
             WHERE asset_record_id = $1
             RETURNING exhausted",
        )
        .bind(asset_id)
        .bind(seconds)
        .bind(i64::from(self.repair_policy.max_attempts()))
        .bind(safe_error)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .unwrap_or(false);
        transaction.commit().await.map_err(storage_error)?;
        Ok(exhausted)
    }

    /// Clears the repair circuit after a successful or idempotent restore.
    pub async fn complete_repair(
        &self,
        lease: &JobLease,
        asset_id: Uuid,
    ) -> Result<(), AssetRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0))")
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        lock_live_repair_lease(&mut transaction, lease).await?;
        sqlx::query(
            "UPDATE asset_repair_states
             SET admitted_attempts = 0, next_allowed_at = NULL,
                 exhausted = FALSE, last_error = NULL, updated_at = clock_timestamp()
             WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(())
    }

    /// Resumes a bounded pass of durable pending generations without a request.
    pub async fn resume_pending_refreshes(&self) -> Result<(), AssetRepositoryError> {
        let mut jobs = PostgresJobTransaction::begin(&self.pool)
            .await
            .map_err(storage_error)?;
        let transaction = jobs.transaction_mut().map_err(job_transaction_error)?;
        lock_asset_capacity(transaction).await?;
        lock_asset_recovery_admission(transaction).await?;
        // Oldest-first with a per-scan touch keeps the bounded window rotating,
        // so a permanently cooling generation cannot starve later assets. An
        // asset with an active repair keeps the repair slot and is skipped.
        // The window is deliberately small: each candidate may stat a network
        // blob backend while this transaction holds the global capacity lock
        // that public asset reads also take.
        const PENDING_SCAN_LIMIT: i64 = 16;
        let assets = sqlx::query_scalar::<_, Uuid>(
            "SELECT s.asset_record_id FROM asset_record_refresh_state AS s
             WHERE s.active_chain_id IS NULL
               AND s.requested_generation > s.completed_generation
               AND NOT EXISTS (
                   SELECT 1 FROM asset_repair_jobs AS arj
                   JOIN jobs AS j ON j.id = arj.job_id
                   WHERE arj.asset_record_id = s.asset_record_id
                     AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')
               )
             ORDER BY s.updated_at, s.asset_record_id LIMIT $1",
        )
        .bind(PENDING_SCAN_LIMIT)
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?;
        for asset_id in assets {
            resume_refresh_after_cache_miss(
                &mut jobs,
                self.blob_store.as_ref(),
                asset_id,
                self.repair_policy,
                self.refresh_policy,
            )
            .await?;
            let transaction = jobs.transaction_mut().map_err(job_transaction_error)?;
            sqlx::query(
                "UPDATE asset_record_refresh_state SET updated_at = clock_timestamp()
                 WHERE asset_record_id = $1",
            )
            .bind(asset_id)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        }
        jobs.commit_inner().await.map_err(storage_error)
    }

    /// Evicts stale/old binary data and removes rows without article
    /// relationships. This operation never deletes a referenced URL record.
    pub async fn maintenance(&self) -> Result<AssetMaintenanceResult, AssetRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        // All writers acquire this decision lock before touching URL, blob, or
        // relationship rows. Taking it first keeps maintenance in the same
        // order and prevents a stale-byte update from deadlocking a writer.
        lock_asset_capacity(&mut transaction).await?;

        let stale_blob_ids = if self.policy.max_age().is_zero() {
            Vec::new()
        } else {
            let seconds = i64::try_from(self.policy.max_age().as_secs()).map_err(|_| {
                AssetRepositoryError::Storage("asset max age exceeds PostgreSQL range".to_owned())
            })?;
            evict_stale_blobs(self.blob_store.as_ref(), &mut transaction, seconds).await?
        };
        let stale_blobs = stale_blob_ids.len() as u64;

        if !stale_blob_ids.is_empty() {
            sqlx::query(
                "UPDATE asset_records
                 SET fetch_status = 'missing',
                     last_error = 'asset bytes evicted by age policy',
                     updated_at = clock_timestamp()
                 WHERE blob_id = ANY($1::uuid[])
                   AND fetch_status = 'available'",
            )
            .bind(&stale_blob_ids)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }

        let mut deferred_evictions = stale_blob_ids.clone();
        let size_evicted_blobs = enforce_cache_limit(
            self.blob_store.as_ref(),
            &mut transaction,
            CacheLimit {
                max_bytes: self.policy.max_cache_size_bytes(),
                additional_bytes: 0,
                protected_blob_ids: &[],
                pending_bytes: 0,
                pending_blob_ids: &[],
                deferred_evictions: &mut deferred_evictions,
            },
        )
        .await?;
        for blob_id in &deferred_evictions {
            sqlx::query(
                "INSERT INTO asset_blob_deletion_queue (blob_id)
                 VALUES ($1) ON CONFLICT (blob_id) DO NOTHING",
            )
            .bind(blob_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }

        let orphan_records = sqlx::query(
            "DELETE FROM asset_records AS r
             WHERE NOT EXISTS (
                 SELECT 1 FROM article_assets AS aa
                 WHERE aa.asset_record_id = r.id
             )
             AND NOT EXISTS (
                 SELECT 1
                 FROM asset_repair_jobs AS arj
                 JOIN jobs AS j ON j.id = arj.job_id
                 WHERE arj.asset_record_id = r.id
                   AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')
             )",
        )
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?
        .rows_affected();

        let orphan_blob_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT b.id
             FROM asset_blobs AS b
             WHERE NOT EXISTS (
                 SELECT 1 FROM asset_records AS r
                 WHERE r.blob_id = b.id
             )",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        // External objects must be deleted before their discovery metadata.
        // If deletion fails, returning here rolls back the transaction and
        // leaves the orphan row available for the next maintenance pass.
        for blob_id in &orphan_blob_ids {
            blob_delete_in_transaction(
                self.blob_store.as_ref(),
                &mut transaction,
                &AssetBlobKey::for_blob_id(*blob_id),
            )
            .await?;
        }
        let orphan_blobs = if orphan_blob_ids.is_empty() {
            0
        } else {
            sqlx::query("DELETE FROM asset_blobs WHERE id = ANY($1::uuid[])")
                .bind(&orphan_blob_ids)
                .execute(&mut *transaction)
                .await
                .map_err(storage_error)?
                .rows_affected()
        };

        transaction.commit().await.map_err(storage_error)?;
        drain_blob_deletion_queue(&self.pool, self.blob_store.as_ref()).await?;
        drain_blob_cleanup_queue(&self.pool, self.blob_store.as_ref()).await?;
        Ok(AssetMaintenanceResult {
            stale_blobs,
            size_evicted_blobs,
            orphan_records,
            orphan_blobs,
        })
    }
}

/// Attempts queued external deletions. Failed rows remain queued for the next
/// maintenance pass, while successful deletes remove their durable intent.
pub(crate) async fn drain_blob_deletion_queue(
    pool: &PgPool,
    blob_store: &dyn AssetBlobStore,
) -> Result<(), AssetRepositoryError> {
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    lock_asset_capacity(&mut transaction).await?;
    let rows =
        sqlx::query("SELECT blob_id FROM asset_blob_deletion_queue ORDER BY created_at, blob_id")
            .fetch_all(&mut *transaction)
            .await
            .map_err(storage_error)?;
    for row in rows {
        let blob_id = row.try_get::<Uuid, _>("blob_id").map_err(storage_error)?;
        blob_delete_in_transaction(
            blob_store,
            &mut transaction,
            &AssetBlobKey::for_blob_id(blob_id),
        )
        .await?;
        sqlx::query("DELETE FROM asset_blob_deletion_queue WHERE blob_id = $1")
            .bind(blob_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
    }
    transaction.commit().await.map_err(storage_error)?;
    Ok(())
}

pub(crate) async fn drain_blob_cleanup_queue(
    pool: &PgPool,
    blob_store: &dyn AssetBlobStore,
) -> Result<(), AssetRepositoryError> {
    let mut transaction = pool.begin().await.map_err(storage_error)?;
    lock_asset_capacity(&mut transaction).await?;
    let rows =
        sqlx::query("SELECT blob_id FROM asset_blob_cleanup_queue ORDER BY created_at, blob_id")
            .fetch_all(&mut *transaction)
            .await
            .map_err(storage_error)?;
    for row in rows {
        let blob_id = row.try_get::<Uuid, _>("blob_id").map_err(storage_error)?;
        blob_delete_in_transaction(
            blob_store,
            &mut transaction,
            &AssetBlobKey::for_blob_id(blob_id),
        )
        .await?;
        sqlx::query("DELETE FROM asset_blob_cleanup_queue WHERE blob_id = $1")
            .bind(blob_id)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
    }
    transaction.commit().await.map_err(storage_error)?;
    Ok(())
}

async fn enqueue_repair_in_transaction(
    job_transaction: &mut PostgresJobTransaction<'_>,
    blob_store: &dyn AssetBlobStore,
    policy: AssetRepairPolicy,
    refresh_policy: AssetRefreshPolicy,
    asset_id: Uuid,
) -> Result<AssetRepairEnqueueResult, AssetRepositoryError> {
    {
        let sql_transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        // Coordinate admission with maintenance's orphan deletion. Capacity is
        // the first lock in the asset write order; without it, maintenance can
        // snapshot this record as an orphan, wait for its row lock, and delete it
        // after the repair job commits.
        lock_asset_capacity(sql_transaction).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0))")
            .execute(&mut **sql_transaction)
            .await
            .map_err(storage_error)?;

        let target = sqlx::query(
            "SELECT r.blob_id
         FROM asset_records AS r
         WHERE r.id = $1
           AND EXISTS (
               SELECT 1 FROM article_assets AS aa
               WHERE aa.asset_record_id = r.id
           )
         FOR UPDATE OF r",
        )
        .bind(asset_id)
        .fetch_optional(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        let Some(target) = target else {
            if let Some(result) = resume_refresh_after_cache_miss(
                job_transaction,
                blob_store,
                asset_id,
                policy,
                refresh_policy,
            )
            .await?
            {
                return Ok(result);
            }
            return Ok(AssetRepairEnqueueResult::NotFound);
        };
        if let Some(blob_id) = target
            .try_get::<Option<Uuid>, _>("blob_id")
            .map_err(storage_error)?
        {
            if !blob_deletion_pending(sql_transaction, blob_id).await?
                && blob_stat_in_transaction(
                    blob_store,
                    sql_transaction,
                    &AssetBlobKey::for_blob_id(blob_id),
                )
                .await?
                .is_some_and(|metadata| metadata.is_present())
            {
                return Ok(AssetRepairEnqueueResult::AlreadyAvailable);
            }
        }

        let active = sqlx::query(
            "SELECT j.id
         FROM asset_repair_jobs AS arj
         JOIN jobs AS j ON j.id = arj.job_id
         WHERE arj.asset_record_id = $1
           AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')
         ORDER BY j.created_at ASC
         LIMIT 1",
        )
        .bind(asset_id)
        .fetch_optional(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        if let Some(active) = active {
            return Ok(AssetRepairEnqueueResult::AlreadyActive {
                job_id: active.try_get("id").map_err(storage_error)?,
            });
        }
    }

    if let Some(result) = resume_refresh_after_cache_miss(
        job_transaction,
        blob_store,
        asset_id,
        policy,
        refresh_policy,
    )
    .await?
    {
        return Ok(result);
    }

    {
        let sql_transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        // No active or pending generation is left, so a live article's refresh
        // lineage decides the miss: all exhausted is terminal, a cooling
        // article is retryable, and only an eligible one may admit a repair.
        let (live, exhausted, cooling) = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT count(*)::bigint,
                    count(*) FILTER (
                        WHERE COALESCE(l.exhausted, FALSE)
                           OR COALESCE(l.attempts, 0) >= $2
                    )::bigint,
                    count(*) FILTER (
                        WHERE NOT (COALESCE(l.exhausted, FALSE)
                                   OR COALESCE(l.attempts, 0) >= $2)
                          AND l.next_allowed_at > clock_timestamp()
                    )::bigint
             FROM article_assets AS aa
             JOIN articles AS a
               ON a.source_id = aa.source_id AND a.review_id = aa.review_id
             LEFT JOIN asset_article_refresh_lineage AS l
               ON l.source_id = aa.source_id AND l.review_id = aa.review_id
             WHERE aa.asset_record_id = $1",
        )
        .bind(asset_id)
        .bind(i64::from(refresh_policy.max_attempts()))
        .fetch_one(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        if live > 0 && exhausted == live {
            return Ok(AssetRepairEnqueueResult::Exhausted);
        }
        if live > 0 && exhausted + cooling == live {
            return Ok(AssetRepairEnqueueResult::Retryable);
        }

        let state = sqlx::query(
            "SELECT admitted_attempts, next_allowed_at, exhausted
         FROM asset_repair_states
         WHERE asset_record_id = $1
         FOR UPDATE",
        )
        .bind(asset_id)
        .fetch_optional(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        if let Some(state) = &state {
            let exhausted: bool = state.try_get("exhausted").map_err(storage_error)?;
            if exhausted {
                return Ok(AssetRepairEnqueueResult::Exhausted);
            }
            let next_allowed_at = state
                .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("next_allowed_at")
                .map_err(storage_error)?;
            let db_now: chrono::DateTime<chrono::Utc> =
                sqlx::query_scalar("SELECT clock_timestamp()")
                    .fetch_one(&mut **sql_transaction)
                    .await
                    .map_err(storage_error)?;
            if next_allowed_at.is_some_and(|next| next > db_now) {
                return Ok(AssetRepairEnqueueResult::Backoff);
            }
            let admitted_attempts: i64 =
                state.try_get("admitted_attempts").map_err(storage_error)?;
            if admitted_attempts >= i64::from(policy.max_attempts()) {
                return Ok(AssetRepairEnqueueResult::Exhausted);
            }
        }

        let global_pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint
         FROM asset_repair_jobs AS arj
         JOIN jobs AS j ON j.id = arj.job_id
         WHERE j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
        )
        .fetch_one(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        let public_pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint
         FROM asset_repair_jobs AS arj
         JOIN jobs AS j ON j.id = arj.job_id
         WHERE arj.admission_class = 'public_repair'
           AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
        )
        .fetch_one(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        if global_pending >= i64::from(policy.max_pending())
            || public_pending >= i64::from(policy.public_max_pending())
        {
            return Ok(AssetRepairEnqueueResult::CapacityFull);
        }
    }

    let now = Utc::now();
    let job_id = match job_transaction
        .enqueue_internal(
            NewJob {
                job_type: JobType::AssetRepair,
                source_id: None,
                priority: 0,
                run_after: now,
                max_attempts: REPAIR_JOB_MAX_ATTEMPTS,
                payload: json!({ "asset_id": asset_id }),
                dedupe_key: format!("asset-repair:{asset_id}"),
                now,
            },
            true,
        )
        .await
        .map_err(job_transaction_error)?
    {
        crate::persistence::repositories::job_repository::EnqueueResult::Inserted(job) => job.id(),
        crate::persistence::repositories::job_repository::EnqueueResult::AlreadyActive {
            job_id,
        } => {
            return Ok(AssetRepairEnqueueResult::AlreadyActive { job_id });
        }
    };

    let sql_transaction = job_transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    sqlx::query(
        "INSERT INTO asset_repair_states
                (asset_record_id, admitted_attempts, next_allowed_at, exhausted, last_error)
         VALUES ($1, 1, NULL, FALSE, NULL)
         ON CONFLICT (asset_record_id) DO UPDATE
         SET admitted_attempts = asset_repair_states.admitted_attempts + 1,
             next_allowed_at = NULL,
             exhausted = FALSE,
             last_error = NULL,
             updated_at = clock_timestamp()",
    )
    .bind(asset_id)
    .execute(&mut **sql_transaction)
    .await
    .map_err(storage_error)?;
    sqlx::query(
        "INSERT INTO asset_repair_jobs (job_id, asset_record_id, admission_class)
         VALUES ($1, $2, 'public_repair')",
    )
    .bind(job_id)
    .bind(asset_id)
    .execute(&mut **sql_transaction)
    .await
    .map_err(storage_error)?;
    Ok(AssetRepairEnqueueResult::Enqueued { job_id })
}

/// Raw-byte deduplication results collected before source/article persistence
/// locks are acquired.
#[derive(Debug, Clone, Default)]
pub(crate) struct AssetBatchPreparation {
    matching_blobs: HashMap<Uuid, Option<Uuid>>,
    matching_inputs: HashMap<Uuid, Uuid>,
}

impl AssetBatchPreparation {
    fn matching_blob(&self, input: &AssetInput) -> Result<Option<Uuid>, AssetRepositoryError> {
        self.matching_blobs
            .get(&input.preparation_id())
            .copied()
            .ok_or(AssetRepositoryError::PreparationRequired)
    }

    fn matching_input(&self, input: &AssetInput) -> Option<Uuid> {
        self.matching_inputs.get(&input.preparation_id()).copied()
    }
}

/// Acquires the aggregate-capacity lock, then checksum locks, and compares
/// candidate raw bytes before the caller begins source/article row locking.
/// The returned IDs are only hints; the write path verifies that the selected
/// immutable blob still has data.
pub(crate) async fn prepare_asset_batch(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    inputs: &[AssetInput],
) -> Result<AssetBatchPreparation, AssetRepositoryError> {
    // `store_for_article` acquires the same lock before taking URL locks. Do
    // the preflight in that order too, otherwise a source-sync transaction
    // can hold a checksum lock while waiting for capacity and deadlock with
    // repair or maintenance holding capacity while waiting for the checksum.
    if !inputs.is_empty() {
        lock_asset_capacity(transaction).await?;
    }

    let checksums = inputs
        .iter()
        .map(|input| input.checksum().to_owned())
        .collect::<BTreeSet<_>>();
    for checksum in checksums {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-checksum:' || $1, 0))")
            .bind(checksum)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
    }

    let mut matches = inputs
        .iter()
        .map(|input| (input.preparation_id(), None))
        .collect::<HashMap<_, _>>();
    let mut matching_inputs = HashMap::new();
    for (index, input) in inputs.iter().enumerate() {
        if let Some(previous) = inputs[..index].iter().find(|previous| {
            previous.checksum() == input.checksum()
                && input_byte_size(previous).ok() == input_byte_size(input).ok()
                && previous.bytes == input.bytes
        }) {
            matching_inputs.insert(input.preparation_id(), previous.preparation_id());
        }
    }
    let keys = inputs
        .iter()
        .map(|input| {
            Ok::<_, AssetRepositoryError>((input.checksum().to_owned(), input_byte_size(input)?))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;

    for (checksum, byte_size) in keys {
        let candidates = sqlx::query(
            "SELECT id
             FROM asset_blobs
             WHERE checksum = $1
               AND byte_size = $2
               AND NOT EXISTS (
                   SELECT 1 FROM asset_blob_deletion_queue AS q
                   WHERE q.blob_id = asset_blobs.id
               )
             ORDER BY id",
        )
        .bind(&checksum)
        .bind(byte_size)
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?;
        let mut candidate_blobs = Vec::with_capacity(candidates.len());
        for row in candidates {
            let blob_id = row.try_get::<Uuid, _>("id").map_err(storage_error)?;
            if let Some(blob) = blob_read_in_transaction(
                blob_store,
                transaction,
                &AssetBlobKey::for_blob_id(blob_id),
            )
            .await?
            {
                candidate_blobs.push((blob_id, blob));
            }
        }

        for input in inputs.iter().filter(|input| {
            input.checksum() == checksum
                && input_byte_size(input).is_ok_and(|size| size == byte_size)
        }) {
            let matching_blob = candidate_blobs
                .iter()
                .find_map(|(blob_id, blob)| (blob.bytes() == input.bytes).then_some(*blob_id));
            matches.insert(input.preparation_id(), matching_blob);
        }
    }

    Ok(AssetBatchPreparation {
        matching_blobs: matches,
        matching_inputs,
    })
}

/// Transaction-scoped asset metadata view owned by `UnitOfWork`.
///
/// PostgreSQL owns the metadata and relationship transaction. Binary reads and
/// writes are delegated to the injected [`AssetBlobStore`], whose operations
/// may have an independent commit boundary.
pub struct PostgresAssetTransaction<'borrow, 'pool> {
    job_transaction: &'borrow mut PostgresJobTransaction<'pool>,
    blob_store: Arc<dyn AssetBlobStore>,
    preparation: Option<AssetBatchPreparation>,
    repair_policy: AssetRepairPolicy,
    policy: AssetCachePolicy,
    mutation_state: Arc<Mutex<AssetMutationState>>,
}

/// Binary changes that must follow the enclosing unit-of-work outcome.
#[derive(Debug, Default)]
pub(crate) struct AssetMutationState {
    pub(crate) newly_written: Vec<Uuid>,
    pub(crate) deferred_evictions: Vec<Uuid>,
    pub(crate) pending_bytes: u64,
}

fn take_newly_written_from(state: &Arc<Mutex<AssetMutationState>>, start: usize) -> Vec<Uuid> {
    let mut state = state.lock().unwrap();
    let split_at = start.min(state.newly_written.len());
    state.newly_written.split_off(split_at)
}

async fn blob_deletion_pending(
    transaction: &mut Transaction<'_, Postgres>,
    blob_id: Uuid,
) -> Result<bool, AssetRepositoryError> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM asset_blob_deletion_queue WHERE blob_id = $1
         )",
    )
    .bind(blob_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)
}

impl<'borrow, 'pool> PostgresAssetTransaction<'borrow, 'pool> {
    /// Creates an asset view over a unit-of-work transaction.
    pub(crate) fn new(
        job_transaction: &'borrow mut PostgresJobTransaction<'pool>,
        blob_store: Arc<dyn AssetBlobStore>,
        preparation: Option<AssetBatchPreparation>,
        repair_policy: AssetRepairPolicy,
        policy: AssetCachePolicy,
        mutation_state: Arc<Mutex<AssetMutationState>>,
    ) -> Self {
        Self {
            job_transaction,
            blob_store,
            preparation,
            repair_policy,
            policy,
            mutation_state,
        }
    }

    fn transaction(&mut self) -> Result<&mut Transaction<'pool, Postgres>, AssetRepositoryError> {
        self.job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)
    }
}

#[async_trait::async_trait]
impl AssetTransactionRepository for PostgresAssetTransaction<'_, '_> {
    async fn store_for_article(
        &mut self,
        source_id: crate::domain::source::SourceId,
        review_id: &str,
        inputs: &[AssetInput],
    ) -> Result<Vec<StoredAsset>, AssetRepositoryError> {
        validate_article_identity(source_id, review_id)?;
        validate_input_count(self.policy, inputs)?;
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        // A batch may pass validation for its first items and still fail when
        // a later body cannot fit the aggregate cache cap. Keep all asset
        // rows and relationship changes behind a savepoint so callers can
        // intentionally keep the article external without committing a
        // partially archived batch.
        let policy = self.policy;
        let preparation = self.preparation.clone();
        let blob_store = Arc::clone(&self.blob_store);
        let mutation_state = Arc::clone(&self.mutation_state);
        let newly_written_start = mutation_state.lock().unwrap().newly_written.len();
        let deferred_start = mutation_state.lock().unwrap().deferred_evictions.len();
        let pending_start = mutation_state.lock().unwrap().pending_bytes;
        let transaction = self.transaction()?;
        // A batch can contain several URLs. Acquire the aggregate-capacity
        // lock before the first per-URL lock so two batches cannot hold
        // different URL locks while waiting for each other's capacity turn.
        lock_asset_capacity(transaction).await?;
        let mut savepoint = transaction.begin().await.map_err(storage_error)?;
        let mut context = AssetWriteContext::new(
            blob_store.as_ref(),
            policy,
            preparation.as_ref(),
            &mutation_state,
        );
        let result = store_inputs(&mut savepoint, source_id, review_id, inputs, &mut context).await;
        let stored = match result {
            Ok(stored) => stored,
            Err(error) => {
                let rollback = savepoint.rollback().await;
                mutation_state
                    .lock()
                    .unwrap()
                    .deferred_evictions
                    .truncate(deferred_start);
                mutation_state.lock().unwrap().pending_bytes = pending_start;
                let newly_written = take_newly_written_from(&mutation_state, newly_written_start);
                cleanup_new_blobs(blob_store.as_ref(), transaction, &newly_written).await;
                return match rollback {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(AssetRepositoryError::Storage(format!(
                        "{error}; asset batch rollback failed: {rollback_error}"
                    ))),
                };
            }
        };
        if let Err(error) = savepoint.commit().await.map_err(storage_error) {
            mutation_state
                .lock()
                .unwrap()
                .deferred_evictions
                .truncate(deferred_start);
            mutation_state.lock().unwrap().pending_bytes = pending_start;
            let newly_written = take_newly_written_from(&mutation_state, newly_written_start);
            cleanup_new_blobs(blob_store.as_ref(), transaction, &newly_written).await;
            return Err(error);
        }
        Ok(stored)
    }

    async fn clear_for_article(
        &mut self,
        source_id: crate::domain::source::SourceId,
        review_id: &str,
    ) -> Result<(), AssetRepositoryError> {
        validate_article_identity(source_id, review_id)?;
        let transaction = self.transaction()?;
        sqlx::query(
            "DELETE FROM article_assets
             WHERE source_id = $1 AND review_id = $2",
        )
        .bind(source_id.as_uuid())
        .bind(review_id.trim())
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
        Ok(())
    }

    async fn replace_for_article(
        &mut self,
        source_id: crate::domain::source::SourceId,
        review_id: &str,
        inputs: &[AssetInput],
    ) -> Result<Vec<StoredAsset>, AssetRepositoryError> {
        validate_article_identity(source_id, review_id)?;
        validate_input_count(self.policy, inputs)?;

        // Keep deletion and insertion in one savepoint. A cache miss or
        // capacity failure is intentionally best-effort for the article
        // sync, so it must not turn a previously archived relationship into
        // an unreferenced asset merely because the replacement failed.
        let policy = self.policy;
        let preparation = self.preparation.clone();
        let blob_store = Arc::clone(&self.blob_store);
        let mutation_state = Arc::clone(&self.mutation_state);
        let newly_written_start = mutation_state.lock().unwrap().newly_written.len();
        let deferred_start = mutation_state.lock().unwrap().deferred_evictions.len();
        let pending_start = mutation_state.lock().unwrap().pending_bytes;
        let transaction = self.transaction()?;
        // Non-empty replacements retain the capacity-first lock order used by
        // maintenance. An empty replacement only changes article relationships;
        // do not acquire capacity after source/article row locks because
        // begin_with_assets(&[]) intentionally does not reserve that lock.
        if !inputs.is_empty() {
            lock_asset_capacity(transaction).await?;
        }
        let mut savepoint = transaction.begin().await.map_err(storage_error)?;
        let result = async {
            sqlx::query(
                "DELETE FROM article_assets
                 WHERE source_id = $1 AND review_id = $2",
            )
            .bind(source_id.as_uuid())
            .bind(review_id.trim())
            .execute(&mut *savepoint)
            .await
            .map_err(storage_error)?;

            let mut context = AssetWriteContext::new(
                blob_store.as_ref(),
                policy,
                preparation.as_ref(),
                &mutation_state,
            );
            store_inputs(&mut savepoint, source_id, review_id, inputs, &mut context).await
        }
        .await;
        match result {
            Ok(stored) => {
                if let Err(error) = savepoint.commit().await.map_err(storage_error) {
                    mutation_state
                        .lock()
                        .unwrap()
                        .deferred_evictions
                        .truncate(deferred_start);
                    mutation_state.lock().unwrap().pending_bytes = pending_start;
                    let newly_written =
                        take_newly_written_from(&mutation_state, newly_written_start);
                    cleanup_new_blobs(blob_store.as_ref(), transaction, &newly_written).await;
                    return Err(error);
                }
                Ok(stored)
            }
            Err(error) => {
                let rollback = savepoint.rollback().await;
                mutation_state
                    .lock()
                    .unwrap()
                    .deferred_evictions
                    .truncate(deferred_start);
                mutation_state.lock().unwrap().pending_bytes = pending_start;
                let newly_written = take_newly_written_from(&mutation_state, newly_written_start);
                cleanup_new_blobs(blob_store.as_ref(), transaction, &newly_written).await;
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(AssetRepositoryError::Storage(format!(
                        "{error}; asset replacement rollback failed: {rollback_error}"
                    ))),
                }
            }
        }
    }

    async fn begin_article_refresh(
        &mut self,
        lease: &JobLease,
    ) -> Result<Option<ArticleRefreshTarget>, AssetRepositoryError> {
        let transaction = self.transaction()?;
        lock_asset_capacity(transaction).await?;
        lock_asset_recovery_admission(transaction).await?;
        lock_live_repair_lease(transaction, lease).await?;
        if lease.job.job_type() != JobType::AssetRefresh {
            return Err(AssetRepositoryError::InvalidMetadata);
        }
        let Some(target) = lock_current_refresh_target(transaction, lease.job.id()).await? else {
            return Ok(None);
        };
        let payload = lease.job.payload();
        let payload_matches = payload["source_id"].as_str()
            == Some(target.source_id.as_uuid().to_string().as_str())
            && payload["review_id"].as_str() == Some(target.review_id.as_str())
            && payload["expired_asset_record_id"].as_str()
                == Some(target.expired_asset_record_id.to_string().as_str())
            && payload["chain_id"].as_str() == Some(target.chain_id.to_string().as_str())
            && payload["generation"].as_i64() == Some(target.served_generation)
            && payload["ordinal"].as_i64() == Some(target.chain_ordinal);
        // The entry this job owns must still be the in-flight snapshot identity
        // the payload names; anything else is a superseded association.
        let entry_is_current = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1 FROM asset_refresh_generation_entries
                 WHERE asset_record_id = $1 AND generation = $2 AND ordinal = $3
                   AND source_id = $4 AND review_id = $5 AND state = 'in_flight'
             )",
        )
        .bind(target.expired_asset_record_id)
        .bind(target.served_generation)
        .bind(target.chain_ordinal)
        .bind(target.source_id.as_uuid())
        .bind(&target.review_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if !payload_matches
            || !entry_is_current
            || target.chain_ordinal <= 0
            || target.served_generation <= 0
        {
            return Err(AssetRepositoryError::InvalidMetadata);
        }
        Ok(Some(target))
    }

    async fn finish_article_refresh(
        &mut self,
        lease: &JobLease,
        outcome: ArticleRefreshOutcome,
        policy: AssetRefreshPolicy,
    ) -> Result<RefreshFinishResult, AssetRepositoryError> {
        let Some(target) = self.begin_article_refresh(lease).await? else {
            return Err(AssetRepositoryError::InvalidMetadata);
        };
        finish_article_refresh_in_transaction(
            self.job_transaction,
            lease,
            &target,
            self.blob_store.as_ref(),
            outcome,
            policy,
        )
        .await
    }

    async fn complete_unresolvable_article_refresh(
        &mut self,
        lease: &JobLease,
    ) -> Result<(), AssetRepositoryError> {
        if lease.job.job_type() != JobType::AssetRefresh {
            return Err(AssetRepositoryError::InvalidMetadata);
        }
        let transaction = self.transaction()?;
        lock_asset_capacity(transaction).await?;
        lock_asset_recovery_admission(transaction).await?;
        lock_live_repair_lease(transaction, lease).await?;
        // Only persisted metadata can identify the association to terminalize;
        // a malformed payload never guesses. Every update stays guarded on this
        // chain's served generation so a newer unrelated chain is untouched.
        // Pending work keeps its slot for the periodic selector.
        if let Some(target) = lock_current_refresh_target(transaction, lease.job.id()).await? {
            sqlx::query(
                "UPDATE asset_refresh_generation_entries
                 SET state = 'done', not_before = NULL
                 WHERE asset_record_id = $1 AND generation = $2 AND ordinal = $3
                   AND state = 'in_flight'",
            )
            .bind(target.expired_asset_record_id)
            .bind(target.served_generation)
            .bind(target.chain_ordinal)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            complete_refresh_generation_if_terminal(
                transaction,
                target.expired_asset_record_id,
                target.served_generation,
            )
            .await?;
            sqlx::query(
                "UPDATE asset_record_refresh_state
                 SET active_chain_id = NULL, updated_at = clock_timestamp()
                 WHERE asset_record_id = $1 AND active_chain_id = $2
                   AND served_generation = $3",
            )
            .bind(target.expired_asset_record_id)
            .bind(target.chain_id)
            .bind(target.served_generation)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        }
        let owner = lease
            .job
            .lease_owner()
            .ok_or(AssetRepositoryError::LeaseLost)?;
        JobRepositoryTransaction::succeed(
            self.job_transaction,
            lease.job.id(),
            owner,
            lease.token,
            Utc::now(),
        )
        .await
        .map_err(job_transaction_error)?;
        Ok(())
    }

    async fn reset_article_refresh_lineage(
        &mut self,
        source_id: crate::domain::source::SourceId,
        review_id: &str,
    ) -> Result<(), AssetRepositoryError> {
        validate_article_identity(source_id, review_id)?;
        let affected = {
            let transaction = self.transaction()?;
            sqlx::query(
                "INSERT INTO asset_article_refresh_lineage
                     (source_id, review_id, attempts, next_allowed_at, exhausted,
                      last_outcome, updated_at)
                 SELECT source_id, review_id, 0, NULL, FALSE, 'succeeded', clock_timestamp()
                 FROM articles WHERE source_id = $1 AND review_id = $2
                 ON CONFLICT (source_id, review_id) DO UPDATE
                 SET attempts = 0, next_allowed_at = NULL, exhausted = FALSE,
                     last_outcome = 'succeeded', updated_at = clock_timestamp()",
            )
            .bind(source_id.as_uuid())
            .bind(review_id.trim())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            reconcile_article_refresh_entries(transaction, source_id.as_uuid(), review_id.trim())
                .await?
        };
        // Reevaluate the affected generations in this same unit of work: complete
        // terminal ones, then admit pending work while global capacity allows.
        let blob_store = Arc::clone(&self.blob_store);
        let refresh_policy = self.job_transaction.refresh_policy();
        let repair_policy = self.repair_policy;
        for (asset_id, generation) in affected {
            let selection = {
                let transaction = self.transaction()?;
                if !refresh_capacity_available(transaction, repair_policy).await?
                    || !refresh_generation_pending(transaction, asset_id, generation).await?
                {
                    None
                } else {
                    select_refresh_entry(
                        transaction,
                        blob_store.as_ref(),
                        asset_id,
                        generation,
                        None,
                        refresh_policy,
                    )
                    .await?
                }
            };
            let Some(entry) = selection else {
                let transaction = self.transaction()?;
                complete_refresh_generation_if_terminal(transaction, asset_id, generation).await?;
                continue;
            };
            admit_refresh_entry(
                self.job_transaction,
                asset_id,
                generation,
                Uuid::new_v4(),
                entry,
                refresh_policy,
                true,
            )
            .await?;
        }
        Ok(())
    }
}

async fn reconcile_article_refresh_entries(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    review_id: &str,
) -> Result<Vec<(Uuid, i64)>, AssetRepositoryError> {
    // Only the generations this article supersedes are reevaluated; unrelated
    // per-asset high-water rows stay untouched.
    let mut affected = sqlx::query_as::<_, (Uuid, i64)>(
        "UPDATE asset_refresh_generation_entries
         SET state = 'done', not_before = NULL
         WHERE source_id = $1 AND review_id = $2 AND state IN ('unseen', 'cooling')
         RETURNING asset_record_id, generation",
    )
    .bind(source_id)
    .bind(review_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(storage_error)?;
    affected.sort_unstable();
    affected.dedup();
    for (asset_id, generation) in &affected {
        complete_refresh_generation_if_terminal(transaction, *asset_id, *generation).await?;
    }
    Ok(affected)
}

/// Reports whether one more recovery job fits the cluster-wide bound.
async fn refresh_capacity_available(
    transaction: &mut Transaction<'_, Postgres>,
    policy: AssetRepairPolicy,
) -> Result<bool, AssetRepositoryError> {
    let active = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM asset_repair_jobs AS arj
         JOIN jobs AS j ON j.id = arj.job_id
         WHERE j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(active < i64::from(policy.max_pending()))
}

/// Reports whether a generation still has work the selector may admit.
async fn refresh_generation_pending(
    transaction: &mut Transaction<'_, Postgres>,
    asset_id: Uuid,
    generation: i64,
) -> Result<bool, AssetRepositoryError> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1 FROM asset_record_refresh_state
             WHERE asset_record_id = $1 AND served_generation = $2
               AND active_chain_id IS NULL
               AND requested_generation > completed_generation
         )",
    )
    .bind(asset_id)
    .bind(generation)
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)
}

/// Reads the persisted refresh association for one fenced job.
///
/// `for_update` locks the job's recovery-metadata row and the per-asset
/// generation-state row. Callers lock those only after the source and article
/// rows so finalization keeps the documented lock order.
async fn read_refresh_target(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    for_update: bool,
) -> Result<Option<ArticleRefreshTarget>, AssetRepositoryError> {
    let lock = if for_update {
        " FOR UPDATE OF arj, ars"
    } else {
        ""
    };
    let row = sqlx::query(&format!(
        "SELECT arj.refresh_source_id, arj.refresh_review_id,
                arj.expired_asset_record_id, arj.chain_id, arj.chain_ordinal,
                ars.served_generation
         FROM asset_repair_jobs AS arj
         JOIN asset_record_refresh_state AS ars
           ON ars.asset_record_id = arj.expired_asset_record_id
         WHERE arj.job_id = $1 AND arj.admission_class = 'internal_refresh'
           AND arj.refresh_source_id IS NOT NULL
           AND arj.refresh_review_id IS NOT NULL
           AND arj.expired_asset_record_id IS NOT NULL
           AND arj.chain_id IS NOT NULL AND arj.chain_ordinal IS NOT NULL
           AND ars.active_chain_id = arj.chain_id{lock}",
    ))
    .bind(job_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(ArticleRefreshTarget {
        source_id: crate::domain::source::SourceId::from_uuid(
            row.try_get("refresh_source_id").map_err(storage_error)?,
        ),
        review_id: row.try_get("refresh_review_id").map_err(storage_error)?,
        expired_asset_record_id: row
            .try_get("expired_asset_record_id")
            .map_err(storage_error)?,
        chain_id: row.try_get("chain_id").map_err(storage_error)?,
        chain_ordinal: row.try_get("chain_ordinal").map_err(storage_error)?,
        served_generation: row.try_get("served_generation").map_err(storage_error)?,
    }))
}

/// Locks the source then article rows that a refresh mutation will touch.
async fn lock_refresh_article_rows(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: crate::domain::source::SourceId,
    review_id: &str,
) -> Result<(), AssetRepositoryError> {
    sqlx::query("SELECT id FROM sources WHERE id = $1 FOR UPDATE")
        .bind(source_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
    sqlx::query(
        "SELECT review_id FROM articles WHERE source_id = $1 AND review_id = $2 FOR UPDATE",
    )
    .bind(source_id.as_uuid())
    .bind(review_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

/// Locks the current refresh association in the finalization lock order.
///
/// The unlocked read only learns which source/article rows to lock first; the
/// association is re-read under its metadata locks and must be unchanged, so a
/// concurrent terminalization never yields a stale target.
async fn lock_current_refresh_target(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<Option<ArticleRefreshTarget>, AssetRepositoryError> {
    let Some(peeked) = read_refresh_target(transaction, job_id, false).await? else {
        return Ok(None);
    };
    lock_refresh_article_rows(transaction, peeked.source_id, &peeked.review_id).await?;
    let Some(locked) = read_refresh_target(transaction, job_id, true).await? else {
        return Ok(None);
    };
    Ok((locked == peeked).then_some(locked))
}

fn validate_input_count(
    policy: AssetCachePolicy,
    inputs: &[AssetInput],
) -> Result<(), AssetRepositoryError> {
    if inputs.len() > usize::try_from(policy.max_count_per_article()).unwrap_or(usize::MAX) {
        return Err(AssetRepositoryError::TooManyAssets {
            max_count: policy.max_count_per_article(),
        });
    }
    Ok(())
}

async fn store_inputs(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: crate::domain::source::SourceId,
    review_id: &str,
    inputs: &[AssetInput],
    context: &mut AssetWriteContext<'_>,
) -> Result<Vec<StoredAsset>, AssetRepositoryError> {
    context
        .preparation
        .ok_or(AssetRepositoryError::PreparationRequired)?;
    let mut stored = Vec::with_capacity(inputs.len());
    // Keep bodies already returned by this batch out of LRU eviction. If a
    // later input cannot fit without evicting one of these bodies, reject the
    // whole savepoint instead of returning a URL that is immediately missing.
    let mut protected_blob_ids = Vec::with_capacity(inputs.len());
    let mut batch_blobs = HashMap::with_capacity(inputs.len());
    for input in inputs {
        validate_input(input, context.policy)?;
        let asset = store_one(
            transaction,
            context,
            input,
            &protected_blob_ids,
            &batch_blobs,
        )
        .await?;
        attach(transaction, source_id, review_id, input, asset.id()).await?;
        if !protected_blob_ids.contains(&asset.blob_id()) {
            protected_blob_ids.push(asset.blob_id());
        }
        batch_blobs.insert(input.preparation_id(), asset.blob_id());
        stored.push(asset);
    }
    Ok(stored)
}

struct AssetWriteContext<'a> {
    blob_store: &'a dyn AssetBlobStore,
    policy: AssetCachePolicy,
    preparation: Option<&'a AssetBatchPreparation>,
    mutation_state: &'a Arc<Mutex<AssetMutationState>>,
}

impl<'a> AssetWriteContext<'a> {
    fn new(
        blob_store: &'a dyn AssetBlobStore,
        policy: AssetCachePolicy,
        preparation: Option<&'a AssetBatchPreparation>,
        mutation_state: &'a Arc<Mutex<AssetMutationState>>,
    ) -> Self {
        Self {
            blob_store,
            policy,
            preparation,
            mutation_state,
        }
    }
}

async fn store_one(
    transaction: &mut Transaction<'_, Postgres>,
    context: &mut AssetWriteContext<'_>,
    input: &AssetInput,
    protected_blob_ids: &[Uuid],
    batch_blobs: &HashMap<Uuid, Uuid>,
) -> Result<StoredAsset, AssetRepositoryError> {
    let preparation = context
        .preparation
        .ok_or(AssetRepositoryError::PreparationRequired)?;
    let source_url = normalize_url(input.source_url.clone())?;
    let final_url = normalize_url(input.final_url.clone())?;
    let checksum = input.checksum().to_owned();
    let byte_size = input_byte_size(input)?;

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-url:' || $1, 0))")
        .bind(source_url.as_str())
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;

    let current = sqlx::query(
        "SELECT r.id AS record_id,
                    r.version AS record_version,
                    r.blob_id AS record_blob_id,
                    b.checksum AS blob_checksum,
                    b.byte_size AS blob_byte_size,
                    b.media_type AS blob_media_type
             FROM asset_records AS r
             LEFT JOIN asset_blobs AS b ON b.id = r.blob_id
             WHERE r.source_url = $1
             ORDER BY r.version DESC
             LIMIT 1
             FOR UPDATE OF r",
    )
    .bind(source_url.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;

    if let Some(row) = &current {
        let current_checksum = row
            .try_get::<Option<String>, _>("blob_checksum")
            .map_err(storage_error)?;
        let current_size = row
            .try_get::<Option<i64>, _>("blob_byte_size")
            .map_err(storage_error)?;
        let prepared_blob = preparation.matching_blob(input)?;
        if prepared_blob
            == row
                .try_get::<Option<Uuid>, _>("record_blob_id")
                .map_err(storage_error)?
            && current_checksum.as_deref() == Some(checksum.as_str())
            && current_size == Some(byte_size)
        {
            let record_id = row.try_get("record_id").map_err(storage_error)?;
            let blob_id = row
                .try_get::<Option<Uuid>, _>("record_blob_id")
                .map_err(storage_error)?
                .ok_or(AssetRepositoryError::InvalidMetadata)?;

            // Protect a reused blob from a concurrent capacity decision. The
            // initial row snapshot may be stale: another transaction can
            // evict the blob between this SELECT and the relationship insert.
            // Lock the capacity decision and refresh/lock the blob before
            // declaring the fast path successful.
            lock_asset_capacity(transaction).await?;
            let refreshed_blob = sqlx::query(
                "SELECT id
                 FROM asset_blobs
                 WHERE id = $1
                   AND checksum = $2
                   AND byte_size = $3
                   AND NOT EXISTS (
                       SELECT 1 FROM asset_blob_deletion_queue AS q
                       WHERE q.blob_id = asset_blobs.id
                   )
                 FOR UPDATE",
            )
            .bind(blob_id)
            .bind(&checksum)
            .bind(byte_size)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
            if refreshed_blob.is_some() {
                if let Some(metadata) = blob_stat_in_transaction(
                    context.blob_store,
                    transaction,
                    &AssetBlobKey::for_blob_id(blob_id),
                )
                .await?
                .filter(|metadata| {
                    metadata.is_present()
                        && metadata.checksum() == checksum
                        && metadata.byte_size() == byte_size as u64
                }) {
                    let media_type = metadata.media_type().to_owned();
                    sqlx::query(
                        "UPDATE asset_records
                         SET final_url = $2,
                             fetch_status = 'available',
                             last_error = NULL,
                             updated_at = clock_timestamp()
                         WHERE id = $1",
                    )
                    .bind(record_id)
                    .bind(final_url.as_str())
                    .execute(&mut **transaction)
                    .await
                    .map_err(storage_error)?;
                    sqlx::query(
                        "UPDATE asset_blobs
                         SET last_fetched_at = clock_timestamp(),
                             last_accessed_at = clock_timestamp()
                         WHERE id = $1",
                    )
                    .bind(blob_id)
                    .execute(&mut **transaction)
                    .await
                    .map_err(storage_error)?;
                    return Ok(StoredAsset::new(
                        record_id,
                        source_url,
                        blob_id,
                        checksum,
                        media_type,
                        u64::try_from(byte_size).expect("non-negative asset size"),
                    ));
                }
            }
        }
    }

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-checksum:' || $1, 0))")
        .bind(&checksum)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    let batch_match = preparation
        .matching_input(input)
        .and_then(|previous| batch_blobs.get(&previous).copied());
    let prepared_match = preparation.matching_blob(input)?;
    let matching_blob = batch_match.or(prepared_match);

    let (blob_id, media_type) = if let Some(blob_id) = matching_blob {
        let row = sqlx::query(
            "SELECT id
             FROM asset_blobs
             WHERE id = $1
               AND checksum = $2
               AND byte_size = $3
               AND NOT EXISTS (
                   SELECT 1 FROM asset_blob_deletion_queue AS q
                   WHERE q.blob_id = asset_blobs.id
               )
             FOR UPDATE",
        )
        .bind(blob_id)
        .bind(&checksum)
        .bind(byte_size)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
        let metadata = if row.is_some() {
            blob_stat_in_transaction(
                context.blob_store,
                transaction,
                &AssetBlobKey::for_blob_id(blob_id),
            )
            .await?
            .filter(|metadata| {
                metadata.is_present()
                    && metadata.checksum() == checksum
                    && metadata.byte_size() == byte_size as u64
            })
        } else {
            None
        };
        if let Some(metadata) = metadata {
            sqlx::query(
                "UPDATE asset_blobs
                 SET last_fetched_at = clock_timestamp(),
                     last_accessed_at = clock_timestamp()
                 WHERE id = $1",
            )
            .bind(blob_id)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            (blob_id, metadata.media_type().to_owned())
        } else {
            insert_blob(
                transaction,
                context,
                input,
                &checksum,
                byte_size,
                protected_blob_ids,
            )
            .await?
        }
    } else {
        insert_blob(
            transaction,
            context,
            input,
            &checksum,
            byte_size,
            protected_blob_ids,
        )
        .await?
    };

    let next_version = current
        .as_ref()
        .map(|row| row.try_get::<i64, _>("record_version"))
        .transpose()
        .map_err(storage_error)?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| AssetRepositoryError::Storage("asset version overflow".to_owned()))?;
    let record_id: Uuid = sqlx::query_scalar(
        "INSERT INTO asset_records
                (id, source_url, version, final_url, blob_id, fetch_status)
             VALUES ($1, $2, $3, $4, $5, 'available')
             RETURNING id",
    )
    .bind(Uuid::new_v4())
    .bind(source_url.as_str())
    .bind(next_version)
    .bind(final_url.as_str())
    .bind(blob_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)?;

    Ok(StoredAsset::new(
        record_id,
        source_url,
        blob_id,
        checksum,
        media_type,
        u64::try_from(byte_size).expect("non-negative asset size"),
    ))
}

async fn insert_blob(
    transaction: &mut Transaction<'_, Postgres>,
    context: &mut AssetWriteContext<'_>,
    input: &AssetInput,
    checksum: &str,
    byte_size: i64,
    protected_blob_ids: &[Uuid],
) -> Result<(Uuid, String), AssetRepositoryError> {
    let mut deferred_evictions = {
        context
            .mutation_state
            .lock()
            .unwrap()
            .deferred_evictions
            .clone()
    };
    let (pending_blob_ids, pending_bytes) = {
        let state = context.mutation_state.lock().unwrap();
        (state.newly_written.clone(), state.pending_bytes)
    };
    enforce_cache_limit(
        context.blob_store,
        transaction,
        CacheLimit {
            max_bytes: context.policy.max_cache_size_bytes(),
            additional_bytes: u64::try_from(byte_size).expect("non-negative asset size"),
            protected_blob_ids,
            pending_bytes,
            pending_blob_ids: &pending_blob_ids,
            deferred_evictions: &mut deferred_evictions,
        },
    )
    .await?;
    context.mutation_state.lock().unwrap().deferred_evictions = deferred_evictions;
    let blob_id = Uuid::new_v4();
    let blob = AssetBlob::new(
        input.bytes.clone(),
        input.media_type.clone(),
        checksum.to_owned(),
    )?;
    let externally_owned = !context.blob_store.as_any().is::<PostgresAssetBlobStore>();
    // Record the key before an external write. A backend may accept the
    // write and lose its response, so returning an error does not prove that
    // the key is absent. Keeping it in the mutation state lets the enclosing
    // savepoint or unit of work retry cleanup for that exact key. PostgreSQL
    // writes stay untracked until success because cleanup there updates the
    // existing metadata row in the caller's transaction.
    if externally_owned {
        context
            .mutation_state
            .lock()
            .unwrap()
            .newly_written
            .push(blob_id);
    }
    let put_result = blob_put_in_transaction(
        context.blob_store,
        transaction,
        &AssetBlobKey::for_blob_id(blob_id),
        &blob,
    )
    .await;
    put_result?;
    if !externally_owned {
        context
            .mutation_state
            .lock()
            .unwrap()
            .newly_written
            .push(blob_id);
    }
    sqlx::query(
        "INSERT INTO asset_blobs
                (id, checksum_algorithm, checksum, byte_size, media_type, data)
         VALUES ($1, 'sha256', $2, $3, $4, NULL)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(blob_id)
    .bind(blob.checksum())
    .bind(byte_size)
    .bind(blob.media_type())
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    context.mutation_state.lock().unwrap().pending_bytes =
        pending_bytes.checked_add(blob.byte_size()).ok_or_else(|| {
            AssetRepositoryError::Storage("asset batch byte count overflow".to_owned())
        })?;
    Ok((blob_id, input.media_type.clone()))
}

async fn attach(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: crate::domain::source::SourceId,
    review_id: &str,
    input: &AssetInput,
    asset_id: Uuid,
) -> Result<(), AssetRepositoryError> {
    sqlx::query(
        "INSERT INTO article_assets
                (source_id, review_id, asset_record_id, occurrence, role,
                 referer_url, origin, user_agent)
             VALUES ($1, $2, $3, $4, 'body', $5, $6, $7)
             ON CONFLICT (source_id, review_id, occurrence) DO UPDATE
             SET asset_record_id = EXCLUDED.asset_record_id,
                 role = EXCLUDED.role,
                 referer_url = EXCLUDED.referer_url,
                 origin = EXCLUDED.origin,
                 user_agent = EXCLUDED.user_agent",
    )
    .bind(source_id.as_uuid())
    .bind(review_id.trim())
    .bind(asset_id)
    .bind(i32::try_from(input.occurrence).map_err(|_| AssetRepositoryError::InvalidMetadata)?)
    .bind(input.referer_url.as_str())
    .bind(input.origin.as_deref())
    .bind(input.user_agent.as_deref())
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

async fn cleanup_new_blobs(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    blob_ids: &[Uuid],
) {
    if blob_ids.is_empty() {
        return;
    }
    let mut delete_failed = false;
    for blob_id in blob_ids {
        if let Err(error) = if blob_store.as_any().is::<PostgresAssetBlobStore>() {
            PostgresAssetBlobStore::delete_in_transaction(
                transaction,
                &AssetBlobKey::for_blob_id(*blob_id),
            )
            .await
            .map_err(AssetRepositoryError::from)
        } else {
            blob_store
                .delete(&AssetBlobKey::for_blob_id(*blob_id))
                .await
                .map_err(AssetRepositoryError::from)
        } {
            delete_failed = true;
            if !blob_store.as_any().is::<PostgresAssetBlobStore>() {
                if let Err(queue_error) = sqlx::query(
                    "INSERT INTO asset_blob_cleanup_queue (blob_id)
                     VALUES ($1) ON CONFLICT (blob_id) DO NOTHING",
                )
                .bind(blob_id)
                .execute(&mut **transaction)
                .await
                {
                    tracing::warn!(error = %queue_error, "unable to retain failed asset blob cleanup");
                }
            }
            tracing::warn!(%blob_id, error = %error, "unable to clean up an uncommitted asset blob");
        }
    }
    if !delete_failed {
        if let Err(error) = sqlx::query("DELETE FROM asset_blobs WHERE id = ANY($1::uuid[])")
            .bind(blob_ids.to_vec())
            .execute(&mut **transaction)
            .await
        {
            tracing::warn!(error = %error, "unable to clean up uncommitted asset blob metadata");
        }
    }
}

async fn cleanup_blob_ids_after_rollback(
    pool: &PgPool,
    blob_store: &dyn AssetBlobStore,
    blob_ids: &[Uuid],
) -> Result<(), AssetRepositoryError> {
    if blob_ids.is_empty() {
        return Ok(());
    }
    let mut failed = Vec::new();
    for blob_id in blob_ids {
        if let Err(error) = blob_store
            .delete(&AssetBlobKey::for_blob_id(*blob_id))
            .await
        {
            tracing::warn!(%blob_id, error = %error, "unable to clean up a restored asset blob after rollback");
            failed.push(*blob_id);
        }
    }
    if failed.is_empty() {
        return Ok(());
    }

    let mut transaction = pool.begin().await.map_err(storage_error)?;
    for blob_id in failed {
        sqlx::query(
            "INSERT INTO asset_blob_cleanup_queue (blob_id)
             VALUES ($1) ON CONFLICT (blob_id) DO NOTHING",
        )
        .bind(blob_id)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
    }
    transaction.commit().await.map_err(storage_error)
}

async fn evict_stale_blobs(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    seconds: i64,
) -> Result<Vec<Uuid>, AssetRepositoryError> {
    let rows = sqlx::query(
        "SELECT id
         FROM asset_blobs
         WHERE last_accessed_at < clock_timestamp()
               - make_interval(secs => $1::double precision)
         ORDER BY last_accessed_at ASC, id ASC",
    )
    .bind(seconds)
    .fetch_all(&mut **transaction)
    .await
    .map_err(storage_error)?;

    let mut evicted = Vec::new();
    for row in rows {
        let blob_id = row.try_get::<Uuid, _>("id").map_err(storage_error)?;
        let key = AssetBlobKey::for_blob_id(blob_id);
        if blob_stat_in_transaction(blob_store, transaction, &key)
            .await?
            .is_some_and(|metadata| metadata.stored_byte_size().is_some())
        {
            evicted.push(blob_id);
        }
    }
    Ok(evicted)
}

async fn cached_byte_size(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    excluded_blob_ids: &[Uuid],
) -> Result<i64, AssetRepositoryError> {
    if blob_store.as_any().is::<PostgresAssetBlobStore>() {
        return sqlx::query_scalar(
            "SELECT COALESCE(SUM(byte_size), 0)::BIGINT
             FROM asset_blobs
             WHERE data IS NOT NULL
               AND NOT (id = ANY($1::uuid[]))",
        )
        .bind(excluded_blob_ids.to_vec())
        .fetch_one(&mut **transaction)
        .await
        .map_err(storage_error);
    }
    let rows = sqlx::query(
        "SELECT id
         FROM asset_blobs
         WHERE NOT (id = ANY($1::uuid[]))",
    )
    .bind(excluded_blob_ids.to_vec())
    .fetch_all(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let mut total = 0_i64;
    for row in rows {
        let blob_id = row.try_get::<Uuid, _>("id").map_err(storage_error)?;
        let stored_byte_size =
            blob_stat_in_transaction(blob_store, transaction, &AssetBlobKey::for_blob_id(blob_id))
                .await?
                .and_then(|metadata| metadata.stored_byte_size());
        if let Some(byte_size) = stored_byte_size {
            total = total
                .checked_add(i64::try_from(byte_size).map_err(|_| {
                    AssetRepositoryError::Storage(
                        "asset byte count exceeds PostgreSQL range".to_owned(),
                    )
                })?)
                .ok_or_else(|| {
                    AssetRepositoryError::Storage("asset byte count overflow".to_owned())
                })?;
        }
    }
    Ok(total)
}

async fn oldest_stored(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    protected_blob_ids: &[Uuid],
) -> Result<Option<(Uuid, i64)>, AssetRepositoryError> {
    if blob_store.as_any().is::<PostgresAssetBlobStore>() {
        return sqlx::query_as::<_, (Uuid, i64)>(
            "SELECT id, byte_size
             FROM asset_blobs
             WHERE data IS NOT NULL
               AND NOT (id = ANY($1::uuid[]))
             ORDER BY last_accessed_at ASC, id ASC
             LIMIT 1",
        )
        .bind(protected_blob_ids.to_vec())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error);
    }
    let rows = sqlx::query(
        "SELECT id
         FROM asset_blobs
         WHERE NOT (id = ANY($1::uuid[]))
         ORDER BY last_accessed_at ASC, id ASC",
    )
    .bind(protected_blob_ids.to_vec())
    .fetch_all(&mut **transaction)
    .await
    .map_err(storage_error)?;
    for row in rows {
        let blob_id = row.try_get::<Uuid, _>("id").map_err(storage_error)?;
        if let Some(byte_size) =
            blob_stat_in_transaction(blob_store, transaction, &AssetBlobKey::for_blob_id(blob_id))
                .await?
                .and_then(|metadata| metadata.stored_byte_size())
        {
            let byte_size = i64::try_from(byte_size).map_err(|_| {
                AssetRepositoryError::Storage(
                    "asset byte count exceeds PostgreSQL range".to_owned(),
                )
            })?;
            return Ok(Some((blob_id, byte_size)));
        }
    }
    Ok(None)
}

struct CacheLimit<'a> {
    max_bytes: u64,
    additional_bytes: u64,
    protected_blob_ids: &'a [Uuid],
    pending_bytes: u64,
    pending_blob_ids: &'a [Uuid],
    deferred_evictions: &'a mut Vec<Uuid>,
}

async fn enforce_cache_limit(
    blob_store: &dyn AssetBlobStore,
    transaction: &mut Transaction<'_, Postgres>,
    limit: CacheLimit<'_>,
) -> Result<u64, AssetRepositoryError> {
    let CacheLimit {
        max_bytes,
        additional_bytes,
        protected_blob_ids,
        pending_bytes,
        pending_blob_ids,
        deferred_evictions,
    } = limit;
    if max_bytes == 0 {
        return Ok(0);
    }
    let max_bytes_i64 = match i64::try_from(max_bytes) {
        Ok(value) => value,
        Err(_) => return Ok(0),
    };
    let additional_i64 =
        i64::try_from(additional_bytes).map_err(|_| AssetRepositoryError::CapacityExceeded {
            requested_bytes: additional_bytes,
            max_bytes,
        })?;
    if additional_i64 > max_bytes_i64 {
        return Err(AssetRepositoryError::CapacityExceeded {
            requested_bytes: additional_bytes,
            max_bytes,
        });
    }

    // Serialize aggregate-capacity decisions across URL transactions. A
    // per-URL lock alone would allow concurrent first observations of
    // different URLs to all pass the same stale SUM and exceed the cap.
    lock_asset_capacity(transaction).await?;

    let pending_i64 =
        i64::try_from(pending_bytes).map_err(|_| AssetRepositoryError::CapacityExceeded {
            requested_bytes: additional_bytes,
            max_bytes,
        })?;
    let mut excluded_blob_ids = pending_blob_ids.to_vec();
    excluded_blob_ids.extend(deferred_evictions.iter().copied());
    let mut total = cached_byte_size(blob_store, transaction, &excluded_blob_ids)
        .await?
        .checked_add(pending_i64)
        .ok_or(AssetRepositoryError::CapacityExceeded {
            requested_bytes: additional_bytes,
            max_bytes,
        })?;
    let mut evicted = 0;
    loop {
        let exceeds = total
            .checked_add(additional_i64)
            .is_none_or(|value| value > max_bytes_i64);
        if !exceeds {
            break;
        }

        let mut protected = protected_blob_ids.to_vec();
        protected.extend(deferred_evictions.iter().copied());
        let Some((blob_id, byte_size)) = oldest_stored(blob_store, transaction, &protected).await?
        else {
            return Err(AssetRepositoryError::CapacityExceeded {
                requested_bytes: additional_bytes,
                max_bytes,
            });
        };
        deferred_evictions.push(blob_id);
        sqlx::query(
            "UPDATE asset_records
             SET fetch_status = 'missing',
                 last_error = 'asset bytes evicted by size policy',
                 updated_at = clock_timestamp()
             WHERE blob_id = $1
               AND fetch_status = 'available'",
        )
        .bind(blob_id)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
        total = total.saturating_sub(byte_size);
        evicted += 1;
    }
    Ok(evicted)
}

async fn lock_asset_recovery_admission(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), AssetRepositoryError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0))")
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    Ok(())
}

async fn snapshot_asset_generation(
    transaction: &mut Transaction<'_, Postgres>,
    asset_id: Uuid,
    generation: i64,
) -> Result<i64, AssetRepositoryError> {
    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id, state)
         SELECT $1, $2, row_number() OVER (ORDER BY source_id, review_id),
                source_id, review_id, 'unseen'
         FROM (
             SELECT DISTINCT source_id, review_id
             FROM article_assets WHERE asset_record_id = $1
         ) AS live_articles",
    )
    .bind(asset_id)
    .bind(generation)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM asset_refresh_generation_entries
         WHERE asset_record_id = $1 AND generation = $2",
    )
    .bind(asset_id)
    .bind(generation)
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)
}

async fn complete_refresh_generation_if_terminal(
    transaction: &mut Transaction<'_, Postgres>,
    asset_id: Uuid,
    generation: i64,
) -> Result<(), AssetRepositoryError> {
    let unfinished = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1 FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND generation = $2 AND state <> 'done'
         )",
    )
    .bind(asset_id)
    .bind(generation)
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if !unfinished {
        sqlx::query(
            "UPDATE asset_record_refresh_state
             SET completed_generation = GREATEST(completed_generation, $2),
                 updated_at = clock_timestamp()
             WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .bind(generation)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    }
    Ok(())
}

async fn mark_exhausted_refresh_entries(
    transaction: &mut Transaction<'_, Postgres>,
    asset_id: Uuid,
    generation: i64,
    policy: AssetRefreshPolicy,
) -> Result<(), AssetRepositoryError> {
    sqlx::query(
        "UPDATE asset_refresh_generation_entries AS e
         SET state = 'done', not_before = NULL
         FROM asset_article_refresh_lineage AS l
         WHERE e.asset_record_id = $1 AND e.generation = $2
           AND e.state IN ('unseen', 'cooling')
           AND l.source_id = e.source_id AND l.review_id = e.review_id
           AND (l.exhausted OR l.attempts >= $3)",
    )
    .bind(asset_id)
    .bind(generation)
    .bind(i64::from(policy.max_attempts()))
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

/// Selects the next admissible snapshot entry for one generation.
///
/// Runs under the capacity and recovery-admission locks. The article, live
/// relationship, blob, article-scoped dedupe key, and lineage are rechecked
/// after the source and article rows are locked, so only a still-current entry
/// becomes `in_flight`; a collision or stale entry is skipped without reserving
/// an attempt.
async fn select_refresh_entry(
    transaction: &mut Transaction<'_, Postgres>,
    blob_store: &dyn AssetBlobStore,
    asset_id: Uuid,
    generation: i64,
    after_ordinal: Option<i64>,
    policy: AssetRefreshPolicy,
) -> Result<Option<(i64, Uuid, String)>, AssetRepositoryError> {
    // Entries whose article or live relationship disappeared can never run.
    sqlx::query(
        "UPDATE asset_refresh_generation_entries AS e
         SET state = 'done', not_before = NULL
         WHERE e.asset_record_id = $1 AND e.generation = $2
           AND e.state IN ('unseen', 'cooling')
           AND NOT EXISTS (
               SELECT 1 FROM article_assets aa JOIN articles a
                 ON a.source_id = aa.source_id AND a.review_id = aa.review_id
               WHERE aa.asset_record_id = e.asset_record_id
                 AND aa.source_id = e.source_id AND aa.review_id = e.review_id
           )",
    )
    .bind(asset_id)
    .bind(generation)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let blob_id =
        sqlx::query_scalar::<_, Option<Uuid>>("SELECT blob_id FROM asset_records WHERE id = $1")
            .bind(asset_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?
            .flatten();
    let mut blob_present = false;
    if let Some(blob_id) = blob_id {
        blob_present = !blob_deletion_pending(transaction, blob_id).await?
            && blob_stat_in_transaction(
                blob_store,
                transaction,
                &AssetBlobKey::for_blob_id(blob_id),
            )
            .await?
            .is_some_and(|metadata| metadata.is_present());
    }
    if blob_present {
        // A restored body makes every pending entry superseded.
        sqlx::query(
            "UPDATE asset_refresh_generation_entries
             SET state = 'done', not_before = NULL
             WHERE asset_record_id = $1 AND generation = $2
               AND state IN ('unseen', 'cooling')",
        )
        .bind(asset_id)
        .bind(generation)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    }
    let mut cursor = after_ordinal;
    loop {
        let candidate = sqlx::query(
            "SELECT e.ordinal, e.source_id, e.review_id
             FROM asset_refresh_generation_entries AS e
             LEFT JOIN asset_article_refresh_lineage AS l
               ON l.source_id = e.source_id AND l.review_id = e.review_id
             WHERE e.asset_record_id = $1 AND e.generation = $2
               AND ($3::bigint IS NULL OR e.ordinal > $3)
               AND (e.state = 'unseen' OR
                    (e.state = 'cooling' AND e.not_before <= clock_timestamp()))
               AND COALESCE(l.exhausted, FALSE) = FALSE
               AND COALESCE(l.attempts, 0) < $4
               AND (l.next_allowed_at IS NULL OR l.next_allowed_at <= clock_timestamp())
               AND NOT EXISTS (
                   SELECT 1 FROM jobs j
                   WHERE j.dedupe_key = 'asset-refresh:' || e.source_id || ':' || e.review_id
                     AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')
               )
             ORDER BY e.ordinal
             LIMIT 1",
        )
        .bind(asset_id)
        .bind(generation)
        .bind(cursor)
        .bind(i64::from(policy.max_attempts()))
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let ordinal: i64 = candidate.try_get("ordinal").map_err(storage_error)?;
        let source_id: Uuid = candidate.try_get("source_id").map_err(storage_error)?;
        let review_id: String = candidate.try_get("review_id").map_err(storage_error)?;
        // Source then article locks precede every entry and lineage mutation.
        lock_refresh_article_rows(
            transaction,
            crate::domain::source::SourceId::from_uuid(source_id),
            &review_id,
        )
        .await?;
        let still_live = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1 FROM articles a
                 JOIN article_assets aa
                   ON aa.source_id = a.source_id AND aa.review_id = a.review_id
                 WHERE a.source_id = $1 AND a.review_id = $2
                   AND aa.asset_record_id = $3
             )",
        )
        .bind(source_id)
        .bind(&review_id)
        .bind(asset_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if !still_live {
            sqlx::query(
                "UPDATE asset_refresh_generation_entries
                 SET state = 'done', not_before = NULL
                 WHERE asset_record_id = $1 AND generation = $2 AND ordinal = $3
                   AND state IN ('unseen', 'cooling')",
            )
            .bind(asset_id)
            .bind(generation)
            .bind(ordinal)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            cursor = Some(ordinal);
            continue;
        }
        let entry_still_eligible = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1 FROM asset_refresh_generation_entries
                 WHERE asset_record_id = $1 AND generation = $2 AND ordinal = $3
                   AND (state = 'unseen' OR
                        (state = 'cooling' AND not_before <= clock_timestamp()))
             )",
        )
        .bind(asset_id)
        .bind(generation)
        .bind(ordinal)
        .fetch_one(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if !entry_still_eligible {
            cursor = Some(ordinal);
            continue;
        }
        return Ok(Some((ordinal, source_id, review_id)));
    }
}

async fn reserve_refresh_lineage(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
    review_id: &str,
    policy: AssetRefreshPolicy,
) -> Result<bool, AssetRepositoryError> {
    let attempts = sqlx::query_scalar::<_, i64>(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, next_allowed_at, exhausted,
              last_outcome, updated_at)
         VALUES ($1, $2, 1, NULL, FALSE, NULL, clock_timestamp())
         ON CONFLICT (source_id, review_id) DO UPDATE
         SET attempts = asset_article_refresh_lineage.attempts + 1,
             next_allowed_at = NULL, last_outcome = NULL,
             updated_at = clock_timestamp()
         WHERE asset_article_refresh_lineage.exhausted = FALSE
           AND asset_article_refresh_lineage.attempts < $3
           AND (asset_article_refresh_lineage.next_allowed_at IS NULL OR
                asset_article_refresh_lineage.next_allowed_at <= clock_timestamp())
         RETURNING attempts",
    )
    .bind(source_id)
    .bind(review_id)
    .bind(i64::from(policy.max_attempts()))
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(attempts.is_some())
}

async fn insert_refresh_job(
    job_transaction: &mut PostgresJobTransaction<'_>,
    asset_id: Uuid,
    generation: i64,
    chain_id: Uuid,
    ordinal: i64,
    source_id: Uuid,
    review_id: &str,
) -> Result<Option<Uuid>, AssetRepositoryError> {
    let now = Utc::now();
    let result = job_transaction
        .enqueue_internal(
            NewJob {
                job_type: JobType::AssetRefresh,
                source_id: None,
                priority: 0,
                run_after: now,
                max_attempts: REPAIR_JOB_MAX_ATTEMPTS,
                payload: json!({
                    "source_id": source_id,
                    "review_id": review_id,
                    "expired_asset_record_id": asset_id,
                    "chain_id": chain_id,
                    "generation": generation,
                    "ordinal": ordinal,
                }),
                dedupe_key: format!("asset-refresh:{source_id}:{review_id}"),
                now,
            },
            true,
        )
        .await
        .map_err(job_transaction_error)?;
    match result {
        crate::persistence::repositories::job_repository::EnqueueResult::Inserted(job) => {
            let transaction = job_transaction
                .transaction_mut()
                .map_err(job_transaction_error)?;
            sqlx::query(
                "INSERT INTO asset_repair_jobs
                     (job_id, asset_record_id, admission_class, refresh_source_id,
                      refresh_review_id, chain_id, chain_ordinal,
                      expired_asset_record_id, expiry_refunded)
                 VALUES ($1, NULL, 'internal_refresh', $2, $3, $4, $5, $6, FALSE)",
            )
            .bind(job.id())
            .bind(source_id)
            .bind(review_id)
            .bind(chain_id)
            .bind(ordinal)
            .bind(asset_id)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            Ok(Some(job.id()))
        }
        // The article-scoped dedupe key already owns this article's refresh.
        // The caller leaves the entry unseen so a later ordinal can be tried.
        crate::persistence::repositories::job_repository::EnqueueResult::AlreadyActive {
            ..
        } => Ok(None),
    }
}

async fn admit_refresh_entry(
    job_transaction: &mut PostgresJobTransaction<'_>,
    asset_id: Uuid,
    generation: i64,
    chain_id: Uuid,
    entry: (i64, Uuid, String),
    policy: AssetRefreshPolicy,
    resume: bool,
) -> Result<Option<Uuid>, AssetRepositoryError> {
    let (ordinal, source_id, review_id) = entry;
    let Some(job_id) = insert_refresh_job(
        job_transaction,
        asset_id,
        generation,
        chain_id,
        ordinal,
        source_id,
        &review_id,
    )
    .await?
    else {
        return Ok(None);
    };
    let reserved = {
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        reserve_refresh_lineage(transaction, source_id, &review_id, policy).await?
    };
    if !reserved {
        // The attempt is only consumed by an inserted job, so drop the job and
        // its metadata together and terminalize the entry only when lineage is
        // actually exhausted; a raced cooldown keeps it pending.
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query(
            "UPDATE asset_refresh_generation_entries AS e
             SET state = 'done', not_before = NULL
             FROM asset_article_refresh_lineage AS l
             WHERE e.asset_record_id = $1 AND e.generation = $2 AND e.ordinal = $3
               AND l.source_id = e.source_id AND l.review_id = e.review_id
               AND (l.exhausted OR l.attempts >= $4)",
        )
        .bind(asset_id)
        .bind(generation)
        .bind(ordinal)
        .bind(i64::from(policy.max_attempts()))
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
        return Ok(None);
    }
    let transaction = job_transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    let activated = sqlx::query(
        "UPDATE asset_refresh_generation_entries
         SET state = 'in_flight', not_before = NULL
         WHERE asset_record_id = $1 AND generation = $2 AND ordinal = $3
           AND state IN ('unseen', 'cooling')",
    )
    .bind(asset_id)
    .bind(generation)
    .bind(ordinal)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if activated.rows_affected() == 0 {
        sqlx::query("DELETE FROM asset_repair_jobs WHERE job_id = $1")
            .bind(job_id)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("DELETE FROM jobs WHERE id = $1")
            .bind(job_id)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        return Ok(None);
    }
    if resume {
        sqlx::query(
            "UPDATE asset_record_refresh_state
             SET active_chain_id = $2, updated_at = clock_timestamp()
             WHERE asset_record_id = $1
               AND (active_chain_id IS NULL OR active_chain_id = $2)",
        )
        .bind(asset_id)
        .bind(chain_id)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    }
    Ok(Some(job_id))
}

async fn resume_refresh_after_cache_miss(
    job_transaction: &mut PostgresJobTransaction<'_>,
    blob_store: &dyn AssetBlobStore,
    asset_id: Uuid,
    repair_policy: AssetRepairPolicy,
    refresh_policy: AssetRefreshPolicy,
) -> Result<Option<AssetRepairEnqueueResult>, AssetRepositoryError> {
    let state = {
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        sqlx::query(
            "SELECT requested_generation, completed_generation, served_generation, active_chain_id
             FROM asset_record_refresh_state WHERE asset_record_id = $1 FOR UPDATE",
        )
        .bind(asset_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
    };
    let Some(state) = state else {
        return Ok(None);
    };
    let requested: i64 = state
        .try_get("requested_generation")
        .map_err(storage_error)?;
    let completed: i64 = state
        .try_get("completed_generation")
        .map_err(storage_error)?;
    let served: i64 = state.try_get("served_generation").map_err(storage_error)?;
    let active_chain: Option<Uuid> = state.try_get("active_chain_id").map_err(storage_error)?;
    if active_chain.is_some() {
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        sqlx::query(
            "UPDATE asset_record_refresh_state
             SET requested_generation = GREATEST(requested_generation, served_generation + 1),
                 updated_at = clock_timestamp() WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
        return Ok(Some(AssetRepairEnqueueResult::Retryable));
    }
    if requested <= completed || served == 0 {
        return Ok(None);
    }

    let transaction = job_transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    let global_pending: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM asset_repair_jobs arj
         JOIN jobs j ON j.id = arj.job_id
         WHERE j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if global_pending >= i64::from(repair_policy.max_pending()) {
        return Ok(Some(AssetRepairEnqueueResult::Retryable));
    }
    if served <= completed {
        let job_id = start_follow_up_generation(
            job_transaction,
            blob_store,
            asset_id,
            requested,
            refresh_policy,
        )
        .await?;
        return Ok(Some(
            job_id.map_or(AssetRepairEnqueueResult::Retryable, |job_id| {
                AssetRepairEnqueueResult::Enqueued { job_id }
            }),
        ));
    }
    mark_exhausted_refresh_entries(transaction, asset_id, served, refresh_policy).await?;
    let entry = select_refresh_entry(
        transaction,
        blob_store,
        asset_id,
        served,
        None,
        refresh_policy,
    )
    .await?;
    let Some(entry) = entry else {
        // Nothing in this snapshot is admissible yet; complete it when every
        // entry is done and otherwise keep it pending for the next pass.
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        complete_refresh_generation_if_terminal(transaction, asset_id, served).await?;
        return Ok(Some(
            resume_follow_up_generation(
                job_transaction,
                blob_store,
                asset_id,
                served,
                refresh_policy,
            )
            .await?,
        ));
    };
    let chain_id = Uuid::new_v4();
    let job_id = admit_refresh_entry(
        job_transaction,
        asset_id,
        served,
        chain_id,
        entry,
        refresh_policy,
        true,
    )
    .await?;
    let Some(job_id) = job_id else {
        return Ok(Some(AssetRepairEnqueueResult::Retryable));
    };
    Ok(Some(AssetRepairEnqueueResult::Enqueued { job_id }))
}

/// Starts a coalesced follow-up after a pending generation completes.
async fn resume_follow_up_generation(
    job_transaction: &mut PostgresJobTransaction<'_>,
    blob_store: &dyn AssetBlobStore,
    asset_id: Uuid,
    served: i64,
    policy: AssetRefreshPolicy,
) -> Result<AssetRepairEnqueueResult, AssetRepositoryError> {
    let completed = refresh_generation_completed(job_transaction, asset_id, served).await?;
    if !completed {
        return Ok(AssetRepairEnqueueResult::Retryable);
    }
    let (requested, current) = {
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT requested_generation, served_generation
             FROM asset_record_refresh_state
             WHERE asset_record_id = $1 FOR UPDATE",
        )
        .bind(asset_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .unwrap_or((0, 0))
    };
    if requested <= current {
        return Ok(AssetRepairEnqueueResult::Retryable);
    }
    Ok(
        start_follow_up_generation(job_transaction, blob_store, asset_id, requested, policy)
            .await?
            .map_or(AssetRepairEnqueueResult::Retryable, |job_id| {
                AssetRepairEnqueueResult::Enqueued { job_id }
            }),
    )
}

async fn finish_article_refresh_in_transaction(
    job_transaction: &mut PostgresJobTransaction<'_>,
    lease: &JobLease,
    target: &ArticleRefreshTarget,
    blob_store: &dyn AssetBlobStore,
    outcome: ArticleRefreshOutcome,
    policy: AssetRefreshPolicy,
) -> Result<RefreshFinishResult, AssetRepositoryError> {
    let transaction = job_transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    let cooldown = i64::try_from(policy.cooldown().as_secs())
        .map_err(|_| AssetRepositoryError::InvalidMetadata)?;
    match outcome {
        ArticleRefreshOutcome::Succeeded => {
            sqlx::query(
                "UPDATE asset_article_refresh_lineage
                 SET attempts = 0, next_allowed_at = NULL, exhausted = FALSE,
                     last_outcome = 'succeeded', updated_at = clock_timestamp()
                 WHERE source_id = $1 AND review_id = $2",
            )
            .bind(target.source_id.as_uuid())
            .bind(&target.review_id)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
            set_refresh_entry_done(transaction, target).await?;
            reconcile_article_refresh_entries(
                transaction,
                target.source_id.as_uuid(),
                &target.review_id,
            )
            .await?;
        }
        ArticleRefreshOutcome::Inconclusive => {
            let exhausted = sqlx::query_scalar::<_, bool>(
                "UPDATE asset_article_refresh_lineage
                 SET next_allowed_at = clock_timestamp()
                         + make_interval(secs => $3::double precision),
                     exhausted = attempts >= $4,
                     last_outcome = CASE WHEN attempts >= $4 THEN 'exhausted' ELSE 'inconclusive' END,
                     updated_at = clock_timestamp()
                 WHERE source_id = $1 AND review_id = $2
                 RETURNING exhausted",
            )
            .bind(target.source_id.as_uuid())
            .bind(&target.review_id)
            .bind(cooldown)
            .bind(i64::from(policy.max_attempts()))
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?
            .unwrap_or(true);
            if exhausted {
                set_refresh_entry_done(transaction, target).await?;
                reconcile_article_refresh_entries(
                    transaction,
                    target.source_id.as_uuid(),
                    &target.review_id,
                )
                .await?;
            } else {
                sqlx::query(
                    "UPDATE asset_refresh_generation_entries
                     SET state = 'cooling',
                         not_before = clock_timestamp()
                             + make_interval(secs => $4::double precision)
                     WHERE asset_record_id = $1 AND generation = $2 AND ordinal = $3",
                )
                .bind(target.expired_asset_record_id)
                .bind(target.served_generation)
                .bind(target.chain_ordinal)
                .bind(cooldown)
                .execute(&mut **transaction)
                .await
                .map_err(storage_error)?;
            }
        }
        ArticleRefreshOutcome::Superseded => {
            set_refresh_entry_done(transaction, target).await?;
        }
        ArticleRefreshOutcome::Deleted => {
            set_refresh_entry_done(transaction, target).await?;
        }
    }

    let owner = lease
        .job
        .lease_owner()
        .ok_or(AssetRepositoryError::LeaseLost)?;
    JobRepositoryTransaction::succeed(
        job_transaction,
        lease.job.id(),
        owner,
        lease.token,
        Utc::now(),
    )
    .await
    .map_err(job_transaction_error)?;

    // The selector can retire later entries, so completion is measured after
    // continuation rather than before it.
    let mut next_job_id =
        continue_refresh_chain(job_transaction, blob_store, target, policy).await?;
    let (requested, served) = {
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        complete_refresh_generation_if_terminal(
            transaction,
            target.expired_asset_record_id,
            target.served_generation,
        )
        .await?;
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT requested_generation, served_generation
             FROM asset_record_refresh_state
             WHERE asset_record_id = $1 FOR UPDATE",
        )
        .bind(target.expired_asset_record_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(storage_error)?
    };
    let generation_completed = refresh_generation_completed(
        job_transaction,
        target.expired_asset_record_id,
        target.served_generation,
    )
    .await?;
    // A coalesced follow-up starts only once its generation is complete, and
    // never re-snapshots the generation that just finished.
    if next_job_id.is_none() && generation_completed && requested > served {
        next_job_id = start_follow_up_generation(
            job_transaction,
            blob_store,
            target.expired_asset_record_id,
            requested,
            policy,
        )
        .await?;
    }
    Ok(RefreshFinishResult {
        next_job_id,
        generation_completed,
    })
}

/// Reports whether a generation is complete: its high-water mark passed or
/// every snapshot entry is `done`.
async fn refresh_generation_completed(
    job_transaction: &mut PostgresJobTransaction<'_>,
    asset_id: Uuid,
    generation: i64,
) -> Result<bool, AssetRepositoryError> {
    let transaction = job_transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    sqlx::query_scalar::<_, bool>(
        "SELECT s.completed_generation >= $2 OR NOT EXISTS (
             SELECT 1 FROM asset_refresh_generation_entries e
             WHERE e.asset_record_id = s.asset_record_id
               AND e.generation = $2 AND e.state <> 'done'
         )
         FROM asset_record_refresh_state s
         WHERE s.asset_record_id = $1",
    )
    .bind(asset_id)
    .bind(generation)
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)
}

async fn start_follow_up_generation(
    job_transaction: &mut PostgresJobTransaction<'_>,
    blob_store: &dyn AssetBlobStore,
    asset_id: Uuid,
    generation: i64,
    policy: AssetRefreshPolicy,
) -> Result<Option<Uuid>, AssetRepositoryError> {
    let transaction = job_transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    // Advance to a strictly newer generation so a completed snapshot is never
    // re-created under the same identity.
    let advanced = sqlx::query(
        "UPDATE asset_record_refresh_state
         SET served_generation = $2, updated_at = clock_timestamp()
         WHERE asset_record_id = $1 AND requested_generation >= $2
           AND served_generation < $2 AND active_chain_id IS NULL",
    )
    .bind(asset_id)
    .bind(generation)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?
    .rows_affected();
    if advanced == 0 {
        return Ok(None);
    }
    let count = snapshot_asset_generation(transaction, asset_id, generation).await?;
    if count == 0 {
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        sqlx::query(
            "UPDATE asset_record_refresh_state
             SET completed_generation = $2, updated_at = clock_timestamp()
             WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .bind(generation)
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
        return Ok(None);
    }
    let transaction = job_transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    mark_exhausted_refresh_entries(transaction, asset_id, generation, policy).await?;
    complete_refresh_generation_if_terminal(transaction, asset_id, generation).await?;
    let entry =
        select_refresh_entry(transaction, blob_store, asset_id, generation, None, policy).await?;
    let Some(entry) = entry else {
        return Ok(None);
    };
    let chain_id = Uuid::new_v4();
    admit_refresh_entry(
        job_transaction,
        asset_id,
        generation,
        chain_id,
        entry,
        policy,
        true,
    )
    .await
}

async fn set_refresh_entry_done(
    transaction: &mut Transaction<'_, Postgres>,
    target: &ArticleRefreshTarget,
) -> Result<(), AssetRepositoryError> {
    sqlx::query(
        "UPDATE asset_refresh_generation_entries
         SET state = 'done', not_before = NULL
         WHERE asset_record_id = $1 AND generation = $2 AND ordinal = $3",
    )
    .bind(target.expired_asset_record_id)
    .bind(target.served_generation)
    .bind(target.chain_ordinal)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

async fn continue_refresh_chain(
    job_transaction: &mut PostgresJobTransaction<'_>,
    blob_store: &dyn AssetBlobStore,
    target: &ArticleRefreshTarget,
    policy: AssetRefreshPolicy,
) -> Result<Option<Uuid>, AssetRepositoryError> {
    {
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        mark_exhausted_refresh_entries(
            transaction,
            target.expired_asset_record_id,
            target.served_generation,
            policy,
        )
        .await?;
    }
    let entry = {
        let transaction = job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        select_refresh_entry(
            transaction,
            blob_store,
            target.expired_asset_record_id,
            target.served_generation,
            Some(target.chain_ordinal),
            policy,
        )
        .await?
    };
    if let Some(entry) = entry {
        if let Some(job_id) = admit_refresh_entry(
            job_transaction,
            target.expired_asset_record_id,
            target.served_generation,
            target.chain_id,
            entry,
            policy,
            false,
        )
        .await?
        {
            return Ok(Some(job_id));
        }
    }
    let transaction = job_transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    sqlx::query(
        "UPDATE asset_record_refresh_state
         SET active_chain_id = NULL, updated_at = clock_timestamp()
         WHERE asset_record_id = $1 AND active_chain_id = $2",
    )
    .bind(target.expired_asset_record_id)
    .bind(target.chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(None)
}

async fn route_confirmed_expiry(
    transaction: &mut PostgresJobTransaction<'_>,
    blob_store: &dyn AssetBlobStore,
    asset_id: Uuid,
    repair_policy: AssetRepairPolicy,
    refresh_policy: AssetRefreshPolicy,
) -> Result<AssetRefreshAdmissionResult, AssetRepositoryError> {
    let state = {
        let sql_transaction = transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;
        sqlx::query(
            "SELECT requested_generation, completed_generation, served_generation, active_chain_id
             FROM asset_record_refresh_state WHERE asset_record_id = $1 FOR UPDATE",
        )
        .bind(asset_id)
        .fetch_optional(&mut **sql_transaction)
        .await
        .map_err(storage_error)?
    };
    let requested = state
        .as_ref()
        .map(|row| row.try_get::<i64, _>("requested_generation"))
        .transpose()
        .map_err(storage_error)?
        .unwrap_or(0);
    let completed = state
        .as_ref()
        .map(|row| row.try_get::<i64, _>("completed_generation"))
        .transpose()
        .map_err(storage_error)?
        .unwrap_or(0);
    let served = state
        .as_ref()
        .map(|row| row.try_get::<i64, _>("served_generation"))
        .transpose()
        .map_err(storage_error)?
        .unwrap_or(0);
    let active_chain = state
        .as_ref()
        .map(|row| row.try_get::<Option<Uuid>, _>("active_chain_id"))
        .transpose()
        .map_err(storage_error)?
        .flatten();

    let sql_transaction = transaction
        .transaction_mut()
        .map_err(job_transaction_error)?;
    if active_chain.is_some() {
        sqlx::query(
            "UPDATE asset_record_refresh_state
             SET requested_generation = GREATEST(requested_generation, served_generation + 1),
                 updated_at = clock_timestamp() WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .execute(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        return Ok(AssetRefreshAdmissionResult::AlreadyHandled);
    }

    if served > completed {
        sqlx::query(
            "UPDATE asset_record_refresh_state
             SET requested_generation = GREATEST(requested_generation, served_generation + 1),
                 updated_at = clock_timestamp() WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .execute(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        mark_exhausted_refresh_entries(sql_transaction, asset_id, served, refresh_policy).await?;
        let entry = select_refresh_entry(
            sql_transaction,
            blob_store,
            asset_id,
            served,
            None,
            refresh_policy,
        )
        .await?;
        let Some(entry) = entry else {
            complete_refresh_generation_if_terminal(sql_transaction, asset_id, served).await?;
            return Ok(AssetRefreshAdmissionResult::NoEligibleArticle);
        };
        let chain_id = Uuid::new_v4();
        let ordinal = entry.0;
        let Some(job_id) = admit_refresh_entry(
            transaction,
            asset_id,
            served,
            chain_id,
            entry,
            refresh_policy,
            true,
        )
        .await?
        else {
            return Ok(AssetRefreshAdmissionResult::NoEligibleArticle);
        };
        return Ok(AssetRefreshAdmissionResult::Enqueued {
            job_id,
            chain_id,
            generation: served,
            ordinal,
        });
    }

    // Reuse an already-requested but not-yet-served generation so a coalesced
    // follow-up is never skipped.
    let generation = if requested > completed {
        requested
    } else {
        requested
            .checked_add(1)
            .ok_or(AssetRepositoryError::InvalidMetadata)?
    };
    sqlx::query(
        "INSERT INTO asset_record_refresh_state
             (asset_record_id, requested_generation, completed_generation,
              served_generation, active_chain_id)
         VALUES ($1, $2, $3, $4, NULL)
         ON CONFLICT (asset_record_id) DO UPDATE
         SET requested_generation = $2, served_generation = $4,
             updated_at = clock_timestamp()",
    )
    .bind(asset_id)
    .bind(generation)
    .bind(completed)
    .bind(generation)
    .execute(&mut **sql_transaction)
    .await
    .map_err(storage_error)?;
    let entry_count = snapshot_asset_generation(sql_transaction, asset_id, generation).await?;
    if entry_count == 0 {
        sqlx::query(
            "UPDATE asset_record_refresh_state
             SET completed_generation = $2, updated_at = clock_timestamp()
             WHERE asset_record_id = $1",
        )
        .bind(asset_id)
        .bind(generation)
        .execute(&mut **sql_transaction)
        .await
        .map_err(storage_error)?;
        return Ok(AssetRefreshAdmissionResult::NoEligibleArticle);
    }
    mark_exhausted_refresh_entries(sql_transaction, asset_id, generation, refresh_policy).await?;
    let entry = select_refresh_entry(
        sql_transaction,
        blob_store,
        asset_id,
        generation,
        None,
        refresh_policy,
    )
    .await?;
    let Some(entry) = entry else {
        complete_refresh_generation_if_terminal(sql_transaction, asset_id, generation).await?;
        return Ok(AssetRefreshAdmissionResult::NoEligibleArticle);
    };
    let ordinal = entry.0;
    let chain_id = Uuid::new_v4();
    let Some(job_id) = admit_refresh_entry(
        transaction,
        asset_id,
        generation,
        chain_id,
        entry,
        refresh_policy,
        true,
    )
    .await?
    else {
        return Ok(AssetRefreshAdmissionResult::NoEligibleArticle);
    };
    let _ = repair_policy;
    Ok(AssetRefreshAdmissionResult::Enqueued {
        job_id,
        chain_id,
        generation,
        ordinal,
    })
}

async fn lock_asset_capacity(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), AssetRepositoryError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-capacity', 0))")
        .execute(&mut **transaction)
        .await
        .map_err(storage_error)?;
    Ok(())
}

/// Locks and verifies the current lease before an asset-repair mutation.
///
/// The row lock is held until the surrounding transaction commits. This makes
/// lease recovery wait for a fenced mutation instead of allowing an expired
/// worker to commit after a replacement worker has been granted the job.
async fn lock_live_repair_lease(
    transaction: &mut Transaction<'_, Postgres>,
    lease: &JobLease,
) -> Result<(), AssetRepositoryError> {
    let Some(owner) = lease.job.lease_owner() else {
        return Err(AssetRepositoryError::LeaseLost);
    };
    let live_job = sqlx::query_scalar::<_, Uuid>(
        "SELECT id
         FROM jobs
         WHERE id = $1
           AND status = 'running'
           AND lease_owner = $2
           AND lease_token = $3
           AND lease_until > clock_timestamp()
         FOR UPDATE",
    )
    .bind(lease.job.id())
    .bind(owner)
    .bind(lease.token.as_uuid())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    live_job.map(|_| ()).ok_or(AssetRepositoryError::LeaseLost)
}

fn validate_article_identity(
    source_id: crate::domain::source::SourceId,
    review_id: &str,
) -> Result<(), AssetRepositoryError> {
    if source_id.as_uuid().is_nil() || review_id.trim().is_empty() {
        return Err(AssetRepositoryError::InvalidArticleIdentity);
    }
    Ok(())
}

fn validate_input(
    input: &AssetInput,
    policy: AssetCachePolicy,
) -> Result<(), AssetRepositoryError> {
    if !input.checksum_matches_bytes() {
        return Err(AssetRepositoryError::ChecksumMismatch);
    }
    for url in [&input.source_url, &input.final_url, &input.referer_url] {
        if has_provider_signature(url) {
            return Err(AssetRepositoryError::InvalidUrl);
        }
    }
    normalize_url(input.source_url.clone())?;
    normalize_url(input.final_url.clone())?;
    normalize_url(input.referer_url.clone())?;
    if !matches!(
        input.media_type.as_str(),
        "image/png" | "image/jpeg" | "image/gif" | "image/webp"
    ) {
        return Err(AssetRepositoryError::InvalidMediaType);
    }
    if input.bytes.is_empty() {
        return Err(AssetRepositoryError::EmptyBody);
    }
    let bytes =
        u64::try_from(input.bytes.len()).map_err(|_| AssetRepositoryError::InvalidMetadata)?;
    if bytes > policy.max_asset_size_bytes() {
        return Err(AssetRepositoryError::AssetTooLarge {
            bytes,
            max_bytes: policy.max_asset_size_bytes(),
        });
    }
    if input.origin.as_deref().is_some_and(|origin| {
        origin.is_empty() || origin.len() > 512 || origin.chars().any(char::is_control)
    }) || input.user_agent.as_deref().is_some_and(|agent| {
        agent.is_empty() || agent.len() > 512 || agent.chars().any(char::is_control)
    }) {
        return Err(AssetRepositoryError::InvalidMetadata);
    }
    Ok(())
}

fn input_byte_size(input: &AssetInput) -> Result<i64, AssetRepositoryError> {
    i64::try_from(input.bytes.len()).map_err(|_| {
        AssetRepositoryError::Storage("asset body exceeds PostgreSQL byte range".to_owned())
    })
}

fn normalize_url(mut url: Url) -> Result<Url, AssetRepositoryError> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(AssetRepositoryError::InvalidUrl);
    }
    if (url.scheme() == "http" && url.port() == Some(80))
        || (url.scheme() == "https" && url.port() == Some(443))
    {
        url.set_port(None)
            .map_err(|_| AssetRepositoryError::InvalidUrl)?;
    }
    Ok(url)
}

fn parse_url(value: &str) -> Result<Url, AssetRepositoryError> {
    normalize_url(Url::parse(value).map_err(|_| AssetRepositoryError::InvalidUrl)?)
}

fn job_transaction_error(error: JobRepositoryError) -> AssetRepositoryError {
    AssetRepositoryError::Storage(error.to_string())
}

fn storage_error(error: impl fmt::Display) -> AssetRepositoryError {
    AssetRepositoryError::Storage(error.to_string())
}
