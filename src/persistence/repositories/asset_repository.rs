//! PostgreSQL persistence for archived article assets.
//!
//! Metadata and binary bytes deliberately have different lifetimes. Eviction
//! clears `asset_blobs.data` while retaining the URL/version row and the
//! article relationship, so the stable public asset ID can report a repairable
//! miss later. All add/attach operations are transaction-scoped and compare
//! raw bytes after using SHA-256 and size as candidate keys.

use std::{
    collections::{BTreeSet, HashMap},
    fmt,
};

use chrono::Utc;
use serde_json::json;
use sqlx::{Acquire, PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::archive::asset_store::{
    AssetCachePolicy, AssetInput, AssetMaintenanceResult, AssetRead, AssetRepairPolicy,
    AssetRepairTarget, StoredAsset,
};
use crate::domain::job::{JobType, NewJob};

use super::job_repository::{JobLease, JobRepositoryError, PostgresJobTransaction};

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
}

/// Pool-backed asset reads and cache maintenance.
#[derive(Clone)]
pub struct PostgresAssetStore {
    pool: PgPool,
    policy: AssetCachePolicy,
    repair_policy: AssetRepairPolicy,
}

impl fmt::Debug for PostgresAssetStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAssetStore")
            .field("pool", &"<postgres pool>")
            .field("policy", &self.policy)
            .field("repair_policy", &self.repair_policy)
            .finish()
    }
}

impl PostgresAssetStore {
    /// Creates a database-backed asset store.
    pub fn new(pool: PgPool, policy: AssetCachePolicy) -> Self {
        Self {
            pool,
            policy,
            repair_policy: AssetRepairPolicy::default(),
        }
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
            "SELECT r.source_url, b.media_type, b.checksum, b.data
             FROM asset_records AS r
             LEFT JOIN asset_blobs AS b ON b.id = r.blob_id
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
        let media_type = row
            .try_get::<Option<String>, _>("media_type")
            .map_err(storage_error)?;
        let checksum = row
            .try_get::<Option<String>, _>("checksum")
            .map_err(storage_error)?;
        let bytes = row
            .try_get::<Option<Vec<u8>>, _>("data")
            .map_err(storage_error)?;

        let result = match (media_type, checksum, bytes) {
            (Some(media_type), Some(checksum), Some(bytes)) => {
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
                    media_type,
                    checksum,
                    bytes,
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
            "SELECT r.source_url, aa.referer_url, aa.origin, aa.user_agent,
                    ars.next_allowed_at, COALESCE(ars.exhausted, FALSE) AS exhausted
             FROM asset_records AS r
             JOIN article_assets AS aa ON aa.asset_record_id = r.id
             LEFT JOIN asset_blobs AS b ON b.id = r.blob_id
             LEFT JOIN asset_repair_states AS ars ON ars.asset_record_id = r.id
             WHERE r.id = $1
               AND (r.blob_id IS NULL OR b.data IS NULL)
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
        let result =
            enqueue_repair_in_transaction(&mut transaction, self.repair_policy, asset_id).await?;
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
            "SELECT r.source_url, b.data
             FROM asset_records AS r
             LEFT JOIN asset_blobs AS b ON b.id = r.blob_id
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
        if record
            .try_get::<Option<Vec<u8>>, _>("data")
            .map_err(storage_error)?
            .is_some()
        {
            transaction.commit().await.map_err(storage_error)?;
            return Ok(AssetRepairRestoreResult::AlreadyAvailable);
        }

        let candidates = sqlx::query(
            "SELECT id, media_type, data
             FROM asset_blobs
             WHERE checksum = $1 AND byte_size = $2 AND data IS NOT NULL
             ORDER BY id
             FOR UPDATE",
        )
        .bind(&checksum)
        .bind(byte_size)
        .fetch_all(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let matching = candidates.iter().find_map(|row| {
            let data = row.try_get::<Vec<u8>, _>("data").ok()?;
            if data != input.bytes {
                return None;
            }
            Some((
                row.try_get::<Uuid, _>("id").ok()?,
                row.try_get::<String, _>("media_type").ok()?,
            ))
        });
        let (blob_id, media_type) = if let Some(matching) = matching {
            matching
        } else {
            insert_blob(
                &mut transaction,
                input,
                &checksum,
                byte_size,
                self.policy,
                &[],
            )
            .await?
        };
        // Hold the job row lock through commit so lease recovery cannot grant
        // the same job to another worker while this mutation is being fenced.
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
        transaction.commit().await.map_err(storage_error)?;
        let _ = media_type;
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

    /// Evicts stale/old binary data and removes rows without article
    /// relationships. This operation never deletes a referenced URL record.
    pub async fn maintenance(&self) -> Result<AssetMaintenanceResult, AssetRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        // All writers acquire this decision lock before touching URL, blob, or
        // relationship rows. Taking it first keeps maintenance in the same
        // order and prevents a stale-byte update from deadlocking a writer.
        lock_asset_capacity(&mut transaction).await?;

        let stale_blobs = if self.policy.max_age().is_zero() {
            0
        } else {
            let seconds = i64::try_from(self.policy.max_age().as_secs()).map_err(|_| {
                AssetRepositoryError::Storage("asset max age exceeds PostgreSQL range".to_owned())
            })?;
            sqlx::query(
                "UPDATE asset_blobs
                 SET data = NULL
                 WHERE data IS NOT NULL
                   AND last_accessed_at < clock_timestamp()
                       - make_interval(secs => $1::double precision)",
            )
            .bind(seconds)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?
            .rows_affected()
        };

        if stale_blobs > 0 {
            sqlx::query(
                "UPDATE asset_records
                 SET fetch_status = 'missing',
                     last_error = 'asset bytes evicted by age policy',
                     updated_at = clock_timestamp()
                 WHERE blob_id IN (
                     SELECT id FROM asset_blobs WHERE data IS NULL
                 )
                   AND fetch_status = 'available'",
            )
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        }

        let size_evicted_blobs =
            enforce_cache_limit(&mut transaction, self.policy.max_cache_size_bytes(), 0, &[])
                .await?;

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

        let orphan_blobs = sqlx::query(
            "DELETE FROM asset_blobs AS b
             WHERE NOT EXISTS (
                 SELECT 1 FROM asset_records AS r
                 WHERE r.blob_id = b.id
             )",
        )
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?
        .rows_affected();

        transaction.commit().await.map_err(storage_error)?;
        Ok(AssetMaintenanceResult {
            stale_blobs,
            size_evicted_blobs,
            orphan_records,
            orphan_blobs,
        })
    }
}

async fn enqueue_repair_in_transaction(
    job_transaction: &mut PostgresJobTransaction<'_>,
    policy: AssetRepairPolicy,
    asset_id: Uuid,
) -> Result<AssetRepairEnqueueResult, AssetRepositoryError> {
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
        "SELECT r.blob_id, b.data
         FROM asset_records AS r
         LEFT JOIN asset_blobs AS b ON b.id = r.blob_id
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
        return Ok(AssetRepairEnqueueResult::NotFound);
    };
    if target
        .try_get::<Option<Vec<u8>>, _>("data")
        .map_err(storage_error)?
        .is_some()
    {
        return Ok(AssetRepairEnqueueResult::AlreadyAvailable);
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
        let db_now: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut **sql_transaction)
            .await
            .map_err(storage_error)?;
        if next_allowed_at.is_some_and(|next| next > db_now) {
            return Ok(AssetRepairEnqueueResult::Backoff);
        }
        let admitted_attempts: i64 = state.try_get("admitted_attempts").map_err(storage_error)?;
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
            "SELECT id, data
             FROM asset_blobs
             WHERE checksum = $1
               AND byte_size = $2
               AND data IS NOT NULL
             ORDER BY id",
        )
        .bind(&checksum)
        .bind(byte_size)
        .fetch_all(&mut **transaction)
        .await
        .map_err(storage_error)?;

        for input in inputs.iter().filter(|input| {
            input.checksum() == checksum
                && input_byte_size(input).is_ok_and(|size| size == byte_size)
        }) {
            let matching_blob = candidates.iter().find_map(|row| {
                let data = row.try_get::<Vec<u8>, _>("data").ok()?;
                (data == input.bytes).then(|| row.try_get::<Uuid, _>("id").ok())?
            });
            matches.insert(input.preparation_id(), matching_blob);
        }
    }

    Ok(AssetBatchPreparation {
        matching_blobs: matches,
        matching_inputs,
    })
}

/// Transaction-scoped PostgreSQL asset view owned by `UnitOfWork`.
pub struct PostgresAssetTransaction<'borrow, 'pool> {
    job_transaction: &'borrow mut PostgresJobTransaction<'pool>,
    preparation: Option<AssetBatchPreparation>,
    policy: AssetCachePolicy,
}

impl<'borrow, 'pool> PostgresAssetTransaction<'borrow, 'pool> {
    /// Creates an asset view over a unit-of-work transaction.
    pub(crate) fn new(
        job_transaction: &'borrow mut PostgresJobTransaction<'pool>,
        preparation: Option<AssetBatchPreparation>,
        policy: AssetCachePolicy,
    ) -> Self {
        Self {
            job_transaction,
            preparation,
            policy,
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
        let transaction = self.transaction()?;
        // A batch can contain several URLs. Acquire the aggregate-capacity
        // lock before the first per-URL lock so two batches cannot hold
        // different URL locks while waiting for each other's capacity turn.
        lock_asset_capacity(transaction).await?;
        let mut savepoint = transaction.begin().await.map_err(storage_error)?;
        let result = store_inputs(
            &mut savepoint,
            policy,
            source_id,
            review_id,
            inputs,
            preparation.as_ref(),
        )
        .await;
        let stored = match result {
            Ok(stored) => stored,
            Err(error) => {
                return match savepoint.rollback().await {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(AssetRepositoryError::Storage(format!(
                        "{error}; asset batch rollback failed: {rollback_error}"
                    ))),
                };
            }
        };
        savepoint.commit().await.map_err(storage_error)?;
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

            store_inputs(
                &mut savepoint,
                policy,
                source_id,
                review_id,
                inputs,
                preparation.as_ref(),
            )
            .await
        }
        .await;
        match result {
            Ok(stored) => {
                savepoint.commit().await.map_err(storage_error)?;
                Ok(stored)
            }
            Err(error) => match savepoint.rollback().await {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(AssetRepositoryError::Storage(format!(
                    "{error}; asset replacement rollback failed: {rollback_error}"
                ))),
            },
        }
    }
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
    policy: AssetCachePolicy,
    source_id: crate::domain::source::SourceId,
    review_id: &str,
    inputs: &[AssetInput],
    preparation: Option<&AssetBatchPreparation>,
) -> Result<Vec<StoredAsset>, AssetRepositoryError> {
    let preparation = preparation.ok_or(AssetRepositoryError::PreparationRequired)?;
    let mut stored = Vec::with_capacity(inputs.len());
    // Keep bodies already returned by this batch out of LRU eviction. If a
    // later input cannot fit without evicting one of these bodies, reject the
    // whole savepoint instead of returning a URL that is immediately missing.
    let mut protected_blob_ids = Vec::with_capacity(inputs.len());
    let mut batch_blobs = HashMap::with_capacity(inputs.len());
    for input in inputs {
        validate_input(input, policy)?;
        let asset = store_one(
            transaction,
            policy,
            input,
            preparation,
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

async fn store_one(
    transaction: &mut Transaction<'_, Postgres>,
    policy: AssetCachePolicy,
    input: &AssetInput,
    preparation: &AssetBatchPreparation,
    protected_blob_ids: &[Uuid],
    batch_blobs: &HashMap<Uuid, Uuid>,
) -> Result<StoredAsset, AssetRepositoryError> {
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
                "SELECT checksum, byte_size, media_type
                 FROM asset_blobs
                 WHERE id = $1
                   AND checksum = $2
                   AND byte_size = $3
                   AND data IS NOT NULL
                 FOR UPDATE",
            )
            .bind(blob_id)
            .bind(&checksum)
            .bind(byte_size)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
            if let Some(refreshed_blob) = refreshed_blob {
                let refreshed_checksum = refreshed_blob
                    .try_get::<String, _>("checksum")
                    .map_err(storage_error)?;
                let refreshed_size = refreshed_blob
                    .try_get::<i64, _>("byte_size")
                    .map_err(storage_error)?;
                if refreshed_checksum == checksum && refreshed_size == byte_size {
                    let media_type = refreshed_blob
                        .try_get::<String, _>("media_type")
                        .map_err(storage_error)?;
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
            "SELECT media_type
             FROM asset_blobs
             WHERE id = $1
               AND checksum = $2
               AND byte_size = $3
               AND data IS NOT NULL
             FOR UPDATE",
        )
        .bind(blob_id)
        .bind(&checksum)
        .bind(byte_size)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if let Some(row) = row {
            let media_type = row.try_get("media_type").map_err(storage_error)?;
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
            (blob_id, media_type)
        } else {
            insert_blob(
                transaction,
                input,
                &checksum,
                byte_size,
                policy,
                protected_blob_ids,
            )
            .await?
        }
    } else {
        insert_blob(
            transaction,
            input,
            &checksum,
            byte_size,
            policy,
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
    input: &AssetInput,
    checksum: &str,
    byte_size: i64,
    policy: AssetCachePolicy,
    protected_blob_ids: &[Uuid],
) -> Result<(Uuid, String), AssetRepositoryError> {
    enforce_cache_limit(
        transaction,
        policy.max_cache_size_bytes(),
        u64::try_from(byte_size).expect("non-negative asset size"),
        protected_blob_ids,
    )
    .await?;
    let blob_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO asset_blobs
                (id, checksum_algorithm, checksum, byte_size, media_type, data)
             VALUES ($1, 'sha256', $2, $3, $4, $5)",
    )
    .bind(blob_id)
    .bind(checksum)
    .bind(byte_size)
    .bind(&input.media_type)
    .bind(&input.bytes)
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
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

async fn enforce_cache_limit(
    transaction: &mut Transaction<'_, Postgres>,
    max_bytes: u64,
    additional_bytes: u64,
    protected_blob_ids: &[Uuid],
) -> Result<u64, AssetRepositoryError> {
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

    let mut total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(byte_size), 0)::bigint
         FROM asset_blobs
         WHERE data IS NOT NULL",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)?;
    let mut evicted = 0;
    loop {
        let exceeds = total
            .checked_add(additional_i64)
            .is_none_or(|value| value > max_bytes_i64);
        if !exceeds {
            break;
        }

        let row = sqlx::query(
            "SELECT id, byte_size
             FROM asset_blobs
             WHERE data IS NOT NULL
               AND NOT (id = ANY($1::uuid[]))
             ORDER BY last_accessed_at ASC, id ASC
             LIMIT 1
             FOR UPDATE",
        )
        .bind(protected_blob_ids.to_vec())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
        let Some(row) = row else {
            return Err(AssetRepositoryError::CapacityExceeded {
                requested_bytes: additional_bytes,
                max_bytes,
            });
        };
        let blob_id: Uuid = row.try_get("id").map_err(storage_error)?;
        let byte_size: i64 = row.try_get("byte_size").map_err(storage_error)?;
        sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
            .bind(blob_id)
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
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
