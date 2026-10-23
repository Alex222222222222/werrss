//! PostgreSQL source repository and transaction-scoped source mutations.
//!
//! A source row owns the normalized public-account identity, feed-visible
//! configuration, scheduler gate, and durable scheduling timestamps. Reads are
//! short pool operations. Creation and mutations are exposed through a
//! transaction-scoped view so a source change can share the `UnitOfWork` commit
//! with article/archive writes, feed-cache publication, and job completion.
//!
//! Responsibilities:
//!
//! - validate and persist [`NewSource`] values;
//! - find sources by durable ID or unique WeRead `book_id`;
//! - update operator-controlled enable/gate state and worker scheduling
//!   timestamps; and
//! - advance the monotonic feed revision with compare-and-swap semantics.
//!
//! Non-responsibilities: resolving article URLs, browser access, credential
//! storage, feed-token generation, job execution, or deciding quiet hours.
//! When present, an article URL must already be resolved and verified by the
//! acquisition boundary; book-only sources persist a NULL URL.
//!
//! Cache interaction: feed-visible source changes must call
//! `bump_feed_revision` in the same transaction as their source update. The
//! feed-cache reader then treats an older cache revision as stale without
//! requiring a destructive cache delete. Scheduling-only changes do not bump
//! the revision.
//!
//! High availability: source reads and transaction mutations use the shared
//! PostgreSQL pool. Revision compare-and-swap prevents two concurrent source
//! mutations from silently overwriting the same feed-visible state. The
//! scheduler's cross-table due-source reservation remains in
//! `scheduler_repository`; it must not be reconstructed from these operations.

use std::fmt;

use chrono::{DateTime, Duration, Utc};
use sqlx::{postgres::PgRow, PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::source::{
    FeedRevision, NewSource, SchedulingGate, Source, SourceError, SourceId, SourceParts,
    SourcePatch, VerifiedWechatArticleUrl,
};

use super::job_repository::{JobRepositoryError, PostgresJobTransaction};

const SOURCE_COLUMNS: &str = "id, book_id, display_name, article_url, account_id, feed_revision, enabled, scheduling_gate, sync_interval_seconds, rss_item_limit, next_fetch_at, failure_cooldown_until, schedule_reserved_until, priority, max_attempts, created_at, updated_at";

/// Errors returned by source repositories.
#[derive(Debug, Error)]
pub enum SourceRepositoryError {
    /// A source value failed domain validation.
    #[error(transparent)]
    Domain(#[from] SourceError),
    /// The requested source does not exist.
    #[error("source {source_id} was not found")]
    NotFound {
        /// Missing source identifier.
        source_id: SourceId,
    },
    /// A source with the same normalized WeRead book identifier exists.
    #[error("source with book_id {book_id:?} already exists")]
    BookIdConflict {
        /// Conflicting unique identity.
        book_id: String,
    },
    /// The expected feed revision no longer matches the persisted value.
    #[error("source {source_id} feed revision changed from {expected} to {actual}")]
    RevisionConflict {
        /// Source being updated.
        source_id: SourceId,
        /// Revision supplied by the caller.
        expected: FeedRevision,
        /// Revision found in PostgreSQL.
        actual: FeedRevision,
    },
    /// A domain revision cannot be represented by PostgreSQL `BIGINT`.
    #[error("source feed revision is outside the PostgreSQL BIGINT range")]
    RevisionOutOfRange,
    /// The backing PostgreSQL operation failed.
    #[error("source repository storage error: {0}")]
    Storage(String),
}

/// Pool-backed source reads.
#[async_trait::async_trait]
pub trait SourceRepository: Send + Sync {
    /// Returns all sources in deterministic display-name order.
    async fn list(&self) -> Result<Vec<Source>, SourceRepositoryError>;

    /// Finds one source by its durable identifier.
    async fn find(&self, source_id: SourceId) -> Result<Option<Source>, SourceRepositoryError>;

    /// Finds one source by its unique normalized WeRead book identifier.
    async fn find_by_book_id(&self, book_id: &str)
        -> Result<Option<Source>, SourceRepositoryError>;
}

/// PostgreSQL source reader backed by the shared pool.
#[derive(Clone)]
pub struct PostgresSourceRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresSourceRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresSourceRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresSourceRepository {
    /// Creates a source reader backed by the configured PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl SourceRepository for PostgresSourceRepository {
    async fn list(&self) -> Result<Vec<Source>, SourceRepositoryError> {
        sqlx::query(&format!(
            "SELECT {SOURCE_COLUMNS} FROM sources ORDER BY display_name ASC, id ASC"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_source)
        .collect()
    }

    async fn find(&self, source_id: SourceId) -> Result<Option<Source>, SourceRepositoryError> {
        validate_source_id(source_id)?;
        sqlx::query(&format!(
            "SELECT {SOURCE_COLUMNS} FROM sources WHERE id = $1"
        ))
        .bind(source_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(decode_source)
        .transpose()
    }

    async fn find_by_book_id(
        &self,
        book_id: &str,
    ) -> Result<Option<Source>, SourceRepositoryError> {
        let book_id = validate_book_id(book_id)?;
        sqlx::query(&format!(
            "SELECT {SOURCE_COLUMNS} FROM sources WHERE book_id = $1"
        ))
        .bind(book_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(decode_source)
        .transpose()
    }
}

/// Operations on source rows inside the shared application transaction.
#[async_trait::async_trait]
pub trait SourceTransactionRepository {
    /// Inserts a validated source at feed revision zero.
    async fn insert(&mut self, source: NewSource) -> Result<Source, SourceRepositoryError>;

    /// Changes the operator-controlled enabled flag without changing feed
    /// revision.
    async fn set_enabled(
        &mut self,
        source_id: SourceId,
        enabled: bool,
    ) -> Result<Source, SourceRepositoryError>;

    /// Applies a partial source configuration and advances feed revision when
    /// the rendered feed can change.
    async fn update(
        &mut self,
        source_id: SourceId,
        patch: SourcePatch,
    ) -> Result<Source, SourceRepositoryError>;

    /// Deletes a source and its source-owned jobs and dependent rows.
    async fn delete(&mut self, source_id: SourceId) -> Result<(), SourceRepositoryError>;

    /// Changes the scheduling gate without changing feed revision.
    async fn set_scheduling_gate(
        &mut self,
        source_id: SourceId,
        gate: SchedulingGate,
    ) -> Result<Source, SourceRepositoryError>;

    /// Finds and locks the latest source row for a finalization transaction.
    ///
    /// Source synchronization acquires a source snapshot before doing network
    /// work. Finalization must re-read the row under the transaction lock so a
    /// concurrent operator edit, especially a sync-interval change, is not
    /// followed by a schedule calculated from stale state.
    async fn find_for_update(
        &mut self,
        source_id: SourceId,
    ) -> Result<Source, SourceRepositoryError>;

    /// Updates scheduling timestamps without changing feed revision.
    async fn update_schedule(
        &mut self,
        source_id: SourceId,
        next_fetch_at: DateTime<Utc>,
        failure_cooldown_until: Option<DateTime<Utc>>,
        schedule_reserved_until: Option<DateTime<Utc>>,
    ) -> Result<Source, SourceRepositoryError>;

    /// Advances a feed revision only if `expected` is still current.
    async fn bump_feed_revision(
        &mut self,
        source_id: SourceId,
        expected: FeedRevision,
    ) -> Result<FeedRevision, SourceRepositoryError>;
}

/// Transaction-scoped PostgreSQL source view owned by
/// [`crate::persistence::unit_of_work::UnitOfWork`].
pub struct PostgresSourceTransaction<'borrow, 'pool> {
    job_transaction: &'borrow mut PostgresJobTransaction<'pool>,
}

impl<'borrow, 'pool> PostgresSourceTransaction<'borrow, 'pool> {
    /// Creates a source view over the transaction owned by the unit of work.
    pub(crate) fn new(job_transaction: &'borrow mut PostgresJobTransaction<'pool>) -> Self {
        Self { job_transaction }
    }

    fn transaction(
        &mut self,
    ) -> Result<&mut sqlx::Transaction<'pool, sqlx::Postgres>, SourceRepositoryError> {
        self.job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)
    }
}

#[async_trait::async_trait]
impl SourceTransactionRepository for PostgresSourceTransaction<'_, '_> {
    async fn insert(&mut self, source: NewSource) -> Result<Source, SourceRepositoryError> {
        let source = Source::new(source)?;
        let sync_interval_seconds = duration_seconds(source.sync_interval())?;
        let query = format!(
            r#"
            INSERT INTO sources (
                id, book_id, display_name, article_url, account_id,
                feed_revision, enabled, scheduling_gate, sync_interval_seconds,
                rss_item_limit, next_fetch_at, priority, max_attempts
            )
            VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9, $10, $11, $12)
            RETURNING {SOURCE_COLUMNS}
            "#
        );
        let transaction = self.transaction()?;
        sqlx::query(&query)
            .bind(source.id().as_uuid())
            .bind(source.book_id())
            .bind(source.display_name())
            .bind(source.article_url().map(VerifiedWechatArticleUrl::as_str))
            .bind(source.account_id().map(|account| account.as_uuid()))
            .bind(source.enabled())
            .bind(source.scheduling_gate().as_str())
            .bind(sync_interval_seconds)
            .bind(i64::from(source.rss_item_limit()))
            .bind(source.next_fetch_at())
            .bind(source.priority())
            .bind(i64::from(source.max_attempts()))
            .fetch_one(&mut **transaction)
            .await
            .map_err(|error| map_insert_error(error, source.book_id()))
            .and_then(decode_source)
    }

    async fn set_enabled(
        &mut self,
        source_id: SourceId,
        enabled: bool,
    ) -> Result<Source, SourceRepositoryError> {
        validate_source_id(source_id)?;
        let transaction = self.transaction()?;
        sqlx::query(&format!(
            "UPDATE sources SET enabled = $2, updated_at = clock_timestamp() WHERE id = $1 RETURNING {SOURCE_COLUMNS}"
        ))
        .bind(source_id.as_uuid())
        .bind(enabled)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .ok_or(SourceRepositoryError::NotFound { source_id })
            .and_then(decode_source)
    }

    async fn update(
        &mut self,
        source_id: SourceId,
        patch: SourcePatch,
    ) -> Result<Source, SourceRepositoryError> {
        validate_source_id(source_id)?;
        let transaction = self.transaction()?;
        let current = sqlx::query(&format!(
            "SELECT {SOURCE_COLUMNS} FROM sources WHERE id = $1 FOR UPDATE"
        ))
        .bind(source_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .ok_or(SourceRepositoryError::NotFound { source_id })
        .and_then(decode_source)?;
        let updated = current.apply_patch(patch)?;
        let sync_interval_seconds = duration_seconds(updated.sync_interval())?;
        let feed_revision = persisted_revision(updated.feed_revision())?;
        sqlx::query(&format!(
            "UPDATE sources SET book_id = $2, display_name = $3, article_url = $4, account_id = $5, feed_revision = $6, sync_interval_seconds = $7, rss_item_limit = $8, priority = $9, max_attempts = $10, updated_at = clock_timestamp() WHERE id = $1 RETURNING {SOURCE_COLUMNS}"
        ))
        .bind(source_id.as_uuid())
        .bind(updated.book_id())
        .bind(updated.display_name())
        .bind(updated.article_url().map(VerifiedWechatArticleUrl::as_str))
        .bind(updated.account_id().map(|account| account.as_uuid()))
        .bind(feed_revision)
        .bind(sync_interval_seconds)
        .bind(i64::from(updated.rss_item_limit()))
        .bind(updated.priority())
        .bind(i64::from(updated.max_attempts()))
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| map_update_error(error, updated.book_id()))
        .and_then(decode_source)
    }

    async fn delete(&mut self, source_id: SourceId) -> Result<(), SourceRepositoryError> {
        validate_source_id(source_id)?;
        let transaction = self.transaction()?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-capacity', 0))")
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0))")
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        let exists =
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM sources WHERE id = $1 FOR UPDATE")
                .bind(source_id.as_uuid())
                .fetch_optional(&mut **transaction)
                .await
                .map_err(storage_error)?;
        if exists.is_none() {
            return Err(SourceRepositoryError::NotFound { source_id });
        }

        // jobs.source_id intentionally has no foreign key because jobs can be
        // created before a source transaction is committed. Remove every
        // source-owned job explicitly; other source-owned tables cascade from
        // the source row below.
        sqlx::query("DELETE FROM jobs WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query("DELETE FROM sources WHERE id = $1")
            .bind(source_id.as_uuid())
            .execute(&mut **transaction)
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    async fn set_scheduling_gate(
        &mut self,
        source_id: SourceId,
        gate: SchedulingGate,
    ) -> Result<Source, SourceRepositoryError> {
        validate_source_id(source_id)?;
        let gate = gate.as_str();
        let transaction = self.transaction()?;
        sqlx::query(&format!(
            "UPDATE sources SET scheduling_gate = $2, updated_at = clock_timestamp() WHERE id = $1 RETURNING {SOURCE_COLUMNS}"
        ))
        .bind(source_id.as_uuid())
        .bind(gate)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .ok_or(SourceRepositoryError::NotFound { source_id })
        .and_then(decode_source)
    }

    async fn find_for_update(
        &mut self,
        source_id: SourceId,
    ) -> Result<Source, SourceRepositoryError> {
        validate_source_id(source_id)?;
        let transaction = self.transaction()?;
        sqlx::query(&format!(
            "SELECT {SOURCE_COLUMNS} FROM sources WHERE id = $1 FOR UPDATE"
        ))
        .bind(source_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .ok_or(SourceRepositoryError::NotFound { source_id })
        .and_then(decode_source)
    }

    async fn update_schedule(
        &mut self,
        source_id: SourceId,
        next_fetch_at: DateTime<Utc>,
        failure_cooldown_until: Option<DateTime<Utc>>,
        schedule_reserved_until: Option<DateTime<Utc>>,
    ) -> Result<Source, SourceRepositoryError> {
        validate_source_id(source_id)?;
        let transaction = self.transaction()?;
        sqlx::query(&format!(
            "UPDATE sources SET next_fetch_at = $2, failure_cooldown_until = $3, schedule_reserved_until = $4, updated_at = clock_timestamp() WHERE id = $1 RETURNING {SOURCE_COLUMNS}"
        ))
        .bind(source_id.as_uuid())
        .bind(next_fetch_at)
        .bind(failure_cooldown_until)
        .bind(schedule_reserved_until)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .ok_or(SourceRepositoryError::NotFound { source_id })
        .and_then(decode_source)
    }

    async fn bump_feed_revision(
        &mut self,
        source_id: SourceId,
        expected: FeedRevision,
    ) -> Result<FeedRevision, SourceRepositoryError> {
        validate_source_id(source_id)?;
        let expected_value = persisted_revision(expected)?;
        let transaction = self.transaction()?;
        let row = sqlx::query(
            "UPDATE sources SET feed_revision = feed_revision + 1, updated_at = clock_timestamp() WHERE id = $1 AND feed_revision = $2 RETURNING feed_revision",
        )
        .bind(source_id.as_uuid())
        .bind(expected_value)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
        if let Some(row) = row {
            return decode_revision(row);
        }

        let current = sqlx::query("SELECT feed_revision FROM sources WHERE id = $1")
            .bind(source_id.as_uuid())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
        let Some(current) = current else {
            return Err(SourceRepositoryError::NotFound { source_id });
        };
        let actual = decode_revision_value(
            current
                .try_get::<i64, _>("feed_revision")
                .map_err(storage_error)?,
        )?;
        Err(SourceRepositoryError::RevisionConflict {
            source_id,
            expected,
            actual,
        })
    }
}

fn decode_source(row: PgRow) -> Result<Source, SourceRepositoryError> {
    let source_id = SourceId::from_uuid(row.try_get("id").map_err(storage_error)?);
    let sync_interval_seconds: i64 = row
        .try_get("sync_interval_seconds")
        .map_err(storage_error)?;
    let sync_interval =
        Duration::try_seconds(sync_interval_seconds).ok_or(SourceError::InvalidSyncInterval)?;
    let rss_item_limit = u32::try_from(
        row.try_get::<i64, _>("rss_item_limit")
            .map_err(storage_error)?,
    )
    .map_err(|_| SourceError::InvalidRssItemLimit)?;
    let max_attempts = u32::try_from(
        row.try_get::<i64, _>("max_attempts")
            .map_err(storage_error)?,
    )
    .map_err(|_| SourceError::InvalidMaxAttempts)?;
    let feed_revision = u64::try_from(
        row.try_get::<i64, _>("feed_revision")
            .map_err(storage_error)?,
    )
    .map_err(|_| SourceError::InvalidRevision)?;
    let account_id = row
        .try_get::<Option<Uuid>, _>("account_id")
        .map_err(storage_error)?
        .map(crate::domain::credentials::WeReadAccountId::from_uuid);
    Source::from_parts(SourceParts {
        id: source_id,
        book_id: row.try_get("book_id").map_err(storage_error)?,
        display_name: row.try_get("display_name").map_err(storage_error)?,
        article_url: row
            .try_get::<Option<String>, _>("article_url")
            .map_err(storage_error)?
            .map(|value| {
                value
                    .parse::<VerifiedWechatArticleUrl>()
                    .map_err(SourceRepositoryError::Domain)
            })
            .transpose()?,
        enabled: row.try_get("enabled").map_err(storage_error)?,
        sync_interval,
        rss_item_limit,
        account_id,
        scheduling_gate: row
            .try_get::<String, _>("scheduling_gate")
            .map_err(storage_error)?
            .parse::<SchedulingGate>()?,
        feed_revision: FeedRevision::from_u64(feed_revision),
        next_fetch_at: row.try_get("next_fetch_at").map_err(storage_error)?,
        failure_cooldown_until: row
            .try_get("failure_cooldown_until")
            .map_err(storage_error)?,
        schedule_reserved_until: row
            .try_get("schedule_reserved_until")
            .map_err(storage_error)?,
        priority: row.try_get("priority").map_err(storage_error)?,
        max_attempts,
    })
    .map_err(SourceRepositoryError::Domain)
}

fn decode_revision(row: PgRow) -> Result<FeedRevision, SourceRepositoryError> {
    let value = row
        .try_get::<i64, _>("feed_revision")
        .map_err(storage_error)?;
    decode_revision_value(value)
}

fn decode_revision_value(value: i64) -> Result<FeedRevision, SourceRepositoryError> {
    u64::try_from(value)
        .map(FeedRevision::from_u64)
        .map_err(|_| SourceRepositoryError::Domain(SourceError::InvalidRevision))
}

fn validate_source_id(source_id: SourceId) -> Result<(), SourceRepositoryError> {
    if source_id.as_uuid().is_nil() {
        Err(SourceRepositoryError::Domain(SourceError::InvalidId))
    } else {
        Ok(())
    }
}

fn validate_book_id(book_id: &str) -> Result<&str, SourceRepositoryError> {
    if book_id.trim().is_empty() {
        Err(SourceRepositoryError::Domain(SourceError::EmptyBookId))
    } else {
        Ok(book_id.trim())
    }
}

fn duration_seconds(duration: Duration) -> Result<i64, SourceRepositoryError> {
    if duration != Duration::seconds(duration.num_seconds()) {
        return Err(SourceRepositoryError::Domain(
            SourceError::InvalidSyncInterval,
        ));
    }
    Ok(duration.num_seconds())
}

fn persisted_revision(revision: FeedRevision) -> Result<i64, SourceRepositoryError> {
    i64::try_from(revision.as_u64()).map_err(|_| SourceRepositoryError::RevisionOutOfRange)
}

fn map_insert_error(error: sqlx::Error, book_id: &str) -> SourceRepositoryError {
    if let sqlx::Error::Database(database_error) = &error {
        if database_error.constraint() == Some("sources_book_id_idx") {
            return SourceRepositoryError::BookIdConflict {
                book_id: book_id.to_owned(),
            };
        }
    }
    storage_error(error)
}

fn map_update_error(error: sqlx::Error, book_id: &str) -> SourceRepositoryError {
    if let sqlx::Error::Database(database_error) = &error {
        if database_error.constraint() == Some("sources_book_id_idx") {
            return SourceRepositoryError::BookIdConflict {
                book_id: book_id.to_owned(),
            };
        }
    }
    storage_error(error)
}

fn job_transaction_error(error: JobRepositoryError) -> SourceRepositoryError {
    SourceRepositoryError::Storage(error.to_string())
}

fn storage_error(error: impl fmt::Display) -> SourceRepositoryError {
    SourceRepositoryError::Storage(error.to_string())
}
