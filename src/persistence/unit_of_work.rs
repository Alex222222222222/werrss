//! Shared PostgreSQL unit-of-work boundary.
//!
//! Purpose: make article/archive mutations, source revision and schedule
//! changes, sync-run persistence, feed-cache replacement, and fenced job
//! completion atomic without exposing SQLx transactions to application code.
//!
//! `UnitOfWorkFactory::begin` creates one short-lived SQLx transaction. Its
//! returned handle exposes transaction-scoped job, source, article, asset,
//! sync-run, and feed-cache views today. Only the unit of work can commit; dropping it
//! or returning an error rolls all component writes back. Repository views
//! borrow the unit of work and therefore cannot outlive or independently commit
//! their transaction.
//!
//! Minimum executable contract:
//!
//! - `UnitOfWorkFactory::begin()` creates the transaction;
//! - `jobs()` remains the interim compatibility view;
//! - `job_outcomes()` borrows only the transaction-scoped worker-outcome port;
//! - `job_enqueue()` borrows only the transaction-scoped enqueue port;
//! - `source()` borrows the transaction-scoped source mutation view;
//! - `articles()` borrows the transaction-scoped article mutation view;
//! - `assets(policy)` borrows the transaction-scoped asset storage view;
//! - `feed_cache()` borrows the transaction-scoped feed-cache publication view;
//! - `database_now()` samples the PostgreSQL clock for persisted ordering
//!   timestamps;
//! - `commit(self)` is the only successful exit for a completed unit of work;
//! - `rollback(self)` is available for explicit cleanup in tests or callers
//!   that need to await rollback; and
//! - archive metadata acquisition and URL rewriting use `assets(policy)` while
//!   borrowing this same transaction; PostgreSQL blob operations share it,
//!   while external allocations and evictions are finalized at the unit
//!   commit boundary; and
//! - article upserts return feed-visible change information so the caller can
//!   decide whether to bump the source revision in this same transaction.
//!
//! Retry, deferral, cancellation, and failure outcomes use the same boundary
//! because they record sync results or alter source scheduling gates/cooldowns.
//! Queue-only claim, heartbeat, and read operations remain short independent
//! transactions. Enqueueing can be independent for external requests, or can
//! use `job_enqueue()` when another aggregate (such as a newly created source)
//! must be published atomically with its job. Expired-lease recovery is a
//! dedicated atomic persistence operation because exhausting a failure budget
//! may also update the source cooldown.
//!
//! Synchronization data flow:
//!
//! 1. perform browser/network acquisition and normalization without a database
//!    transaction;
//! 2. keep the job lease alive through a separate pool connection;
//! 3. begin a unit of work and, when assets are present, preflight their
//!    checksums/raw-byte candidates before source/article row locks;
//! 4. verify the job owner, fencing token, and live lease, then verify the
//!    expected base feed revision and persist idempotent article/archive
//!    changes, and advance to the candidate revision when feed-visible data
//!    changed;
//! 5. persist an already-rendered cache payload only for that exact revision,
//!    verify/release the feed-build lease, update the sync run and source
//!    schedule/gate, and mark the job successful;
//! 6. commit once.
//!
//! Non-responsibilities: browser calls, sleeping, rendering large documents
//! while locks are held, heartbeat task ownership, retry policy, or HTTP error
//! mapping. A unit of work must use bounded lock/statement timeouts and should
//! be short enough that a worker can safely heartbeat independently.
//!
//! High availability: the job view already participates in the shared
//! transaction, so future fenced job outcomes can commit together with business
//! writes. If ownership is lost, the unit of work rolls back instead of
//! publishing writes from a stale worker. Cache compare-and-swap and feed-build
//! fencing checks happen inside this transaction through the feed-cache view.

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use thiserror::Error;

use crate::{
    archive::asset_store::{
        AssetBlobStore, AssetCachePolicy, AssetInput, AssetRefreshPolicy, AssetRepairPolicy,
    },
    domain::job::{Job, NewJob},
};

use super::repositories::{
    article_repository::PostgresArticleTransaction,
    asset_repository::{
        drain_blob_cleanup_queue, drain_blob_deletion_queue, prepare_asset_batch,
        AssetRepositoryError, PostgresAssetBlobStore, PostgresAssetTransaction,
    },
    feed_cache_repository::PostgresFeedCacheTransaction,
    job_repository::PostgresJobTransaction,
    job_repository::{
        EnqueueResult, JobEnqueueTransaction, JobOutcome, JobOutcomeTransaction, JobRepositoryError,
    },
    source_repository::PostgresSourceTransaction,
    sync_run_repository::PostgresSyncRunTransaction,
};

/// Errors raised while opening or completing a unit of work.
#[derive(Debug, Error)]
pub enum UnitOfWorkError {
    /// SQLx could not start, commit, or roll back the transaction.
    #[error("unit of work transaction error: {0}")]
    Transaction(#[source] sqlx::Error),
    /// Asset digest/collision preflight could not complete before persistence.
    #[error("asset preflight error: {0}")]
    AssetPreparation(#[source] AssetRepositoryError),
    /// Binary cleanup after a failed unit-of-work boundary.
    #[error("asset blob cleanup error: {0}")]
    AssetCleanup(String),
    /// SQLx could not sample the authoritative PostgreSQL clock.
    #[error("unit of work database clock error: {0}")]
    DatabaseClock(#[source] sqlx::Error),
}

/// Factory for short-lived, shared persistence transactions.
#[derive(Clone)]
pub struct UnitOfWorkFactory {
    pool: PgPool,
    blob_store: Arc<dyn AssetBlobStore>,
    refresh_policy: AssetRefreshPolicy,
    repair_policy: AssetRepairPolicy,
}

impl fmt::Debug for UnitOfWorkFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnitOfWorkFactory")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl UnitOfWorkFactory {
    /// Creates a factory backed by the configured PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self::with_blob_store(pool.clone(), Arc::new(PostgresAssetBlobStore::new(pool)))
    }

    /// Creates a factory with an injected binary asset backend.
    pub fn with_blob_store(pool: PgPool, blob_store: Arc<dyn AssetBlobStore>) -> Self {
        Self {
            pool,
            blob_store,
            refresh_policy: AssetRefreshPolicy::default(),
            repair_policy: AssetRepairPolicy::default(),
        }
    }

    /// Uses the same lineage limits as refresh admission for terminal outcomes.
    pub const fn with_refresh_policy(mut self, policy: AssetRefreshPolicy) -> Self {
        self.refresh_policy = policy;
        self
    }

    /// Uses the configured global recovery bound for complete acquisitions.
    pub const fn with_repair_policy(mut self, policy: AssetRepairPolicy) -> Self {
        self.repair_policy = policy;
        self
    }

    /// Begins a transaction whose repository views share one commit boundary.
    pub async fn begin(&self) -> Result<UnitOfWork<'_>, UnitOfWorkError> {
        self.begin_with_assets_inner(&[], false, false).await
    }

    /// Begins a transaction after preflighting asset digests and raw-byte
    /// deduplication. This must be called before source/article row locks are
    /// acquired when the unit of work will store asset inputs.
    pub async fn begin_with_assets(
        &self,
        inputs: &[AssetInput],
    ) -> Result<UnitOfWork<'_>, UnitOfWorkError> {
        self.begin_with_assets_inner(inputs, false, false).await
    }

    /// Begins an article acquisition transaction before source/article locks,
    /// serializing lineage reconciliation with refresh admission.
    pub async fn begin_article_acquisition_with_assets(
        &self,
        inputs: &[AssetInput],
    ) -> Result<UnitOfWork<'_>, UnitOfWorkError> {
        self.begin_with_assets_inner(inputs, true, true).await
    }

    /// Begins a refresh transaction after reserving asset capacity, even when
    /// the acquisition produced no asset inputs. Refresh finalization takes
    /// source/article row locks later, so capacity must be first in that order.
    pub async fn begin_refresh_with_assets(
        &self,
        inputs: &[AssetInput],
    ) -> Result<UnitOfWork<'_>, UnitOfWorkError> {
        self.begin_with_assets_inner(inputs, true, false).await
    }

    async fn begin_with_assets_inner(
        &self,
        inputs: &[AssetInput],
        reserve_capacity: bool,
        reserve_recovery: bool,
    ) -> Result<UnitOfWork<'_>, UnitOfWorkError> {
        tracing::trace!("beginning PostgreSQL unit of work");
        let mut jobs = PostgresJobTransaction::begin(&self.pool)
            .await
            .map_err(UnitOfWorkError::Transaction)?;
        jobs.set_refresh_policy(self.refresh_policy);
        let transaction = jobs.transaction_mut().map_err(|error| {
            UnitOfWorkError::AssetPreparation(AssetRepositoryError::Storage(error.to_string()))
        })?;
        if reserve_capacity {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-capacity', 0))")
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    UnitOfWorkError::AssetPreparation(AssetRepositoryError::Storage(
                        error.to_string(),
                    ))
                })?;
        }
        if reserve_recovery {
            sqlx::query(
                "SELECT pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0))",
            )
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                UnitOfWorkError::AssetPreparation(AssetRepositoryError::Storage(error.to_string()))
            })?;
        }
        let asset_preparation = prepare_asset_batch(self.blob_store.as_ref(), transaction, inputs)
            .await
            .map_err(UnitOfWorkError::AssetPreparation)?;
        let result = Ok(UnitOfWork {
            jobs: Some(jobs),
            asset_preparation: Some(asset_preparation),
            blob_store: Arc::clone(&self.blob_store),
            pool: self.pool.clone(),
            mutation_state: Arc::new(Mutex::new(Default::default())),
            finished: Arc::new(AtomicBool::new(false)),
            repair_policy: self.repair_policy,
        });
        if let Err(error) = &result {
            tracing::warn!(error = %error, "unable to begin PostgreSQL unit of work");
        }
        result
    }

    /// Samples PostgreSQL's wall clock without opening a long-lived transaction.
    ///
    /// Application services use this for persisted timestamps that participate
    /// in cross-replica ordering. The query intentionally uses
    /// `clock_timestamp()` rather than a process-local clock.
    pub async fn database_now(&self) -> Result<DateTime<Utc>, UnitOfWorkError> {
        let result = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await
            .map_err(UnitOfWorkError::DatabaseClock);
        if let Err(error) = &result {
            tracing::warn!(error = %error, "unable to read PostgreSQL authoritative clock");
        }
        result
    }
}

/// A short-lived transaction-scoped persistence unit.
pub struct UnitOfWork<'a> {
    jobs: Option<PostgresJobTransaction<'a>>,
    asset_preparation: Option<super::repositories::asset_repository::AssetBatchPreparation>,
    blob_store: Arc<dyn AssetBlobStore>,
    pool: PgPool,
    mutation_state: Arc<Mutex<super::repositories::asset_repository::AssetMutationState>>,
    finished: Arc<AtomicBool>,
    repair_policy: AssetRepairPolicy,
}

/// Outcome-only view over the job transaction owned by [`UnitOfWork`].
///
/// This wrapper intentionally exposes no queue, recovery, or commit operation.
/// A worker can apply one fenced [`JobOutcome`] through it, then the enclosing
/// unit of work can persist related state and commit everything together.
pub struct JobOutcomeView<'u, 'a> {
    transaction: &'u mut PostgresJobTransaction<'a>,
}

/// Enqueue-only view over the job transaction owned by [`UnitOfWork`].
///
/// Source creation uses this capability to persist its initial sync job in the
/// same transaction as the source row. The wrapper intentionally does not
/// expose worker claims, outcomes, recovery, or commit.
pub struct JobEnqueueView<'u, 'a> {
    transaction: &'u mut PostgresJobTransaction<'a>,
}

#[async_trait::async_trait]
impl JobEnqueueTransaction for JobEnqueueView<'_, '_> {
    async fn enqueue_job(&mut self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError> {
        JobEnqueueTransaction::enqueue_job(&mut *self.transaction, spec).await
    }
}

#[async_trait::async_trait]
impl JobOutcomeTransaction for JobOutcomeView<'_, '_> {
    async fn apply_outcome(&mut self, outcome: JobOutcome) -> Result<Job, JobRepositoryError> {
        JobOutcomeTransaction::apply_outcome(&mut *self.transaction, outcome).await
    }
}

#[async_trait::async_trait]
impl JobOutcomeTransaction for UnitOfWork<'_> {
    async fn apply_outcome(&mut self, outcome: JobOutcome) -> Result<Job, JobRepositoryError> {
        self.job_outcomes().apply_outcome(outcome).await
    }
}

impl<'a> UnitOfWork<'a> {
    /// Borrows the job repository view without exposing an independent commit.
    pub fn jobs(&mut self) -> &mut PostgresJobTransaction<'a> {
        self.jobs
            .as_mut()
            .expect("unit-of-work transaction is closed")
    }

    /// Borrows the outcome-only job view without exposing a commit operation.
    ///
    /// The returned opaque view can apply a fenced [`JobOutcome`] while this
    /// unit of work remains open. Callers should persist related business
    /// changes through the other views and then call [`Self::commit`] once.
    pub fn job_outcomes(&mut self) -> JobOutcomeView<'_, 'a> {
        JobOutcomeView {
            transaction: self
                .jobs
                .as_mut()
                .expect("unit-of-work transaction is closed"),
        }
    }

    /// Borrows the enqueue-only job view without exposing a commit operation.
    pub fn job_enqueue(&mut self) -> JobEnqueueView<'_, 'a> {
        JobEnqueueView {
            transaction: self
                .jobs
                .as_mut()
                .expect("unit-of-work transaction is closed"),
        }
    }

    /// Borrows the transaction-scoped article mutation view.
    pub fn articles(&mut self) -> PostgresArticleTransaction<'_, 'a> {
        PostgresArticleTransaction::new(
            self.jobs
                .as_mut()
                .expect("unit-of-work transaction is closed"),
        )
    }

    /// Borrows the transaction-scoped binary asset mutation view.
    pub fn assets(&mut self, policy: AssetCachePolicy) -> PostgresAssetTransaction<'_, 'a> {
        PostgresAssetTransaction::new(
            self.jobs
                .as_mut()
                .expect("unit-of-work transaction is closed"),
            Arc::clone(&self.blob_store),
            self.asset_preparation.clone(),
            self.repair_policy,
            policy,
            Arc::clone(&self.mutation_state),
        )
    }

    /// Borrows the transaction-scoped feed-cache publication view.
    pub fn feed_cache(&mut self) -> PostgresFeedCacheTransaction<'_, 'a> {
        PostgresFeedCacheTransaction::new(
            self.jobs
                .as_mut()
                .expect("unit-of-work transaction is closed"),
        )
    }

    /// Borrows the transaction-scoped source mutation view.
    pub fn source(&mut self) -> PostgresSourceTransaction<'_, 'a> {
        PostgresSourceTransaction::new(
            self.jobs
                .as_mut()
                .expect("unit-of-work transaction is closed"),
        )
    }

    /// Borrows the transaction-scoped synchronization-run view.
    pub fn sync_runs(&mut self) -> PostgresSyncRunTransaction<'_, 'a> {
        PostgresSyncRunTransaction::new(
            self.jobs
                .as_mut()
                .expect("unit-of-work transaction is closed"),
        )
    }

    /// Commits all mutations made through this unit of work.
    pub async fn commit(self) -> Result<(), UnitOfWorkError> {
        let mut this = self;
        let mut jobs = this
            .jobs
            .take()
            .expect("unit-of-work transaction is closed");
        let evictions = this
            .mutation_state
            .lock()
            .unwrap()
            .deferred_evictions
            .clone();
        if !evictions.is_empty() {
            let transaction = jobs
                .transaction_mut()
                .map_err(|error| UnitOfWorkError::AssetCleanup(error.to_string()))?;
            for blob_id in evictions {
                sqlx::query(
                    "INSERT INTO asset_blob_deletion_queue (blob_id)
                     VALUES ($1) ON CONFLICT (blob_id) DO NOTHING",
                )
                .bind(blob_id)
                .execute(&mut **transaction)
                .await
                .map_err(UnitOfWorkError::Transaction)?;
            }
        }
        // From this point PostgreSQL may have committed even when the client
        // receives an error, so Drop must not attempt destructive cleanup.
        this.finished.store(true, Ordering::Release);
        let result = jobs
            .commit_inner()
            .await
            .map_err(UnitOfWorkError::Transaction);
        if result.is_ok() {
            drain_blob_deletion_queue(&this.pool, this.blob_store.as_ref())
                .await
                .map_err(|error| UnitOfWorkError::AssetCleanup(error.to_string()))?;
            drain_blob_cleanup_queue(&this.pool, this.blob_store.as_ref())
                .await
                .map_err(|error| UnitOfWorkError::AssetCleanup(error.to_string()))?;
        }
        match &result {
            Ok(()) => tracing::trace!("committed PostgreSQL unit of work"),
            Err(error) => {
                tracing::warn!(error = %error, "failed to commit PostgreSQL unit of work")
            }
        }
        result
    }

    /// Explicitly rolls back all mutations made through this unit of work.
    pub async fn rollback(self) -> Result<(), UnitOfWorkError> {
        self.finished.store(true, Ordering::Release);
        let mut this = self;
        let jobs = this
            .jobs
            .take()
            .expect("unit-of-work transaction is closed");
        let result = jobs
            .rollback_inner()
            .await
            .map_err(UnitOfWorkError::Transaction);
        cleanup_external_blobs(&this.pool, &this.blob_store, &this.mutation_state).await?;
        match &result {
            Ok(()) => tracing::trace!("rolled back PostgreSQL unit of work"),
            Err(error) => {
                tracing::warn!(error = %error, "failed to roll back PostgreSQL unit of work")
            }
        }
        result
    }
}

async fn cleanup_external_blobs(
    pool: &PgPool,
    blob_store: &Arc<dyn AssetBlobStore>,
    mutation_state: &Arc<Mutex<super::repositories::asset_repository::AssetMutationState>>,
) -> Result<(), UnitOfWorkError> {
    let mut remaining = std::mem::take(&mut mutation_state.lock().unwrap().newly_written);
    let mut first_error = None;
    // Retry transient object-store failures while the rollback boundary is
    // still active; retain any persistent failures in the shared state.
    for _ in 0..2 {
        let mut failed = Vec::new();
        for blob_id in remaining {
            if let Err(error) = blob_store
                .delete(&crate::archive::asset_store::AssetBlobKey::for_blob_id(
                    blob_id,
                ))
                .await
            {
                first_error.get_or_insert_with(|| error.to_string());
                failed.push(blob_id);
            }
        }
        if failed.is_empty() {
            return Ok(());
        }
        remaining = failed;
    }
    mutation_state.lock().unwrap().newly_written = remaining;
    let queued = mutation_state.lock().unwrap().newly_written.clone();
    for blob_id in queued {
        sqlx::query(
            "INSERT INTO asset_blob_cleanup_queue (blob_id)
             VALUES ($1) ON CONFLICT (blob_id) DO NOTHING",
        )
        .bind(blob_id)
        .execute(pool)
        .await
        .map_err(UnitOfWorkError::Transaction)?;
    }
    Err(UnitOfWorkError::AssetCleanup(
        first_error.unwrap_or_else(|| "asset blob cleanup failed".to_owned()),
    ))
}

impl fmt::Debug for UnitOfWork<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnitOfWork")
            .field("jobs", &self.jobs)
            .finish()
    }
}

impl Drop for UnitOfWork<'_> {
    fn drop(&mut self) {
        if self.finished.load(Ordering::Acquire)
            || self.blob_store.as_any().is::<PostgresAssetBlobStore>()
            || self.mutation_state.lock().unwrap().newly_written.is_empty()
        {
            return;
        }

        let pool = self.pool.clone();
        let blob_store = Arc::clone(&self.blob_store);
        let mutation_state = Arc::clone(&self.mutation_state);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(error) = cleanup_external_blobs(&pool, &blob_store, &mutation_state).await {
                    tracing::warn!(error = %error, "unable to clean up blobs from implicitly rolled back unit of work");
                }
            });
        } else {
            tracing::warn!(
                "implicitly rolled back unit of work with external blobs outside an async runtime"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sqlx::test(migrator = "crate::persistence::postgres::MIGRATOR")]
    async fn empty_article_acquisition_reserves_capacity_and_recovery_before_rows(pool: PgPool) {
        let factory = UnitOfWorkFactory::new(pool.clone());
        let uow = factory
            .begin_article_acquisition_with_assets(&[])
            .await
            .unwrap();
        let mut probe = pool.begin().await.unwrap();
        for lock in ["asset-capacity", "asset-repair-admission"] {
            assert!(
                !sqlx::query_scalar::<_, bool>(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
                )
                .bind(lock)
                .fetch_one(&mut *probe)
                .await
                .unwrap(),
                "{lock} must be reserved before caller row locks"
            );
        }
        uow.rollback().await.unwrap();
        for lock in ["asset-capacity", "asset-repair-admission"] {
            assert!(sqlx::query_scalar::<_, bool>(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
            )
            .bind(lock)
            .fetch_one(&mut *probe)
            .await
            .unwrap());
        }
        probe.rollback().await.unwrap();
    }
}

// TODO(design): add the remaining verify-fence and business-coupled source,
// article, sync-run, and archive commands behind this boundary; and prevent
// SyncService from receiving a job-only commit API.
