//! One-pass durable job execution.
//!
//! This module is the executable worker unit underneath future role-specific
//! runtime composition. [`Worker::run_once`] claims one allowed job, runs a
//! handler while heartbeating its lease, then applies the handler's typed
//! outcome through a caller-owned transaction and commits that transaction
//! exactly once. Atomic business handlers may instead commit the job and their
//! business data together and return [`JobExecution::Committed`].
//! [`Worker::run_until_shutdown`] repeats that pass with bounded idle polling
//! and transient-error backoff.
//!
//! Responsibilities:
//!
//! - define the small handler and outcome contracts used by worker dispatch;
//! - keep a claimed lease alive while a handler is pending;
//! - stop polling the handler when a heartbeat loses ownership;
//! - recover expired leases before claiming new work;
//! - bind success, deferral, retry, and permanent failure to the current
//!   fencing token; and
//! - make idle passes and transaction failures observable to the polling loop;
//! - honor shutdown without abandoning a pass already holding a lease; and
//! - avoid sleeping between successful jobs so a queued backlog drains quickly.
//!
//! Non-responsibilities: deciding retry backoff, selecting browser selectors,
//! fetching WeChat content, rendering RSS, or serving HTTP. The loop only
//! supplies bounded polling and error waits; runtime composition still owns
//! role selection, concurrency across multiple workers, and metrics. A handler
//! must perform acquisition outside its outcome transaction and return a
//! [`JobExecution`] that already contains the chosen retry/defer time.
//!
//! PostgreSQL/high-availability behavior: claim, heartbeat, and fencing are
//! delegated to [`JobService`], so replicas still coordinate through
//! PostgreSQL `SKIP LOCKED` and lease tokens. The worker can commit an outcome
//! through either the compatibility queue transaction or the shared
//! [`UnitOfWork`] boundary. A future synchronization-specific handler must add
//! a two-phase handler contract so acquisition stays outside the transaction
//! while article, source, sync, cache, and job writes commit together in that
//! same unit of work. If a heartbeat fails, the handler future is cancelled and
//! no stale outcome is attempted.
//!
//! RSS-cache interaction: this module does not render, read, or publish feeds.
//! The feed-rebuild handler uses the committed-outcome path so cache
//! publication and job completion remain one transaction. Synchronization
//! handlers still need a two-phase contract to stage their article/source/
//! sync-run writes in the same [`UnitOfWork`]. Queue-only completion must not
//! be used to publish business data separately from the job outcome.

use std::{sync::Arc, time::Duration as StdDuration};

use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::time::{self, Instant, MissedTickBehavior};

use crate::{
    application::job_service::{JobService, JobServiceError},
    domain::job::{Job, JobType},
    persistence::repositories::job_repository::{
        ExpiredJobRecovery, JobLease, JobOutcomeTransaction, JobQueue, JobRepository,
        JobRepositoryError, JobRepositoryTransaction,
    },
    persistence::unit_of_work::{UnitOfWork, UnitOfWorkError, UnitOfWorkFactory},
};

/// The durable result selected by a claimed job handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobExecution {
    /// The handler completed and its queue outcome may be persisted.
    Succeeded,
    /// The handler completed the job and its atomic business transaction.
    ///
    /// The worker does not apply another outcome for this variant. It reads
    /// the committed job snapshot after the handler returns so a handler such
    /// as feed rebuild can commit business data and job state together.
    Committed,
    /// The handler reached a non-failure eligibility boundary.
    Deferred {
        /// Next instant at which the job may be claimed.
        resume_at: DateTime<Utc>,
    },
    /// The handler encountered a retryable failure.
    Retry {
        /// Earliest instant at which another worker may retry the job.
        retry_at: DateTime<Utc>,
        /// Bounded, secret-free diagnostic summary.
        error: String,
    },
    /// The handler reached a terminal failure condition.
    Failed {
        /// Bounded, secret-free diagnostic summary.
        error: String,
    },
}

/// Handler capability used by one worker pass.
#[async_trait::async_trait]
pub trait JobHandler: Send + Sync {
    /// Runs one claimed job at the supplied compatibility/test timestamp.
    ///
    /// Most handlers return an outcome for the worker to commit. A handler
    /// returning [`JobExecution::Committed`] may own the shared transaction
    /// itself when business data and job completion must be atomic.
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution;
}

/// Readiness gate for jobs whose handler requires the browser sidecar.
pub trait BrowserJobReadiness: Send + Sync {
    /// Returns whether browser-backed jobs may be claimed now.
    fn browser_jobs_allowed(&self) -> bool;
}

/// Validated heartbeat and dispatch policy for one worker pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    allowed_job_types: Vec<JobType>,
    heartbeat_every: StdDuration,
    execution_timeout: StdDuration,
}

impl WorkerConfig {
    /// Default maximum wall-clock duration for one claimed job execution.
    pub const DEFAULT_EXECUTION_TIMEOUT: StdDuration = StdDuration::from_secs(30 * 60);

    /// Creates a worker policy. An empty allowed-kind list is rejected so a
    /// configured worker cannot appear healthy while it can never claim work.
    /// The default execution timeout is thirty minutes.
    pub fn new(
        allowed_job_types: impl Into<Vec<JobType>>,
        heartbeat_every: StdDuration,
    ) -> Result<Self, WorkerConfigError> {
        Self::new_with_execution_timeout(
            allowed_job_types,
            heartbeat_every,
            Self::DEFAULT_EXECUTION_TIMEOUT,
        )
    }

    /// Creates a worker policy with an explicit per-attempt execution timeout.
    ///
    /// The timeout is an absolute wall-clock deadline for the handler. Lease
    /// heartbeats keep the durable job claim alive, but do not extend this
    /// deadline.
    pub fn new_with_execution_timeout(
        allowed_job_types: impl Into<Vec<JobType>>,
        heartbeat_every: StdDuration,
        execution_timeout: StdDuration,
    ) -> Result<Self, WorkerConfigError> {
        let allowed_job_types = allowed_job_types.into();
        if allowed_job_types.is_empty() {
            return Err(WorkerConfigError::NoAllowedJobTypes);
        }
        if heartbeat_every.is_zero() {
            return Err(WorkerConfigError::InvalidHeartbeat);
        }
        if execution_timeout.is_zero() {
            return Err(WorkerConfigError::InvalidExecutionTimeout);
        }
        Ok(Self {
            allowed_job_types,
            heartbeat_every,
            execution_timeout,
        })
    }

    /// Returns the job kinds this worker may claim.
    pub fn allowed_job_types(&self) -> &[JobType] {
        &self.allowed_job_types
    }

    pub(crate) fn allowed_job_types_mut(&mut self) -> &mut Vec<JobType> {
        &mut self.allowed_job_types
    }

    /// Returns the interval between lease heartbeats.
    pub const fn heartbeat_every(&self) -> StdDuration {
        self.heartbeat_every
    }

    /// Returns the absolute wall-clock limit for one handler execution.
    pub const fn execution_timeout(&self) -> StdDuration {
        self.execution_timeout
    }

    fn validate_for_lease(&self, lease_for: chrono::Duration) -> Result<(), WorkerConfigError> {
        let lease_for = lease_for
            .to_std()
            .map_err(|_| WorkerConfigError::InvalidLease)?;
        if self.heartbeat_every >= lease_for {
            return Err(WorkerConfigError::HeartbeatNotShorterThanLease);
        }
        Ok(())
    }
}

/// Invalid worker runtime policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WorkerConfigError {
    /// A worker with no allowed kinds can never make progress.
    #[error("worker must allow at least one job type")]
    NoAllowedJobTypes,
    /// The heartbeat interval must be positive.
    #[error("worker heartbeat interval must be positive")]
    InvalidHeartbeat,
    /// The execution timeout must be positive.
    #[error("worker execution timeout must be positive")]
    InvalidExecutionTimeout,
    /// The configured lease could not be converted to a standard duration.
    #[error("worker lease duration is invalid")]
    InvalidLease,
    /// A heartbeat at or after expiry cannot protect the lease.
    #[error("worker heartbeat interval must be shorter than the job lease")]
    HeartbeatNotShorterThanLease,
}

/// Validated polling policy for the shutdown-aware worker loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerLoopConfig {
    idle_poll_interval: StdDuration,
    error_backoff: StdDuration,
}

impl WorkerLoopConfig {
    /// Creates a loop policy with positive idle and error waits.
    pub fn new(
        idle_poll_interval: StdDuration,
        error_backoff: StdDuration,
    ) -> Result<Self, WorkerLoopConfigError> {
        if idle_poll_interval.is_zero() {
            return Err(WorkerLoopConfigError::InvalidIdlePollInterval);
        }
        if error_backoff.is_zero() {
            return Err(WorkerLoopConfigError::InvalidErrorBackoff);
        }
        Ok(Self {
            idle_poll_interval,
            error_backoff,
        })
    }

    /// Returns the delay after an idle claim pass.
    pub const fn idle_poll_interval(self) -> StdDuration {
        self.idle_poll_interval
    }

    /// Returns the delay after a transient worker error.
    pub const fn error_backoff(self) -> StdDuration {
        self.error_backoff
    }
}

/// Invalid worker-loop timing policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WorkerLoopConfigError {
    /// Idle loops must yield to avoid a database hot loop.
    #[error("worker idle poll interval must be positive")]
    InvalidIdlePollInterval,
    /// Repeated queue/lease errors must be rate limited.
    #[error("worker error backoff must be positive")]
    InvalidErrorBackoff,
}

/// Counters returned when a worker loop observes shutdown.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkerLoopStats {
    /// Number of one-pass attempts, including idle and failed attempts.
    pub passes: u64,
    /// Number of jobs whose outcome transaction committed.
    pub completed: u64,
    /// Number of passes that found no claimable job.
    pub idle: u64,
    /// Number of transient queue, heartbeat, or transaction errors.
    pub errors: u64,
}

/// Errors raised by one worker pass.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Claiming or applying an outcome through the job service failed.
    #[error("worker job operation failed: {0}")]
    Job(#[source] JobServiceError),
    /// A lease heartbeat failed; the handler has been cancelled.
    #[error("worker lease heartbeat failed: {0}")]
    Heartbeat(#[source] JobServiceError),
    /// The outcome transaction could not begin or commit.
    #[error("worker outcome transaction failed: {0}")]
    Transaction(#[source] WorkerPersistenceError),
}

/// Persistence failures while opening or committing an outcome transaction.
#[derive(Debug, Error)]
pub enum WorkerPersistenceError {
    /// The compatibility job repository rejected the transaction operation.
    #[error(transparent)]
    JobRepository(#[from] JobRepositoryError),
    /// The shared cross-repository transaction could not begin or commit.
    #[error(transparent)]
    UnitOfWork(#[from] UnitOfWorkError),
}

/// Result of one call to [`Worker::run_once`].
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerRun {
    /// No job matched the configured allowed kinds at the requested instant.
    Idle,
    /// One job completed its handler and durable outcome transaction.
    Completed {
        /// Job snapshot after the durable outcome transaction.
        job: Box<Job>,
        /// Outcome selected by the handler.
        outcome: JobExecution,
    },
}

/// Transaction that can apply and commit one worker outcome.
#[async_trait::async_trait]
pub trait WorkerOutcomeTransaction: JobOutcomeTransaction {
    /// Commits all changes made through this outcome transaction.
    async fn commit(self) -> Result<(), WorkerPersistenceError>
    where
        Self: Sized;
}

/// Factory for transaction-scoped worker outcomes.
#[async_trait::async_trait]
pub trait WorkerOutcomeFactory: Send + Sync {
    /// Transaction type borrowed from this factory's persistence backend.
    type Transaction<'a>: WorkerOutcomeTransaction + Send + 'a
    where
        Self: 'a;

    /// Begins a transaction without committing it.
    async fn begin(&self) -> Result<Self::Transaction<'_>, WorkerPersistenceError>;
}

#[async_trait::async_trait]
impl<T> WorkerOutcomeTransaction for T
where
    T: JobRepositoryTransaction + Send,
{
    async fn commit(self) -> Result<(), WorkerPersistenceError> {
        JobRepositoryTransaction::commit(self)
            .await
            .map_err(WorkerPersistenceError::from)
    }
}

#[async_trait::async_trait]
impl WorkerOutcomeTransaction for UnitOfWork<'_> {
    async fn commit(self) -> Result<(), WorkerPersistenceError> {
        UnitOfWork::commit(self)
            .await
            .map_err(WorkerPersistenceError::from)
    }
}

#[async_trait::async_trait]
impl<R> WorkerOutcomeFactory for R
where
    R: JobRepository,
{
    type Transaction<'a>
        = R::Transaction<'a>
    where
        R: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, WorkerPersistenceError> {
        JobRepository::begin(self)
            .await
            .map_err(WorkerPersistenceError::from)
    }
}

#[async_trait::async_trait]
impl WorkerOutcomeFactory for UnitOfWorkFactory {
    type Transaction<'a>
        = UnitOfWork<'a>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, WorkerPersistenceError> {
        UnitOfWorkFactory::begin(self)
            .await
            .map_err(WorkerPersistenceError::from)
    }
}

/// Executes one claimed job against the supplied handler.
pub struct Worker<Q, F, H> {
    jobs: Arc<JobService<Q>>,
    outcomes: F,
    handler: H,
    config: WorkerConfig,
    browser_readiness: Option<Arc<dyn BrowserJobReadiness>>,
}

impl<Q, F, H> Worker<Q, F, H> {
    /// Creates a one-pass worker over a queue, outcome factory, and handler.
    pub fn new(
        jobs: JobService<Q>,
        outcomes: F,
        handler: H,
        config: WorkerConfig,
    ) -> Result<Self, WorkerConfigError> {
        config.validate_for_lease(jobs.config().lease_for())?;
        Ok(Self {
            jobs: Arc::new(jobs),
            outcomes,
            handler,
            config,
            browser_readiness: None,
        })
    }

    /// Adds a shared browser-readiness gate to this worker.
    ///
    /// Database-only jobs remain claimable while the gate is closed. Jobs that
    /// require a browser are left queued until the gate reports readiness.
    pub fn with_browser_readiness(mut self, readiness: Arc<dyn BrowserJobReadiness>) -> Self {
        self.browser_readiness = Some(readiness);
        self
    }

    /// Returns the dispatch and heartbeat policy.
    pub const fn config(&self) -> &WorkerConfig {
        &self.config
    }
}

impl<Q, F, H> Worker<Q, F, H>
where
    Q: JobQueue + ExpiredJobRecovery,
    F: WorkerOutcomeFactory,
    H: JobHandler,
{
    /// Claims and executes at most one job.
    ///
    /// A bounded expired-lease recovery pass runs before claiming so work left
    /// by a crashed worker can become claimable again.
    ///
    /// `now` is the compatibility/test clock accepted by the queue port. The
    /// PostgreSQL adapter makes lease decisions with its own statement-local
    /// clock; the in-memory adapter uses this value deterministically.
    #[tracing::instrument(skip_all, level = "debug", fields(now = %now))]
    pub async fn run_once(&self, now: DateTime<Utc>) -> Result<WorkerRun, WorkerError> {
        self.jobs
            .recover_expired(now)
            .await
            .map_err(WorkerError::Job)?;

        let filtered_job_types = self
            .browser_readiness
            .as_ref()
            .filter(|readiness| !readiness.browser_jobs_allowed())
            .map(|_| {
                self.config
                    .allowed_job_types()
                    .iter()
                    .copied()
                    .filter(|job_type| !job_type.requires_browser())
                    .collect::<Vec<_>>()
            });
        if filtered_job_types.is_some() {
            tracing::debug!(
                allowed_job_types = ?self.config.allowed_job_types(),
                "browser-backed worker jobs are temporarily gated"
            );
        }
        let allowed_job_types = filtered_job_types
            .as_deref()
            .unwrap_or_else(|| self.config.allowed_job_types());

        let Some(lease) = self
            .jobs
            .claim_next(now, allowed_job_types)
            .await
            .map_err(WorkerError::Job)?
        else {
            tracing::debug!("worker found no claimable jobs");
            return Ok(WorkerRun::Idle);
        };

        tracing::debug!(
            job_id = %lease.job.id(),
            job_type = ?lease.job.job_type(),
            "worker claimed job"
        );

        let outcome = match time::timeout(
            self.config.execution_timeout(),
            self.execute_with_heartbeats(&lease, now),
        )
        .await
        {
            Ok(outcome) => outcome?,
            Err(_) => {
                let timeout = self.config.execution_timeout();
                let error = format!("job execution timed out after {timeout:?}");
                tracing::warn!(
                    job_id = %lease.job.id(),
                    job_type = ?lease.job.job_type(),
                    timeout_ms = timeout.as_millis(),
                    "worker cancelled a job that exceeded its execution timeout"
                );
                JobExecution::Retry {
                    retry_at: now,
                    error,
                }
            }
        };
        match &outcome {
            JobExecution::Succeeded | JobExecution::Committed => tracing::info!(
                job_id = %lease.job.id(),
                job_type = ?lease.job.job_type(),
                outcome = execution_kind(&outcome),
                "worker completed job"
            ),
            JobExecution::Deferred { resume_at } => tracing::debug!(
                job_id = %lease.job.id(),
                job_type = ?lease.job.job_type(),
                resume_at = %resume_at,
                "worker deferred job"
            ),
            JobExecution::Retry { retry_at, error } => tracing::warn!(
                job_id = %lease.job.id(),
                job_type = ?lease.job.job_type(),
                retry_at = %retry_at,
                error = %error,
                "worker scheduled a job retry"
            ),
            JobExecution::Failed { error } => tracing::error!(
                job_id = %lease.job.id(),
                job_type = ?lease.job.job_type(),
                error = %error,
                "worker reached a terminal job failure"
            ),
        }
        let completed = if outcome == JobExecution::Committed {
            self.jobs
                .find(lease.job.id())
                .await
                .map_err(WorkerError::Job)?
                .ok_or_else(|| {
                    WorkerError::Job(JobServiceError::Repository(JobRepositoryError::NotFound {
                        job_id: lease.job.id(),
                    }))
                })?
        } else {
            let mut transaction = self
                .outcomes
                .begin()
                .await
                .map_err(WorkerError::Transaction)?;
            let completed = self
                .apply_outcome(&mut transaction, &lease, outcome.clone(), now)
                .await
                .map_err(WorkerError::Job)?;
            transaction
                .commit()
                .await
                .map_err(WorkerError::Transaction)?;
            completed
        };
        Ok(WorkerRun::Completed {
            job: Box::new(completed),
            outcome,
        })
    }

    /// Polls one worker until the shutdown watch becomes true or is dropped.
    ///
    /// Successful passes immediately try the next job, while idle passes and
    /// errors wait before polling again. Shutdown is checked between passes and
    /// during waits; a pass that has already claimed a job is allowed to finish
    /// its handler and fenced outcome transaction before returning. Handler
    /// execution is still bounded by the configured execution timeout. Errors
    /// are counted and retried because an expired lease, temporary database
    /// outage, or lost heartbeat should not terminate an otherwise healthy
    /// replica.
    pub async fn run_until_shutdown(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        loop_config: WorkerLoopConfig,
    ) -> WorkerLoopStats {
        tracing::info!(
            allowed_job_types = ?self.config.allowed_job_types(),
            heartbeat_every_ms = self.config.heartbeat_every().as_millis(),
            "worker loop started"
        );
        let mut stats = WorkerLoopStats::default();
        loop {
            if shutdown.has_changed().is_err() || *shutdown.borrow() {
                tracing::info!(
                    passes = stats.passes,
                    completed = stats.completed,
                    idle = stats.idle,
                    errors = stats.errors,
                    "worker loop stopped"
                );
                return stats;
            }

            stats.passes += 1;
            let wait = match self.run_once(Utc::now()).await {
                Ok(WorkerRun::Completed { .. }) => {
                    stats.completed += 1;
                    None
                }
                Ok(WorkerRun::Idle) => {
                    stats.idle += 1;
                    Some(loop_config.idle_poll_interval())
                }
                Err(error) => {
                    stats.errors += 1;
                    tracing::warn!(error = %error, "worker pass failed; retrying");
                    Some(loop_config.error_backoff())
                }
            };

            if let Some(wait) = wait {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            tracing::info!(
                                passes = stats.passes,
                                completed = stats.completed,
                                idle = stats.idle,
                                errors = stats.errors,
                                "worker loop stopped"
                            );
                            return stats;
                        }
                    }
                    _ = time::sleep(wait) => {}
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn execute_with_heartbeats(
        &self,
        lease: &JobLease,
        now: DateTime<Utc>,
    ) -> Result<JobExecution, WorkerError> {
        tracing::debug!(job_id = %lease.job.id(), "worker started job handler");
        let mut ticker = time::interval_at(
            Instant::now() + self.config.heartbeat_every(),
            self.config.heartbeat_every(),
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let handler = self.handler.execute(lease, now);
        tokio::pin!(handler);

        loop {
            tokio::select! {
                outcome = &mut handler => return Ok(outcome),
                _ = ticker.tick() => {
                    let heartbeat = self.jobs.heartbeat(lease, now);
                    tokio::pin!(heartbeat);
                    tokio::select! {
                        outcome = &mut handler => return Ok(outcome),
                        result = &mut heartbeat => {
                            match result {
                                Ok(_) => tracing::trace!(job_id = %lease.job.id(), "worker heartbeated job lease"),
                                Err(error) => {
                                    tracing::warn!(
                                        job_id = %lease.job.id(),
                                        error = %error,
                                        "worker lost the job lease heartbeat"
                                    );
                                    return Err(WorkerError::Heartbeat(error));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn apply_outcome<T>(
        &self,
        transaction: &mut T,
        lease: &JobLease,
        outcome: JobExecution,
        now: DateTime<Utc>,
    ) -> Result<Job, JobServiceError>
    where
        T: WorkerOutcomeTransaction,
    {
        match outcome {
            JobExecution::Succeeded => self.jobs.succeed(transaction, lease, now).await,
            JobExecution::Committed => unreachable!("committed outcomes bypass apply_outcome"),
            JobExecution::Deferred { resume_at } => {
                self.jobs.defer(transaction, lease, now, resume_at).await
            }
            JobExecution::Retry { retry_at, error } => {
                self.jobs
                    .retry(transaction, lease, now, retry_at, error)
                    .await
            }
            JobExecution::Failed { error } => self.jobs.fail(transaction, lease, now, error).await,
        }
    }
}

fn execution_kind(execution: &JobExecution) -> &'static str {
    match execution {
        JobExecution::Succeeded => "succeeded",
        JobExecution::Committed => "committed",
        JobExecution::Deferred { .. } => "deferred",
        JobExecution::Retry { .. } => "retry",
        JobExecution::Failed { .. } => "failed",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use chrono::TimeZone;
    use serde_json::json;
    use tokio::time::sleep;

    use super::*;
    use crate::{
        domain::job::{JobStatus, NewJob},
        persistence::repositories::job_repository::{EnqueueResult, MemoryJobRepository},
    };

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn new_job(key: &str) -> NewJob {
        NewJob {
            job_type: JobType::SourceSync,
            source_id: None,
            priority: 1,
            run_after: at(0),
            max_attempts: 2,
            payload: json!({"key": key}),
            dedupe_key: key.to_owned(),
            now: at(0),
        }
    }

    fn config() -> WorkerConfig {
        WorkerConfig::new(vec![JobType::SourceSync], StdDuration::from_millis(5))
            .expect("worker configuration should be valid")
    }

    #[derive(Clone)]
    struct FixedHandler {
        calls: Arc<AtomicUsize>,
        outcome: JobExecution,
        delay: StdDuration,
    }

    #[async_trait::async_trait]
    impl JobHandler for FixedHandler {
        async fn execute(&self, _lease: &JobLease, _now: DateTime<Utc>) -> JobExecution {
            self.calls.fetch_add(1, Ordering::Relaxed);
            sleep(self.delay).await;
            self.outcome.clone()
        }
    }

    struct CancellationProbe(Arc<AtomicBool>);

    impl Drop for CancellationProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    struct HangingHandler {
        cancelled: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl JobHandler for HangingHandler {
        async fn execute(&self, _lease: &JobLease, _now: DateTime<Utc>) -> JobExecution {
            let _probe = CancellationProbe(self.cancelled.clone());
            std::future::pending::<()>().await;
            unreachable!("the execution timeout should cancel this handler")
        }
    }

    #[test]
    fn rejects_empty_kinds_zero_heartbeat_and_heartbeat_at_lease() {
        assert_eq!(
            WorkerConfig::new(Vec::new(), StdDuration::from_secs(1)),
            Err(WorkerConfigError::NoAllowedJobTypes)
        );
        assert_eq!(
            WorkerConfig::new(vec![JobType::SourceSync], StdDuration::ZERO),
            Err(WorkerConfigError::InvalidHeartbeat)
        );
        let repository = MemoryJobRepository::new();
        let jobs = JobService::new(
            repository.clone(),
            crate::application::job_service::JobServiceConfig::new(
                "worker",
                chrono::Duration::seconds(2),
                1,
            )
            .unwrap(),
        );
        let worker_config =
            WorkerConfig::new(vec![JobType::SourceSync], StdDuration::from_secs(2)).unwrap();
        assert!(matches!(
            Worker::new(
                jobs,
                repository,
                FixedHandler {
                    calls: Arc::new(AtomicUsize::new(0)),
                    outcome: JobExecution::Succeeded,
                    delay: StdDuration::ZERO,
                },
                worker_config,
            ),
            Err(WorkerConfigError::HeartbeatNotShorterThanLease)
        ));
    }

    #[test]
    fn rejects_a_zero_execution_timeout() {
        assert_eq!(
            WorkerConfig::new_with_execution_timeout(
                vec![JobType::SourceSync],
                StdDuration::from_secs(1),
                StdDuration::ZERO,
            ),
            Err(WorkerConfigError::InvalidExecutionTimeout)
        );
    }

    #[test]
    fn rejects_zero_worker_loop_waits() {
        assert_eq!(
            WorkerLoopConfig::new(StdDuration::ZERO, StdDuration::from_secs(1)),
            Err(WorkerLoopConfigError::InvalidIdlePollInterval)
        );
        assert_eq!(
            WorkerLoopConfig::new(StdDuration::from_secs(1), StdDuration::ZERO),
            Err(WorkerLoopConfigError::InvalidErrorBackoff)
        );
    }

    #[tokio::test]
    async fn idle_pass_does_not_invoke_the_handler() {
        let repository = MemoryJobRepository::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let worker = Worker::new(
            JobService::new(
                repository.clone(),
                crate::application::job_service::JobServiceConfig::new(
                    "worker",
                    chrono::Duration::seconds(1),
                    1,
                )
                .unwrap(),
            ),
            repository,
            FixedHandler {
                calls: calls.clone(),
                outcome: JobExecution::Succeeded,
                delay: StdDuration::ZERO,
            },
            config(),
        )
        .unwrap();

        assert_eq!(worker.run_once(at(1)).await.unwrap(), WorkerRun::Idle);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn handler_outcome_is_committed_after_heartbeats() {
        let repository = MemoryJobRepository::new();
        let inserted = repository.enqueue(new_job("worker-success")).await.unwrap();
        assert!(matches!(inserted, EnqueueResult::Inserted(_)));
        let calls = Arc::new(AtomicUsize::new(0));
        let worker = Worker::new(
            JobService::new(
                repository.clone(),
                crate::application::job_service::JobServiceConfig::new(
                    "worker",
                    chrono::Duration::seconds(1),
                    1,
                )
                .unwrap(),
            ),
            repository.clone(),
            FixedHandler {
                calls: calls.clone(),
                outcome: JobExecution::Succeeded,
                delay: StdDuration::from_millis(15),
            },
            config(),
        )
        .unwrap();

        let result = worker.run_once(at(1)).await.unwrap();
        let WorkerRun::Completed { job, outcome } = result else {
            panic!("a queued job should be completed")
        };
        assert_eq!(outcome, JobExecution::Succeeded);
        assert_eq!(job.status(), JobStatus::Succeeded);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            repository.find(job.id()).await.unwrap().unwrap().status(),
            JobStatus::Succeeded
        );
    }

    #[tokio::test]
    async fn timed_out_handler_is_cancelled_and_requeued() {
        let repository = MemoryJobRepository::new();
        let inserted = repository.enqueue(new_job("worker-timeout")).await.unwrap();
        let job_id = match inserted {
            EnqueueResult::Inserted(job) => job.id(),
            EnqueueResult::AlreadyActive { .. } => panic!("the timeout test job should be new"),
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_config = WorkerConfig::new_with_execution_timeout(
            vec![JobType::SourceSync],
            StdDuration::from_millis(5),
            StdDuration::from_millis(20),
        )
        .unwrap();
        let worker = Worker::new(
            JobService::new(
                repository.clone(),
                crate::application::job_service::JobServiceConfig::new(
                    "worker",
                    chrono::Duration::seconds(1),
                    1,
                )
                .unwrap(),
            ),
            repository.clone(),
            HangingHandler {
                cancelled: cancelled.clone(),
            },
            worker_config,
        )
        .unwrap();

        let result = worker.run_once(at(1)).await.unwrap();
        let WorkerRun::Completed { job, outcome } = result else {
            panic!("a queued job should complete with a timeout outcome")
        };
        assert!(matches!(
            outcome,
            JobExecution::Retry { retry_at, error }
                if retry_at == at(1) && error.contains("job execution timed out")
        ));
        assert_eq!(job.id(), job_id);
        assert_eq!(job.status(), JobStatus::RetryWait);
        assert_eq!(job.failure_count(), 1);
        assert!(job.lease_owner().is_none());
        assert!(cancelled.load(Ordering::Relaxed));
    }
}
