//! Worker adapter for database-only feed rebuilds.
//!
//! [`FeedRebuildService`] owns the atomic cache-publication and claimed-job
//! completion transaction. This adapter is intentionally separate from the
//! generic [`super::worker::JobHandler`] outcome path: a successful rebuild
//! returns [`super::worker::JobExecution::Committed`] so the generic worker
//! does not attempt to complete the same job a second time.
//!
//! Rebuild errors are converted to bounded, secret-free worker outcomes. Bad
//! job shapes, missing sources, invalid normalized content, and invalid
//! configuration are permanent. Repository, lease, transaction, and cache
//! failures are retryable. If another builder currently owns the source, the
//! job is deferred without consuming retry budget.

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use crate::{
    application::{
        feed_rebuild_service::{
            FeedRebuildError, FeedRebuildOutcome, FeedRebuildService, FeedRebuildUnitOfWorkFactory,
        },
        source_service::SourceReader,
        worker::{JobExecution, JobHandler},
    },
    persistence::repositories::{
        article_repository::ArticleRepository, feed_cache_repository::FeedBuildLeaseRepository,
        job_repository::JobLease,
    },
};

/// Retry and deferral policy for one feed-rebuild handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedRebuildJobHandlerConfig {
    retry_after: Duration,
}

impl FeedRebuildJobHandlerConfig {
    /// Creates a handler policy with a positive retry/defer delay.
    pub fn new(retry_after: Duration) -> Result<Self, FeedRebuildJobHandlerConfigError> {
        if retry_after <= Duration::zero() {
            return Err(FeedRebuildJobHandlerConfigError::InvalidRetryAfter);
        }
        Ok(Self { retry_after })
    }

    /// Returns the delay used for retryable failures and active builders.
    pub const fn retry_after(self) -> Duration {
        self.retry_after
    }
}

impl Default for FeedRebuildJobHandlerConfig {
    fn default() -> Self {
        Self::new(Duration::minutes(1)).expect("default retry delay must be valid")
    }
}

/// Invalid feed-rebuild handler policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FeedRebuildJobHandlerConfigError {
    /// Retry and deferral must not create an immediate polling loop.
    #[error("feed rebuild retry delay must be positive")]
    InvalidRetryAfter,
}

/// Executes claimed feed-rebuild jobs through the atomic rebuild service.
pub struct FeedRebuildJobHandler<S, A, L, U> {
    service: FeedRebuildService<S, A, L, U>,
    config: FeedRebuildJobHandlerConfig,
}

impl<S, A, L, U> FeedRebuildJobHandler<S, A, L, U> {
    /// Creates a handler over the feed rebuild service and worker policy.
    pub fn new(
        service: FeedRebuildService<S, A, L, U>,
        config: FeedRebuildJobHandlerConfig,
    ) -> Self {
        Self { service, config }
    }

    /// Returns the retry and deferral policy.
    pub const fn config(&self) -> FeedRebuildJobHandlerConfig {
        self.config
    }
}

#[async_trait::async_trait]
impl<S, A, L, U> JobHandler for FeedRebuildJobHandler<S, A, L, U>
where
    S: SourceReader,
    A: ArticleRepository,
    L: FeedBuildLeaseRepository + 'static,
    U: FeedRebuildUnitOfWorkFactory,
{
    #[tracing::instrument(skip_all, level = "debug", fields(job_id = %lease.job.id()))]
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        tracing::debug!("starting feed rebuild job");
        let execution = match self.service.rebuild_for_job(lease).await {
            Ok(outcome) => {
                tracing::info!(job_id = %lease.job.id(), outcome = ?outcome, "feed rebuild completed");
                classify_outcome(outcome, now, self.config.retry_after())
            }
            Err(error) => {
                let execution = classify_error(&error, now, self.config.retry_after());
                match &execution {
                    JobExecution::Failed { error: message } => tracing::error!(
                        job_id = %lease.job.id(),
                        error = %message,
                        "feed rebuild reached a terminal failure"
                    ),
                    JobExecution::Retry {
                        retry_at,
                        error: message,
                    } => tracing::warn!(
                        job_id = %lease.job.id(),
                        retry_at = %retry_at,
                        error = %message,
                        "feed rebuild will be retried"
                    ),
                    JobExecution::Deferred { resume_at } => tracing::debug!(
                        job_id = %lease.job.id(),
                        resume_at = %resume_at,
                        "feed rebuild was deferred"
                    ),
                    JobExecution::Succeeded | JobExecution::Committed => {}
                }
                execution
            }
        };
        execution
    }
}

fn classify_outcome(
    outcome: FeedRebuildOutcome,
    now: DateTime<Utc>,
    retry_after: Duration,
) -> JobExecution {
    match outcome {
        FeedRebuildOutcome::AlreadyActive => {
            retry_or_fail(now, retry_after, "feed rebuild is already active").map_or_else(
                |error| error,
                |retry_at| JobExecution::Deferred {
                    resume_at: retry_at,
                },
            )
        }
        FeedRebuildOutcome::Published { .. }
        | FeedRebuildOutcome::SourceRevisionChanged { .. }
        | FeedRebuildOutcome::ExistingCacheNewer => JobExecution::Committed,
    }
}

fn classify_error(
    error: &FeedRebuildError,
    now: DateTime<Utc>,
    retry_after: Duration,
) -> JobExecution {
    let (permanent, message) = match error {
        FeedRebuildError::InvalidSourceId
        | FeedRebuildError::JobTypeMismatch { .. }
        | FeedRebuildError::JobMissingSource { .. }
        | FeedRebuildError::JobLeaseMissingOwner { .. }
        | FeedRebuildError::EmptyOwner
        | FeedRebuildError::SourceNotFound { .. }
        | FeedRebuildError::Render(_)
        | FeedRebuildError::Config(_) => (true, "feed rebuild job data is invalid"),
        FeedRebuildError::Source(_)
        | FeedRebuildError::Articles(_)
        | FeedRebuildError::Lease(_)
        | FeedRebuildError::Cache(_)
        | FeedRebuildError::Job(_)
        | FeedRebuildError::UnitOfWork(_) => {
            (false, "feed rebuild storage is temporarily unavailable")
        }
    };

    if permanent {
        JobExecution::Failed {
            error: message.to_owned(),
        }
    } else {
        retry_or_fail(now, retry_after, message).map_or_else(
            |error| error,
            |retry_at| JobExecution::Retry {
                retry_at,
                error: message.to_owned(),
            },
        )
    }
}

fn retry_or_fail(
    now: DateTime<Utc>,
    retry_after: Duration,
    message: &'static str,
) -> Result<DateTime<Utc>, JobExecution> {
    now.checked_add_signed(retry_after)
        .ok_or_else(|| JobExecution::Failed {
            error: format!("{message}; retry time is outside the supported range"),
        })
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};
    use uuid::Uuid;

    use super::*;
    use crate::{
        application::source_service::SourceServiceError, domain::source::SourceId,
        persistence::repositories::source_repository::SourceRepositoryError,
    };

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    #[test]
    fn rejects_zero_and_negative_retry_delays() {
        assert_eq!(
            FeedRebuildJobHandlerConfig::new(Duration::zero()),
            Err(FeedRebuildJobHandlerConfigError::InvalidRetryAfter)
        );
        assert_eq!(
            FeedRebuildJobHandlerConfig::new(Duration::seconds(-1)),
            Err(FeedRebuildJobHandlerConfigError::InvalidRetryAfter)
        );
    }

    #[test]
    fn classifies_invalid_job_data_as_a_bounded_permanent_failure() {
        let error = FeedRebuildError::SourceNotFound {
            source_id: SourceId::from_uuid(Uuid::from_u128(1)),
        };

        assert_eq!(
            classify_error(&error, at(10), Duration::minutes(1)),
            JobExecution::Failed {
                error: "feed rebuild job data is invalid".to_owned()
            }
        );
    }

    #[test]
    fn classifies_storage_errors_as_retryable_at_the_configured_time() {
        let error = FeedRebuildError::Source(SourceServiceError::Source(
            SourceRepositoryError::Storage("database unavailable".to_owned()),
        ));
        let outcome = classify_error(&error, at(10), Duration::minutes(1));
        assert_eq!(
            outcome,
            JobExecution::Retry {
                retry_at: at(70),
                error: "feed rebuild storage is temporarily unavailable".to_owned()
            }
        );
    }

    #[test]
    fn defers_an_active_builder_without_spending_failure_budget() {
        assert_eq!(
            classify_outcome(
                FeedRebuildOutcome::AlreadyActive,
                at(10),
                Duration::minutes(1)
            ),
            JobExecution::Deferred { resume_at: at(70) }
        );
    }

    #[test]
    fn successful_rebuild_outcomes_use_the_atomic_commit_path() {
        for outcome in [
            FeedRebuildOutcome::Published {
                feed_revision: crate::domain::source::FeedRevision::from_u64(2),
            },
            FeedRebuildOutcome::SourceRevisionChanged {
                current_revision: crate::domain::source::FeedRevision::from_u64(3),
            },
            FeedRebuildOutcome::ExistingCacheNewer,
        ] {
            assert_eq!(
                classify_outcome(outcome, at(10), Duration::minutes(1)),
                JobExecution::Committed
            );
        }
    }

    #[test]
    fn turns_retry_time_overflow_into_a_terminal_bounded_failure() {
        let outcome =
            retry_or_fail(DateTime::<Utc>::MAX_UTC, Duration::seconds(1), "temporary").unwrap_err();

        assert_eq!(
            outcome,
            JobExecution::Failed {
                error: "temporary; retry time is outside the supported range".to_owned()
            }
        );
    }
}
