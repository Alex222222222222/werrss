//! Durable job domain model.
//!
//! A [`Job`] is the storage-independent representation of one scheduled unit
//! of work. PostgreSQL will persist these fields and coordinate concurrent
//! workers, but the invariants and state transitions live here so they are
//! shared by repositories, application services, and tests.
//!
//! Responsibilities:
//!
//! - identify supported work kinds and explicit lifecycle states;
//! - validate creation values such as the deduplication key and attempt limit;
//! - assign and renew an instance-owned lease with a per-claim fencing token;
//! - prevent stale workers from completing, retrying, or failing a job after
//!   its lease has expired; and
//! - move abandoned work back to the queue with a bounded retry count.
//!
//! Non-responsibilities: SQL, row locks, `SKIP LOCKED`, timers, exponential
//! backoff calculation, browser execution, and HTTP responses. The job
//! repository must use a PostgreSQL transaction and row lock around calls that
//! mutate a persisted job. The scheduler supplies `run_after` values and
//! decides which due sources should be enqueued; this module only checks a
//! candidate job's local invariants.
//!
//! The target lifecycle is:
//!
//! ```text
//! queued -> running -> succeeded
//! queued -> running -> retry_wait -> running
//! queued -> running -> deferred -> running
//! queued/retry_wait/deferred -> failed (cancelled)
//! running -> failed
//! running (expired lease) -> queued | failed
//! ```
//!
//! The model separates durable claim observability from the retry failure
//! budget. `claim_count` increments whenever ownership is granted;
//! `failure_count` increments only for a retryable execution failure or an
//! expired lease and is compared with `max_attempts`. A quiet-hours transition
//! enters `deferred`, sets `run_after`, and changes neither counter nor the
//! terminal outcome.
//!
//! High availability depends on two layers. PostgreSQL must atomically claim a
//! due row using `FOR UPDATE SKIP LOCKED` and persist the lease owner, fencing
//! token, and expiry. This model then rejects work from a stale owner or stale
//! claim, allowing another application replica to recover an expired lease
//! safely. Job handlers still need idempotent article and cache writes because
//! a process can crash after performing side effects but before completing its
//! job.
//!
//! Domain transitions accept an explicit timestamp to remain pure and
//! deterministic. In production that timestamp is supplied by the PostgreSQL
//! repository's statement-local server clock, not by the application replica.
//! The interim in-memory repository accepts an explicit test timestamp; the
//! production repository ignores that compatibility timestamp for distributed
//! lease decisions and uses PostgreSQL time instead.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

/// Categories of durable work known to the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    /// Fetch the current article list and pages for one source.
    SourceSync,
    /// Render and persist one source's RSS document from archived records.
    FeedRebuild,
    /// Fetch or repair one article that was not complete during a sync.
    ArticleBackfill,
    /// Refresh an account credential or complete an interactive login flow.
    CredentialRefresh,
    /// Re-fetch one missing binary asset while retaining its stable record.
    AssetRepair,
}

impl JobType {
    /// Every job kind known by this application.
    pub const ALL: &'static [Self] = &[
        Self::SourceSync,
        Self::FeedRebuild,
        Self::ArticleBackfill,
        Self::CredentialRefresh,
        Self::AssetRepair,
    ];

    /// Returns whether the job needs the browser sidecar at execution time.
    ///
    /// Credential refresh is intentionally excluded: the current runtime
    /// refresh transport is an injected application service and does not use a
    /// WebDriver session. Interactive browser login remains a future job kind.
    pub const fn requires_browser(self) -> bool {
        matches!(self, Self::SourceSync | Self::ArticleBackfill)
    }
}

/// Persisted lifecycle state for a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Eligible work that has not been claimed by a worker.
    Queued,
    /// Work currently owned by a worker lease.
    Running,
    /// Work waiting until `run_after` for another attempt.
    RetryWait,
    /// Work intentionally postponed without consuming failure budget.
    Deferred,
    /// Work completed successfully.
    Succeeded,
    /// Work stopped permanently or exhausted its failure budget.
    Failed,
}

impl JobStatus {
    /// Returns whether the status can be selected as active work.
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Running | Self::RetryWait | Self::Deferred
        )
    }

    /// Returns whether no further worker transition is expected.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

/// Input required to create a queued job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewJob {
    /// Kind of work the application service will execute.
    pub job_type: JobType,
    /// Source associated with the job; global jobs may omit it.
    pub source_id: Option<Uuid>,
    /// Higher values may be selected first by the repository.
    pub priority: i32,
    /// Earliest time at which a worker may claim the job.
    pub run_after: DateTime<Utc>,
    /// Maximum number of retryable failures before the job becomes terminal.
    pub max_attempts: u32,
    /// Typed job parameters. Secrets must not be placed in this payload.
    pub payload: Value,
    /// Key used by the repository's active-job uniqueness constraint.
    pub dedupe_key: String,
    /// Creation time supplied by the application clock.
    pub now: DateTime<Utc>,
}

/// Complete job state read from durable storage.
///
/// Persistence adapters use this value to rehydrate a [`Job`] without
/// generating a new identifier or resetting counters and lease fields. The
/// conversion is validated so malformed rows cannot silently enter application
/// orchestration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedJob {
    /// Durable job identifier.
    pub id: Uuid,
    /// Kind of work represented by the row.
    pub job_type: JobType,
    /// Optional associated source.
    pub source_id: Option<Uuid>,
    /// Persisted lifecycle state.
    pub status: JobStatus,
    /// Scheduling priority.
    pub priority: i32,
    /// Earliest next claim time.
    pub run_after: DateTime<Utc>,
    /// Number of claims already made, including claims after deferral.
    pub claim_count: u32,
    /// Number of retryable failures already recorded.
    pub failure_count: u32,
    /// Maximum number of permitted retryable failures.
    pub max_attempts: u32,
    /// Instance currently holding the lease.
    pub lease_owner: Option<String>,
    /// Fencing token for the current lease.
    pub lease_token: Option<LeaseToken>,
    /// Current lease expiry.
    pub lease_until: Option<DateTime<Utc>>,
    /// Most recent lease heartbeat.
    pub heartbeat_at: Option<DateTime<Utc>>,
    /// First time a worker claimed the job.
    pub started_at: Option<DateTime<Utc>>,
    /// Terminal completion time, if any.
    pub finished_at: Option<DateTime<Utc>>,
    /// Most recent safe error summary.
    pub last_error: Option<String>,
    /// Worker parameters.
    pub payload: Value,
    /// Active-job deduplication key.
    pub dedupe_key: String,
    /// Insertion timestamp.
    pub created_at: DateTime<Utc>,
    /// Most recent mutation timestamp.
    pub updated_at: DateTime<Utc>,
}

/// Unforgeable identity for one claim of a job lease.
///
/// The owner identifies an application instance, while this token identifies
/// one lease incarnation. Both values are required for worker mutations so a
/// stale worker cannot act after the same instance reclaims an expired job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseToken(Uuid);

impl LeaseToken {
    /// Creates a fresh fencing token for a new lease incarnation.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps a token read from durable storage.
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the UUID persisted with the job lease.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for LeaseToken {
    fn default() -> Self {
        Self::new()
    }
}

/// A validated, durable job and its lease state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    id: Uuid,
    job_type: JobType,
    source_id: Option<Uuid>,
    status: JobStatus,
    priority: i32,
    run_after: DateTime<Utc>,
    claim_count: u32,
    failure_count: u32,
    max_attempts: u32,
    lease_owner: Option<String>,
    lease_token: Option<LeaseToken>,
    lease_until: Option<DateTime<Utc>>,
    heartbeat_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    payload: Value,
    dedupe_key: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Job {
    /// Creates a queued job with no lease and zero claims.
    pub fn new(spec: NewJob) -> Result<Self, JobError> {
        if spec.max_attempts == 0 {
            return Err(JobError::InvalidAttemptLimit);
        }
        if spec.dedupe_key.trim().is_empty() {
            return Err(JobError::EmptyDedupeKey);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            job_type: spec.job_type,
            source_id: spec.source_id,
            status: JobStatus::Queued,
            priority: spec.priority,
            run_after: spec.run_after,
            claim_count: 0,
            failure_count: 0,
            max_attempts: spec.max_attempts,
            lease_owner: None,
            lease_token: None,
            lease_until: None,
            heartbeat_at: None,
            started_at: None,
            finished_at: None,
            last_error: None,
            payload: spec.payload,
            dedupe_key: spec.dedupe_key,
            created_at: spec.now,
            updated_at: spec.now,
        })
    }

    /// Rehydrates a job from a persisted row while checking state invariants.
    pub fn from_persisted(record: PersistedJob) -> Result<Self, JobError> {
        let job = Self {
            id: record.id,
            job_type: record.job_type,
            source_id: record.source_id,
            status: record.status,
            priority: record.priority,
            run_after: record.run_after,
            claim_count: record.claim_count,
            failure_count: record.failure_count,
            max_attempts: record.max_attempts,
            lease_owner: record.lease_owner,
            lease_token: record.lease_token,
            lease_until: record.lease_until,
            heartbeat_at: record.heartbeat_at,
            started_at: record.started_at,
            finished_at: record.finished_at,
            last_error: record.last_error,
            payload: record.payload,
            dedupe_key: record.dedupe_key,
            created_at: record.created_at,
            updated_at: record.updated_at,
        };
        job.validate_persisted_state()?;
        Ok(job)
    }

    /// Returns the immutable job identifier.
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the kind of work represented by this job.
    pub const fn job_type(&self) -> JobType {
        self.job_type
    }

    /// Returns the optional source relationship.
    pub const fn source_id(&self) -> Option<Uuid> {
        self.source_id
    }

    /// Returns the current lifecycle state.
    pub const fn status(&self) -> JobStatus {
        self.status
    }

    /// Returns the scheduling priority.
    pub const fn priority(&self) -> i32 {
        self.priority
    }

    /// Returns the earliest claim time.
    pub const fn run_after(&self) -> DateTime<Utc> {
        self.run_after
    }

    /// Returns the number of worker claims made so far.
    pub const fn claim_count(&self) -> u32 {
        self.claim_count
    }

    /// Returns the number of retryable failures recorded so far.
    pub const fn failure_count(&self) -> u32 {
        self.failure_count
    }

    /// Returns the maximum number of retryable failures permitted.
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns the currently assigned instance, if any.
    pub fn lease_owner(&self) -> Option<&str> {
        self.lease_owner.as_deref()
    }

    /// Returns the fencing token for the current lease, if any.
    pub const fn lease_token(&self) -> Option<LeaseToken> {
        self.lease_token
    }

    /// Returns the lease expiry, if the job is currently leased.
    pub const fn lease_until(&self) -> Option<DateTime<Utc>> {
        self.lease_until
    }

    /// Returns the last lease heartbeat time.
    pub const fn heartbeat_at(&self) -> Option<DateTime<Utc>> {
        self.heartbeat_at
    }

    /// Returns when the first worker started this job.
    pub const fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }

    /// Returns when this job reached a terminal state.
    pub const fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }

    /// Returns the most recent retry or recovery error.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Returns the JSON parameters for the worker.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// Returns the key used for active-job deduplication.
    pub fn dedupe_key(&self) -> &str {
        &self.dedupe_key
    }

    /// Returns the insertion timestamp.
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the timestamp of the most recent domain mutation.
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Claims a due active job for one application instance.
    ///
    /// The repository must perform the due-row selection and update under a
    /// PostgreSQL lock. This method is the second line of defense and updates
    /// the in-memory/domain representation with the same lease values.
    pub fn claim(
        &mut self,
        owner: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<LeaseToken, JobError> {
        validate_owner(owner)?;
        let lease_until = lease_expiry(now, lease_for)?;
        if !matches!(
            self.status,
            JobStatus::Queued | JobStatus::RetryWait | JobStatus::Deferred
        ) {
            return Err(JobError::InvalidTransition {
                status: self.status,
                operation: "claim",
            });
        }
        if self.run_after > now {
            return Err(JobError::NotDue);
        }
        if self.failure_count >= self.max_attempts {
            return Err(JobError::AttemptsExhausted);
        }

        let lease_token = LeaseToken::new();
        self.claim_count = self
            .claim_count
            .checked_add(1)
            .ok_or(JobError::ClaimCountOverflow)?;
        self.status = JobStatus::Running;
        self.lease_owner = Some(owner.to_owned());
        self.lease_token = Some(lease_token);
        self.lease_until = Some(lease_until);
        self.heartbeat_at = Some(now);
        self.started_at.get_or_insert(now);
        self.finished_at = None;
        self.updated_at = now;
        Ok(lease_token)
    }

    /// Defers a live job without consuming retry budget.
    ///
    /// This is used for quiet hours and other non-failure eligibility gates.
    /// The caller supplies the next eligible instant after evaluating its
    /// timezone policy; the lease itself is still fenced exactly like a retry.
    pub fn defer(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        resume_at: DateTime<Utc>,
    ) -> Result<(), JobError> {
        validate_owner(owner)?;
        if resume_at <= now {
            return Err(JobError::InvalidDeferralTime);
        }
        self.ensure_live_owner(owner, token, now, "defer")?;
        self.status = JobStatus::Deferred;
        self.run_after = resume_at;
        self.clear_lease();
        self.last_error = None;
        self.finished_at = None;
        self.updated_at = now;
        Ok(())
    }

    /// Extends a live lease for its current owner.
    pub fn heartbeat(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<(), JobError> {
        validate_owner(owner)?;
        let lease_until = lease_expiry(now, lease_for)?;
        self.ensure_live_owner(owner, token, now, "heartbeat")?;
        self.lease_until = Some(lease_until);
        self.heartbeat_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Marks a live job as successfully completed by its current owner.
    pub fn succeed(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
    ) -> Result<(), JobError> {
        validate_owner(owner)?;
        self.ensure_live_owner(owner, token, now, "succeed")?;
        self.status = JobStatus::Succeeded;
        self.clear_lease();
        self.last_error = None;
        self.finished_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Records a retry or turns the job terminal when failures are exhausted.
    ///
    /// The returned status tells the application service whether it should
    /// enqueue future work (`RetryWait`) or record a terminal failure
    /// (`Failed`). Backoff is intentionally calculated by the application
    /// service, so policy changes do not belong in this domain transition.
    pub fn retry(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: impl Into<String>,
    ) -> Result<JobStatus, JobError> {
        validate_owner(owner)?;
        let error = nonempty_error(error)?;
        self.ensure_live_owner(owner, token, now, "retry")?;

        self.last_error = Some(error);
        self.run_after = retry_at;
        self.clear_lease();
        self.updated_at = now;

        self.failure_count = self
            .failure_count
            .checked_add(1)
            .ok_or(JobError::FailureCountOverflow)?;
        if self.failure_count >= self.max_attempts {
            self.status = JobStatus::Failed;
            self.finished_at = Some(now);
            Ok(JobStatus::Failed)
        } else {
            self.status = JobStatus::RetryWait;
            self.finished_at = None;
            Ok(JobStatus::RetryWait)
        }
    }

    /// Permanently fails a live job with an operator-visible error summary.
    pub fn fail(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        error: impl Into<String>,
    ) -> Result<(), JobError> {
        validate_owner(owner)?;
        let error = nonempty_error(error)?;
        self.ensure_live_owner(owner, token, now, "fail")?;
        self.status = JobStatus::Failed;
        self.clear_lease();
        self.last_error = Some(error);
        self.finished_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Cancels an unclaimed job and records a terminal failure reason.
    ///
    /// Queued, retry-wait, and deferred jobs have no worker owner, so cancellation is an
    /// ownerless administrative transition. Running jobs must use [`Self::fail`]
    /// with their current lease token, preventing an operator action from
    /// racing an active worker without repository-level authorization.
    pub fn cancel(
        &mut self,
        now: DateTime<Utc>,
        reason: impl Into<String>,
    ) -> Result<(), JobError> {
        let reason = nonempty_error(reason)?;
        if !matches!(
            self.status,
            JobStatus::Queued | JobStatus::RetryWait | JobStatus::Deferred
        ) {
            return Err(JobError::InvalidTransition {
                status: self.status,
                operation: "cancel",
            });
        }
        self.status = JobStatus::Failed;
        self.clear_lease();
        self.last_error = Some(reason);
        self.finished_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Recovers a running job whose lease is absent or no longer valid.
    ///
    /// Returns `true` when this call changed the job. A live job and a job that
    /// has already been recovered both return `false`, which makes a locked
    /// recovery pass naturally idempotent. Expired ownership consumes one
    /// failure budget unit: jobs with failure budget remaining return to
    /// `queued`, while exhausted jobs become terminal `failed`.
    pub fn recover_expired_lease(&mut self, now: DateTime<Utc>) -> bool {
        if self.status != JobStatus::Running
            || self
                .lease_until
                .is_some_and(|lease_until| lease_until > now)
        {
            return false;
        }

        self.clear_lease();
        self.run_after = now;
        self.last_error = Some("worker lease expired".to_owned());
        self.updated_at = now;
        self.failure_count = self.failure_count.saturating_add(1);
        if self.failure_count >= self.max_attempts {
            self.status = JobStatus::Failed;
            self.finished_at = Some(now);
        } else {
            self.status = JobStatus::Queued;
            self.finished_at = None;
        }
        true
    }

    fn ensure_live_owner(
        &self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        operation: &'static str,
    ) -> Result<(), JobError> {
        if self.status != JobStatus::Running {
            return Err(JobError::InvalidTransition {
                status: self.status,
                operation,
            });
        }
        if self.lease_owner.as_deref() != Some(owner) {
            return Err(JobError::LeaseOwnerMismatch);
        }
        if self.lease_token != Some(token) {
            return Err(JobError::LeaseTokenMismatch);
        }
        match self.lease_until {
            Some(lease_until) if lease_until > now => Ok(()),
            Some(_) => Err(JobError::LeaseExpired),
            None => Err(JobError::LeaseMissing),
        }
    }

    fn clear_lease(&mut self) {
        self.lease_owner = None;
        self.lease_token = None;
        self.lease_until = None;
    }

    fn validate_persisted_state(&self) -> Result<(), JobError> {
        if self.max_attempts == 0 || self.failure_count > self.max_attempts {
            return Err(JobError::InvalidPersistedState {
                reason: "failure_count must be within a non-zero max_attempts limit",
            });
        }
        if self.dedupe_key.trim().is_empty() {
            return Err(JobError::InvalidPersistedState {
                reason: "dedupe_key must not be empty",
            });
        }

        let has_lease_owner = self.lease_owner.is_some();
        let has_lease_token = self.lease_token.is_some();
        let has_lease_expiry = self.lease_until.is_some();
        let lease_is_complete = has_lease_owner && has_lease_token && has_lease_expiry;
        let lease_is_empty = !has_lease_owner && !has_lease_token && !has_lease_expiry;

        match self.status {
            JobStatus::Running if self.claim_count == 0 => Err(JobError::InvalidPersistedState {
                reason: "running jobs require at least one claim",
            }),
            JobStatus::Running if !lease_is_complete => Err(JobError::InvalidPersistedState {
                reason: "running jobs require owner, token, and lease expiry",
            }),
            JobStatus::Running
                if self
                    .lease_owner
                    .as_deref()
                    .is_some_and(|owner| owner.trim().is_empty()) =>
            {
                Err(JobError::InvalidPersistedState {
                    reason: "running job lease owner must not be empty",
                })
            }
            JobStatus::Running if self.finished_at.is_some() => {
                Err(JobError::InvalidPersistedState {
                    reason: "running jobs cannot have finished_at",
                })
            }
            JobStatus::Running if self.failure_count >= self.max_attempts => {
                Err(JobError::InvalidPersistedState {
                    reason: "running jobs must have failure budget remaining",
                })
            }
            JobStatus::Queued | JobStatus::RetryWait | JobStatus::Deferred if !lease_is_empty => {
                Err(JobError::InvalidPersistedState {
                    reason: "queued jobs cannot retain a lease",
                })
            }
            JobStatus::Queued | JobStatus::RetryWait | JobStatus::Deferred
                if self.finished_at.is_some() =>
            {
                Err(JobError::InvalidPersistedState {
                    reason: "non-terminal jobs cannot have finished_at",
                })
            }
            JobStatus::Queued | JobStatus::RetryWait | JobStatus::Deferred
                if self.failure_count >= self.max_attempts =>
            {
                Err(JobError::InvalidPersistedState {
                    reason: "active jobs must have failure budget remaining",
                })
            }
            JobStatus::Succeeded | JobStatus::Failed if !lease_is_empty => {
                Err(JobError::InvalidPersistedState {
                    reason: "terminal jobs cannot retain a lease",
                })
            }
            JobStatus::Succeeded | JobStatus::Failed if self.finished_at.is_none() => {
                Err(JobError::InvalidPersistedState {
                    reason: "terminal jobs require finished_at",
                })
            }
            _ => Ok(()),
        }
    }
}

/// Errors raised when a job violates a domain invariant or transition rule.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JobError {
    /// A job cannot be created without at least one allowed claim.
    #[error("job max_attempts must be greater than zero")]
    InvalidAttemptLimit,
    /// Active-job deduplication requires a stable non-empty key.
    #[error("job dedupe_key must not be empty")]
    EmptyDedupeKey,
    /// A worker identity cannot be blank.
    #[error("job lease owner must not be empty")]
    EmptyLeaseOwner,
    /// A lease must have a positive representable duration.
    #[error("job lease duration must be positive and fit in the timestamp range")]
    InvalidLeaseDuration,
    /// The job is not due at the supplied clock time.
    #[error("job is not due")]
    NotDue,
    /// The job has no claims remaining.
    #[error("job has exhausted its failure budget")]
    AttemptsExhausted,
    /// The claim counter cannot represent another lease acquisition.
    #[error("job claim count overflowed")]
    ClaimCountOverflow,
    /// The failure counter cannot represent another retryable failure.
    #[error("job failure count overflowed")]
    FailureCountOverflow,
    /// A deferral must resume after the current transition time.
    #[error("job deferral time must be after the current time")]
    InvalidDeferralTime,
    /// A worker attempted an operation from an incompatible state.
    #[error("cannot {operation} a job in {status:?} state")]
    InvalidTransition {
        /// Current state at the time of the rejected operation.
        status: JobStatus,
        /// Domain operation requested by the caller.
        operation: &'static str,
    },
    /// A different instance currently owns the lease.
    #[error("job lease belongs to another instance")]
    LeaseOwnerMismatch,
    /// The worker supplied a token from an older lease incarnation.
    #[error("job lease token is no longer current")]
    LeaseTokenMismatch,
    /// The lease ended before the worker attempted its operation.
    #[error("job lease has expired")]
    LeaseExpired,
    /// A running job was missing its lease expiry.
    #[error("running job has no lease expiry")]
    LeaseMissing,
    /// A persisted row violates the job state invariants.
    #[error("invalid persisted job state: {reason}")]
    InvalidPersistedState {
        /// Invariant that the row violated.
        reason: &'static str,
    },
    /// A terminal operation needs a non-empty persisted error summary.
    #[error("job error summary must not be empty")]
    EmptyError,
}

fn validate_owner(owner: &str) -> Result<(), JobError> {
    if owner.trim().is_empty() {
        Err(JobError::EmptyLeaseOwner)
    } else {
        Ok(())
    }
}

fn nonempty_error(error: impl Into<String>) -> Result<String, JobError> {
    let error = error.into();
    if error.trim().is_empty() {
        Err(JobError::EmptyError)
    } else {
        Ok(error)
    }
}

fn lease_expiry(now: DateTime<Utc>, lease_for: Duration) -> Result<DateTime<Utc>, JobError> {
    if lease_for <= Duration::zero() {
        return Err(JobError::InvalidLeaseDuration);
    }
    now.checked_add_signed(lease_for)
        .ok_or(JobError::InvalidLeaseDuration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    const OWNER: &str = "instance-a";
    const OTHER_OWNER: &str = "instance-b";

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn new_job(max_attempts: u32) -> Job {
        Job::new(NewJob {
            job_type: JobType::SourceSync,
            source_id: Some(Uuid::nil()),
            priority: 10,
            run_after: at(100),
            max_attempts,
            payload: json!({"source_id": Uuid::nil()}),
            dedupe_key: "source-sync:nil".to_owned(),
            now: at(90),
        })
        .expect("test job should be valid")
    }

    #[test]
    fn rejects_invalid_creation_values() {
        let invalid_attempts = Job::new(NewJob {
            job_type: JobType::FeedRebuild,
            source_id: None,
            priority: 0,
            run_after: at(0),
            max_attempts: 0,
            payload: Value::Null,
            dedupe_key: "feed".to_owned(),
            now: at(0),
        });
        assert_eq!(invalid_attempts, Err(JobError::InvalidAttemptLimit));

        let empty_key = Job::new(NewJob {
            job_type: JobType::FeedRebuild,
            source_id: None,
            priority: 0,
            run_after: at(0),
            max_attempts: 1,
            payload: Value::Null,
            dedupe_key: "  ".to_owned(),
            now: at(0),
        });
        assert_eq!(empty_key, Err(JobError::EmptyDedupeKey));
    }

    #[test]
    fn identifies_only_browser_backed_job_types() {
        assert!(JobType::SourceSync.requires_browser());
        assert!(JobType::ArticleBackfill.requires_browser());
        assert!(!JobType::FeedRebuild.requires_browser());
        assert!(!JobType::CredentialRefresh.requires_browser());
    }

    #[test]
    fn claim_requires_due_time_and_records_lease_state() {
        let mut job = new_job(3);
        assert_eq!(
            job.claim(OWNER, at(99), Duration::seconds(30)),
            Err(JobError::NotDue)
        );
        assert_eq!(job.status(), JobStatus::Queued);
        assert_eq!(job.claim_count(), 0);
        assert_eq!(job.failure_count(), 0);

        let token = job
            .claim(OWNER, at(100), Duration::seconds(30))
            .expect("due job should be claimable");
        assert_eq!(job.status(), JobStatus::Running);
        assert_eq!(job.claim_count(), 1);
        assert_eq!(job.failure_count(), 0);
        assert_eq!(job.lease_owner(), Some(OWNER));
        assert_eq!(job.lease_token(), Some(token));
        assert_eq!(job.lease_until(), Some(at(130)));
        assert_eq!(job.heartbeat_at(), Some(at(100)));
        assert_eq!(job.started_at(), Some(at(100)));
        assert!(!job.status().is_terminal());
    }

    #[test]
    fn heartbeat_and_completion_reject_wrong_or_expired_owners() {
        let mut job = new_job(2);
        let token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();

        assert_eq!(
            job.heartbeat(OTHER_OWNER, token, at(110), Duration::seconds(30)),
            Err(JobError::LeaseOwnerMismatch)
        );
        assert_eq!(
            job.succeed(OWNER, token, at(130)),
            Err(JobError::LeaseExpired)
        );
        assert_eq!(job.status(), JobStatus::Running);

        job.heartbeat(OWNER, token, at(120), Duration::seconds(60))
            .expect("current owner should renew a live lease");
        job.succeed(OWNER, token, at(150))
            .expect("current owner should complete a renewed lease");
        assert_eq!(job.status(), JobStatus::Succeeded);
        assert_eq!(job.finished_at(), Some(at(150)));
        assert_eq!(job.lease_owner(), None);
        assert_eq!(job.lease_until(), None);
        assert!(job.status().is_terminal());
    }

    #[test]
    fn retry_waits_then_fails_when_failure_budget_is_exhausted() {
        let mut job = new_job(2);
        let token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();

        let status = job
            .retry(OWNER, token, at(110), at(200), "temporary browser failure")
            .expect("first attempt should be retryable");
        assert_eq!(status, JobStatus::RetryWait);
        assert_eq!(job.status(), JobStatus::RetryWait);
        assert_eq!(job.claim_count(), 1);
        assert_eq!(job.failure_count(), 1);
        assert_eq!(job.run_after(), at(200));
        assert_eq!(job.last_error(), Some("temporary browser failure"));
        assert_eq!(job.lease_owner(), None);

        let token = job.claim(OWNER, at(200), Duration::seconds(30)).unwrap();
        let status = job
            .retry(OWNER, token, at(210), at(300), "second browser failure")
            .expect("exhaustion should become a terminal result");
        assert_eq!(status, JobStatus::Failed);
        assert_eq!(job.status(), JobStatus::Failed);
        assert_eq!(job.claim_count(), 2);
        assert_eq!(job.failure_count(), 2);
        assert_eq!(job.finished_at(), Some(at(210)));
        assert!(job.status().is_terminal());
    }

    #[test]
    fn expired_lease_recovery_is_idempotent_and_bounded() {
        let mut retryable = new_job(2);
        retryable
            .claim(OWNER, at(100), Duration::seconds(30))
            .unwrap();
        assert!(retryable.recover_expired_lease(at(130)));
        assert_eq!(retryable.status(), JobStatus::Queued);
        assert_eq!(retryable.claim_count(), 1);
        assert_eq!(retryable.failure_count(), 1);
        assert_eq!(retryable.run_after(), at(130));
        assert_eq!(retryable.last_error(), Some("worker lease expired"));
        assert!(!retryable.recover_expired_lease(at(131)));

        let mut exhausted = new_job(1);
        exhausted
            .claim(OWNER, at(100), Duration::seconds(30))
            .unwrap();
        assert!(exhausted.recover_expired_lease(at(130)));
        assert_eq!(exhausted.status(), JobStatus::Failed);
        assert_eq!(exhausted.failure_count(), 1);
        assert_eq!(exhausted.finished_at(), Some(at(130)));
    }

    #[test]
    fn deferral_allows_more_claims_without_consuming_failure_budget() {
        let mut job = new_job(1);
        let first_token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();
        job.defer(OWNER, first_token, at(110), at(200))
            .expect("a live job should be deferrable");

        assert_eq!(job.status(), JobStatus::Deferred);
        assert_eq!(job.claim_count(), 1);
        assert_eq!(job.failure_count(), 0);
        assert_eq!(job.run_after(), at(200));
        assert_eq!(job.last_error(), None);

        let second_token = job.claim(OWNER, at(200), Duration::seconds(30)).unwrap();
        assert_eq!(job.claim_count(), 2);
        assert_eq!(job.failure_count(), 0);
        assert_eq!(
            job.retry(OWNER, second_token, at(210), at(300), "fetch failed")
                .unwrap(),
            JobStatus::Failed
        );
        assert_eq!(job.failure_count(), 1);
    }

    #[test]
    fn deferral_requires_a_future_resume_time_and_current_lease() {
        let mut job = new_job(2);
        let token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();

        assert_eq!(
            job.defer(OWNER, token, at(110), at(110)),
            Err(JobError::InvalidDeferralTime)
        );
        assert_eq!(
            job.defer(OTHER_OWNER, token, at(110), at(200)),
            Err(JobError::LeaseOwnerMismatch)
        );
        assert_eq!(job.status(), JobStatus::Running);
    }

    #[test]
    fn invalid_lease_and_terminal_transitions_are_rejected() {
        let mut job = new_job(1);
        assert_eq!(
            job.claim(OWNER, at(100), Duration::zero()),
            Err(JobError::InvalidLeaseDuration)
        );
        assert_eq!(
            job.claim(" ", at(100), Duration::seconds(30)),
            Err(JobError::EmptyLeaseOwner)
        );

        let token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();
        job.fail(OWNER, token, at(110), "operator stopped job")
            .unwrap();
        assert_eq!(
            job.succeed(OWNER, token, at(111)),
            Err(JobError::InvalidTransition {
                status: JobStatus::Failed,
                operation: "succeed"
            })
        );
        assert_eq!(
            job.retry(OWNER, token, at(111), at(120), "late retry"),
            Err(JobError::InvalidTransition {
                status: JobStatus::Failed,
                operation: "retry"
            })
        );
    }

    #[test]
    fn fencing_token_rejects_a_stale_claim_from_the_same_owner() {
        let mut job = new_job(3);
        let first_token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();
        assert!(job.recover_expired_lease(at(130)));

        let second_token = job.claim(OWNER, at(130), Duration::seconds(30)).unwrap();
        assert_ne!(first_token, second_token);
        assert_eq!(
            job.succeed(OWNER, first_token, at(140)),
            Err(JobError::LeaseTokenMismatch)
        );
        assert_eq!(job.status(), JobStatus::Running);
        job.succeed(OWNER, second_token, at(140))
            .expect("current claim should remain usable");
    }

    #[test]
    fn queued_and_retry_wait_jobs_can_be_cancelled_without_a_lease() {
        let mut queued = new_job(2);
        queued
            .cancel(at(101), "source was deleted")
            .expect("queued job should be cancellable");
        assert_eq!(queued.status(), JobStatus::Failed);
        assert_eq!(queued.last_error(), Some("source was deleted"));
        assert_eq!(queued.finished_at(), Some(at(101)));

        let mut retry_wait = new_job(2);
        let token = retry_wait
            .claim(OWNER, at(100), Duration::seconds(30))
            .unwrap();
        retry_wait
            .retry(OWNER, token, at(110), at(200), "temporary failure")
            .unwrap();
        retry_wait
            .cancel(at(120), "operator disabled source")
            .expect("retry-wait job should be cancellable");
        assert_eq!(retry_wait.status(), JobStatus::Failed);
        assert_eq!(retry_wait.finished_at(), Some(at(120)));
    }

    fn persisted_record(job: &Job) -> PersistedJob {
        PersistedJob {
            id: job.id(),
            job_type: job.job_type(),
            source_id: job.source_id(),
            status: job.status(),
            priority: job.priority(),
            run_after: job.run_after(),
            claim_count: job.claim_count(),
            failure_count: job.failure_count(),
            max_attempts: job.max_attempts(),
            lease_owner: job.lease_owner().map(str::to_owned),
            lease_token: job.lease_token(),
            lease_until: job.lease_until(),
            heartbeat_at: job.heartbeat_at(),
            started_at: job.started_at(),
            finished_at: job.finished_at(),
            last_error: job.last_error().map(str::to_owned),
            payload: job.payload().clone(),
            dedupe_key: job.dedupe_key().to_owned(),
            created_at: job.created_at(),
            updated_at: job.updated_at(),
        }
    }

    #[test]
    fn rehydrates_persisted_state_without_resetting_lease_fields() {
        let mut job = new_job(2);
        job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();
        let record = persisted_record(&job);

        let rehydrated = Job::from_persisted(record).expect("valid row should rehydrate");
        assert_eq!(rehydrated, job);
    }

    #[test]
    fn rejects_malformed_persisted_state() {
        let job = new_job(2);
        let mut record = persisted_record(&job);
        record.status = JobStatus::Running;

        assert!(matches!(
            Job::from_persisted(record),
            Err(JobError::InvalidPersistedState { .. })
        ));
    }

    #[test]
    fn rejects_persisted_jobs_with_no_valid_failure_budget() {
        let job = new_job(1);
        let mut queued = persisted_record(&job);
        queued.failure_count = queued.max_attempts;
        assert!(matches!(
            Job::from_persisted(queued),
            Err(JobError::InvalidPersistedState { .. })
        ));

        let mut running = persisted_record(&job);
        running.status = JobStatus::Running;
        running.claim_count = 0;
        running.lease_owner = Some(OWNER.to_owned());
        running.lease_token = Some(LeaseToken(Uuid::new_v4()));
        running.lease_until = Some(at(130));
        assert!(matches!(
            Job::from_persisted(running),
            Err(JobError::InvalidPersistedState { .. })
        ));
    }
}
