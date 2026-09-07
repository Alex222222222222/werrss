//! Integration-style tests for worker execution and its polling boundary.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::json;
use tokio::sync::{watch, Mutex};
use uuid::Uuid;
use werrss::{
    acquisition::{browser_pool::AccountLeaseStore, browser_pool::BrowserPool},
    application::{
        job_service::{JobService, JobServiceConfig},
        worker::{
            BrowserJobReadiness, JobExecution, JobHandler, Worker, WorkerConfig, WorkerLoopConfig,
            WorkerLoopStats, WorkerRun,
        },
    },
    domain::credentials::WeReadAccountId,
    domain::job::{JobStatus, JobType, NewJob},
    persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository,
    persistence::repositories::job_repository::{
        EnqueueResult, JobLease, JobQueue, JobRepository, JobRepositoryTransaction,
        MemoryJobRepository,
    },
};

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}

fn job(key: &str, job_type: JobType) -> NewJob {
    NewJob {
        job_type,
        source_id: None,
        priority: 1,
        run_after: at(0),
        max_attempts: 2,
        payload: json!({"key": key}),
        dedupe_key: key.to_owned(),
        now: at(0),
    }
}

fn worker_config(types: Vec<JobType>) -> WorkerConfig {
    WorkerConfig::new(types, StdDuration::from_millis(5))
        .expect("worker configuration should be valid")
}

#[derive(Clone)]
struct RecordingHandler {
    seen: Arc<Mutex<Vec<JobType>>>,
    outcome: JobExecution,
}

#[async_trait::async_trait]
impl JobHandler for RecordingHandler {
    async fn execute(&self, lease: &JobLease, _now: DateTime<Utc>) -> JobExecution {
        self.seen.lock().await.push(lease.job.job_type());
        self.outcome.clone()
    }
}

#[derive(Clone, Copy)]
struct FixedBrowserReadiness(bool);

impl BrowserJobReadiness for FixedBrowserReadiness {
    fn browser_jobs_allowed(&self) -> bool {
        self.0
    }
}

#[derive(Clone)]
struct ShutdownHandler {
    shutdown: watch::Sender<bool>,
}

#[async_trait::async_trait]
impl JobHandler for ShutdownHandler {
    async fn execute(&self, _lease: &JobLease, _now: DateTime<Utc>) -> JobExecution {
        self.shutdown
            .send(true)
            .expect("worker loop receiver should still be alive");
        JobExecution::Succeeded
    }
}

#[derive(Clone)]
struct ShutdownAfterCallsHandler {
    shutdown: watch::Sender<bool>,
    calls: Arc<AtomicUsize>,
    stop_after: usize,
}

#[async_trait::async_trait]
impl JobHandler for ShutdownAfterCallsHandler {
    async fn execute(&self, _lease: &JobLease, _now: DateTime<Utc>) -> JobExecution {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        if call >= self.stop_after {
            self.shutdown
                .send(true)
                .expect("worker loop receiver should still be alive");
        }
        JobExecution::Succeeded
    }
}

#[derive(Clone)]
struct CloseSenderHandler {
    shutdown: Arc<Mutex<Option<watch::Sender<bool>>>>,
    calls: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CommittingHandler {
    repository: MemoryJobRepository,
}

#[async_trait::async_trait]
impl JobHandler for CommittingHandler {
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        let mut transaction = self
            .repository
            .begin()
            .await
            .expect("handler transaction should begin");
        transaction
            .succeed(
                lease.job.id(),
                lease.job.lease_owner().expect("claimed job has an owner"),
                lease.token,
                now,
            )
            .await
            .expect("handler should complete the claimed job");
        transaction
            .commit()
            .await
            .expect("handler transaction should commit");
        JobExecution::Committed
    }
}

#[async_trait::async_trait]
impl JobHandler for CloseSenderHandler {
    async fn execute(&self, _lease: &JobLease, _now: DateTime<Utc>) -> JobExecution {
        self.calls.fetch_add(1, Ordering::Relaxed);
        drop(self.shutdown.lock().await.take());
        JobExecution::Succeeded
    }
}

#[derive(Clone)]
struct AuthenticatedHangingHandler {
    browser_pool: BrowserPool,
    leases: MemoryAccountLeaseRepository,
    account_id: WeReadAccountId,
}

#[async_trait::async_trait]
impl JobHandler for AuthenticatedHangingHandler {
    async fn execute(&self, _lease: &JobLease, _now: DateTime<Utc>) -> JobExecution {
        let _session = self
            .browser_pool
            .open_authenticated(
                self.leases.clone(),
                self.account_id,
                "integration-timeout-worker",
                Duration::seconds(30),
            )
            .await
            .expect("authenticated browser acquisition should succeed")
            .expect("the integration worker should acquire the account lease");

        std::future::pending::<()>().await;
        JobExecution::Succeeded
    }
}

fn build_worker(
    repository: MemoryJobRepository,
    handler: RecordingHandler,
    types: Vec<JobType>,
) -> Worker<MemoryJobRepository, MemoryJobRepository, RecordingHandler> {
    Worker::new(
        JobService::new(
            repository.clone(),
            JobServiceConfig::new("integration-worker", Duration::seconds(1), 2)
                .expect("job service configuration should be valid"),
        ),
        repository,
        handler,
        worker_config(types),
    )
    .expect("worker configuration should match the job lease")
}

#[tokio::test]
async fn allowed_kind_filter_leaves_disallowed_job_queued() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("source", JobType::SourceSync))
        .await
        .expect("source job should enqueue");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = build_worker(
        repository.clone(),
        RecordingHandler {
            seen: seen.clone(),
            outcome: JobExecution::Succeeded,
        },
        vec![JobType::FeedRebuild],
    );

    assert_eq!(
        worker.run_once(at(1)).await.expect("worker should idle"),
        WorkerRun::Idle
    );
    assert!(seen.lock().await.is_empty());
    let lease = repository
        .claim_next(
            "test-reader",
            at(1),
            Duration::seconds(1),
            &[JobType::SourceSync],
        )
        .await
        .expect("queue read should succeed")
        .expect("disallowed job should remain claimable");
    assert_eq!(lease.job.status(), JobStatus::Running);
}

#[tokio::test]
async fn unhealthy_browser_gate_leaves_browser_jobs_queued_but_runs_feed_rebuilds() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("source", JobType::SourceSync))
        .await
        .expect("source job should enqueue");
    repository
        .enqueue(job("feed", JobType::FeedRebuild))
        .await
        .expect("feed rebuild job should enqueue");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = build_worker(
        repository.clone(),
        RecordingHandler {
            seen: seen.clone(),
            outcome: JobExecution::Succeeded,
        },
        vec![JobType::SourceSync, JobType::FeedRebuild],
    )
    .with_browser_readiness(Arc::new(FixedBrowserReadiness(false)));

    let WorkerRun::Completed { job, .. } = worker
        .run_once(at(1))
        .await
        .expect("database-only work should remain executable")
    else {
        panic!("the feed rebuild should be claimed while the browser is unhealthy")
    };
    assert_eq!(job.job_type(), JobType::FeedRebuild);
    assert_eq!(*seen.lock().await, vec![JobType::FeedRebuild]);

    let source_lease = repository
        .claim_next(
            "test-reader",
            at(1),
            Duration::seconds(1),
            &[JobType::SourceSync],
        )
        .await
        .expect("queue read should succeed")
        .expect("browser-backed source job should remain queued");
    assert_eq!(source_lease.job.job_type(), JobType::SourceSync);
}

#[tokio::test]
async fn healthy_browser_gate_allows_source_sync_jobs() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("source-ready", JobType::SourceSync))
        .await
        .expect("source job should enqueue");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = build_worker(
        repository,
        RecordingHandler {
            seen: seen.clone(),
            outcome: JobExecution::Succeeded,
        },
        vec![JobType::SourceSync],
    )
    .with_browser_readiness(Arc::new(FixedBrowserReadiness(true)));

    let WorkerRun::Completed { job, .. } = worker
        .run_once(at(1))
        .await
        .expect("healthy browser should allow source work")
    else {
        panic!("a source job should be claimed when browser readiness is true")
    };
    assert_eq!(job.job_type(), JobType::SourceSync);
    assert_eq!(*seen.lock().await, vec![JobType::SourceSync]);
}

#[tokio::test]
async fn unhealthy_browser_gate_returns_idle_when_only_browser_jobs_are_due() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("source-only", JobType::SourceSync))
        .await
        .expect("source job should enqueue");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = build_worker(
        repository.clone(),
        RecordingHandler {
            seen: seen.clone(),
            outcome: JobExecution::Succeeded,
        },
        vec![JobType::SourceSync],
    )
    .with_browser_readiness(Arc::new(FixedBrowserReadiness(false)));

    assert_eq!(
        worker
            .run_once(at(1))
            .await
            .expect("a gated queue should be polled successfully"),
        WorkerRun::Idle
    );
    assert!(seen.lock().await.is_empty());
    assert!(repository
        .claim_next(
            "test-reader",
            at(1),
            Duration::seconds(1),
            &[JobType::SourceSync],
        )
        .await
        .expect("queue read should succeed")
        .is_some());
}

#[tokio::test]
async fn retry_outcome_increments_failure_count_without_running_a_second_job() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("retry", JobType::SourceSync))
        .await
        .expect("job should enqueue");
    let worker = build_worker(
        repository.clone(),
        RecordingHandler {
            seen: Arc::new(Mutex::new(Vec::new())),
            outcome: JobExecution::Retry {
                retry_at: at(20),
                error: "temporary upstream failure".to_owned(),
            },
        },
        vec![JobType::SourceSync],
    );

    let first = worker
        .run_once(at(1))
        .await
        .expect("worker should complete");
    let WorkerRun::Completed { job, outcome } = first else {
        panic!("queued job should not be idle")
    };
    assert!(matches!(outcome, JobExecution::Retry { .. }));
    assert_eq!(job.status(), JobStatus::RetryWait);
    assert_eq!(job.failure_count(), 1);
    assert_eq!(
        worker.run_once(at(2)).await.expect("worker should wait"),
        WorkerRun::Idle
    );
}

#[tokio::test]
async fn timed_out_authenticated_handler_releases_account_lease() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("authenticated-timeout", JobType::SourceSync))
        .await
        .expect("job should enqueue");
    let leases = MemoryAccountLeaseRepository::new(Utc::now());
    let account_id = WeReadAccountId::from_uuid(Uuid::from_u128(1));
    let worker = Worker::new(
        JobService::new(
            repository.clone(),
            JobServiceConfig::new("integration-timeout", Duration::seconds(1), 2)
                .expect("job service configuration should be valid"),
        ),
        repository,
        AuthenticatedHangingHandler {
            browser_pool: BrowserPool::new(1).expect("browser pool should be valid"),
            leases: leases.clone(),
            account_id,
        },
        WorkerConfig::new_with_execution_timeout(
            vec![JobType::SourceSync],
            StdDuration::from_millis(5),
            StdDuration::from_millis(50),
        )
        .expect("worker configuration should be valid"),
    )
    .expect("worker configuration should match the job lease");

    let WorkerRun::Completed { outcome, .. } = worker
        .run_once(at(1))
        .await
        .expect("timed-out work should be converted into a retry")
    else {
        panic!("the authenticated job should be claimed")
    };
    assert!(matches!(outcome, JobExecution::Retry { .. }));

    let reacquired = tokio::time::timeout(StdDuration::from_secs(1), async {
        loop {
            if let Some(lease) = leases
                .acquire(account_id, "integration-reclaimer", Duration::seconds(30))
                .await
                .expect("account lease lookup should succeed")
            {
                break lease;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping timed-out work should release the account lease promptly");
    leases
        .release(
            reacquired.account_id(),
            reacquired.owner(),
            reacquired.token(),
        )
        .await
        .expect("the test reclaimer should release the account lease");
}

#[tokio::test]
async fn deferred_outcome_preserves_failure_budget_until_resume() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("deferred", JobType::SourceSync))
        .await
        .expect("job should enqueue");
    let worker = build_worker(
        repository.clone(),
        RecordingHandler {
            seen: Arc::new(Mutex::new(Vec::new())),
            outcome: JobExecution::Deferred { resume_at: at(20) },
        },
        vec![JobType::SourceSync],
    );

    let WorkerRun::Completed { job, outcome } = worker
        .run_once(at(1))
        .await
        .expect("worker should defer the claimed job")
    else {
        panic!("queued job should not be idle")
    };

    assert_eq!(outcome, JobExecution::Deferred { resume_at: at(20) });
    assert_eq!(job.status(), JobStatus::Deferred);
    assert_eq!(job.claim_count(), 1);
    assert_eq!(job.failure_count(), 0);
    assert_eq!(job.run_after(), at(20));
}

#[tokio::test]
async fn committed_handler_result_is_read_without_a_second_outcome_transition() {
    let repository = MemoryJobRepository::new();
    let inserted = repository
        .enqueue(job("committed", JobType::FeedRebuild))
        .await
        .expect("job should enqueue");
    let job_id = match inserted {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { job_id } => job_id,
    };
    let worker = Worker::new(
        JobService::new(
            repository.clone(),
            JobServiceConfig::new("integration-worker", Duration::seconds(1), 2)
                .expect("job configuration should be valid"),
        ),
        repository.clone(),
        CommittingHandler {
            repository: repository.clone(),
        },
        worker_config(vec![JobType::FeedRebuild]),
    )
    .expect("worker configuration should be valid");

    let WorkerRun::Completed { job, outcome } = worker
        .run_once(at(1))
        .await
        .expect("worker should accept the atomically committed result")
    else {
        panic!("queued job should not be idle")
    };

    assert_eq!(job.id(), job_id);
    assert_eq!(outcome, JobExecution::Committed);
    assert_eq!(job.status(), JobStatus::Succeeded);
    assert_eq!(
        repository.find(job_id).await.unwrap().unwrap().status(),
        JobStatus::Succeeded
    );
}

#[tokio::test]
async fn worker_loop_with_pre_requested_shutdown_does_not_claim_work() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("pre-shutdown", JobType::SourceSync))
        .await
        .expect("job should enqueue");
    let (shutdown_tx, shutdown_rx) = watch::channel(true);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = build_worker(
        repository.clone(),
        RecordingHandler {
            seen: seen.clone(),
            outcome: JobExecution::Succeeded,
        },
        vec![JobType::SourceSync],
    );

    let stats = worker
        .run_until_shutdown(
            shutdown_rx,
            WorkerLoopConfig::new(StdDuration::from_secs(1), StdDuration::from_secs(1))
                .expect("worker loop configuration should be valid"),
        )
        .await;

    assert_eq!(stats, WorkerLoopStats::default());
    assert!(seen.lock().await.is_empty());
    drop(shutdown_tx);
    let lease = repository
        .claim_next(
            "test-reader",
            at(1),
            Duration::seconds(1),
            &[JobType::SourceSync],
        )
        .await
        .expect("queue read should succeed")
        .expect("pre-shutdown job should remain queued");
    assert_eq!(lease.job.status(), JobStatus::Running);
}

#[tokio::test]
async fn worker_loop_finishes_claimed_job_before_shutdown() {
    let repository = MemoryJobRepository::new();
    let inserted = repository
        .enqueue(job("shutdown", JobType::SourceSync))
        .await
        .expect("job should enqueue");
    let job_id = match inserted {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { job_id } => job_id,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker = Worker::new(
        JobService::new(
            repository.clone(),
            JobServiceConfig::new("integration-loop", Duration::seconds(1), 2)
                .expect("job service configuration should be valid"),
        ),
        repository.clone(),
        ShutdownHandler {
            shutdown: shutdown_tx,
        },
        worker_config(vec![JobType::SourceSync]),
    )
    .expect("worker configuration should match the job lease");

    let stats = worker
        .run_until_shutdown(
            shutdown_rx,
            WorkerLoopConfig::new(StdDuration::from_secs(1), StdDuration::from_secs(1))
                .expect("worker loop configuration should be valid"),
        )
        .await;

    assert_eq!(
        stats,
        WorkerLoopStats {
            passes: 1,
            completed: 1,
            idle: 0,
            errors: 0,
        }
    );
    assert_eq!(
        repository
            .find(job_id)
            .await
            .expect("job lookup should succeed")
            .expect("job should still exist")
            .status(),
        JobStatus::Succeeded
    );
}

#[tokio::test]
async fn worker_loop_stops_when_shutdown_sender_closes_after_success() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("close-after-first", JobType::SourceSync))
        .await
        .expect("first job should enqueue");
    repository
        .enqueue(job("remain-queued", JobType::SourceSync))
        .await
        .expect("second job should enqueue");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = Worker::new(
        JobService::new(
            repository.clone(),
            JobServiceConfig::new("integration-close", Duration::seconds(1), 2)
                .expect("job service configuration should be valid"),
        ),
        repository,
        CloseSenderHandler {
            shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
            calls: calls.clone(),
        },
        worker_config(vec![JobType::SourceSync]),
    )
    .expect("worker configuration should match the job lease");

    let stats = tokio::time::timeout(
        StdDuration::from_secs(1),
        worker.run_until_shutdown(
            shutdown_rx,
            WorkerLoopConfig::new(StdDuration::from_secs(1), StdDuration::from_secs(1))
                .expect("worker loop configuration should be valid"),
        ),
    )
    .await
    .expect("closed shutdown channel should stop a successful worker loop");

    assert_eq!(
        stats,
        WorkerLoopStats {
            passes: 1,
            completed: 1,
            idle: 0,
            errors: 0,
        }
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn worker_loop_dispatches_successful_backlog_until_shutdown() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("backlog-one", JobType::SourceSync))
        .await
        .expect("first backlog job should enqueue");
    repository
        .enqueue(job("backlog-two", JobType::SourceSync))
        .await
        .expect("second backlog job should enqueue");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = Worker::new(
        JobService::new(
            repository.clone(),
            JobServiceConfig::new("integration-backlog", Duration::seconds(1), 2)
                .expect("job service configuration should be valid"),
        ),
        repository.clone(),
        ShutdownAfterCallsHandler {
            shutdown: shutdown_tx,
            calls: calls.clone(),
            stop_after: 2,
        },
        worker_config(vec![JobType::SourceSync]),
    )
    .expect("worker configuration should match the job lease");

    let stats = worker
        .run_until_shutdown(
            shutdown_rx,
            WorkerLoopConfig::new(StdDuration::from_secs(1), StdDuration::from_secs(1))
                .expect("worker loop configuration should be valid"),
        )
        .await;

    assert_eq!(
        stats,
        WorkerLoopStats {
            passes: 2,
            completed: 2,
            idle: 0,
            errors: 0,
        }
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert_eq!(
        repository
            .claim_next(
                "verification-reader",
                Utc::now(),
                Duration::seconds(1),
                &[JobType::SourceSync],
            )
            .await
            .expect("queue read should succeed"),
        None
    );
}
