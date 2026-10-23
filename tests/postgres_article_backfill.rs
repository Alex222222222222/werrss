//! PostgreSQL integration coverage for missed-article repair jobs.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use sqlx::{PgPool, Row};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use uuid::Uuid;
use werrss::{
    acquisition::{
        article_page::{ArticlePageError, ExtractedArticlePage},
        weread::WeReadArticleReference,
    },
    application::{
        article_backfill_handler::{
            article_backfill_job, ArticleBackfillJobHandler, ArticleBackfillJobHandlerConfig,
            ArticleBackfillJobHandlerDependencies,
        },
        asset_archive_service::AssetArchiveService,
        job_service::{JobService, JobServiceConfig},
        source_sync_handler::SourceSyncAcquirer,
        sync_service::SyncAcquisitionError,
        worker::{JobExecution, JobHandler, Worker, WorkerConfig, WorkerRun},
    },
    archive::asset_store::AssetCachePolicy,
    domain::{
        article::{ArticleObservationVersion, NewArticle},
        job::{JobStatus, JobType},
        pacing::QuietHours,
        source::{NewSource, SchedulingGate, Source, SourceId, VerifiedWechatArticleUrl},
    },
    persistence::{
        repositories::{
            article_repository::{ArticleTransactionRepository, PostgresArticleRepository},
            job_repository::{EnqueueResult, JobLease, JobQueue, PostgresJobRepository},
            source_repository::{
                PostgresSourceRepository, SourceRepository, SourceTransactionRepository,
            },
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

type BackfillHandler =
    ArticleBackfillJobHandler<PostgresSourceRepository, PostgresArticleRepository, FakeAcquirer>;

#[derive(Clone)]
struct FakeAcquirer {
    page: ExtractedArticlePage,
    timeout: bool,
    no_account: bool,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SourceSyncAcquirer for FakeAcquirer {
    async fn list_article_references(
        &self,
        _source: &Source,
    ) -> Result<werrss::application::source_sync_handler::SourceSyncReferences, SyncAcquisitionError>
    {
        Err(SyncAcquisitionError::NoAccountEnrolled)
    }

    async fn fetch_article(
        &self,
        _source: &Source,
        _reference: &WeReadArticleReference,
        _account_id: Option<werrss::domain::credentials::WeReadAccountId>,
    ) -> Result<ExtractedArticlePage, SyncAcquisitionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.no_account {
            return Err(SyncAcquisitionError::NoAccountEnrolled);
        }
        if self.timeout {
            return Err(SyncAcquisitionError::ArticlePage(
                ArticlePageError::OperationTimedOut,
            ));
        }
        Ok(self.page.clone())
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn backfill_repairs_an_incomplete_article_and_invalidates_its_feed_atomically(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let reference = reference(
        "repair-article",
        "https://mp.weixin.qq.com/s/repair-article",
    );
    insert_incomplete_article(&factory, source_id, &reference).await;
    let source = source(&pool, source_id).await;
    let job_id = enqueue_backfill(&pool, &source, &reference).await;
    let lease = claim(&pool, "backfill-worker", Utc::now()).await;
    let calls = Arc::new(AtomicUsize::new(0));

    let handler = handler(
        &pool,
        FakeAcquirer {
            page: page(&reference, "Repaired title"),
            timeout: false,
            no_account: false,
            calls: calls.clone(),
        },
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let job_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_status, "succeeded");

    let article = sqlx::query(
        "SELECT title, original_url, content_html FROM articles WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("repair-article")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(article.get::<String, _>("title"), "Repaired title");
    assert_eq!(
        article.get::<String, _>("original_url"),
        "https://mp.weixin.qq.com/s/repair-article"
    );
    assert_eq!(article.get::<String, _>("content_html"), "<p>repaired</p>");

    let revision: i64 = sqlx::query_scalar("SELECT feed_revision FROM sources WHERE id = $1")
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(revision, 1);
    let rebuild_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE source_id = $1 AND job_type = 'feed_rebuild' AND status = 'queued'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rebuild_count, 1);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn backfill_with_asset_archiving_does_not_bump_revision_for_unchanged_article(pool: PgPool) {
    let (asset_archiver, asset_server, asset_url) = asset_archiver_fixture(2).await;
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let reference = reference(
        "asset-idempotent-backfill",
        "https://mp.weixin.qq.com/s/asset-idempotent-backfill",
    );
    let source = source(&pool, source_id).await;
    let mut article_page = page(&reference, "Asset idempotent backfill");
    article_page.content_html = format!("<p>repaired</p><img src=\"{asset_url}\">");

    let first_job_id = enqueue_backfill(&pool, &source, &reference).await;
    let first_lease = claim(&pool, "asset-backfill-worker", Utc::now()).await;
    let first_handler = handler_with_archiver(
        &pool,
        FakeAcquirer {
            page: article_page.clone(),
            timeout: false,
            no_account: false,
            calls: Arc::new(AtomicUsize::new(0)),
        },
        ArticleBackfillJobHandlerConfig::new(Duration::seconds(30)).unwrap(),
        Some(asset_archiver.clone()),
    );
    assert_eq!(
        first_handler.execute(&first_lease, Utc::now()).await,
        JobExecution::Committed
    );
    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, next_allowed_at, exhausted, last_outcome)
         VALUES ($1, $2, 2, clock_timestamp() + interval '1 hour', TRUE, 'inconclusive')",
    )
    .bind(source_id.as_uuid())
    .bind("asset-idempotent-backfill")
    .execute(&pool)
    .await
    .unwrap();

    let second_job_id = enqueue_backfill(&pool, &source, &reference).await;
    let second_lease = claim(&pool, "asset-backfill-worker", Utc::now()).await;
    let second_handler = handler_with_archiver(
        &pool,
        FakeAcquirer {
            page: article_page,
            timeout: false,
            no_account: false,
            calls: Arc::new(AtomicUsize::new(0)),
        },
        ArticleBackfillJobHandlerConfig::new(Duration::seconds(30)).unwrap(),
        Some(asset_archiver),
    );
    assert_eq!(
        second_handler.execute(&second_lease, Utc::now()).await,
        JobExecution::Committed
    );

    let revision: i64 = sqlx::query_scalar("SELECT feed_revision FROM sources WHERE id = $1")
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        revision, 1,
        "the unchanged archived article must be idempotent"
    );

    let article_html: String = sqlx::query_scalar(
        "SELECT content_html FROM articles WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-idempotent-backfill")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(article_html.contains("/assets/"));
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<chrono::DateTime<Utc>>, bool)>(
            "SELECT attempts, next_allowed_at, exhausted
             FROM asset_article_refresh_lineage WHERE source_id = $1 AND review_id = $2",
        )
        .bind(source_id.as_uuid())
        .bind("asset-idempotent-backfill")
        .fetch_one(&pool)
        .await
        .unwrap(),
        (0, None, false),
        "a complete accepted backfill resets refresh lineage"
    );

    for job_id in [first_job_id, second_job_id] {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "succeeded"
        );
    }
    asset_server
        .await
        .expect("asset fixture should serve both backfill fetches");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn backfill_preserves_cached_article_when_asset_fetch_is_incomplete(pool: PgPool) {
    let (asset_archiver, asset_server, asset_url) = asset_archiver_fixture(1).await;
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let reference = reference(
        "asset-fetch-failure-backfill",
        "https://mp.weixin.qq.com/s/asset-fetch-failure-backfill",
    );
    let source = source(&pool, source_id).await;
    let mut article_page = page(&reference, "Asset fetch failure backfill");
    article_page.content_html = format!("<p>repaired</p><img src=\"{asset_url}\">");

    enqueue_backfill(&pool, &source, &reference).await;
    let first_lease = claim(&pool, "backfill-asset-worker", Utc::now()).await;
    let first_handler = handler_with_archiver(
        &pool,
        FakeAcquirer {
            page: article_page.clone(),
            timeout: false,
            no_account: false,
            calls: Arc::new(AtomicUsize::new(0)),
        },
        ArticleBackfillJobHandlerConfig::new(Duration::seconds(30)).unwrap(),
        Some(asset_archiver.clone()),
    );
    assert_eq!(
        first_handler.execute(&first_lease, Utc::now()).await,
        JobExecution::Committed
    );

    let cached_html: String = sqlx::query_scalar(
        "SELECT content_html FROM articles WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-fetch-failure-backfill")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cached_html.contains("/assets/"));
    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, next_allowed_at, exhausted, last_outcome)
         VALUES ($1, $2, 2, clock_timestamp() + interval '1 hour', TRUE, 'inconclusive')",
    )
    .bind(source_id.as_uuid())
    .bind("asset-fetch-failure-backfill")
    .execute(&pool)
    .await
    .unwrap();
    let lineage_before = sqlx::query_as::<_, (i64, Option<chrono::DateTime<Utc>>, bool)>(
        "SELECT attempts, next_allowed_at, exhausted
         FROM asset_article_refresh_lineage WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-fetch-failure-backfill")
    .fetch_one(&pool)
    .await
    .unwrap();

    enqueue_backfill(&pool, &source, &reference).await;
    let second_lease = claim(&pool, "backfill-asset-worker", Utc::now()).await;
    let second_handler = handler_with_archiver(
        &pool,
        FakeAcquirer {
            page: article_page,
            timeout: false,
            no_account: false,
            calls: Arc::new(AtomicUsize::new(0)),
        },
        ArticleBackfillJobHandlerConfig::new(Duration::seconds(30)).unwrap(),
        Some(asset_archiver),
    );
    assert_eq!(
        second_handler.execute(&second_lease, Utc::now()).await,
        JobExecution::Committed
    );

    let persisted_html: String = sqlx::query_scalar(
        "SELECT content_html FROM articles WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-fetch-failure-backfill")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted_html, cached_html);

    let relationship_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM article_assets WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-fetch-failure-backfill")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(relationship_count, 1);
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<chrono::DateTime<Utc>>, bool)>(
            "SELECT attempts, next_allowed_at, exhausted
             FROM asset_article_refresh_lineage WHERE source_id = $1 AND review_id = $2",
        )
        .bind(source_id.as_uuid())
        .bind("asset-fetch-failure-backfill")
        .fetch_one(&pool)
        .await
        .unwrap(),
        (2, lineage_before.1, true,),
        "an incomplete backfill preserves the exhausted refresh lineage"
    );

    asset_server
        .await
        .expect("asset fixture should serve the initial backfill fetch");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn backfill_and_source_deletion_use_a_consistent_lock_order(pool: PgPool) {
    sqlx::query(
        r#"
        CREATE FUNCTION test_hold_article_lock() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            PERFORM pg_advisory_lock(2147483646::bigint);
            PERFORM pg_sleep(1);
            PERFORM pg_advisory_unlock(2147483646::bigint);
            RETURN NEW;
        END;
        $$
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_hold_article_lock_trigger BEFORE INSERT OR UPDATE ON articles FOR EACH ROW EXECUTE FUNCTION test_hold_article_lock()",
    )
    .execute(&pool)
    .await
    .unwrap();

    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let reference = reference(
        "lock-order-article",
        "https://mp.weixin.qq.com/s/lock-order-article",
    );
    let source = source(&pool, source_id).await;
    enqueue_backfill(&pool, &source, &reference).await;
    let lease = claim(&pool, "lock-order-backfill-worker", Utc::now()).await;
    let handler = handler(
        &pool,
        FakeAcquirer {
            page: page(&reference, "Lock-order article"),
            timeout: false,
            no_account: false,
            calls: Arc::new(AtomicUsize::new(0)),
        },
    );

    let backfill_task = tokio::spawn(async move { handler.execute(&lease, Utc::now()).await });
    wait_for_advisory_lock(&pool).await;

    let delete_factory = factory.clone();
    let delete_task = tokio::spawn(async move {
        let mut unit_of_work = delete_factory.begin().await.unwrap();
        unit_of_work.source().delete(source_id).await.unwrap();
        unit_of_work.commit().await.unwrap();
    });

    let (backfill_outcome, delete_result) =
        tokio::time::timeout(StdDuration::from_secs(5), async {
            (backfill_task.await, delete_task.await)
        })
        .await
        .expect("backfill and source deletion should not deadlock");
    assert_eq!(backfill_outcome.unwrap(), JobExecution::Committed);
    delete_result.unwrap();
    let source_count: i64 = sqlx::query_scalar("SELECT count(*) FROM sources WHERE id = $1")
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(source_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn backfill_retries_a_transient_fetch_without_persisting_a_partial_article(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let reference = reference("retry-article", "https://mp.weixin.qq.com/s/retry-article");
    let source = source(&pool, source_id).await;
    let job_id = enqueue_backfill(&pool, &source, &reference).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = worker(
        pool.clone(),
        "backfill-retry-worker",
        FakeAcquirer {
            page: page(&reference, "Not persisted"),
            timeout: true,
            no_account: false,
            calls: calls.clone(),
        },
    );

    let result = worker
        .run_once(Utc::now())
        .await
        .expect("worker should record the transient backfill failure");
    let WorkerRun::Completed { job, outcome } = result else {
        panic!("a due backfill job should be completed")
    };

    assert_eq!(job.id(), job_id);
    assert!(matches!(outcome, JobExecution::Retry { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(job.status(), JobStatus::RetryWait);
    assert_eq!(job.failure_count(), 1);
    assert_eq!(
        job.last_error(),
        Some("article backfill acquisition failed temporarily")
    );
    let article_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM articles WHERE source_id = $1 AND review_id = $2")
            .bind(source_id.as_uuid())
            .bind("retry-article")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(article_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "retry_wait"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn backfill_waits_for_account_enrollment_instead_of_becoming_terminal(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let reference = reference(
        "account-needed",
        "https://mp.weixin.qq.com/s/account-needed",
    );
    let source = source(&pool, source_id).await;
    let job_id = enqueue_backfill(&pool, &source, &reference).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = worker(
        pool.clone(),
        "backfill-account-worker",
        FakeAcquirer {
            page: page(&reference, "Waiting for account"),
            timeout: false,
            no_account: true,
            calls: calls.clone(),
        },
    );

    let result = worker
        .run_once(Utc::now())
        .await
        .expect("worker should keep the backfill retryable");
    let WorkerRun::Completed { job, outcome } = result else {
        panic!("a due backfill job should be completed")
    };

    assert_eq!(job.id(), job_id);
    assert!(matches!(outcome, JobExecution::Retry { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(job.status(), JobStatus::RetryWait);
    assert_eq!(job.failure_count(), 1);
    assert_eq!(
        job.last_error(),
        Some("article backfill is waiting for a usable WeRead account")
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn backfill_defers_without_fetching_during_quiet_hours(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let reference = reference(
        "quiet-backfill",
        "https://mp.weixin.qq.com/s/quiet-backfill",
    );
    let source = source(&pool, source_id).await;
    let job_id = enqueue_backfill(&pool, &source, &reference).await;
    let lease = claim(&pool, "quiet-backfill-worker", Utc::now()).await;
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let (quiet_start, _) = database_now
        .time()
        .overflowing_sub_signed(Duration::hours(1));
    let (quiet_end, _) = database_now
        .time()
        .overflowing_add_signed(Duration::hours(1));
    let quiet_hours = QuietHours::new(chrono_tz::UTC, quiet_start, quiet_end).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = handler_with_config(
        &pool,
        FakeAcquirer {
            page: page(&reference, "Should not fetch"),
            timeout: false,
            no_account: false,
            calls: calls.clone(),
        },
        ArticleBackfillJobHandlerConfig::default().with_quiet_hours(Some(quiet_hours)),
    );

    let outcome = handler.execute(&lease, Utc::now()).await;

    assert!(matches!(outcome, JobExecution::Deferred { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "running"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn an_expired_backfill_lease_is_recovered_and_completed_by_the_next_worker(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let reference = reference(
        "crashed-article",
        "https://mp.weixin.qq.com/s/crashed-article",
    );
    let source = source(&pool, source_id).await;
    let job_id = enqueue_backfill(&pool, &source, &reference).await;
    let abandoned = claim(&pool, "crashed-backfill-worker", Utc::now()).await;
    assert_eq!(abandoned.job.id(), job_id);
    sqlx::query(
        "UPDATE jobs SET lease_until = clock_timestamp() - interval '1 second', heartbeat_at = clock_timestamp() - interval '1 second' WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .unwrap();

    let worker = worker(
        pool.clone(),
        "recovery-backfill-worker",
        FakeAcquirer {
            page: page(&reference, "Recovered title"),
            timeout: false,
            no_account: false,
            calls: Arc::new(AtomicUsize::new(0)),
        },
    );
    let result = worker
        .run_once(Utc::now())
        .await
        .expect("the next worker should recover the abandoned job");
    let WorkerRun::Completed { job, outcome } = result else {
        panic!("an expired backfill should be claimed after recovery")
    };

    assert_eq!(job.id(), job_id);
    assert_eq!(outcome, JobExecution::Committed);
    assert_eq!(job.status(), JobStatus::Succeeded);
    assert_eq!(job.claim_count(), 2);
    assert_eq!(job.failure_count(), 1);
    let article_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM articles WHERE source_id = $1 AND review_id = $2")
            .bind(source_id.as_uuid())
            .bind("crashed-article")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(article_count, 1);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn active_backfill_jobs_are_deduplicated_by_source_and_review_id(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let source = source(&pool, source_id).await;
    let reference = reference(
        "duplicate-article",
        "https://mp.weixin.qq.com/s/duplicate-article",
    );
    let now = Utc::now() - Duration::seconds(1);
    let spec = article_backfill_job(&source, &reference, now).expect("URL should be backfillable");
    let repository = PostgresJobRepository::new(pool.clone());
    let first = repository.enqueue(spec.clone()).await.unwrap();
    let first_id = match first {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { .. } => panic!("first backfill should be inserted"),
    };
    let second = repository.enqueue(spec).await.unwrap();

    assert!(matches!(
        second,
        EnqueueResult::AlreadyActive { job_id } if job_id == first_id
    ));
    let active_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE dedupe_key = $1 AND status IN ('queued', 'running', 'retry_wait', 'deferred')",
    )
    .bind(format!("article_backfill:{source_id}:duplicate-article"))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(active_count, 1);
}

fn handler(pool: &PgPool, acquirer: FakeAcquirer) -> BackfillHandler {
    handler_with_config(
        pool,
        acquirer,
        ArticleBackfillJobHandlerConfig::new(Duration::seconds(30)).unwrap(),
    )
}

fn handler_with_config(
    pool: &PgPool,
    acquirer: FakeAcquirer,
    config: ArticleBackfillJobHandlerConfig,
) -> BackfillHandler {
    handler_with_archiver(pool, acquirer, config, None)
}

fn handler_with_archiver(
    pool: &PgPool,
    acquirer: FakeAcquirer,
    config: ArticleBackfillJobHandlerConfig,
    asset_archiver: Option<AssetArchiveService>,
) -> BackfillHandler {
    ArticleBackfillJobHandler::new(
        ArticleBackfillJobHandlerDependencies {
            sources: PostgresSourceRepository::new(pool.clone()),
            articles: PostgresArticleRepository::new(pool.clone()),
            unit_of_work: UnitOfWorkFactory::new(pool.clone()),
            acquirer,
            sync_service: werrss::application::sync_service::SyncService::new(),
            asset_archiver,
        },
        config,
    )
}

async fn asset_archiver_fixture(
    requests: usize,
) -> (AssetArchiveService, tokio::task::JoinHandle<()>, String) {
    asset_archiver_fixture_with_policy(requests, AssetCachePolicy::default()).await
}

async fn asset_archiver_fixture_with_policy(
    requests: usize,
    policy: AssetCachePolicy,
) -> (AssetArchiveService, tokio::task::JoinHandle<()>, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("asset fixture listener should bind");
    let address = listener
        .local_addr()
        .expect("asset fixture listener should have an address");
    let server = tokio::spawn(async move {
        for _ in 0..requests {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("asset fixture request should connect");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream
                    .read(&mut buffer)
                    .await
                    .expect("asset fixture request should be readable");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let body = b"\x89PNG\r\n\x1a\nasset-fixture";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("asset fixture response headers should be writable");
            stream
                .write_all(body)
                .await
                .expect("asset fixture response body should be writable");
        }
    });

    let client = reqwest::Client::builder()
        .no_proxy()
        .resolve("assets.example.test", address)
        .build()
        .expect("asset fixture client should be constructible");
    let asset_url = "http://assets.example.test/image.png".to_owned();
    let service = AssetArchiveService::with_client_for_test(policy, None, client);
    (service, server, asset_url)
}

fn worker(
    pool: PgPool,
    owner: &str,
    acquirer: FakeAcquirer,
) -> Worker<PostgresJobRepository, UnitOfWorkFactory, BackfillHandler> {
    let queue = PostgresJobRepository::new(pool.clone());
    Worker::new(
        JobService::new(
            queue,
            JobServiceConfig::new(owner, Duration::minutes(5), 10).unwrap(),
        ),
        UnitOfWorkFactory::new(pool.clone()),
        handler(&pool, acquirer),
        WorkerConfig::new(vec![JobType::ArticleBackfill], StdDuration::from_secs(1)).unwrap(),
    )
    .unwrap()
}

async fn source(pool: &PgPool, source_id: SourceId) -> Source {
    PostgresSourceRepository::new(pool.clone())
        .find(source_id)
        .await
        .unwrap()
        .expect("source should exist")
}

async fn create_source(factory: &UnitOfWorkFactory, source_id: SourceId) {
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .source()
        .insert(NewSource {
            id: source_id,
            book_id: format!("book-{source_id}"),
            display_name: "Backfill test source".to_owned(),
            article_url: Some("https://mp.weixin.qq.com/s/source".parse().unwrap()),
            enabled: true,
            sync_interval: Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: at(0),
            priority: 10,
            max_attempts: 3,
        })
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
}

async fn insert_incomplete_article(
    factory: &UnitOfWorkFactory,
    source_id: SourceId,
    reference: &WeReadArticleReference,
) {
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .articles()
        .upsert(NewArticle {
            source_id,
            review_id: reference.review_id.clone(),
            title: reference.title.clone().unwrap(),
            author: None,
            summary: None,
            cover_url: None,
            original_url: reference.article_url.clone(),
            published_at: reference.published_at.unwrap(),
            content_html: String::new(),
            content_hash: None,
            observation_version: ArticleObservationVersion::from_u64(1),
            fetched_at: at(100),
        })
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
}

async fn enqueue_backfill(
    pool: &PgPool,
    source: &Source,
    reference: &WeReadArticleReference,
) -> Uuid {
    let now = Utc::now() - Duration::seconds(1);
    let result = PostgresJobRepository::new(pool.clone())
        .enqueue(article_backfill_job(source, reference, now).unwrap())
        .await
        .unwrap();
    match result {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { job_id } => job_id,
    }
}

async fn claim(pool: &PgPool, owner: &str, now: DateTime<Utc>) -> JobLease {
    PostgresJobRepository::new(pool.clone())
        .claim_next(
            owner,
            now,
            Duration::minutes(5),
            &[JobType::ArticleBackfill],
        )
        .await
        .unwrap()
        .expect("backfill should be claimable")
}

async fn wait_for_advisory_lock(pool: &PgPool) {
    for _ in 0..100 {
        let held: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid = 2147483646)",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if held {
            return;
        }
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    panic!("backfill did not reach the article write lock");
}

fn reference(review_id: &str, url: &str) -> WeReadArticleReference {
    WeReadArticleReference {
        review_id: review_id.to_owned(),
        article_url: Some(url.parse::<VerifiedWechatArticleUrl>().unwrap()),
        title: Some("List title".to_owned()),
        summary: Some("List summary".to_owned()),
        author: Some("List author".to_owned()),
        cover_url: None,
        published_at: Some(at(1_700_000_000)),
    }
}

fn page(reference: &WeReadArticleReference, title: &str) -> ExtractedArticlePage {
    ExtractedArticlePage {
        canonical_url: reference.article_url.clone().unwrap(),
        title: title.to_owned(),
        author: Some("Page author".to_owned()),
        summary: Some("Page summary".to_owned()),
        published_at: None,
        content_html: "<p>repaired</p>".to_owned(),
        cover_url: None,
    }
}

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}
