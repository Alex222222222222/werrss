//! PostgreSQL integration coverage for source-sync job finalization.

use std::{collections::HashMap, sync::Arc, time::Duration as StdDuration};

use chrono::{Duration, TimeZone, Utc};
use sqlx::{PgPool, Row};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};
use uuid::Uuid;
use werrss::{
    acquisition::{
        article_page::{ArticlePageError, ExtractedArticlePage},
        weread::{WeReadAdapterError, WeReadArticleReference},
    },
    application::{
        asset_archive_service::AssetArchiveService,
        source_sync_handler::{
            SourceSyncAcquirer, SourceSyncJobHandler, SourceSyncJobHandlerConfig,
            SourceSyncJobHandlerDependencies, SourceSyncReferences,
        },
        sync_service::SyncAcquisitionError,
        worker::{JobExecution, JobHandler},
    },
    archive::asset_store::AssetCachePolicy,
    domain::{
        job::{JobType, NewJob},
        pacing::QuietHours,
        source::{NewSource, SchedulingGate, SourceId, SourcePatch, VerifiedWechatArticleUrl},
        sync::SyncOutcome,
    },
    persistence::{
        repositories::{
            article_repository::PostgresArticleRepository,
            job_repository::{JobLease, JobQueue, PostgresJobRepository},
            source_repository::{PostgresSourceRepository, SourceTransactionRepository},
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

struct FakeAcquirer {
    references: Vec<WeReadArticleReference>,
    pages: HashMap<String, ExtractedArticlePage>,
    timeout_articles: Vec<String>,
    blocked_articles: Vec<String>,
    authentication_error_articles: Vec<String>,
    authentication_error: bool,
    list_started: Option<Arc<Notify>>,
    list_release: Option<Arc<Notify>>,
    fetch_started: Option<Arc<Notify>>,
    fetch_release: Option<Arc<Notify>>,
}

impl FakeAcquirer {
    fn successful(reference: WeReadArticleReference, page: ExtractedArticlePage) -> Self {
        let mut pages = HashMap::new();
        pages.insert(reference.review_id.clone(), page);
        Self {
            references: vec![reference],
            pages,
            timeout_articles: Vec::new(),
            blocked_articles: Vec::new(),
            authentication_error_articles: Vec::new(),
            authentication_error: false,
            list_started: None,
            list_release: None,
            fetch_started: None,
            fetch_release: None,
        }
    }
}

#[async_trait::async_trait]
impl SourceSyncAcquirer for FakeAcquirer {
    async fn list_article_references(
        &self,
        _source: &werrss::domain::source::Source,
    ) -> Result<SourceSyncReferences, SyncAcquisitionError> {
        if let Some(list_started) = &self.list_started {
            list_started.notify_one();
        }
        if let Some(list_release) = &self.list_release {
            list_release.notified().await;
        }
        if self.authentication_error {
            Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::AuthenticationExpired { code: 401 },
            ))
        } else {
            Ok(SourceSyncReferences::new(self.references.clone(), None))
        }
    }

    async fn fetch_article(
        &self,
        _source: &werrss::domain::source::Source,
        reference: &WeReadArticleReference,
        _account_id: Option<werrss::domain::credentials::WeReadAccountId>,
    ) -> Result<ExtractedArticlePage, SyncAcquisitionError> {
        if let Some(fetch_started) = &self.fetch_started {
            fetch_started.notify_one();
        }
        if let Some(fetch_release) = &self.fetch_release {
            fetch_release.notified().await;
        }
        if self
            .blocked_articles
            .iter()
            .any(|id| id == &reference.review_id)
        {
            return Err(SyncAcquisitionError::ArticlePage(
                ArticlePageError::VerificationRequired,
            ));
        }
        if self
            .timeout_articles
            .iter()
            .any(|id| id == &reference.review_id)
        {
            return Err(SyncAcquisitionError::ArticlePage(
                ArticlePageError::OperationTimedOut,
            ));
        }
        if self
            .authentication_error_articles
            .iter()
            .any(|id| id == &reference.review_id)
        {
            return Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::AuthenticationExpired { code: 401 },
            ));
        }
        self.pages
            .get(&reference.review_id)
            .cloned()
            .ok_or_else(|| {
                SyncAcquisitionError::WeRead(WeReadAdapterError::Protocol(
                    "test article is missing".to_owned(),
                ))
            })
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_commits_article_run_schedule_and_feed_rebuild(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;

    let reference = reference("review-success", "https://mp.weixin.qq.com/s/success");
    let page = page("https://mp.weixin.qq.com/s/success", "Fetched title");
    let handler = handler(
        pool.clone(),
        FakeAcquirer::successful(reference, page),
        SourceSyncJobHandlerConfig::new(Duration::minutes(1), Duration::minutes(5)).unwrap(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let job_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_status, "succeeded");
    let article_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM articles WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(article_count, 1);
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
    let run = sqlx::query(
        "SELECT outcome, articles_seen, articles_created, articles_updated, articles_failed, archived_articles, archived_assets FROM sync_runs WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(run.get::<String, _>("outcome"), "succeeded");
    assert_eq!(run.get::<i64, _>("articles_seen"), 1);
    assert_eq!(run.get::<i64, _>("articles_created"), 1);
    assert_eq!(run.get::<i64, _>("articles_updated"), 0);
    assert_eq!(run.get::<i64, _>("articles_failed"), 0);
    assert_eq!(run.get::<i64, _>("archived_articles"), 1);
    assert_eq!(run.get::<i64, _>("archived_assets"), 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_with_asset_archiving_does_not_bump_revision_for_unchanged_article(
    pool: PgPool,
) {
    let (asset_archiver, asset_server, asset_url) = asset_archiver_fixture(2).await;
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;

    let reference = reference(
        "asset-idempotent",
        "https://mp.weixin.qq.com/s/asset-idempotent",
    );
    let mut article_page = page(
        "https://mp.weixin.qq.com/s/asset-idempotent",
        "Asset idempotent article",
    );
    article_page.content_html = format!("<p>content</p><img src=\"{asset_url}\">");

    let first_job_id = enqueue_source_sync(&pool, source_id).await;
    let first_lease = claim(&pool, Utc::now()).await;
    let first_handler = handler_with_archiver(
        pool.clone(),
        FakeAcquirer::successful(reference.clone(), article_page.clone()),
        SourceSyncJobHandlerConfig::default(),
        Some(asset_archiver.clone()),
    );
    assert_eq!(
        first_handler.execute(&first_lease, Utc::now()).await,
        JobExecution::Committed
    );

    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, next_allowed_at, exhausted, last_outcome)
         VALUES ($1, $2, 2, clock_timestamp() + interval '1 hour', TRUE, 'inconclusive')
         ON CONFLICT (source_id, review_id) DO UPDATE
         SET attempts = EXCLUDED.attempts, next_allowed_at = EXCLUDED.next_allowed_at,
             exhausted = EXCLUDED.exhausted, last_outcome = EXCLUDED.last_outcome",
    )
    .bind(source_id.as_uuid())
    .bind("asset-idempotent")
    .execute(&pool)
    .await
    .unwrap();

    let second_job_id = enqueue_source_sync(&pool, source_id).await;
    let second_lease = claim(&pool, Utc::now()).await;
    let second_handler = handler_with_archiver(
        pool.clone(),
        FakeAcquirer::successful(reference, article_page),
        SourceSyncJobHandlerConfig::default(),
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

    let second_run =
        sqlx::query("SELECT articles_created, articles_updated FROM sync_runs WHERE job_id = $1")
            .bind(second_job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(second_run.get::<i64, _>("articles_created"), 0);
    assert_eq!(second_run.get::<i64, _>("articles_updated"), 0);

    let stored_html: String = sqlx::query_scalar(
        "SELECT content_html FROM articles WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-idempotent")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(stored_html.contains("/assets/"));
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<chrono::DateTime<Utc>>, bool)>(
            "SELECT attempts, next_allowed_at, exhausted
             FROM asset_article_refresh_lineage WHERE source_id = $1 AND review_id = $2",
        )
        .bind(source_id.as_uuid())
        .bind("asset-idempotent")
        .fetch_one(&pool)
        .await
        .unwrap(),
        (0, None, false),
        "a complete accepted normal acquisition resets refresh lineage"
    );

    let first_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(first_job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(first_status, "succeeded");
    asset_server
        .await
        .expect("asset fixture should serve both fetches");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_preserves_cached_article_when_asset_fetch_is_incomplete(pool: PgPool) {
    let (asset_archiver, asset_server, asset_url) = asset_archiver_fixture(1).await;
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;

    let reference = reference(
        "asset-fetch-failure",
        "https://mp.weixin.qq.com/s/asset-fetch-failure",
    );
    let mut article_page = page(
        "https://mp.weixin.qq.com/s/asset-fetch-failure",
        "Asset fetch failure",
    );
    article_page.content_html = format!("<p>content</p><img src=\"{asset_url}\">");

    enqueue_source_sync(&pool, source_id).await;
    let first_lease = claim(&pool, Utc::now()).await;
    let first_handler = handler_with_archiver(
        pool.clone(),
        FakeAcquirer::successful(reference.clone(), article_page.clone()),
        SourceSyncJobHandlerConfig::default(),
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
    .bind("asset-fetch-failure")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cached_html.contains("/assets/"));
    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts, next_allowed_at, exhausted, last_outcome)
         VALUES ($1, $2, 2, clock_timestamp() + interval '1 hour', TRUE, 'inconclusive')
         ON CONFLICT (source_id, review_id) DO UPDATE
         SET attempts = EXCLUDED.attempts, next_allowed_at = EXCLUDED.next_allowed_at,
             exhausted = EXCLUDED.exhausted, last_outcome = EXCLUDED.last_outcome",
    )
    .bind(source_id.as_uuid())
    .bind("asset-fetch-failure")
    .execute(&pool)
    .await
    .unwrap();
    let lineage_before = sqlx::query_as::<_, (i64, Option<chrono::DateTime<Utc>>, bool)>(
        "SELECT attempts, next_allowed_at, exhausted
         FROM asset_article_refresh_lineage WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-fetch-failure")
    .fetch_one(&pool)
    .await
    .unwrap();

    enqueue_source_sync(&pool, source_id).await;
    let second_lease = claim(&pool, Utc::now()).await;
    let second_handler = handler_with_archiver(
        pool.clone(),
        FakeAcquirer::successful(reference, article_page),
        SourceSyncJobHandlerConfig::default(),
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
    .bind("asset-fetch-failure")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted_html, cached_html);

    let relationship_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM article_assets WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-fetch-failure")
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
        .bind("asset-fetch-failure")
        .fetch_one(&pool)
        .await
        .unwrap(),
        lineage_before,
        "an incomplete source sync must preserve the exhausted refresh lineage"
    );

    asset_server
        .await
        .expect("asset fixture should serve the initial fetch");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_preserves_cached_article_when_asset_capacity_is_exceeded(pool: PgPool) {
    let (initial_archiver, initial_server, initial_url) = asset_archiver_fixture(1).await;
    let constrained_policy = AssetCachePolicy::new(
        5,
        StdDuration::from_secs(30),
        10_000_000,
        100,
        100_000_000,
        StdDuration::from_secs(120),
        StdDuration::from_secs(30),
        5,
    )
    .unwrap();
    let (constrained_archiver, constrained_server, constrained_url) =
        asset_archiver_fixture_with_policy_and_body(
            1,
            constrained_policy,
            b"\x89PNG\r\n\x1a\nnew-asset-body",
        )
        .await;
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;

    let reference = reference(
        "asset-capacity-failure",
        "https://mp.weixin.qq.com/s/asset-capacity-failure",
    );
    let mut initial_page = page(
        "https://mp.weixin.qq.com/s/asset-capacity-failure",
        "Asset capacity failure",
    );
    initial_page.content_html = format!("<p>content</p><img src=\"{initial_url}\">");

    enqueue_source_sync(&pool, source_id).await;
    let first_lease = claim(&pool, Utc::now()).await;
    let first_handler = handler_with_archiver(
        pool.clone(),
        FakeAcquirer::successful(reference.clone(), initial_page),
        SourceSyncJobHandlerConfig::default(),
        Some(initial_archiver),
    );
    assert_eq!(
        first_handler.execute(&first_lease, Utc::now()).await,
        JobExecution::Committed
    );

    let cached_html: String = sqlx::query_scalar(
        "SELECT content_html FROM articles WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-capacity-failure")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cached_html.contains("/assets/"));

    let mut constrained_page = page(
        "https://mp.weixin.qq.com/s/asset-capacity-failure",
        "Asset capacity failure",
    );
    constrained_page.content_html = format!("<p>new</p><img src=\"{constrained_url}\">");
    enqueue_source_sync(&pool, source_id).await;
    let second_lease = claim(&pool, Utc::now()).await;
    let second_handler = handler_with_archiver(
        pool.clone(),
        FakeAcquirer::successful(reference, constrained_page),
        SourceSyncJobHandlerConfig::default(),
        Some(constrained_archiver),
    );
    assert_eq!(
        second_handler.execute(&second_lease, Utc::now()).await,
        JobExecution::Committed
    );

    let persisted_html: String = sqlx::query_scalar(
        "SELECT content_html FROM articles WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-capacity-failure")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted_html, cached_html);

    let relationship_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM article_assets WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id.as_uuid())
    .bind("asset-capacity-failure")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(relationship_count, 1);

    initial_server
        .await
        .expect("initial asset fixture should serve one fetch");
    constrained_server
        .await
        .expect("constrained asset fixture should serve one fetch");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_defers_before_upstream_work_during_quiet_hours(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;

    let database_now: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
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
    let handler = handler(
        pool.clone(),
        FakeAcquirer {
            authentication_error: true,
            ..FakeAcquirer::successful(
                reference("quiet", "https://mp.weixin.qq.com/s/quiet"),
                page("https://mp.weixin.qq.com/s/quiet", "Should not fetch"),
            )
        },
        SourceSyncJobHandlerConfig::new(Duration::minutes(1), Duration::minutes(5))
            .unwrap()
            .with_quiet_hours(Some(quiet_hours)),
    );

    let outcome = handler
        .execute(
            &lease,
            Utc.timestamp_opt(1_700_000_000, 0).single().unwrap(),
        )
        .await;
    assert!(matches!(outcome, JobExecution::Deferred { .. }));

    let run_count: i64 = sqlx::query_scalar("SELECT count(*) FROM sync_runs WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        run_count, 0,
        "quiet-hours deferral must precede run creation"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_queues_a_missed_article_backfill_and_completes_the_run(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let reference = reference("review-timeout", "https://mp.weixin.qq.com/s/timeout");
    let mut acquirer = FakeAcquirer::successful(
        reference,
        page("https://mp.weixin.qq.com/s/timeout", "Timeout"),
    );
    acquirer.timeout_articles.push("review-timeout".to_owned());
    let handler = handler(
        pool.clone(),
        acquirer,
        SourceSyncJobHandlerConfig::new(Duration::minutes(2), Duration::minutes(5)).unwrap(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let job = sqlx::query("SELECT status, failure_count FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job.get::<String, _>("status"), "succeeded");
    assert_eq!(job.get::<i64, _>("failure_count"), 0);
    let source = sqlx::query(
        "SELECT scheduling_gate, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(source.get::<String, _>("scheduling_gate"), "ready");
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("failure_cooldown_until")
        .is_none());
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("schedule_reserved_until")
        .is_none());
    let outcome: String = sqlx::query_scalar("SELECT outcome FROM sync_runs WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outcome, SyncOutcome::Succeeded.as_str());
    let run = sqlx::query(
        "SELECT articles_seen, articles_created, articles_updated, articles_failed FROM sync_runs WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(run.get::<i64, _>("articles_seen"), 1);
    assert_eq!(run.get::<i64, _>("articles_created"), 0);
    assert_eq!(run.get::<i64, _>("articles_updated"), 0);
    assert_eq!(run.get::<i64, _>("articles_failed"), 1);
    let backfill = sqlx::query(
        "SELECT status, payload_json, dedupe_key FROM jobs WHERE source_id = $1 AND job_type = 'article_backfill'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(backfill.get::<String, _>("status"), "queued");
    assert_eq!(
        backfill.get::<String, _>("dedupe_key"),
        format!("article_backfill:{source_id}:review-timeout")
    );
    assert_eq!(
        backfill
            .get::<serde_json::Value, _>("payload_json")
            .get("review_id")
            .and_then(serde_json::Value::as_str),
        Some("review-timeout")
    );
    let article_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM articles WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(article_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_persistence_retry_preserves_backfill_candidates(pool: PgPool) {
    sqlx::query("CREATE SEQUENCE test_source_sync_finish_failure_once")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
        CREATE FUNCTION test_fail_first_source_sync_finish() RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF nextval('test_source_sync_finish_failure_once') = 1 THEN
                RAISE EXCEPTION 'intentional transient sync-run persistence failure';
            END IF;
            RETURN NEW;
        END;
        $$
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_fail_first_source_sync_finish_trigger BEFORE UPDATE OF outcome ON sync_runs FOR EACH ROW EXECUTE FUNCTION test_fail_first_source_sync_finish()",
    )
    .execute(&pool)
    .await
    .unwrap();

    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let reference = reference(
        "review-persistence-retry",
        "https://mp.weixin.qq.com/s/retry",
    );
    let mut acquirer = FakeAcquirer::successful(
        reference,
        page("https://mp.weixin.qq.com/s/retry", "Retry candidate"),
    );
    acquirer
        .timeout_articles
        .push("review-persistence-retry".to_owned());
    let handler = handler(
        pool.clone(),
        acquirer,
        SourceSyncJobHandlerConfig::default(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "retry_wait"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT outcome FROM sync_runs WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        SyncOutcome::RetryableFailure.as_str()
    );
    let backfill_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE source_id = $1 AND job_type = 'article_backfill' AND status = 'queued'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(backfill_count, 1);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_preserves_backfill_candidates_when_a_later_article_gates_the_source(
    pool: PgPool,
) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;

    let first = reference(
        "review-before-auth-error",
        "https://mp.weixin.qq.com/s/before-auth",
    );
    let second = reference("review-auth-error", "https://mp.weixin.qq.com/s/auth-error");
    let mut pages = HashMap::new();
    pages.insert(
        first.review_id.clone(),
        page(first.article_url.as_ref().unwrap().as_str(), "Before auth"),
    );
    pages.insert(
        second.review_id.clone(),
        page(second.article_url.as_ref().unwrap().as_str(), "Auth error"),
    );
    let handler = handler(
        pool.clone(),
        FakeAcquirer {
            references: vec![first, second],
            pages,
            timeout_articles: vec!["review-before-auth-error".to_owned()],
            blocked_articles: Vec::new(),
            authentication_error_articles: vec!["review-auth-error".to_owned()],
            authentication_error: false,
            list_started: None,
            list_release: None,
            fetch_started: None,
            fetch_release: None,
        },
        SourceSyncJobHandlerConfig::default(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let backfill = sqlx::query(
        "SELECT payload_json FROM jobs WHERE source_id = $1 AND job_type = 'article_backfill' AND status = 'queued'",
    )
    .bind(source_id.as_uuid())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(backfill.len(), 1);
    assert_eq!(
        backfill[0]
            .get::<serde_json::Value, _>("payload_json")
            .get("review_id")
            .and_then(serde_json::Value::as_str),
        Some("review-before-auth-error")
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "failed"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_queues_backfill_after_article_normalization_fails(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let mut reference = reference(
        "review-missing-published-at",
        "https://mp.weixin.qq.com/s/missing-published-at",
    );
    reference.published_at = None;
    let mut page = page(
        "https://mp.weixin.qq.com/s/missing-published-at",
        "Missing publication time",
    );
    page.published_at = None;
    let handler = handler(
        pool.clone(),
        FakeAcquirer::successful(reference, page),
        SourceSyncJobHandlerConfig::default(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let backfill_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE source_id = $1 AND job_type = 'article_backfill' AND status = 'queued'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(backfill_count, 1);
    let failed_articles: i64 =
        sqlx::query_scalar("SELECT articles_failed FROM sync_runs WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(failed_articles, 1);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn authentication_failure_gates_the_source_and_does_not_retry_the_job(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let handler = handler(
        pool.clone(),
        FakeAcquirer {
            references: Vec::new(),
            pages: HashMap::new(),
            timeout_articles: Vec::new(),
            blocked_articles: Vec::new(),
            authentication_error_articles: Vec::new(),
            authentication_error: true,
            list_started: None,
            list_release: None,
            fetch_started: None,
            fetch_release: None,
        },
        SourceSyncJobHandlerConfig::default(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let source_gate: String =
        sqlx::query_scalar("SELECT scheduling_gate FROM sources WHERE id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(source_gate, "authentication_required");
    let job_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_status, "failed");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn authentication_failure_clears_transient_schedule_without_restoring_stale_due_time(
    pool: PgPool,
) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let list_started = Arc::new(Notify::new());
    let list_release = Arc::new(Notify::new());
    let handler = handler(
        pool.clone(),
        FakeAcquirer {
            authentication_error: true,
            list_started: Some(list_started.clone()),
            list_release: Some(list_release.clone()),
            ..FakeAcquirer::successful(
                reference(
                    "review-auth-schedule",
                    "https://mp.weixin.qq.com/s/auth-schedule",
                ),
                page("https://mp.weixin.qq.com/s/auth-schedule", "Auth schedule"),
            )
        },
        SourceSyncJobHandlerConfig::default(),
    );

    let execution = tokio::spawn(async move { handler.execute(&lease, Utc::now()).await });
    list_started.notified().await;

    let next_fetch_at = Utc.with_ymd_and_hms(2030, 2, 3, 4, 5, 6).single().unwrap();
    let cooldown_until = next_fetch_at + Duration::minutes(10);
    let reservation_until = next_fetch_at + Duration::minutes(20);
    let mut update = factory.begin().await.unwrap();
    update
        .source()
        .update_schedule(
            source_id,
            next_fetch_at,
            Some(cooldown_until),
            Some(reservation_until),
        )
        .await
        .unwrap();
    update.commit().await.unwrap();

    list_release.notify_one();
    assert_eq!(execution.await.unwrap(), JobExecution::Committed);

    let source = sqlx::query(
        "SELECT scheduling_gate, next_fetch_at, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        source.get::<String, _>("scheduling_gate"),
        "authentication_required"
    );
    assert_eq!(
        source.get::<chrono::DateTime<Utc>, _>("next_fetch_at"),
        next_fetch_at
    );
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("failure_cooldown_until")
        .is_none());
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("schedule_reserved_until")
        .is_none());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_finalization_uses_a_sync_interval_edited_during_fetch(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let fetch_started = Arc::new(Notify::new());
    let fetch_release = Arc::new(Notify::new());
    let handler = handler(
        pool.clone(),
        FakeAcquirer {
            fetch_started: Some(fetch_started.clone()),
            fetch_release: Some(fetch_release.clone()),
            ..FakeAcquirer::successful(
                reference(
                    "review-interval-edit",
                    "https://mp.weixin.qq.com/s/interval-edit",
                ),
                page("https://mp.weixin.qq.com/s/interval-edit", "Interval edit"),
            )
        },
        SourceSyncJobHandlerConfig::default(),
    );

    let execution = tokio::spawn(async move { handler.execute(&lease, Utc::now()).await });
    fetch_started.notified().await;

    let mut update = factory.begin().await.unwrap();
    update
        .source()
        .update(
            source_id,
            SourcePatch {
                sync_interval: Some(Duration::hours(2)),
                ..SourcePatch::default()
            },
        )
        .await
        .unwrap();
    update.commit().await.unwrap();

    fetch_release.notify_one();
    assert_eq!(execution.await.unwrap(), JobExecution::Committed);

    let schedule = sqlx::query(
        "SELECT sources.next_fetch_at, sync_runs.finished_at FROM sources JOIN sync_runs ON sync_runs.job_id = $1 WHERE sources.id = $2",
    )
    .bind(job_id)
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let next_fetch_at = schedule.get::<chrono::DateTime<Utc>, _>("next_fetch_at");
    let finished_at = schedule.get::<chrono::DateTime<Utc>, _>("finished_at");
    assert_eq!(next_fetch_at, finished_at + Duration::hours(2));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn blocked_public_acquisition_gates_the_source_and_does_not_retry_the_job(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let reference = reference("review-blocked", "https://mp.weixin.qq.com/s/blocked");
    let mut acquirer = FakeAcquirer::successful(
        reference,
        page("https://mp.weixin.qq.com/s/blocked", "Blocked"),
    );
    acquirer.blocked_articles.push("review-blocked".to_owned());
    let handler = handler(
        pool.clone(),
        acquirer,
        SourceSyncJobHandlerConfig::default(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let source = sqlx::query(
        "SELECT scheduling_gate, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        source.get::<String, _>("scheduling_gate"),
        "risk_controlled"
    );
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("failure_cooldown_until")
        .is_none());
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("schedule_reserved_until")
        .is_none());

    let job_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_status, "failed");
    let outcome: String = sqlx::query_scalar("SELECT outcome FROM sync_runs WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outcome, SyncOutcome::Blocked.as_str());
}

fn handler(
    pool: PgPool,
    acquirer: FakeAcquirer,
    config: SourceSyncJobHandlerConfig,
) -> SourceSyncJobHandler<PostgresSourceRepository, PostgresArticleRepository, FakeAcquirer> {
    handler_with_archiver(pool, acquirer, config, None)
}

fn handler_with_archiver(
    pool: PgPool,
    acquirer: FakeAcquirer,
    config: SourceSyncJobHandlerConfig,
    asset_archiver: Option<AssetArchiveService>,
) -> SourceSyncJobHandler<PostgresSourceRepository, PostgresArticleRepository, FakeAcquirer> {
    SourceSyncJobHandler::new(
        SourceSyncJobHandlerDependencies {
            sources: PostgresSourceRepository::new(pool.clone()),
            articles: PostgresArticleRepository::new(pool.clone()),
            unit_of_work: UnitOfWorkFactory::new(pool),
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
    asset_archiver_fixture_with_policy_and_body(requests, policy, b"\x89PNG\r\n\x1a\nasset-fixture")
        .await
}

async fn asset_archiver_fixture_with_policy_and_body(
    requests: usize,
    policy: AssetCachePolicy,
    body: &'static [u8],
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

async fn create_source(factory: &UnitOfWorkFactory, source_id: SourceId) {
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .source()
        .insert(NewSource {
            id: source_id,
            book_id: format!("book-{source_id}"),
            display_name: "Test source".to_owned(),
            article_url: Some("https://mp.weixin.qq.com/s/source".parse().unwrap()),
            enabled: true,
            sync_interval: Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: Utc::now(),
            priority: 10,
            max_attempts: 3,
        })
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
}

async fn enqueue_source_sync(pool: &PgPool, source_id: SourceId) -> Uuid {
    let repository = PostgresJobRepository::new(pool.clone());
    let job = repository
        .enqueue_immediately(NewJob {
            job_type: JobType::SourceSync,
            source_id: Some(source_id.as_uuid()),
            priority: 10,
            run_after: Utc::now(),
            max_attempts: 3,
            payload: serde_json::json!({"source_id": source_id.to_string()}),
            dedupe_key: format!("source_sync:{source_id}"),
            now: Utc::now(),
        })
        .await
        .unwrap();
    match job {
        werrss::persistence::repositories::job_repository::EnqueueResult::Inserted(job) => job.id(),
        werrss::persistence::repositories::job_repository::EnqueueResult::AlreadyActive {
            job_id,
        } => job_id,
    }
}

async fn claim(pool: &PgPool, now: chrono::DateTime<Utc>) -> JobLease {
    PostgresJobRepository::new(pool.clone())
        .claim_next(
            "source-sync-test",
            now,
            Duration::minutes(5),
            &[JobType::SourceSync],
        )
        .await
        .unwrap()
        .expect("source-sync job should be claimable")
}

fn reference(review_id: &str, url: &str) -> WeReadArticleReference {
    WeReadArticleReference {
        review_id: review_id.to_owned(),
        article_url: Some(url.parse::<VerifiedWechatArticleUrl>().unwrap()),
        title: Some("List title".to_owned()),
        summary: None,
        author: None,
        cover_url: None,
        published_at: Some(Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()),
    }
}

fn page(url: &str, title: &str) -> ExtractedArticlePage {
    ExtractedArticlePage {
        canonical_url: url.parse().unwrap(),
        title: title.to_owned(),
        author: Some("Author".to_owned()),
        summary: Some("Summary".to_owned()),
        published_at: None,
        content_html: "<p>content</p>".to_owned(),
        cover_url: None,
    }
}
