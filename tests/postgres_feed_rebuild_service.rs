use chrono::{DateTime, Duration, TimeZone, Utc};
use sqlx::PgPool;
use url::Url;
use uuid::Uuid;
use werrss::{
    application::{
        feed_rebuild_handler::{FeedRebuildJobHandler, FeedRebuildJobHandlerConfig},
        feed_rebuild_service::{
            FeedRebuildConfig, FeedRebuildDependencies, FeedRebuildError, FeedRebuildOutcome,
            FeedRebuildService,
        },
        job_service::{JobService, JobServiceConfig},
        worker::{JobExecution, Worker, WorkerConfig, WorkerRun},
    },
    domain::{
        article::NewArticle,
        job::{JobType, NewJob},
        source::{NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
    },
    persistence::{
        repositories::{
            article_repository::{
                ArticleRepository, ArticleTransactionRepository, PostgresArticleRepository,
            },
            feed_cache_repository::{
                FeedBuildLeaseRepository, FeedCacheRepository, PostgresFeedBuildLeaseRepository,
                PostgresFeedCacheRepository,
            },
            job_repository::{EnqueueResult, JobQueue, PostgresJobRepository},
            source_repository::{PostgresSourceRepository, SourceTransactionRepository},
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rebuild_renders_normalized_articles_and_releases_the_build_lease(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    insert_article(&pool, &factory, source_id, "review-1", "Article one", 200).await;
    insert_article(&pool, &factory, source_id, "review-2", "Article two", 100).await;

    let service = rebuild_service(&pool, &factory, "builder-a");
    let database_time_before: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .expect("database clock should be queryable");
    let result = service
        .rebuild(source_id)
        .await
        .expect("feed rebuild should succeed");
    assert_eq!(
        result,
        FeedRebuildOutcome::Published {
            feed_revision: werrss::domain::source::FeedRevision::zero()
        }
    );

    let cache = PostgresFeedCacheRepository::new(pool.clone())
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .expect("rebuild should publish a cache row");
    assert!(cache.is_fresh());
    let database_time_after: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .expect("database clock should be queryable");
    assert!(cache.cache().generated_at() >= database_time_before);
    assert!(cache.cache().generated_at() <= database_time_after);
    assert_eq!(
        cache.cache().expires_at(),
        cache.cache().generated_at() + Duration::minutes(30)
    );
    let xml = String::from_utf8(cache.cache().xml_bytes().to_vec())
        .expect("rendered feed should be UTF-8");
    assert!(xml.contains("<guid isPermaLink=\"false\">review-1</guid>"));
    assert!(xml.contains("<title>Article one</title>"));
    assert!(xml.contains("<title>Article two</title>"));
    assert!(
        xml.find("<guid isPermaLink=\"false\">review-1</guid>")
            < xml.find("<guid isPermaLink=\"false\">review-2</guid>")
    );

    let lease_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM feed_build_leases WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("lease count should be queryable");
    assert_eq!(lease_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rebuild_emits_absolute_asset_urls_when_public_root_is_configured(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    insert_article_with_content(
        &pool,
        &factory,
        source_id,
        "asset-review",
        "Asset article",
        200,
        "<p>Body</p><img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
    )
    .await;

    let config = FeedRebuildConfig::new(
        Duration::minutes(5),
        Duration::minutes(30),
        "https://rss.example.test/feed.xml",
        "Integration test feed",
    )
    .expect("rebuild config should be valid")
    .with_asset_url_root(
        Url::parse("https://rss.example.test/werrss").expect("asset root should be valid"),
    );
    let service = FeedRebuildService::new(
        FeedRebuildDependencies::new(
            PostgresSourceRepository::new(pool.clone()),
            PostgresArticleRepository::new(pool.clone()),
            PostgresFeedBuildLeaseRepository::new(pool.clone()),
            factory,
        ),
        config,
        "builder-a",
    )
    .expect("rebuild service should be valid");

    service
        .rebuild(source_id)
        .await
        .expect("feed rebuild should succeed");

    let cache = PostgresFeedCacheRepository::new(pool)
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .expect("rebuild should publish a cache row");
    let xml = String::from_utf8(cache.cache().xml_bytes().to_vec())
        .expect("rendered feed should be UTF-8");
    assert!(xml.contains(
        "src=\"https://rss.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\""
    ));
    assert!(!xml.contains("src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\""));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rebuild_for_claimed_job_completes_cache_and_job_in_one_unit_of_work(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let jobs = PostgresJobRepository::new(pool.clone());
    let job_id = match jobs
        .enqueue(NewJob {
            job_type: JobType::FeedRebuild,
            source_id: Some(source_id.as_uuid()),
            priority: 1,
            run_after: timestamp(0),
            max_attempts: 3,
            payload: serde_json::json!({"source_id": source_id.as_uuid()}),
            dedupe_key: format!("feed-rebuild:{source_id}"),
            now: timestamp(0),
        })
        .await
        .expect("feed rebuild job should enqueue")
    {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { .. } => panic!("test job should be unique"),
    };
    let lease = jobs
        .claim_next(
            "worker-a",
            timestamp(1),
            Duration::minutes(5),
            &[JobType::FeedRebuild],
        )
        .await
        .expect("feed rebuild job should claim")
        .expect("feed rebuild job should be due");

    let service = rebuild_service(&pool, &factory, "builder-a");
    assert_eq!(
        service
            .rebuild_for_job(&lease)
            .await
            .expect("feed rebuild should complete its claimed job"),
        FeedRebuildOutcome::Published {
            feed_revision: werrss::domain::source::FeedRevision::zero()
        }
    );

    let job = jobs
        .find(job_id)
        .await
        .expect("job lookup should succeed")
        .expect("completed job should remain");
    assert_eq!(job.status(), werrss::domain::job::JobStatus::Succeeded);
    assert_eq!(job.claim_count(), 1);
    assert_eq!(job.failure_count(), 0);
    assert!(PostgresFeedCacheRepository::new(pool)
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .is_some());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_rebuild_handler_completes_cache_and_job_without_double_completion(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    insert_article(
        &pool,
        &factory,
        source_id,
        "handler-review",
        "Handler article",
        100,
    )
    .await;

    let jobs = PostgresJobRepository::new(pool.clone());
    let job_id = match jobs
        .enqueue(NewJob {
            job_type: JobType::FeedRebuild,
            source_id: Some(source_id.as_uuid()),
            priority: 1,
            run_after: timestamp(0),
            max_attempts: 3,
            payload: serde_json::json!({"source_id": source_id.as_uuid()}),
            dedupe_key: format!("feed-rebuild-handler:{source_id}"),
            now: timestamp(0),
        })
        .await
        .expect("feed rebuild job should enqueue")
    {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { .. } => panic!("feed rebuild job should be unique"),
    };

    let service = rebuild_service(&pool, &factory, "builder-a");
    let handler = FeedRebuildJobHandler::new(
        service,
        FeedRebuildJobHandlerConfig::new(Duration::minutes(1))
            .expect("handler configuration should be valid"),
    );
    let worker = Worker::new(
        JobService::new(
            jobs.clone(),
            JobServiceConfig::new("worker-a", Duration::minutes(5), 10)
                .expect("job service configuration should be valid"),
        ),
        factory,
        handler,
        WorkerConfig::new(
            vec![JobType::FeedRebuild],
            std::time::Duration::from_secs(1),
        )
        .expect("worker configuration should be valid"),
    )
    .expect("worker should be valid");

    let WorkerRun::Completed { job, outcome } = worker
        .run_once(timestamp(1))
        .await
        .expect("feed rebuild worker should complete")
    else {
        panic!("feed rebuild job should be claimed")
    };
    assert_eq!(job.id(), job_id);
    assert_eq!(outcome, JobExecution::Committed);
    assert_eq!(job.status(), werrss::domain::job::JobStatus::Succeeded);
    let cache = PostgresFeedCacheRepository::new(pool)
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .expect("handler should publish cache");
    assert!(String::from_utf8_lossy(cache.cache().xml_bytes()).contains("Handler article"));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_rebuild_handler_permanently_fails_when_source_is_missing(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    let jobs = PostgresJobRepository::new(pool.clone());
    jobs.enqueue(NewJob {
        job_type: JobType::FeedRebuild,
        source_id: Some(source_id.as_uuid()),
        priority: 1,
        run_after: timestamp(0),
        max_attempts: 3,
        payload: serde_json::json!({"source_id": source_id.as_uuid()}),
        dedupe_key: format!("feed-rebuild-missing:{source_id}"),
        now: timestamp(0),
    })
    .await
    .expect("feed rebuild job should enqueue");

    let handler = FeedRebuildJobHandler::new(
        rebuild_service(&pool, &factory, "builder-a"),
        FeedRebuildJobHandlerConfig::default(),
    );
    let worker = Worker::new(
        JobService::new(
            jobs.clone(),
            JobServiceConfig::new("worker-a", Duration::minutes(5), 10)
                .expect("job service configuration should be valid"),
        ),
        factory,
        handler,
        WorkerConfig::new(
            vec![JobType::FeedRebuild],
            std::time::Duration::from_secs(1),
        )
        .expect("worker configuration should be valid"),
    )
    .expect("worker should be valid");

    let WorkerRun::Completed { job, outcome } = worker
        .run_once(timestamp(1))
        .await
        .expect("missing source should be a handled permanent failure")
    else {
        panic!("missing source job should be claimed")
    };
    assert_eq!(job.status(), werrss::domain::job::JobStatus::Failed);
    assert_eq!(job.failure_count(), 0);
    assert_eq!(
        outcome,
        JobExecution::Failed {
            error: "feed rebuild job data is invalid".to_owned()
        }
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rebuild_reports_an_active_builder_without_reading_or_publishing(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let leases = PostgresFeedBuildLeaseRepository::new(pool.clone());
    leases
        .acquire_build(source_id, "builder-a", Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("first builder should acquire the lease");

    let service = rebuild_service(&pool, &factory, "builder-b");
    assert_eq!(
        service.rebuild(source_id).await.unwrap(),
        FeedRebuildOutcome::AlreadyActive
    );
    assert!(
        PostgresFeedCacheRepository::new(pool)
            .get(source_id)
            .await
            .unwrap()
            .is_none(),
        "a blocked builder must not publish a cache"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rebuild_releases_the_lease_when_the_source_disappears(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    let service = rebuild_service(&pool, &factory, "builder-a");

    let result = service.rebuild(source_id).await;
    assert!(
        matches!(
            result,
            Err(FeedRebuildError::SourceNotFound { source_id: missing }) if missing == source_id
        ),
        "unexpected rebuild result: {result:?}"
    );

    let lease_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM feed_build_leases WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("lease count should be queryable");
    assert_eq!(lease_count, 0);
}

fn rebuild_service(
    pool: &PgPool,
    factory: &UnitOfWorkFactory,
    owner: &str,
) -> FeedRebuildService<
    PostgresSourceRepository,
    PostgresArticleRepository,
    PostgresFeedBuildLeaseRepository,
    UnitOfWorkFactory,
> {
    FeedRebuildService::new(
        FeedRebuildDependencies::new(
            PostgresSourceRepository::new(pool.clone()),
            PostgresArticleRepository::new(pool.clone()),
            PostgresFeedBuildLeaseRepository::new(pool.clone()),
            factory.clone(),
        ),
        FeedRebuildConfig::new(
            Duration::minutes(5),
            Duration::minutes(30),
            "https://rss.example.test/feed.xml",
            "Integration test feed",
        )
        .expect("rebuild config should be valid"),
        owner,
    )
    .expect("rebuild service should be valid")
}

async fn create_source(factory: &UnitOfWorkFactory, source_id: SourceId) {
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    unit_of_work
        .source()
        .insert(NewSource {
            id: source_id,
            book_id: format!("book-{source_id}"),
            display_name: "Integration source".to_owned(),
            article_url: Some(
                "https://mp.weixin.qq.com/s/source"
                    .parse::<VerifiedWechatArticleUrl>()
                    .expect("source URL should be valid"),
            ),
            enabled: true,
            sync_interval: Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: timestamp(0),
            priority: 0,
            max_attempts: 3,
        })
        .await
        .expect("source should be inserted");
    unit_of_work.commit().await.expect("source should commit");
}

async fn insert_article(
    pool: &PgPool,
    factory: &UnitOfWorkFactory,
    source_id: SourceId,
    review_id: &str,
    title: &str,
    published_at: i64,
) {
    insert_article_with_content(
        pool,
        factory,
        source_id,
        review_id,
        title,
        published_at,
        &format!("<p>{title}</p>"),
    )
    .await;
}

async fn insert_article_with_content(
    pool: &PgPool,
    factory: &UnitOfWorkFactory,
    source_id: SourceId,
    review_id: &str,
    title: &str,
    published_at: i64,
    content_html: &str,
) {
    let repository = PostgresArticleRepository::new(pool.clone());
    let observation_version = repository
        .allocate_observation_version()
        .await
        .expect("observation version should be allocated");
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    unit_of_work
        .articles()
        .upsert(NewArticle {
            source_id,
            review_id: review_id.to_owned(),
            title: title.to_owned(),
            author: Some("Author".to_owned()),
            summary: Some("Summary".to_owned()),
            cover_url: None,
            original_url: Some(
                format!("https://mp.weixin.qq.com/s/{review_id}")
                    .parse::<VerifiedWechatArticleUrl>()
                    .expect("article URL should be valid"),
            ),
            published_at: timestamp(published_at),
            content_html: content_html.to_owned(),
            content_hash: Some(format!("hash-{review_id}")),
            observation_version,
            fetched_at: timestamp(published_at + 10),
        })
        .await
        .expect("article should be inserted");
    unit_of_work.commit().await.expect("article should commit");
}

fn timestamp(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}
