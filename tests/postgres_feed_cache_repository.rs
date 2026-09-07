use chrono::{DateTime, TimeZone, Utc};
use sqlx::PgPool;
use url::Url;
use uuid::Uuid;
use werrss::{
    application::feed_rebuild_service::{
        FeedRebuildConfig, FeedRebuildDependencies, FeedRebuildService,
    },
    application::feed_service::{
        FeedAssetUrlPolicy, FeedDelivery, FeedRebuildJobConfig, FeedRebuildStatus, FeedRequest,
        FeedService, FeedServiceConfig, PostgresFeedRebuildQueue,
    },
    domain::{
        feed::FeedCacheCandidate,
        source::{FeedRevision, SourceId},
    },
    persistence::{
        repositories::article_repository::PostgresArticleRepository,
        repositories::feed_cache_repository::{
            FeedBuildLeaseRepository, FeedCachePublishResult, FeedCacheRepository,
            FeedCacheTransactionRepository, PostgresFeedBuildLeaseRepository,
            PostgresFeedCacheRepository,
        },
        repositories::job_repository::PostgresJobRepository,
        repositories::source_repository::PostgresSourceRepository,
        unit_of_work::UnitOfWorkFactory,
    },
    rss::renderer::{RenderArticle, RenderFeedInput, RssRenderer},
};

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_feed_cache_publishes_and_reads_through_unit_of_work(pool: PgPool) {
    let source_id = insert_source(&pool, 1).await;
    let lease_repository = PostgresFeedBuildLeaseRepository::new(pool.clone());
    let lease = lease_repository
        .acquire_build(source_id, "builder-a", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("source should have no competing builder");

    let initial_generated_at = Utc::now() - chrono::Duration::seconds(1);
    let initial_candidate = candidate_at(
        source_id,
        1,
        initial_generated_at,
        initial_generated_at + chrono::Duration::minutes(30),
        b"<rss><channel/></rss>",
        "etag-1",
    );
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .feed_cache()
        .publish_if_current(initial_candidate, lease.owner(), lease.token())
        .await
        .expect("cache publication should succeed");
    let published = match result {
        FeedCachePublishResult::Published(cache) => cache,
        other => panic!("expected publication, got {other:?}"),
    };
    assert_eq!(published.source_id(), source_id);
    assert_eq!(published.feed_revision(), FeedRevision::from_u64(1));
    assert_eq!(published.xml_bytes(), b"<rss><channel/></rss>");
    unit_of_work
        .commit()
        .await
        .expect("cache publication should commit");

    let cache = PostgresFeedCacheRepository::new(pool.clone())
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .expect("committed cache should be readable");
    assert!(cache.is_fresh());
    assert_eq!(cache.source_revision(), FeedRevision::from_u64(1));
    assert_eq!(cache.cache().etag(), "etag-1");
    assert_eq!(cache.cache().content_hash(), "hash-1");
    assert_eq!(cache.cache().feed_revision(), FeedRevision::from_u64(1));
    assert_eq!(cache.cache().xml_bytes(), b"<rss><channel/></rss>");
    assert_eq!(count_build_leases(&pool, source_id).await, 0);

    let rollback_lease = lease_repository
        .acquire_build(source_id, "rollback-builder", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("released lease should be available");
    let rollback_generated_at = Utc::now();
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .feed_cache()
        .publish_if_current(
            candidate_at(
                source_id,
                1,
                rollback_generated_at,
                rollback_generated_at + chrono::Duration::minutes(30),
                b"uncommitted",
                "etag-uncommitted",
            ),
            rollback_lease.owner(),
            rollback_lease.token(),
        )
        .await
        .expect("candidate should publish inside the transaction");
    assert!(matches!(result, FeedCachePublishResult::Published(_)));
    unit_of_work
        .rollback()
        .await
        .expect("rollback should discard cache publication and lease release");
    lease_repository
        .release_build(source_id, rollback_lease.owner(), rollback_lease.token())
        .await
        .expect("rolled-back lease should be released explicitly");

    let cache = PostgresFeedCacheRepository::new(pool.clone())
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .expect("original cache should remain after rollback");
    assert_eq!(cache.cache().xml_bytes(), b"<rss><channel/></rss>");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rendered_feed_candidate_publishes_through_fenced_cache_path(pool: PgPool) {
    let source_id = insert_source(&pool, 1).await;
    let generated_at = Utc::now() - chrono::Duration::seconds(1);
    let candidate = RssRenderer
        .render(RenderFeedInput {
            source_id,
            title: "Test feed".to_owned(),
            feed_url: "https://rss.example.test/feed".to_owned(),
            description: "Integration test feed".to_owned(),
            source_revision: FeedRevision::from_u64(1),
            generated_at,
            expires_at: generated_at + chrono::Duration::minutes(30),
            articles: vec![RenderArticle {
                review_id: "review-1".to_owned(),
                title: "An article".to_owned(),
                author: Some("Author".to_owned()),
                summary: Some("A summary".to_owned()),
                original_url: Some("https://mp.weixin.qq.com/s/article".to_owned()),
                published_at: at(110),
                content_html: "<p>Archived content</p>".to_owned(),
            }],
        })
        .expect("normalized feed should render")
        .into_candidate();

    let lease_repository = PostgresFeedBuildLeaseRepository::new(pool.clone());
    publish(&pool, &lease_repository, candidate.clone()).await;

    let cache = PostgresFeedCacheRepository::new(pool)
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .expect("rendered candidate should be readable");
    assert!(cache.is_fresh());
    assert_eq!(cache.cache().feed_revision(), FeedRevision::from_u64(1));
    assert_eq!(cache.cache().etag(), candidate.etag());
    assert_eq!(cache.cache().content_hash(), candidate.content_hash());
    assert_eq!(cache.cache().xml_bytes(), candidate.xml_bytes());
    assert_eq!(cache.cache().generated_at(), generated_at);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_service_rebuilds_an_expired_cache_before_delivery(pool: PgPool) {
    let source_id = insert_source(&pool, 1).await;
    let lease_repository = PostgresFeedBuildLeaseRepository::new(pool.clone());
    publish(
        &pool,
        &lease_repository,
        candidate(source_id, 1, 10, 20, b"stale-feed", "etag-stale"),
    )
    .await;

    let queue = PostgresFeedRebuildQueue::new(
        PostgresJobRepository::new(pool.clone()),
        FeedRebuildJobConfig::default(),
    );
    let factory = UnitOfWorkFactory::new(pool.clone());
    let rebuild_service = FeedRebuildService::new(
        FeedRebuildDependencies::new(
            PostgresSourceRepository::new(pool.clone()),
            PostgresArticleRepository::new(pool.clone()),
            PostgresFeedBuildLeaseRepository::new(pool.clone()),
            factory,
        ),
        FeedRebuildConfig::new(
            chrono::Duration::minutes(5),
            chrono::Duration::minutes(30),
            "https://feeds.example.test/werrss.xml",
            "Integration test feed",
        )
        .expect("feed rebuild configuration should be valid"),
        "api-feed-builder",
    )
    .expect("feed rebuild service should be constructible");
    let service = FeedService::new(
        PostgresFeedCacheRepository::new(pool.clone()),
        queue,
        rebuild_service,
        FeedServiceConfig::default(),
    );

    let first = service
        .get_feed(FeedRequest::new(source_id, None))
        .await
        .expect("stale cache should be rebuilt");
    assert!(matches!(
        first,
        FeedDelivery::Cached {
            status: werrss::application::feed_service::FeedCacheStatus::Fresh,
            rebuild: FeedRebuildStatus::Rebuilt,
            cache,
        } if String::from_utf8_lossy(cache.xml_bytes()).contains("Test source")
    ));

    let second = service
        .get_feed(FeedRequest::new(source_id, None))
        .await
        .expect("fresh rebuilt cache should be served");
    assert!(matches!(
        second,
        FeedDelivery::Cached {
            status: werrss::application::feed_service::FeedCacheStatus::Fresh,
            rebuild: FeedRebuildStatus::NotNeeded,
            ..
        }
    ));

    let job_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE source_id = $1 AND job_type = 'feed_rebuild'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("rebuild jobs should be queryable");
    assert_eq!(job_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_service_rebuilds_a_fresh_cache_with_incompatible_asset_urls(pool: PgPool) {
    let source_id = insert_source(&pool, 1).await;
    insert_article(
        &pool,
        source_id,
        "<p>Body</p><img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
    )
    .await;
    let asset_root =
        Url::parse("https://rss.example.test/werrss").expect("asset root should be valid");
    let lease_repository = PostgresFeedBuildLeaseRepository::new(pool.clone());
    let generated_at = Utc::now() - chrono::Duration::seconds(1);
    publish(
        &pool,
        &lease_repository,
        candidate_at(
            source_id,
            1,
            generated_at,
            generated_at + chrono::Duration::minutes(30),
            b"<rss><channel><item><content:encoded><![CDATA[<img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />]]></content:encoded></item></channel></rss>",
            "etag-relative-assets",
        ),
    )
    .await;

    let queue = PostgresFeedRebuildQueue::new(
        PostgresJobRepository::new(pool.clone()),
        FeedRebuildJobConfig::default(),
    );
    let factory = UnitOfWorkFactory::new(pool.clone());
    let rebuild_service = FeedRebuildService::new(
        FeedRebuildDependencies::new(
            PostgresSourceRepository::new(pool.clone()),
            PostgresArticleRepository::new(pool.clone()),
            PostgresFeedBuildLeaseRepository::new(pool.clone()),
            factory,
        ),
        FeedRebuildConfig::new(
            chrono::Duration::minutes(5),
            chrono::Duration::minutes(30),
            "https://rss.example.test/werrss.xml",
            "Integration test feed",
        )
        .expect("feed rebuild configuration should be valid")
        .with_asset_url_root(asset_root.clone()),
        "api-feed-builder",
    )
    .expect("feed rebuild service should be constructible");
    let service = FeedService::new(
        PostgresFeedCacheRepository::new(pool.clone()),
        queue,
        rebuild_service,
        FeedServiceConfig::default(),
    )
    .with_asset_url_policy(Some(FeedAssetUrlPolicy::new(asset_root, true)));

    let delivery = service
        .get_feed(FeedRequest::new(source_id, None))
        .await
        .expect("incompatible fresh cache should be rebuilt");
    assert!(matches!(
        delivery,
        FeedDelivery::Cached {
            status: werrss::application::feed_service::FeedCacheStatus::Fresh,
            rebuild: FeedRebuildStatus::Rebuilt,
            cache,
        } if String::from_utf8_lossy(cache.xml_bytes()).contains(
            "src=\"https://rss.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\""
        )
    ));

    let persisted = PostgresFeedCacheRepository::new(pool)
        .get(source_id)
        .await
        .expect("rebuilt cache should be readable")
        .expect("rebuilt cache should be persisted");
    assert!(
        String::from_utf8_lossy(persisted.cache().xml_bytes()).contains(
            "src=\"https://rss.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\""
        )
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_feed_cache_rejects_older_candidates_and_releases_on_revision_conflict(
    pool: PgPool,
) {
    let source_id = insert_source(&pool, 1).await;
    let lease_repository = PostgresFeedBuildLeaseRepository::new(pool.clone());
    publish(
        &pool,
        &lease_repository,
        candidate(source_id, 1, 10, 20, b"newer", "etag-newer"),
    )
    .await;

    let older_lease = lease_repository
        .acquire_build(source_id, "builder-old", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("released lease should be available");
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .feed_cache()
        .publish_if_current(
            candidate(source_id, 1, 9, 19, b"older", "etag-older"),
            older_lease.owner(),
            older_lease.token(),
        )
        .await
        .expect("newer cache should be a normal no-op");
    assert_eq!(result, FeedCachePublishResult::ExistingCacheNewer);
    unit_of_work
        .commit()
        .await
        .expect("lease release should commit");

    sqlx::query("UPDATE sources SET feed_revision = 2 WHERE id = $1")
        .bind(source_id.as_uuid())
        .execute(&pool)
        .await
        .expect("source revision should be mutable in the test");
    let stale_lease = lease_repository
        .acquire_build(source_id, "builder-stale", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("released lease should be available");
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .feed_cache()
        .publish_if_current(
            candidate(source_id, 1, 30, 40, b"stale", "etag-stale"),
            stale_lease.owner(),
            stale_lease.token(),
        )
        .await
        .expect("revision conflict should be a normal result");
    assert_eq!(
        result,
        FeedCachePublishResult::SourceRevisionChanged {
            current_revision: FeedRevision::from_u64(2),
        }
    );
    unit_of_work
        .commit()
        .await
        .expect("lease release should commit");

    let cache = PostgresFeedCacheRepository::new(pool.clone())
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .expect("original cache should remain");
    assert_eq!(cache.cache().xml_bytes(), b"newer");
    assert_eq!(cache.cache().feed_revision(), FeedRevision::from_u64(1));
    assert!(!cache.is_fresh());
    assert_eq!(count_build_leases(&pool, source_id).await, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_feed_cache_rejects_stale_owner_and_rolls_back_publication(pool: PgPool) {
    let source_id = insert_source(&pool, 1).await;
    let lease_repository = PostgresFeedBuildLeaseRepository::new(pool.clone());
    let stale_lease = lease_repository
        .acquire_build(source_id, "builder-stale", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("source should have no competing builder");
    expire_lease(&pool, source_id).await;
    let current_lease = lease_repository
        .acquire_build(source_id, "builder-current", chrono::Duration::minutes(5))
        .await
        .expect("takeover should succeed")
        .expect("expired lease should be available");

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let error = unit_of_work
        .feed_cache()
        .publish_if_current(
            candidate(source_id, 1, 50, 60, b"stale-owner", "etag-stale"),
            stale_lease.owner(),
            stale_lease.token(),
        )
        .await
        .expect_err("stale owner must not publish");
    assert!(matches!(
        error,
        werrss::persistence::repositories::feed_cache_repository::FeedCacheRepositoryError::LeaseLost { source_id: lost }
            if lost == source_id
    ));
    unit_of_work
        .rollback()
        .await
        .expect("stale transaction should roll back");

    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .feed_cache()
        .publish_if_current(
            candidate(source_id, 1, 70, 80, b"current", "etag-current"),
            current_lease.owner(),
            current_lease.token(),
        )
        .await
        .expect("current owner should publish");
    assert!(matches!(result, FeedCachePublishResult::Published(_)));
    unit_of_work
        .commit()
        .await
        .expect("current publication should commit");

    let cache = PostgresFeedCacheRepository::new(pool.clone())
        .get(source_id)
        .await
        .expect("cache read should succeed")
        .expect("current publication should be readable");
    assert_eq!(cache.cache().xml_bytes(), b"current");
}

async fn publish(
    pool: &PgPool,
    lease_repository: &PostgresFeedBuildLeaseRepository,
    candidate: FeedCacheCandidate,
) {
    let lease = lease_repository
        .acquire_build(
            candidate.source_id(),
            "builder",
            chrono::Duration::minutes(5),
        )
        .await
        .expect("lease acquisition should succeed")
        .expect("source should have no competing builder");
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .feed_cache()
        .publish_if_current(candidate, lease.owner(), lease.token())
        .await
        .expect("cache publication should succeed");
    assert!(matches!(result, FeedCachePublishResult::Published(_)));
    unit_of_work
        .commit()
        .await
        .expect("cache publication should commit");
}

fn candidate(
    source_id: SourceId,
    revision: u64,
    generated_at: i64,
    expires_at: i64,
    xml_bytes: &[u8],
    etag: &str,
) -> FeedCacheCandidate {
    candidate_at(
        source_id,
        revision,
        at(generated_at),
        at(expires_at),
        xml_bytes,
        etag,
    )
}

fn candidate_at(
    source_id: SourceId,
    revision: u64,
    generated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    xml_bytes: &[u8],
    etag: &str,
) -> FeedCacheCandidate {
    let suffix = etag.strip_prefix("etag-").unwrap_or(etag);
    FeedCacheCandidate::from_parts(
        source_id,
        xml_bytes.to_vec(),
        etag.to_owned(),
        generated_at,
        expires_at,
        FeedRevision::from_u64(revision),
        format!("hash-{suffix}"),
    )
}

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}

async fn insert_source(pool: &PgPool, revision: i64) -> SourceId {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    sqlx::query(
        "INSERT INTO sources (id, book_id, display_name, article_url, feed_revision) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(source_id.as_uuid())
    .bind(format!("book-{source_id}"))
    .bind("Test source")
    .bind("https://mp.weixin.qq.com/s/test")
    .bind(revision)
        .execute(pool)
        .await
        .expect("test source should be insertable");
    source_id
}

async fn insert_article(pool: &PgPool, source_id: SourceId, content_html: &str) {
    sqlx::query(
        "INSERT INTO articles (source_id, review_id, title, published_at, content_html, fetched_at) VALUES ($1, $2, $3, $4, $5, $4)",
    )
    .bind(source_id.as_uuid())
    .bind("asset-review")
    .bind("Asset article")
    .bind(Utc::now())
    .bind(content_html)
    .execute(pool)
    .await
    .expect("test article should be insertable");
}

async fn expire_lease(pool: &PgPool, source_id: SourceId) {
    sqlx::query(
        "UPDATE feed_build_leases SET lease_until = clock_timestamp() - interval '1 second' WHERE source_id = $1",
    )
    .bind(source_id.as_uuid())
    .execute(pool)
    .await
    .expect("test lease should be expirable");
}

async fn count_build_leases(pool: &PgPool, source_id: SourceId) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM feed_build_leases WHERE source_id = $1")
        .bind(source_id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("test lease count should be queryable")
}
