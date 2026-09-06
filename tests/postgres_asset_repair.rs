//! PostgreSQL and HTTP integration coverage for automatic asset repair.

use std::{
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use chrono::{Duration, Utc};
use sqlx::{PgPool, Row};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use uuid::Uuid;
use werrss::{
    application::{
        asset_archive_service::AssetArchiveService,
        asset_repair_handler::AssetRepairJobHandler,
        job_service::{JobService, JobServiceConfig},
        worker::{JobExecution, Worker, WorkerConfig, WorkerRun},
    },
    archive::asset_store::{AssetCachePolicy, AssetInput, AssetRead, AssetRepairPolicy},
    domain::{
        article::{ArticleObservationVersion, NewArticle},
        job::JobType,
        source::{NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
    },
    persistence::{
        repositories::{
            article_repository::ArticleTransactionRepository,
            asset_repository::{
                AssetRepairEnqueueResult, AssetTransactionRepository, PostgresAssetStore,
            },
            job_repository::{JobQueue, PostgresJobRepository},
            source_repository::SourceTransactionRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

const BODY: &[u8] = b"\x89PNG\r\n\x1a\nrepaired-image";

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn worker_repairs_a_missing_asset_and_preserves_request_context(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_article(&pool, source_id).await;
    let policy = test_policy();
    let input = asset_input();
    let stored = store_asset(&pool, policy, source_id, &input).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("missing asset should admit one repair job, got {other:?}"),
    };
    let (archiver, server, captured) = asset_server_fixture().await;
    let handler = AssetRepairJobHandler::new(store.clone(), archiver, Duration::seconds(1));
    let worker = Worker::new(
        JobService::new(
            PostgresJobRepository::new(pool.clone()),
            JobServiceConfig::new("asset-repair-worker", Duration::minutes(5), 10).unwrap(),
        ),
        UnitOfWorkFactory::new(pool.clone()),
        handler,
        WorkerConfig::new(vec![JobType::AssetRepair], StdDuration::from_secs(1)).unwrap(),
    )
    .unwrap();

    let result = worker.run_once(Utc::now()).await.unwrap();
    assert!(matches!(
        result,
        WorkerRun::Completed {
            outcome: JobExecution::Succeeded,
            ..
        }
    ));
    server.await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "succeeded"
    );
    assert!(matches!(
        store.read_and_touch(stored.id()).await.unwrap(),
        Some(AssetRead::Available { ref bytes, .. }) if bytes == BODY
    ));
    let request = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
    assert!(request.contains("referer: https://mp.weixin.qq.com/s/repair-asset"));
    assert!(request.contains("origin: https://mp.weixin.qq.com"));
    assert!(request.contains("user-agent: captured-agent"));
    assert!(!request.to_ascii_lowercase().contains("cookie:"));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn worker_recovers_an_expired_asset_repair_lease(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_article(&pool, source_id).await;
    let policy = test_policy();
    let input = asset_input();
    let stored = store_asset(&pool, policy, source_id, &input).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("missing asset should admit one repair job, got {other:?}"),
    };
    let expired_token = Uuid::new_v4();
    sqlx::query(
        "UPDATE jobs
         SET status = 'running', claim_count = 1, failure_count = 0,
             lease_owner = 'crashed-worker', lease_token = $2,
             lease_until = clock_timestamp() - interval '1 second',
             heartbeat_at = clock_timestamp() - interval '2 seconds',
             started_at = clock_timestamp() - interval '3 seconds',
             finished_at = NULL, updated_at = clock_timestamp()
         WHERE id = $1",
    )
    .bind(job_id)
    .bind(expired_token)
    .execute(&pool)
    .await
    .unwrap();

    let (archiver, server, _) = asset_server_fixture().await;
    let handler = AssetRepairJobHandler::new(store.clone(), archiver, Duration::seconds(1));
    let worker = Worker::new(
        JobService::new(
            PostgresJobRepository::new(pool.clone()),
            JobServiceConfig::new("asset-repair-recovery-worker", Duration::minutes(5), 10)
                .unwrap(),
        ),
        UnitOfWorkFactory::new(pool.clone()),
        handler,
        WorkerConfig::new(vec![JobType::AssetRepair], StdDuration::from_secs(1)).unwrap(),
    )
    .unwrap();

    let result = worker.run_once(Utc::now()).await.unwrap();
    assert!(matches!(
        result,
        WorkerRun::Completed {
            outcome: JobExecution::Succeeded,
            ..
        }
    ));
    server.await.unwrap();
    assert!(matches!(
        store.read_and_touch(stored.id()).await.unwrap(),
        Some(AssetRead::Available { ref bytes, .. }) if bytes == BODY
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "succeeded"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn stale_asset_repair_lease_cannot_restore_bytes(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_article(&pool, source_id).await;
    let policy = test_policy();
    let input = asset_input();
    let stored = store_asset(&pool, policy, source_id, &input).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("missing asset should admit one repair job, got {other:?}"),
    };
    let stale_lease = claim_repair_job(&pool).await;
    sqlx::query(
        "UPDATE jobs
         SET lease_owner = 'replacement-worker', lease_token = $2,
             lease_until = clock_timestamp() + interval '5 minutes'
         WHERE id = $1",
    )
    .bind(job_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(
        store
            .restore_missing_asset(&stale_lease, stored.id(), &input)
            .await,
        Err(werrss::persistence::repositories::asset_repository::AssetRepositoryError::LeaseLost)
    );
    assert!(matches!(
        store.read_and_touch(stored.id()).await.unwrap(),
        Some(AssetRead::Missing { .. })
    ));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn stale_asset_repair_lease_cannot_record_failure(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_article(&pool, source_id).await;
    let policy = test_policy();
    let input = asset_input();
    let stored = store_asset(&pool, policy, source_id, &input).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let store = PostgresAssetStore::new(pool.clone(), policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("missing asset should admit one repair job, got {other:?}"),
    };
    let stale_lease = claim_repair_job(&pool).await;
    sqlx::query(
        "UPDATE jobs
         SET lease_owner = 'replacement-worker', lease_token = $2,
             lease_until = clock_timestamp() + interval '5 minutes'
         WHERE id = $1",
    )
    .bind(job_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(
        store
            .record_repair_failure(&stale_lease, stored.id(), "stale worker failure")
            .await,
        Err(werrss::persistence::repositories::asset_repository::AssetRepositoryError::LeaseLost)
    );
    let state = sqlx::query(
        "SELECT admitted_attempts, next_allowed_at, exhausted, last_error
         FROM asset_repair_states
         WHERE asset_record_id = $1",
    )
    .bind(stored.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state.try_get::<i64, _>("admitted_attempts").unwrap(), 1);
    assert!(state
        .try_get::<Option<chrono::DateTime<Utc>>, _>("next_allowed_at")
        .unwrap()
        .is_none());
    assert!(!state.try_get::<bool, _>("exhausted").unwrap());
    assert!(state
        .try_get::<Option<String>, _>("last_error")
        .unwrap()
        .is_none());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn recovered_repair_job_defers_during_persisted_backoff(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_article(&pool, source_id).await;
    let policy = test_policy();
    let input = asset_input();
    let stored = store_asset(&pool, policy, source_id, &input).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let repair_policy = AssetRepairPolicy::new(10, 10, StdDuration::from_secs(60), 3).unwrap();
    let store = PostgresAssetStore::new(pool.clone(), policy).with_repair_policy(repair_policy);
    let job_id = match store.enqueue_repair(stored.id()).await.unwrap() {
        AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
        other => panic!("repair should be admitted, got {other:?}"),
    };
    let lease = claim_repair_job(&pool).await;
    assert!(!store
        .record_repair_failure(&lease, stored.id(), "simulated worker failure")
        .await
        .unwrap());
    sqlx::query(
        "UPDATE jobs
         SET lease_until = clock_timestamp() - interval '1 second',
             heartbeat_at = clock_timestamp() - interval '2 seconds'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .unwrap();

    let archiver = AssetArchiveService::with_client_for_test(
        policy,
        None,
        reqwest::Client::builder().no_proxy().build().unwrap(),
    );
    let handler = AssetRepairJobHandler::new(store, archiver, Duration::seconds(1));
    let worker = Worker::new(
        JobService::new(
            PostgresJobRepository::new(pool.clone()),
            JobServiceConfig::new("asset-repair-recovery-worker", Duration::minutes(5), 10)
                .unwrap(),
        ),
        UnitOfWorkFactory::new(pool.clone()),
        handler,
        WorkerConfig::new(vec![JobType::AssetRepair], StdDuration::from_secs(1)).unwrap(),
    )
    .unwrap();

    let result = worker.run_once(Utc::now()).await.unwrap();
    assert!(matches!(
        result,
        WorkerRun::Completed {
            outcome: JobExecution::Deferred { .. },
            ..
        }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "deferred"
    );
    assert!(matches!(
        PostgresAssetStore::new(pool, policy)
            .read_and_touch(stored.id())
            .await
            .unwrap(),
        Some(AssetRead::Missing { .. })
    ));
}

fn test_policy() -> AssetCachePolicy {
    AssetCachePolicy::new(
        0,
        StdDuration::from_secs(30),
        1024,
        10,
        1024 * 1024,
        StdDuration::from_secs(10),
        StdDuration::from_secs(2),
        2,
    )
    .unwrap()
}

fn asset_input() -> AssetInput {
    AssetInput::new(
        "http://assets.example.test/repaired.png".parse().unwrap(),
        "http://assets.example.test/repaired.png".parse().unwrap(),
        "image/png".to_owned(),
        BODY.to_vec(),
        0,
        "https://mp.weixin.qq.com/s/repair-asset".parse().unwrap(),
        Some("https://mp.weixin.qq.com".to_owned()),
        Some("captured-agent".to_owned()),
    )
}

async fn asset_server_fixture() -> (
    AssetArchiveService,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<u8>>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("asset repair fixture should bind");
    let address = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let captured_by_server = Arc::clone(&captured);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        *captured_by_server.lock().unwrap() = request;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            BODY.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(BODY).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .resolve("assets.example.test", address)
        .build()
        .unwrap();
    let service = AssetArchiveService::with_client_for_test(test_policy(), None, client);
    (service, server, captured)
}

async fn create_source_and_article(pool: &PgPool, source_id: SourceId) {
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .source()
        .insert(NewSource {
            id: source_id,
            book_id: format!("asset-repair-book-{source_id}"),
            display_name: "Asset repair test source".to_owned(),
            article_url: Some("https://mp.weixin.qq.com/s/source".parse().unwrap()),
            enabled: true,
            sync_interval: Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: Utc::now(),
            priority: 0,
            max_attempts: 3,
        })
        .await
        .unwrap();
    unit_of_work
        .articles()
        .upsert(NewArticle {
            source_id,
            review_id: "repair-asset".to_owned(),
            title: "Asset repair article".to_owned(),
            author: None,
            summary: None,
            cover_url: None,
            original_url: Some(
                VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/repair-asset").unwrap(),
            ),
            published_at: Utc::now(),
            content_html: "<p>asset repair</p>".to_owned(),
            content_hash: Some("asset-repair-hash".to_owned()),
            observation_version: ArticleObservationVersion::from_u64(1),
            fetched_at: Utc::now(),
        })
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
}

async fn store_asset(
    pool: &PgPool,
    policy: AssetCachePolicy,
    source_id: SourceId,
    input: &AssetInput,
) -> werrss::archive::asset_store::StoredAsset {
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "repair-asset", std::slice::from_ref(input))
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    unit_of_work.commit().await.unwrap();
    stored
}

async fn claim_repair_job(
    pool: &PgPool,
) -> werrss::persistence::repositories::job_repository::JobLease {
    PostgresJobRepository::new(pool.clone())
        .claim_next(
            "asset-repair-test-worker",
            Utc::now(),
            Duration::minutes(5),
            &[JobType::AssetRepair],
        )
        .await
        .unwrap()
        .expect("repair job should be claimable")
}
