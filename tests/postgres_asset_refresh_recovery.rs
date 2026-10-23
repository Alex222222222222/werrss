//! Atomic queue failure cleanup for whole-article refresh reservations.

use chrono::{Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;
use werrss::{
    archive::asset_store::{AssetCachePolicy, AssetRefreshPolicy},
    domain::job::{JobStatus, JobType},
    persistence::{
        repositories::{
            asset_repository::{AssetRepairEnqueueResult, PostgresAssetStore},
            job_repository::{
                ExpiredJobRecovery, JobLease, JobOutcome, JobOutcomeTransaction, JobQueue,
                JobRepository, PostgresJobRepository,
            },
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

struct RefreshFixture {
    source_id: Uuid,
    asset_id: Uuid,
    chain_id: Uuid,
    job_id: Uuid,
}

impl RefreshFixture {
    async fn new(pool: &PgPool, attempts: i64, max_job_attempts: i64) -> Self {
        let fixture = Self {
            source_id: Uuid::new_v4(),
            asset_id: Uuid::new_v4(),
            chain_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
        };
        sqlx::query(
            "INSERT INTO sources (id, book_id, display_name, article_url)
             VALUES ($1, $2, 'Refresh recovery', 'https://mp.weixin.qq.com/s/recovery')",
        )
        .bind(fixture.source_id)
        .bind(format!("refresh-recovery-{}", fixture.source_id))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO articles (source_id, review_id, title, published_at, fetched_at)
             VALUES ($1, 'review', 'Refresh recovery', now(), now())",
        )
        .bind(fixture.source_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO asset_article_refresh_lineage (source_id, review_id, attempts)
             VALUES ($1, 'review', $2)",
        )
        .bind(fixture.source_id)
        .bind(attempts)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO asset_record_refresh_state
                 (asset_record_id, requested_generation, served_generation, active_chain_id)
             VALUES ($1, 1, 1, $2)",
        )
        .bind(fixture.asset_id)
        .bind(fixture.chain_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO asset_refresh_generation_entries
                 (asset_record_id, generation, ordinal, source_id, review_id, state)
             VALUES ($1, 1, 1, $2, 'review', 'in_flight')",
        )
        .bind(fixture.asset_id)
        .bind(fixture.source_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO jobs
                 (id, job_type, status, priority, run_after, claim_count, failure_count,
                  max_attempts, payload_json, dedupe_key, created_at, updated_at)
             VALUES ($1, 'asset_refresh', 'queued', 0, now(), 0, 0, $2, '{}'::jsonb, $3, now(), now())",
        )
        .bind(fixture.job_id)
        .bind(max_job_attempts)
        .bind(format!("asset-refresh:{}:review", fixture.source_id))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO asset_repair_jobs
                 (job_id, admission_class, refresh_source_id, refresh_review_id,
                  chain_id, chain_ordinal, expired_asset_record_id)
             VALUES ($1, 'internal_refresh', $2, 'review', $3, 1, $4)",
        )
        .bind(fixture.job_id)
        .bind(fixture.source_id)
        .bind(fixture.chain_id)
        .bind(fixture.asset_id)
        .execute(pool)
        .await
        .unwrap();
        fixture
    }

    async fn claim(&self, repository: &PostgresJobRepository) -> JobLease {
        let lease = repository
            .claim_next(
                "refresh-worker",
                Utc::now(),
                Duration::minutes(5),
                &[JobType::AssetRefresh],
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.job.id(), self.job_id);
        lease
    }

    async fn expire(&self, pool: &PgPool) {
        sqlx::query(
            "UPDATE jobs SET lease_until = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
        )
        .bind(self.job_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn assert_reserved(&self, pool: &PgPool, attempts: i64) {
        let state: (String, Option<Uuid>, i64, bool, bool) = sqlx::query_as(
            "SELECT e.state, s.active_chain_id, l.attempts, l.next_allowed_at IS NULL, l.exhausted
             FROM asset_refresh_generation_entries e
             JOIN asset_record_refresh_state s USING (asset_record_id)
             JOIN asset_article_refresh_lineage l USING (source_id, review_id)
             WHERE e.asset_record_id = $1",
        )
        .bind(self.asset_id)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(
            state,
            (
                "in_flight".to_owned(),
                Some(self.chain_id),
                attempts,
                true,
                false
            )
        );
    }

    async fn assert_cooled(&self, pool: &PgPool, cooldown_seconds: i64) {
        let state: (String, Option<Uuid>, i64, bool, bool, bool, i64) = sqlx::query_as(
            "SELECT e.state, s.active_chain_id, l.attempts, l.exhausted,
                    COALESCE(e.not_before = l.next_allowed_at, FALSE),
                    COALESCE(l.next_allowed_at BETWEEN clock_timestamp() + make_interval(secs => $2::double precision - 10)
                        AND clock_timestamp() + make_interval(secs => $2::double precision), FALSE),
                    s.completed_generation
             FROM asset_refresh_generation_entries e
             JOIN asset_record_refresh_state s USING (asset_record_id)
             JOIN asset_article_refresh_lineage l USING (source_id, review_id)
             WHERE e.asset_record_id = $1",
        )
        .bind(self.asset_id)
        .bind(cooldown_seconds)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(state, ("cooling".to_owned(), None, 1, false, true, true, 0));
    }
}

fn retry(lease: &JobLease) -> JobOutcome {
    JobOutcome::Retry {
        job_id: lease.job.id(),
        owner: "refresh-worker".to_owned(),
        token: lease.token,
        now: Utc::now(),
        retry_at: Utc::now() - Duration::seconds(1),
        error: "asset refresh persistence unavailable".to_owned(),
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn terminal_retry_cools_reservation_in_the_same_transaction(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 1, 1).await;
    let repository = PostgresJobRepository::new(pool.clone());
    let factory = UnitOfWorkFactory::new(pool.clone());
    let lease = fixture.claim(&repository).await;

    let mut uow = factory.begin().await.unwrap();
    assert_eq!(
        uow.apply_outcome(retry(&lease)).await.unwrap().status(),
        JobStatus::Failed
    );
    uow.rollback().await.unwrap();
    assert_eq!(
        repository
            .find(fixture.job_id)
            .await
            .unwrap()
            .unwrap()
            .status(),
        JobStatus::Running
    );
    fixture.assert_reserved(&pool, 1).await;

    let mut uow = factory.begin().await.unwrap();
    uow.apply_outcome(retry(&lease)).await.unwrap();
    uow.commit().await.unwrap();
    fixture.assert_cooled(&pool, 3600).await;
    assert!(repository
        .retry(
            fixture.job_id,
            "refresh-worker",
            lease.token,
            Utc::now(),
            Utc::now(),
            "asset refresh persistence unavailable"
        )
        .await
        .is_err());
    fixture.assert_cooled(&pool, 3600).await;
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn infrastructure_retry_keeps_the_same_reservation_until_terminal(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 1, 2).await;
    let repository = PostgresJobRepository::new(pool.clone());
    let lease = fixture.claim(&repository).await;
    let retried = repository
        .retry(
            fixture.job_id,
            "refresh-worker",
            lease.token,
            Utc::now(),
            Utc::now() - Duration::seconds(1),
            "asset refresh persistence unavailable",
        )
        .await
        .unwrap();
    assert_eq!(retried.status(), JobStatus::RetryWait);
    fixture.assert_reserved(&pool, 1).await;
    let next = fixture.claim(&repository).await;
    assert_ne!(next.token, lease.token);
    assert!(repository
        .retry(
            fixture.job_id,
            "refresh-worker",
            lease.token,
            Utc::now(),
            Utc::now(),
            "asset refresh persistence unavailable"
        )
        .await
        .is_err());
    fixture.assert_reserved(&pool, 1).await;
    repository
        .retry(
            fixture.job_id,
            "refresh-worker",
            next.token,
            Utc::now(),
            Utc::now(),
            "asset refresh persistence unavailable",
        )
        .await
        .unwrap();
    fixture.assert_cooled(&pool, 3600).await;
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn expired_lease_preserves_reservation_until_budget_exhaustion_and_rejects_stale_token(
    pool: PgPool,
) {
    let fixture = RefreshFixture::new(&pool, 1, 2).await;
    let repository = PostgresJobRepository::new(pool.clone());
    let lease = fixture.claim(&repository).await;
    fixture.expire(&pool).await;
    assert!(repository
        .recover_expired(Utc::now(), 0)
        .await
        .unwrap()
        .is_empty());
    fixture.assert_reserved(&pool, 1).await;
    let recovered = repository.recover_expired(Utc::now(), 1).await.unwrap();
    assert_eq!(recovered[0].status(), JobStatus::Queued);
    fixture.assert_reserved(&pool, 1).await;
    let next = fixture.claim(&repository).await;
    assert!(repository
        .retry(
            fixture.job_id,
            "refresh-worker",
            lease.token,
            Utc::now(),
            Utc::now(),
            "asset refresh persistence unavailable"
        )
        .await
        .is_err());
    fixture.assert_reserved(&pool, 1).await;
    fixture.expire(&pool).await;
    let recovered = repository.recover_expired(Utc::now(), 1).await.unwrap();
    assert_eq!(recovered[0].status(), JobStatus::Failed);
    fixture.assert_cooled(&pool, 3600).await;
    assert!(repository
        .heartbeat(
            fixture.job_id,
            "refresh-worker",
            next.token,
            Utc::now(),
            Duration::minutes(5)
        )
        .await
        .is_err());
    fixture.assert_cooled(&pool, 3600).await;
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn later_cache_miss_resumes_the_cooled_snapshot_after_terminal_retry(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 1, 1).await;
    sqlx::query(
        "INSERT INTO asset_records (id, source_url, version, final_url)
         VALUES ($1, 'https://cdn.example.test/recovery.png', 1,
                 'https://cdn.example.test/recovery.png')",
    )
    .bind(fixture.asset_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO article_assets (source_id, review_id, asset_record_id, occurrence, referer_url)
         VALUES ($1, 'review', $2, 0, 'https://mp.weixin.qq.com/s/recovery')",
    )
    .bind(fixture.source_id)
    .bind(fixture.asset_id)
    .execute(&pool)
    .await
    .unwrap();
    let repository = PostgresJobRepository::new(pool.clone());
    let store = PostgresAssetStore::new(pool.clone(), AssetCachePolicy::default());
    let lease = fixture.claim(&repository).await;
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut uow = factory.begin().await.unwrap();
    uow.apply_outcome(retry(&lease)).await.unwrap();
    uow.commit().await.unwrap();
    assert_eq!(
        store.enqueue_repair(fixture.asset_id).await.unwrap(),
        AssetRepairEnqueueResult::Retryable
    );
    fixture.assert_cooled(&pool, 3600).await;
    sqlx::query(
        "UPDATE asset_article_refresh_lineage SET next_allowed_at = clock_timestamp() - INTERVAL '1 second'
         WHERE source_id = $1",
    )
    .bind(fixture.source_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE asset_refresh_generation_entries SET not_before = clock_timestamp() - INTERVAL '1 second'
         WHERE asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .execute(&pool)
    .await
    .unwrap();
    let AssetRepairEnqueueResult::Enqueued { job_id } =
        store.enqueue_repair(fixture.asset_id).await.unwrap()
    else {
        panic!("cooled snapshot should resume after cooldown");
    };
    assert_ne!(job_id, fixture.job_id);
    let next = repository.find(job_id).await.unwrap().unwrap();
    assert_eq!(next.job_type(), JobType::AssetRefresh);
    let state: (String, i64, i64, bool) = sqlx::query_as(
        "SELECT e.state, e.generation, l.attempts, s.active_chain_id IS NOT NULL
         FROM asset_refresh_generation_entries e
         JOIN asset_record_refresh_state s USING (asset_record_id)
         JOIN asset_article_refresh_lineage l USING (source_id, review_id)
         WHERE e.asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, ("in_flight".to_owned(), 1, 2, true));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn terminal_retry_uses_the_factory_refresh_policy(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 1, 1).await;
    let policy = AssetRefreshPolicy::new(5, std::time::Duration::from_secs(120)).unwrap();
    let factory = UnitOfWorkFactory::new(pool.clone()).with_refresh_policy(policy);
    let repository = PostgresJobRepository::new(pool.clone());
    let lease = fixture.claim(&repository).await;
    let mut uow = factory.begin().await.unwrap();
    uow.apply_outcome(retry(&lease)).await.unwrap();
    uow.commit().await.unwrap();
    fixture.assert_cooled(&pool, 120).await;
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn expired_lease_uses_the_repository_refresh_policy(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 1, 1).await;
    let policy = AssetRefreshPolicy::new(1, std::time::Duration::from_secs(120)).unwrap();
    let repository = PostgresJobRepository::new(pool.clone()).with_refresh_policy(policy);
    fixture.claim(&repository).await;
    fixture.expire(&pool).await;
    repository.recover_expired(Utc::now(), 1).await.unwrap();
    let state: (String, bool, Option<Uuid>, i64) = sqlx::query_as(
        "SELECT e.state, l.exhausted, s.active_chain_id, s.completed_generation
         FROM asset_refresh_generation_entries e
         JOIN asset_record_refresh_state s USING (asset_record_id)
         JOIN asset_article_refresh_lineage l USING (source_id, review_id)
         WHERE e.asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, ("done".to_owned(), true, None, 1));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn deleted_article_releases_terminal_refresh_reservation(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 1, 1).await;
    let repository = PostgresJobRepository::new(pool.clone());
    fixture.claim(&repository).await;
    sqlx::query("DELETE FROM articles WHERE source_id = $1")
        .bind(fixture.source_id)
        .execute(&pool)
        .await
        .unwrap();
    fixture.expire(&pool).await;
    repository.recover_expired(Utc::now(), 1).await.unwrap();
    let state: (String, Option<Uuid>, i64) = sqlx::query_as(
        "SELECT e.state, s.active_chain_id, s.completed_generation
         FROM asset_refresh_generation_entries e
         JOIN asset_record_refresh_state s USING (asset_record_id)
         WHERE e.asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, ("done".to_owned(), None, 1));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn obsolete_refresh_job_cannot_release_a_newer_chain(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 1, 1).await;
    let repository = PostgresJobRepository::new(pool.clone());
    let lease = fixture.claim(&repository).await;
    let newer_chain = Uuid::new_v4();
    sqlx::query(
        "UPDATE asset_record_refresh_state SET active_chain_id = $2 WHERE asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .bind(newer_chain)
    .execute(&pool)
    .await
    .unwrap();
    repository
        .retry(
            fixture.job_id,
            "refresh-worker",
            lease.token,
            Utc::now(),
            Utc::now(),
            "asset refresh persistence unavailable",
        )
        .await
        .unwrap();
    let active: Option<Uuid> = sqlx::query_scalar(
        "SELECT active_chain_id FROM asset_record_refresh_state WHERE asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(active, Some(newer_chain));
    let state: String = sqlx::query_scalar(
        "SELECT state FROM asset_refresh_generation_entries WHERE asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, "in_flight");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn exhausted_lineage_finishes_entry_and_generation_on_expired_lease(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 3, 1).await;
    let repository = PostgresJobRepository::new(pool.clone());
    fixture.claim(&repository).await;
    fixture.expire(&pool).await;
    repository.recover_expired(Utc::now(), 1).await.unwrap();
    let state: (String, bool, Option<Uuid>, i64, bool, String, i64) = sqlx::query_as(
        "SELECT e.state, e.not_before IS NULL, s.active_chain_id, s.completed_generation,
                l.exhausted, COALESCE(l.last_outcome, ''), l.attempts
         FROM asset_refresh_generation_entries e
         JOIN asset_record_refresh_state s USING (asset_record_id)
         JOIN asset_article_refresh_lineage l USING (source_id, review_id)
         WHERE e.asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        state,
        (
            "done".to_owned(),
            true,
            None,
            1,
            true,
            "exhausted".to_owned(),
            3
        )
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn directly_failed_refresh_job_releases_its_chain(pool: PgPool) {
    let fixture = RefreshFixture::new(&pool, 0, 3).await;
    let repository = PostgresJobRepository::new(pool.clone());
    let factory = UnitOfWorkFactory::new(pool.clone());
    let lease = fixture.claim(&repository).await;

    let mut uow = factory.begin().await.unwrap();
    assert_eq!(
        uow.apply_outcome(JobOutcome::Failed {
            job_id: lease.job.id(),
            owner: "refresh-worker".to_owned(),
            token: lease.token,
            now: Utc::now(),
            error: "asset refresh handler is not configured".to_owned(),
        })
        .await
        .unwrap()
        .status(),
        JobStatus::Failed
    );
    uow.commit().await.unwrap();

    let state: (String, Option<Uuid>) = sqlx::query_as(
        "SELECT e.state, s.active_chain_id
         FROM asset_refresh_generation_entries e
         JOIN asset_record_refresh_state s USING (asset_record_id)
         WHERE e.asset_record_id = $1",
    )
    .bind(fixture.asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        state.1.is_none(),
        "a directly failed refresh job must not leave the chain wedged"
    );
    assert!(
        matches!(state.0.as_str(), "cooling" | "done"),
        "the abandoned entry must be cooled or completed for later resumption, got {}",
        state.0
    );
}
