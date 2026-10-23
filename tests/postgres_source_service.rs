use chrono::{Duration, Utc};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;
use werrss::{
    application::source_service::{SourceService, SourceServiceError},
    domain::source::{NewSource, SchedulingGate, SourceId, SourcePatch, VerifiedWechatArticleUrl},
    persistence::repositories::source_repository::SourceRepositoryError,
    persistence::unit_of_work::UnitOfWorkFactory,
};

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_service_atomically_creates_source_and_initial_job(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let service = service(&pool);
    let created = service
        .create(source_spec(source_id, "book-service-create"))
        .await
        .expect("source creation should succeed");

    assert_eq!(
        service
            .find(source_id)
            .await
            .expect("source lookup should succeed"),
        Some(created.clone())
    );
    assert_eq!(
        service
            .find_by_book_id(" book-service-create ")
            .await
            .expect("book id lookup should succeed")
            .expect("created source should be found")
            .id(),
        source_id
    );

    let (job_type, persisted_source_id, status, priority, run_after, max_attempts, dedupe_key, payload): (
        String,
        Option<Uuid>,
        String,
        i32,
        chrono::DateTime<Utc>,
        i64,
        String,
        Value,
    ) = sqlx::query_as(
        "SELECT job_type, source_id, status, priority, run_after, max_attempts, dedupe_key, payload_json FROM jobs WHERE dedupe_key = $1",
    )
    .bind(format!("source_sync:{source_id}"))
    .fetch_one(&pool)
    .await
    .expect("initial source-sync job should exist");

    assert_eq!(job_type, "source_sync");
    assert_eq!(persisted_source_id, Some(source_id.as_uuid()));
    assert_eq!(status, "queued");
    assert_eq!(priority, created.priority());
    assert_eq!(run_after, created.next_fetch_at());
    assert_eq!(max_attempts, i64::from(created.max_attempts()));
    assert_eq!(dedupe_key, format!("source_sync:{source_id}"));
    assert_eq!(
        payload,
        serde_json::json!({"source_id": source_id.to_string()})
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_service_does_not_enqueue_for_disabled_or_blocked_sources(pool: PgPool) {
    let service = service(&pool);
    let disabled_id = SourceId::from_uuid(Uuid::new_v4());
    let blocked_id = SourceId::from_uuid(Uuid::new_v4());

    service
        .create(NewSource {
            enabled: false,
            ..source_spec(disabled_id, "book-service-disabled")
        })
        .await
        .expect("disabled source creation should succeed");
    service
        .create(NewSource {
            scheduling_gate: SchedulingGate::AuthenticationRequired,
            ..source_spec(blocked_id, "book-service-blocked")
        })
        .await
        .expect("blocked source creation should succeed");

    let job_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE source_id IN ($1, $2) AND job_type = 'source_sync'",
    )
    .bind(disabled_id.as_uuid())
    .bind(blocked_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("job count should be queryable");
    assert_eq!(job_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_service_persists_a_book_only_source_with_a_null_article_url(pool: PgPool) {
    let service = service(&pool);
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let created = service
        .create(NewSource {
            article_url: None,
            ..source_spec(source_id, "book-service-only")
        })
        .await
        .expect("book-only source creation should succeed");

    assert_eq!(created.article_url(), None);
    assert_eq!(
        service
            .find(source_id)
            .await
            .expect("source lookup should succeed")
            .expect("source should exist")
            .article_url(),
        None
    );
    let stored: Option<String> =
        sqlx::query_scalar("SELECT article_url FROM sources WHERE id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("article URL should be queryable");
    assert_eq!(stored, None);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_service_updates_and_deletes_source_owned_jobs(pool: PgPool) {
    let service = service(&pool);
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let created = service
        .create(source_spec(source_id, "book-service-edit"))
        .await
        .expect("source creation should succeed");

    let updated = service
        .update(
            source_id,
            SourcePatch {
                book_id: Some(" book-service-edited ".to_owned()),
                display_name: Some(" Edited source ".to_owned()),
                article_url: Some(None),
                sync_interval: Some(Duration::hours(2)),
                rss_item_limit: Some(10),
                priority: Some(20),
                max_attempts: Some(5),
                ..SourcePatch::default()
            },
        )
        .await
        .expect("source update should succeed");
    assert_eq!(updated.book_id(), "book-service-edited");
    assert_eq!(updated.display_name(), "Edited source");
    assert_eq!(updated.article_url(), None);
    assert_eq!(
        updated.feed_revision(),
        werrss::domain::source::FeedRevision::from_u64(1)
    );
    assert_eq!(updated.next_fetch_at(), created.next_fetch_at());

    service
        .delete(source_id)
        .await
        .expect("source deletion should succeed");
    assert!(service
        .find(source_id)
        .await
        .expect("deleted source lookup should succeed")
        .is_none());
    let remaining_jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE source_id = $1")
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("source jobs should be queryable");
    assert_eq!(remaining_jobs, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_service_operator_changes_preserve_feed_revision(pool: PgPool) {
    let service = service(&pool);
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let created = service
        .create(source_spec(source_id, "book-service-operator"))
        .await
        .expect("source creation should succeed");

    let disabled = service
        .set_enabled(source_id, false)
        .await
        .expect("disabling a source should succeed");
    assert!(!disabled.enabled());
    assert_eq!(disabled.feed_revision(), created.feed_revision());

    let gated = service
        .set_scheduling_gate(source_id, SchedulingGate::RiskControlled)
        .await
        .expect("changing the source gate should succeed");
    assert_eq!(gated.scheduling_gate(), SchedulingGate::RiskControlled);
    assert_eq!(gated.feed_revision(), created.feed_revision());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_service_duplicate_identity_does_not_leave_a_partial_source_or_job(pool: PgPool) {
    let service = service(&pool);
    let first_id = SourceId::from_uuid(Uuid::new_v4());
    let second_id = SourceId::from_uuid(Uuid::new_v4());
    service
        .create(source_spec(first_id, "book-service-duplicate"))
        .await
        .expect("first source creation should succeed");

    let error = service
        .create(source_spec(second_id, "book-service-duplicate"))
        .await
        .expect_err("duplicate book id should fail");
    assert!(matches!(
        error,
        SourceServiceError::Source(
            werrss::persistence::repositories::source_repository::SourceRepositoryError::BookIdConflict { ref book_id }
        ) if book_id == "book-service-duplicate"
    ));

    let source_count: i64 = sqlx::query_scalar("SELECT count(*) FROM sources WHERE id = $1")
        .bind(second_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("source count should be queryable");
    let job_count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE source_id = $1")
        .bind(second_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("job count should be queryable");
    assert_eq!(source_count, 0);
    assert_eq!(job_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_service_rolls_back_invalid_source_input(pool: PgPool) {
    let service = service(&pool);
    let source_id = SourceId::from_uuid(Uuid::nil());

    let error = service
        .create(source_spec(source_id, "book-service-invalid"))
        .await
        .expect_err("nil source id should be rejected");
    assert!(matches!(
        error,
        SourceServiceError::Source(SourceRepositoryError::Domain(
            werrss::domain::source::SourceError::InvalidId
        ))
    ));

    let source_count: i64 = sqlx::query_scalar("SELECT count(*) FROM sources WHERE id = $1")
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("source count should be queryable");
    let job_count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE source_id = $1")
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .expect("job count should be queryable");
    assert_eq!(source_count, 0);
    assert_eq!(job_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_service_operator_changes_report_missing_sources(pool: PgPool) {
    let service = service(&pool);
    let source_id = SourceId::from_uuid(Uuid::new_v4());

    let disable_error = service
        .set_enabled(source_id, false)
        .await
        .expect_err("missing source should not be disabled");
    assert!(matches!(
        disable_error,
        SourceServiceError::Source(SourceRepositoryError::NotFound { source_id: found })
            if found == source_id
    ));

    let gate_error = service
        .set_scheduling_gate(source_id, SchedulingGate::RiskControlled)
        .await
        .expect_err("missing source should not change scheduling gate");
    assert!(matches!(
        gate_error,
        SourceServiceError::Source(SourceRepositoryError::NotFound { source_id: found })
            if found == source_id
    ));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_deletion_waits_for_asset_recovery_locks_before_locking_source(pool: PgPool) {
    let service = service(&pool);
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    service
        .create(source_spec(source_id, "book-delete-lock-order"))
        .await
        .expect("source should be created");

    let mut capacity = pool
        .begin()
        .await
        .expect("capacity transaction should begin");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *capacity)
        .await
        .expect("capacity lock should be acquired");

    let delete = tokio::spawn(async move { service.delete(source_id).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !delete.is_finished(),
        "deletion must wait on asset capacity before locking the source"
    );

    let mut source_probe = pool.begin().await.expect("source probe should begin");
    let source_locked: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM sources WHERE id = $1 FOR UPDATE NOWAIT")
            .bind(source_id.as_uuid())
            .fetch_optional(&mut *source_probe)
            .await
            .expect("deletion must not hold the source row while waiting for capacity");
    assert_eq!(source_locked, Some(source_id.as_uuid()));
    source_probe
        .rollback()
        .await
        .expect("source probe should roll back");

    capacity
        .commit()
        .await
        .expect("capacity lock should release");
    delete
        .await
        .expect("delete task should finish")
        .expect("source deletion should complete");
}

fn service(pool: &PgPool) -> SourceService {
    SourceService::new(
        werrss::persistence::repositories::source_repository::PostgresSourceRepository::new(
            pool.clone(),
        ),
        UnitOfWorkFactory::new(pool.clone()),
    )
}

fn source_spec(source_id: SourceId, book_id: &str) -> NewSource {
    NewSource {
        id: source_id,
        book_id: book_id.to_owned(),
        display_name: "Service test source".to_owned(),
        article_url: Some(
            "https://mp.weixin.qq.com/s/service-test"
                .parse::<VerifiedWechatArticleUrl>()
                .expect("test URL should be valid"),
        ),
        enabled: true,
        sync_interval: Duration::hours(1),
        rss_item_limit: 20,
        account_id: None,
        scheduling_gate: SchedulingGate::Ready,
        next_fetch_at: Utc::now(),
        priority: 12,
        max_attempts: 3,
    }
}
