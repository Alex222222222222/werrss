//! Migration contract for durable whole-article asset refresh metadata.

use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn refresh_snapshot_and_active_job_survive_article_and_asset_deletion(pool: PgPool) {
    let source_id = Uuid::new_v4();
    let review_id = "refresh-migration-review";
    let asset_record_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    let repair_job_id = Uuid::new_v4();
    let chain_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO sources (id, book_id, display_name, article_url)
         VALUES ($1, $2, 'Refresh migration test', 'https://mp.weixin.qq.com/mp/profile_ext?action=home')",
    )
    .bind(source_id)
    .bind(format!("refresh-migration-{}", Uuid::new_v4()))
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO articles
             (source_id, review_id, title, published_at, fetched_at)
         VALUES ($1, $2, 'Refresh migration test', now(), now())",
    )
    .bind(source_id)
    .bind(review_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO asset_article_refresh_lineage
             (source_id, review_id, attempts)
         VALUES ($1, $2, 1)",
    )
    .bind(source_id)
    .bind(review_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO asset_records (id, source_url, version, final_url)
         VALUES ($1, 'https://cdn.example.test/image.png', 1,
                 'https://cdn.example.test/image.png')",
    )
    .bind(asset_record_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO asset_record_refresh_state
             (asset_record_id, requested_generation, completed_generation,
              served_generation, active_chain_id)
         VALUES ($1, 1, 0, 1, $2)",
    )
    .bind(asset_record_id)
    .bind(chain_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id, state)
         VALUES ($1, 1, 1, $2, $3, 'in_flight')",
    )
    .bind(asset_record_id)
    .bind(source_id)
    .bind(review_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO jobs
             (id, job_type, status, priority, run_after, claim_count,
              failure_count, max_attempts, payload_json, dedupe_key,
              created_at, updated_at)
         VALUES ($1, 'asset_refresh', 'queued', 0, now(), 0, 0, 3, '{}'::jsonb,
                 $2, now(), now())",
    )
    .bind(job_id)
    .bind(format!("asset-refresh:{}:{}", source_id, review_id))
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO asset_repair_jobs
             (job_id, asset_record_id, admission_class, refresh_source_id,
              refresh_review_id, chain_id, chain_ordinal,
              expired_asset_record_id)
         VALUES ($1, NULL, 'internal_refresh', $2, $3, $4, 1, $5)",
    )
    .bind(job_id)
    .bind(source_id)
    .bind(review_id)
    .bind(chain_id)
    .bind(asset_record_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO jobs
             (id, job_type, status, priority, run_after, claim_count,
              failure_count, max_attempts, payload_json, dedupe_key,
              created_at, updated_at)
         VALUES ($1, 'asset_repair', 'queued', 0, now(), 0, 0, 3, '{}'::jsonb,
                 $2, now(), now())",
    )
    .bind(repair_job_id)
    .bind(format!("asset-repair:{}", asset_record_id))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_repair_jobs (job_id, asset_record_id, admission_class)
         VALUES ($1, $2, 'public_repair')",
    )
    .bind(repair_job_id)
    .bind(asset_record_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query("DELETE FROM articles WHERE source_id = $1 AND review_id = $2")
        .bind(source_id)
        .bind(review_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM asset_records WHERE id = $1")
        .bind(asset_record_id)
        .execute(&pool)
        .await
        .unwrap();

    let retained = sqlx::query_as::<_, (i64, i64, i64, Option<Uuid>, i64)>(
        "SELECT
             (SELECT count(*) FROM jobs j
              JOIN asset_repair_jobs arj ON arj.job_id = j.id
              WHERE j.id = $1 AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')),
             (SELECT count(*) FROM asset_record_refresh_state WHERE asset_record_id = $2),
             (SELECT count(*) FROM asset_refresh_generation_entries
              WHERE asset_record_id = $2 AND generation = 1),
             (SELECT asset_record_id FROM asset_repair_jobs WHERE job_id = $3),
             (SELECT count(*) FROM jobs j
              JOIN asset_repair_jobs arj ON arj.job_id = j.id
              WHERE j.id = $3 AND j.status IN ('queued', 'running', 'retry_wait', 'deferred'))",
    )
    .bind(job_id)
    .bind(asset_record_id)
    .bind(repair_job_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(retained, (1, 1, 1, None, 1));

    let lineage_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM asset_article_refresh_lineage
         WHERE source_id = $1 AND review_id = $2",
    )
    .bind(source_id)
    .bind(review_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        lineage_count, 0,
        "article deletion should clean its lineage"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn generation_snapshot_identity_and_order_are_immutable(pool: PgPool) {
    let asset_record_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO asset_record_refresh_state (asset_record_id)
         VALUES ($1)",
    )
    .bind(asset_record_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id)
         VALUES ($1, 1, 1, $2, 'snapshot-review')",
    )
    .bind(asset_record_id)
    .bind(source_id)
    .execute(&pool)
    .await
    .unwrap();

    let update = sqlx::query(
        "UPDATE asset_refresh_generation_entries
         SET ordinal = 2
         WHERE asset_record_id = $1 AND generation = 1 AND ordinal = 1",
    )
    .bind(asset_record_id)
    .execute(&pool)
    .await;

    assert!(
        update.is_err(),
        "generation snapshot ordinals must not change"
    );

    let delete = sqlx::query(
        "DELETE FROM asset_refresh_generation_entries
         WHERE asset_record_id = $1 AND generation = 1 AND ordinal = 1",
    )
    .bind(asset_record_id)
    .execute(&pool)
    .await;

    assert!(
        delete.is_err(),
        "generation snapshot entries must not be removed"
    );
}

async fn insert_snapshot(pool: &PgPool, state: &str) -> Uuid {
    let asset_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO asset_record_refresh_state
             (asset_record_id, requested_generation, served_generation)
         VALUES ($1, 1, 1)",
    )
    .bind(asset_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO asset_refresh_generation_entries
             (asset_record_id, generation, ordinal, source_id, review_id, state, not_before)
         VALUES ($1, 1, 1, $2, 'snapshot-review', $3,
                 CASE WHEN $3 = 'cooling' THEN now() + interval '1 hour' END)",
    )
    .bind(asset_id)
    .bind(Uuid::new_v4())
    .bind(state)
    .execute(pool)
    .await
    .unwrap();
    asset_id
}

async fn insert_recovery_job(pool: &PgPool, asset_id: Uuid, refresh: bool) -> Uuid {
    let job_id = Uuid::new_v4();
    let job_type = if refresh {
        "asset_refresh"
    } else {
        "asset_repair"
    };
    let payload = if refresh {
        serde_json::json!({"generation": 1})
    } else {
        serde_json::json!({"asset_id": asset_id})
    };
    sqlx::query(
        "INSERT INTO jobs
             (id, job_type, status, priority, run_after, claim_count,
              failure_count, max_attempts, payload_json, dedupe_key, created_at, updated_at)
         VALUES ($1, $2, 'queued', 0, now(), 0, 0, 3, $3, $4, now(), now())",
    )
    .bind(job_id)
    .bind(job_type)
    .bind(payload)
    .bind(format!("migration-cleanup:{job_id}"))
    .execute(pool)
    .await
    .unwrap();
    if refresh {
        sqlx::query(
            "INSERT INTO asset_repair_jobs
                 (job_id, admission_class, refresh_source_id, refresh_review_id,
                  chain_id, chain_ordinal, expired_asset_record_id)
             VALUES ($1, 'internal_refresh', $2, 'snapshot-review', $3, 1, $4)",
        )
        .bind(job_id)
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .bind(asset_id)
        .execute(pool)
        .await
        .unwrap();
    } else {
        sqlx::query(
            "INSERT INTO asset_repair_jobs (job_id, admission_class)
             VALUES ($1, 'public_repair')",
        )
        .bind(job_id)
        .execute(pool)
        .await
        .unwrap();
    }
    job_id
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn guarded_pruning_rejects_unfinished_snapshots(pool: PgPool) {
    for state in ["unseen", "cooling", "in_flight"] {
        let asset_id = insert_snapshot(&pool, state).await;
        sqlx::query("UPDATE asset_record_refresh_state SET completed_generation = 1 WHERE asset_record_id = $1")
            .bind(asset_id).execute(&pool).await.unwrap();
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL werrss.asset_refresh_prune = 'on'")
            .execute(&mut *transaction)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO asset_refresh_generation_entries
                 (asset_record_id, generation, ordinal, source_id, review_id, state)
             VALUES ($1, 1, 2, $2, 'done-sibling', 'done')",
        )
        .bind(asset_id)
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .unwrap();
        let deletion = sqlx::query(
            "DELETE FROM asset_refresh_generation_entries
             WHERE asset_record_id = $1 AND ordinal = 2",
        )
        .bind(asset_id)
        .execute(&mut *transaction)
        .await;
        assert!(deletion.is_err(), "guard must retain {state} snapshots");
        transaction.rollback().await.unwrap();
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn terminal_snapshot_requires_guard_and_completed_high_water(pool: PgPool) {
    let asset_id = insert_snapshot(&pool, "done").await;
    assert!(
        sqlx::query("DELETE FROM asset_refresh_generation_entries WHERE asset_record_id = $1")
            .bind(asset_id)
            .execute(&pool)
            .await
            .is_err()
    );

    let count: i64 = sqlx::query_scalar("SELECT prune_asset_refresh_generations()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "all-done entries await generation completion");
    sqlx::query(
        "UPDATE asset_record_refresh_state SET completed_generation = 1 WHERE asset_record_id = $1",
    )
    .bind(asset_id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        sqlx::query("DELETE FROM asset_refresh_generation_entries WHERE asset_record_id = $1")
            .bind(asset_id)
            .execute(&pool)
            .await
            .is_err(),
        "completed generation deletion still requires the guard"
    );
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL werrss.asset_refresh_prune = 'on'")
        .execute(&mut *transaction)
        .await
        .unwrap();
    assert!(sqlx::query("UPDATE asset_refresh_generation_entries SET review_id = 'changed' WHERE asset_record_id = $1")
        .bind(asset_id).execute(&mut *transaction).await.is_err(),
        "prune guard never permits identity updates");
    transaction.rollback().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT prune_asset_refresh_generations()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let retained: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM asset_record_refresh_state WHERE asset_record_id = $1",
    )
    .bind(asset_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        retained, 1,
        "snapshot pruning preserves generation high-water state"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn terminal_snapshot_retains_active_chain_and_job_references(pool: PgPool) {
    let asset_id = insert_snapshot(&pool, "done").await;
    sqlx::query("UPDATE asset_record_refresh_state SET completed_generation = 1, active_chain_id = $2 WHERE asset_record_id = $1")
        .bind(asset_id).bind(Uuid::new_v4()).execute(&pool).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT prune_asset_refresh_generations()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "active chain retains its snapshot");
    sqlx::query(
        "UPDATE asset_record_refresh_state SET active_chain_id = NULL WHERE asset_record_id = $1",
    )
    .bind(asset_id)
    .execute(&pool)
    .await
    .unwrap();
    let job_id = insert_recovery_job(&pool, asset_id, true).await;
    let count: i64 = sqlx::query_scalar("SELECT prune_asset_refresh_generations()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "active refresh job retains its snapshot");
    sqlx::query("UPDATE jobs SET status = 'succeeded', claim_count = 1, started_at = now(), finished_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT prune_asset_refresh_generations()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "terminal job releases its snapshot");
    let mut transaction = pool.begin().await.unwrap();
    let guard: Option<String> =
        sqlx::query_scalar("SELECT current_setting('werrss.asset_refresh_prune', true)")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    assert_ne!(
        guard.as_deref(),
        Some("on"),
        "prune guard must not leak into later transactions"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn orphan_refresh_state_waits_for_asset_and_recovery_references(pool: PgPool) {
    let asset_id = insert_snapshot(&pool, "done").await;
    sqlx::query(
        "UPDATE asset_record_refresh_state SET completed_generation = 1 WHERE asset_record_id = $1",
    )
    .bind(asset_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("SELECT prune_asset_refresh_generations()")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO asset_records (id, source_url, version, final_url) VALUES ($1, 'https://cdn.example.test/retained.png', 1, 'https://cdn.example.test/retained.png')")
        .bind(asset_id).execute(&pool).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT prune_orphan_asset_refresh_state()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "live assets retain generation high-water state");
    sqlx::query("DELETE FROM asset_records WHERE id = $1")
        .bind(asset_id)
        .execute(&pool)
        .await
        .unwrap();
    let refresh_job = insert_recovery_job(&pool, asset_id, true).await;
    let count: i64 = sqlx::query_scalar("SELECT prune_orphan_asset_refresh_state()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "active recovery metadata retains orphan state");
    sqlx::query("UPDATE jobs SET status = 'succeeded', claim_count = 1, started_at = now(), finished_at = now() WHERE id = $1")
        .bind(refresh_job)
        .execute(&pool)
        .await
        .unwrap();
    let repair_job = insert_recovery_job(&pool, asset_id, false).await;
    sqlx::query("UPDATE jobs SET status = 'failed', finished_at = now() WHERE id = $1")
        .bind(repair_job)
        .execute(&pool)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT prune_orphan_asset_refresh_state()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "retained terminal repair payload retains orphan state"
    );
    sqlx::query("DELETE FROM jobs WHERE id = $1")
        .bind(repair_job)
        .execute(&pool)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT prune_orphan_asset_refresh_state()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn refresh_job_requires_a_non_null_chain_ordinal(pool: PgPool) {
    let job_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();

    sqlx::query(
        "INSERT INTO jobs
             (id, job_type, status, priority, run_after, claim_count,
              failure_count, max_attempts, payload_json, dedupe_key,
              created_at, updated_at)
         VALUES ($1, 'asset_refresh', 'queued', 0, now(), 0, 0, 3, '{}'::jsonb,
                 $2, now(), now())",
    )
    .bind(job_id)
    .bind(format!("asset-refresh:{}:{}", source_id, Uuid::new_v4()))
    .execute(&pool)
    .await
    .unwrap();

    let insert = sqlx::query(
        "INSERT INTO asset_repair_jobs
             (job_id, asset_record_id, admission_class, refresh_source_id,
              refresh_review_id, chain_id, chain_ordinal,
              expired_asset_record_id)
         VALUES ($1, NULL, 'internal_refresh', $2, 'refresh-review', $3, NULL, $4)",
    )
    .bind(job_id)
    .bind(source_id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await;

    assert!(insert.is_err(), "refresh job ordinal must be non-null");
}
