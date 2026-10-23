use chrono::{DateTime, TimeZone, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;
use werrss::{
    application::config_transfer::{
        ConfigBundle, ConfigExportSelection, ImportPreview, ImportResult, PortableSource,
        PortableWeReadAccount, ValidatedConfigBundle,
    },
    persistence::repositories::config_transfer_repository::{
        ConfigTransferRepositoryError, PostgresConfigTransferRepository,
    },
};

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("timestamp should be valid")
}

fn sample_portable_source(
    id: Uuid,
    book_id: &str,
    display_name: &str,
    account_id: Option<Uuid>,
) -> PortableSource {
    PortableSource {
        id,
        book_id: book_id.to_string(),
        display_name: display_name.to_string(),
        article_url: Some(format!("https://mp.weixin.qq.com/s/{}", book_id)),
        account_id,
        enabled: true,
        sync_interval_seconds: 3600,
        rss_item_limit: 20,
        priority: 5,
        max_attempts: 3,
    }
}

fn sample_portable_account(id: Uuid, display_name: &str, enabled: bool) -> PortableWeReadAccount {
    PortableWeReadAccount {
        account_id: id,
        display_name: display_name.to_string(),
        enabled,
    }
}

fn make_validated_bundle(
    sources: Option<Vec<PortableSource>>,
    accounts: Option<Vec<PortableWeReadAccount>>,
) -> ValidatedConfigBundle {
    let bundle = ConfigBundle::new(Utc::now(), sources, accounts);
    ValidatedConfigBundle::try_from(bundle).expect("test bundle must be valid")
}

async fn seed_enrolled_account(
    pool: &PgPool,
    account_id: Uuid,
    display_name: &str,
    disabled: bool,
    ciphertext: &[u8],
    expires_at: DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO weread_accounts ( \
             account_id, display_name, credentials_ciphertext, access_expires_at, \
             credential_version, disabled, setup_required, created_at, updated_at \
         ) VALUES ($1, $2, $3, $4, 1, $5, FALSE, clock_timestamp(), clock_timestamp())",
    )
    .bind(account_id)
    .bind(display_name)
    .bind(ciphertext)
    .bind(expires_at)
    .bind(disabled)
    .execute(pool)
    .await
    .expect("enrolled account seed should succeed");
}

async fn seed_placeholder_account(
    pool: &PgPool,
    account_id: Uuid,
    display_name: &str,
    disabled: bool,
) {
    sqlx::query(
        "INSERT INTO weread_accounts ( \
             account_id, display_name, credentials_ciphertext, access_expires_at, \
             credential_version, disabled, setup_required, created_at, updated_at \
         ) VALUES ($1, $2, NULL, NULL, 1, $3, TRUE, clock_timestamp(), clock_timestamp())",
    )
    .bind(account_id)
    .bind(display_name)
    .bind(disabled)
    .execute(pool)
    .await
    .expect("placeholder account seed should succeed");
}

#[allow(clippy::too_many_arguments)]
async fn seed_source_full(
    pool: &PgPool,
    id: Uuid,
    book_id: &str,
    display_name: &str,
    article_url: Option<&str>,
    account_id: Option<Uuid>,
    feed_revision: i64,
    enabled: bool,
    scheduling_gate: &str,
    next_fetch_at: DateTime<Utc>,
    failure_cooldown_until: Option<DateTime<Utc>>,
    schedule_reserved_until: Option<DateTime<Utc>>,
    sync_interval_seconds: i64,
    rss_item_limit: i64,
    priority: i32,
    max_attempts: i64,
) {
    sqlx::query(
        "INSERT INTO sources ( \
             id, book_id, display_name, article_url, account_id, feed_revision, \
             enabled, scheduling_gate, sync_interval_seconds, rss_item_limit, \
             next_fetch_at, failure_cooldown_until, schedule_reserved_until, \
             priority, max_attempts, created_at, updated_at \
         ) VALUES ( \
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, \
             clock_timestamp(), clock_timestamp() \
         )",
    )
    .bind(id)
    .bind(book_id)
    .bind(display_name)
    .bind(article_url)
    .bind(account_id)
    .bind(feed_revision)
    .bind(enabled)
    .bind(scheduling_gate)
    .bind(sync_interval_seconds)
    .bind(rss_item_limit)
    .bind(next_fetch_at)
    .bind(failure_cooldown_until)
    .bind(schedule_reserved_until)
    .bind(priority)
    .bind(max_attempts)
    .execute(pool)
    .await
    .expect("source seed should succeed");
}

async fn seed_article(pool: &PgPool, source_id: Uuid, review_id: &str, title: &str) {
    sqlx::query(
        "INSERT INTO articles ( \
             source_id, review_id, title, published_at, fetched_at, created_at, updated_at \
         ) VALUES ($1, $2, $3, clock_timestamp(), clock_timestamp(), clock_timestamp(), clock_timestamp())",
    )
    .bind(source_id)
    .bind(review_id)
    .bind(title)
    .execute(pool)
    .await
    .expect("article seed should succeed");
}

async fn seed_job(pool: &PgPool, job_id: Uuid, source_id: Uuid, dedupe_key: &str) {
    sqlx::query(
        "INSERT INTO jobs ( \
             id, job_type, source_id, status, priority, run_after, claim_count, \
             failure_count, max_attempts, payload_json, dedupe_key, created_at, updated_at \
         ) VALUES ( \
             $1, 'source_sync', $2, 'queued', 0, clock_timestamp(), 0, 0, 3, \
             '{}'::jsonb, $3, clock_timestamp(), clock_timestamp() \
         )",
    )
    .bind(job_id)
    .bind(source_id)
    .bind(dedupe_key)
    .execute(pool)
    .await
    .expect("job seed should succeed");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn export_sources_only_includes_configuration_columns_and_omits_accounts(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let account_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();

    seed_enrolled_account(
        &pool,
        account_id,
        "Account Alpha",
        false,
        b"top-secret-ciphertext",
        at(1_800_000_000),
    )
    .await;

    seed_source_full(
        &pool,
        source_id,
        "book-export-1",
        "Source Export 1",
        Some("https://mp.weixin.qq.com/s/sample-export"),
        Some(account_id),
        3,
        true,
        "ready",
        at(1_000_000),
        None,
        None,
        7200,
        25,
        10,
        5,
    )
    .await;

    seed_article(&pool, source_id, "rev-001", "Article One").await;
    seed_job(&pool, Uuid::new_v4(), source_id, "dedupe-job-1").await;

    let selection = ConfigExportSelection::new(true, false).expect("selection should be valid");
    let bundle = repo.export(selection).await.expect("export should succeed");

    assert!(bundle.weread_accounts.is_none());
    let sources = bundle
        .sources
        .as_ref()
        .expect("sources section should be present");
    assert_eq!(sources.len(), 1);

    let src = &sources[0];
    assert_eq!(src.id, source_id);
    assert_eq!(src.book_id, "book-export-1");
    assert_eq!(src.display_name, "Source Export 1");
    assert_eq!(
        src.article_url.as_deref(),
        Some("https://mp.weixin.qq.com/s/sample-export")
    );
    assert_eq!(src.account_id, Some(account_id));
    assert!(src.enabled);
    assert_eq!(src.sync_interval_seconds, 7200);
    assert_eq!(src.rss_item_limit, 25);
    assert_eq!(src.priority, 10);
    assert_eq!(src.max_attempts, 5);

    let json_bytes = bundle
        .serialize_limited()
        .expect("serialization should succeed");
    let json_str = String::from_utf8(json_bytes).expect("JSON should be valid utf-8");

    assert!(!json_str.contains("top-secret-ciphertext"));
    assert!(!json_str.contains("credentials_ciphertext"));
    assert!(!json_str.contains("access_expires_at"));
    assert!(!json_str.contains("setup_required"));
    assert!(!json_str.contains("Article One"));
    assert!(!json_str.contains("rev-001"));
    assert!(!json_str.contains("dedupe-job-1"));
    assert!(!json_str.contains("scheduling_gate"));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn export_accounts_only_includes_portable_columns_and_omits_sources(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let account_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();

    seed_enrolled_account(
        &pool,
        account_id,
        "Account Beta",
        true,
        b"very-secret-token",
        at(1_900_000_000),
    )
    .await;

    seed_source_full(
        &pool,
        source_id,
        "book-export-2",
        "Source Export 2",
        None,
        Some(account_id),
        1,
        true,
        "ready",
        at(1_000),
        None,
        None,
        3600,
        20,
        0,
        3,
    )
    .await;

    let selection = ConfigExportSelection::new(false, true).expect("selection should be valid");
    let bundle = repo.export(selection).await.expect("export should succeed");

    assert!(bundle.sources.is_none());
    let accounts = bundle
        .weread_accounts
        .as_ref()
        .expect("accounts section should be present");
    assert_eq!(accounts.len(), 1);

    let acct = &accounts[0];
    assert_eq!(acct.account_id, account_id);
    assert_eq!(acct.display_name, "Account Beta");
    assert!(!acct.enabled); // disabled=true in DB maps to enabled=false

    let json_bytes = bundle
        .serialize_limited()
        .expect("serialization should succeed");
    let json_str = String::from_utf8(json_bytes).expect("JSON should be valid utf-8");

    assert!(!json_str.contains("very-secret-token"));
    assert!(!json_str.contains("credentials_ciphertext"));
    assert!(!json_str.contains("access_expires_at"));
    assert!(!json_str.contains("setup_required"));
    assert!(!json_str.contains("book-export-2"));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn export_both_sections_includes_both_consistently(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let account_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();

    seed_placeholder_account(&pool, account_id, "Placeholder Account", false).await;
    seed_source_full(
        &pool,
        source_id,
        "book-export-3",
        "Source Export 3",
        None,
        Some(account_id),
        0,
        true,
        "ready",
        at(2_000),
        None,
        None,
        3600,
        20,
        0,
        3,
    )
    .await;

    let selection = ConfigExportSelection::new(true, true).expect("selection should be valid");
    let bundle = repo.export(selection).await.expect("export should succeed");

    assert_eq!(bundle.sources.as_ref().map(Vec::len), Some(1));
    assert_eq!(bundle.weread_accounts.as_ref().map(Vec::len), Some(1));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn preview_and_apply_insert_new_source_and_account_by_stable_id(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let account_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();

    let account = sample_portable_account(account_id, "New Brand Account", true);
    let source = sample_portable_source(source_id, "book-new-1", "Brand Source", Some(account_id));

    let bundle = make_validated_bundle(Some(vec![source]), Some(vec![account]));

    let preview = repo.preview(&bundle).await.expect("preview should succeed");
    assert_eq!(
        preview,
        ImportPreview {
            sources_created: 1,
            sources_updated: 0,
            accounts_created: 1,
            accounts_updated: 0,
            placeholders_created: 1,
            account_bindings_preserved: 1,
            account_bindings_cleared: 0,
            conflicts: vec![],
        }
    );

    let result = repo.apply(bundle).await.expect("apply should succeed");
    assert_eq!(
        result,
        ImportResult {
            sources_created: 1,
            sources_updated: 0,
            accounts_created: 1,
            accounts_updated: 0,
            placeholders_created: 1,
            account_bindings_preserved: 1,
            account_bindings_cleared: 0,
        }
    );

    // Verify database state:
    let account_row = sqlx::query(
        "SELECT display_name, credentials_ciphertext, access_expires_at, credential_version, disabled, setup_required \
         FROM weread_accounts WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_one(&pool)
    .await
    .expect("account should exist in database");

    assert_eq!(
        account_row.get::<String, _>("display_name"),
        "New Brand Account"
    );
    assert!(account_row
        .get::<Option<Vec<u8>>, _>("credentials_ciphertext")
        .is_none());
    assert!(account_row
        .get::<Option<DateTime<Utc>>, _>("access_expires_at")
        .is_none());
    assert_eq!(account_row.get::<i64, _>("credential_version"), 1);
    assert!(!account_row.get::<bool, _>("disabled"));
    assert!(account_row.get::<bool, _>("setup_required"));

    let source_row = sqlx::query(
        "SELECT book_id, display_name, account_id, feed_revision, enabled, scheduling_gate \
         FROM sources WHERE id = $1",
    )
    .bind(source_id)
    .fetch_one(&pool)
    .await
    .expect("source should exist in database");

    assert_eq!(source_row.get::<String, _>("book_id"), "book-new-1");
    assert_eq!(source_row.get::<String, _>("display_name"), "Brand Source");
    assert_eq!(
        source_row.get::<Option<Uuid>, _>("account_id"),
        Some(account_id)
    );
    assert_eq!(source_row.get::<i64, _>("feed_revision"), 0);
    assert!(source_row.get::<bool, _>("enabled"));
    assert_eq!(source_row.get::<String, _>("scheduling_gate"), "ready");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn preview_and_apply_update_existing_source_and_preserve_scheduler_fields(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let source_id = Uuid::new_v4();
    let future_fetch = at(1_999_000_000);
    let future_cooldown = at(1_999_000_100);
    let future_reserved = at(1_999_000_200);

    seed_source_full(
        &pool,
        source_id,
        "book-existing-1",
        "Original Name",
        Some("https://mp.weixin.qq.com/s/orig"),
        None,
        5,
        true,
        "ready",
        future_fetch,
        Some(future_cooldown),
        Some(future_reserved),
        3600,
        20,
        0,
        3,
    )
    .await;

    // Bundle updates portable sync_interval, priority, max_attempts, but NOT feed-visible fields
    let updated_source = PortableSource {
        id: source_id,
        book_id: "book-existing-1".to_string(),
        display_name: "Original Name".to_string(),
        article_url: Some("https://mp.weixin.qq.com/s/orig".to_string()),
        account_id: None,
        enabled: false,
        sync_interval_seconds: 7200,
        rss_item_limit: 20,
        priority: 15,
        max_attempts: 6,
    };

    let bundle = make_validated_bundle(Some(vec![updated_source]), None);

    let preview = repo.preview(&bundle).await.expect("preview should succeed");
    assert_eq!(preview.sources_created, 0);
    assert_eq!(preview.sources_updated, 1);
    assert!(preview.conflicts.is_empty());

    let result = repo.apply(bundle).await.expect("apply should succeed");
    assert_eq!(result.sources_updated, 1);

    let row = sqlx::query(
        "SELECT feed_revision, enabled, scheduling_gate, sync_interval_seconds, priority, max_attempts, \
                next_fetch_at, failure_cooldown_until, schedule_reserved_until \
         FROM sources WHERE id = $1",
    )
    .bind(source_id)
    .fetch_one(&pool)
    .await
    .expect("source should exist");

    assert_eq!(row.get::<i64, _>("feed_revision"), 5); // Unchanged!
    assert!(!row.get::<bool, _>("enabled"));
    assert_eq!(row.get::<String, _>("scheduling_gate"), "ready");
    assert_eq!(row.get::<i64, _>("sync_interval_seconds"), 7200);
    assert_eq!(row.get::<i32, _>("priority"), 15);
    assert_eq!(row.get::<i64, _>("max_attempts"), 6);
    assert_eq!(row.get::<DateTime<Utc>, _>("next_fetch_at"), future_fetch);
    assert_eq!(
        row.get::<Option<DateTime<Utc>>, _>("failure_cooldown_until"),
        Some(future_cooldown)
    );
    assert_eq!(
        row.get::<Option<DateTime<Utc>>, _>("schedule_reserved_until"),
        Some(future_reserved)
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_feed_revision_advances_only_for_feed_visible_fields(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let source_id = Uuid::new_v4();

    seed_source_full(
        &pool,
        source_id,
        "book-rev-1",
        "Initial Name",
        Some("https://mp.weixin.qq.com/s/rev1"),
        None,
        10,
        true,
        "ready",
        at(100),
        None,
        None,
        3600,
        20,
        0,
        3,
    )
    .await;

    // Subcase 1: Change display_name -> feed_revision should advance to 11
    let mut s1 = sample_portable_source(source_id, "book-rev-1", "Initial Name", None);
    s1.display_name = "New Changed Name".to_string();
    let b1 = make_validated_bundle(Some(vec![s1]), None);
    repo.apply(b1).await.expect("apply 1 should succeed");

    let rev1: i64 = sqlx::query_scalar("SELECT feed_revision FROM sources WHERE id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rev1, 11);

    // Subcase 2: Change article_url -> feed_revision should advance to 12
    let mut s2 = sample_portable_source(source_id, "book-rev-1", "New Changed Name", None);
    s2.article_url = Some("https://mp.weixin.qq.com/s/rev2".to_string());
    let b2 = make_validated_bundle(Some(vec![s2]), None);
    repo.apply(b2).await.expect("apply 2 should succeed");

    let rev2: i64 = sqlx::query_scalar("SELECT feed_revision FROM sources WHERE id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rev2, 12);

    // Subcase 3: Change rss_item_limit -> feed_revision should advance to 13
    let mut s3 = sample_portable_source(source_id, "book-rev-1", "New Changed Name", None);
    s3.article_url = Some("https://mp.weixin.qq.com/s/rev2".to_string());
    s3.rss_item_limit = 40;
    let b3 = make_validated_bundle(Some(vec![s3]), None);
    repo.apply(b3).await.expect("apply 3 should succeed");

    let rev3: i64 = sqlx::query_scalar("SELECT feed_revision FROM sources WHERE id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rev3, 13);

    // Subcase 4: Change only non-feed-visible fields (sync interval, priority, max attempts) -> remains 13
    let mut s4 = sample_portable_source(source_id, "book-rev-1", "New Changed Name", None);
    s4.article_url = Some("https://mp.weixin.qq.com/s/rev2".to_string());
    s4.rss_item_limit = 40;
    s4.sync_interval_seconds = 7200;
    s4.priority = 99;
    s4.max_attempts = 10;
    let b4 = make_validated_bundle(Some(vec![s4]), None);
    repo.apply(b4).await.expect("apply 4 should succeed");

    let rev4: i64 = sqlx::query_scalar("SELECT feed_revision FROM sources WHERE id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rev4, 13);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn account_placeholder_creation_and_credential_preservation_on_update(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let account_id = Uuid::new_v4();
    let ciphertext = b"immutable-credentials-ciphertext";
    let expires_at = at(1_888_888_888);

    seed_enrolled_account(
        &pool,
        account_id,
        "Original Enrolled Account",
        false,
        ciphertext,
        expires_at,
    )
    .await;

    let updated_account = sample_portable_account(account_id, "Updated Name Account", false);
    let bundle = make_validated_bundle(None, Some(vec![updated_account]));

    let preview = repo.preview(&bundle).await.expect("preview should succeed");
    assert_eq!(preview.accounts_created, 0);
    assert_eq!(preview.accounts_updated, 1);
    assert_eq!(preview.placeholders_created, 0);

    let result = repo.apply(bundle).await.expect("apply should succeed");
    assert_eq!(result.accounts_updated, 1);

    let row = sqlx::query(
        "SELECT display_name, credentials_ciphertext, access_expires_at, credential_version, disabled, setup_required \
         FROM weread_accounts WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_one(&pool)
    .await
    .expect("account must exist");

    assert_eq!(row.get::<String, _>("display_name"), "Updated Name Account");
    assert!(row.get::<bool, _>("disabled")); // updated to disabled
    assert!(!row.get::<bool, _>("setup_required")); // preserved!
    assert_eq!(row.get::<i64, _>("credential_version"), 1); // preserved!
    assert_eq!(row.get::<Vec<u8>, _>("credentials_ciphertext"), ciphertext); // preserved!
    assert_eq!(row.get::<DateTime<Utc>, _>("access_expires_at"), expires_at); // preserved!
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_book_id_conflict_reports_in_preview_and_rolls_back_in_apply(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let existing_source_id = Uuid::new_v4();
    let incoming_source_id = Uuid::new_v4();
    let shared_book_id = "book-collision-100";

    seed_source_full(
        &pool,
        existing_source_id,
        shared_book_id,
        "Source Already Existing",
        None,
        None,
        0,
        true,
        "ready",
        at(100),
        None,
        None,
        3600,
        20,
        0,
        3,
    )
    .await;

    let incoming_source = sample_portable_source(
        incoming_source_id,
        shared_book_id,
        "Source Inbound With Duplicate BookId",
        None,
    );
    let bundle = make_validated_bundle(Some(vec![incoming_source]), None);

    let preview = repo.preview(&bundle).await.expect("preview should succeed");
    assert_eq!(preview.conflicts.len(), 1);
    let conflict = &preview.conflicts[0];
    assert_eq!(conflict.kind, "source_book_id_collision");
    assert_eq!(conflict.source_id, Some(incoming_source_id));
    assert_eq!(conflict.existing_source_id, Some(existing_source_id));
    assert_eq!(conflict.book_id.as_deref(), Some(shared_book_id));

    let err = repo
        .apply(bundle)
        .await
        .expect_err("apply must fail on conflict");
    match err {
        ConfigTransferRepositoryError::Conflict(c) => {
            assert_eq!(c.kind, "source_book_id_collision");
            assert_eq!(c.source_id, Some(incoming_source_id));
            assert_eq!(c.existing_source_id, Some(existing_source_id));
            assert_eq!(c.book_id.as_deref(), Some(shared_book_id));
        }
        other => panic!("expected Conflict error, got {:?}", other),
    }

    // Verify incoming source was not created
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sources WHERE id = $1)")
        .bind(incoming_source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!exists);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn sources_only_import_clears_missing_account_binding_and_retains_existing(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let existing_account_id = Uuid::new_v4();
    let missing_account_id = Uuid::new_v4();

    seed_placeholder_account(&pool, existing_account_id, "Existing Account", false).await;

    let s1_id = Uuid::new_v4();
    let s2_id = Uuid::new_v4();

    let s1 = sample_portable_source(
        s1_id,
        "book-bind-1",
        "Source With Existing Acct",
        Some(existing_account_id),
    );
    let s2 = sample_portable_source(
        s2_id,
        "book-bind-2",
        "Source With Missing Acct",
        Some(missing_account_id),
    );

    let bundle = make_validated_bundle(Some(vec![s1, s2]), None);

    let preview = repo.preview(&bundle).await.expect("preview should succeed");
    assert_eq!(preview.account_bindings_preserved, 1);
    assert_eq!(preview.account_bindings_cleared, 1);

    let result = repo.apply(bundle).await.expect("apply should succeed");
    assert_eq!(result.account_bindings_preserved, 1);
    assert_eq!(result.account_bindings_cleared, 1);

    let s1_acct: Option<Uuid> = sqlx::query_scalar("SELECT account_id FROM sources WHERE id = $1")
        .bind(s1_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(s1_acct, Some(existing_account_id));

    let s2_acct: Option<Uuid> = sqlx::query_scalar("SELECT account_id FROM sources WHERE id = $1")
        .bind(s2_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(s2_acct, None); // Cleared because account does not exist locally
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn combined_import_retains_binding_to_newly_created_placeholder(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let new_account_id = Uuid::new_v4();
    let new_source_id = Uuid::new_v4();

    let account = sample_portable_account(new_account_id, "Placeholder Bound Account", true);
    let source = sample_portable_source(
        new_source_id,
        "book-bound-to-new",
        "Source Bound To New",
        Some(new_account_id),
    );

    let bundle = make_validated_bundle(Some(vec![source]), Some(vec![account]));

    let preview = repo.preview(&bundle).await.expect("preview should succeed");
    assert_eq!(preview.placeholders_created, 1);
    assert_eq!(preview.account_bindings_preserved, 1);
    assert_eq!(preview.account_bindings_cleared, 0);

    let result = repo.apply(bundle).await.expect("apply should succeed");
    assert_eq!(result.placeholders_created, 1);
    assert_eq!(result.account_bindings_preserved, 1);
    assert_eq!(result.account_bindings_cleared, 0);

    let bound_id: Option<Uuid> = sqlx::query_scalar("SELECT account_id FROM sources WHERE id = $1")
        .bind(new_source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(bound_id, Some(new_account_id));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn enabling_enrolled_disabled_account_resumes_authentication_required_sources_and_leaves_risk_controlled(
    pool: PgPool,
) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let account_id = Uuid::new_v4();
    let auth_source_id = Uuid::new_v4();
    let risk_source_id = Uuid::new_v4();

    // Account is enrolled (setup_required=false), but disabled
    seed_enrolled_account(
        &pool,
        account_id,
        "Disabled Enrolled Acct",
        true, // disabled
        b"valid-ciphertext",
        at(1_900_000_000),
    )
    .await;

    // Source 1 is authentication_required
    seed_source_full(
        &pool,
        auth_source_id,
        "book-auth-gated",
        "Auth Gated Source",
        None,
        Some(account_id),
        0,
        true,
        "authentication_required",
        at(500),
        Some(at(900)),
        Some(at(900)),
        3600,
        20,
        0,
        3,
    )
    .await;

    // Source 2 is risk_controlled
    seed_source_full(
        &pool,
        risk_source_id,
        "book-risk-gated",
        "Risk Gated Source",
        None,
        Some(account_id),
        0,
        true,
        "risk_controlled",
        at(500),
        Some(at(900)),
        Some(at(900)),
        3600,
        20,
        0,
        3,
    )
    .await;

    // Bundle enables the account
    let acct = sample_portable_account(account_id, "Disabled Enrolled Acct", true); // enabled=true
    let bundle = make_validated_bundle(None, Some(vec![acct]));

    repo.apply(bundle).await.expect("apply should succeed");

    // Source 1 should be resumed to ready and due now
    let s1 = sqlx::query(
        "SELECT scheduling_gate, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1",
    )
    .bind(auth_source_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(s1.get::<String, _>("scheduling_gate"), "ready");
    assert!(s1
        .get::<Option<DateTime<Utc>>, _>("failure_cooldown_until")
        .is_none());
    assert!(s1
        .get::<Option<DateTime<Utc>>, _>("schedule_reserved_until")
        .is_none());

    // Source 2 must remain risk_controlled!
    let s2_gate: String = sqlx::query_scalar("SELECT scheduling_gate FROM sources WHERE id = $1")
        .bind(risk_source_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(s2_gate, "risk_controlled");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn enabling_placeholder_account_does_not_resume_sources(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());
    let account_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();

    // Account is a placeholder (setup_required=true) and disabled
    seed_placeholder_account(&pool, account_id, "Placeholder Account", true).await;

    seed_source_full(
        &pool,
        source_id,
        "book-placeholder-gated",
        "Gated Source",
        None,
        Some(account_id),
        0,
        true,
        "authentication_required",
        at(500),
        None,
        None,
        3600,
        20,
        0,
        3,
    )
    .await;

    // Import bundle sets enabled=true on placeholder
    let acct = sample_portable_account(account_id, "Placeholder Account", true);
    let bundle = make_validated_bundle(None, Some(vec![acct]));

    repo.apply(bundle).await.expect("apply should succeed");

    // Source should STILL be authentication_required because account has no credentials!
    let gate: String = sqlx::query_scalar("SELECT scheduling_gate FROM sources WHERE id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(gate, "authentication_required");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn mixed_import_conflict_rolls_back_all_mutations_atomically(pool: PgPool) {
    let repo = PostgresConfigTransferRepository::new(pool.clone());

    let existing_acct_id = Uuid::new_v4();
    let existing_source_id = Uuid::new_v4();
    let clash_book_id = "book-atomic-clash";

    seed_enrolled_account(
        &pool,
        existing_acct_id,
        "Original Account Name",
        false,
        b"existing-ciphertext",
        at(1_800_000_000),
    )
    .await;

    seed_source_full(
        &pool,
        existing_source_id,
        clash_book_id,
        "Existing Source",
        None,
        Some(existing_acct_id),
        0,
        true,
        "ready",
        at(100),
        None,
        None,
        3600,
        20,
        0,
        3,
    )
    .await;

    seed_article(&pool, existing_source_id, "art-1", "Original Article").await;
    seed_job(
        &pool,
        Uuid::new_v4(),
        existing_source_id,
        "job-dedupe-original",
    )
    .await;

    // Bundle contains:
    // 1. New account (to be created as placeholder)
    // 2. Existing account (display_name modified)
    // 3. New source (to be created)
    // 4. Conflicting source (new UUID, but same clash_book_id)
    let new_acct_id = Uuid::new_v4();
    let new_source_id = Uuid::new_v4();
    let conflict_source_id = Uuid::new_v4();

    let new_acct = sample_portable_account(new_acct_id, "New Placeholder Candidate", true);
    let mod_acct = sample_portable_account(existing_acct_id, "Mutated Account Name", false);

    let new_src = sample_portable_source(
        new_source_id,
        "book-brand-new",
        "Brand New Source",
        Some(new_acct_id),
    );
    let conflict_src = sample_portable_source(
        conflict_source_id,
        clash_book_id, // Conflicts with existing_source_id!
        "Conflicting Source",
        None,
    );

    let bundle = make_validated_bundle(
        Some(vec![new_src, conflict_src]),
        Some(vec![new_acct, mod_acct]),
    );

    let err = repo
        .apply(bundle)
        .await
        .expect_err("apply must fail on collision");
    assert!(matches!(err, ConfigTransferRepositoryError::Conflict(_)));

    // Verify COMPLETE rollback:
    // 1. New account was NOT created
    let new_acct_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM weread_accounts WHERE account_id = $1)")
            .bind(new_acct_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!new_acct_exists);

    // 2. Existing account display_name was NOT changed
    let existing_name: String =
        sqlx::query_scalar("SELECT display_name FROM weread_accounts WHERE account_id = $1")
            .bind(existing_acct_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(existing_name, "Original Account Name");

    // 3. New source was NOT created
    let new_src_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sources WHERE id = $1)")
            .bind(new_source_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!new_src_exists);

    // 4. Conflicting source was NOT created
    let conflict_src_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sources WHERE id = $1)")
            .bind(conflict_source_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!conflict_src_exists);

    // 5. Existing article is untouched
    let art_title: String = sqlx::query_scalar(
        "SELECT title FROM articles WHERE source_id = $1 AND review_id = 'art-1'",
    )
    .bind(existing_source_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(art_title, "Original Article");

    // 6. Existing job is untouched
    let job_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE dedupe_key = 'job-dedupe-original'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(job_count, 1);
}
