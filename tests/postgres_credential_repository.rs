use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use chrono::{TimeZone, Utc};
use secrecy::SecretString;
use sqlx::PgPool;
use tokio::time::{sleep, timeout, Duration};
use uuid::Uuid;
use werrss::persistence::repositories::credential_repository::{
    CredentialReplacement, CredentialRepository, CredentialRepositoryError,
    PostgresCredentialRepository,
};
use werrss::{
    application::auth_service::{
        AuthRefreshOutcome, AuthService, AuthServiceConfig, AuthServiceDependencies,
        AuthServiceError, CredentialCipher, CredentialCipherError, CredentialProvision,
        CredentialRefresher, ManualCredentialRefresher, RefreshedCredentials, RingCredentialCipher,
    },
    application::source_sync_acquirer::{
        CredentialRepositoryAccountSelector, WeReadAccountSelector,
    },
    domain::credentials::{WeReadAccountId, WeReadCredentials},
    persistence::repositories::account_lease_repository::{
        AccountLeaseStore, PostgresAccountLeaseRepository,
    },
};

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}

fn account_id() -> WeReadAccountId {
    WeReadAccountId::from_uuid(Uuid::new_v4())
}

#[derive(Clone, Default)]
struct PanicCipher(Arc<AtomicUsize>);

impl CredentialCipher for PanicCipher {
    fn encrypt(
        &self,
        _account_id: WeReadAccountId,
        _credentials: &WeReadCredentials,
    ) -> Result<Vec<u8>, CredentialCipherError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("placeholder auth path must not encrypt before loading the placeholder")
    }

    fn decrypt(
        &self,
        _account_id: WeReadAccountId,
        _ciphertext: &[u8],
    ) -> Result<WeReadCredentials, CredentialCipherError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("setup-required credentials must never reach the cipher")
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn setup_required_auth_lookup_returns_safe_metadata_without_cipher_calls(pool: PgPool) {
    let account_id = account_id();
    seed_placeholder(&pool, account_id).await;
    let cipher = PanicCipher::default();
    let calls = cipher.0.clone();
    let service = AuthService::new(
        AuthServiceDependencies {
            accounts: PostgresCredentialRepository::new(pool.clone()),
            leases: PostgresAccountLeaseRepository::new(pool.clone()),
            refresher: ManualCredentialRefresher,
            cipher,
        },
        AuthServiceConfig::new(
            chrono::Duration::seconds(30),
            chrono::Duration::seconds(30),
            chrono::Duration::seconds(5),
        )
        .unwrap(),
    );
    let account = service
        .account(account_id)
        .await
        .expect("placeholder lookup should return metadata");
    assert!(account.setup_required());
    assert_eq!(account.access_expires_at(), None);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        service.credentials(account_id).await,
        Err(AuthServiceError::SetupRequired { account_id: required }) if required == account_id
    ));
    assert!(matches!(
        service
            .refresh_if_needed(account_id, "placeholder-auth-test")
            .await,
        Err(AuthServiceError::SetupRequired { account_id: required }) if required == account_id
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn account_selection_ignores_expired_and_disabled_accounts(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let expired = account_id();
    let disabled = account_id();
    let usable = account_id();
    repository
        .insert(
            expired,
            "expired",
            b"expired-ciphertext",
            Utc::now() - chrono::Duration::seconds(1),
        )
        .await
        .expect("expired account should be inserted");
    repository
        .insert(
            disabled,
            "disabled",
            b"disabled-ciphertext",
            Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .expect("disabled account should be inserted");
    sqlx::query("UPDATE weread_accounts SET disabled = TRUE WHERE account_id = $1")
        .bind(disabled.as_uuid())
        .execute(&pool)
        .await
        .expect("account should be disabled");
    repository
        .insert(
            usable,
            "usable",
            b"usable-ciphertext",
            Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .expect("usable account should be inserted");

    let selector = CredentialRepositoryAccountSelector::new(repository);
    assert_eq!(selector.select_account(None).await.unwrap(), Some(usable));
    assert_eq!(selector.select_account(Some(expired)).await.unwrap(), None);
    assert_eq!(selector.select_account(Some(disabled)).await.unwrap(), None);
    assert_eq!(
        selector.select_account(Some(usable)).await.unwrap(),
        Some(usable)
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn encrypted_account_rows_support_versioned_replacement(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let account_id = account_id();
    let inserted = repository
        .insert(account_id, "primary", b"ciphertext-v1", at(3_600))
        .await
        .expect("account should be inserted");
    assert_eq!(inserted.account().credential_version(), 1);
    assert_eq!(inserted.ciphertext(), Some(b"ciphertext-v1".as_slice()));

    let raw: Vec<u8> = sqlx::query_scalar(
        "SELECT credentials_ciphertext FROM weread_accounts WHERE account_id = $1",
    )
    .bind(account_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("ciphertext should be stored");
    assert_eq!(raw, b"ciphertext-v1");

    let lease = leases
        .acquire(account_id, "credential-test", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("test should own the account lease");
    let replaced = repository
        .replace(CredentialReplacement {
            account_id,
            display_name: "primary".to_owned(),
            expected_version: 1,
            ciphertext: b"ciphertext-v2".to_vec(),
            access_expires_at: at(7_200),
            lease_owner: "credential-test".to_owned(),
            lease_token: lease.token(),
        })
        .await
        .expect("the expected version should replace credentials");
    assert_eq!(replaced.account().credential_version(), 2);
    assert_eq!(replaced.account().access_expires_at(), Some(at(7_200)));
    assert!(matches!(
        repository
            .replace(CredentialReplacement {
                account_id,
                display_name: "primary".to_owned(),
                expected_version: 1,
                ciphertext: b"stale".to_vec(),
                access_expires_at: at(8_000),
                lease_owner: "credential-test".to_owned(),
                lease_token: lease.token(),
            })
            .await,
        Err(CredentialRepositoryError::Conflict { account_id: lost }) if lost == account_id
    ));
    leases
        .release(account_id, "credential-test", lease.token())
        .await
        .expect("test lease should be released");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn credential_replacement_rejects_a_stale_account_lease(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let account_id = account_id();
    repository
        .insert(account_id, "primary", b"ciphertext-v1", at(3_600))
        .await
        .expect("account should be inserted");

    let stale = leases
        .acquire(account_id, "old-worker", chrono::Duration::minutes(5))
        .await
        .expect("old lease acquisition should succeed")
        .expect("old worker should own the lease");
    sqlx::query("UPDATE account_leases SET lease_until = clock_timestamp() - interval '1 second' WHERE account_id = $1")
        .bind(account_id.as_uuid())
        .execute(&pool)
        .await
        .expect("test should expire the old lease");
    let current = leases
        .acquire(account_id, "new-worker", chrono::Duration::minutes(5))
        .await
        .expect("takeover should succeed")
        .expect("new worker should take over the expired lease");

    assert!(matches!(
        repository
            .replace(CredentialReplacement {
                account_id,
                display_name: "primary".to_owned(),
                expected_version: 1,
                ciphertext: b"stale-worker-write".to_vec(),
                access_expires_at: at(8_000),
                lease_owner: "old-worker".to_owned(),
                lease_token: stale.token(),
            })
            .await,
        Err(CredentialRepositoryError::Conflict { account_id: lost }) if lost == account_id
    ));
    let updated = repository
        .replace(CredentialReplacement {
            account_id,
            display_name: "primary".to_owned(),
            expected_version: 1,
            ciphertext: b"current-worker-write".to_vec(),
            access_expires_at: at(8_000),
            lease_owner: "new-worker".to_owned(),
            lease_token: current.token(),
        })
        .await
        .expect("current lease should fence the replacement");
    assert_eq!(
        updated.ciphertext(),
        Some(b"current-worker-write".as_slice())
    );
    leases
        .release(account_id, "new-worker", current.token())
        .await
        .expect("current lease should be released");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn credential_replacement_waits_for_the_live_lease_row_lock(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let account_id = account_id();
    repository
        .insert(account_id, "primary", b"ciphertext-v1", at(3_600))
        .await
        .expect("account should be inserted");
    let lease = leases
        .acquire(account_id, "credential-test", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("test should own the account lease");

    let mut lock_holder = pool
        .begin()
        .await
        .expect("lock-holder transaction should begin");
    sqlx::query("SELECT account_id FROM account_leases WHERE account_id = $1 FOR UPDATE")
        .bind(account_id.as_uuid())
        .fetch_one(&mut *lock_holder)
        .await
        .expect("test should hold the account lease row lock");

    let replacement = CredentialReplacement {
        account_id,
        display_name: "primary".to_owned(),
        expected_version: 1,
        ciphertext: b"ciphertext-v2".to_vec(),
        access_expires_at: at(7_200),
        lease_owner: "credential-test".to_owned(),
        lease_token: lease.token(),
    };
    let replacement_task = tokio::spawn(async move { repository.replace(replacement).await });
    timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%WITH live_lease AS MATERIALIZED%' AND cardinality(pg_blocking_pids(pid)) > 0)",
            )
            .fetch_one(&pool)
            .await
            .expect("database should expose the replacement lock wait");
            if blocked {
                break;
            }
            assert!(
                !replacement_task.is_finished(),
                "replacement completed before waiting on the lease row lock"
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement should reach the lease row lock within the test timeout");

    lock_holder
        .commit()
        .await
        .expect("lock-holder transaction should commit");
    let replaced = replacement_task
        .await
        .expect("replacement task should finish")
        .expect("replacement should succeed after the lease lock is released");
    assert_eq!(replaced.account().credential_version(), 2);
    leases
        .release(account_id, "credential-test", lease.token())
        .await
        .expect("test lease should be released");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn credential_replacement_for_another_account_is_not_blocked_by_a_lease_row_lock(
    pool: PgPool,
) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let locked_account_id = account_id();
    let other_account_id = account_id();
    repository
        .insert(
            locked_account_id,
            "locked",
            b"locked-ciphertext-v1",
            at(3_600),
        )
        .await
        .expect("locked account should be inserted");
    repository
        .insert(other_account_id, "other", b"other-ciphertext-v1", at(3_600))
        .await
        .expect("other account should be inserted");
    let locked_lease = leases
        .acquire(
            locked_account_id,
            "credential-test-locked",
            chrono::Duration::minutes(5),
        )
        .await
        .expect("locked account lease acquisition should succeed")
        .expect("test should own the locked account lease");
    let other_lease = leases
        .acquire(
            other_account_id,
            "credential-test-other",
            chrono::Duration::minutes(5),
        )
        .await
        .expect("other account lease acquisition should succeed")
        .expect("test should own the other account lease");

    let mut lock_holder = pool
        .begin()
        .await
        .expect("lock-holder transaction should begin");
    sqlx::query("SELECT account_id FROM account_leases WHERE account_id = $1 FOR UPDATE")
        .bind(locked_account_id.as_uuid())
        .fetch_one(&mut *lock_holder)
        .await
        .expect("test should hold the locked account lease row");

    let replaced = timeout(
        Duration::from_secs(5),
        repository.replace(CredentialReplacement {
            account_id: other_account_id,
            display_name: "other-updated".to_owned(),
            expected_version: 1,
            ciphertext: b"other-ciphertext-v2".to_vec(),
            access_expires_at: at(7_200),
            lease_owner: "credential-test-other".to_owned(),
            lease_token: other_lease.token(),
        }),
    )
    .await
    .expect("an unrelated account replacement should not wait for the locked row")
    .expect("other account replacement should succeed");
    assert_eq!(replaced.account().account_id(), other_account_id);
    assert_eq!(replaced.account().credential_version(), 2);

    lock_holder
        .commit()
        .await
        .expect("lock-holder transaction should commit");
    leases
        .release(
            locked_account_id,
            "credential-test-locked",
            locked_lease.token(),
        )
        .await
        .expect("locked account lease should be released");
    leases
        .release(
            other_account_id,
            "credential-test-other",
            other_lease.token(),
        )
        .await
        .expect("other account lease should be released");
}

#[derive(Clone)]
struct TestRefresher;

#[async_trait::async_trait]
impl CredentialRefresher for TestRefresher {
    async fn refresh(
        &self,
        _account_id: WeReadAccountId,
        refresh_token: &str,
    ) -> Result<RefreshedCredentials, werrss::application::auth_service::CredentialRefreshError>
    {
        assert_eq!(refresh_token, "refresh-v1");
        RefreshedCredentials::new(
            "access-v2",
            Some("refresh-v2".to_owned()),
            Utc::now() + chrono::Duration::hours(1),
        )
        .map_err(|_| werrss::application::auth_service::CredentialRefreshError::InvalidResponse)
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn auth_refresh_round_trips_through_postgres_without_plaintext_storage(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let account_id = account_id();
    let service = AuthService::new(
        AuthServiceDependencies {
            accounts: repository.clone(),
            leases: PostgresAccountLeaseRepository::new(pool.clone()),
            refresher: TestRefresher,
            cipher: RingCredentialCipher::new(&SecretString::new("integration-key".into()))
                .expect("cipher should accept a non-empty key"),
        },
        AuthServiceConfig::new(
            chrono::Duration::seconds(30),
            chrono::Duration::seconds(30),
            chrono::Duration::seconds(5),
        )
        .expect("test auth policy should be valid"),
    );
    service
        .provision(CredentialProvision {
            account_id,
            display_name: "primary".to_owned(),
            credentials: WeReadCredentials::new("access-v1", "refresh-v1", at(110), at(100))
                .expect("credentials should be valid"),
        })
        .await
        .expect("provisioning should commit");

    let outcome = service
        .refresh_if_needed(account_id, "integration-worker")
        .await
        .expect("expired credentials should refresh");
    let AuthRefreshOutcome::Refreshed(account) = outcome else {
        panic!("expected refresh outcome");
    };
    assert_eq!(account.credential_version(), 2);
    let raw: Vec<u8> = sqlx::query_scalar(
        "SELECT credentials_ciphertext FROM weread_accounts WHERE account_id = $1",
    )
    .bind(account_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("refreshed ciphertext should be stored");
    assert!(!raw
        .windows("access-v2".len())
        .any(|window| window == b"access-v2"));
    assert!(!raw
        .windows("refresh-v2".len())
        .any(|window| window == b"refresh-v2"));
    let reloaded = repository
        .find(account_id)
        .await
        .expect("reloading account should work")
        .expect("account should exist");
    assert!(reloaded
        .account()
        .access_expires_at()
        .is_some_and(|expires| expires > Utc::now() + chrono::Duration::minutes(30)));
}

async fn seed_placeholder(pool: &PgPool, account_id: WeReadAccountId) {
    sqlx::query(
        "INSERT INTO weread_accounts (account_id, display_name, credentials_ciphertext, access_expires_at, setup_required, disabled) VALUES ($1, $2, NULL, NULL, TRUE, FALSE)",
    )
    .bind(account_id.as_uuid())
    .bind("Imported account")
    .execute(pool)
    .await
    .expect("setup-required account should be seeded");
}

async fn seed_disabled_placeholder(pool: &PgPool, account_id: WeReadAccountId) {
    sqlx::query("INSERT INTO weread_accounts (account_id, display_name, credentials_ciphertext, access_expires_at, setup_required, disabled) VALUES ($1, $2, NULL, NULL, TRUE, TRUE)")
        .bind(account_id.as_uuid())
        .bind("Imported account")
        .execute(pool)
        .await
        .expect("disabled setup-required account should be seeded");
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct SourceScheduleState {
    scheduling_gate: String,
    next_fetch_at: chrono::DateTime<Utc>,
    failure_cooldown_until: Option<chrono::DateTime<Utc>>,
    schedule_reserved_until: Option<chrono::DateTime<Utc>>,
}

async fn seed_bound_source(pool: &PgPool, account_id: WeReadAccountId, gate: &str) -> Uuid {
    let source_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO sources (id, book_id, display_name, account_id, scheduling_gate, next_fetch_at, failure_cooldown_until, schedule_reserved_until) VALUES ($1, $2, $3, $4, $5, clock_timestamp() - interval '1 hour', clock_timestamp() + interval '1 hour', clock_timestamp() + interval '1 hour')",
    )
    .bind(source_id)
    .bind(format!("book-{}", source_id))
    .bind("Bound source")
    .bind(account_id.as_uuid())
    .bind(gate)
    .execute(pool)
    .await
    .expect("bound source should be seeded");
    source_id
}

async fn enroll_placeholder(
    pool: &PgPool,
    account_id: WeReadAccountId,
) -> Result<
    werrss::persistence::repositories::credential_repository::CredentialRecord,
    CredentialRepositoryError,
> {
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let lease = leases
        .acquire(account_id, "placeholder-test", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("test should own the account lease");
    let repository = PostgresCredentialRepository::new(pool.clone());
    let result = repository
        .enroll_placeholder(
            account_id,
            "Enrolled account",
            b"encrypted-payload",
            at(7_200),
            "placeholder-test",
            lease.token(),
        )
        .await;
    leases
        .release(account_id, "placeholder-test", lease.token())
        .await
        .expect("test lease should be released");
    result
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn setup_required_rows_are_visible_to_admin_but_never_selected_at_runtime(pool: PgPool) {
    let account_id = account_id();
    seed_placeholder(&pool, account_id).await;
    let repository = PostgresCredentialRepository::new(pool.clone());

    let record = repository
        .find(account_id)
        .await
        .expect("placeholder lookup should succeed")
        .expect("placeholder should be visible");
    assert!(record.account().setup_required());
    assert_eq!(record.account().access_expires_at(), None);
    assert_eq!(record.ciphertext(), None);
    assert_eq!(repository.list_all().await.unwrap().len(), 1);
    assert!(repository.list().await.unwrap().is_empty());

    let selector = CredentialRepositoryAccountSelector::new(repository);
    assert_eq!(
        selector.select_account(Some(account_id)).await.unwrap(),
        None
    );
    assert_eq!(selector.select_account(None).await.unwrap(), None);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn enrolling_enabled_placeholder_resumes_only_authentication_gated_sources(pool: PgPool) {
    let account_id = account_id();
    seed_placeholder(&pool, account_id).await;
    let auth_source = seed_bound_source(&pool, account_id, "authentication_required").await;
    let risk_source = seed_bound_source(&pool, account_id, "risk_controlled").await;

    let enrolled = enroll_placeholder(&pool, account_id)
        .await
        .expect("a valid live lease should enroll the placeholder");
    assert!(!enrolled.account().setup_required());
    assert_eq!(enrolled.account().access_expires_at(), Some(at(7_200)));
    assert_eq!(enrolled.ciphertext(), Some(b"encrypted-payload".as_slice()));
    assert!(!enrolled.account().disabled());

    let auth_schedule: SourceScheduleState = sqlx::query_as("SELECT scheduling_gate, next_fetch_at, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1")
        .bind(auth_source)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(auth_schedule.scheduling_gate, "ready");
    assert!(auth_schedule.next_fetch_at <= Utc::now());
    assert_eq!(auth_schedule.failure_cooldown_until, None);
    assert_eq!(auth_schedule.schedule_reserved_until, None);

    let risk_schedule: SourceScheduleState = sqlx::query_as("SELECT scheduling_gate, next_fetch_at, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1")
        .bind(risk_source)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(risk_schedule.scheduling_gate, "risk_controlled");
    assert!(risk_schedule.failure_cooldown_until.is_some());
    assert!(risk_schedule.schedule_reserved_until.is_some());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn enrolling_disabled_placeholder_preserves_preference_and_authentication_gate(pool: PgPool) {
    let account_id = account_id();
    seed_disabled_placeholder(&pool, account_id).await;
    let source_id = seed_bound_source(&pool, account_id, "authentication_required").await;

    let enrolled = enroll_placeholder(&pool, account_id)
        .await
        .expect("a valid live lease should enroll the placeholder");
    assert!(!enrolled.account().setup_required());
    assert!(enrolled.account().disabled());
    let (gate, cooldown, reservation): (
        String,
        Option<chrono::DateTime<Utc>>,
        Option<chrono::DateTime<Utc>>,
    ) = sqlx::query_as("SELECT scheduling_gate, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(gate, "authentication_required");
    assert!(cooldown.is_some());
    assert!(reservation.is_some());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn placeholder_enrollment_rolls_back_if_bound_source_recovery_fails(pool: PgPool) {
    let account_id = account_id();
    seed_placeholder(&pool, account_id).await;
    let source_id = seed_bound_source(&pool, account_id, "authentication_required").await;
    sqlx::query(
        "CREATE FUNCTION reject_auth_source_recovery() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.scheduling_gate = 'authentication_required' AND NEW.scheduling_gate = 'ready' THEN RAISE EXCEPTION 'test source recovery failure'; END IF; RETURN NEW; END; $$",
    )
    .execute(&pool)
    .await
    .expect("failure trigger should be installed");
    sqlx::query("CREATE TRIGGER reject_auth_source_recovery BEFORE UPDATE ON sources FOR EACH ROW EXECUTE FUNCTION reject_auth_source_recovery()")
        .execute(&pool)
        .await
        .expect("failure trigger should be installed");

    assert!(enroll_placeholder(&pool, account_id).await.is_err());
    let repository = PostgresCredentialRepository::new(pool.clone());
    let account = repository
        .find(account_id)
        .await
        .unwrap()
        .expect("placeholder should remain present after rollback");
    assert!(account.account().setup_required());
    assert_eq!(account.ciphertext(), None);
    let gate: String = sqlx::query_scalar("SELECT scheduling_gate FROM sources WHERE id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(gate, "authentication_required");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn placeholder_enrollment_cannot_overwrite_an_enrolled_account(pool: PgPool) {
    let account_id = account_id();
    let repository = PostgresCredentialRepository::new(pool.clone());
    let original = repository
        .insert(account_id, "Original", b"original-ciphertext", at(3_600))
        .await
        .expect("enrolled account should be inserted");
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let lease = leases
        .acquire(account_id, "placeholder-test", chrono::Duration::minutes(5))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        repository
            .enroll_placeholder(
                account_id,
                "Overwritten",
                b"replacement-ciphertext",
                at(7_200),
                "placeholder-test",
                lease.token(),
            )
            .await,
        Err(CredentialRepositoryError::Conflict { account_id: conflict }) if conflict == account_id
    ));
    let reloaded = repository.find(account_id).await.unwrap().unwrap();
    assert_eq!(reloaded, original);
    assert_eq!(
        reloaded.ciphertext(),
        Some(b"original-ciphertext".as_slice())
    );
    leases
        .release(account_id, "placeholder-test", lease.token())
        .await
        .unwrap();
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn placeholder_enrollment_rejects_a_stale_account_lease(pool: PgPool) {
    let account_id = account_id();
    seed_placeholder(&pool, account_id).await;
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let stale = leases
        .acquire(account_id, "old-enrollment", chrono::Duration::minutes(5))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE account_leases SET lease_until = clock_timestamp() - interval '1 second' WHERE account_id = $1")
        .bind(account_id.as_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let current = leases
        .acquire(account_id, "new-enrollment", chrono::Duration::minutes(5))
        .await
        .unwrap()
        .unwrap();
    let repository = PostgresCredentialRepository::new(pool.clone());

    assert!(matches!(
        repository
            .enroll_placeholder(
                account_id,
                "Stale enrollment",
                b"stale-ciphertext",
                at(7_200),
                "old-enrollment",
                stale.token(),
            )
            .await,
        Err(CredentialRepositoryError::Conflict { account_id: conflict }) if conflict == account_id
    ));
    let record = repository.find(account_id).await.unwrap().unwrap();
    assert!(record.account().setup_required());
    assert_eq!(record.ciphertext(), None);
    leases
        .release(account_id, "new-enrollment", current.token())
        .await
        .unwrap();
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn disabling_account_does_not_change_bound_source_scheduling(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let account_id = account_id();
    repository
        .insert(account_id, "Enabled", b"ciphertext", at(3_600))
        .await
        .unwrap();
    let source_id = seed_bound_source(&pool, account_id, "authentication_required").await;
    let before: SourceScheduleState = sqlx::query_as(
        "SELECT scheduling_gate, next_fetch_at, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1",
    )
    .bind(source_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    repository.set_disabled(account_id, true).await.unwrap();

    let after: SourceScheduleState = sqlx::query_as(
        "SELECT scheduling_gate, next_fetch_at, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1",
    )
    .bind(source_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after, before);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn reenabling_account_resumes_only_authentication_gated_bound_sources(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let account_id = account_id();
    repository
        .insert(account_id, "Disabled", b"ciphertext", at(3_600))
        .await
        .unwrap();
    repository.set_disabled(account_id, true).await.unwrap();
    repository.set_disabled(account_id, true).await.unwrap();
    let auth_source = seed_bound_source(&pool, account_id, "authentication_required").await;
    let risk_source = seed_bound_source(&pool, account_id, "risk_controlled").await;

    let enabled = repository
        .set_disabled(account_id, false)
        .await
        .expect("account should be enabled");
    assert!(!enabled.account().disabled());
    let auth_schedule: SourceScheduleState = sqlx::query_as("SELECT scheduling_gate, next_fetch_at, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1")
        .bind(auth_source)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(auth_schedule.scheduling_gate, "ready");
    assert!(auth_schedule.next_fetch_at <= Utc::now());
    assert_eq!(auth_schedule.failure_cooldown_until, None);
    assert_eq!(auth_schedule.schedule_reserved_until, None);
    let risk_schedule: SourceScheduleState = sqlx::query_as("SELECT scheduling_gate, next_fetch_at, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1")
        .bind(risk_source)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(risk_schedule.scheduling_gate, "risk_controlled");
    assert!(risk_schedule.failure_cooldown_until.is_some());
    assert!(risk_schedule.schedule_reserved_until.is_some());
}
