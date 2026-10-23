//! Durable storage for encrypted WeRead account credentials.
//!
//! This repository deliberately accepts only ciphertext. Encryption and
//! decryption belong to the application authentication boundary, while this
//! module owns account metadata, optimistic credential versions, and SQL.
//! Plain tokens never appear in rows, repository errors, or debug output.

use std::{collections::HashMap, fmt, sync::Arc};

use crate::domain::credentials::{AccountLeaseToken, WeReadAccount, WeReadAccountId};
use crate::persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgPool, Row};
use tokio::sync::Mutex;

/// Encrypted account material and non-secret account metadata.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialRecord {
    account: WeReadAccount,
    ciphertext: Option<Vec<u8>>,
}

/// A fenced replacement request for one encrypted account record.
///
/// The lease proof is checked in the same PostgreSQL `UPDATE` that advances
/// the credential version. Optimistic versioning alone is not sufficient:
/// after a lease takeover, a stale worker may still have observed the latest
/// version and would otherwise overwrite the new owner's credentials.
#[derive(Debug, Clone)]
pub struct CredentialReplacement {
    /// Account whose credentials are being replaced.
    pub account_id: WeReadAccountId,
    /// Updated operator-facing account label.
    pub display_name: String,
    /// Version read before the refresh exchange.
    pub expected_version: i64,
    /// New encrypted credential payload.
    pub ciphertext: Vec<u8>,
    /// New access-token expiry.
    pub access_expires_at: DateTime<Utc>,
    /// Owner of the live account lease.
    pub lease_owner: String,
    /// Fencing token of the live account lease.
    pub lease_token: AccountLeaseToken,
}

impl fmt::Debug for CredentialRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialRecord")
            .field("account", &self.account)
            .field("ciphertext", &"<encrypted>")
            .finish()
    }
}

impl CredentialRecord {
    /// Creates a repository record from trusted account metadata and ciphertext.
    pub(crate) fn from_parts(account: WeReadAccount, ciphertext: Option<Vec<u8>>) -> Self {
        Self {
            account,
            ciphertext,
        }
    }

    /// Returns non-secret account metadata.
    pub fn account(&self) -> &WeReadAccount {
        &self.account
    }

    /// Returns ciphertext for decryption by the authentication boundary.
    pub fn ciphertext(&self) -> Option<&[u8]> {
        self.ciphertext.as_deref()
    }
}

/// Errors raised by account credential persistence.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CredentialRepositoryError {
    /// The requested account does not exist.
    #[error("credential account {account_id} was not found")]
    NotFound { account_id: WeReadAccountId },
    /// The requested account was concurrently changed or is absent.
    #[error("credential version conflict for account {account_id}")]
    Conflict { account_id: WeReadAccountId },
    /// The persistence backend failed without exposing query contents.
    #[error("credential repository backend failure: {0}")]
    Backend(String),
    /// Account metadata violates the repository contract.
    #[error("invalid account metadata: {0}")]
    Invalid(String),
}

/// Storage-neutral account credential operations.
#[async_trait]
pub trait CredentialRepository: Send + Sync {
    /// Samples the authoritative persistence clock.
    async fn database_now(&self) -> Result<DateTime<Utc>, CredentialRepositoryError>;

    /// Lists enabled accounts in deterministic order for runtime account
    /// selection. Implementations must not return disabled accounts.
    async fn list(&self) -> Result<Vec<CredentialRecord>, CredentialRepositoryError>;

    /// Lists all enrolled accounts in deterministic order for administrative
    /// status views. Implementations must not decrypt or omit disabled records.
    async fn list_all(&self) -> Result<Vec<CredentialRecord>, CredentialRepositoryError> {
        self.list().await
    }

    /// Loads one account and its encrypted credential payload.
    async fn find(
        &self,
        account_id: WeReadAccountId,
    ) -> Result<Option<CredentialRecord>, CredentialRepositoryError>;

    /// Inserts a newly provisioned account with credential version one.
    async fn insert(
        &self,
        account_id: WeReadAccountId,
        display_name: &str,
        ciphertext: &[u8],
        access_expires_at: DateTime<Utc>,
    ) -> Result<CredentialRecord, CredentialRepositoryError>;

    /// Replaces ciphertext only at the expected version.
    async fn replace(
        &self,
        replacement: CredentialReplacement,
    ) -> Result<CredentialRecord, CredentialRepositoryError>;

    /// Enrolls credentials into a setup-required account under its live lease.
    async fn enroll_placeholder(
        &self,
        account_id: WeReadAccountId,
        display_name: &str,
        ciphertext: &[u8],
        access_expires_at: DateTime<Utc>,
        lease_owner: &str,
        lease_token: AccountLeaseToken,
    ) -> Result<CredentialRecord, CredentialRepositoryError>;

    /// Enables or disables an account, resuming authentication-gated sources
    /// atomically when an enrolled account is re-enabled.
    async fn set_disabled(
        &self,
        account_id: WeReadAccountId,
        disabled: bool,
    ) -> Result<CredentialRecord, CredentialRepositoryError>;

    /// Permanently removes an enrolled account and its lease row.
    async fn delete(&self, account_id: WeReadAccountId) -> Result<(), CredentialRepositoryError>;
}

/// PostgreSQL-backed credential repository.
#[derive(Clone)]
pub struct PostgresCredentialRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresCredentialRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresCredentialRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresCredentialRepository {
    /// Creates a repository backed by the shared PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CredentialRepository for PostgresCredentialRepository {
    async fn database_now(&self) -> Result<DateTime<Utc>, CredentialRepositoryError> {
        sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await
            .map_err(storage_error)
    }

    async fn list(&self) -> Result<Vec<CredentialRecord>, CredentialRepositoryError> {
        sqlx::query(
            "SELECT account_id, display_name, credentials_ciphertext, access_expires_at, credential_version, disabled, setup_required FROM weread_accounts WHERE NOT disabled AND NOT setup_required ORDER BY access_expires_at ASC, account_id ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_record)
            .collect()
    }

    async fn list_all(&self) -> Result<Vec<CredentialRecord>, CredentialRepositoryError> {
        sqlx::query(
            "SELECT account_id, display_name, credentials_ciphertext, access_expires_at, credential_version, disabled, setup_required FROM weread_accounts ORDER BY access_expires_at ASC NULLS LAST, account_id ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?
        .into_iter()
        .map(decode_record)
        .collect()
    }

    async fn find(
        &self,
        account_id: WeReadAccountId,
    ) -> Result<Option<CredentialRecord>, CredentialRepositoryError> {
        let row = sqlx::query(
            "SELECT account_id, display_name, credentials_ciphertext, access_expires_at, credential_version, disabled, setup_required FROM weread_accounts WHERE account_id = $1",
        )
        .bind(account_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_record).transpose()
    }

    async fn insert(
        &self,
        account_id: WeReadAccountId,
        display_name: &str,
        ciphertext: &[u8],
        access_expires_at: DateTime<Utc>,
    ) -> Result<CredentialRecord, CredentialRepositoryError> {
        validate_inputs(account_id, display_name, ciphertext, access_expires_at)?;
        let row = sqlx::query(
            "INSERT INTO weread_accounts (account_id, display_name, credentials_ciphertext, access_expires_at) VALUES ($1, $2, $3, $4) RETURNING account_id, display_name, credentials_ciphertext, access_expires_at, credential_version, disabled, setup_required",
        )
        .bind(account_id.as_uuid())
        .bind(display_name.trim())
        .bind(ciphertext)
        .bind(access_expires_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| match &error {
            sqlx::Error::Database(database_error)
                if database_error.code().as_deref() == Some("23505") => {
                    CredentialRepositoryError::Conflict { account_id }
                }
            _ => storage_error(error),
        })?;
        decode_record(row)
    }

    async fn replace(
        &self,
        replacement: CredentialReplacement,
    ) -> Result<CredentialRecord, CredentialRepositoryError> {
        validate_inputs(
            replacement.account_id,
            &replacement.display_name,
            &replacement.ciphertext,
            replacement.access_expires_at,
        )?;
        if replacement.expected_version <= 0
            || replacement.lease_owner.trim().is_empty()
            || replacement.lease_token.as_uuid().is_nil()
        {
            return Err(CredentialRepositoryError::Conflict {
                account_id: replacement.account_id,
            });
        }
        let row = sqlx::query(
            r#"
            WITH live_lease AS MATERIALIZED (
                SELECT lease.account_id
                FROM account_leases AS lease
                WHERE lease.account_id = $1
                  AND lease.lease_owner = $6
                  AND lease.lease_token = $7
                  AND lease.lease_until > clock_timestamp()
                FOR UPDATE
            )
            UPDATE weread_accounts AS account
            SET display_name = $3,
                credentials_ciphertext = $4,
                access_expires_at = $5,
                credential_version = credential_version + 1,
                updated_at = clock_timestamp()
            WHERE account.account_id = $1
              AND account.credential_version = $2
              AND NOT account.setup_required
              AND EXISTS (
                  SELECT 1
                  FROM live_lease
                  WHERE live_lease.account_id = account.account_id
              )
            RETURNING account.account_id, account.display_name,
                      account.credentials_ciphertext, account.access_expires_at,
                      account.credential_version, account.disabled, account.setup_required
            "#,
        )
        .bind(replacement.account_id.as_uuid())
        .bind(replacement.expected_version)
        .bind(replacement.display_name.trim())
        .bind(&replacement.ciphertext)
        .bind(replacement.access_expires_at)
        .bind(replacement.lease_owner)
        .bind(replacement.lease_token.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_record)
            .transpose()?
            .ok_or(CredentialRepositoryError::Conflict {
                account_id: replacement.account_id,
            })
    }

    async fn enroll_placeholder(
        &self,
        account_id: WeReadAccountId,
        display_name: &str,
        ciphertext: &[u8],
        access_expires_at: DateTime<Utc>,
        lease_owner: &str,
        lease_token: AccountLeaseToken,
    ) -> Result<CredentialRecord, CredentialRepositoryError> {
        validate_inputs(account_id, display_name, ciphertext, access_expires_at)?;
        if lease_owner.trim().is_empty() || lease_token.as_uuid().is_nil() {
            return Err(CredentialRepositoryError::Conflict { account_id });
        }
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            r#"
            WITH live_lease AS MATERIALIZED (
                SELECT account_id
                FROM account_leases
                WHERE account_id = $1 AND lease_owner = $5 AND lease_token = $6
                  AND lease_until > clock_timestamp()
                FOR UPDATE
            )
            UPDATE weread_accounts AS account
            SET display_name = $2, credentials_ciphertext = $3,
                access_expires_at = $4, setup_required = FALSE,
                credential_version = credential_version + 1,
                updated_at = clock_timestamp()
            WHERE account.account_id = $1 AND account.setup_required
              AND account.credentials_ciphertext IS NULL AND account.access_expires_at IS NULL
              AND EXISTS (SELECT 1 FROM live_lease WHERE live_lease.account_id = account.account_id)
            RETURNING account.account_id, account.display_name, account.credentials_ciphertext,
                      account.access_expires_at, account.credential_version, account.disabled,
                      account.setup_required
            "#,
        )
        .bind(account_id.as_uuid())
        .bind(display_name.trim())
        .bind(ciphertext)
        .bind(access_expires_at)
        .bind(lease_owner)
        .bind(lease_token.as_uuid())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?
        .ok_or(CredentialRepositoryError::Conflict { account_id })?;
        let record = decode_record(row)?;
        if !record.account().disabled() {
            resume_authentication_gated_sources(&mut transaction, account_id).await?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn set_disabled(
        &self,
        account_id: WeReadAccountId,
        disabled: bool,
    ) -> Result<CredentialRecord, CredentialRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let current: Option<(bool, bool)> = sqlx::query_as(
            "SELECT disabled, setup_required FROM weread_accounts WHERE account_id = $1 FOR UPDATE",
        )
        .bind(account_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let Some((was_disabled, setup_required)) = current else {
            return Err(CredentialRepositoryError::NotFound { account_id });
        };
        let row = sqlx::query(
            "UPDATE weread_accounts SET disabled = $2, updated_at = clock_timestamp() WHERE account_id = $1 RETURNING account_id, display_name, credentials_ciphertext, access_expires_at, credential_version, disabled, setup_required",
        )
        .bind(account_id.as_uuid())
        .bind(disabled)
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let record = decode_record(row)?;
        if was_disabled && !disabled && !setup_required {
            resume_authentication_gated_sources(&mut transaction, account_id).await?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn delete(&self, account_id: WeReadAccountId) -> Result<(), CredentialRepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let result = sqlx::query("DELETE FROM weread_accounts WHERE account_id = $1")
            .bind(account_id.as_uuid())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        if result.rows_affected() == 0 {
            return Err(CredentialRepositoryError::NotFound { account_id });
        }
        sqlx::query("DELETE FROM account_leases WHERE account_id = $1")
            .bind(account_id.as_uuid())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)
    }
}

async fn resume_authentication_gated_sources(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: WeReadAccountId,
) -> Result<(), CredentialRepositoryError> {
    sqlx::query(
        "UPDATE sources SET scheduling_gate = 'ready', failure_cooldown_until = NULL, schedule_reserved_until = NULL, next_fetch_at = clock_timestamp(), updated_at = clock_timestamp() WHERE account_id = $1 AND scheduling_gate = 'authentication_required'",
    )
    .bind(account_id.as_uuid())
    .execute(&mut **transaction)
    .await
    .map_err(storage_error)?;
    Ok(())
}

fn decode_record(row: PgRow) -> Result<CredentialRecord, CredentialRepositoryError> {
    let account_id = WeReadAccountId::from_uuid(row.try_get("account_id").map_err(storage_error)?);
    if account_id.as_uuid().is_nil() {
        return Err(CredentialRepositoryError::Invalid(
            "account id must not be nil".to_owned(),
        ));
    }
    let display_name: String = row.try_get("display_name").map_err(storage_error)?;
    let ciphertext: Option<Vec<u8>> = row
        .try_get("credentials_ciphertext")
        .map_err(storage_error)?;
    let access_expires_at: Option<DateTime<Utc>> =
        row.try_get("access_expires_at").map_err(storage_error)?;
    let credential_version = row.try_get("credential_version").map_err(storage_error)?;
    let disabled = row.try_get("disabled").map_err(storage_error)?;
    let setup_required: bool = row.try_get("setup_required").map_err(storage_error)?;
    let valid_state = if setup_required {
        ciphertext.is_none() && access_expires_at.is_none()
    } else {
        ciphertext.as_ref().is_some_and(|value| !value.is_empty()) && access_expires_at.is_some()
    };
    if credential_version <= 0 || display_name.trim().is_empty() || !valid_state {
        return Err(CredentialRepositoryError::Invalid(
            "account row contains invalid metadata".to_owned(),
        ));
    }
    Ok(CredentialRecord::from_parts(
        WeReadAccount::from_parts(
            account_id,
            display_name,
            credential_version,
            access_expires_at,
            disabled,
            setup_required,
        ),
        ciphertext,
    ))
}

fn validate_inputs(
    account_id: WeReadAccountId,
    display_name: &str,
    ciphertext: &[u8],
    access_expires_at: DateTime<Utc>,
) -> Result<(), CredentialRepositoryError> {
    if account_id.as_uuid().is_nil() {
        return Err(CredentialRepositoryError::Invalid(
            "account id must not be nil".to_owned(),
        ));
    }
    if display_name.trim().is_empty() {
        return Err(CredentialRepositoryError::Invalid(
            "display name must not be empty".to_owned(),
        ));
    }
    if ciphertext.is_empty() {
        return Err(CredentialRepositoryError::Invalid(
            "credential ciphertext must not be empty".to_owned(),
        ));
    }
    if access_expires_at <= DateTime::<Utc>::UNIX_EPOCH {
        return Err(CredentialRepositoryError::Invalid(
            "access expiry must be after the Unix epoch".to_owned(),
        ));
    }
    Ok(())
}

fn storage_error(error: impl fmt::Display) -> CredentialRepositoryError {
    CredentialRepositoryError::Backend(error.to_string())
}

#[derive(Clone)]
struct MemoryState {
    now: DateTime<Utc>,
    records: HashMap<WeReadAccountId, CredentialRecord>,
}

/// In-memory credential repository for deterministic application tests.
#[derive(Clone)]
pub struct MemoryCredentialRepository {
    state: Arc<Mutex<MemoryState>>,
    lease_store: Option<MemoryAccountLeaseRepository>,
}

impl fmt::Debug for MemoryCredentialRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryCredentialRepository")
            .finish_non_exhaustive()
    }
}

impl Default for MemoryCredentialRepository {
    fn default() -> Self {
        Self::new(Utc::now())
    }
}

impl MemoryCredentialRepository {
    /// Creates a deterministic repository with the supplied clock value.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState {
                now,
                records: HashMap::new(),
            })),
            lease_store: None,
        }
    }

    /// Attaches the in-memory account lease store used by the service under
    /// test so replacements receive the same fencing guarantees as SQL.
    pub fn with_lease_store(mut self, lease_store: MemoryAccountLeaseRepository) -> Self {
        self.lease_store = Some(lease_store);
        self
    }

    /// Advances the deterministic clock used by authentication tests.
    pub async fn set_now(&self, now: DateTime<Utc>) {
        self.state.lock().await.now = now;
    }
}

#[async_trait]
impl CredentialRepository for MemoryCredentialRepository {
    async fn database_now(&self) -> Result<DateTime<Utc>, CredentialRepositoryError> {
        Ok(self.state.lock().await.now)
    }

    async fn list(&self) -> Result<Vec<CredentialRecord>, CredentialRepositoryError> {
        let mut records: Vec<_> = self
            .state
            .lock()
            .await
            .records
            .values()
            .filter(|record| {
                !record.account().disabled()
                    && !record.account().setup_required()
                    && record.account().access_expires_at().is_some()
            })
            .cloned()
            .collect();
        records.sort_by_key(|record| {
            (
                record
                    .account()
                    .access_expires_at()
                    .unwrap_or(DateTime::<Utc>::MAX_UTC),
                record.account().account_id().as_uuid(),
            )
        });
        Ok(records)
    }

    async fn list_all(&self) -> Result<Vec<CredentialRecord>, CredentialRepositoryError> {
        let mut records: Vec<_> = self.state.lock().await.records.values().cloned().collect();
        records.sort_by_key(|record| {
            (
                record
                    .account()
                    .access_expires_at()
                    .unwrap_or(DateTime::<Utc>::MAX_UTC),
                record.account().account_id().as_uuid(),
            )
        });
        Ok(records)
    }

    async fn find(
        &self,
        account_id: WeReadAccountId,
    ) -> Result<Option<CredentialRecord>, CredentialRepositoryError> {
        Ok(self.state.lock().await.records.get(&account_id).cloned())
    }

    async fn insert(
        &self,
        account_id: WeReadAccountId,
        display_name: &str,
        ciphertext: &[u8],
        access_expires_at: DateTime<Utc>,
    ) -> Result<CredentialRecord, CredentialRepositoryError> {
        validate_inputs(account_id, display_name, ciphertext, access_expires_at)?;
        let mut state = self.state.lock().await;
        if state.records.contains_key(&account_id) {
            return Err(CredentialRepositoryError::Conflict { account_id });
        }
        let record = CredentialRecord::from_parts(
            WeReadAccount::from_parts(
                account_id,
                display_name.trim().to_owned(),
                1,
                Some(access_expires_at),
                false,
                false,
            ),
            Some(ciphertext.to_vec()),
        );
        state.records.insert(account_id, record.clone());
        Ok(record)
    }

    async fn replace(
        &self,
        replacement: CredentialReplacement,
    ) -> Result<CredentialRecord, CredentialRepositoryError> {
        validate_inputs(
            replacement.account_id,
            &replacement.display_name,
            &replacement.ciphertext,
            replacement.access_expires_at,
        )?;
        if replacement.expected_version <= 0
            || replacement.lease_owner.trim().is_empty()
            || replacement.lease_token.as_uuid().is_nil()
        {
            return Err(CredentialRepositoryError::Conflict {
                account_id: replacement.account_id,
            });
        }
        if let Some(lease_store) = &self.lease_store {
            if !lease_store
                .is_held(
                    replacement.account_id,
                    &replacement.lease_owner,
                    replacement.lease_token,
                )
                .await
            {
                return Err(CredentialRepositoryError::Conflict {
                    account_id: replacement.account_id,
                });
            }
        }
        let mut state = self.state.lock().await;
        let current = state
            .records
            .get(&replacement.account_id)
            .filter(|record| record.account().credential_version() == replacement.expected_version)
            .ok_or(CredentialRepositoryError::Conflict {
                account_id: replacement.account_id,
            })?;
        let account = WeReadAccount::from_parts(
            replacement.account_id,
            replacement.display_name.trim().to_owned(),
            replacement.expected_version + 1,
            Some(replacement.access_expires_at),
            current.account().disabled(),
            false,
        );
        let record = CredentialRecord::from_parts(account, Some(replacement.ciphertext));
        state.records.insert(replacement.account_id, record.clone());
        Ok(record)
    }

    async fn enroll_placeholder(
        &self,
        account_id: WeReadAccountId,
        display_name: &str,
        ciphertext: &[u8],
        access_expires_at: DateTime<Utc>,
        lease_owner: &str,
        lease_token: AccountLeaseToken,
    ) -> Result<CredentialRecord, CredentialRepositoryError> {
        validate_inputs(account_id, display_name, ciphertext, access_expires_at)?;
        if lease_owner.trim().is_empty() || lease_token.as_uuid().is_nil() {
            return Err(CredentialRepositoryError::Conflict { account_id });
        }
        let Some(lease_store) = &self.lease_store else {
            return Err(CredentialRepositoryError::Conflict { account_id });
        };
        if !lease_store
            .is_held(account_id, lease_owner, lease_token)
            .await
        {
            return Err(CredentialRepositoryError::Conflict { account_id });
        }
        let mut state = self.state.lock().await;
        let current = state
            .records
            .get(&account_id)
            .filter(|record| record.account().setup_required())
            .ok_or(CredentialRepositoryError::Conflict { account_id })?;
        let account = WeReadAccount::from_parts(
            account_id,
            display_name.trim().to_owned(),
            current.account().credential_version() + 1,
            Some(access_expires_at),
            current.account().disabled(),
            false,
        );
        let record = CredentialRecord::from_parts(account, Some(ciphertext.to_vec()));
        state.records.insert(account_id, record.clone());
        Ok(record)
    }

    async fn set_disabled(
        &self,
        account_id: WeReadAccountId,
        disabled: bool,
    ) -> Result<CredentialRecord, CredentialRepositoryError> {
        let mut state = self.state.lock().await;
        let current = state
            .records
            .get(&account_id)
            .ok_or(CredentialRepositoryError::NotFound { account_id })?;
        let account = WeReadAccount::from_parts(
            account_id,
            current.account().display_name().to_owned(),
            current.account().credential_version(),
            current.account().access_expires_at(),
            disabled,
            current.account().setup_required(),
        );
        let record =
            CredentialRecord::from_parts(account, current.ciphertext().map(<[u8]>::to_vec));
        state.records.insert(account_id, record.clone());
        Ok(record)
    }

    async fn delete(&self, account_id: WeReadAccountId) -> Result<(), CredentialRepositoryError> {
        let mut state = self.state.lock().await;
        state
            .records
            .remove(&account_id)
            .map(|_| ())
            .ok_or(CredentialRepositoryError::NotFound { account_id })
    }
}
