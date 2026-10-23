//! PostgreSQL configuration transfer persistence and atomic merge.
//!
//! This repository provides snapshot export, read-only preview, and atomic apply
//! operations for moving configuration bundles between instances.
//!
//! Responsibilities:
//! - Export selected configuration sections (`sources`, `weread_accounts`) in a
//!   consistent snapshot while strictly enforcing section limits (<= 1,000 items)
//!   and payload limits (<= 2 MiB).
//! - Non-mutating preview analyzing ID matching, natural-key conflicts, placeholder
//!   creation, and source-to-account binding preservation/clearing.
//! - Single-transaction atomic apply that creates placeholders for new accounts,
//!   updates only portable metadata for existing accounts without touching credentials,
//!   resumes authentication-gated sources when an enrolled disabled account becomes
//!   enabled, creates new sources with ready/due scheduler state, and preserves
//!   scheduling state on source updates while advancing feed revision only for
//!   feed-visible configuration changes.
//! - Atomic rollback: any conflict or validation error aborts the complete transaction
//!   leaving all tables completely unchanged.
//!
//! Non-responsibilities:
//! - Exporting or importing credentials, tokens, session cookies, ciphertext,
//!   encryption keys, articles, assets, sync runs, jobs, leases, or feed tokens.
//! - Network, browser, or QR login operations.

use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use chrono::Utc;
use sqlx::{PgPool, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::application::config_transfer::{
    ConfigBundle, ConfigExportSelection, ConfigTransferConflict, ConfigTransferValidationError,
    ImportPreview, ImportResult, PortableSource, PortableWeReadAccount, ValidatedConfigBundle,
    MAX_SECTION_RECORDS,
};

/// Errors returned by configuration transfer persistence operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigTransferRepositoryError {
    /// A stable-key or natural-key conflict prevented import application.
    #[error("configuration transfer conflict: {0}")]
    Conflict(ConfigTransferConflict),

    /// The bundle or export selection failed validation.
    #[error(transparent)]
    Validation(#[from] ConfigTransferValidationError),

    /// The underlying storage operation failed.
    #[error("configuration transfer storage error: {0}")]
    Storage(String),
}

impl From<sqlx::Error> for ConfigTransferRepositoryError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

/// Abstract contract for configuration transfer persistence.
#[async_trait::async_trait]
pub trait ConfigTransferRepository: Send + Sync {
    /// Reads a consistent snapshot and returns the selected configuration sections.
    async fn export(
        &self,
        selection: ConfigExportSelection,
    ) -> Result<ConfigBundle, ConfigTransferRepositoryError>;

    /// Analyzes an import bundle against local database state without mutations.
    async fn preview(
        &self,
        bundle: &ValidatedConfigBundle,
    ) -> Result<ImportPreview, ConfigTransferRepositoryError>;

    /// Atomically applies an import bundle in a single PostgreSQL transaction.
    async fn apply(
        &self,
        bundle: ValidatedConfigBundle,
    ) -> Result<ImportResult, ConfigTransferRepositoryError>;
}

/// PostgreSQL implementation of configuration transfer persistence.
#[derive(Clone)]
pub struct PostgresConfigTransferRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresConfigTransferRepository {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConfigTransferRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresConfigTransferRepository {
    /// Creates a new configuration transfer repository backed by the given connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Reads a consistent snapshot and returns the selected configuration sections.
    pub async fn export(
        &self,
        selection: ConfigExportSelection,
    ) -> Result<ConfigBundle, ConfigTransferRepositoryError> {
        <Self as ConfigTransferRepository>::export(self, selection).await
    }

    /// Analyzes an import bundle against local database state without mutations.
    pub async fn preview(
        &self,
        bundle: &ValidatedConfigBundle,
    ) -> Result<ImportPreview, ConfigTransferRepositoryError> {
        <Self as ConfigTransferRepository>::preview(self, bundle).await
    }

    /// Atomically applies an import bundle in a single PostgreSQL transaction.
    pub async fn apply(
        &self,
        bundle: ValidatedConfigBundle,
    ) -> Result<ImportResult, ConfigTransferRepositoryError> {
        <Self as ConfigTransferRepository>::apply(self, bundle).await
    }
}

#[async_trait::async_trait]
impl ConfigTransferRepository for PostgresConfigTransferRepository {
    async fn export(
        &self,
        selection: ConfigExportSelection,
    ) -> Result<ConfigBundle, ConfigTransferRepositoryError> {
        selection.validate()?;

        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;

        let sources = if selection.sources {
            let limit = (MAX_SECTION_RECORDS + 1) as i64;
            let rows = sqlx::query(
                "SELECT id, book_id, display_name, article_url, account_id, enabled, \
                        sync_interval_seconds, rss_item_limit, priority, max_attempts \
                 FROM sources \
                 ORDER BY display_name ASC, id ASC \
                 LIMIT $1",
            )
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage_error)?;

            if rows.len() > MAX_SECTION_RECORDS {
                return Err(ConfigTransferValidationError::SectionTooLarge {
                    section: "sources",
                    count: rows.len(),
                    limit: MAX_SECTION_RECORDS,
                }
                .into());
            }

            let mut portable_sources = Vec::with_capacity(rows.len());
            for row in rows {
                let id: Uuid = row.try_get("id").map_err(storage_error)?;
                let book_id: String = row.try_get("book_id").map_err(storage_error)?;
                let display_name: String = row.try_get("display_name").map_err(storage_error)?;
                let article_url: Option<String> =
                    row.try_get("article_url").map_err(storage_error)?;
                let account_id: Option<Uuid> = row.try_get("account_id").map_err(storage_error)?;
                let enabled: bool = row.try_get("enabled").map_err(storage_error)?;
                let sync_interval_seconds: i64 = row
                    .try_get("sync_interval_seconds")
                    .map_err(storage_error)?;
                let rss_item_limit_raw: i64 =
                    row.try_get("rss_item_limit").map_err(storage_error)?;
                let rss_item_limit = u32::try_from(rss_item_limit_raw).map_err(|_| {
                    ConfigTransferValidationError::InvalidNumericBound {
                        field: "rss_item_limit",
                        message: "exceeds u32 limit",
                    }
                })?;
                let priority: i32 = row.try_get("priority").map_err(storage_error)?;
                let max_attempts_raw: i64 = row.try_get("max_attempts").map_err(storage_error)?;
                let max_attempts = u32::try_from(max_attempts_raw).map_err(|_| {
                    ConfigTransferValidationError::InvalidNumericBound {
                        field: "max_attempts",
                        message: "exceeds u32 limit",
                    }
                })?;

                portable_sources.push(PortableSource {
                    id,
                    book_id,
                    display_name,
                    article_url,
                    account_id,
                    enabled,
                    sync_interval_seconds,
                    rss_item_limit,
                    priority,
                    max_attempts,
                });
            }

            Some(portable_sources)
        } else {
            None
        };

        let weread_accounts = if selection.weread_accounts {
            let limit = (MAX_SECTION_RECORDS + 1) as i64;
            let rows = sqlx::query(
                "SELECT account_id, display_name, disabled \
                 FROM weread_accounts \
                 ORDER BY display_name ASC, account_id ASC \
                 LIMIT $1",
            )
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(storage_error)?;

            if rows.len() > MAX_SECTION_RECORDS {
                return Err(ConfigTransferValidationError::SectionTooLarge {
                    section: "weread_accounts",
                    count: rows.len(),
                    limit: MAX_SECTION_RECORDS,
                }
                .into());
            }

            let mut portable_accounts = Vec::with_capacity(rows.len());
            for row in rows {
                let account_id: Uuid = row.try_get("account_id").map_err(storage_error)?;
                let display_name: String = row.try_get("display_name").map_err(storage_error)?;
                let disabled: bool = row.try_get("disabled").map_err(storage_error)?;

                portable_accounts.push(PortableWeReadAccount {
                    account_id,
                    display_name,
                    enabled: !disabled,
                });
            }

            Some(portable_accounts)
        } else {
            None
        };

        tx.commit().await.map_err(storage_error)?;

        let bundle = ConfigBundle::new(Utc::now(), sources, weread_accounts);
        bundle.serialize_limited()?;
        Ok(bundle)
    }

    async fn preview(
        &self,
        bundle: &ValidatedConfigBundle,
    ) -> Result<ImportPreview, ConfigTransferRepositoryError> {
        let raw = bundle.bundle();
        let mut preview = ImportPreview::default();

        let account_rows = sqlx::query("SELECT account_id FROM weread_accounts")
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;

        let mut known_account_ids: HashSet<Uuid> = account_rows
            .into_iter()
            .map(|r| r.try_get::<Uuid, _>("account_id"))
            .collect::<Result<_, _>>()
            .map_err(storage_error)?;

        if let Some(accounts) = &raw.weread_accounts {
            for account in accounts {
                if known_account_ids.contains(&account.account_id) {
                    preview.accounts_updated += 1;
                } else {
                    preview.accounts_created += 1;
                    preview.placeholders_created += 1;
                    known_account_ids.insert(account.account_id);
                }
            }
        }

        let source_rows = sqlx::query("SELECT id, book_id FROM sources")
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?;

        let mut existing_sources_by_id = HashSet::new();
        let mut existing_sources_by_book_id = HashMap::new();

        for r in source_rows {
            let id: Uuid = r.try_get("id").map_err(storage_error)?;
            let book_id: String = r.try_get("book_id").map_err(storage_error)?;
            existing_sources_by_id.insert(id);
            existing_sources_by_book_id.insert(book_id, id);
        }

        if let Some(sources) = &raw.sources {
            for source in sources {
                let id_exists = existing_sources_by_id.contains(&source.id);
                if id_exists {
                    if let Some(existing_source_id) =
                        existing_sources_by_book_id.get(&source.book_id)
                    {
                        if *existing_source_id != source.id {
                            preview
                                .conflicts
                                .push(ConfigTransferConflict::source_book_id(
                                    source.id,
                                    *existing_source_id,
                                    &source.book_id,
                                ));
                        }
                    }
                    preview.sources_updated += 1;
                } else {
                    if let Some(existing_source_id) =
                        existing_sources_by_book_id.get(&source.book_id)
                    {
                        preview
                            .conflicts
                            .push(ConfigTransferConflict::source_book_id(
                                source.id,
                                *existing_source_id,
                                &source.book_id,
                            ));
                    }
                    preview.sources_created += 1;
                }

                if let Some(ref aid) = source.account_id {
                    if known_account_ids.contains(aid) {
                        preview.account_bindings_preserved += 1;
                    } else {
                        preview.account_bindings_cleared += 1;
                    }
                }
            }
        }

        Ok(preview)
    }

    async fn apply(
        &self,
        bundle: ValidatedConfigBundle,
    ) -> Result<ImportResult, ConfigTransferRepositoryError> {
        let raw = bundle.into_bundle();
        let mut tx = self.pool.begin().await.map_err(storage_error)?;

        let result = match apply_transaction(&mut tx, &raw).await {
            Ok(res) => res,
            Err(err) => {
                let _ = tx.rollback().await;
                return Err(err);
            }
        };

        tx.commit().await.map_err(storage_error)?;
        Ok(result)
    }
}

async fn apply_transaction(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    raw: &ConfigBundle,
) -> Result<ImportResult, ConfigTransferRepositoryError> {
    let mut result = ImportResult::default();

    if let Some(accounts) = &raw.weread_accounts {
        for account in accounts {
            let existing_row = sqlx::query(
                "SELECT account_id, disabled, setup_required \
                 FROM weread_accounts \
                 WHERE account_id = $1 \
                 FOR UPDATE",
            )
            .bind(account.account_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(storage_error)?;

            if let Some(row) = existing_row {
                let existing_disabled: bool = row.try_get("disabled").map_err(storage_error)?;
                let setup_required: bool = row.try_get("setup_required").map_err(storage_error)?;

                sqlx::query(
                    "UPDATE weread_accounts \
                     SET display_name = $2, disabled = $3, updated_at = clock_timestamp() \
                     WHERE account_id = $1",
                )
                .bind(account.account_id)
                .bind(&account.display_name)
                .bind(!account.enabled)
                .execute(&mut **tx)
                .await
                .map_err(storage_error)?;

                if !setup_required && existing_disabled && account.enabled {
                    resume_authentication_gated_sources(tx, account.account_id).await?;
                }

                result.accounts_updated += 1;
            } else {
                sqlx::query(
                    "INSERT INTO weread_accounts ( \
                         account_id, display_name, credentials_ciphertext, access_expires_at, \
                         credential_version, disabled, setup_required, created_at, updated_at \
                     ) VALUES ( \
                         $1, $2, NULL, NULL, 1, $3, TRUE, clock_timestamp(), clock_timestamp() \
                     )",
                )
                .bind(account.account_id)
                .bind(&account.display_name)
                .bind(!account.enabled)
                .execute(&mut **tx)
                .await
                .map_err(storage_error)?;

                result.accounts_created += 1;
                result.placeholders_created += 1;
            }
        }
    }

    if let Some(sources) = &raw.sources {
        let account_rows = sqlx::query(
            "SELECT account_id, NOT disabled AND NOT setup_required \
             AND access_expires_at > clock_timestamp() AS usable FROM weread_accounts",
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(storage_error)?;

        let account_states: HashMap<Uuid, bool> = account_rows
            .into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<Uuid, _>("account_id")?,
                    row.try_get::<bool, _>("usable")?,
                ))
            })
            .collect::<Result<_, sqlx::Error>>()
            .map_err(storage_error)?;
        let known_account_ids: HashSet<Uuid> = account_states.keys().copied().collect();

        for source in sources {
            let resolved_account_id = match source.account_id {
                Some(aid) => {
                    if known_account_ids.contains(&aid) {
                        result.account_bindings_preserved += 1;
                        Some(aid)
                    } else {
                        result.account_bindings_cleared += 1;
                        None
                    }
                }
                None => None,
            };
            let requires_authentication =
                resolved_account_id.is_some_and(|account_id| !account_states[&account_id]);
            let new_gate = imported_source_gate(
                None,
                resolved_account_id.map(|account_id| account_states[&account_id]),
            );

            let existing_row = sqlx::query(
                "SELECT id, book_id, display_name, article_url, rss_item_limit, feed_revision, scheduling_gate \
                 FROM sources \
                 WHERE id = $1 \
                 FOR UPDATE",
            )
            .bind(source.id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(storage_error)?;

            if let Some(row) = existing_row {
                let conflict_row =
                    sqlx::query("SELECT id FROM sources WHERE book_id = $1 AND id != $2 LIMIT 1")
                        .bind(&source.book_id)
                        .bind(source.id)
                        .fetch_optional(&mut **tx)
                        .await
                        .map_err(storage_error)?;

                if let Some(conflict) = conflict_row {
                    let other_id: Uuid = conflict.try_get("id").map_err(storage_error)?;
                    return Err(ConfigTransferRepositoryError::Conflict(
                        ConfigTransferConflict::source_book_id(
                            source.id,
                            other_id,
                            &source.book_id,
                        ),
                    ));
                }

                let existing_book_id: String = row.try_get("book_id").map_err(storage_error)?;
                let existing_display_name: String =
                    row.try_get("display_name").map_err(storage_error)?;
                let existing_article_url: Option<String> =
                    row.try_get("article_url").map_err(storage_error)?;
                let existing_rss_item_limit: i64 =
                    row.try_get("rss_item_limit").map_err(storage_error)?;
                let existing_feed_revision: i64 =
                    row.try_get("feed_revision").map_err(storage_error)?;
                let existing_gate: String =
                    row.try_get("scheduling_gate").map_err(storage_error)?;
                let resumes_authentication = existing_gate == "authentication_required"
                    && resolved_account_id.is_some()
                    && !requires_authentication;
                let scheduling_gate = imported_source_gate(
                    Some(&existing_gate),
                    resolved_account_id.map(|account_id| account_states[&account_id]),
                );

                let feed_visible_changed = existing_book_id != source.book_id
                    || existing_display_name != source.display_name
                    || existing_article_url != source.article_url
                    || existing_rss_item_limit != i64::from(source.rss_item_limit);

                let new_feed_revision = if feed_visible_changed {
                    existing_feed_revision.checked_add(1).ok_or_else(|| {
                        ConfigTransferRepositoryError::Storage(
                            "feed revision exhausted".to_string(),
                        )
                    })?
                } else {
                    existing_feed_revision
                };

                sqlx::query(
                    "UPDATE sources \
                     SET book_id = $2, display_name = $3, article_url = $4, account_id = $5, \
                         scheduling_gate = $6, \
                         next_fetch_at = CASE WHEN $7 AND scheduling_gate = 'authentication_required' \
                             THEN clock_timestamp() ELSE next_fetch_at END, \
                         failure_cooldown_until = CASE WHEN $7 THEN NULL ELSE failure_cooldown_until END, \
                         schedule_reserved_until = CASE WHEN $7 THEN NULL ELSE schedule_reserved_until END, \
                         feed_revision = $8, enabled = $9, sync_interval_seconds = $10, \
                         rss_item_limit = $11, priority = $12, max_attempts = $13, \
                         updated_at = clock_timestamp() \
                     WHERE id = $1",
                )
                .bind(source.id)
                .bind(&source.book_id)
                .bind(&source.display_name)
                .bind(source.article_url.as_deref())
                .bind(resolved_account_id)
                .bind(scheduling_gate)
                .bind(resumes_authentication)
                .bind(new_feed_revision)
                .bind(source.enabled)
                .bind(source.sync_interval_seconds)
                .bind(i64::from(source.rss_item_limit))
                .bind(source.priority)
                .bind(i64::from(source.max_attempts))
                .execute(&mut **tx)
                .await
                .map_err(storage_error)?;

                result.sources_updated += 1;
            } else {
                let conflict_row = sqlx::query("SELECT id FROM sources WHERE book_id = $1 LIMIT 1")
                    .bind(&source.book_id)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(storage_error)?;

                if let Some(conflict) = conflict_row {
                    let other_id: Uuid = conflict.try_get("id").map_err(storage_error)?;
                    return Err(ConfigTransferRepositoryError::Conflict(
                        ConfigTransferConflict::source_book_id(
                            source.id,
                            other_id,
                            &source.book_id,
                        ),
                    ));
                }

                sqlx::query(
                    "INSERT INTO sources ( \
                         id, book_id, display_name, article_url, account_id, feed_revision, \
                         enabled, scheduling_gate, sync_interval_seconds, rss_item_limit, \
                         next_fetch_at, failure_cooldown_until, schedule_reserved_until, \
                         priority, max_attempts, created_at, updated_at \
                     ) VALUES ( \
                         $1, $2, $3, $4, $5, 0, $6, $11, $7, $8, clock_timestamp(), \
                         NULL, NULL, $9, $10, clock_timestamp(), clock_timestamp() \
                     )",
                )
                .bind(source.id)
                .bind(&source.book_id)
                .bind(&source.display_name)
                .bind(source.article_url.as_deref())
                .bind(resolved_account_id)
                .bind(source.enabled)
                .bind(source.sync_interval_seconds)
                .bind(i64::from(source.rss_item_limit))
                .bind(source.priority)
                .bind(i64::from(source.max_attempts))
                .bind(new_gate)
                .execute(&mut **tx)
                .await
                .map_err(storage_error)?;

                result.sources_created += 1;
            }
        }
    }

    Ok(result)
}

async fn resume_authentication_gated_sources(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
) -> Result<(), ConfigTransferRepositoryError> {
    sqlx::query(
        "UPDATE sources \
         SET scheduling_gate = 'ready', \
             failure_cooldown_until = NULL, \
             schedule_reserved_until = NULL, \
             next_fetch_at = clock_timestamp(), \
             updated_at = clock_timestamp() \
         WHERE account_id = $1 AND scheduling_gate = 'authentication_required'",
    )
    .bind(account_id)
    .execute(&mut **tx)
    .await
    .map_err(storage_error)?;

    Ok(())
}

fn imported_source_gate(existing_gate: Option<&str>, account_usable: Option<bool>) -> &'static str {
    if existing_gate == Some("risk_controlled") {
        return "risk_controlled";
    }
    if account_usable == Some(false) {
        return "authentication_required";
    }
    if existing_gate == Some("authentication_required") && account_usable != Some(true) {
        return "authentication_required";
    }
    "ready"
}

fn storage_error(error: sqlx::Error) -> ConfigTransferRepositoryError {
    if matches!(&error, sqlx::Error::Database(database_error) if database_error.code().as_deref() == Some("23505"))
    {
        return ConfigTransferRepositoryError::Conflict(
            ConfigTransferConflict::unique_constraint_violation(),
        );
    }
    ConfigTransferRepositoryError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::imported_source_gate;

    #[test]
    fn import_gates_unusable_bindings_and_only_resumes_auth_gates() {
        assert_eq!(
            imported_source_gate(None, Some(false)),
            "authentication_required"
        );
        assert_eq!(imported_source_gate(None, Some(true)), "ready");
        assert_eq!(imported_source_gate(None, None), "ready");
        assert_eq!(
            imported_source_gate(Some("authentication_required"), Some(true)),
            "ready"
        );
        assert_eq!(
            imported_source_gate(Some("authentication_required"), None),
            "authentication_required"
        );
        assert_eq!(
            imported_source_gate(Some("risk_controlled"), Some(false)),
            "risk_controlled"
        );
    }
}
