//! Configuration transfer bundle DTOs, strict validation, and export/import limits.
//!
//! Provides versioned data transfer objects for migrating source and WeRead account
//! configurations between instances without exporting secrets, credentials, or runtime
//! history.

use std::{collections::HashSet, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::source::VerifiedWechatArticleUrl;

/// Format identifier for portable configuration bundles.
pub const CONFIG_BUNDLE_FORMAT: &str = "werrss-config";

/// Supported configuration bundle version.
pub const CONFIG_BUNDLE_VERSION: u32 = 1;

/// Maximum allowed serialized bundle size (2 MiB).
pub const MAX_BUNDLE_BYTES: usize = 2 * 1024 * 1024;

/// Maximum allowed records per section in a bundle (1,000).
pub const MAX_SECTION_RECORDS: usize = 1_000;

/// Selection flags for export, requiring at least one selected section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigExportSelection {
    /// Whether to include WeChat official account sources.
    pub sources: bool,
    /// Whether to include WeRead accounts (credentials excluded).
    pub weread_accounts: bool,
}

impl ConfigExportSelection {
    /// Creates and validates a new export selection.
    pub fn new(
        sources: bool,
        weread_accounts: bool,
    ) -> Result<Self, ConfigTransferValidationError> {
        let selection = Self {
            sources,
            weread_accounts,
        };
        selection.validate()?;
        Ok(selection)
    }

    /// Validates that at least one section is selected.
    pub fn validate(&self) -> Result<(), ConfigTransferValidationError> {
        if !self.sources && !self.weread_accounts {
            return Err(ConfigTransferValidationError::EmptySelection);
        }
        Ok(())
    }
}

/// Portable representation of a subscribed WeChat public account source.
///
/// Contains only identity and portable configuration. Excludes article history,
/// cached assets, feed tokens, and local scheduling state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableSource {
    /// Durable source identifier.
    pub id: Uuid,
    /// Normalized WeRead book identifier.
    pub book_id: String,
    /// Human-readable source name.
    pub display_name: String,
    /// Verified originating public WeChat article URL, when configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub article_url: Option<String>,
    /// Account used by authenticated WeRead acquisition, if configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<Uuid>,
    /// Whether automatic synchronization is enabled.
    pub enabled: bool,
    /// Interval used by successful synchronization scheduling in whole seconds.
    pub sync_interval_seconds: i64,
    /// Maximum number of articles included in an RSS document.
    pub rss_item_limit: u32,
    /// Scheduling priority copied to source-sync jobs.
    pub priority: i32,
    /// Retry limit for source synchronization jobs.
    pub max_attempts: u32,
}

/// Portable representation of a configured WeRead account.
///
/// Contains only label and enabled preference. Completely excludes credentials,
/// cookies, tokens, ciphertext, encryption keys, and leases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableWeReadAccount {
    /// Durable account identifier.
    pub account_id: Uuid,
    /// Human-readable account label.
    pub display_name: String,
    /// Operator enabled preference.
    pub enabled: bool,
}

/// Wire bundle containing exported configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigBundle {
    /// Format identifier (`werrss-config`).
    pub format: String,
    /// Format version (`1`).
    pub version: u32,
    /// Timestamp when this bundle was exported.
    pub exported_at: DateTime<Utc>,
    /// Selected sources section.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<PortableSource>>,
    /// Selected WeRead accounts section.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weread_accounts: Option<Vec<PortableWeReadAccount>>,
}

impl ConfigBundle {
    /// Constructs a new bundle with default format and version.
    pub fn new(
        exported_at: DateTime<Utc>,
        sources: Option<Vec<PortableSource>>,
        weread_accounts: Option<Vec<PortableWeReadAccount>>,
    ) -> Self {
        Self {
            format: CONFIG_BUNDLE_FORMAT.to_string(),
            version: CONFIG_BUNDLE_VERSION,
            exported_at,
            sources,
            weread_accounts,
        }
    }

    /// Serializes the bundle to JSON bytes, enforcing section and size limits.
    pub fn to_serialized_limited(&self) -> Result<Vec<u8>, ConfigTransferValidationError> {
        self.serialize_limited()
    }

    /// Serializes the bundle to JSON bytes, enforcing section and size limits.
    pub fn serialize_limited(&self) -> Result<Vec<u8>, ConfigTransferValidationError> {
        if let Some(sources) = &self.sources {
            if sources.len() > MAX_SECTION_RECORDS {
                return Err(ConfigTransferValidationError::SectionTooLarge {
                    section: "sources",
                    count: sources.len(),
                    limit: MAX_SECTION_RECORDS,
                });
            }
        }
        if let Some(accounts) = &self.weread_accounts {
            if accounts.len() > MAX_SECTION_RECORDS {
                return Err(ConfigTransferValidationError::SectionTooLarge {
                    section: "weread_accounts",
                    count: accounts.len(),
                    limit: MAX_SECTION_RECORDS,
                });
            }
        }
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| ConfigTransferValidationError::Json(e.to_string()))?;
        if bytes.len() > MAX_BUNDLE_BYTES {
            return Err(ConfigTransferValidationError::PayloadTooLarge {
                size: bytes.len(),
                limit: MAX_BUNDLE_BYTES,
            });
        }
        Ok(bytes)
    }
}

/// A validated configuration bundle whose size, schema, format, records, and bounds have been checked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ValidatedConfigBundle(ConfigBundle);

impl ValidatedConfigBundle {
    /// Parses and strictly validates a configuration bundle from raw bytes.
    ///
    /// Rejects payloads larger than 2 MiB before JSON deserialization.
    pub fn parse_limited(bytes: &[u8]) -> Result<Self, ConfigTransferValidationError> {
        if bytes.len() > MAX_BUNDLE_BYTES {
            return Err(ConfigTransferValidationError::PayloadTooLarge {
                size: bytes.len(),
                limit: MAX_BUNDLE_BYTES,
            });
        }
        let bundle: ConfigBundle = serde_json::from_slice(bytes)
            .map_err(|e| ConfigTransferValidationError::Json(e.to_string()))?;
        Self::try_from(bundle)
    }

    /// Returns a reference to the inner bundle.
    pub fn bundle(&self) -> &ConfigBundle {
        &self.0
    }

    /// Consumes this wrapper and returns the inner bundle.
    pub fn into_bundle(self) -> ConfigBundle {
        self.0
    }

    /// Returns a slice of portable sources if the sources section was selected.
    pub fn sources(&self) -> Option<&[PortableSource]> {
        self.0.sources.as_deref()
    }

    /// Returns a slice of portable accounts if the accounts section was selected.
    pub fn weread_accounts(&self) -> Option<&[PortableWeReadAccount]> {
        self.0.weread_accounts.as_deref()
    }

    /// Returns the bundle format identifier.
    pub fn format(&self) -> &str {
        &self.0.format
    }

    /// Returns the bundle version.
    pub fn version(&self) -> u32 {
        self.0.version
    }

    /// Returns the timestamp when this bundle was exported.
    pub fn exported_at(&self) -> DateTime<Utc> {
        self.0.exported_at
    }

    /// Deconstructs the bundle into its optional source and account sections.
    pub fn into_parts(
        self,
    ) -> (
        Option<Vec<PortableSource>>,
        Option<Vec<PortableWeReadAccount>>,
    ) {
        (self.0.sources, self.0.weread_accounts)
    }
}

impl TryFrom<ConfigBundle> for ValidatedConfigBundle {
    type Error = ConfigTransferValidationError;

    fn try_from(bundle: ConfigBundle) -> Result<Self, Self::Error> {
        if bundle.format != CONFIG_BUNDLE_FORMAT {
            return Err(ConfigTransferValidationError::UnsupportedFormat(
                bundle.format,
            ));
        }
        if bundle.version != CONFIG_BUNDLE_VERSION {
            return Err(ConfigTransferValidationError::UnsupportedVersion(
                bundle.version,
            ));
        }
        if bundle.sources.is_none() && bundle.weread_accounts.is_none() {
            return Err(ConfigTransferValidationError::NoSectionsPresent);
        }

        if let Some(sources) = &bundle.sources {
            if sources.len() > MAX_SECTION_RECORDS {
                return Err(ConfigTransferValidationError::SectionTooLarge {
                    section: "sources",
                    count: sources.len(),
                    limit: MAX_SECTION_RECORDS,
                });
            }
            let mut seen_ids = HashSet::with_capacity(sources.len());
            let mut seen_book_ids = HashSet::with_capacity(sources.len());

            for source in sources {
                if source.id.is_nil() {
                    return Err(ConfigTransferValidationError::NilId { field: "id" });
                }
                if !seen_ids.insert(source.id) {
                    return Err(ConfigTransferValidationError::DuplicateSourceId(source.id));
                }

                if source.book_id.trim().is_empty() {
                    return Err(ConfigTransferValidationError::EmptyField { field: "book_id" });
                }
                if !seen_book_ids.insert(source.book_id.clone()) {
                    return Err(ConfigTransferValidationError::DuplicateBookId(
                        source.book_id.clone(),
                    ));
                }

                if source.display_name.trim().is_empty() {
                    return Err(ConfigTransferValidationError::EmptyField {
                        field: "display_name",
                    });
                }

                if let Some(account_id) = source.account_id {
                    if account_id.is_nil() {
                        return Err(ConfigTransferValidationError::NilId {
                            field: "account_id",
                        });
                    }
                }

                if let Some(url_str) = &source.article_url {
                    VerifiedWechatArticleUrl::parse(url_str).map_err(|e| {
                        ConfigTransferValidationError::InvalidArticleUrl(e.to_string())
                    })?;
                }

                if source.sync_interval_seconds <= 0 {
                    return Err(ConfigTransferValidationError::InvalidNumericBound {
                        field: "sync_interval_seconds",
                        message: "must be positive",
                    });
                }

                if source.rss_item_limit == 0 {
                    return Err(ConfigTransferValidationError::InvalidNumericBound {
                        field: "rss_item_limit",
                        message: "must be positive",
                    });
                }

                if source.max_attempts == 0 {
                    return Err(ConfigTransferValidationError::InvalidNumericBound {
                        field: "max_attempts",
                        message: "must be positive",
                    });
                }
            }
        }

        if let Some(accounts) = &bundle.weread_accounts {
            if accounts.len() > MAX_SECTION_RECORDS {
                return Err(ConfigTransferValidationError::SectionTooLarge {
                    section: "weread_accounts",
                    count: accounts.len(),
                    limit: MAX_SECTION_RECORDS,
                });
            }
            let mut seen_ids = HashSet::with_capacity(accounts.len());

            for account in accounts {
                if account.account_id.is_nil() {
                    return Err(ConfigTransferValidationError::NilId {
                        field: "account_id",
                    });
                }
                if !seen_ids.insert(account.account_id) {
                    return Err(ConfigTransferValidationError::DuplicateAccountId(
                        account.account_id,
                    ));
                }

                if account.display_name.trim().is_empty() {
                    return Err(ConfigTransferValidationError::EmptyField {
                        field: "display_name",
                    });
                }
            }
        }

        bundle.serialize_limited()?;
        Ok(Self(bundle))
    }
}

/// Non-mutating preview of an import operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ImportPreview {
    /// Number of sources that would be newly inserted.
    pub sources_created: usize,
    /// Number of existing sources that would be updated.
    pub sources_updated: usize,
    /// Number of WeRead accounts that would be created.
    pub accounts_created: usize,
    /// Number of existing WeRead accounts that would be updated.
    pub accounts_updated: usize,
    /// Number of credential-free setup-required placeholder accounts that would be created.
    pub placeholders_created: usize,
    /// Number of source-to-account bindings retained.
    pub account_bindings_preserved: usize,
    /// Number of source-to-account bindings cleared due to missing target accounts.
    pub account_bindings_cleared: usize,
    /// Detected stable-key or natural-key conflicts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<ConfigTransferConflict>,
}

impl ImportPreview {
    /// Reports whether any merge conflicts were detected.
    pub fn has_conflicts(&self) -> bool {
        !self.conflicts.is_empty()
    }
}

/// Result of an applied import operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ImportResult {
    /// Number of sources created.
    pub sources_created: usize,
    /// Number of sources updated.
    pub sources_updated: usize,
    /// Number of WeRead accounts created.
    pub accounts_created: usize,
    /// Number of WeRead accounts updated.
    pub accounts_updated: usize,
    /// Number of credential-free setup-required placeholder accounts created.
    pub placeholders_created: usize,
    /// Number of source-to-account bindings retained.
    pub account_bindings_preserved: usize,
    /// Number of source-to-account bindings cleared.
    pub account_bindings_cleared: usize,
}

/// Description of a stable-key or natural-key conflict detected during import or preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigTransferConflict {
    /// Category of the conflict.
    pub kind: String,
    /// Source ID involved in the conflict, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<Uuid>,
    /// Book ID involved in the collision, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub book_id: Option<String>,
    /// Existing source ID holding the book ID, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_source_id: Option<Uuid>,
    /// Safe human-readable description of the conflict without SQL or secrets.
    pub message: String,
}

impl ConfigTransferConflict {
    /// Creates a source book ID collision conflict.
    pub fn source_book_id(
        source_id: Uuid,
        existing_source_id: Uuid,
        book_id: impl Into<String>,
    ) -> Self {
        let book_id = book_id.into();
        let message = format!(
            "source '{}' specifies book_id '{}' which is already used by existing source '{}'",
            source_id, book_id, existing_source_id
        );
        Self {
            kind: "source_book_id_collision".to_string(),
            source_id: Some(source_id),
            book_id: Some(book_id),
            existing_source_id: Some(existing_source_id),
            message,
        }
    }

    /// Creates a safe conflict for a unique constraint race detected by PostgreSQL.
    pub fn unique_constraint_violation() -> Self {
        Self {
            kind: "unique_constraint_violation".to_string(),
            source_id: None,
            book_id: None,
            existing_source_id: None,
            message: "configuration conflicts with existing data; refresh preview and retry"
                .to_string(),
        }
    }
}

impl fmt::Display for ConfigTransferConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Errors raised when parsing, validating, or serializing configuration transfer data.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigTransferValidationError {
    /// Raw payload exceeds the maximum byte limit.
    #[error("payload exceeds limit of {limit} bytes (got {size} bytes)")]
    PayloadTooLarge {
        /// Actual byte size.
        size: usize,
        /// Maximum allowed bytes.
        limit: usize,
    },

    /// Deserialization failed due to syntax errors or unknown/forbidden fields.
    #[error("json deserialization failed: {0}")]
    Json(String),

    /// Document specifies an unsupported format string.
    #[error("unsupported bundle format '{0}', expected 'werrss-config'")]
    UnsupportedFormat(String),

    /// Document specifies an unsupported version number.
    #[error("unsupported bundle version {0}, expected 1")]
    UnsupportedVersion(u32),

    /// Bundle has neither sources nor weread_accounts section.
    #[error("bundle must contain at least one section")]
    NoSectionsPresent,

    /// Export selection specifies neither sources nor weread_accounts.
    #[error("export selection must include at least one section")]
    EmptySelection,

    /// A section exceeds the maximum record limit.
    #[error("section '{section}' exceeds maximum of {limit} records (got {count})")]
    SectionTooLarge {
        /// Section name.
        section: &'static str,
        /// Actual record count.
        count: usize,
        /// Maximum allowed records.
        limit: usize,
    },

    /// An identifier contains a nil UUID.
    #[error("field '{field}' must not contain nil UUID")]
    NilId {
        /// Field name.
        field: &'static str,
    },

    /// A required string field is empty or contains only whitespace.
    #[error("field '{field}' must not be empty")]
    EmptyField {
        /// Field name.
        field: &'static str,
    },

    /// An article URL is invalid according to WeChat URL policy.
    #[error("source article_url is invalid: {0}")]
    InvalidArticleUrl(String),

    /// A numeric setting is outside valid domain bounds.
    #[error("numeric field '{field}' is invalid: {message}")]
    InvalidNumericBound {
        /// Field name.
        field: &'static str,
        /// Human-readable bound explanation.
        message: &'static str,
    },

    /// Multiple sources within the bundle share the same source ID.
    #[error("duplicate source id '{0}' within bundle")]
    DuplicateSourceId(Uuid),

    /// Multiple sources within the bundle share the same book ID.
    #[error("duplicate book id '{0}' within bundle")]
    DuplicateBookId(String),

    /// Multiple accounts within the bundle share the same account ID.
    #[error("duplicate account id '{0}' within bundle")]
    DuplicateAccountId(Uuid),
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_valid_source_only_bundle() {
        let source_id = Uuid::new_v4();
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": source_id,
                    "book_id": "book_123",
                    "display_name": "Test Source",
                    "article_url": "https://mp.weixin.qq.com/s?__biz=abc&mid=1",
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });

        let bytes = serde_json::to_vec(&payload).unwrap();
        let validated =
            ValidatedConfigBundle::parse_limited(&bytes).expect("valid source-only bundle");
        let sources = validated.sources().expect("sources present");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].id, source_id);
        assert_eq!(sources[0].book_id, "book_123");
        assert_eq!(sources[0].display_name, "Test Source");
        assert_eq!(
            sources[0].article_url.as_deref(),
            Some("https://mp.weixin.qq.com/s?__biz=abc&mid=1")
        );
        assert!(sources[0].account_id.is_none());
        assert!(sources[0].enabled);
        assert_eq!(sources[0].sync_interval_seconds, 3600);
        assert_eq!(sources[0].rss_item_limit, 20);
        assert_eq!(sources[0].priority, 10);
        assert_eq!(sources[0].max_attempts, 3);
        assert!(validated.weread_accounts().is_none());
    }

    #[test]
    fn test_valid_account_only_bundle() {
        let account_id = Uuid::new_v4();
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "weread_accounts": [
                {
                    "account_id": account_id,
                    "display_name": "Test Account",
                    "enabled": true
                }
            ]
        });

        let bytes = serde_json::to_vec(&payload).unwrap();
        let validated =
            ValidatedConfigBundle::parse_limited(&bytes).expect("valid account-only bundle");
        assert!(validated.sources().is_none());
        let accounts = validated.weread_accounts().expect("accounts present");
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].account_id, account_id);
        assert_eq!(accounts[0].display_name, "Test Account");
        assert!(accounts[0].enabled);
    }

    #[test]
    fn test_valid_both_sections_bundle() {
        let source_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": source_id,
                    "book_id": "book_456",
                    "display_name": "Source 2",
                    "article_url": null,
                    "account_id": account_id,
                    "enabled": false,
                    "sync_interval_seconds": 7200,
                    "rss_item_limit": 50,
                    "priority": 0,
                    "max_attempts": 5
                }
            ],
            "weread_accounts": [
                {
                    "account_id": account_id,
                    "display_name": "Account 2",
                    "enabled": false
                }
            ]
        });

        let bytes = serde_json::to_vec(&payload).unwrap();
        let validated =
            ValidatedConfigBundle::parse_limited(&bytes).expect("valid both sections bundle");
        assert_eq!(validated.sources().unwrap().len(), 1);
        assert_eq!(validated.weread_accounts().unwrap().len(), 1);
    }

    #[test]
    fn test_empty_selection_rejected() {
        let selection = ConfigExportSelection {
            sources: false,
            weread_accounts: false,
        };
        assert_eq!(
            selection.validate().unwrap_err(),
            ConfigTransferValidationError::EmptySelection
        );
        assert_eq!(
            ConfigExportSelection::new(false, false).unwrap_err(),
            ConfigTransferValidationError::EmptySelection
        );

        // Bundle with neither section present
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339()
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(err, ConfigTransferValidationError::NoSectionsPresent);
    }

    #[test]
    fn test_unsupported_version_rejected() {
        let payload = json!({
            "format": "werrss-config",
            "version": 2,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": []
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(err, ConfigTransferValidationError::UnsupportedVersion(2));
    }

    #[test]
    fn test_unsupported_format_rejected() {
        let payload = json!({
            "format": "invalid-format",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": []
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::UnsupportedFormat("invalid-format".to_string())
        );
    }

    #[test]
    fn test_unknown_keys_rejected() {
        // Unknown root key
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [],
            "extra_key": "not allowed"
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        match err {
            ConfigTransferValidationError::Json(msg) => {
                assert!(msg.contains("unknown field `extra_key`"), "got {msg}");
            }
            other => panic!("expected Json unknown field error, got {other:?}"),
        }

        // Unknown key inside sources
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "book_1",
                    "display_name": "Test",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3,
                    "unexpected": "field"
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        match err {
            ConfigTransferValidationError::Json(msg) => {
                assert!(msg.contains("unknown field `unexpected`"), "got {msg}");
            }
            other => panic!("expected Json unknown field error, got {other:?}"),
        }

        // Unknown key inside weread_accounts
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "weread_accounts": [
                {
                    "account_id": Uuid::new_v4(),
                    "display_name": "Account",
                    "enabled": true,
                    "another_key": 42
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        match err {
            ConfigTransferValidationError::Json(msg) => {
                assert!(msg.contains("unknown field `another_key`"), "got {msg}");
            }
            other => panic!("expected Json unknown field error, got {other:?}"),
        }
    }

    #[test]
    fn test_duplicate_source_id_rejected() {
        let duplicate_id = Uuid::new_v4();
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": duplicate_id,
                    "book_id": "book_1",
                    "display_name": "First",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                },
                {
                    "id": duplicate_id,
                    "book_id": "book_2",
                    "display_name": "Second",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::DuplicateSourceId(duplicate_id)
        );
    }

    #[test]
    fn test_duplicate_book_id_rejected() {
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "same_book_id",
                    "display_name": "First",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                },
                {
                    "id": Uuid::new_v4(),
                    "book_id": "same_book_id",
                    "display_name": "Second",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::DuplicateBookId("same_book_id".to_string())
        );
    }

    #[test]
    fn test_duplicate_account_id_rejected() {
        let duplicate_account_id = Uuid::new_v4();
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "weread_accounts": [
                {
                    "account_id": duplicate_account_id,
                    "display_name": "Account 1",
                    "enabled": true
                },
                {
                    "account_id": duplicate_account_id,
                    "display_name": "Account 2",
                    "enabled": false
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::DuplicateAccountId(duplicate_account_id)
        );
    }

    #[test]
    fn test_nil_ids_rejected() {
        // Nil source id
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::nil(),
                    "book_id": "book_nil",
                    "display_name": "Nil ID Source",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(err, ConfigTransferValidationError::NilId { field: "id" });

        // Nil source account_id
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "book_nil_acc",
                    "display_name": "Nil Account ID",
                    "article_url": null,
                    "account_id": Uuid::nil(),
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::NilId {
                field: "account_id"
            }
        );

        // Nil account id
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "weread_accounts": [
                {
                    "account_id": Uuid::nil(),
                    "display_name": "Nil Account",
                    "enabled": true
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::NilId {
                field: "account_id"
            }
        );
    }

    #[test]
    fn test_empty_required_fields_rejected() {
        // Empty book_id
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "   ",
                    "display_name": "Non-empty Name",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::EmptyField { field: "book_id" }
        );

        // Empty display_name in source
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "book_valid",
                    "display_name": "",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::EmptyField {
                field: "display_name"
            }
        );

        // Empty display_name in account
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "weread_accounts": [
                {
                    "account_id": Uuid::new_v4(),
                    "display_name": "  ",
                    "enabled": true
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::EmptyField {
                field: "display_name"
            }
        );
    }

    #[test]
    fn test_invalid_article_urls_rejected() {
        let invalid_urls = [
            "",
            "http://mp.weixin.qq.com/s/123",
            "https://example.com/article/123",
            "https://mp.weixin.qq.com:8443/s/123",
            "https://user:pass@mp.weixin.qq.com/s/123",
            "https://mp.weixin.qq.com/s/123#frag",
        ];

        for invalid_url in invalid_urls {
            let payload = json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "sources": [
                    {
                        "id": Uuid::new_v4(),
                        "book_id": "book_url_test",
                        "display_name": "URL Test",
                        "article_url": invalid_url,
                        "account_id": null,
                        "enabled": true,
                        "sync_interval_seconds": 3600,
                        "rss_item_limit": 20,
                        "priority": 10,
                        "max_attempts": 3
                    }
                ]
            });
            let bytes = serde_json::to_vec(&payload).unwrap();
            let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
            assert!(
                matches!(err, ConfigTransferValidationError::InvalidArticleUrl(_)),
                "expected InvalidArticleUrl for '{invalid_url}', got {err:?}"
            );
        }

        // Valid public URL accepted
        let clean_payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "book_url_valid",
                    "display_name": "Valid URL Test",
                    "article_url": "https://mp.weixin.qq.com/s?__biz=abc&mid=123",
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&clean_payload).unwrap();
        assert!(ValidatedConfigBundle::parse_limited(&bytes).is_ok());
    }

    #[test]
    fn test_invalid_numeric_bounds_rejected() {
        // sync_interval_seconds = 0
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "b1",
                    "display_name": "S1",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 0,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::InvalidNumericBound {
                field: "sync_interval_seconds",
                message: "must be positive"
            }
        );

        // sync_interval_seconds < 0
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "b1",
                    "display_name": "S1",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": -30,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::InvalidNumericBound {
                field: "sync_interval_seconds",
                message: "must be positive"
            }
        );

        // rss_item_limit = 0
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "b1",
                    "display_name": "S1",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 0,
                    "priority": 10,
                    "max_attempts": 3
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::InvalidNumericBound {
                field: "rss_item_limit",
                message: "must be positive"
            }
        );

        // max_attempts = 0
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": [
                {
                    "id": Uuid::new_v4(),
                    "book_id": "b1",
                    "display_name": "S1",
                    "article_url": null,
                    "account_id": null,
                    "enabled": true,
                    "sync_interval_seconds": 3600,
                    "rss_item_limit": 20,
                    "priority": 10,
                    "max_attempts": 0
                }
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::InvalidNumericBound {
                field: "max_attempts",
                message: "must be positive"
            }
        );
    }

    #[test]
    fn test_forbidden_secret_and_history_fields_rejected() {
        let forbidden_payloads = [
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "articles": []
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "credentials": {}
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "jobs": []
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "sync_runs": []
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "feed_tokens": []
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "assets": []
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "weread_accounts": [
                    {
                        "account_id": Uuid::new_v4(),
                        "display_name": "Acc",
                        "enabled": true,
                        "access_token": "secret123"
                    }
                ]
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "weread_accounts": [
                    {
                        "account_id": Uuid::new_v4(),
                        "display_name": "Acc",
                        "enabled": true,
                        "ciphertext": "encrypted_blob"
                    }
                ]
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "weread_accounts": [
                    {
                        "account_id": Uuid::new_v4(),
                        "display_name": "Acc",
                        "enabled": true,
                        "web_cookie": "cookie_data"
                    }
                ]
            }),
            json!({
                "format": "werrss-config",
                "version": 1,
                "exported_at": Utc::now().to_rfc3339(),
                "sources": [
                    {
                        "id": Uuid::new_v4(),
                        "book_id": "b1",
                        "display_name": "S1",
                        "article_url": null,
                        "account_id": null,
                        "enabled": true,
                        "sync_interval_seconds": 3600,
                        "rss_item_limit": 20,
                        "priority": 10,
                        "max_attempts": 3,
                        "last_sync_at": Utc::now().to_rfc3339()
                    }
                ]
            }),
        ];

        for payload in forbidden_payloads {
            let bytes = serde_json::to_vec(&payload).unwrap();
            let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
            assert!(
                matches!(err, ConfigTransferValidationError::Json(_)),
                "expected Json rejection for forbidden field in payload {payload:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn test_payload_over_2_mib_rejected_before_json_parsing() {
        let oversized_len = MAX_BUNDLE_BYTES + 1;
        let mut bytes = vec![b'a'; oversized_len];
        // Even if bytes start with invalid JSON, the size check triggers first:
        bytes[0] = b'{';
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::PayloadTooLarge {
                size: oversized_len,
                limit: MAX_BUNDLE_BYTES,
            }
        );
    }

    #[test]
    fn test_sources_section_over_1000_entries_rejected() {
        let mut sources = Vec::with_capacity(1001);
        for i in 0..1001 {
            sources.push(json!({
                "id": Uuid::new_v4(),
                "book_id": format!("book_{i}"),
                "display_name": format!("Source {i}"),
                "article_url": null,
                "account_id": null,
                "enabled": true,
                "sync_interval_seconds": 3600,
                "rss_item_limit": 20,
                "priority": 10,
                "max_attempts": 3
            }));
        }
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "sources": sources
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::SectionTooLarge {
                section: "sources",
                count: 1001,
                limit: MAX_SECTION_RECORDS,
            }
        );
    }

    #[test]
    fn test_accounts_section_over_1000_entries_rejected() {
        let mut accounts = Vec::with_capacity(1001);
        for i in 0..1001 {
            accounts.push(json!({
                "account_id": Uuid::new_v4(),
                "display_name": format!("Account {i}"),
                "enabled": true
            }));
        }
        let payload = json!({
            "format": "werrss-config",
            "version": 1,
            "exported_at": Utc::now().to_rfc3339(),
            "weread_accounts": accounts
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let err = ValidatedConfigBundle::parse_limited(&bytes).unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::SectionTooLarge {
                section: "weread_accounts",
                count: 1001,
                limit: MAX_SECTION_RECORDS,
            }
        );
    }

    #[test]
    fn test_export_section_over_1000_rejected() {
        let mut sources = Vec::with_capacity(1001);
        for i in 0..1001 {
            sources.push(PortableSource {
                id: Uuid::new_v4(),
                book_id: format!("book_{i}"),
                display_name: format!("Source {i}"),
                article_url: None,
                account_id: None,
                enabled: true,
                sync_interval_seconds: 3600,
                rss_item_limit: 20,
                priority: 10,
                max_attempts: 3,
            });
        }
        let bundle = ConfigBundle::new(Utc::now(), Some(sources), None);
        let err = bundle.to_serialized_limited().unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::SectionTooLarge {
                section: "sources",
                count: 1001,
                limit: MAX_SECTION_RECORDS,
            }
        );

        let mut accounts = Vec::with_capacity(1001);
        for i in 0..1001 {
            accounts.push(PortableWeReadAccount {
                account_id: Uuid::new_v4(),
                display_name: format!("Account {i}"),
                enabled: true,
            });
        }
        let bundle = ConfigBundle::new(Utc::now(), None, Some(accounts));
        let err = bundle.to_serialized_limited().unwrap_err();
        assert_eq!(
            err,
            ConfigTransferValidationError::SectionTooLarge {
                section: "weread_accounts",
                count: 1001,
                limit: MAX_SECTION_RECORDS,
            }
        );
    }

    #[test]
    fn test_export_serialized_over_2_mib_rejected() {
        let mut sources = Vec::new();
        let big_name = "x".repeat(3000);
        for i in 0..800 {
            sources.push(PortableSource {
                id: Uuid::new_v4(),
                book_id: format!("book_{i}"),
                display_name: big_name.clone(),
                article_url: None,
                account_id: None,
                enabled: true,
                sync_interval_seconds: 3600,
                rss_item_limit: 20,
                priority: 10,
                max_attempts: 3,
            });
        }
        let bundle = ConfigBundle::new(Utc::now(), Some(sources), None);
        let err = bundle.to_serialized_limited().unwrap_err();
        assert!(matches!(
            err,
            ConfigTransferValidationError::PayloadTooLarge { .. }
        ));
    }

    #[test]
    fn test_serialized_output_never_contains_forbidden_property_names() {
        let source_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        let bundle = ConfigBundle::new(
            Utc::now(),
            Some(vec![PortableSource {
                id: source_id,
                book_id: "book_test".to_string(),
                display_name: "Test Source".to_string(),
                article_url: Some("https://mp.weixin.qq.com/s?__biz=abc&mid=1".to_string()),
                account_id: Some(account_id),
                enabled: true,
                sync_interval_seconds: 3600,
                rss_item_limit: 20,
                priority: 10,
                max_attempts: 3,
            }]),
            Some(vec![PortableWeReadAccount {
                account_id,
                display_name: "Test Account".to_string(),
                enabled: true,
            }]),
        );

        let serialized_bytes = bundle
            .to_serialized_limited()
            .expect("serialization succeeds");
        let serialized_str = std::str::from_utf8(&serialized_bytes).unwrap();

        let forbidden_strings = [
            "access_token",
            "refresh_token",
            "web_cookie",
            "cookie",
            "credentials",
            "ciphertext",
            "credentials_ciphertext",
            "access_expires_at",
            "credential_version",
            "lease_owner",
            "lease_token",
            "articles",
            "jobs",
            "sync_runs",
            "feed_tokens",
            "assets",
            "history",
            "setup_required",
        ];

        for forbidden in forbidden_strings {
            assert!(
                !serialized_str.contains(forbidden),
                "serialized output must not contain forbidden string '{forbidden}'"
            );
        }
    }

    #[test]
    fn test_typed_bundle_construction_cannot_bypass_limits() {
        // Direct construction with invalid version
        let mut bundle = ConfigBundle::new(Utc::now(), Some(vec![]), None);
        bundle.version = 99;
        assert_eq!(
            ValidatedConfigBundle::try_from(bundle).unwrap_err(),
            ConfigTransferValidationError::UnsupportedVersion(99)
        );

        // Direct construction with duplicate source IDs
        let dup_id = Uuid::new_v4();
        let bundle = ConfigBundle::new(
            Utc::now(),
            Some(vec![
                PortableSource {
                    id: dup_id,
                    book_id: "b1".to_string(),
                    display_name: "S1".to_string(),
                    article_url: None,
                    account_id: None,
                    enabled: true,
                    sync_interval_seconds: 3600,
                    rss_item_limit: 20,
                    priority: 10,
                    max_attempts: 3,
                },
                PortableSource {
                    id: dup_id,
                    book_id: "b2".to_string(),
                    display_name: "S2".to_string(),
                    article_url: None,
                    account_id: None,
                    enabled: true,
                    sync_interval_seconds: 3600,
                    rss_item_limit: 20,
                    priority: 10,
                    max_attempts: 3,
                },
            ]),
            None,
        );
        assert_eq!(
            ValidatedConfigBundle::try_from(bundle).unwrap_err(),
            ConfigTransferValidationError::DuplicateSourceId(dup_id)
        );

        // Direct construction with nil source ID
        let bundle = ConfigBundle::new(
            Utc::now(),
            Some(vec![PortableSource {
                id: Uuid::nil(),
                book_id: "b1".to_string(),
                display_name: "S1".to_string(),
                article_url: None,
                account_id: None,
                enabled: true,
                sync_interval_seconds: 3600,
                rss_item_limit: 20,
                priority: 10,
                max_attempts: 3,
            }]),
            None,
        );
        assert_eq!(
            ValidatedConfigBundle::try_from(bundle).unwrap_err(),
            ConfigTransferValidationError::NilId { field: "id" }
        );

        // Direct construction with invalid URL
        let bundle = ConfigBundle::new(
            Utc::now(),
            Some(vec![PortableSource {
                id: Uuid::new_v4(),
                book_id: "b1".to_string(),
                display_name: "S1".to_string(),
                article_url: Some("http://invalid.com".to_string()),
                account_id: None,
                enabled: true,
                sync_interval_seconds: 3600,
                rss_item_limit: 20,
                priority: 10,
                max_attempts: 3,
            }]),
            None,
        );
        assert!(matches!(
            ValidatedConfigBundle::try_from(bundle).unwrap_err(),
            ConfigTransferValidationError::InvalidArticleUrl(_)
        ));

        // Direct construction with no sections
        let bundle = ConfigBundle::new(Utc::now(), None, None);
        assert_eq!(
            ValidatedConfigBundle::try_from(bundle).unwrap_err(),
            ConfigTransferValidationError::NoSectionsPresent
        );
    }

    #[test]
    fn test_typed_bundle_rejects_serialized_payload_over_limit() {
        let bundle = ConfigBundle::new(
            Utc::now(),
            None,
            Some(vec![PortableWeReadAccount {
                account_id: Uuid::new_v4(),
                display_name: "a".repeat(MAX_BUNDLE_BYTES),
                enabled: true,
            }]),
        );

        assert!(matches!(
            ValidatedConfigBundle::try_from(bundle),
            Err(ConfigTransferValidationError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn test_import_preview_and_result_types_and_conflict() {
        let preview = ImportPreview::default();
        assert_eq!(preview.sources_created, 0);
        assert_eq!(preview.sources_updated, 0);
        assert_eq!(preview.accounts_created, 0);
        assert_eq!(preview.accounts_updated, 0);
        assert_eq!(preview.placeholders_created, 0);
        assert_eq!(preview.account_bindings_preserved, 0);
        assert_eq!(preview.account_bindings_cleared, 0);
        assert!(!preview.has_conflicts());

        let source_id = Uuid::new_v4();
        let existing_source_id = Uuid::new_v4();
        let conflict =
            ConfigTransferConflict::source_book_id(source_id, existing_source_id, "book_test");
        assert_eq!(conflict.kind, "source_book_id_collision");
        assert_eq!(conflict.source_id, Some(source_id));
        assert_eq!(conflict.existing_source_id, Some(existing_source_id));
        assert_eq!(conflict.book_id.as_deref(), Some("book_test"));
        assert!(conflict.message.contains("book_test"));
        assert_eq!(format!("{conflict}"), conflict.message);

        let result = ImportResult::default();
        assert_eq!(result.sources_created, 0);
        assert_eq!(result.sources_updated, 0);
        assert_eq!(result.accounts_created, 0);
        assert_eq!(result.accounts_updated, 0);
        assert_eq!(result.placeholders_created, 0);
        assert_eq!(result.account_bindings_preserved, 0);
        assert_eq!(result.account_bindings_cleared, 0);
    }
}
