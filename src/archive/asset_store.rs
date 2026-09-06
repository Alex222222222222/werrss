//! Values shared by asset acquisition, persistence, and URL rewriting.
//!
//! The database implementation lives in `persistence::repositories`, while
//! this module deliberately contains no SQL or HTTP client. Keeping the policy
//! here makes the disabled default explicit and gives the application and
//! repository the same byte/age/count limits.

use std::time::Duration;

use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

/// First-version limits for binary asset caching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetCachePolicy {
    max_cache_size_bytes: u64,
    max_age: Duration,
    max_asset_size_bytes: u64,
    max_count_per_article: u32,
    max_fetch_bytes_per_article: u64,
    max_fetch_time_per_article: Duration,
    fetch_timeout: Duration,
    max_redirects: u32,
}

impl AssetCachePolicy {
    /// Creates a validated cache policy.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_cache_size_bytes: u64,
        max_age: Duration,
        max_asset_size_bytes: u64,
        max_count_per_article: u32,
        max_fetch_bytes_per_article: u64,
        max_fetch_time_per_article: Duration,
        fetch_timeout: Duration,
        max_redirects: u32,
    ) -> Result<Self, AssetCachePolicyError> {
        if max_asset_size_bytes == 0 {
            return Err(AssetCachePolicyError::ZeroLimit {
                field: "max_asset_size_bytes",
            });
        }
        if max_count_per_article == 0 {
            return Err(AssetCachePolicyError::ZeroLimit {
                field: "max_count_per_article",
            });
        }
        if max_fetch_bytes_per_article == 0 {
            return Err(AssetCachePolicyError::ZeroLimit {
                field: "max_fetch_bytes_per_article",
            });
        }
        if max_fetch_time_per_article.is_zero() {
            return Err(AssetCachePolicyError::ZeroDuration {
                field: "max_fetch_time_per_article",
            });
        }
        if fetch_timeout.is_zero() {
            return Err(AssetCachePolicyError::ZeroDuration {
                field: "fetch_timeout",
            });
        }
        if fetch_timeout > max_fetch_time_per_article {
            return Err(AssetCachePolicyError::TimeoutExceedsArticleBudget);
        }

        Ok(Self {
            max_cache_size_bytes,
            max_age,
            max_asset_size_bytes,
            max_count_per_article,
            max_fetch_bytes_per_article,
            max_fetch_time_per_article,
            fetch_timeout,
            max_redirects,
        })
    }

    /// Maximum aggregate number of cached raw binary bytes; zero is unlimited.
    pub const fn max_cache_size_bytes(self) -> u64 {
        self.max_cache_size_bytes
    }

    /// Maximum idle age after the last successful asset access; zero is
    /// unlimited.
    pub const fn max_age(self) -> Duration {
        self.max_age
    }

    /// Maximum raw binary size of one asset.
    pub const fn max_asset_size_bytes(self) -> u64 {
        self.max_asset_size_bytes
    }

    /// Maximum number of distinct assets considered for one article.
    pub const fn max_count_per_article(self) -> u32 {
        self.max_count_per_article
    }

    /// Maximum aggregate response bytes read for one article.
    pub const fn max_fetch_bytes_per_article(self) -> u64 {
        self.max_fetch_bytes_per_article
    }

    /// Maximum wall-clock time spent fetching one article's assets.
    pub const fn max_fetch_time_per_article(self) -> Duration {
        self.max_fetch_time_per_article
    }

    /// Maximum duration for one asset request.
    pub const fn fetch_timeout(self) -> Duration {
        self.fetch_timeout
    }

    /// Maximum redirects followed for one asset request.
    pub const fn max_redirects(self) -> u32 {
        self.max_redirects
    }
}

impl Default for AssetCachePolicy {
    fn default() -> Self {
        Self::new(
            5_000_000_000,
            Duration::from_secs(30 * 24 * 60 * 60),
            10_000_000,
            100,
            100_000_000,
            Duration::from_secs(120),
            Duration::from_secs(30),
            5,
        )
        .expect("default asset policy must be valid")
    }
}

/// Durable admission and retry policy for missing-asset repair jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetRepairPolicy {
    max_pending: u32,
    public_max_pending: u32,
    requeue_after: Duration,
    max_attempts: u32,
}

impl AssetRepairPolicy {
    /// Creates a validated repair policy.
    pub fn new(
        max_pending: u32,
        public_max_pending: u32,
        requeue_after: Duration,
        max_attempts: u32,
    ) -> Result<Self, AssetRepairPolicyError> {
        if max_pending == 0 {
            return Err(AssetRepairPolicyError::ZeroLimit {
                field: "max_pending",
            });
        }
        if public_max_pending > max_pending {
            return Err(AssetRepairPolicyError::PublicLimitExceedsGlobal);
        }
        if requeue_after.is_zero() {
            return Err(AssetRepairPolicyError::ZeroDuration);
        }
        if max_attempts == 0 {
            return Err(AssetRepairPolicyError::ZeroLimit {
                field: "max_attempts",
            });
        }
        Ok(Self {
            max_pending,
            public_max_pending,
            requeue_after,
            max_attempts,
        })
    }

    /// Maximum number of active asset-repair jobs across the database.
    pub const fn max_pending(self) -> u32 {
        self.max_pending
    }

    /// Maximum number of active public cache-miss repair jobs.
    pub const fn public_max_pending(self) -> u32 {
        self.public_max_pending
    }

    /// Minimum delay before a terminal repair may be admitted again.
    pub const fn requeue_after(self) -> Duration {
        self.requeue_after
    }

    /// Number of independently admitted repair attempts before a circuit opens.
    pub const fn max_attempts(self) -> u32 {
        self.max_attempts
    }
}

impl Default for AssetRepairPolicy {
    fn default() -> Self {
        Self::new(100, 50, Duration::from_secs(60), 3)
            .expect("default asset repair policy must be valid")
    }
}

/// Invalid asset-repair admission configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AssetRepairPolicyError {
    /// A positive repair limit was set to zero.
    #[error("asset repair policy field {field} must be greater than zero")]
    ZeroLimit { field: &'static str },
    /// Public work cannot be allowed to exceed the global repair capacity.
    #[error("public asset repair limit must not exceed the global limit")]
    PublicLimitExceedsGlobal,
    /// A retry delay must prevent a hot loop after a terminal failure.
    #[error("asset repair requeue delay must be positive")]
    ZeroDuration,
}

/// Invalid asset cache policy input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AssetCachePolicyError {
    /// A positive size or count limit was set to zero.
    #[error("asset policy field {field} must be greater than zero")]
    ZeroLimit { field: &'static str },
    /// A positive duration was set to zero.
    #[error("asset policy field {field} must be greater than zero")]
    ZeroDuration { field: &'static str },
    /// A request could outlive the per-article wall-clock budget.
    #[error("asset request timeout must not exceed the per-article fetch time budget")]
    TimeoutExceedsArticleBudget,
}

/// One validated binary response ready for persistence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetInput {
    /// URL found in sanitized article HTML.
    pub source_url: Url,
    /// URL after the HTTP redirect chain.
    pub final_url: Url,
    /// Validated response media type.
    pub media_type: String,
    /// Raw response body.
    pub bytes: Vec<u8>,
    /// Stable image occurrence in the sanitized article.
    pub occurrence: u32,
    /// Article page URL sent as the HTTP `Referer`.
    pub referer_url: Url,
    /// Page origin sent as the HTTP `Origin`, when one is available.
    pub origin: Option<String>,
    /// Browser User-Agent profile used for the article page, when configured.
    pub user_agent: Option<String>,
    checksum: String,
    preparation_id: Uuid,
}

/// Non-secret context needed to repair one referenced asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetRepairTarget {
    /// Stable asset record restored by the repair.
    pub asset_id: Uuid,
    /// Original upstream asset URL.
    pub source_url: Url,
    /// Article page URL used as the request referer.
    pub referer_url: Url,
    /// Optional article origin captured at acquisition time.
    pub origin: Option<String>,
    /// Optional browser User-Agent captured at acquisition time.
    pub user_agent: Option<String>,
    /// Earliest instant at which this durable repair lineage may be attempted.
    ///
    /// This is populated when a worker records a failure before it crashes.
    /// A recovered job must defer until this instant instead of bypassing the
    /// public-admission backoff window.
    pub next_allowed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Whether the bounded repair-attempt budget is exhausted.
    pub exhausted: bool,
}

impl AssetInput {
    /// Creates an asset input and computes its digest before persistence.
    ///
    /// The digest is deliberately part of the value rather than calculated by
    /// the PostgreSQL repository. Callers construct this value after the
    /// network response has been bounded and validated, before opening the
    /// short article persistence transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_url: Url,
        final_url: Url,
        media_type: String,
        bytes: Vec<u8>,
        occurrence: u32,
        referer_url: Url,
        origin: Option<String>,
        user_agent: Option<String>,
    ) -> Self {
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        Self {
            source_url,
            final_url,
            media_type,
            bytes,
            occurrence,
            referer_url,
            origin,
            user_agent,
            checksum,
            preparation_id: Uuid::new_v4(),
        }
    }

    /// Returns the precomputed raw-byte checksum.
    pub(crate) fn checksum(&self) -> &str {
        &self.checksum
    }

    /// Returns whether the raw body still matches the digest captured at
    /// construction time.
    pub(crate) fn checksum_matches_bytes(&self) -> bool {
        format!("{:x}", Sha256::digest(&self.bytes)) == self.checksum
    }

    /// Returns the identity used to carry preflight results into persistence.
    pub(crate) const fn preparation_id(&self) -> Uuid {
        self.preparation_id
    }
}

/// One persisted URL/version and its shared binary blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAsset {
    id: Uuid,
    source_url: Url,
    blob_id: Uuid,
    checksum: String,
    media_type: String,
    byte_size: u64,
}

impl StoredAsset {
    /// Creates a stored-asset value from trusted repository fields.
    pub(crate) fn new(
        id: Uuid,
        source_url: Url,
        blob_id: Uuid,
        checksum: String,
        media_type: String,
        byte_size: u64,
    ) -> Self {
        Self {
            id,
            source_url,
            blob_id,
            checksum,
            media_type,
            byte_size,
        }
    }

    /// Stable opaque ID used by the public `/assets/{id}` route.
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// URL that produced this asset version.
    pub fn source_url(&self) -> &Url {
        &self.source_url
    }

    /// Shared blob identity, useful for integration diagnostics.
    pub const fn blob_id(&self) -> Uuid {
        self.blob_id
    }

    /// Lowercase SHA-256 checksum of the raw bytes.
    pub fn checksum(&self) -> &str {
        &self.checksum
    }

    /// Validated media type used by the response.
    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    /// Raw byte count used for cache accounting.
    pub const fn byte_size(&self) -> u64 {
        self.byte_size
    }
}

/// Result of reading a stable asset route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetRead {
    /// The asset is referenced and its bytes are present.
    Available {
        /// Stored response media type.
        media_type: String,
        /// Lowercase SHA-256 checksum.
        checksum: String,
        /// Raw bytes.
        bytes: Vec<u8>,
    },
    /// The URL metadata remains, but bytes need to be repaired or fetched.
    Missing {
        /// Original URL retained for a future repair worker.
        source_url: Url,
    },
}

/// Counts returned by one cache-maintenance pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AssetMaintenanceResult {
    /// Blobs whose data was evicted by idle age.
    pub stale_blobs: u64,
    /// Blobs whose data was evicted to satisfy aggregate size.
    pub size_evicted_blobs: u64,
    /// URL/version rows deleted after losing every article relationship.
    pub orphan_records: u64,
    /// Shared blobs deleted after losing every URL/version row.
    pub orphan_blobs: u64,
}

impl AssetMaintenanceResult {
    /// Returns whether the pass changed persistent cache state.
    pub const fn changed(self) -> bool {
        self.stale_blobs > 0
            || self.size_evicted_blobs > 0
            || self.orphan_records > 0
            || self.orphan_blobs > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_uses_documented_database_cache_limits() {
        let policy = AssetCachePolicy::default();

        assert_eq!(policy.max_cache_size_bytes(), 5_000_000_000);
        assert_eq!(policy.max_age(), Duration::from_secs(2_592_000));
        assert_eq!(policy.max_asset_size_bytes(), 10_000_000);
        assert_eq!(policy.max_count_per_article(), 100);
    }

    #[test]
    fn asset_input_precomputes_the_raw_byte_checksum() {
        let input = AssetInput::new(
            Url::parse("https://cdn.example/image.png").unwrap(),
            Url::parse("https://cdn.example/image.png").unwrap(),
            "image/png".to_owned(),
            b"asset-bytes".to_vec(),
            0,
            Url::parse("https://mp.weixin.qq.com/s/article").unwrap(),
            Some("https://mp.weixin.qq.com".to_owned()),
            None,
        );

        assert_eq!(
            input.checksum(),
            "c092df87ad240efa9f032f792b57f5d3812a833b47de33172f59cf70ee2f01c4"
        );
        assert!(input.checksum_matches_bytes());
    }

    #[test]
    fn asset_input_detects_raw_bytes_mutated_after_construction() {
        let mut input = AssetInput::new(
            Url::parse("https://cdn.example/image.png").unwrap(),
            Url::parse("https://cdn.example/image.png").unwrap(),
            "image/png".to_owned(),
            b"asset-bytes".to_vec(),
            0,
            Url::parse("https://mp.weixin.qq.com/s/article").unwrap(),
            Some("https://mp.weixin.qq.com".to_owned()),
            None,
        );

        input.bytes = b"changed-bytes".to_vec();

        assert!(!input.checksum_matches_bytes());
    }

    #[test]
    fn policy_rejects_zero_required_limits_and_timeout_mismatch() {
        assert_eq!(
            AssetCachePolicy::new(
                1,
                Duration::from_secs(1),
                0,
                1,
                1,
                Duration::from_secs(1),
                Duration::from_secs(1),
                0,
            ),
            Err(AssetCachePolicyError::ZeroLimit {
                field: "max_asset_size_bytes"
            })
        );

        assert_eq!(
            AssetCachePolicy::new(
                1,
                Duration::from_secs(1),
                1,
                1,
                1,
                Duration::from_secs(1),
                Duration::from_secs(2),
                0,
            ),
            Err(AssetCachePolicyError::TimeoutExceedsArticleBudget)
        );
    }

    #[test]
    fn repair_policy_rejects_zero_global_pending_limit() {
        assert_eq!(
            AssetRepairPolicy::new(0, 0, Duration::from_secs(1), 1),
            Err(AssetRepairPolicyError::ZeroLimit {
                field: "max_pending"
            })
        );
    }

    #[test]
    fn repair_policy_rejects_public_limit_above_global_limit() {
        assert_eq!(
            AssetRepairPolicy::new(1, 2, Duration::from_secs(1), 1),
            Err(AssetRepairPolicyError::PublicLimitExceedsGlobal)
        );
    }

    #[test]
    fn repair_policy_rejects_zero_requeue_delay() {
        assert_eq!(
            AssetRepairPolicy::new(1, 0, Duration::ZERO, 1),
            Err(AssetRepairPolicyError::ZeroDuration)
        );
    }

    #[test]
    fn repair_policy_rejects_zero_attempt_limit() {
        assert_eq!(
            AssetRepairPolicy::new(1, 0, Duration::from_secs(1), 0),
            Err(AssetRepairPolicyError::ZeroLimit {
                field: "max_attempts"
            })
        );
    }
}
