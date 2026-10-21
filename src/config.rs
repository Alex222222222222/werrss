//! Typed environment-only runtime configuration.
//!
//! This is the second implemented slice of the Rust service. Version one reads
//! configuration from process environment variables only. There is deliberately
//! no application configuration file and no command-line override layer.
//!
//! The raw environment representation is private. Callers receive a validated
//! [`AppConfig`] containing parsed URLs, durations, role settings, browser
//! settings, pacing, quiet hours, asset-storage policy, and secret wrappers.
//! Required connection and encryption values fail startup; optional operational
//! values have documented defaults.
//!
//! Environment names are grouped by concern:
//!
//! ```text
//! DATABASE_URL
//! DATABASE_POOL_MIN_CONNECTIONS / DATABASE_POOL_MAX_CONNECTIONS
//! WEBDRIVER_URL / BROWSER_ENGINE / WORKER_CONCURRENCY
//! BROWSER_USER_AGENT / BROWSER_LOCALE / BROWSER_VIEWPORT_WIDTH /
//! BROWSER_VIEWPORT_HEIGHT / BROWSER_EXTRA_ARGS
//! WEREAD_ACCOUNT_ID / WEREAD_ARTICLE_LIST_URL
//! APP_INSTANCE_ID / HTTP_BIND / HTTP_PORT / APP_ROLES / LOG_LEVEL
//! APP_TIMEZONE / QUIET_HOURS_START / QUIET_HOURS_END
//! JOB_POLL_SECONDS / JOB_LEASE_SECONDS / JOB_HEARTBEAT_SECONDS /
//! JOB_EXECUTION_TIMEOUT_SECONDS / JOB_MAX_ATTEMPTS
//! ACCOUNT_LEASE_SECONDS / ACCOUNT_HEARTBEAT_SECONDS
//! SOURCE_FAILURE_COOLDOWN_SECONDS
//! RSS_CACHE_TTL_SECONDS / RSS_STALE_WHILE_REVALIDATE_SECONDS /
//! RSS_CACHE_MISS_WAIT_MS / SERVER_ROOT_URL
//! FEED_BUILD_LEASE_SECONDS / FEED_BUILD_HEARTBEAT_SECONDS
//! PACING_* / SCROLL_*
//! ASSET_ARCHIVE_BACKEND / ASSET_LOCAL_ROOT / ASSET_CACHE_MAX_SIZE_MB /
//! ASSET_S3_BUCKET / ASSET_S3_ENDPOINT / ASSET_S3_REGION /
//! ASSET_S3_ACCESS_KEY_ID / ASSET_S3_SECRET_ACCESS_KEY /
//! ASSET_OSS_* / ASSET_GCS_* / ASSET_AZURE_* /
//! ASSET_FTP_* / ASSET_SFTP_* / ASSET_WEBDAV_* /
//! ASSET_CACHE_MAX_AGE_DAYS / ASSET_MAX_SIZE_MB /
//! ASSET_USE_ABSOLUTE_URLS /
//! ASSET_MAX_COUNT_PER_ARTICLE / ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB /
//! ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS / ASSET_FETCH_TIMEOUT_SECONDS /
//! ASSET_MAX_REDIRECTS / ASSET_REPAIR_MAX_PENDING /
//! ASSET_REPAIR_PUBLIC_MAX_PENDING / ASSET_REPAIR_REQUEUE_SECONDS /
//! ASSET_REPAIR_MAX_ATTEMPTS
//! ADMIN_ENABLED / ADMIN_USERNAME / ADMIN_PASSWORD / SESSION_SIGNING_KEY /
//! CREDENTIAL_ENCRYPTION_KEY
//! ```
//!
//! `APP_INSTANCE_ID` is optional for local use; when omitted, a random UUID is
//! generated for the process so replicas do not share lease ownership. The
//! lease must exceed the heartbeat interval plus the maximum page-operation
//! duration. Pacing delays and page-operation duration are also capped by the
//! loader before they can be converted to runtime durations. Scroll action and
//! pixel limits are bounded before they can reach the browser adapter.
//!
//! PostgreSQL SSL mode, CA certificates, client certificates, private keys,
//! passwords, and other connection options belong in `DATABASE_URL` (including
//! its query parameters) and are passed through to SQLx. This module should not
//! introduce separate PostgreSQL SSL or certificate environment variables.
//!
//! Kubernetes ConfigMaps and Secrets may inject these values, but the
//! application still consumes them through its environment. Diagnostics must
//! expose variable names and validation failures, never secret contents.
//!
//! Unknown settings under the documented application-owned prefixes are
//! rejected, while unrelated process variables remain ignored. The two legacy
//! `ARCHIVE_*` names are rejected with a migration hint instead of being
//! silently ignored. This keeps a misspelled deployment from appearing to
//! start with a different policy than the operator configured.

use std::{env, str::FromStr, time::Duration};

use base64::Engine as _;
use chrono::NaiveTime;
use chrono_tz::Tz;
use secrecy::SecretString;
use serde::Deserialize;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::domain::pacing::{
    DelayDistribution, PacingError, PacingPolicy, QuietHours, MAX_SCROLL_PIXELS, MAX_SCROLL_STEPS,
};
use crate::{
    archive::asset_store::{
        AssetCachePolicy, AssetCachePolicyError, AssetRepairPolicy, AssetRepairPolicyError,
    },
    domain::credentials::WeReadAccountId,
};

const MAX_CONFIGURED_DELAY_MS: f64 = 300_000.0;
const MAX_PAGE_OPERATION_SECONDS: u64 = 3_600;
const MAX_JOB_EXECUTION_TIMEOUT_SECONDS: u64 = 24 * 60 * 60;
const MAX_WORKER_CONCURRENCY: u32 = 1_024;
const MAX_SOURCE_FAILURE_COOLDOWN_SECONDS: u64 = 7 * 24 * 60 * 60;
const MAX_STALE_WHILE_REVALIDATE_SECONDS: u64 = 24 * 60 * 60;
const MAX_CACHE_MISS_WAIT_MS: u64 = 60_000;
const DEFAULT_BROWSER_LOCALE: &str = "zh-CN";
const DEFAULT_BROWSER_VIEWPORT_WIDTH: u32 = 1_280;
const DEFAULT_BROWSER_VIEWPORT_HEIGHT: u32 = 2_000;
const MAX_BROWSER_VIEWPORT_DIMENSION: u32 = 8_192;
const MAX_BROWSER_USER_AGENT_LENGTH: usize = 512;
const MAX_BROWSER_EXTRA_ARGS: usize = 32;

const KNOWN_ENVIRONMENT_VARIABLES: &[&str] = &[
    "DATABASE_URL",
    "DATABASE_POOL_MIN_CONNECTIONS",
    "DATABASE_POOL_MAX_CONNECTIONS",
    "WEBDRIVER_URL",
    "BROWSER_ENGINE",
    "BROWSER_USER_AGENT",
    "BROWSER_LOCALE",
    "BROWSER_VIEWPORT_WIDTH",
    "BROWSER_VIEWPORT_HEIGHT",
    "BROWSER_EXTRA_ARGS",
    "WEREAD_ACCOUNT_ID",
    "WEREAD_ARTICLE_LIST_URL",
    "WORKER_CONCURRENCY",
    "APP_INSTANCE_ID",
    "HTTP_BIND",
    "HTTP_PORT",
    "APP_ROLES",
    "LOG_LEVEL",
    "APP_TIMEZONE",
    "QUIET_HOURS_START",
    "QUIET_HOURS_END",
    "JOB_POLL_SECONDS",
    "JOB_LEASE_SECONDS",
    "JOB_HEARTBEAT_SECONDS",
    "JOB_EXECUTION_TIMEOUT_SECONDS",
    "JOB_MAX_ATTEMPTS",
    "ACCOUNT_LEASE_SECONDS",
    "ACCOUNT_HEARTBEAT_SECONDS",
    "SOURCE_FAILURE_COOLDOWN_SECONDS",
    "RSS_CACHE_TTL_SECONDS",
    "RSS_STALE_WHILE_REVALIDATE_SECONDS",
    "RSS_CACHE_MISS_WAIT_MS",
    "SERVER_ROOT_URL",
    "FEED_BUILD_LEASE_SECONDS",
    "FEED_BUILD_HEARTBEAT_SECONDS",
    "PACING_REQUEST_MEAN_MS",
    "PACING_REQUEST_STDDEV_MS",
    "PACING_REQUEST_MIN_MS",
    "PACING_REQUEST_MAX_MS",
    "PACING_PAGE_NAVIGATION_MEAN_MS",
    "PACING_PAGE_NAVIGATION_STDDEV_MS",
    "PACING_PAGE_NAVIGATION_MIN_MS",
    "PACING_PAGE_NAVIGATION_MAX_MS",
    "PACING_PAGE_ACTION_MEAN_MS",
    "PACING_PAGE_ACTION_STDDEV_MS",
    "PACING_PAGE_ACTION_MIN_MS",
    "PACING_PAGE_ACTION_MAX_MS",
    "PACING_SCROLL_SETTLE_MEAN_MS",
    "PACING_SCROLL_SETTLE_STDDEV_MS",
    "PACING_SCROLL_SETTLE_MIN_MS",
    "PACING_SCROLL_SETTLE_MAX_MS",
    "SCROLL_MAX_STEPS",
    "SCROLL_MAX_PIXELS",
    "SCROLL_MAX_OPERATION_SECONDS",
    "ASSET_ARCHIVE_BACKEND",
    "ASSET_LOCAL_ROOT",
    "ASSET_S3_BUCKET",
    "ASSET_S3_ENDPOINT",
    "ASSET_S3_REGION",
    "ASSET_S3_ACCESS_KEY_ID",
    "ASSET_S3_SECRET_ACCESS_KEY",
    "ASSET_OSS_BUCKET",
    "ASSET_OSS_ENDPOINT",
    "ASSET_OSS_ADDRESSING_STYLE",
    "ASSET_OSS_VERSIONING",
    "ASSET_OSS_ACCESS_KEY_ID",
    "ASSET_OSS_ACCESS_KEY_SECRET",
    "ASSET_OSS_SECURITY_TOKEN",
    "ASSET_OSS_SKIP_SIGNATURE",
    "ASSET_GCS_BUCKET",
    "ASSET_GCS_ENDPOINT",
    "ASSET_GCS_CREDENTIAL",
    "ASSET_GCS_SKIP_SIGNATURE",
    "ASSET_AZURE_CONTAINER",
    "ASSET_AZURE_ENDPOINT",
    "ASSET_AZURE_ACCOUNT_NAME",
    "ASSET_AZURE_ACCOUNT_KEY",
    "ASSET_AZURE_SKIP_SIGNATURE",
    "ASSET_FTP_ENDPOINT",
    "ASSET_FTP_ROOT",
    "ASSET_FTP_USER",
    "ASSET_FTP_PASSWORD",
    "ASSET_SFTP_ENDPOINT",
    "ASSET_SFTP_ROOT",
    "ASSET_SFTP_USER",
    "ASSET_SFTP_KEY",
    "ASSET_WEBDAV_ENDPOINT",
    "ASSET_WEBDAV_ROOT",
    "ASSET_WEBDAV_USER",
    "ASSET_WEBDAV_PASSWORD",
    "ASSET_CACHE_MAX_SIZE_MB",
    "ASSET_CACHE_MAX_AGE_DAYS",
    "ASSET_USE_ABSOLUTE_URLS",
    "ASSET_MAX_SIZE_MB",
    "ASSET_MAX_COUNT_PER_ARTICLE",
    "ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB",
    "ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS",
    "ASSET_FETCH_TIMEOUT_SECONDS",
    "ASSET_MAX_REDIRECTS",
    "ASSET_REPAIR_MAX_PENDING",
    "ASSET_REPAIR_PUBLIC_MAX_PENDING",
    "ASSET_REPAIR_REQUEUE_SECONDS",
    "ASSET_REPAIR_MAX_ATTEMPTS",
    "ADMIN_ENABLED",
    "ADMIN_USERNAME",
    "ADMIN_PASSWORD",
    "SESSION_SIGNING_KEY",
    "CREDENTIAL_ENCRYPTION_KEY",
];

const APPLICATION_ENVIRONMENT_PREFIXES: &[&str] = &[
    "APP_",
    "HTTP_",
    "WORKER_",
    "JOB_",
    "ACCOUNT_",
    "SOURCE_",
    "RSS_",
    "FEED_",
    "PACING_",
    "SCROLL_",
    "ASSET_",
    "ADMIN_",
    "SESSION_",
    "CREDENTIAL_",
    "QUIET_",
    "WEBDRIVER_",
    "BROWSER_",
    "WEREAD_",
    "DATABASE_POOL_",
    "LOG_",
];

const LEGACY_ENVIRONMENT_VARIABLES: &[&str] = &["ARCHIVE_BACKEND", "ARCHIVE_LOCAL_PATH"];
const IGNORED_RUNTIME_ENVIRONMENT_VARIABLES: &[&str] = &["HTTP_PROXY"];

/// A component that may be enabled in one process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AppRole {
    /// Serve feed, health, and optionally administrative HTTP routes.
    Api,
    /// Enqueue due source work and recover abandoned jobs.
    Scheduler,
    /// Claim and execute browser-backed or database-only jobs.
    Worker,
}

/// Validated set of process roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppRoles(u8);

impl AppRoles {
    const API: u8 = 0b001;
    const SCHEDULER: u8 = 0b010;
    const WORKER: u8 = 0b100;

    /// Returns the role set containing every component.
    pub const fn all() -> Self {
        Self(Self::API | Self::SCHEDULER | Self::WORKER)
    }

    /// Parses `all` or a comma-separated role set.
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(ConfigError::InvalidValue {
                variable: "APP_ROLES",
                reason: "must contain api, scheduler, worker, or all",
            });
        }
        if value.eq_ignore_ascii_case("all") {
            return Ok(Self::all());
        }

        let mut bits = 0;
        for role in value.split(',') {
            match role.trim().to_ascii_lowercase().as_str() {
                "api" => bits |= Self::API,
                "scheduler" => bits |= Self::SCHEDULER,
                "worker" => bits |= Self::WORKER,
                _ => {
                    return Err(ConfigError::InvalidValue {
                        variable: "APP_ROLES",
                        reason: "must contain only api, scheduler, and worker",
                    });
                }
            }
        }

        if bits == 0 {
            return Err(ConfigError::InvalidValue {
                variable: "APP_ROLES",
                reason: "must contain at least one role",
            });
        }
        Ok(Self(bits))
    }

    /// Reports whether the process should construct the requested component.
    pub const fn contains(self, role: AppRole) -> bool {
        let bit = match role {
            AppRole::Api => Self::API,
            AppRole::Scheduler => Self::SCHEDULER,
            AppRole::Worker => Self::WORKER,
        };
        self.0 & bit != 0
    }
}

impl Default for AppRoles {
    fn default() -> Self {
        Self(Self::API)
    }
}

/// Asset storage policy. Binary asset archiving is disabled by default; the
/// first enabled implementation stores metadata and bytes in PostgreSQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetArchiveConfig {
    /// Keep approved external asset URLs and do not persist binary assets.
    Disabled,
    /// Store binary assets and URL metadata in PostgreSQL.
    Database {
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
    /// Store binary assets through OpenDAL's local filesystem service.
    Local {
        /// Root directory used by the filesystem backend.
        root: String,
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
    /// Store binary assets through an OpenDAL S3-compatible service.
    S3 {
        /// S3-compatible connection settings.
        storage: AssetS3Config,
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
    /// Store binary assets through an OpenDAL Aliyun OSS service.
    Oss {
        /// Aliyun OSS connection settings.
        storage: AssetOssConfig,
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
    /// Store binary assets through an OpenDAL Google Cloud Storage service.
    Gcs {
        /// Google Cloud Storage connection settings.
        storage: AssetGcsConfig,
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
    /// Store binary assets through an OpenDAL Azure Blob service.
    Azure {
        /// Azure Blob connection settings.
        storage: AssetAzureConfig,
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
    /// Store binary assets through an FTP server.
    Ftp {
        /// FTP connection settings.
        storage: AssetFtpConfig,
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
    /// Store binary assets through an SFTP server.
    Sftp {
        /// SFTP connection settings.
        storage: AssetSftpConfig,
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
    /// Store binary assets through a WebDAV server.
    Webdav {
        /// WebDAV connection settings.
        storage: AssetWebdavConfig,
        /// Limits applied to acquisition, storage, and maintenance.
        policy: AssetCachePolicy,
        /// Cluster-wide admission and retry limits for public cache misses.
        repair_policy: AssetRepairPolicy,
    },
}

/// Credentials and endpoint settings for an S3-compatible asset backend.
#[derive(Clone)]
pub struct AssetS3Config {
    bucket: String,
    endpoint: Option<String>,
    region: String,
    access_key_id: secrecy::SecretString,
    secret_access_key: secrecy::SecretString,
}

impl PartialEq for AssetS3Config {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.endpoint == other.endpoint
            && self.region == other.region
            && secrecy::ExposeSecret::expose_secret(&self.access_key_id)
                == secrecy::ExposeSecret::expose_secret(&other.access_key_id)
            && secrecy::ExposeSecret::expose_secret(&self.secret_access_key)
                == secrecy::ExposeSecret::expose_secret(&other.secret_access_key)
    }
}

impl Eq for AssetS3Config {}

impl std::fmt::Debug for AssetS3Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssetS3Config")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

impl AssetS3Config {
    /// Returns the configured bucket.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the optional provider endpoint.
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// Returns the signing region.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Returns the access key for backend construction.
    pub fn access_key_id(&self) -> &str {
        secrecy::ExposeSecret::expose_secret(&self.access_key_id)
    }

    /// Returns the secret key for backend construction.
    pub fn secret_access_key(&self) -> &str {
        secrecy::ExposeSecret::expose_secret(&self.secret_access_key)
    }
}

/// Credentials and endpoint settings for an Aliyun OSS asset backend.
#[derive(Clone)]
pub struct AssetOssConfig {
    bucket: String,
    endpoint: String,
    addressing_style: String,
    access_key_id: Option<secrecy::SecretString>,
    access_key_secret: Option<secrecy::SecretString>,
    security_token: Option<secrecy::SecretString>,
    skip_signature: bool,
}

impl PartialEq for AssetOssConfig {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.endpoint == other.endpoint
            && self.addressing_style == other.addressing_style
            && self.skip_signature == other.skip_signature
            && match (&self.access_key_id, &other.access_key_id) {
                (Some(left), Some(right)) => {
                    secrecy::ExposeSecret::expose_secret(left)
                        == secrecy::ExposeSecret::expose_secret(right)
                }
                (None, None) => true,
                _ => false,
            }
            && match (&self.access_key_secret, &other.access_key_secret) {
                (Some(left), Some(right)) => {
                    secrecy::ExposeSecret::expose_secret(left)
                        == secrecy::ExposeSecret::expose_secret(right)
                }
                (None, None) => true,
                _ => false,
            }
            && match (&self.security_token, &other.security_token) {
                (Some(left), Some(right)) => {
                    secrecy::ExposeSecret::expose_secret(left)
                        == secrecy::ExposeSecret::expose_secret(right)
                }
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for AssetOssConfig {}

impl std::fmt::Debug for AssetOssConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssetOssConfig")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("addressing_style", &self.addressing_style)
            .field("access_key_id", &"<redacted>")
            .field("access_key_secret", &"<redacted>")
            .field("security_token", &"<redacted>")
            .field("skip_signature", &self.skip_signature)
            .finish()
    }
}

impl AssetOssConfig {
    /// Returns the configured bucket.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the configured OSS endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the OSS addressing style.
    pub fn addressing_style(&self) -> &str {
        &self.addressing_style
    }

    /// Returns the access key ID, when configured.
    pub fn access_key_id(&self) -> Option<&str> {
        self.access_key_id
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret)
    }

    /// Returns the access key secret, when configured.
    pub fn access_key_secret(&self) -> Option<&str> {
        self.access_key_secret
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret)
    }

    /// Returns the temporary security token, when configured.
    pub fn security_token(&self) -> Option<&str> {
        self.security_token
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret)
    }

    /// Returns whether OpenDAL should send unsigned requests.
    pub const fn skip_signature(&self) -> bool {
        self.skip_signature
    }
}

/// Credentials and endpoint settings for a Google Cloud Storage asset backend.
#[derive(Clone)]
pub struct AssetGcsConfig {
    bucket: String,
    endpoint: Option<String>,
    credential: Option<secrecy::SecretString>,
    skip_signature: bool,
}

impl PartialEq for AssetGcsConfig {
    fn eq(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.endpoint == other.endpoint
            && self.skip_signature == other.skip_signature
            && match (&self.credential, &other.credential) {
                (Some(left), Some(right)) => {
                    secrecy::ExposeSecret::expose_secret(left)
                        == secrecy::ExposeSecret::expose_secret(right)
                }
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for AssetGcsConfig {}

impl std::fmt::Debug for AssetGcsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssetGcsConfig")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .field("credential", &"<redacted>")
            .finish()
    }
}

impl AssetGcsConfig {
    /// Returns the configured bucket.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the optional provider endpoint.
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// Returns the base64-encoded Google credential passed to OpenDAL, when configured.
    pub fn credential(&self) -> Option<&str> {
        self.credential
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret)
    }

    /// Returns whether OpenDAL should send unsigned requests.
    pub const fn skip_signature(&self) -> bool {
        self.skip_signature
    }
}

/// Credentials and endpoint settings for an Azure Blob asset backend.
#[derive(Clone)]
pub struct AssetAzureConfig {
    container: String,
    endpoint: String,
    account_name: Option<secrecy::SecretString>,
    account_key: Option<secrecy::SecretString>,
    skip_signature: bool,
}

impl PartialEq for AssetAzureConfig {
    fn eq(&self, other: &Self) -> bool {
        self.container == other.container
            && self.endpoint == other.endpoint
            && self.skip_signature == other.skip_signature
            && match (&self.account_name, &other.account_name) {
                (Some(left), Some(right)) => {
                    secrecy::ExposeSecret::expose_secret(left)
                        == secrecy::ExposeSecret::expose_secret(right)
                }
                (None, None) => true,
                _ => false,
            }
            && match (&self.account_key, &other.account_key) {
                (Some(left), Some(right)) => {
                    secrecy::ExposeSecret::expose_secret(left)
                        == secrecy::ExposeSecret::expose_secret(right)
                }
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for AssetAzureConfig {}

impl std::fmt::Debug for AssetAzureConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssetAzureConfig")
            .field("container", &self.container)
            .field("endpoint", &self.endpoint)
            .field("account_name", &"<redacted>")
            .field("account_key", &"<redacted>")
            .field("skip_signature", &self.skip_signature)
            .finish()
    }
}

impl AssetAzureConfig {
    /// Returns the configured container.
    pub fn container(&self) -> &str {
        &self.container
    }

    /// Returns the configured service endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the account name used to sign requests, when configured.
    pub fn account_name(&self) -> Option<&str> {
        self.account_name
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret)
    }

    /// Returns the account key used to sign requests, when configured.
    pub fn account_key(&self) -> Option<&str> {
        self.account_key
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret)
    }

    /// Returns whether OpenDAL should send unsigned requests.
    pub const fn skip_signature(&self) -> bool {
        self.skip_signature
    }
}

/// Endpoint and optional credentials for an FTP asset backend.
#[derive(Clone)]
pub struct AssetFtpConfig {
    endpoint: String,
    root: Option<String>,
    user: Option<SecretString>,
    password: Option<SecretString>,
}
impl PartialEq for AssetFtpConfig {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
            && self.root == other.root
            && self.user.as_ref().map(secrecy::ExposeSecret::expose_secret)
                == other
                    .user
                    .as_ref()
                    .map(secrecy::ExposeSecret::expose_secret)
            && self
                .password
                .as_ref()
                .map(secrecy::ExposeSecret::expose_secret)
                == other
                    .password
                    .as_ref()
                    .map(secrecy::ExposeSecret::expose_secret)
    }
}
impl Eq for AssetFtpConfig {}

impl std::fmt::Debug for AssetFtpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssetFtpConfig")
            .field("endpoint", &"<redacted>")
            .field("root", &"<redacted>")
            .field("user", &"<redacted>")
            .field("password", &"<redacted>")
            .finish()
    }
}
impl AssetFtpConfig {
    /// Returns the FTP or FTPS endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    /// Returns the optional remote root.
    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }
    /// Returns the optional FTP username.
    pub fn user(&self) -> Option<&str> {
        self.user.as_ref().map(secrecy::ExposeSecret::expose_secret)
    }
    /// Returns the optional FTP password.
    pub fn password(&self) -> Option<&str> {
        self.password
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret)
    }
}

/// Endpoint and private-key settings for an SFTP asset backend.
#[derive(Clone)]
pub struct AssetSftpConfig {
    endpoint: String,
    root: Option<String>,
    user: SecretString,
    key: SecretString,
}
impl PartialEq for AssetSftpConfig {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
            && self.root == other.root
            && secrecy::ExposeSecret::expose_secret(&self.user)
                == secrecy::ExposeSecret::expose_secret(&other.user)
            && secrecy::ExposeSecret::expose_secret(&self.key)
                == secrecy::ExposeSecret::expose_secret(&other.key)
    }
}
impl Eq for AssetSftpConfig {}
impl std::fmt::Debug for AssetSftpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssetSftpConfig")
            .field("endpoint", &"<redacted>")
            .field("root", &"<redacted>")
            .field("user", &"<redacted>")
            .field("key", &"<redacted>")
            .finish()
    }
}
impl AssetSftpConfig {
    /// Returns the SFTP endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    /// Returns the optional remote root.
    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }
    /// Returns the required SSH username.
    pub fn user(&self) -> &str {
        secrecy::ExposeSecret::expose_secret(&self.user)
    }
    /// Returns the mounted private-key file path.
    pub fn key(&self) -> &str {
        secrecy::ExposeSecret::expose_secret(&self.key)
    }
}

/// Endpoint and optional credentials for a WebDAV asset backend.
#[derive(Clone)]
pub struct AssetWebdavConfig {
    endpoint: String,
    root: Option<String>,
    user: Option<SecretString>,
    password: Option<SecretString>,
}
impl PartialEq for AssetWebdavConfig {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
            && self.root == other.root
            && self.user.as_ref().map(secrecy::ExposeSecret::expose_secret)
                == other
                    .user
                    .as_ref()
                    .map(secrecy::ExposeSecret::expose_secret)
            && self
                .password
                .as_ref()
                .map(secrecy::ExposeSecret::expose_secret)
                == other
                    .password
                    .as_ref()
                    .map(secrecy::ExposeSecret::expose_secret)
    }
}
impl Eq for AssetWebdavConfig {}
impl std::fmt::Debug for AssetWebdavConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssetWebdavConfig")
            .field("endpoint", &"<redacted>")
            .field("root", &"<redacted>")
            .field("user", &"<redacted>")
            .field("password", &"<redacted>")
            .finish()
    }
}
impl AssetWebdavConfig {
    /// Returns the HTTP or HTTPS WebDAV endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    /// Returns the optional remote root.
    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }
    /// Returns the optional WebDAV username.
    pub fn user(&self) -> Option<&str> {
        self.user.as_ref().map(secrecy::ExposeSecret::expose_secret)
    }
    pub fn password(&self) -> Option<&str> {
        self.password
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret)
    }
}

impl AssetArchiveConfig {
    /// Returns the FTP settings when FTP storage is enabled.
    pub fn ftp_storage(&self) -> Option<&AssetFtpConfig> {
        match self {
            Self::Ftp { storage, .. } => Some(storage),
            _ => None,
        }
    }

    /// Returns the SFTP settings when SFTP storage is enabled.
    pub fn sftp_storage(&self) -> Option<&AssetSftpConfig> {
        match self {
            Self::Sftp { storage, .. } => Some(storage),
            _ => None,
        }
    }

    /// Returns the WebDAV settings when WebDAV storage is enabled.
    pub fn webdav_storage(&self) -> Option<&AssetWebdavConfig> {
        match self {
            Self::Webdav { storage, .. } => Some(storage),
            _ => None,
        }
    }

    /// Returns the database policy when binary caching is enabled.
    pub const fn database_policy(&self) -> Option<AssetCachePolicy> {
        match self {
            Self::Disabled => None,
            Self::Database { policy, .. }
            | Self::Local { policy, .. }
            | Self::S3 { policy, .. }
            | Self::Oss { policy, .. }
            | Self::Gcs { policy, .. }
            | Self::Azure { policy, .. }
            | Self::Ftp { policy, .. }
            | Self::Sftp { policy, .. }
            | Self::Webdav { policy, .. } => Some(*policy),
        }
    }

    /// Returns the repair admission policy when binary caching is enabled.
    pub const fn database_repair_policy(&self) -> Option<AssetRepairPolicy> {
        match self {
            Self::Disabled => None,
            Self::Database { repair_policy, .. }
            | Self::Local { repair_policy, .. }
            | Self::S3 { repair_policy, .. }
            | Self::Oss { repair_policy, .. }
            | Self::Gcs { repair_policy, .. }
            | Self::Azure { repair_policy, .. }
            | Self::Ftp { repair_policy, .. }
            | Self::Sftp { repair_policy, .. }
            | Self::Webdav { repair_policy, .. } => Some(*repair_policy),
        }
    }

    /// Returns the configured local filesystem root, when the local backend is enabled.
    pub fn local_root(&self) -> Option<&str> {
        match self {
            Self::Local { root, .. } => Some(root),
            _ => None,
        }
    }

    /// Returns the configured S3-compatible storage settings.
    pub fn s3_storage(&self) -> Option<&AssetS3Config> {
        match self {
            Self::S3 { storage, .. } => Some(storage),
            _ => None,
        }
    }

    /// Returns the configured Aliyun OSS storage settings.
    pub fn oss_storage(&self) -> Option<&AssetOssConfig> {
        match self {
            Self::Oss { storage, .. } => Some(storage),
            _ => None,
        }
    }

    /// Returns the configured Google Cloud Storage settings.
    pub fn gcs_storage(&self) -> Option<&AssetGcsConfig> {
        match self {
            Self::Gcs { storage, .. } => Some(storage),
            _ => None,
        }
    }

    /// Returns the configured Azure Blob storage settings.
    pub fn azure_storage(&self) -> Option<&AssetAzureConfig> {
        match self {
            Self::Azure { storage, .. } => Some(storage),
            _ => None,
        }
    }
}

/// Browser implementation selected for a WebDriver sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserEngine {
    /// Chromium controlled through ChromeDriver.
    Chromium,
    /// Firefox controlled through GeckoDriver.
    Firefox,
}

impl FromStr for BrowserEngine {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "chromium" | "chrome" => Ok(Self::Chromium),
            "firefox" | "firefox-esr" => Ok(Self::Firefox),
            _ => Err(ConfigError::InvalidValue {
                variable: "BROWSER_ENGINE",
                reason: "expected chromium or firefox",
            }),
        }
    }
}

/// Errors returned while loading or validating environment configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// `envy` could not deserialize an environment value into its raw type.
    #[error("invalid environment configuration: {0}")]
    Environment(#[from] envy::Error),
    /// A required environment variable is absent or empty.
    #[error("environment variable {variable} is required")]
    Missing { variable: &'static str },
    /// An environment variable has a semantically invalid value.
    #[error("environment variable {variable} is invalid: {reason}")]
    InvalidValue {
        variable: &'static str,
        reason: &'static str,
    },
    /// An unknown setting used an application-owned environment prefix.
    #[error("unknown application environment variable {variable}")]
    UnknownVariable {
        /// Unknown environment variable name.
        variable: String,
    },
    /// Quiet-hours configuration is only partially supplied.
    #[error("QUIET_HOURS_START and QUIET_HOURS_END must be set together")]
    IncompleteQuietHours,
    /// A pacing or scroll policy failed domain validation.
    #[error("invalid pacing policy: {0}")]
    Pacing(#[from] PacingError),
}

/// Validated configuration used by future runtime components.
#[derive(Debug)]
pub struct AppConfig {
    /// Components constructed by this process.
    pub roles: AppRoles,
    /// Maximum severity emitted by the process logger.
    pub log_level: tracing::level_filters::LevelFilter,
    /// Maximum number of jobs executed concurrently by a worker process.
    pub worker_concurrency: u32,
    /// PostgreSQL URL, retained as a secret because it may contain a password.
    pub database_url: SecretString,
    /// Minimum number of PostgreSQL connections kept in the pool.
    pub database_pool_min_connections: u32,
    /// Maximum number of PostgreSQL connections allowed in the pool.
    pub database_pool_max_connections: u32,
    /// Internal WebDriver endpoint.
    pub webdriver_url: Url,
    /// Browser implementation expected behind the WebDriver endpoint.
    pub browser_engine: BrowserEngine,
    /// Optional fixed browser User-Agent used for controlled diagnostics.
    pub browser_user_agent: Option<String>,
    /// Locale passed to the browser profile.
    pub browser_locale: String,
    /// Requested browser viewport width in CSS pixels.
    pub browser_viewport_width: u32,
    /// Requested browser viewport height in CSS pixels.
    pub browser_viewport_height: u32,
    /// Additional browser arguments, one argument per whitespace-separated
    /// token. These are operator-controlled and are applied only to the
    /// selected WebDriver browser.
    pub browser_extra_args: Vec<String>,
    /// Stable WeRead account identity used by sources without an override.
    pub weread_account_id: Option<WeReadAccountId>,
    /// HTTPS endpoint used by the authenticated WeRead article-list adapter.
    pub weread_article_list_url: Url,
    /// Stable identity used in PostgreSQL job leases.
    pub instance_id: String,
    /// HTTP bind address or hostname.
    pub http_bind: String,
    /// HTTP listen port.
    pub http_port: u16,
    /// Timezone used by quiet-hours policy and browser setup.
    pub timezone: Tz,
    /// Optional local-time window during which upstream work is paused.
    pub quiet_hours: Option<QuietHours>,
    /// Poll interval for the job enqueue/recovery loops.
    pub job_poll_interval: Duration,
    /// Duration for which a claimed job lease remains valid.
    pub job_lease: Duration,
    /// Maximum interval between worker lease heartbeats.
    pub job_heartbeat: Duration,
    /// Absolute wall-clock limit for one claimed job execution.
    pub job_execution_timeout: Duration,
    /// Maximum attempts for retryable jobs.
    pub job_max_attempts: u32,
    /// Duration for which one authenticated account may be leased.
    pub account_lease: Duration,
    /// Maximum interval between authenticated-account lease heartbeats.
    pub account_heartbeat: Duration,
    /// Cooldown applied after a source failure before automatic re-enqueueing.
    pub source_failure_cooldown: Duration,
    /// Freshness period for persisted RSS XML.
    pub rss_cache_ttl: Duration,
    /// Additional period for serving stale RSS while rebuilding in the
    /// background.
    pub rss_stale_while_revalidate: Duration,
    /// Bounded wait associated with a cache miss before returning retry advice.
    pub rss_cache_miss_wait: Duration,
    /// Optional canonical public URL written to generated RSS channel links.
    /// The API and worker roles require this value because no placeholder URL
    /// is safe to publish when a feed is rebuilt on demand.
    pub server_root_url: Option<Url>,
    /// Duration for which one feed rebuild may hold its distributed lease.
    pub feed_build_lease: Duration,
    /// Maximum interval between feed-build lease heartbeats.
    pub feed_build_heartbeat: Duration,
    /// Shared request/page/scroll pacing policy.
    pub pacing: PacingPolicy,
    /// Optional binary asset archive configuration.
    pub asset_archive: AssetArchiveConfig,
    /// Whether generated feeds should use absolute URLs for cached assets.
    pub asset_use_absolute_urls: bool,
    /// Whether administrative routes should be constructed.
    pub admin_enabled: bool,
    /// Administrator username, present only when administration is enabled.
    pub admin_username: Option<String>,
    /// Administrator password, present only when administration is enabled.
    pub admin_password: Option<SecretString>,
    /// Session-signing key, present only when administration is enabled.
    pub session_signing_key: Option<SecretString>,
    /// Credential-encryption key.
    pub credential_encryption_key: SecretString,
}

impl AppConfig {
    /// Loads, parses, and validates the real process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_env_iter(env::vars())
    }

    /// Loads configuration from key/value pairs.
    ///
    /// This public test seam avoids mutating process-global environment state.
    /// Production callers should normally use [`Self::from_env`].
    pub fn from_env_iter<I>(variables: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let variables: Vec<_> = variables.into_iter().collect();
        reject_legacy_variables(&variables)?;
        reject_unknown_variables(&variables)?;
        let raw: RawConfig = envy::from_iter(variables)?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let roles = AppRoles::parse(raw.app_roles.as_deref().unwrap_or("api"))?;
        let log_level = parse_log_level(raw.log_level.as_deref())?;
        let worker_concurrency = bounded_positive_u32(
            raw.worker_concurrency.unwrap_or(1),
            "WORKER_CONCURRENCY",
            MAX_WORKER_CONCURRENCY,
        )?;
        let asset_archive = asset_archive_config(&raw)?;

        let database_url = required(raw.database_url, "DATABASE_URL")?;
        let credential_encryption_key =
            required(raw.credential_encryption_key, "CREDENTIAL_ENCRYPTION_KEY")?;

        let admin_enabled = parse_bool(
            raw.admin_enabled.as_deref().unwrap_or("false"),
            "ADMIN_ENABLED",
        )?;
        let (admin_username, admin_password, session_signing_key) = if admin_enabled {
            let admin_username = required(raw.admin_username.clone(), "ADMIN_USERNAME")?;
            let admin_password = required(raw.admin_password.clone(), "ADMIN_PASSWORD")?;
            let session_signing_key =
                required(raw.session_signing_key.clone(), "SESSION_SIGNING_KEY")?;
            if session_signing_key == admin_password
                || session_signing_key == credential_encryption_key
            {
                return Err(ConfigError::InvalidValue {
                    variable: "SESSION_SIGNING_KEY",
                    reason: "must be independent from the other configured secrets",
                });
            }
            (
                Some(admin_username),
                Some(SecretString::new(admin_password.into_boxed_str())),
                Some(SecretString::new(session_signing_key.into_boxed_str())),
            )
        } else {
            (None, None, None)
        };

        let parsed_database_url =
            Url::parse(&database_url).map_err(|_| ConfigError::InvalidValue {
                variable: "DATABASE_URL",
                reason: "expected a valid PostgreSQL URL",
            })?;
        if !matches!(parsed_database_url.scheme(), "postgres" | "postgresql") {
            return Err(ConfigError::InvalidValue {
                variable: "DATABASE_URL",
                reason: "expected a postgres or postgresql URL scheme",
            });
        }

        let database_pool_min_connections = raw.database_pool_min_connections.unwrap_or(1);
        let database_pool_max_connections = raw.database_pool_max_connections.unwrap_or(10);
        if database_pool_max_connections == 0 {
            return Err(ConfigError::InvalidValue {
                variable: "DATABASE_POOL_MAX_CONNECTIONS",
                reason: "must be greater than zero",
            });
        }
        if database_pool_min_connections > database_pool_max_connections {
            return Err(ConfigError::InvalidValue {
                variable: "DATABASE_POOL_MIN_CONNECTIONS",
                reason: "must not exceed DATABASE_POOL_MAX_CONNECTIONS",
            });
        }

        let webdriver_url = raw
            .webdriver_url
            .unwrap_or_else(|| "http://webdriver:4444".to_owned())
            .parse::<Url>()
            .map_err(|_| ConfigError::InvalidValue {
                variable: "WEBDRIVER_URL",
                reason: "expected a valid http or https URL",
            })?;
        if !matches!(webdriver_url.scheme(), "http" | "https") || webdriver_url.host().is_none() {
            return Err(ConfigError::InvalidValue {
                variable: "WEBDRIVER_URL",
                reason: "expected a URL with an http/https scheme and host",
            });
        }

        let timezone_name = raw.app_timezone.unwrap_or_else(|| "UTC".to_owned());
        let timezone = timezone_name
            .parse::<Tz>()
            .map_err(|_| ConfigError::InvalidValue {
                variable: "APP_TIMEZONE",
                reason: "expected an IANA timezone name",
            })?;

        let quiet_hours = match (raw.quiet_hours_start, raw.quiet_hours_end) {
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => return Err(ConfigError::IncompleteQuietHours),
            (Some(start), Some(end)) => Some(QuietHours::new(
                timezone,
                parse_time(&start, "QUIET_HOURS_START")?,
                parse_time(&end, "QUIET_HOURS_END")?,
            )?),
        };

        let browser_engine = raw
            .browser_engine
            .unwrap_or_else(|| "chromium".to_owned())
            .parse()?;
        let browser_user_agent = parse_browser_user_agent(raw.browser_user_agent)?;
        let browser_locale = parse_browser_locale(raw.browser_locale)?;
        let browser_viewport_width = bounded_browser_viewport(
            raw.browser_viewport_width
                .unwrap_or(DEFAULT_BROWSER_VIEWPORT_WIDTH),
            "BROWSER_VIEWPORT_WIDTH",
        )?;
        let browser_viewport_height = bounded_browser_viewport(
            raw.browser_viewport_height
                .unwrap_or(DEFAULT_BROWSER_VIEWPORT_HEIGHT),
            "BROWSER_VIEWPORT_HEIGHT",
        )?;
        let browser_extra_args = parse_browser_extra_args(raw.browser_extra_args)?;
        let weread_account_id = parse_optional_weread_account_id(raw.weread_account_id)?;
        let weread_article_list_url = parse_weread_article_list_url(raw.weread_article_list_url)?;
        let http_port = raw.http_port.unwrap_or(8080);
        if http_port == 0 {
            return Err(ConfigError::InvalidValue {
                variable: "HTTP_PORT",
                reason: "must be greater than zero",
            });
        }

        let job_poll_seconds =
            positive_u64(raw.job_poll_seconds.unwrap_or(30), "JOB_POLL_SECONDS")?;
        let job_lease_seconds =
            positive_u64(raw.job_lease_seconds.unwrap_or(600), "JOB_LEASE_SECONDS")?;
        let job_heartbeat_seconds = positive_u64(
            raw.job_heartbeat_seconds.unwrap_or(60),
            "JOB_HEARTBEAT_SECONDS",
        )?;
        let job_execution_timeout_seconds = bounded_positive_u64(
            raw.job_execution_timeout_seconds.unwrap_or(30 * 60),
            "JOB_EXECUTION_TIMEOUT_SECONDS",
            MAX_JOB_EXECUTION_TIMEOUT_SECONDS,
        )?;
        if job_heartbeat_seconds >= job_lease_seconds {
            return Err(ConfigError::InvalidValue {
                variable: "JOB_LEASE_SECONDS",
                reason: "must be greater than JOB_HEARTBEAT_SECONDS",
            });
        }
        let job_max_attempts = raw.job_max_attempts.unwrap_or(3);
        if job_max_attempts == 0 {
            return Err(ConfigError::InvalidValue {
                variable: "JOB_MAX_ATTEMPTS",
                reason: "must be greater than zero",
            });
        }

        let account_lease_seconds = positive_u64(
            raw.account_lease_seconds.unwrap_or(600),
            "ACCOUNT_LEASE_SECONDS",
        )?;
        let account_heartbeat_seconds = positive_u64(
            raw.account_heartbeat_seconds.unwrap_or(60),
            "ACCOUNT_HEARTBEAT_SECONDS",
        )?;
        if account_heartbeat_seconds >= account_lease_seconds {
            return Err(ConfigError::InvalidValue {
                variable: "ACCOUNT_LEASE_SECONDS",
                reason: "must be greater than ACCOUNT_HEARTBEAT_SECONDS",
            });
        }

        let source_failure_cooldown_seconds = bounded_non_negative_u64(
            raw.source_failure_cooldown_seconds.unwrap_or(300),
            "SOURCE_FAILURE_COOLDOWN_SECONDS",
            MAX_SOURCE_FAILURE_COOLDOWN_SECONDS,
        )?;

        let rss_cache_seconds = positive_u64(
            raw.rss_cache_ttl_seconds.unwrap_or(1_800),
            "RSS_CACHE_TTL_SECONDS",
        )?;
        let rss_stale_while_revalidate_seconds = bounded_non_negative_u64(
            raw.rss_stale_while_revalidate_seconds.unwrap_or(60),
            "RSS_STALE_WHILE_REVALIDATE_SECONDS",
            MAX_STALE_WHILE_REVALIDATE_SECONDS,
        )?;
        let rss_cache_miss_wait_ms = bounded_positive_u64(
            raw.rss_cache_miss_wait_ms.unwrap_or(5_000),
            "RSS_CACHE_MISS_WAIT_MS",
            MAX_CACHE_MISS_WAIT_MS,
        )?;
        let server_root_url = parse_optional_http_url(raw.server_root_url, "SERVER_ROOT_URL")?;
        let asset_use_absolute_urls = parse_bool(
            raw.asset_use_absolute_urls.as_deref().unwrap_or("true"),
            "ASSET_USE_ABSOLUTE_URLS",
        )?;
        if asset_use_absolute_urls
            && matches!(
                &asset_archive,
                AssetArchiveConfig::Database { .. }
                    | AssetArchiveConfig::Local { .. }
                    | AssetArchiveConfig::S3 { .. }
                    | AssetArchiveConfig::Oss { .. }
                    | AssetArchiveConfig::Gcs { .. }
                    | AssetArchiveConfig::Azure { .. }
                    | AssetArchiveConfig::Ftp { .. }
                    | AssetArchiveConfig::Sftp { .. }
                    | AssetArchiveConfig::Webdav { .. }
            )
            && server_root_url.is_none()
        {
            return Err(ConfigError::Missing {
                variable: "SERVER_ROOT_URL",
            });
        }

        let feed_build_lease_seconds = positive_u64(
            raw.feed_build_lease_seconds.unwrap_or(600),
            "FEED_BUILD_LEASE_SECONDS",
        )?;
        let feed_build_heartbeat_seconds = positive_u64(
            raw.feed_build_heartbeat_seconds.unwrap_or(60),
            "FEED_BUILD_HEARTBEAT_SECONDS",
        )?;
        if feed_build_heartbeat_seconds >= feed_build_lease_seconds {
            return Err(ConfigError::InvalidValue {
                variable: "FEED_BUILD_LEASE_SECONDS",
                reason: "must be greater than FEED_BUILD_HEARTBEAT_SECONDS",
            });
        }

        let scroll_max_operation_seconds = raw.scroll_max_operation_seconds.unwrap_or(30);
        if scroll_max_operation_seconds > MAX_PAGE_OPERATION_SECONDS {
            return Err(ConfigError::InvalidValue {
                variable: "SCROLL_MAX_OPERATION_SECONDS",
                reason: "exceeds the maximum supported page-operation duration",
            });
        }
        let scroll_max_steps = bounded_positive_u32(
            raw.scroll_max_steps.unwrap_or(4),
            "SCROLL_MAX_STEPS",
            MAX_SCROLL_STEPS,
        )?;
        let scroll_max_pixels = bounded_positive_u32(
            raw.scroll_max_pixels.unwrap_or(4_000),
            "SCROLL_MAX_PIXELS",
            MAX_SCROLL_PIXELS,
        )?;

        let required_lease_seconds = job_heartbeat_seconds
            .checked_add(scroll_max_operation_seconds)
            .ok_or(ConfigError::InvalidValue {
                variable: "JOB_LEASE_SECONDS",
                reason: "is too large to validate against worker timing",
            })?;
        if job_lease_seconds <= required_lease_seconds {
            return Err(ConfigError::InvalidValue {
                variable: "JOB_LEASE_SECONDS",
                reason: "must exceed heartbeat interval plus maximum page-operation duration",
            });
        }

        let pacing = PacingPolicy::new(
            distribution(
                raw.pacing_request_mean_ms,
                raw.pacing_request_stddev_ms,
                raw.pacing_request_min_ms,
                raw.pacing_request_max_ms,
                (2_000.0, 250.0, 1_000.0, 4_000.0),
            )?,
            distribution(
                raw.pacing_page_navigation_mean_ms,
                raw.pacing_page_navigation_stddev_ms,
                raw.pacing_page_navigation_min_ms,
                raw.pacing_page_navigation_max_ms,
                (3_000.0, 500.0, 1_500.0, 7_000.0),
            )?,
            distribution(
                raw.pacing_page_action_mean_ms,
                raw.pacing_page_action_stddev_ms,
                raw.pacing_page_action_min_ms,
                raw.pacing_page_action_max_ms,
                (1_000.0, 200.0, 500.0, 3_000.0),
            )?,
            distribution(
                raw.pacing_scroll_settle_mean_ms,
                raw.pacing_scroll_settle_stddev_ms,
                raw.pacing_scroll_settle_min_ms,
                raw.pacing_scroll_settle_max_ms,
                (1_000.0, 200.0, 500.0, 3_000.0),
            )?,
            scroll_max_steps,
            scroll_max_pixels,
            Duration::from_secs(scroll_max_operation_seconds),
        )?;

        let http_bind = raw.http_bind.unwrap_or_else(|| "0.0.0.0".to_owned());
        if http_bind.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                variable: "HTTP_BIND",
                reason: "must not be empty",
            });
        }

        Ok(Self {
            roles,
            log_level,
            worker_concurrency,
            database_url: SecretString::new(database_url.into_boxed_str()),
            database_pool_min_connections,
            database_pool_max_connections,
            webdriver_url,
            browser_engine,
            browser_user_agent,
            browser_locale,
            browser_viewport_width,
            browser_viewport_height,
            browser_extra_args,
            weread_account_id,
            weread_article_list_url,
            instance_id: raw
                .app_instance_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("werrss-{}", Uuid::new_v4())),
            http_bind,
            http_port,
            timezone,
            quiet_hours,
            job_poll_interval: Duration::from_secs(job_poll_seconds),
            job_lease: Duration::from_secs(job_lease_seconds),
            job_heartbeat: Duration::from_secs(job_heartbeat_seconds),
            job_execution_timeout: Duration::from_secs(job_execution_timeout_seconds),
            job_max_attempts,
            account_lease: Duration::from_secs(account_lease_seconds),
            account_heartbeat: Duration::from_secs(account_heartbeat_seconds),
            source_failure_cooldown: Duration::from_secs(source_failure_cooldown_seconds),
            rss_cache_ttl: Duration::from_secs(rss_cache_seconds),
            rss_stale_while_revalidate: Duration::from_secs(rss_stale_while_revalidate_seconds),
            rss_cache_miss_wait: Duration::from_millis(rss_cache_miss_wait_ms),
            server_root_url,
            feed_build_lease: Duration::from_secs(feed_build_lease_seconds),
            feed_build_heartbeat: Duration::from_secs(feed_build_heartbeat_seconds),
            pacing,
            asset_archive,
            asset_use_absolute_urls,
            admin_enabled,
            admin_username,
            admin_password,
            session_signing_key,
            credential_encryption_key: SecretString::new(
                credential_encryption_key.into_boxed_str(),
            ),
        })
    }

    /// Returns whether an environment-configured default account is
    /// available. Source synchronization can also select an admin-enrolled
    /// account at job time when this returns `false`.
    pub const fn weread_source_sync_configured(&self) -> bool {
        self.weread_account_id.is_some()
    }
}

#[derive(Debug, Deserialize, Default)]
struct RawConfig {
    database_url: Option<String>,
    database_pool_min_connections: Option<u32>,
    database_pool_max_connections: Option<u32>,
    webdriver_url: Option<String>,
    browser_engine: Option<String>,
    browser_user_agent: Option<String>,
    browser_locale: Option<String>,
    browser_viewport_width: Option<u32>,
    browser_viewport_height: Option<u32>,
    browser_extra_args: Option<String>,
    weread_account_id: Option<String>,
    weread_article_list_url: Option<String>,
    worker_concurrency: Option<u32>,
    app_instance_id: Option<String>,
    http_bind: Option<String>,
    http_port: Option<u16>,
    app_roles: Option<String>,
    log_level: Option<String>,
    app_timezone: Option<String>,
    quiet_hours_start: Option<String>,
    quiet_hours_end: Option<String>,
    job_poll_seconds: Option<u64>,
    job_lease_seconds: Option<u64>,
    job_heartbeat_seconds: Option<u64>,
    job_execution_timeout_seconds: Option<u64>,
    job_max_attempts: Option<u32>,
    account_lease_seconds: Option<u64>,
    account_heartbeat_seconds: Option<u64>,
    source_failure_cooldown_seconds: Option<u64>,
    rss_cache_ttl_seconds: Option<u64>,
    rss_stale_while_revalidate_seconds: Option<u64>,
    rss_cache_miss_wait_ms: Option<u64>,
    server_root_url: Option<String>,
    feed_build_lease_seconds: Option<u64>,
    feed_build_heartbeat_seconds: Option<u64>,
    pacing_request_mean_ms: Option<f64>,
    pacing_request_stddev_ms: Option<f64>,
    pacing_request_min_ms: Option<f64>,
    pacing_request_max_ms: Option<f64>,
    pacing_page_navigation_mean_ms: Option<f64>,
    pacing_page_navigation_stddev_ms: Option<f64>,
    pacing_page_navigation_min_ms: Option<f64>,
    pacing_page_navigation_max_ms: Option<f64>,
    pacing_page_action_mean_ms: Option<f64>,
    pacing_page_action_stddev_ms: Option<f64>,
    pacing_page_action_min_ms: Option<f64>,
    pacing_page_action_max_ms: Option<f64>,
    pacing_scroll_settle_mean_ms: Option<f64>,
    pacing_scroll_settle_stddev_ms: Option<f64>,
    pacing_scroll_settle_min_ms: Option<f64>,
    pacing_scroll_settle_max_ms: Option<f64>,
    scroll_max_steps: Option<u32>,
    scroll_max_pixels: Option<u32>,
    scroll_max_operation_seconds: Option<u64>,
    asset_archive_backend: Option<String>,
    asset_local_root: Option<String>,
    asset_s3_bucket: Option<String>,
    asset_s3_endpoint: Option<String>,
    asset_s3_region: Option<String>,
    asset_s3_access_key_id: Option<String>,
    asset_s3_secret_access_key: Option<String>,
    asset_oss_bucket: Option<String>,
    asset_oss_endpoint: Option<String>,
    asset_oss_addressing_style: Option<String>,
    asset_oss_versioning: Option<String>,
    asset_oss_access_key_id: Option<String>,
    asset_oss_access_key_secret: Option<String>,
    asset_oss_security_token: Option<String>,
    asset_oss_skip_signature: Option<String>,
    asset_gcs_bucket: Option<String>,
    asset_gcs_endpoint: Option<String>,
    asset_gcs_credential: Option<String>,
    asset_gcs_skip_signature: Option<String>,
    asset_azure_container: Option<String>,
    asset_azure_endpoint: Option<String>,
    asset_azure_account_name: Option<String>,
    asset_azure_account_key: Option<String>,
    asset_azure_skip_signature: Option<String>,
    asset_ftp_endpoint: Option<String>,
    asset_ftp_root: Option<String>,
    asset_ftp_user: Option<String>,
    asset_ftp_password: Option<String>,
    asset_sftp_endpoint: Option<String>,
    asset_sftp_root: Option<String>,
    asset_sftp_user: Option<String>,
    asset_sftp_key: Option<String>,
    asset_webdav_endpoint: Option<String>,
    asset_webdav_root: Option<String>,
    asset_webdav_user: Option<String>,
    asset_webdav_password: Option<String>,
    asset_cache_max_size_mb: Option<u64>,
    asset_cache_max_age_days: Option<u64>,
    asset_use_absolute_urls: Option<String>,
    asset_max_size_mb: Option<u64>,
    asset_max_count_per_article: Option<u32>,
    asset_max_fetch_bytes_per_article_mb: Option<u64>,
    asset_max_fetch_time_per_article_seconds: Option<u64>,
    asset_fetch_timeout_seconds: Option<u64>,
    asset_max_redirects: Option<u32>,
    asset_repair_max_pending: Option<u32>,
    asset_repair_public_max_pending: Option<u32>,
    asset_repair_requeue_seconds: Option<u64>,
    asset_repair_max_attempts: Option<u32>,
    admin_enabled: Option<String>,
    admin_username: Option<String>,
    admin_password: Option<String>,
    session_signing_key: Option<String>,
    credential_encryption_key: Option<String>,
}

fn parse_browser_user_agent(value: Option<String>) -> Result<Option<String>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() || value.len() > MAX_BROWSER_USER_AGENT_LENGTH {
        return Err(ConfigError::InvalidValue {
            variable: "BROWSER_USER_AGENT",
            reason: "must be non-empty and no longer than 512 characters",
        });
    }
    if value.chars().any(char::is_control) {
        return Err(ConfigError::InvalidValue {
            variable: "BROWSER_USER_AGENT",
            reason: "must not contain control characters",
        });
    }
    Ok(Some(value.to_owned()))
}

fn parse_browser_locale(value: Option<String>) -> Result<String, ConfigError> {
    let value = value.unwrap_or_else(|| DEFAULT_BROWSER_LOCALE.to_owned());
    let value = value.trim();
    if value.is_empty()
        || value.len() > 64
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(ConfigError::InvalidValue {
            variable: "BROWSER_LOCALE",
            reason: "must be a non-empty locale token without whitespace",
        });
    }
    Ok(value.to_owned())
}

fn bounded_browser_viewport(value: u32, variable: &'static str) -> Result<u32, ConfigError> {
    if value == 0 || value > MAX_BROWSER_VIEWPORT_DIMENSION {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "must be between 1 and 8192 pixels",
        });
    }
    Ok(value)
}

fn parse_browser_extra_args(value: Option<String>) -> Result<Vec<String>, ConfigError> {
    let arguments = value
        .unwrap_or_default()
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if arguments.len() > MAX_BROWSER_EXTRA_ARGS {
        return Err(ConfigError::InvalidValue {
            variable: "BROWSER_EXTRA_ARGS",
            reason: "must contain no more than 32 arguments",
        });
    }
    if arguments
        .iter()
        .any(|argument| argument.is_empty() || !argument.starts_with('-'))
    {
        return Err(ConfigError::InvalidValue {
            variable: "BROWSER_EXTRA_ARGS",
            reason: "each argument must begin with '-'",
        });
    }
    if arguments.iter().any(|argument| {
        browser_argument_name(argument).is_some_and(|name| {
            matches!(
                name,
                "--lang"
                    | "--user-agent"
                    | "--window-size"
                    | "--user-data-dir"
                    | "--headless"
                    | "-headless"
                    | "-profile"
            )
        })
    }) {
        return Err(ConfigError::InvalidValue {
            variable: "BROWSER_EXTRA_ARGS",
            reason: "must not override controlled browser profile arguments",
        });
    }
    Ok(arguments)
}

fn parse_optional_weread_account_id(
    value: Option<String>,
) -> Result<Option<WeReadAccountId>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let account_id = value
        .trim()
        .parse::<Uuid>()
        .map_err(|_| ConfigError::InvalidValue {
            variable: "WEREAD_ACCOUNT_ID",
            reason: "expected a valid UUID",
        })?;
    if account_id.is_nil() {
        return Err(ConfigError::InvalidValue {
            variable: "WEREAD_ACCOUNT_ID",
            reason: "must not be the nil UUID",
        });
    }
    Ok(Some(WeReadAccountId::from_uuid(account_id)))
}

fn parse_log_level(
    value: Option<&str>,
) -> Result<tracing::level_filters::LevelFilter, ConfigError> {
    let value = value.unwrap_or("warn").trim();
    if value.is_empty() {
        return Err(ConfigError::InvalidValue {
            variable: "LOG_LEVEL",
            reason: "expected off, error, warn, info, debug, or trace",
        });
    }
    value.parse().map_err(|_| ConfigError::InvalidValue {
        variable: "LOG_LEVEL",
        reason: "expected off, error, warn, info, debug, or trace",
    })
}

fn parse_weread_article_list_url(value: Option<String>) -> Result<Url, ConfigError> {
    let value = value.unwrap_or_else(|| "https://weread.qq.com/api/mp/cover".to_owned());
    let url = value
        .trim()
        .parse::<Url>()
        .map_err(|_| ConfigError::InvalidValue {
            variable: "WEREAD_ARTICLE_LIST_URL",
            reason: "expected a valid HTTPS WeRead article-list URL",
        })?;
    if url.scheme() != "https"
        || url.host_str() != Some("weread.qq.com")
        || !matches!(url.path(), "/web/mp/articles" | "/api/mp/cover")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.port().is_some_and(|port| port != 443)
    {
        return Err(ConfigError::InvalidValue {
            variable: "WEREAD_ARTICLE_LIST_URL",
            reason: "must use HTTPS weread.qq.com/web/mp/articles or /api/mp/cover without credentials, fragments, or a non-default port",
        });
    }
    Ok(url)
}

fn browser_argument_name(argument: &str) -> Option<&str> {
    argument
        .split_once('=')
        .map_or(Some(argument), |(name, _)| {
            (!name.is_empty()).then_some(name)
        })
}

fn reject_legacy_variables(variables: &[(String, String)]) -> Result<(), ConfigError> {
    for (name, _) in variables {
        if LEGACY_ENVIRONMENT_VARIABLES.contains(&name.as_str()) {
            return Err(ConfigError::InvalidValue {
                variable: if name == "ARCHIVE_BACKEND" {
                    "ARCHIVE_BACKEND"
                } else {
                    "ARCHIVE_LOCAL_PATH"
                },
                reason: "use ASSET_ARCHIVE_* instead",
            });
        }
    }
    Ok(())
}

fn reject_unknown_variables(variables: &[(String, String)]) -> Result<(), ConfigError> {
    for (name, _) in variables {
        let owned = APPLICATION_ENVIRONMENT_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix));
        if owned
            && !KNOWN_ENVIRONMENT_VARIABLES.contains(&name.as_str())
            && !IGNORED_RUNTIME_ENVIRONMENT_VARIABLES.contains(&name.as_str())
        {
            return Err(ConfigError::UnknownVariable {
                variable: name.clone(),
            });
        }
    }
    Ok(())
}

fn asset_archive_config(raw: &RawConfig) -> Result<AssetArchiveConfig, ConfigError> {
    let backend = raw
        .asset_archive_backend
        .as_deref()
        .unwrap_or("disabled")
        .trim()
        .to_ascii_lowercase();
    let policy = asset_cache_policy(raw)?;
    match backend.as_str() {
        "disabled" => Ok(AssetArchiveConfig::Disabled),
        "database" | "postgres" => Ok(AssetArchiveConfig::Database {
            policy,
            repair_policy: asset_repair_policy(raw)?,
        }),
        "local" => {
            let root = raw
                .asset_local_root
                .as_deref()
                .map(str::trim)
                .filter(|root| !root.is_empty())
                .ok_or(ConfigError::Missing {
                    variable: "ASSET_LOCAL_ROOT",
                })?;
            Ok(AssetArchiveConfig::Local {
                root: root.to_owned(),
                policy,
                repair_policy: asset_repair_policy(raw)?,
            })
        }
        "s3" => {
            let bucket = required(raw.asset_s3_bucket.clone(), "ASSET_S3_BUCKET")?;
            let region = required(raw.asset_s3_region.clone(), "ASSET_S3_REGION")?;
            let access_key_id =
                required(raw.asset_s3_access_key_id.clone(), "ASSET_S3_ACCESS_KEY_ID")?;
            let secret_access_key = required(
                raw.asset_s3_secret_access_key.clone(),
                "ASSET_S3_SECRET_ACCESS_KEY",
            )?;
            let endpoint = raw
                .asset_s3_endpoint
                .as_deref()
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(str::to_owned);
            Ok(AssetArchiveConfig::S3 {
                storage: AssetS3Config {
                    bucket,
                    endpoint,
                    region,
                    access_key_id: secrecy::SecretString::new(access_key_id.into_boxed_str()),
                    secret_access_key: secrecy::SecretString::new(
                        secret_access_key.into_boxed_str(),
                    ),
                },
                policy,
                repair_policy: asset_repair_policy(raw)?,
            })
        }
        "oss" => {
            let bucket = required(raw.asset_oss_bucket.clone(), "ASSET_OSS_BUCKET")?;
            let versioning = required(
                raw.asset_oss_versioning.clone(),
                "ASSET_OSS_VERSIONING",
            )?;
            if !versioning.trim().eq_ignore_ascii_case("disabled") {
                return Err(ConfigError::InvalidValue {
                    variable: "ASSET_OSS_VERSIONING",
                    reason: "must be disabled; versioned and suspended buckets are unsupported",
                });
            }
            let endpoint = required(raw.asset_oss_endpoint.clone(), "ASSET_OSS_ENDPOINT")?
                .trim()
                .trim_end_matches('/')
                .to_owned();
            if endpoint.is_empty() {
                return Err(ConfigError::Missing {
                    variable: "ASSET_OSS_ENDPOINT",
                });
            }
            let endpoint_url = parse_optional_http_url(Some(endpoint), "ASSET_OSS_ENDPOINT")?
                .expect("a supplied OSS endpoint must produce a URL");
            if endpoint_url.path() != "/" || endpoint_url.query().is_some() {
                return Err(ConfigError::InvalidValue {
                    variable: "ASSET_OSS_ENDPOINT",
                    reason: "must not contain a path or query because the OSS adapter does not preserve endpoint components",
                });
            }
            let endpoint = endpoint_url.to_string().trim_end_matches('/').to_owned();
            let addressing_style = parse_oss_addressing_style(
                raw.asset_oss_addressing_style
                    .as_deref()
                    .unwrap_or("virtual"),
            )?;
            let skip_signature = parse_bool(
                raw.asset_oss_skip_signature.as_deref().unwrap_or("false"),
                "ASSET_OSS_SKIP_SIGNATURE",
            )?;
            let access_key_id = if skip_signature {
                raw.asset_oss_access_key_id
                    .clone()
                    .filter(|value| !value.trim().is_empty())
            } else {
                Some(required(
                    raw.asset_oss_access_key_id.clone(),
                    "ASSET_OSS_ACCESS_KEY_ID",
                )?)
            };
            let access_key_secret = if skip_signature {
                raw.asset_oss_access_key_secret
                    .clone()
                    .filter(|value| !value.trim().is_empty())
            } else {
                Some(required(
                    raw.asset_oss_access_key_secret.clone(),
                    "ASSET_OSS_ACCESS_KEY_SECRET",
                )?)
            };
            let security_token = raw
                .asset_oss_security_token
                .clone()
                .filter(|value| !value.trim().is_empty());
            Ok(AssetArchiveConfig::Oss {
                storage: AssetOssConfig {
                    bucket,
                    endpoint,
                    addressing_style,
                    access_key_id: access_key_id
                        .map(|value| secrecy::SecretString::new(value.into_boxed_str())),
                    access_key_secret: access_key_secret
                        .map(|value| secrecy::SecretString::new(value.into_boxed_str())),
                    security_token: security_token
                        .map(|value| secrecy::SecretString::new(value.into_boxed_str())),
                    skip_signature,
                },
                policy,
                repair_policy: asset_repair_policy(raw)?,
            })
        }
        "gcs" => {
            let bucket = required(raw.asset_gcs_bucket.clone(), "ASSET_GCS_BUCKET")?;
            let skip_signature = parse_bool(
                raw.asset_gcs_skip_signature.as_deref().unwrap_or("false"),
                "ASSET_GCS_SKIP_SIGNATURE",
            )?;
            let endpoint = raw
                .asset_gcs_endpoint
                .as_deref()
                .map(str::trim)
                .map(|endpoint| endpoint.trim_end_matches('/'))
                .filter(|endpoint| !endpoint.is_empty())
                .map(str::to_owned);
            if skip_signature && endpoint.is_none() {
                return Err(ConfigError::Missing {
                    variable: "ASSET_GCS_ENDPOINT",
                });
            }
            let credential = if skip_signature {
                raw.asset_gcs_credential
                    .clone()
                    .filter(|credential| !credential.trim().is_empty())
            } else {
                Some(validate_gcs_credential(required(
                    raw.asset_gcs_credential.clone(),
                    "ASSET_GCS_CREDENTIAL",
                )?)?)
            };
            Ok(AssetArchiveConfig::Gcs {
                storage: AssetGcsConfig {
                    bucket,
                    endpoint,
                    credential: credential
                        .map(|credential| secrecy::SecretString::new(credential.into_boxed_str())),
                    skip_signature,
                },
                policy,
                repair_policy: asset_repair_policy(raw)?,
            })
        }
        "azure" | "azblob" => {
            let container = required(
                raw.asset_azure_container.clone(),
                "ASSET_AZURE_CONTAINER",
            )?;
            let endpoint = required(
                raw.asset_azure_endpoint.clone(),
                "ASSET_AZURE_ENDPOINT",
            )?
            .trim()
            .trim_end_matches('/')
            .to_owned();
            if endpoint.is_empty() {
                return Err(ConfigError::Missing {
                    variable: "ASSET_AZURE_ENDPOINT",
                });
            }
            let skip_signature = parse_bool(
                raw.asset_azure_skip_signature.as_deref().unwrap_or("false"),
                "ASSET_AZURE_SKIP_SIGNATURE",
            )?;
            let account_name = if skip_signature {
                raw.asset_azure_account_name
                    .clone()
                    .filter(|value| !value.trim().is_empty())
            } else {
                Some(required(
                    raw.asset_azure_account_name.clone(),
                    "ASSET_AZURE_ACCOUNT_NAME",
                )?)
            };
            let account_key = if skip_signature {
                raw.asset_azure_account_key
                    .clone()
                    .filter(|value| !value.trim().is_empty())
            } else {
                Some(validate_azure_account_key(required(
                    raw.asset_azure_account_key.clone(),
                    "ASSET_AZURE_ACCOUNT_KEY",
                )?)?)
            };
            Ok(AssetArchiveConfig::Azure {
                storage: AssetAzureConfig {
                    container,
                    endpoint,
                    account_name: account_name.map(|value| {
                        secrecy::SecretString::new(value.into_boxed_str())
                    }),
                    account_key: account_key.map(|value| {
                        secrecy::SecretString::new(value.into_boxed_str())
                    }),
                    skip_signature,
                },
                policy,
                repair_policy: asset_repair_policy(raw)?,
            })
        }
        "ftp" => {
            let endpoint = protocol_endpoint(
                raw.asset_ftp_endpoint.clone(),
                "ASSET_FTP_ENDPOINT",
                &["ftp", "ftps"],
                true,
                true,
            )?;
            let root = optional_non_empty(raw.asset_ftp_root.clone());
            let user = optional_secret(raw.asset_ftp_user.clone());
            let password = optional_secret(raw.asset_ftp_password.clone());
            if user.is_some() != password.is_some() {
                return Err(ConfigError::InvalidValue {
                    variable: if user.is_some() { "ASSET_FTP_PASSWORD" } else { "ASSET_FTP_USER" },
                    reason: "FTP user and password must be configured together",
                });
            }
            Ok(AssetArchiveConfig::Ftp {
                storage: AssetFtpConfig {
                    endpoint,
                    root,
                    user: user.map(|value| SecretString::new(value.into_boxed_str())),
                    password: password.map(|value| SecretString::new(value.into_boxed_str())),
                },
                policy,
                repair_policy: asset_repair_policy(raw)?,
            })
        }
        "sftp" => {
            let endpoint = protocol_endpoint(
                raw.asset_sftp_endpoint.clone(),
                "ASSET_SFTP_ENDPOINT",
                &["sftp"],
                true,
                true,
            )?;
            let user = required(raw.asset_sftp_user.clone(), "ASSET_SFTP_USER")?;
            if user.trim().is_empty() {
                return Err(ConfigError::Missing {
                    variable: "ASSET_SFTP_USER",
                });
            }
            let key = required(raw.asset_sftp_key.clone(), "ASSET_SFTP_KEY")?;
            let key = key.trim().to_owned();
            if !std::path::Path::new(&key).is_absolute() {
                return Err(ConfigError::InvalidValue {
                    variable: "ASSET_SFTP_KEY",
                    reason: "must be an absolute path to a mounted private-key file",
                });
            }
            Ok(AssetArchiveConfig::Sftp {
                storage: AssetSftpConfig {
                    endpoint,
                    root: optional_non_empty(raw.asset_sftp_root.clone()),
                    user: SecretString::new(user.into_boxed_str()),
                    key: SecretString::new(key.into_boxed_str()),
                },
                policy,
                repair_policy: asset_repair_policy(raw)?,
            })
        }
        "webdav" => {
            let endpoint = protocol_endpoint(
                raw.asset_webdav_endpoint.clone(),
                "ASSET_WEBDAV_ENDPOINT",
                &["http", "https"],
                false,
                true,
            )?;
            let root = optional_non_empty(raw.asset_webdav_root.clone());
            let user = optional_secret(raw.asset_webdav_user.clone());
            let password = optional_secret(raw.asset_webdav_password.clone());
            if user.is_some() != password.is_some() {
                return Err(ConfigError::InvalidValue {
                    variable: if user.is_some() { "ASSET_WEBDAV_PASSWORD" } else { "ASSET_WEBDAV_USER" },
                    reason: "WebDAV user and password must be configured together",
                });
            }
            Ok(AssetArchiveConfig::Webdav {
                storage: AssetWebdavConfig {
                    endpoint,
                    root,
                    user: user.map(|value| SecretString::new(value.into_boxed_str())),
                    password: password.map(|value| SecretString::new(value.into_boxed_str())),
                },
                policy,
                repair_policy: asset_repair_policy(raw)?,
            })
        }
        _ => Err(ConfigError::InvalidValue {
            variable: "ASSET_ARCHIVE_BACKEND",
            reason: "expected disabled, database, local, s3, oss, gcs, azure, ftp, sftp, or webdav (postgres and azblob are aliases)",
        }),
    }
}

fn optional_non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn optional_secret(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

fn protocol_endpoint(
    value: Option<String>,
    variable: &'static str,
    schemes: &[&str],
    forbid_path: bool,
    forbid_query: bool,
) -> Result<String, ConfigError> {
    let endpoint = required(value, variable)?.trim().to_owned();
    let parsed = Url::parse(&endpoint).map_err(|_| ConfigError::InvalidValue {
        variable,
        reason: "expected an absolute URL using the required protocol",
    })?;
    let authority_has_userinfo = endpoint.split_once("://").is_some_and(|(_, authority)| {
        authority
            .split(['/', '?', '#'])
            .next()
            .is_some_and(|host| host.contains('@'))
    });
    if !schemes.contains(&parsed.scheme())
        || parsed.host_str().is_none()
        || authority_has_userinfo
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || (forbid_path && !matches!(parsed.path(), "" | "/"))
        || (forbid_query && parsed.query().is_some())
    {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "endpoint has an unsupported protocol or contains credentials, path, query, or fragment",
        });
    }
    Ok(endpoint.trim_end_matches('/').to_owned())
}

fn asset_repair_policy(raw: &RawConfig) -> Result<AssetRepairPolicy, ConfigError> {
    let max_pending = bounded_positive_u32(
        raw.asset_repair_max_pending.unwrap_or(100),
        "ASSET_REPAIR_MAX_PENDING",
        100_000,
    )?;
    let public_max_pending = raw.asset_repair_public_max_pending.unwrap_or(50);
    if public_max_pending > max_pending {
        return Err(ConfigError::InvalidValue {
            variable: "ASSET_REPAIR_PUBLIC_MAX_PENDING",
            reason: "must not exceed ASSET_REPAIR_MAX_PENDING",
        });
    }
    let requeue_after = bounded_positive_u64(
        raw.asset_repair_requeue_seconds.unwrap_or(60),
        "ASSET_REPAIR_REQUEUE_SECONDS",
        24 * 60 * 60,
    )?;
    let max_attempts = bounded_positive_u32(
        raw.asset_repair_max_attempts.unwrap_or(3),
        "ASSET_REPAIR_MAX_ATTEMPTS",
        100,
    )?;
    AssetRepairPolicy::new(
        max_pending,
        public_max_pending,
        Duration::from_secs(requeue_after),
        max_attempts,
    )
    .map_err(|error| match error {
        AssetRepairPolicyError::ZeroLimit { field } => ConfigError::InvalidValue {
            variable: match field {
                "max_pending" => "ASSET_REPAIR_MAX_PENDING",
                "max_attempts" => "ASSET_REPAIR_MAX_ATTEMPTS",
                _ => "ASSET_REPAIR_MAX_PENDING",
            },
            reason: "must be greater than zero",
        },
        AssetRepairPolicyError::PublicLimitExceedsGlobal => ConfigError::InvalidValue {
            variable: "ASSET_REPAIR_PUBLIC_MAX_PENDING",
            reason: "must not exceed ASSET_REPAIR_MAX_PENDING",
        },
        AssetRepairPolicyError::ZeroDuration => ConfigError::InvalidValue {
            variable: "ASSET_REPAIR_REQUEUE_SECONDS",
            reason: "must be greater than zero",
        },
    })
}

fn asset_cache_policy(raw: &RawConfig) -> Result<AssetCachePolicy, ConfigError> {
    let policy = AssetCachePolicy::new(
        decimal_megabytes(
            raw.asset_cache_max_size_mb.unwrap_or(5_000),
            "ASSET_CACHE_MAX_SIZE_MB",
        )?,
        Duration::from_secs(
            raw.asset_cache_max_age_days
                .unwrap_or(30)
                .checked_mul(24 * 60 * 60)
                .ok_or(ConfigError::InvalidValue {
                    variable: "ASSET_CACHE_MAX_AGE_DAYS",
                    reason: "is too large",
                })?,
        ),
        decimal_megabytes(raw.asset_max_size_mb.unwrap_or(10), "ASSET_MAX_SIZE_MB")?,
        raw.asset_max_count_per_article.unwrap_or(100),
        decimal_megabytes(
            raw.asset_max_fetch_bytes_per_article_mb.unwrap_or(100),
            "ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB",
        )?,
        Duration::from_secs(raw.asset_max_fetch_time_per_article_seconds.unwrap_or(120)),
        Duration::from_secs(raw.asset_fetch_timeout_seconds.unwrap_or(30)),
        raw.asset_max_redirects.unwrap_or(5),
    )
    .map_err(|error| match error {
        AssetCachePolicyError::ZeroLimit { field } => ConfigError::InvalidValue {
            variable: asset_policy_variable(field),
            reason: "must be greater than zero",
        },
        AssetCachePolicyError::ZeroDuration { field } => ConfigError::InvalidValue {
            variable: asset_policy_variable(field),
            reason: "must be greater than zero",
        },
        AssetCachePolicyError::TimeoutExceedsArticleBudget => ConfigError::InvalidValue {
            variable: "ASSET_FETCH_TIMEOUT_SECONDS",
            reason: "must not exceed ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS",
        },
    })?;
    Ok(policy)
}

fn decimal_megabytes(value: u64, variable: &'static str) -> Result<u64, ConfigError> {
    value
        .checked_mul(1_000_000)
        .ok_or(ConfigError::InvalidValue {
            variable,
            reason: "is too large",
        })
}

fn asset_policy_variable(field: &str) -> &'static str {
    match field {
        "max_asset_size_bytes" => "ASSET_MAX_SIZE_MB",
        "max_count_per_article" => "ASSET_MAX_COUNT_PER_ARTICLE",
        "max_fetch_bytes_per_article" => "ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB",
        "max_fetch_time_per_article" => "ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS",
        "fetch_timeout" => "ASSET_FETCH_TIMEOUT_SECONDS",
        _ => "ASSET_ARCHIVE_BACKEND",
    }
}

fn required(value: Option<String>, variable: &'static str) -> Result<String, ConfigError> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or(ConfigError::Missing { variable })
}

fn validate_gcs_credential(credential: String) -> Result<String, ConfigError> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(credential.as_bytes())
        .map_err(|_| ConfigError::InvalidValue {
            variable: "ASSET_GCS_CREDENTIAL",
            reason: "must be standard-base64-encoded Google credential JSON",
        })?;
    let document: serde_json::Value =
        serde_json::from_slice(&decoded).map_err(|_| ConfigError::InvalidValue {
            variable: "ASSET_GCS_CREDENTIAL",
            reason: "must be standard-base64-encoded Google credential JSON",
        })?;
    let credential_type = document
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(ConfigError::InvalidValue {
            variable: "ASSET_GCS_CREDENTIAL",
            reason: "must contain a supported Google credential type",
        })?;
    if !matches!(
        credential_type,
        "service_account" | "external_account" | "impersonated_service_account" | "authorized_user"
    ) {
        return Err(ConfigError::InvalidValue {
            variable: "ASSET_GCS_CREDENTIAL",
            reason: "must contain a supported Google credential type",
        });
    }
    Ok(credential)
}

fn validate_azure_account_key(account_key: String) -> Result<String, ConfigError> {
    base64::engine::general_purpose::STANDARD
        .decode(account_key.as_bytes())
        .map_err(|_| ConfigError::InvalidValue {
            variable: "ASSET_AZURE_ACCOUNT_KEY",
            reason: "must be standard-base64-encoded Azure account key",
        })?;
    Ok(account_key)
}

fn parse_oss_addressing_style(value: &str) -> Result<String, ConfigError> {
    let value = value.trim().to_ascii_lowercase();
    if matches!(value.as_str(), "virtual" | "path" | "cname") {
        return Ok(value);
    }
    Err(ConfigError::InvalidValue {
        variable: "ASSET_OSS_ADDRESSING_STYLE",
        reason: "expected virtual, path, or cname",
    })
}

fn parse_optional_http_url(
    value: Option<String>,
    variable: &'static str,
) -> Result<Option<Url>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let url = value
        .trim()
        .parse::<Url>()
        .map_err(|_| ConfigError::InvalidValue {
            variable,
            reason: "expected a valid http or https URL",
        })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "expected a public URL with an http/https scheme and host",
        });
    }
    Ok(Some(url))
}

fn parse_time(value: &str, variable: &'static str) -> Result<NaiveTime, ConfigError> {
    NaiveTime::parse_from_str(value.trim(), "%H:%M").map_err(|_| ConfigError::InvalidValue {
        variable,
        reason: "expected HH:MM",
    })
}

fn positive_u64(value: u64, variable: &'static str) -> Result<u64, ConfigError> {
    if value == 0 {
        Err(ConfigError::InvalidValue {
            variable,
            reason: "must be greater than zero",
        })
    } else {
        Ok(value)
    }
}

fn bounded_positive_u64(
    value: u64,
    variable: &'static str,
    maximum: u64,
) -> Result<u64, ConfigError> {
    if value == 0 {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "must be greater than zero",
        });
    }
    if value > maximum {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "exceeds the supported maximum",
        });
    }
    Ok(value)
}

fn bounded_non_negative_u64(
    value: u64,
    variable: &'static str,
    maximum: u64,
) -> Result<u64, ConfigError> {
    if value > maximum {
        Err(ConfigError::InvalidValue {
            variable,
            reason: "exceeds the supported maximum",
        })
    } else {
        Ok(value)
    }
}

fn bounded_positive_u32(
    value: u32,
    variable: &'static str,
    maximum: u32,
) -> Result<u32, ConfigError> {
    if value == 0 {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "must be greater than zero",
        });
    }
    if value > maximum {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "exceeds the supported maximum",
        });
    }
    Ok(value)
}

fn parse_bool(value: &str, variable: &'static str) -> Result<bool, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ConfigError::InvalidValue {
            variable,
            reason: "expected true or false",
        }),
    }
}

fn distribution(
    mean_ms: Option<f64>,
    stddev_ms: Option<f64>,
    min_ms: Option<f64>,
    max_ms: Option<f64>,
    defaults: (f64, f64, f64, f64),
) -> Result<DelayDistribution, ConfigError> {
    let mean_ms = bounded_delay(mean_ms, "PACING_DELAY_MEAN_MS")?;
    let stddev_ms = bounded_delay(stddev_ms, "PACING_DELAY_STDDEV_MS")?;
    let min_ms = bounded_delay(min_ms, "PACING_DELAY_MIN_MS")?;
    let max_ms = bounded_delay(max_ms, "PACING_DELAY_MAX_MS")?;
    let (default_mean, default_stddev, default_min, default_max) = defaults;
    DelayDistribution::new(
        mean_ms.unwrap_or(default_mean),
        stddev_ms.unwrap_or(default_stddev),
        min_ms.unwrap_or(default_min),
        max_ms.unwrap_or(default_max),
    )
    .map_err(ConfigError::Pacing)
}

fn bounded_delay(value: Option<f64>, variable: &'static str) -> Result<Option<f64>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.is_finite() || value < 0.0 {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "must be finite and non-negative",
        });
    }
    if value > MAX_CONFIGURED_DELAY_MS {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "exceeds the maximum supported delay",
        });
    }
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use secrecy::ExposeSecret;

    use super::*;

    fn valid_environment() -> Vec<(String, String)> {
        vec![
            ("DATABASE_URL", "postgres://user:password@db/werrss"),
            ("CREDENTIAL_ENCRYPTION_KEY", "test-encryption-key"),
            ("WEBDRIVER_URL", "http://webdriver:4444"),
            ("DATABASE_POOL_MIN_CONNECTIONS", "2"),
            ("DATABASE_POOL_MAX_CONNECTIONS", "12"),
            ("APP_TIMEZONE", "Asia/Shanghai"),
            ("QUIET_HOURS_START", "23:00"),
            ("QUIET_HOURS_END", "07:00"),
            ("HTTP_BIND", "127.0.0.1"),
            ("HTTP_PORT", "8088"),
            ("JOB_LEASE_SECONDS", "120"),
            ("RSS_CACHE_TTL_SECONDS", "1800"),
            ("ASSET_OSS_VERSIONING", "disabled"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
    }

    fn valid_gcs_credential() -> String {
        base64::engine::general_purpose::STANDARD.encode(
            br#"{"type":"service_account","client_email":"test@example.test","private_key":"test"}"#,
        )
    }

    fn valid_azure_account_key() -> String {
        base64::engine::general_purpose::STANDARD.encode(b"azure-account-key")
    }

    fn replace_environment(
        environment: Vec<(String, String)>,
        variable: &str,
        value: &str,
    ) -> Vec<(String, String)> {
        let mut replaced = false;
        let mut environment = environment
            .into_iter()
            .map(|(key, current)| {
                if key == variable {
                    replaced = true;
                    (key, value.to_owned())
                } else {
                    (key, current)
                }
            })
            .collect::<Vec<_>>();
        if !replaced {
            environment.push((variable.to_owned(), value.to_owned()));
        }
        environment
    }

    #[test]
    fn loads_defaults_and_explicit_values_from_environment_pairs() {
        let config = AppConfig::from_env_iter(valid_environment()).unwrap();

        assert_eq!(config.log_level, tracing::level_filters::LevelFilter::WARN);
        assert_eq!(config.browser_engine, BrowserEngine::Chromium);
        assert!(config.browser_user_agent.is_none());
        assert_eq!(config.browser_locale, "zh-CN");
        assert_eq!(config.browser_viewport_width, 1_280);
        assert_eq!(config.browser_viewport_height, 2_000);
        assert!(config.browser_extra_args.is_empty());
        assert!(config.weread_account_id.is_none());
        assert_eq!(
            config.weread_article_list_url.as_str(),
            "https://weread.qq.com/api/mp/cover"
        );
        assert!(config.roles.contains(AppRole::Api));
        assert!(!config.roles.contains(AppRole::Scheduler));
        assert!(!config.roles.contains(AppRole::Worker));
        assert_eq!(config.worker_concurrency, 1);
        assert_eq!(config.database_pool_min_connections, 2);
        assert_eq!(config.database_pool_max_connections, 12);
        assert_eq!(config.http_bind, "127.0.0.1");
        assert_eq!(config.http_port, 8088);
        assert_eq!(config.timezone, chrono_tz::Asia::Shanghai);
        assert_eq!(config.job_lease, Duration::from_secs(120));
        assert_eq!(config.job_heartbeat, Duration::from_secs(60));
        assert_eq!(config.job_execution_timeout, Duration::from_secs(1_800));
        assert_eq!(config.account_lease, Duration::from_secs(600));
        assert_eq!(config.account_heartbeat, Duration::from_secs(60));
        assert_eq!(config.source_failure_cooldown, Duration::from_secs(300));
        assert_eq!(config.rss_cache_ttl, Duration::from_secs(1_800));
        assert_eq!(config.rss_stale_while_revalidate, Duration::from_secs(60));
        assert_eq!(config.rss_cache_miss_wait, Duration::from_secs(5));
        assert!(config.server_root_url.is_none());
        assert!(config.asset_use_absolute_urls);
        assert_eq!(config.feed_build_lease, Duration::from_secs(600));
        assert_eq!(config.feed_build_heartbeat, Duration::from_secs(60));
        assert!(matches!(config.asset_archive, AssetArchiveConfig::Disabled));
        assert!(!config.admin_enabled);
        assert!(config.admin_username.is_none());
        assert!(config.admin_password.is_none());
        assert!(config.session_signing_key.is_none());
        assert!(config.quiet_hours.unwrap().is_quiet_at(
            "2026-08-27T15:00:00Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        ));
    }

    #[test]
    fn parses_configured_log_levels_and_rejects_unknown_values() {
        for (value, expected) in [
            ("off", tracing::level_filters::LevelFilter::OFF),
            ("error", tracing::level_filters::LevelFilter::ERROR),
            (" warn ", tracing::level_filters::LevelFilter::WARN),
            ("info", tracing::level_filters::LevelFilter::INFO),
            ("debug", tracing::level_filters::LevelFilter::DEBUG),
            ("trace", tracing::level_filters::LevelFilter::TRACE),
        ] {
            let config = AppConfig::from_env_iter(replace_environment(
                valid_environment(),
                "LOG_LEVEL",
                value,
            ))
            .unwrap();
            assert_eq!(config.log_level, expected, "LOG_LEVEL={value:?}");
        }

        for value in ["", "verbose", "warning", "info,sqlx=debug"] {
            assert!(
                matches!(
                    AppConfig::from_env_iter(replace_environment(
                        valid_environment(),
                        "LOG_LEVEL",
                        value,
                    )),
                    Err(ConfigError::InvalidValue {
                        variable: "LOG_LEVEL",
                        ..
                    })
                ),
                "LOG_LEVEL={value:?} should be rejected"
            );
        }
    }

    #[test]
    fn defaults_http_listener_when_bind_and_port_are_absent() {
        let environment = valid_environment()
            .into_iter()
            .filter(|(key, _)| key != "HTTP_BIND" && key != "HTTP_PORT")
            .collect::<Vec<_>>();

        let config = AppConfig::from_env_iter(environment).unwrap();

        assert_eq!(config.http_bind, "0.0.0.0");
        assert_eq!(config.http_port, 8080);
    }

    #[test]
    fn does_not_require_quiet_hours_when_both_values_are_absent() {
        let environment: Vec<(String, String)> = valid_environment()
            .into_iter()
            .filter(|(key, _)| key != "QUIET_HOURS_START" && key != "QUIET_HOURS_END")
            .collect::<Vec<_>>();

        assert!(AppConfig::from_env_iter(environment)
            .unwrap()
            .quiet_hours
            .is_none());
    }

    #[test]
    fn rejects_missing_required_secrets() {
        let environment = valid_environment()
            .into_iter()
            .filter(|(key, _)| key != "CREDENTIAL_ENCRYPTION_KEY")
            .collect::<Vec<_>>();

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "CREDENTIAL_ENCRYPTION_KEY"
            })
        ));
    }

    #[test]
    fn rejects_partial_quiet_hours() {
        let environment = valid_environment()
            .into_iter()
            .filter(|(key, _)| key != "QUIET_HOURS_END")
            .collect::<Vec<_>>();

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::IncompleteQuietHours)
        ));
    }

    #[test]
    fn rejects_invalid_timezone_and_webdriver_url() {
        let environment = valid_environment()
            .into_iter()
            .map(|(key, value)| {
                if key == "APP_TIMEZONE" {
                    (key, "Not/A-Timezone".to_owned())
                } else {
                    (key, value)
                }
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "APP_TIMEZONE",
                ..
            })
        ));

        let environment = valid_environment()
            .into_iter()
            .map(|(key, value)| {
                if key == "WEBDRIVER_URL" {
                    (key, "ftp://webdriver:4444".to_owned())
                } else {
                    (key, value)
                }
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "WEBDRIVER_URL",
                ..
            })
        ));
    }

    #[test]
    fn rejects_non_postgres_database_urls() {
        let environment = replace_environment(
            valid_environment(),
            "DATABASE_URL",
            "http://db.example/werrss",
        );

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "DATABASE_URL",
                ..
            })
        ));
    }

    #[test]
    fn rejects_invalid_database_pool_ranges() {
        let environment =
            replace_environment(valid_environment(), "DATABASE_POOL_MIN_CONNECTIONS", "13");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "DATABASE_POOL_MIN_CONNECTIONS",
                ..
            })
        ));

        let environment =
            replace_environment(valid_environment(), "DATABASE_POOL_MAX_CONNECTIONS", "0");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "DATABASE_POOL_MAX_CONNECTIONS",
                ..
            })
        ));
    }

    #[test]
    fn ftp_asset_mode_requires_an_endpoint() {
        let environment = replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "ftp");

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_FTP_ENDPOINT"
            })
        ));
    }

    #[test]
    fn parses_selected_roles_and_worker_concurrency() {
        let mut environment = valid_environment();
        environment.extend([
            ("APP_ROLES".to_owned(), " api,worker ".to_owned()),
            ("WORKER_CONCURRENCY".to_owned(), "12".to_owned()),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        assert!(config.roles.contains(AppRole::Api));
        assert!(!config.roles.contains(AppRole::Scheduler));
        assert!(config.roles.contains(AppRole::Worker));
        assert_eq!(config.worker_concurrency, 12);
    }

    #[test]
    fn parses_and_validates_the_optional_public_feed_url() {
        let environment = replace_environment(
            valid_environment(),
            "SERVER_ROOT_URL",
            " https://feeds.example.test/werrss.xml?source=public ",
        );

        let config = AppConfig::from_env_iter(environment).unwrap();
        assert_eq!(
            config.server_root_url.as_ref().map(Url::as_str),
            Some("https://feeds.example.test/werrss.xml?source=public")
        );
    }

    #[test]
    fn rejects_non_public_http_feed_urls() {
        for value in [
            "",
            "ftp://feeds.example.test/feed.xml",
            "http://",
            "https://user:password@feeds.example.test/feed.xml",
        ] {
            let environment = replace_environment(valid_environment(), "SERVER_ROOT_URL", value);
            let result = AppConfig::from_env_iter(environment);
            match result {
                Err(ConfigError::InvalidValue {
                    variable: "SERVER_ROOT_URL",
                    ..
                }) => {}
                other => panic!("unexpected result for {value:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn rejects_empty_unknown_and_mixed_all_roles() {
        for value in ["", "unknown", "all,worker", "api,"] {
            let environment = replace_environment(valid_environment(), "APP_ROLES", value);
            let result = AppConfig::from_env_iter(environment);
            assert!(matches!(
                result,
                Err(ConfigError::InvalidValue {
                    variable: "APP_ROLES",
                    ..
                })
            ));
        }
    }

    #[test]
    fn rejects_zero_and_oversized_worker_concurrency() {
        for value in ["0", "1025"] {
            let environment = replace_environment(valid_environment(), "WORKER_CONCURRENCY", value);
            assert!(matches!(
                AppConfig::from_env_iter(environment),
                Err(ConfigError::InvalidValue {
                    variable: "WORKER_CONCURRENCY",
                    ..
                })
            ));
        }
    }

    #[test]
    fn validates_account_and_feed_build_lease_ordering() {
        let environment = [
            ("ACCOUNT_LEASE_SECONDS", "60"),
            ("ACCOUNT_HEARTBEAT_SECONDS", "60"),
        ]
        .into_iter()
        .fold(valid_environment(), |environment, (key, value)| {
            replace_environment(environment, key, value)
        });
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ACCOUNT_LEASE_SECONDS",
                ..
            })
        ));

        let environment = [
            ("FEED_BUILD_LEASE_SECONDS", "59"),
            ("FEED_BUILD_HEARTBEAT_SECONDS", "60"),
        ]
        .into_iter()
        .fold(valid_environment(), |environment, (key, value)| {
            replace_environment(environment, key, value)
        });
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "FEED_BUILD_LEASE_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn accepts_zero_cooldown_but_rejects_oversized_cache_waits() {
        let environment = [
            ("SOURCE_FAILURE_COOLDOWN_SECONDS", "0"),
            ("RSS_STALE_WHILE_REVALIDATE_SECONDS", "86400"),
            ("RSS_CACHE_MISS_WAIT_MS", "60000"),
        ]
        .into_iter()
        .fold(valid_environment(), |environment, (key, value)| {
            replace_environment(environment, key, value)
        });
        let config = AppConfig::from_env_iter(environment).unwrap();
        assert_eq!(config.source_failure_cooldown, Duration::ZERO);
        assert_eq!(
            config.rss_stale_while_revalidate,
            Duration::from_secs(86_400)
        );
        assert_eq!(config.rss_cache_miss_wait, Duration::from_secs(60));

        for (key, value) in [
            ("RSS_CACHE_MISS_WAIT_MS", "0"),
            ("RSS_CACHE_MISS_WAIT_MS", "60001"),
            ("RSS_STALE_WHILE_REVALIDATE_SECONDS", "86401"),
            ("SOURCE_FAILURE_COOLDOWN_SECONDS", "604801"),
        ] {
            let environment = replace_environment(valid_environment(), key, value);
            assert!(matches!(
                AppConfig::from_env_iter(environment),
                Err(ConfigError::InvalidValue { variable, .. }) if variable == key
            ));
        }
    }

    #[test]
    fn rejects_unbounded_scroll_limits_before_building_the_policy() {
        for (variable, value) in [
            ("SCROLL_MAX_STEPS", (MAX_SCROLL_STEPS + 1).to_string()),
            ("SCROLL_MAX_PIXELS", (MAX_SCROLL_PIXELS + 1).to_string()),
        ] {
            let environment = replace_environment(valid_environment(), variable, &value);
            assert!(matches!(
                AppConfig::from_env_iter(environment),
                Err(ConfigError::InvalidValue { variable: actual, .. }) if actual == variable
            ));
        }
    }

    #[test]
    fn requires_complete_admin_credentials_only_when_enabled() {
        let environment = replace_environment(valid_environment(), "ADMIN_ENABLED", "true");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ADMIN_USERNAME"
            })
        ));

        let mut environment = replace_environment(valid_environment(), "ADMIN_ENABLED", "true");
        environment.extend([
            ("ADMIN_USERNAME".to_owned(), "admin".to_owned()),
            ("ADMIN_PASSWORD".to_owned(), "admin-password".to_owned()),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "SESSION_SIGNING_KEY"
            })
        ));

        let mut environment = replace_environment(valid_environment(), "ADMIN_ENABLED", "true");
        environment.extend([
            ("ADMIN_USERNAME".to_owned(), "admin".to_owned()),
            ("ADMIN_PASSWORD".to_owned(), "admin-password".to_owned()),
            ("SESSION_SIGNING_KEY".to_owned(), "session-key".to_owned()),
        ]);
        let config = AppConfig::from_env_iter(environment).unwrap();
        assert!(config.admin_enabled);
        assert_eq!(config.admin_username.as_deref(), Some("admin"));
        assert_eq!(
            config.admin_password.unwrap().expose_secret(),
            "admin-password"
        );
        assert_eq!(
            config.session_signing_key.unwrap().expose_secret(),
            "session-key"
        );
    }

    #[test]
    fn rejects_shared_session_secret_and_invalid_admin_switch() {
        let mut environment = replace_environment(valid_environment(), "ADMIN_ENABLED", "yes");
        environment.extend([
            ("ADMIN_PASSWORD".to_owned(), "admin-password".to_owned()),
            ("SESSION_SIGNING_KEY".to_owned(), "session-key".to_owned()),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ADMIN_ENABLED",
                ..
            })
        ));

        let mut environment = replace_environment(valid_environment(), "ADMIN_ENABLED", "true");
        environment.extend([
            ("ADMIN_USERNAME".to_owned(), "admin".to_owned()),
            ("ADMIN_PASSWORD".to_owned(), "same-secret".to_owned()),
            ("SESSION_SIGNING_KEY".to_owned(), "same-secret".to_owned()),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "SESSION_SIGNING_KEY",
                ..
            })
        ));
    }

    #[test]
    fn parses_database_asset_mode_and_all_cache_limits() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", " database ");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test/werrss".to_owned(),
            ),
            ("ASSET_CACHE_MAX_SIZE_MB".to_owned(), "12".to_owned()),
            ("ASSET_CACHE_MAX_AGE_DAYS".to_owned(), "0".to_owned()),
            ("ASSET_MAX_SIZE_MB".to_owned(), "2".to_owned()),
            ("ASSET_MAX_COUNT_PER_ARTICLE".to_owned(), "7".to_owned()),
            (
                "ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB".to_owned(),
                "8".to_owned(),
            ),
            (
                "ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS".to_owned(),
                "40".to_owned(),
            ),
            ("ASSET_FETCH_TIMEOUT_SECONDS".to_owned(), "10".to_owned()),
            ("ASSET_MAX_REDIRECTS".to_owned(), "2".to_owned()),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let AssetArchiveConfig::Database {
            policy,
            repair_policy,
        } = config.asset_archive
        else {
            panic!("database backend should be selected");
        };
        assert_eq!(policy.max_cache_size_bytes(), 12_000_000);
        assert_eq!(policy.max_age(), Duration::ZERO);
        assert_eq!(policy.max_asset_size_bytes(), 2_000_000);
        assert_eq!(policy.max_count_per_article(), 7);
        assert_eq!(policy.max_fetch_bytes_per_article(), 8_000_000);
        assert_eq!(policy.max_fetch_time_per_article(), Duration::from_secs(40));
        assert_eq!(policy.fetch_timeout(), Duration::from_secs(10));
        assert_eq!(policy.max_redirects(), 2);
        assert_eq!(repair_policy.max_pending(), 100);
        assert_eq!(repair_policy.public_max_pending(), 50);
        assert_eq!(repair_policy.requeue_after(), Duration::from_secs(60));
        assert_eq!(repair_policy.max_attempts(), 3);
        assert!(config.asset_use_absolute_urls);
    }

    #[test]
    fn database_asset_mode_requires_server_root_for_default_absolute_urls() {
        let environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "database");

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "SERVER_ROOT_URL"
            })
        ));
    }

    #[test]
    fn database_asset_mode_can_keep_relative_urls_without_server_root() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "database");
        environment.push(("ASSET_USE_ABSOLUTE_URLS".to_owned(), "false".to_owned()));

        let config = AppConfig::from_env_iter(environment).unwrap();

        assert!(!config.asset_use_absolute_urls);
    }

    #[test]
    fn parses_local_asset_mode_and_requires_a_root() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "local");
        environment.extend([
            (
                "ASSET_LOCAL_ROOT".to_owned(),
                "/var/lib/werrss/assets".to_owned(),
            ),
            ("ASSET_USE_ABSOLUTE_URLS".to_owned(), "false".to_owned()),
        ]);
        let config = AppConfig::from_env_iter(environment).unwrap();
        assert_eq!(
            config.asset_archive.local_root(),
            Some("/var/lib/werrss/assets")
        );

        let environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "local");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_LOCAL_ROOT"
            })
        ));
    }

    #[test]
    fn local_asset_mode_requires_server_root_for_absolute_urls() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "local");
        environment.push((
            "ASSET_LOCAL_ROOT".to_owned(),
            "/var/lib/werrss/assets".to_owned(),
        ));

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "SERVER_ROOT_URL"
            })
        ));
    }

    #[test]
    fn parses_s3_asset_mode_without_exposing_credentials_in_debug() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "s3");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            ("ASSET_S3_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_S3_ENDPOINT".to_owned(),
                "http://minio.example.test:9000".to_owned(),
            ),
            ("ASSET_S3_REGION".to_owned(), "us-east-1".to_owned()),
            (
                "ASSET_S3_ACCESS_KEY_ID".to_owned(),
                "access-secret".to_owned(),
            ),
            (
                "ASSET_S3_SECRET_ACCESS_KEY".to_owned(),
                "secret-secret".to_owned(),
            ),
        ]);
        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.s3_storage().unwrap();
        assert_eq!(storage.bucket(), "werrss-assets");
        assert_eq!(storage.endpoint(), Some("http://minio.example.test:9000"));
        assert_eq!(storage.region(), "us-east-1");
        assert_eq!(storage.access_key_id(), "access-secret");
        assert_eq!(storage.secret_access_key(), "secret-secret");
        let debug = format!("{config:?}");
        assert!(!debug.contains("access-secret"));
        assert!(!debug.contains("secret-secret"));
    }

    #[test]
    fn s3_asset_mode_requires_all_connection_settings() {
        let environment = replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "s3");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_S3_BUCKET"
            })
        ));
    }

    #[test]
    fn parses_oss_asset_mode_without_exposing_credentials_in_debug() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                " http://oss.example.test:9000/// ".to_owned(),
            ),
            ("ASSET_OSS_ADDRESSING_STYLE".to_owned(), " PATH ".to_owned()),
            (
                "ASSET_OSS_ACCESS_KEY_ID".to_owned(),
                "access-secret".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_SECRET".to_owned(),
                "secret-secret".to_owned(),
            ),
            (
                "ASSET_OSS_SECURITY_TOKEN".to_owned(),
                "session-token".to_owned(),
            ),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.oss_storage().unwrap();
        assert_eq!(storage.bucket(), "werrss-assets");
        assert_eq!(storage.endpoint(), "http://oss.example.test:9000");
        assert_eq!(storage.addressing_style(), "path");
        assert_eq!(storage.access_key_id(), Some("access-secret"));
        assert_eq!(storage.access_key_secret(), Some("secret-secret"));
        assert_eq!(storage.security_token(), Some("session-token"));
        assert!(!storage.skip_signature());
        let debug = format!("{config:?}");
        assert!(!debug.contains("access-secret"));
        assert!(!debug.contains("secret-secret"));
        assert!(!debug.contains("session-token"));
    }

    #[test]
    fn parses_unsigned_oss_asset_mode_without_credentials() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                "http://oss.example.test:9000".to_owned(),
            ),
            ("ASSET_OSS_ADDRESSING_STYLE".to_owned(), "cname".to_owned()),
            ("ASSET_OSS_SKIP_SIGNATURE".to_owned(), "true".to_owned()),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.oss_storage().unwrap();
        assert_eq!(storage.access_key_id(), None);
        assert_eq!(storage.access_key_secret(), None);
        assert!(storage.skip_signature());
    }

    #[test]
    fn oss_asset_mode_requires_signed_credentials() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                "http://oss.example.test:9000".to_owned(),
            ),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_OSS_ACCESS_KEY_ID"
            })
        ));

        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                "http://oss.example.test:9000".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_ID".to_owned(),
                "access-secret".to_owned(),
            ),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_OSS_ACCESS_KEY_SECRET"
            })
        ));
    }

    #[test]
    fn rejects_invalid_oss_addressing_styles() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                "http://oss.example.test:9000".to_owned(),
            ),
            ("ASSET_OSS_ADDRESSING_STYLE".to_owned(), "host".to_owned()),
            (
                "ASSET_OSS_ACCESS_KEY_ID".to_owned(),
                "access-secret".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_SECRET".to_owned(),
                "secret-secret".to_owned(),
            ),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_OSS_ADDRESSING_STYLE",
                ..
            })
        ));
    }

    #[test]
    fn rejects_credentials_embedded_in_an_oss_endpoint() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                "http://access:secret@oss.example.test:9000".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_ID".to_owned(),
                "configured-access".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_SECRET".to_owned(),
                "configured-secret".to_owned(),
            ),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_OSS_ENDPOINT",
                ..
            })
        ));
    }

    #[test]
    fn rejects_oss_endpoint_paths_that_open_dal_drops() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                "https://proxy.example.test/oss/".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_ID".to_owned(),
                "configured-access".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_SECRET".to_owned(),
                "configured-secret".to_owned(),
            ),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_OSS_ENDPOINT",
                ..
            })
        ));
    }

    #[test]
    fn rejects_oss_endpoint_queries_that_open_dal_drops() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                "https://proxy.example.test/?tenant=werrss".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_ID".to_owned(),
                "configured-access".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_SECRET".to_owned(),
                "configured-secret".to_owned(),
            ),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_OSS_ENDPOINT",
                ..
            })
        ));
    }

    #[test]
    fn oss_asset_mode_requires_an_explicitly_unversioned_bucket() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
        environment.extend([
            ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_OSS_ENDPOINT".to_owned(),
                "https://oss.example.test".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_ID".to_owned(),
                "configured-access".to_owned(),
            ),
            (
                "ASSET_OSS_ACCESS_KEY_SECRET".to_owned(),
                "configured-secret".to_owned(),
            ),
        ]);
        environment.retain(|(key, _)| key != "ASSET_OSS_VERSIONING");

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_OSS_VERSIONING"
            })
        ));

        for versioning in ["enabled", "suspended"] {
            let mut environment =
                replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "oss");
            environment.extend([
                ("ASSET_OSS_BUCKET".to_owned(), "werrss-assets".to_owned()),
                (
                    "ASSET_OSS_ENDPOINT".to_owned(),
                    "https://oss.example.test".to_owned(),
                ),
                (
                    "ASSET_OSS_ACCESS_KEY_ID".to_owned(),
                    "configured-access".to_owned(),
                ),
                (
                    "ASSET_OSS_ACCESS_KEY_SECRET".to_owned(),
                    "configured-secret".to_owned(),
                ),
            ]);
            let environment = replace_environment(environment, "ASSET_OSS_VERSIONING", versioning);
            assert!(matches!(
                AppConfig::from_env_iter(environment),
                Err(ConfigError::InvalidValue {
                    variable: "ASSET_OSS_VERSIONING",
                    ..
                })
            ));
        }
    }

    #[test]
    fn parses_gcs_asset_mode_without_exposing_credentials_in_debug() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "gcs");
        let credential = valid_gcs_credential();
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            ("ASSET_GCS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_GCS_ENDPOINT".to_owned(),
                " http://gcs.example.test:4443/// ".to_owned(),
            ),
            ("ASSET_GCS_CREDENTIAL".to_owned(), credential.clone()),
        ]);
        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.gcs_storage().unwrap();
        assert_eq!(storage.bucket(), "werrss-assets");
        assert_eq!(storage.endpoint(), Some("http://gcs.example.test:4443"));
        assert_eq!(storage.credential(), Some(credential.as_str()));
        assert!(!storage.skip_signature());
        let debug = format!("{config:?}");
        assert!(!debug.contains(&credential));
    }

    #[test]
    fn gcs_asset_mode_requires_bucket_and_credential() {
        let environment = replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "gcs");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_GCS_BUCKET"
            })
        ));

        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "gcs");
        environment.push(("ASSET_GCS_BUCKET".to_owned(), "werrss-assets".to_owned()));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_GCS_CREDENTIAL"
            })
        ));
    }

    #[test]
    fn rejects_malformed_signed_gcs_credentials() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "gcs");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            ("ASSET_GCS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            ("ASSET_GCS_CREDENTIAL".to_owned(), "not-base64".to_owned()),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_GCS_CREDENTIAL",
                ..
            })
        ));
    }

    #[test]
    fn parses_unsigned_gcs_asset_mode_without_credentials() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "gcs");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            ("ASSET_GCS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            (
                "ASSET_GCS_ENDPOINT".to_owned(),
                "http://gcs.example.test:4443".to_owned(),
            ),
            ("ASSET_GCS_SKIP_SIGNATURE".to_owned(), "true".to_owned()),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.gcs_storage().unwrap();
        assert_eq!(storage.credential(), None);
        assert!(storage.skip_signature());
    }

    #[test]
    fn parses_ftp_asset_mode_and_redacts_connection_details() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "ftp");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_FTP_ENDPOINT".to_owned(),
                " ftps://ftp.example.test/ ".to_owned(),
            ),
            ("ASSET_FTP_ROOT".to_owned(), "/assets".to_owned()),
            ("ASSET_FTP_USER".to_owned(), "ftp-user-secret".to_owned()),
            (
                "ASSET_FTP_PASSWORD".to_owned(),
                " ftp-password-secret ".to_owned(),
            ),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.ftp_storage().unwrap();
        let debug = format!("{storage:?}");
        assert_eq!(storage.endpoint(), "ftps://ftp.example.test");
        assert_eq!(storage.root(), Some("/assets"));
        assert_eq!(storage.user(), Some("ftp-user-secret"));
        assert_eq!(storage.password(), Some(" ftp-password-secret "));
        assert!(!debug.contains("ftp.example.test"));
        assert!(!debug.contains("ftp-user-secret"));
        assert!(!debug.contains("ftp-password-secret"));
    }

    #[test]
    fn parses_sftp_asset_mode_and_redacts_connection_details() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "sftp");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_SFTP_ENDPOINT".to_owned(),
                "sftp://sftp.example.test:2222".to_owned(),
            ),
            ("ASSET_SFTP_ROOT".to_owned(), "/assets".to_owned()),
            ("ASSET_SFTP_USER".to_owned(), "sftp-user".to_owned()),
            (
                "ASSET_SFTP_KEY".to_owned(),
                "/run/secrets/ssh_key".to_owned(),
            ),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.sftp_storage().unwrap();
        let debug = format!("{storage:?}");
        assert_eq!(storage.endpoint(), "sftp://sftp.example.test:2222");
        assert_eq!(storage.root(), Some("/assets"));
        assert_eq!(storage.user(), "sftp-user");
        assert_eq!(storage.key(), "/run/secrets/ssh_key");
        assert!(!debug.contains("sftp.example.test"));
        assert!(!debug.contains("sftp-user"));
        assert!(!debug.contains("/run/secrets/ssh_key"));
    }

    #[test]
    fn parses_webdav_asset_mode_with_mount_path_and_redacts_credentials() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "webdav");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_WEBDAV_ENDPOINT".to_owned(),
                "https://dav.example.test/remote.php/dav/files".to_owned(),
            ),
            ("ASSET_WEBDAV_ROOT".to_owned(), "/assets".to_owned()),
            ("ASSET_WEBDAV_USER".to_owned(), "dav-user-secret".to_owned()),
            (
                "ASSET_WEBDAV_PASSWORD".to_owned(),
                "dav-password-secret".to_owned(),
            ),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.webdav_storage().unwrap();
        let debug = format!("{storage:?}");
        assert_eq!(
            storage.endpoint(),
            "https://dav.example.test/remote.php/dav/files"
        );
        assert_eq!(storage.root(), Some("/assets"));
        assert_eq!(storage.user(), Some("dav-user-secret"));
        assert_eq!(storage.password(), Some("dav-password-secret"));
        assert!(!debug.contains("dav.example.test"));
        assert!(!debug.contains("dav-user-secret"));
        assert!(!debug.contains("dav-password-secret"));
    }

    #[test]
    fn rejects_invalid_protocol_endpoints_and_partial_credentials() {
        let invalid_cases = [
            (
                "ftp",
                "ASSET_FTP_ENDPOINT",
                "http://ftp.example.test",
                None,
                None,
            ),
            (
                "ftp",
                "ASSET_FTP_ENDPOINT",
                "ftp://user:pass@ftp.example.test",
                None,
                None,
            ),
            (
                "ftp",
                "ASSET_FTP_ENDPOINT",
                "ftp://@ftp.example.test",
                None,
                None,
            ),
            (
                "ftp",
                "ASSET_FTP_ENDPOINT",
                "ftp://ftp.example.test/#fragment",
                None,
                None,
            ),
            (
                "ftp",
                "ASSET_FTP_ENDPOINT",
                "ftp://ftp.example.test/assets",
                None,
                None,
            ),
            (
                "ftp",
                "ASSET_FTP_ENDPOINT",
                "ftp://ftp.example.test?passive=true",
                None,
                None,
            ),
            (
                "ftp",
                "ASSET_FTP_PASSWORD",
                "ftp://ftp.example.test",
                Some(("ASSET_FTP_USER", "only-user")),
                None,
            ),
            (
                "sftp",
                "ASSET_SFTP_ENDPOINT",
                "https://sftp.example.test",
                Some(("ASSET_SFTP_USER", "user")),
                Some(("ASSET_SFTP_KEY", "/key")),
            ),
            (
                "sftp",
                "ASSET_SFTP_ENDPOINT",
                "sftp://user@sftp.example.test",
                Some(("ASSET_SFTP_USER", "user")),
                Some(("ASSET_SFTP_KEY", "/key")),
            ),
            (
                "sftp",
                "ASSET_SFTP_ENDPOINT",
                "sftp://sftp.example.test/assets",
                Some(("ASSET_SFTP_USER", "user")),
                Some(("ASSET_SFTP_KEY", "/key")),
            ),
            (
                "webdav",
                "ASSET_WEBDAV_ENDPOINT",
                "ftp://dav.example.test",
                None,
                None,
            ),
            (
                "webdav",
                "ASSET_WEBDAV_ENDPOINT",
                "https://user:pass@dav.example.test/dav",
                None,
                None,
            ),
            (
                "webdav",
                "ASSET_WEBDAV_ENDPOINT",
                "https://dav.example.test/dav#fragment",
                None,
                None,
            ),
            (
                "webdav",
                "ASSET_WEBDAV_ENDPOINT",
                "https://dav.example.test/dav?tenant=one",
                None,
                None,
            ),
            (
                "webdav",
                "ASSET_WEBDAV_PASSWORD",
                "https://dav.example.test/dav",
                Some(("ASSET_WEBDAV_USER", "only-user")),
                None,
            ),
            (
                "ftp",
                "ASSET_FTP_USER",
                "ftps://ftp.example.test",
                None,
                Some(("ASSET_FTP_PASSWORD", "only-password")),
            ),
            (
                "webdav",
                "ASSET_WEBDAV_USER",
                "https://dav.example.test/dav",
                None,
                Some(("ASSET_WEBDAV_PASSWORD", "only-password")),
            ),
        ];

        for (backend, variable, endpoint, first_credential, second_credential) in invalid_cases {
            let mut environment =
                replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", backend);
            environment.push((
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ));
            environment.push((
                match backend {
                    "ftp" => "ASSET_FTP_ENDPOINT",
                    "sftp" => "ASSET_SFTP_ENDPOINT",
                    _ => "ASSET_WEBDAV_ENDPOINT",
                }
                .to_owned(),
                endpoint.to_owned(),
            ));
            if let Some((name, value)) = first_credential {
                environment.push((name.to_owned(), value.to_owned()));
            }
            if let Some((name, value)) = second_credential {
                environment.push((name.to_owned(), value.to_owned()));
            }
            if backend == "sftp"
                && !environment
                    .iter()
                    .any(|(name, _)| name == "ASSET_SFTP_USER")
            {
                environment.extend([
                    ("ASSET_SFTP_USER".to_owned(), "user".to_owned()),
                    ("ASSET_SFTP_KEY".to_owned(), "/key".to_owned()),
                ]);
            }

            assert!(
                matches!(
                    AppConfig::from_env_iter(environment),
                    Err(ConfigError::InvalidValue { variable: actual, .. }) if actual == variable
                ),
                "backend {backend} should reject endpoint {endpoint} or partial credentials"
            );
        }
    }

    #[test]
    fn requires_protocol_specific_values() {
        for (backend, variable) in [
            ("ftp", "ASSET_FTP_ENDPOINT"),
            ("sftp", "ASSET_SFTP_ENDPOINT"),
            ("webdav", "ASSET_WEBDAV_ENDPOINT"),
        ] {
            let environment =
                replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", backend);
            assert!(matches!(
                AppConfig::from_env_iter(environment),
                Err(ConfigError::Missing { variable: actual }) if actual == variable
            ));
        }

        let mut sftp = replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "sftp");
        sftp.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_SFTP_ENDPOINT".to_owned(),
                "sftp://sftp.example.test".to_owned(),
            ),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(sftp),
            Err(ConfigError::Missing {
                variable: "ASSET_SFTP_USER"
            })
        ));
    }

    #[test]
    fn rejects_unknown_asset_protocol_environment_names() {
        let environment =
            replace_environment(valid_environment(), "ASSET_FTP_ENDPOINT_TYPO", "ftp://host");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::UnknownVariable { variable }) if variable == "ASSET_FTP_ENDPOINT_TYPO"
        ));
    }

    #[test]
    fn rejects_blank_sftp_usernames() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "sftp");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_SFTP_ENDPOINT".to_owned(),
                "sftp://sftp.example.test".to_owned(),
            ),
            ("ASSET_SFTP_USER".to_owned(), "   ".to_owned()),
            ("ASSET_SFTP_KEY".to_owned(), "/mounted/key".to_owned()),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_SFTP_USER"
            })
        ));
    }

    #[test]
    fn rejects_webdav_endpoint_queries() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "webdav");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_WEBDAV_ENDPOINT".to_owned(),
                "https://dav.example.test/dav?tenant=one".to_owned(),
            ),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_WEBDAV_ENDPOINT",
                ..
            })
        ));
    }

    #[test]
    fn rejects_missing_sftp_key_path() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "sftp");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_SFTP_ENDPOINT".to_owned(),
                "sftp://sftp.example.test".to_owned(),
            ),
            ("ASSET_SFTP_USER".to_owned(), "user".to_owned()),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_SFTP_KEY"
            })
        ));
    }

    #[test]
    fn rejects_relative_sftp_key_paths() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "sftp");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_SFTP_ENDPOINT".to_owned(),
                "sftp://sftp.example.test".to_owned(),
            ),
            ("ASSET_SFTP_USER".to_owned(), "user".to_owned()),
            ("ASSET_SFTP_KEY".to_owned(), "relative/key".to_owned()),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_SFTP_KEY",
                ..
            })
        ));
    }

    #[test]
    fn parses_azure_asset_mode_without_exposing_account_credentials() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "azure");
        let account_key = valid_azure_account_key();
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_AZURE_CONTAINER".to_owned(),
                "werrss-assets".to_owned(),
            ),
            (
                "ASSET_AZURE_ENDPOINT".to_owned(),
                " https://storage.example.test/// ".to_owned(),
            ),
            (
                "ASSET_AZURE_ACCOUNT_NAME".to_owned(),
                "devstoreaccount1".to_owned(),
            ),
            ("ASSET_AZURE_ACCOUNT_KEY".to_owned(), account_key.clone()),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.azure_storage().unwrap();
        assert_eq!(storage.container(), "werrss-assets");
        assert_eq!(storage.endpoint(), "https://storage.example.test");
        assert_eq!(storage.account_name(), Some("devstoreaccount1"));
        assert_eq!(storage.account_key(), Some(account_key.as_str()));
        assert!(!storage.skip_signature());
        let debug = format!("{config:?}");
        assert!(!debug.contains("devstoreaccount1"));
        assert!(!debug.contains(&account_key));
    }

    #[test]
    fn parses_unsigned_azure_asset_mode_without_account_credentials() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "azblob");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            (
                "ASSET_AZURE_CONTAINER".to_owned(),
                "werrss-assets".to_owned(),
            ),
            (
                "ASSET_AZURE_ENDPOINT".to_owned(),
                "http://azurite.example.test:10000/devstoreaccount1".to_owned(),
            ),
            ("ASSET_AZURE_SKIP_SIGNATURE".to_owned(), "true".to_owned()),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        let storage = config.asset_archive.azure_storage().unwrap();
        assert_eq!(storage.account_name(), None);
        assert_eq!(storage.account_key(), None);
        assert!(storage.skip_signature());
    }

    #[test]
    fn azure_asset_mode_requires_signed_account_settings() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "azure");
        environment.extend([
            (
                "ASSET_AZURE_CONTAINER".to_owned(),
                "werrss-assets".to_owned(),
            ),
            (
                "ASSET_AZURE_ENDPOINT".to_owned(),
                "http://azurite.example.test:10000/devstoreaccount1".to_owned(),
            ),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_AZURE_ACCOUNT_NAME"
            })
        ));

        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "azure");
        environment.extend([
            (
                "ASSET_AZURE_CONTAINER".to_owned(),
                "werrss-assets".to_owned(),
            ),
            (
                "ASSET_AZURE_ENDPOINT".to_owned(),
                "http://azurite.example.test:10000/devstoreaccount1".to_owned(),
            ),
            (
                "ASSET_AZURE_ACCOUNT_NAME".to_owned(),
                "devstoreaccount1".to_owned(),
            ),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_AZURE_ACCOUNT_KEY"
            })
        ));
    }

    #[test]
    fn rejects_malformed_azure_account_keys() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "azure");
        environment.extend([
            (
                "ASSET_AZURE_CONTAINER".to_owned(),
                "werrss-assets".to_owned(),
            ),
            (
                "ASSET_AZURE_ENDPOINT".to_owned(),
                "http://azurite.example.test:10000/devstoreaccount1".to_owned(),
            ),
            (
                "ASSET_AZURE_ACCOUNT_NAME".to_owned(),
                "devstoreaccount1".to_owned(),
            ),
            (
                "ASSET_AZURE_ACCOUNT_KEY".to_owned(),
                "not-base64".to_owned(),
            ),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_AZURE_ACCOUNT_KEY",
                ..
            })
        ));
    }

    #[test]
    fn unsigned_gcs_asset_mode_requires_an_explicit_endpoint() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "gcs");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            ("ASSET_GCS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            ("ASSET_GCS_SKIP_SIGNATURE".to_owned(), "true".to_owned()),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "ASSET_GCS_ENDPOINT"
            })
        ));
    }

    #[test]
    fn gcs_signature_setting_requires_a_boolean() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "gcs");
        environment.extend([
            ("ASSET_GCS_BUCKET".to_owned(), "werrss-assets".to_owned()),
            ("ASSET_GCS_CREDENTIAL".to_owned(), valid_gcs_credential()),
            (
                "ASSET_GCS_SKIP_SIGNATURE".to_owned(),
                "sometimes".to_owned(),
            ),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_GCS_SKIP_SIGNATURE",
                ..
            })
        ));
    }

    #[test]
    fn rejects_invalid_asset_absolute_url_setting() {
        let environment =
            replace_environment(valid_environment(), "ASSET_USE_ABSOLUTE_URLS", "sometimes");

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_USE_ABSOLUTE_URLS",
                ..
            })
        ));
    }

    #[test]
    fn accepts_postgres_as_the_database_asset_backend_alias() {
        let environment = replace_environment(
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "postgres"),
            "SERVER_ROOT_URL",
            "https://rss.example.test",
        );

        assert!(matches!(
            AppConfig::from_env_iter(environment).unwrap().asset_archive,
            AssetArchiveConfig::Database { .. }
        ));
    }

    #[test]
    fn parses_asset_repair_admission_limits() {
        let mut environment =
            replace_environment(valid_environment(), "ASSET_ARCHIVE_BACKEND", "database");
        environment.extend([
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://rss.example.test".to_owned(),
            ),
            ("ASSET_REPAIR_MAX_PENDING".to_owned(), "12".to_owned()),
            ("ASSET_REPAIR_PUBLIC_MAX_PENDING".to_owned(), "7".to_owned()),
            ("ASSET_REPAIR_REQUEUE_SECONDS".to_owned(), "45".to_owned()),
            ("ASSET_REPAIR_MAX_ATTEMPTS".to_owned(), "4".to_owned()),
        ]);
        let AssetArchiveConfig::Database { repair_policy, .. } =
            AppConfig::from_env_iter(environment).unwrap().asset_archive
        else {
            panic!("database backend should be selected");
        };

        assert_eq!(repair_policy.max_pending(), 12);
        assert_eq!(repair_policy.public_max_pending(), 7);
        assert_eq!(repair_policy.requeue_after(), Duration::from_secs(45));
        assert_eq!(repair_policy.max_attempts(), 4);
    }

    #[test]
    fn rejects_invalid_asset_cache_limits() {
        for (variable, value) in [
            ("ASSET_MAX_SIZE_MB", "0"),
            ("ASSET_MAX_COUNT_PER_ARTICLE", "0"),
            ("ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB", "0"),
            ("ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS", "0"),
            ("ASSET_FETCH_TIMEOUT_SECONDS", "0"),
        ] {
            assert!(matches!(
                AppConfig::from_env_iter(replace_environment(
                    valid_environment(),
                    variable,
                    value,
                )),
                Err(ConfigError::InvalidValue { variable: actual, .. }) if actual == variable
            ));
        }

        let mut environment = valid_environment();
        environment.extend([
            (
                "ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS".to_owned(),
                "10".to_owned(),
            ),
            ("ASSET_FETCH_TIMEOUT_SECONDS".to_owned(), "11".to_owned()),
        ]);
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ASSET_FETCH_TIMEOUT_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn rejects_legacy_and_unknown_owned_environment_names_but_ignores_runtime_variables() {
        let environment = [
            ("ARCHIVE_BACKEND".to_owned(), "local".to_owned()),
            (
                "HTTP_PROXY".to_owned(),
                "http://proxy.example.test".to_owned(),
            ),
        ]
        .into_iter()
        .fold(valid_environment(), |mut environment, value| {
            environment.push(value);
            environment
        });
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ARCHIVE_BACKEND",
                ..
            })
        ));

        let mut environment = valid_environment();
        environment.push(("APP_ROEL".to_owned(), "worker".to_owned()));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::UnknownVariable { variable }) if variable == "APP_ROEL"
        ));

        let mut environment = valid_environment();
        environment.push(("PATH".to_owned(), "/usr/bin".to_owned()));
        environment.push((
            "HTTP_PROXY".to_owned(),
            "http://proxy.example.test".to_owned(),
        ));
        assert!(AppConfig::from_env_iter(environment).is_ok());
    }

    #[test]
    fn rejects_zero_intervals_and_unknown_browser_engine() {
        let mut environment = valid_environment();
        environment.push(("JOB_POLL_SECONDS".to_owned(), "0".to_owned()));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "JOB_POLL_SECONDS",
                ..
            })
        ));

        let mut environment = valid_environment();
        environment.push(("BROWSER_ENGINE".to_owned(), "webkit".to_owned()));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "BROWSER_ENGINE",
                ..
            })
        ));

        let environment = replace_environment(valid_environment(), "HTTP_PORT", "0");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "HTTP_PORT",
                ..
            })
        ));

        let environment = replace_environment(valid_environment(), "HTTP_BIND", "   ");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "HTTP_BIND",
                ..
            })
        ));
    }

    #[test]
    fn accepts_a_configured_job_execution_timeout() {
        let environment =
            replace_environment(valid_environment(), "JOB_EXECUTION_TIMEOUT_SECONDS", "90");

        let config = AppConfig::from_env_iter(environment).unwrap();

        assert_eq!(config.job_execution_timeout, Duration::from_secs(90));
    }

    #[test]
    fn rejects_a_zero_job_execution_timeout() {
        let environment =
            replace_environment(valid_environment(), "JOB_EXECUTION_TIMEOUT_SECONDS", "0");

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "JOB_EXECUTION_TIMEOUT_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn rejects_an_oversized_job_execution_timeout() {
        let environment = replace_environment(
            valid_environment(),
            "JOB_EXECUTION_TIMEOUT_SECONDS",
            &(MAX_JOB_EXECUTION_TIMEOUT_SECONDS + 1).to_string(),
        );

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "JOB_EXECUTION_TIMEOUT_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn generates_distinct_instance_ids_when_not_configured() {
        let first = AppConfig::from_env_iter(valid_environment()).unwrap();
        let second = AppConfig::from_env_iter(valid_environment()).unwrap();

        assert_ne!(first.instance_id, second.instance_id);
        assert!(first.instance_id.starts_with("werrss-"));
        assert!(second.instance_id.starts_with("werrss-"));
    }

    #[test]
    fn rejects_a_lease_that_cannot_cover_heartbeat_and_page_operation() {
        let mut environment = replace_environment(valid_environment(), "JOB_LEASE_SECONDS", "40");
        environment.extend([
            ("JOB_HEARTBEAT_SECONDS".to_owned(), "20".to_owned()),
            ("SCROLL_MAX_OPERATION_SECONDS".to_owned(), "20".to_owned()),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "JOB_LEASE_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn rejects_oversized_delay_and_page_operation_values() {
        let mut environment = valid_environment();
        environment.push((
            "PACING_REQUEST_MEAN_MS".to_owned(),
            "100000000000000000000".to_owned(),
        ));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "PACING_DELAY_MEAN_MS",
                ..
            })
        ));

        let mut environment = valid_environment();
        environment.push(("SCROLL_MAX_OPERATION_SECONDS".to_owned(), "3601".to_owned()));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "SCROLL_MAX_OPERATION_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn accepts_firefox_and_custom_pacing_bounds() {
        let mut environment = valid_environment();
        environment.extend([
            ("BROWSER_ENGINE".to_owned(), "firefox".to_owned()),
            ("PACING_REQUEST_MEAN_MS".to_owned(), "2500".to_owned()),
            ("PACING_REQUEST_STDDEV_MS".to_owned(), "0".to_owned()),
            ("PACING_REQUEST_MIN_MS".to_owned(), "2000".to_owned()),
            ("PACING_REQUEST_MAX_MS".to_owned(), "3000".to_owned()),
            ("SCROLL_MAX_STEPS".to_owned(), "6".to_owned()),
            ("SCROLL_MAX_PIXELS".to_owned(), "5000".to_owned()),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        assert_eq!(config.browser_engine, BrowserEngine::Firefox);
        assert_eq!(config.pacing.max_scroll_steps(), 6);
        assert_eq!(config.pacing.max_scroll_pixels(), 5_000);
        assert_eq!(
            config
                .pacing
                .distribution(crate::domain::pacing::DelayKind::Request)
                .mean_ms,
            2_500.0
        );
    }

    #[test]
    fn parses_browser_profile_settings_from_environment() {
        let environment: Vec<(String, String)> = valid_environment()
            .into_iter()
            .chain([
                (
                    "BROWSER_USER_AGENT".to_owned(),
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"
                        .to_owned(),
                ),
                ("BROWSER_LOCALE".to_owned(), "en-US".to_owned()),
                ("BROWSER_VIEWPORT_WIDTH".to_owned(), "1440".to_owned()),
                ("BROWSER_VIEWPORT_HEIGHT".to_owned(), "900".to_owned()),
                (
                    "BROWSER_EXTRA_ARGS".to_owned(),
                    "--disable-features=SomeFeature --force-device-scale-factor=1".to_owned(),
                ),
            ])
            .collect();

        let config = AppConfig::from_env_iter(environment).unwrap();
        assert_eq!(
            config.browser_user_agent.as_deref(),
            Some(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"
            )
        );
        assert_eq!(config.browser_locale, "en-US");
        assert_eq!(config.browser_viewport_width, 1_440);
        assert_eq!(config.browser_viewport_height, 900);
        assert_eq!(
            config.browser_extra_args,
            vec![
                "--disable-features=SomeFeature".to_owned(),
                "--force-device-scale-factor=1".to_owned()
            ]
        );
    }

    #[test]
    fn rejects_invalid_browser_profile_settings() {
        for (variable, value) in [
            ("BROWSER_USER_AGENT", " "),
            ("BROWSER_LOCALE", "zh CN"),
            ("BROWSER_VIEWPORT_WIDTH", "0"),
            ("BROWSER_VIEWPORT_HEIGHT", "8193"),
            ("BROWSER_EXTRA_ARGS", "not-an-argument"),
            ("BROWSER_EXTRA_ARGS", "--user-agent=other"),
            ("BROWSER_EXTRA_ARGS", "--user-data-dir=/tmp/other-profile"),
        ] {
            let environment = replace_environment(valid_environment(), variable, value);
            assert!(
                matches!(
                    AppConfig::from_env_iter(environment),
                    Err(ConfigError::InvalidValue { variable: actual, .. }) if actual == variable
                ),
                "{variable}={value:?} should be rejected"
            );
        }
    }

    #[test]
    fn parses_complete_weread_settings() {
        let environment = valid_environment()
            .into_iter()
            .chain([
                (
                    "WEREAD_ACCOUNT_ID".to_owned(),
                    "00000000-0000-0000-0000-000000000001".to_owned(),
                ),
                (
                    "WEREAD_ARTICLE_LIST_URL".to_owned(),
                    "https://weread.qq.com/web/mp/articles?offset=0".to_owned(),
                ),
            ])
            .collect::<Vec<_>>();

        let config = AppConfig::from_env_iter(environment).unwrap();
        assert!(config.weread_source_sync_configured());
        assert_eq!(
            config.weread_account_id.unwrap().as_uuid().to_string(),
            "00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(
            config.weread_article_list_url.as_str(),
            "https://weread.qq.com/web/mp/articles?offset=0"
        );
    }

    #[test]
    fn accepts_the_current_weread_cover_endpoint() {
        let environment = replace_environment(
            valid_environment(),
            "WEREAD_ARTICLE_LIST_URL",
            "https://weread.qq.com/api/mp/cover?bookId=ignored",
        );

        let config = AppConfig::from_env_iter(environment).unwrap();
        assert_eq!(
            config.weread_article_list_url.as_str(),
            "https://weread.qq.com/api/mp/cover?bookId=ignored"
        );
    }

    #[test]
    fn accepts_panel_enrolled_account_selection_without_profile_settings() {
        let account_only = replace_environment(
            valid_environment(),
            "WEREAD_ACCOUNT_ID",
            "00000000-0000-0000-0000-000000000001",
        );
        assert!(AppConfig::from_env_iter(account_only).is_ok());

        for endpoint in [
            "http://weread.qq.com/web/mp/articles",
            "https://example.com/web/mp/articles",
            "https://weread.qq.com.evil.example/web/mp/articles",
            "https://weread.qq.com/web/mp/other",
            "https://weread.qq.com:8443/web/mp/articles",
            "https://user:password@weread.qq.com/web/mp/articles",
            "https://weread.qq.com/web/mp/articles#fragment",
            "https://i.weread.qq.com/web/mp/articles",
        ] {
            let environment =
                replace_environment(valid_environment(), "WEREAD_ARTICLE_LIST_URL", endpoint);
            assert!(
                matches!(
                    AppConfig::from_env_iter(environment),
                    Err(ConfigError::InvalidValue {
                        variable: "WEREAD_ARTICLE_LIST_URL",
                        ..
                    })
                ),
                "unsafe endpoint {endpoint:?} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_the_removed_authenticated_profile_setting() {
        let environment = replace_environment(
            valid_environment(),
            "BROWSER_AUTHENTICATED_PROFILE",
            "/var/lib/werrss/profile",
        );
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::UnknownVariable { variable })
                if variable == "BROWSER_AUTHENTICATED_PROFILE"
        ));
    }
}
