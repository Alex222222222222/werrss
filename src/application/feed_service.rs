//! Cache-first feed delivery and on-demand feed rebuilding.
//!
//! This module is the executable part of the feed request boundary. It reads
//! the persisted cache, honors conditional requests, and rebuilds an expired
//! or missing cache before returning the response. Rendering and fenced
//! publication remain separate use cases from cache reads, while the durable
//! `feed_rebuild` job remains available as a failure fallback.
//!
//! Responsibilities:
//!
//! - classify a database-clocked cache read as fresh or stale;
//! - return a typed cached, not-modified, or temporarily-unavailable result;
//! - recognize normal HTTP `If-None-Match` forms, including lists, weak
//!   validators, quoted validators, and `*`;
//! - rebuild a missing or expired cache synchronously before delivery, with a
//!   bounded wait when another builder already owns the build lease; and
//! - provide the v1 adapter from that port to the existing PostgreSQL `jobs`
//!   table.
//!
//! Non-responsibilities: feed-token lookup, source/article queries, RSS XML
//! rendering, browser work, source synchronization, asset handling, HTTP
//! header construction, and final job completion. Those concerns remain in
//! their owning modules. In particular, this service never calls Thirtyfour or
//! waits for a synchronization job.
//!
//! Expected interfaces: the web layer supplies a source id and an optional
//! `If-None-Match` value; a [`FeedCacheRepository`] performs cache reads;
//! [`FeedRebuilder`] performs the database-only render/publication operation;
//! and [`FeedRebuildQueue`] maps a failed request to the canonical active-job
//! dedupe key. `FeedTokenService` can be placed in front of this service
//! without changing its cache or rebuild contracts.
//!
//! Data flow: a fresh cache is returned immediately, optionally as 304 when
//! its ETag matches. A stale cache or miss invokes the rebuild capability and
//! reads the fresh cache back before constructing the response. The rebuild
//! service's distributed build lease provides cross-replica single-flight. If
//! rebuilding fails or the bounded wait expires, stale content remains
//! available and a cache miss returns `Retry-After` information.
//!
//! Failure behavior: cache storage errors are returned because no response
//! metadata can be trusted. Rebuild and queue errors are represented as
//! [`FeedRebuildStatus::Unavailable`]; stale content remains available, while
//! a miss returns `Retry-After` information. Failures should be logged or
//! metered by the application boundary without exposing database URLs or
//! credentials.
//!
//! PostgreSQL/high-availability considerations: production cache freshness is
//! computed by [`crate::persistence::repositories::feed_cache_repository::PostgresFeedCacheRepository`]
//! using PostgreSQL time, and the
//! v1 queue adapter uses the custom `jobs` table's partial unique index. The
//! service does not use a process-local mutex for distributed coordination.
//! PGMQ is intentionally not required for v1; it is documented as a possible
//! future transport optimization while the `jobs` row remains authoritative.
//!
//! The database-only rebuild orchestration lives in
//! [`super::feed_rebuild_service::FeedRebuildService`]. It is intentionally
//! invoked only for cache misses, expiry, or a persisted cache whose asset URL
//! format no longer matches the configured public feed format.

use std::{fmt, time::Duration as StdDuration};

use chrono::{Duration, Utc};
use serde_json::json;
use thiserror::Error;
use tokio::time::{sleep, Instant};
use url::Url;

use crate::{
    application::feed_rebuild_service::{FeedRebuildOutcome, FeedRebuilder},
    archive::url_rewriter::{
        contains_absolute_asset_urls_outside_root, contains_any_absolute_asset_urls,
        contains_relative_asset_urls,
    },
    domain::{
        feed::{FeedCache, FeedCacheRead},
        job::{JobType, NewJob},
        source::SourceId,
    },
    persistence::repositories::{
        feed_cache_repository::{FeedCacheRepository, FeedCacheRepositoryError},
        job_repository::{EnqueueResult, PostgresJobRepository},
    },
};

/// A cache request after the web layer has resolved its feed token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedRequest {
    /// Source whose persisted feed should be returned.
    pub source_id: SourceId,
    /// Raw HTTP `If-None-Match` value, if the client supplied one.
    pub if_none_match: Option<String>,
}

impl FeedRequest {
    /// Creates a cache request with an optional conditional validator.
    pub fn new(source_id: SourceId, if_none_match: Option<String>) -> Self {
        Self {
            source_id,
            if_none_match,
        }
    }
}

/// A cache freshness classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedCacheStatus {
    /// The cache revision matches the source and its expiry is still ahead of
    /// the PostgreSQL read timestamp.
    Fresh,
    /// The cache exists but is expired or represents an older source revision.
    Stale,
}

/// Outcome of the best-effort rebuild enqueue attempted by this service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedRebuildStatus {
    /// No rebuild was needed because the cache was fresh.
    NotNeeded,
    /// The request rebuilt and published the cache itself.
    Rebuilt,
    /// This request inserted the active rebuild job.
    Enqueued,
    /// A rebuild job already exists for this source.
    AlreadyActive,
    /// The cache response is still usable, but enqueueing failed.
    Unavailable,
}

/// Feed bytes and metadata returned from a cache lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedDelivery {
    /// The client's validator matches the cached ETag.
    NotModified {
        /// Cached document metadata. The body is omitted from the HTTP
        /// response, but remains available to the caller for headers.
        cache: FeedCache,
        /// Whether the cached row is current and unexpired.
        status: FeedCacheStatus,
        /// On-demand rebuild or fallback-queue result, if the row was stale.
        rebuild: FeedRebuildStatus,
    },
    /// Cached XML bytes should be returned with a 200 response.
    Cached {
        /// Cached XML and HTTP metadata.
        cache: FeedCache,
        /// Whether the cached row is current and unexpired.
        status: FeedCacheStatus,
        /// On-demand rebuild or fallback-queue result, if the row was stale.
        rebuild: FeedRebuildStatus,
    },
    /// No cache row was available and the caller should retry later.
    Unavailable {
        /// Suggested delay before trying the feed again.
        retry_after: Duration,
        /// On-demand rebuild or fallback-queue result for the cache miss.
        rebuild: FeedRebuildStatus,
    },
}

/// Settings for cache-miss and stale-cache response behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedServiceConfig {
    stale_while_revalidate: Duration,
    cache_miss_retry_after: Duration,
}

/// Describes the asset URL format required in delivered feed documents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedAssetUrlPolicy {
    server_root_url: Url,
    use_absolute_urls: bool,
}

impl FeedAssetUrlPolicy {
    /// Creates a policy for the configured public root and URL mode.
    pub fn new(server_root_url: Url, use_absolute_urls: bool) -> Self {
        Self {
            server_root_url,
            use_absolute_urls,
        }
    }

    fn is_compatible(&self, xml_bytes: &[u8]) -> bool {
        let xml = String::from_utf8_lossy(xml_bytes);
        if self.use_absolute_urls {
            !contains_relative_asset_urls(&xml)
                && !contains_absolute_asset_urls_outside_root(&xml, &self.server_root_url)
        } else {
            !contains_any_absolute_asset_urls(&xml)
        }
    }
}

impl FeedServiceConfig {
    /// Creates validated feed delivery settings.
    pub fn new(
        stale_while_revalidate: Duration,
        cache_miss_retry_after: Duration,
    ) -> Result<Self, FeedServiceConfigError> {
        if stale_while_revalidate < Duration::zero() {
            return Err(FeedServiceConfigError::InvalidStaleWindow);
        }
        if cache_miss_retry_after <= Duration::zero() {
            return Err(FeedServiceConfigError::InvalidMissRetryAfter);
        }
        Ok(Self {
            stale_while_revalidate,
            cache_miss_retry_after,
        })
    }

    /// Returns the stale-while-revalidate window for HTTP header construction.
    pub const fn stale_while_revalidate(self) -> Duration {
        self.stale_while_revalidate
    }

    /// Returns the retry delay used when no cache exists.
    pub const fn cache_miss_retry_after(self) -> Duration {
        self.cache_miss_retry_after
    }
}

impl Default for FeedServiceConfig {
    fn default() -> Self {
        Self {
            stale_while_revalidate: Duration::minutes(1),
            cache_miss_retry_after: Duration::seconds(5),
        }
    }
}

/// Invalid feed delivery timing configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FeedServiceConfigError {
    /// A stale-while-revalidate period cannot be negative.
    #[error("stale-while-revalidate duration must not be negative")]
    InvalidStaleWindow,
    /// A cache miss needs a positive retry delay.
    #[error("cache-miss retry duration must be positive")]
    InvalidMissRetryAfter,
}

/// A rebuild request passed from feed delivery to the durable queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedRebuildRequest {
    /// Source whose normalized records must be rendered.
    pub source_id: SourceId,
}

/// Result of inserting or finding a deduplicated rebuild job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedRebuildEnqueueResult {
    /// A new active job was inserted.
    Enqueued,
    /// An active job already owns the source's dedupe key.
    AlreadyActive,
}

/// Errors from a rebuild queue implementation.
#[derive(Debug, Error)]
pub enum FeedRebuildQueueError {
    /// The durable queue could not accept or inspect the request.
    #[error("feed rebuild queue unavailable: {0}")]
    Unavailable(String),
}

/// Minimal queue port needed by cache-first feed delivery.
#[async_trait::async_trait]
pub trait FeedRebuildQueue: Send + Sync {
    /// Enqueues one source rebuild under its active dedupe key.
    async fn enqueue_rebuild(
        &self,
        request: FeedRebuildRequest,
    ) -> Result<FeedRebuildEnqueueResult, FeedRebuildQueueError>;
}

/// Job settings used by the v1 PostgreSQL rebuild adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedRebuildJobConfig {
    /// Priority assigned to feed rebuild jobs.
    pub priority: i32,
    /// Retryable failure budget assigned to each rebuild job.
    pub max_attempts: u32,
}

impl FeedRebuildJobConfig {
    /// Creates validated settings for the custom `jobs` table adapter.
    pub fn new(priority: i32, max_attempts: u32) -> Result<Self, FeedRebuildJobConfigError> {
        if max_attempts == 0 {
            return Err(FeedRebuildJobConfigError::InvalidAttemptLimit);
        }
        Ok(Self {
            priority,
            max_attempts,
        })
    }
}

impl Default for FeedRebuildJobConfig {
    fn default() -> Self {
        Self {
            priority: 0,
            max_attempts: 3,
        }
    }
}

/// Invalid durable feed-rebuild job settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FeedRebuildJobConfigError {
    /// The job must be allowed at least one retryable failure.
    #[error("feed rebuild max attempts must be positive")]
    InvalidAttemptLimit,
}

/// PostgreSQL queue adapter that keeps the custom `jobs` table authoritative.
///
/// This adapter deliberately maps only enqueueing. Worker outcome transitions
/// still belong to the shared `UnitOfWork` so article, source, sync-run, cache,
/// and job writes can commit atomically.
#[derive(Clone)]
pub struct PostgresFeedRebuildQueue {
    jobs: PostgresJobRepository,
    config: FeedRebuildJobConfig,
}

impl fmt::Debug for PostgresFeedRebuildQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresFeedRebuildQueue")
            .field("jobs", &self.jobs)
            .field("config", &self.config)
            .finish()
    }
}

impl PostgresFeedRebuildQueue {
    /// Creates a feed rebuild queue over the v1 custom PostgreSQL jobs table.
    pub fn new(jobs: PostgresJobRepository, config: FeedRebuildJobConfig) -> Self {
        Self { jobs, config }
    }
}

#[async_trait::async_trait]
impl FeedRebuildQueue for PostgresFeedRebuildQueue {
    async fn enqueue_rebuild(
        &self,
        request: FeedRebuildRequest,
    ) -> Result<FeedRebuildEnqueueResult, FeedRebuildQueueError> {
        // The PostgreSQL repository replaces these domain-input timestamps
        // with one statement-local database timestamp before inserting.
        let now = Utc::now();
        let result = self
            .jobs
            .enqueue_immediately(NewJob {
                job_type: JobType::FeedRebuild,
                source_id: Some(request.source_id.as_uuid()),
                priority: self.config.priority,
                run_after: now,
                max_attempts: self.config.max_attempts,
                payload: json!({"source_id": request.source_id.to_string()}),
                dedupe_key: format!("feed_rebuild:{}", request.source_id),
                now,
            })
            .await
            .map_err(|error| FeedRebuildQueueError::Unavailable(error.to_string()))?;

        Ok(match result {
            EnqueueResult::Inserted(_) => FeedRebuildEnqueueResult::Enqueued,
            EnqueueResult::AlreadyActive { .. } => FeedRebuildEnqueueResult::AlreadyActive,
        })
    }
}

/// Errors returned by cache-first feed delivery.
#[derive(Debug, Error)]
pub enum FeedServiceError {
    /// A nil source id cannot identify a cache or rebuild job.
    #[error("source id must not be nil")]
    InvalidSourceId,
    /// The cache repository failed to provide a trustworthy read.
    #[error(transparent)]
    Cache(#[from] FeedCacheRepositoryError),
}

/// Cache-first feed delivery service.
pub struct FeedService<C, Q, B> {
    cache: C,
    rebuild_queue: Q,
    rebuilder: B,
    config: FeedServiceConfig,
    asset_url_policy: Option<FeedAssetUrlPolicy>,
}

impl<C, Q, B> FeedService<C, Q, B>
where
    C: FeedCacheRepository,
    Q: FeedRebuildQueue,
    B: FeedRebuilder,
{
    /// Creates a service over a cache reader, durable fallback queue, and
    /// synchronous rebuild capability.
    pub fn new(cache: C, rebuild_queue: Q, rebuilder: B, config: FeedServiceConfig) -> Self {
        Self {
            cache,
            rebuild_queue,
            rebuilder,
            config,
            asset_url_policy: None,
        }
    }

    /// Configures the asset URL format that persisted feeds must contain.
    pub fn with_asset_url_policy(mut self, policy: Option<FeedAssetUrlPolicy>) -> Self {
        self.asset_url_policy = policy;
        self
    }

    /// Returns fresh feed bytes, rebuilding a missing or expired cache first.
    #[tracing::instrument(skip_all, level = "debug", fields(source_id = %request.source_id))]
    pub async fn get_feed(&self, request: FeedRequest) -> Result<FeedDelivery, FeedServiceError> {
        validate_source_id(request.source_id)?;
        let read = self.read_cache(request.source_id).await?;
        match read {
            Some(read) if read.is_fresh() && self.cache_is_compatible(read.cache()) => {
                Ok(cached_delivery(
                    &request,
                    read.cache().clone(),
                    FeedCacheStatus::Fresh,
                    FeedRebuildStatus::NotNeeded,
                ))
            }
            Some(read) => {
                let compatible = self.cache_is_compatible(read.cache());
                if compatible {
                    tracing::debug!("feed cache is expired; rebuilding before delivery");
                } else {
                    tracing::debug!(
                        "feed cache asset URL format is incompatible; rebuilding before delivery"
                    );
                }
                self.rebuild_and_deliver(request, compatible.then(|| read.cache().clone()))
                    .await
            }
            None => {
                tracing::debug!("feed cache is missing; rebuilding before delivery");
                self.rebuild_and_deliver(request, None).await
            }
        }
    }

    /// Returns the configured stale-while-revalidate window for HTTP mapping.
    pub const fn stale_while_revalidate(&self) -> Duration {
        self.config.stale_while_revalidate()
    }

    async fn read_cache(
        &self,
        source_id: SourceId,
    ) -> Result<Option<FeedCacheRead>, FeedServiceError> {
        tracing::trace!(%source_id, "looking up feed cache");
        self.cache.get(source_id).await.map_err(|error| {
            tracing::warn!(%source_id, error = %error, "feed cache lookup failed");
            FeedServiceError::Cache(error)
        })
    }

    async fn rebuild_and_deliver(
        &self,
        request: FeedRequest,
        stale_cache: Option<FeedCache>,
    ) -> Result<FeedDelivery, FeedServiceError> {
        let source_id = request.source_id;
        let rebuild = match self.rebuilder.rebuild(source_id).await {
            Ok(FeedRebuildOutcome::AlreadyActive) => {
                tracing::debug!(
                    %source_id,
                    "feed rebuild is already active; waiting for its cache"
                );
                FeedRebuildStatus::AlreadyActive
            }
            Ok(outcome) => {
                tracing::debug!(%source_id, outcome = ?outcome, "on-demand feed rebuild completed");
                FeedRebuildStatus::Rebuilt
            }
            Err(error) => {
                tracing::warn!(%source_id, error = %error, "on-demand feed rebuild failed");
                self.enqueue_rebuild(source_id).await
            }
        };

        let fresh = if matches!(
            rebuild,
            FeedRebuildStatus::Rebuilt | FeedRebuildStatus::AlreadyActive
        ) {
            self.wait_for_fresh_cache(source_id).await?
        } else {
            None
        };
        if let Some(read) = fresh {
            return Ok(cached_delivery(
                &request,
                read.cache().clone(),
                FeedCacheStatus::Fresh,
                rebuild,
            ));
        }

        if let Some(cache) = stale_cache {
            tracing::warn!(
                %source_id,
                rebuild = ?rebuild,
                "fresh feed cache unavailable; serving expired cache"
            );
            return Ok(cached_delivery(
                &request,
                cache,
                FeedCacheStatus::Stale,
                rebuild,
            ));
        }

        tracing::warn!(
            %source_id,
            rebuild = ?rebuild,
            "feed rebuild did not produce a cache"
        );
        Ok(FeedDelivery::Unavailable {
            retry_after: self.config.cache_miss_retry_after(),
            rebuild,
        })
    }

    async fn wait_for_fresh_cache(
        &self,
        source_id: SourceId,
    ) -> Result<Option<FeedCacheRead>, FeedServiceError> {
        let wait_for = self
            .config
            .cache_miss_retry_after()
            .to_std()
            .unwrap_or_else(|_| StdDuration::from_secs(1));
        let deadline = Instant::now() + wait_for;
        loop {
            let read = self.read_cache(source_id).await?;
            if read
                .as_ref()
                .is_some_and(|read| read.is_fresh() && self.cache_is_compatible(read.cache()))
            {
                return Ok(read);
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let remaining = deadline - now;
            sleep(remaining.min(StdDuration::from_millis(25))).await;
        }
    }

    async fn enqueue_rebuild(&self, source_id: SourceId) -> FeedRebuildStatus {
        match self
            .rebuild_queue
            .enqueue_rebuild(FeedRebuildRequest { source_id })
            .await
        {
            Ok(FeedRebuildEnqueueResult::Enqueued) => FeedRebuildStatus::Enqueued,
            Ok(FeedRebuildEnqueueResult::AlreadyActive) => FeedRebuildStatus::AlreadyActive,
            Err(error) => {
                tracing::warn!(%source_id, error = %error, "unable to enqueue feed rebuild");
                FeedRebuildStatus::Unavailable
            }
        }
    }

    fn cache_is_compatible(&self, cache: &FeedCache) -> bool {
        self.asset_url_policy
            .as_ref()
            .is_none_or(|policy| policy.is_compatible(cache.xml_bytes()))
    }
}

fn cached_delivery(
    request: &FeedRequest,
    cache: FeedCache,
    status: FeedCacheStatus,
    rebuild: FeedRebuildStatus,
) -> FeedDelivery {
    tracing::debug!(
        source_id = %cache.source_id(),
        status = ?status,
        feed_revision = ?cache.feed_revision(),
        rebuild = ?rebuild,
        "feed cache delivery prepared"
    );
    if if_none_match_matches(request.if_none_match.as_deref(), cache.etag()) {
        FeedDelivery::NotModified {
            cache,
            status,
            rebuild,
        }
    } else {
        FeedDelivery::Cached {
            cache,
            status,
            rebuild,
        }
    }
}

fn validate_source_id(source_id: SourceId) -> Result<(), FeedServiceError> {
    if source_id.as_uuid().is_nil() {
        Err(FeedServiceError::InvalidSourceId)
    } else {
        Ok(())
    }
}

/// Matches an HTTP `If-None-Match` value against the internal unquoted ETag.
fn if_none_match_matches(header: Option<&str>, etag: &str) -> bool {
    header.is_some_and(|header| {
        header.split(',').any(|candidate| {
            let candidate = candidate.trim();
            if candidate == "*" {
                return true;
            }
            let candidate = candidate.strip_prefix("W/").unwrap_or(candidate).trim();
            candidate.trim_matches('"') == etag
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, TimeZone};
    use tokio::sync::Mutex;
    use url::Url;
    use uuid::Uuid;

    use super::*;
    use crate::{
        application::feed_rebuild_service::FeedRebuildError,
        domain::{
            feed::{FeedCacheCandidate, FeedCacheRead},
            source::FeedRevision,
        },
    };

    #[derive(Clone)]
    struct TestCache {
        read: Arc<Mutex<Option<FeedCacheRead>>>,
        fail: bool,
    }

    impl TestCache {
        fn hit(read: FeedCacheRead) -> Self {
            Self {
                read: Arc::new(Mutex::new(Some(read))),
                fail: false,
            }
        }

        fn miss() -> Self {
            Self {
                read: Arc::new(Mutex::new(None)),
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                read: Arc::new(Mutex::new(None)),
                fail: true,
            }
        }
    }

    #[derive(Clone)]
    enum TestRebuilderMode {
        Publish(FeedCacheRead),
        AlreadyActive,
        Failing,
    }

    #[derive(Clone)]
    struct TestRebuilder {
        mode: TestRebuilderMode,
        cache: Option<Arc<Mutex<Option<FeedCacheRead>>>>,
        calls: Arc<Mutex<usize>>,
    }

    impl TestRebuilder {
        fn publishing(cache: &TestCache, read: FeedCacheRead) -> Self {
            Self {
                mode: TestRebuilderMode::Publish(read),
                cache: Some(Arc::clone(&cache.read)),
                calls: Arc::new(Mutex::new(0)),
            }
        }

        fn active() -> Self {
            Self {
                mode: TestRebuilderMode::AlreadyActive,
                cache: None,
                calls: Arc::new(Mutex::new(0)),
            }
        }

        fn failing() -> Self {
            Self {
                mode: TestRebuilderMode::Failing,
                cache: None,
                calls: Arc::new(Mutex::new(0)),
            }
        }

        async fn call_count(&self) -> usize {
            *self.calls.lock().await
        }
    }

    #[async_trait::async_trait]
    impl FeedRebuilder for TestRebuilder {
        async fn rebuild(
            &self,
            source_id: SourceId,
        ) -> Result<FeedRebuildOutcome, FeedRebuildError> {
            *self.calls.lock().await += 1;
            match &self.mode {
                TestRebuilderMode::Publish(read) => {
                    *self
                        .cache
                        .as_ref()
                        .expect("publishing cache is configured")
                        .lock()
                        .await = Some(read.clone());
                    Ok(FeedRebuildOutcome::Published {
                        feed_revision: read.cache().feed_revision(),
                    })
                }
                TestRebuilderMode::AlreadyActive => Ok(FeedRebuildOutcome::AlreadyActive),
                TestRebuilderMode::Failing => Err(FeedRebuildError::SourceNotFound { source_id }),
            }
        }
    }

    #[async_trait::async_trait]
    impl FeedCacheRepository for TestCache {
        async fn get(
            &self,
            _source_id: SourceId,
        ) -> Result<Option<FeedCacheRead>, FeedCacheRepositoryError> {
            if self.fail {
                return Err(FeedCacheRepositoryError::Storage("test failure".to_owned()));
            }
            Ok(self.read.lock().await.clone())
        }
    }

    #[derive(Clone)]
    struct TestQueue {
        requests: Arc<Mutex<Vec<FeedRebuildRequest>>>,
        outcome: Result<FeedRebuildEnqueueResult, String>,
    }

    impl TestQueue {
        fn successful(outcome: FeedRebuildEnqueueResult) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                outcome: Ok(outcome),
            }
        }

        fn failing() -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                outcome: Err("test queue failure".to_owned()),
            }
        }

        async fn request_count(&self) -> usize {
            self.requests.lock().await.len()
        }
    }

    #[async_trait::async_trait]
    impl FeedRebuildQueue for TestQueue {
        async fn enqueue_rebuild(
            &self,
            request: FeedRebuildRequest,
        ) -> Result<FeedRebuildEnqueueResult, FeedRebuildQueueError> {
            self.requests.lock().await.push(request);
            self.outcome
                .clone()
                .map_err(FeedRebuildQueueError::Unavailable)
        }
    }

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn cache_read(fresh: bool) -> FeedCacheRead {
        cache_read_with_xml(fresh, b"<rss/>")
    }

    fn cache_read_with_xml(fresh: bool, xml_bytes: &[u8]) -> FeedCacheRead {
        let source_id = source_id();
        let generated_at = at(10);
        let cache = FeedCache::from_candidate(
            FeedCacheCandidate::from_parts(
                source_id,
                xml_bytes.to_vec(),
                "sha256:test".to_owned(),
                generated_at,
                at(40),
                FeedRevision::from_u64(1),
                "test-hash".to_owned(),
            ),
            generated_at,
        );
        FeedCacheRead::from_parts(cache, FeedRevision::from_u64(1), fresh)
    }

    #[tokio::test]
    async fn fresh_matching_etag_returns_not_modified_without_enqueueing() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let rebuilder = TestRebuilder::active();
        let service = FeedService::new(
            TestCache::hit(cache_read(true)),
            queue.clone(),
            rebuilder.clone(),
            FeedServiceConfig::default(),
        );

        let delivery = service
            .get_feed(FeedRequest::new(
                source_id(),
                Some("W/\"sha256:test\", \"other\"".to_owned()),
            ))
            .await
            .expect("cache read should succeed");

        assert!(matches!(
            delivery,
            FeedDelivery::NotModified {
                status: FeedCacheStatus::Fresh,
                rebuild: FeedRebuildStatus::NotNeeded,
                ..
            }
        ));
        assert_eq!(queue.request_count().await, 0);
        assert_eq!(rebuilder.call_count().await, 0);
    }

    #[tokio::test]
    async fn fresh_relative_cache_is_rebuilt_for_absolute_asset_urls() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let cache = TestCache::hit(cache_read_with_xml(
            true,
            b"<img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
        ));
        let rebuilder = TestRebuilder::publishing(
            &cache,
            cache_read_with_xml(
                true,
                b"<img src=\"https://rss.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
            ),
        );
        let service = FeedService::new(
            cache,
            queue,
            rebuilder.clone(),
            FeedServiceConfig::default(),
        )
        .with_asset_url_policy(Some(FeedAssetUrlPolicy::new(
            Url::parse("https://rss.example.test/werrss").unwrap(),
            true,
        )));

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("incompatible fresh cache should be rebuilt");

        assert!(matches!(
            delivery,
            FeedDelivery::Cached {
                cache,
                status: FeedCacheStatus::Fresh,
                rebuild: FeedRebuildStatus::Rebuilt,
            } if String::from_utf8_lossy(cache.xml_bytes()).contains("https://rss.example.test/werrss/assets/")
        ));
        assert_eq!(rebuilder.call_count().await, 1);
    }

    #[tokio::test]
    async fn fresh_absolute_cache_is_rebuilt_for_relative_asset_urls() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let cache = TestCache::hit(cache_read_with_xml(
            true,
            b"<img src=\"https://rss.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
        ));
        let rebuilder = TestRebuilder::publishing(
            &cache,
            cache_read_with_xml(
                true,
                b"<img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
            ),
        );
        let service = FeedService::new(
            cache,
            queue,
            rebuilder.clone(),
            FeedServiceConfig::default(),
        )
        .with_asset_url_policy(Some(FeedAssetUrlPolicy::new(
            Url::parse("https://rss.example.test/werrss").unwrap(),
            false,
        )));

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("incompatible fresh cache should be rebuilt");

        assert!(matches!(
            delivery,
            FeedDelivery::Cached {
                cache,
                status: FeedCacheStatus::Fresh,
                rebuild: FeedRebuildStatus::Rebuilt,
            } if String::from_utf8_lossy(cache.xml_bytes()).contains("src=\"/assets/")
        ));
        assert_eq!(rebuilder.call_count().await, 1);
    }

    #[tokio::test]
    async fn fresh_absolute_cache_from_a_previous_root_is_rebuilt_for_absolute_asset_urls() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let cache = TestCache::hit(cache_read_with_xml(
            true,
            b"<img src=\"https://old.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
        ));
        let rebuilder = TestRebuilder::publishing(
            &cache,
            cache_read_with_xml(
                true,
                b"<img src=\"https://rss.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
            ),
        );
        let service = FeedService::new(
            cache,
            queue,
            rebuilder.clone(),
            FeedServiceConfig::default(),
        )
        .with_asset_url_policy(Some(FeedAssetUrlPolicy::new(
            Url::parse("https://rss.example.test/werrss").unwrap(),
            true,
        )));

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("cache from a previous root should be rebuilt");

        assert!(matches!(
            delivery,
            FeedDelivery::Cached {
                cache,
                status: FeedCacheStatus::Fresh,
                rebuild: FeedRebuildStatus::Rebuilt,
            } if String::from_utf8_lossy(cache.xml_bytes()).contains("https://rss.example.test/werrss/assets/")
        ));
        assert_eq!(rebuilder.call_count().await, 1);
    }

    #[tokio::test]
    async fn fresh_absolute_cache_from_a_previous_root_is_rebuilt_for_relative_asset_urls() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let cache = TestCache::hit(cache_read_with_xml(
            true,
            b"<img src=\"https://old.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
        ));
        let rebuilder = TestRebuilder::publishing(
            &cache,
            cache_read_with_xml(
                true,
                b"<img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
            ),
        );
        let service = FeedService::new(
            cache,
            queue,
            rebuilder.clone(),
            FeedServiceConfig::default(),
        )
        .with_asset_url_policy(Some(FeedAssetUrlPolicy::new(
            Url::parse("https://rss.example.test/werrss").unwrap(),
            false,
        )));

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("cache from a previous root should be rebuilt");

        assert!(matches!(
            delivery,
            FeedDelivery::Cached {
                cache,
                status: FeedCacheStatus::Fresh,
                rebuild: FeedRebuildStatus::Rebuilt,
            } if String::from_utf8_lossy(cache.xml_bytes()).contains("src=\"/assets/")
        ));
        assert_eq!(rebuilder.call_count().await, 1);
    }

    #[tokio::test]
    async fn incompatible_cache_is_not_served_when_rebuild_fails() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let rebuilder = TestRebuilder::failing();
        let service = FeedService::new(
            TestCache::hit(cache_read_with_xml(
                true,
                b"<img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />",
            )),
            queue.clone(),
            rebuilder,
            FeedServiceConfig::default(),
        )
        .with_asset_url_policy(Some(FeedAssetUrlPolicy::new(
            Url::parse("https://rss.example.test/werrss").unwrap(),
            true,
        )));

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("rebuild failure should produce a retryable delivery");

        assert!(matches!(
            delivery,
            FeedDelivery::Unavailable {
                rebuild: FeedRebuildStatus::Enqueued,
                ..
            }
        ));
        assert_eq!(queue.request_count().await, 1);
    }

    #[tokio::test]
    async fn stale_cache_is_rebuilt_and_the_fresh_result_is_returned() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let cache = TestCache::hit(cache_read(false));
        let rebuilder = TestRebuilder::publishing(&cache, cache_read_with_xml(true, b"fresh"));
        let service = FeedService::new(
            cache,
            queue.clone(),
            rebuilder.clone(),
            FeedServiceConfig::default(),
        );

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("stale cache should be rebuilt");

        match delivery {
            FeedDelivery::Cached {
                cache,
                status: FeedCacheStatus::Fresh,
                rebuild: FeedRebuildStatus::Rebuilt,
            } => assert_eq!(cache.xml_bytes(), b"fresh"),
            other => panic!("expected rebuilt cached delivery, got {other:?}"),
        }
        assert_eq!(queue.request_count().await, 0);
        assert_eq!(rebuilder.call_count().await, 1);
    }

    #[tokio::test]
    async fn missing_cache_is_rebuilt_and_published_before_delivery() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let cache = TestCache::miss();
        let rebuilder = TestRebuilder::publishing(&cache, cache_read_with_xml(true, b"built"));
        let service = FeedService::new(
            cache,
            queue.clone(),
            rebuilder.clone(),
            FeedServiceConfig::default(),
        );

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("a cache miss should be rebuilt");

        assert!(matches!(
            delivery,
            FeedDelivery::Cached {
                cache,
                status: FeedCacheStatus::Fresh,
                rebuild: FeedRebuildStatus::Rebuilt,
            } if cache.xml_bytes() == b"built"
        ));
        assert_eq!(queue.request_count().await, 0);
        assert_eq!(rebuilder.call_count().await, 1);
    }

    #[tokio::test]
    async fn cache_miss_returns_retry_after_even_when_queue_is_temporarily_unavailable() {
        let queue = TestQueue::failing();
        let rebuilder = TestRebuilder::failing();
        let config = FeedServiceConfig::new(Duration::minutes(1), Duration::seconds(7))
            .expect("test timing should be valid");
        let service = FeedService::new(TestCache::miss(), queue.clone(), rebuilder, config);

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("queue failure should not become a cache error");

        assert!(matches!(
            delivery,
            FeedDelivery::Unavailable {
                retry_after,
                rebuild: FeedRebuildStatus::Unavailable,
            } if retry_after == Duration::seconds(7)
        ));
        assert_eq!(queue.request_count().await, 1);
    }

    #[tokio::test]
    async fn failed_rebuild_serves_the_expired_cache_and_queues_a_retry() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let rebuilder = TestRebuilder::failing();
        let service = FeedService::new(
            TestCache::hit(cache_read_with_xml(false, b"expired")),
            queue.clone(),
            rebuilder,
            FeedServiceConfig::default(),
        );

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("an expired cache should remain available when rebuilding fails");

        assert!(matches!(
            delivery,
            FeedDelivery::Cached {
                cache,
                status: FeedCacheStatus::Stale,
                rebuild: FeedRebuildStatus::Enqueued,
            } if cache.xml_bytes() == b"expired"
        ));
        assert_eq!(queue.request_count().await, 1);
    }

    #[tokio::test]
    async fn active_rebuild_without_a_cache_returns_retryable_unavailability() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let rebuilder = TestRebuilder::active();
        let config = FeedServiceConfig::new(Duration::zero(), Duration::milliseconds(1))
            .expect("short test wait should be valid");
        let service = FeedService::new(TestCache::miss(), queue.clone(), rebuilder, config);

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("an active rebuild should produce a retryable result");

        assert!(matches!(
            delivery,
            FeedDelivery::Unavailable {
                rebuild: FeedRebuildStatus::AlreadyActive,
                ..
            }
        ));
        assert_eq!(queue.request_count().await, 0);
    }

    #[tokio::test]
    async fn invalid_source_and_cache_errors_are_not_converted_to_rebuilds() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let rebuilder = TestRebuilder::active();
        let service = FeedService::new(
            TestCache::failing(),
            queue.clone(),
            rebuilder,
            FeedServiceConfig::default(),
        );

        assert!(matches!(
            service
                .get_feed(FeedRequest::new(SourceId::from_uuid(Uuid::nil()), None))
                .await,
            Err(FeedServiceError::InvalidSourceId)
        ));
        assert!(matches!(
            service.get_feed(FeedRequest::new(source_id(), None)).await,
            Err(FeedServiceError::Cache(FeedCacheRepositoryError::Storage(
                _
            )))
        ));
        assert_eq!(queue.request_count().await, 0);
    }

    #[test]
    fn validates_feed_delivery_and_job_timing() {
        assert!(matches!(
            FeedServiceConfig::new(Duration::seconds(-1), Duration::seconds(1)),
            Err(FeedServiceConfigError::InvalidStaleWindow)
        ));
        assert!(matches!(
            FeedServiceConfig::new(Duration::zero(), Duration::zero()),
            Err(FeedServiceConfigError::InvalidMissRetryAfter)
        ));
        assert!(matches!(
            FeedRebuildJobConfig::new(0, 0),
            Err(FeedRebuildJobConfigError::InvalidAttemptLimit)
        ));
    }

    #[test]
    fn matches_wildcard_and_weak_quoted_entity_tags() {
        assert!(if_none_match_matches(Some("*"), "etag"));
        assert!(if_none_match_matches(Some("W/\"etag\""), "etag"));
        assert!(if_none_match_matches(Some("\"other\", etag"), "etag"));
        assert!(!if_none_match_matches(Some("\"other\""), "etag"));
    }
}
