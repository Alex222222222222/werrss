//! Database-only RSS feed rebuild orchestration.
//!
//! This service turns the current normalized source/article snapshot into a
//! revision-aware feed-cache candidate. It is intentionally separate from
//! [`super::feed_service::FeedService`]: this path is allowed to load articles,
//! render XML, and publish through the fenced cache transaction, while feed
//! delivery decides when a request should invoke it.
//!
//! Responsibilities:
//!
//! - acquire the durable per-source feed-build lease;
//! - read one source and its bounded, deterministic article list;
//! - map persisted articles into the pure [`crate::rss::renderer::RssRenderer`];
//! - publish the candidate only when the source revision and lease fence still
//!   match;
//! - optionally apply a claimed `feed_rebuild` job's fenced success outcome in
//!   that same transaction; and
//! - commit the cache publication, lease release, and optional job outcome as
//!   one `UnitOfWork`.
//!
//! Non-responsibilities: browser or WeChat access, article synchronization,
//! feed-token lookup, HTTP response construction, or cache reads. A
//! `feed_rebuild` job handler may pass its claimed lease to
//! [`Self::rebuild_for_job`] so successful publication and job completion share
//! one transaction. Pre-publication failures and the `AlreadyActive` result
//! still leave job-outcome selection to the worker/application boundary.
//!
//! Data flow is deliberately short and explicit: acquire lease -> read source
//! and articles -> render outside a database transaction -> begin the shared
//! transaction -> fenced publish/release -> commit. If any pre-publication
//! step fails, the service attempts to release the lease. If publication loses
//! its fence, the transaction result is committed only when the repository has
//! already released the lease as part of that result; stale candidates never
//! replace a newer cache.
//!
//! PostgreSQL/high-availability behavior is supplied by
//! [`PostgresFeedBuildLeaseRepository`](crate::persistence::repositories::feed_cache_repository::PostgresFeedBuildLeaseRepository),
//! PostgreSQL article/source readers, and the transaction-scoped feed-cache
//! repository. The lease and source revision are the distributed coordination
//! mechanisms; this service uses no process-local mutex. Two replicas can read
//! concurrently, but only the current build owner and fencing token can
//! publish. A failed worker's lease expires or is explicitly released, making
//! the next rebuild eligible.
//!
//! RSS-cache interaction: the source's current `feed_revision` is carried into
//! the rendered candidate, and the configured TTL determines `expires_at`.
//! Candidate timestamps come from the PostgreSQL clock supplied by the unit of
//! work factory, so same-revision publication ordering is independent of a
//! replica's local wall clock.
//! Rebuilding does not bump the revision because it only republishes the
//! current normalized records. Article/source synchronization owns revision
//! changes. Token rotation and revocation remain independent of this path.
//!
//! Failure behavior is typed and fail-closed. Missing sources, article read
//! failures, renderer validation failures, lease loss, and transaction errors
//! do not report a successful rebuild. Cleanup errors are logged while the
//! primary error is preserved; an unclean lease remains bounded by its durable
//! expiry.

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use url::Url;

use crate::{
    application::source_service::{SourceReader, SourceServiceError},
    archive::url_rewriter::absolutize_asset_urls,
    domain::{
        article::Article,
        feed::FeedCacheCandidate,
        job::{JobType, LeaseToken},
        source::{FeedBuildLease, FeedBuildLeaseToken, FeedRevision, SourceId},
    },
    persistence::{
        repositories::{
            article_repository::{ArticleRepository, ArticleRepositoryError},
            feed_cache_repository::{
                FeedBuildLeaseError, FeedBuildLeaseRepository, FeedCachePublishResult,
                FeedCacheRepositoryError, FeedCacheTransactionRepository,
            },
            job_repository::{JobLease, JobOutcome, JobOutcomeTransaction, JobRepositoryError},
        },
        unit_of_work::{UnitOfWork, UnitOfWorkError, UnitOfWorkFactory},
    },
    rss::renderer::{RenderArticle, RenderError, RenderFeedInput, RssRenderer},
};

/// Settings for one database-only feed rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedRebuildConfig {
    lease_for: Duration,
    cache_ttl: Duration,
    feed_url: String,
    description: String,
    asset_url_root: Option<Url>,
}

impl FeedRebuildConfig {
    /// Creates validated lease, cache, and channel metadata settings.
    pub fn new(
        lease_for: Duration,
        cache_ttl: Duration,
        feed_url: impl Into<String>,
        description: impl Into<String>,
    ) -> Result<Self, FeedRebuildConfigError> {
        if lease_for <= Duration::zero() {
            return Err(FeedRebuildConfigError::InvalidLeaseDuration);
        }
        if cache_ttl <= Duration::zero() {
            return Err(FeedRebuildConfigError::InvalidCacheTtl);
        }
        let feed_url = feed_url.into();
        if feed_url.trim().is_empty() {
            return Err(FeedRebuildConfigError::EmptyFeedUrl);
        }
        Ok(Self {
            lease_for,
            cache_ttl,
            feed_url,
            description: description.into(),
            asset_url_root: None,
        })
    }

    /// Returns the duration for which the build fence is held.
    pub const fn lease_for(&self) -> Duration {
        self.lease_for
    }

    /// Returns the freshness TTL assigned to a newly rendered cache row.
    pub const fn cache_ttl(&self) -> Duration {
        self.cache_ttl
    }

    /// Returns the channel link supplied to the RSS renderer.
    pub fn feed_url(&self) -> &str {
        &self.feed_url
    }

    /// Returns the channel description supplied to the RSS renderer.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Configures the public root used for stable asset URLs in RSS content.
    ///
    /// Article rows retain relative `/assets/{id}` routes. The root is only
    /// applied while rendering a feed so changing deployment hostnames does
    /// not require rewriting every stored article.
    pub fn with_asset_url_root(mut self, root: Url) -> Self {
        self.asset_url_root = Some(root);
        self
    }

    /// Returns the optional public root used for asset URLs.
    pub fn asset_url_root(&self) -> Option<&Url> {
        self.asset_url_root.as_ref()
    }
}

impl Default for FeedRebuildConfig {
    fn default() -> Self {
        Self::new(
            Duration::minutes(10),
            Duration::minutes(30),
            "https://rss.example.test/feed.xml",
            "WeChat article feed",
        )
        .expect("default feed rebuild settings must be valid")
    }
}

/// Invalid feed-rebuild settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FeedRebuildConfigError {
    /// The build lease must protect rendering for a positive period.
    #[error("feed rebuild lease duration must be positive")]
    InvalidLeaseDuration,
    /// A cache candidate must have a future expiry.
    #[error("feed rebuild cache TTL must be positive")]
    InvalidCacheTtl,
    /// RSS channel links must not be empty.
    #[error("feed rebuild feed URL must not be empty")]
    EmptyFeedUrl,
    /// The generated expiry could not be represented by `chrono`.
    #[error("feed rebuild cache expiry is outside the supported time range")]
    ExpiryOutOfRange,
}

/// Dependencies needed by [`FeedRebuildService`].
pub struct FeedRebuildDependencies<S, A, L, U> {
    sources: S,
    articles: A,
    leases: L,
    unit_of_work: U,
}

impl<S, A, L, U> FeedRebuildDependencies<S, A, L, U> {
    /// Groups the independent reads, lease repository, and shared transaction
    /// factory without making the service constructor argument-heavy.
    pub fn new(sources: S, articles: A, leases: L, unit_of_work: U) -> Self {
        Self {
            sources,
            articles,
            leases,
            unit_of_work,
        }
    }
}

/// Result of one rebuild attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedRebuildOutcome {
    /// The candidate was published for the source's current revision.
    Published {
        /// Revision represented by the published cache row.
        feed_revision: FeedRevision,
    },
    /// Another live builder owns the source lease.
    AlreadyActive,
    /// The source revision changed after the snapshot was rendered.
    SourceRevisionChanged {
        /// Revision observed by the publication transaction.
        current_revision: FeedRevision,
    },
    /// A newer/equal-revision cache candidate was already present.
    ExistingCacheNewer,
}

/// Errors raised by a database-only rebuild.
#[derive(Debug, Error)]
pub enum FeedRebuildError {
    /// The requested source identifier cannot identify a source.
    #[error("source id must not be nil")]
    InvalidSourceId,
    /// The claimed job is not a feed-rebuild job.
    #[error("job {job_id} is not a feed_rebuild job")]
    JobTypeMismatch {
        /// Unexpected job identifier.
        job_id: uuid::Uuid,
    },
    /// The claimed feed-rebuild job has no source relationship.
    #[error("feed_rebuild job {job_id} has no source")]
    JobMissingSource {
        /// Invalid job identifier.
        job_id: uuid::Uuid,
    },
    /// The claimed job does not contain an active owner.
    #[error("feed_rebuild job {job_id} has no lease owner")]
    JobLeaseMissingOwner {
        /// Invalid job identifier.
        job_id: uuid::Uuid,
    },
    /// The fixed durable lease owner is empty.
    #[error("feed rebuild owner must not be empty")]
    EmptyOwner,
    /// The source disappeared between queueing and rebuild execution.
    #[error("source {source_id} was not found")]
    SourceNotFound { source_id: SourceId },
    /// Source read or domain mapping failed.
    #[error(transparent)]
    Source(#[from] SourceServiceError),
    /// Article list loading failed.
    #[error(transparent)]
    Articles(#[from] ArticleRepositoryError),
    /// Build-lease acquisition, cleanup, or fencing failed.
    #[error(transparent)]
    Lease(#[from] FeedBuildLeaseError),
    /// RSS candidate validation or serialization failed.
    #[error(transparent)]
    Render(#[from] RenderError),
    /// Candidate publication failed inside the shared transaction.
    #[error(transparent)]
    Cache(#[from] FeedCacheRepositoryError),
    /// The claimed feed-rebuild job could not be completed in the shared
    /// transaction.
    #[error(transparent)]
    Job(#[from] JobRepositoryError),
    /// The shared transaction could not begin or commit.
    #[error(transparent)]
    UnitOfWork(#[from] UnitOfWorkError),
    /// Settings became impossible to apply at the current clock instant.
    #[error(transparent)]
    Config(#[from] FeedRebuildConfigError),
}

/// Capability used by feed delivery to rebuild one source synchronously.
#[async_trait::async_trait]
pub trait FeedRebuilder: Send + Sync {
    /// Rebuilds and publishes the current feed-cache snapshot for `source_id`.
    async fn rebuild(&self, source_id: SourceId) -> Result<FeedRebuildOutcome, FeedRebuildError>;
}

/// Transaction-scoped feed-cache publication capability.
#[async_trait::async_trait]
pub trait FeedRebuildUnitOfWork: JobOutcomeTransaction {
    /// Publishes/releases the build lease as part of this transaction. The
    /// inherited outcome port can complete a claimed worker job before commit.
    async fn publish_feed(
        &mut self,
        candidate: FeedCacheCandidate,
        owner: &str,
        token: FeedBuildLeaseToken,
    ) -> Result<FeedCachePublishResult, FeedCacheRepositoryError>;

    /// Commits the cache publication and lease release.
    async fn commit(self) -> Result<(), UnitOfWorkError>
    where
        Self: Sized;
}

/// Factory for the shared transaction used by a rebuild.
#[async_trait::async_trait]
pub trait FeedRebuildUnitOfWorkFactory: Clone + Send + Sync {
    /// Transaction type borrowed from this factory.
    type Transaction<'a>: FeedRebuildUnitOfWork + Send + 'a
    where
        Self: 'a;

    /// Begins a transaction without committing it.
    async fn begin(&self) -> Result<Self::Transaction<'_>, UnitOfWorkError>;

    /// Samples the authoritative database clock for candidate timestamps.
    async fn database_now(&self) -> Result<DateTime<Utc>, UnitOfWorkError>;
}

#[async_trait::async_trait]
impl FeedRebuildUnitOfWork for UnitOfWork<'_> {
    async fn publish_feed(
        &mut self,
        candidate: FeedCacheCandidate,
        owner: &str,
        token: FeedBuildLeaseToken,
    ) -> Result<FeedCachePublishResult, FeedCacheRepositoryError> {
        let mut cache = self.feed_cache();
        cache.publish_if_current(candidate, owner, token).await
    }

    async fn commit(self) -> Result<(), UnitOfWorkError> {
        UnitOfWork::commit(self).await
    }
}

#[async_trait::async_trait]
impl FeedRebuildUnitOfWorkFactory for UnitOfWorkFactory {
    type Transaction<'a> = UnitOfWork<'a>;

    async fn begin(&self) -> Result<Self::Transaction<'_>, UnitOfWorkError> {
        UnitOfWorkFactory::begin(self).await
    }

    async fn database_now(&self) -> Result<DateTime<Utc>, UnitOfWorkError> {
        UnitOfWorkFactory::database_now(self).await
    }
}

/// Database-only feed rebuild application service.
pub struct FeedRebuildService<S, A, L, U> {
    sources: S,
    articles: A,
    leases: L,
    unit_of_work: U,
    config: FeedRebuildConfig,
    owner: String,
    renderer: RssRenderer,
}

impl<S, A, L, U> FeedRebuildService<S, A, L, U>
where
    S: SourceReader,
    A: ArticleRepository,
    L: FeedBuildLeaseRepository,
    U: FeedRebuildUnitOfWorkFactory,
{
    /// Creates a rebuild service over the supplied persistence capabilities.
    pub fn new(
        dependencies: FeedRebuildDependencies<S, A, L, U>,
        config: FeedRebuildConfig,
        owner: impl Into<String>,
    ) -> Result<Self, FeedRebuildError> {
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(FeedRebuildError::EmptyOwner);
        }
        Ok(Self {
            sources: dependencies.sources,
            articles: dependencies.articles,
            leases: dependencies.leases,
            unit_of_work: dependencies.unit_of_work,
            config,
            owner: owner.trim().to_owned(),
            renderer: RssRenderer,
        })
    }

    /// Rebuilds one source's feed without contacting upstream systems.
    #[tracing::instrument(skip_all, fields(source_id = %source_id))]
    pub async fn rebuild(
        &self,
        source_id: SourceId,
    ) -> Result<FeedRebuildOutcome, FeedRebuildError> {
        self.rebuild_inner(source_id, None).await
    }

    /// Rebuilds a feed and completes a claimed `feed_rebuild` job atomically.
    ///
    /// Rendering and all reads remain outside the final transaction. When
    /// publication reaches the finalization boundary, the cache publication,
    /// build-lease release, and fenced `succeeded` job outcome are committed by
    /// the same [`FeedRebuildUnitOfWork`]. If the build lease is already held by
    /// another builder, or a pre-publication operation fails, this method does
    /// not choose a retry/deferral outcome for the job; the caller retains that
    /// responsibility.
    #[tracing::instrument(skip_all, fields(job_id = %lease.job.id()))]
    pub async fn rebuild_for_job(
        &self,
        lease: &JobLease,
    ) -> Result<FeedRebuildOutcome, FeedRebuildError> {
        let job_id = lease.job.id();
        if lease.job.job_type() != JobType::FeedRebuild {
            return Err(FeedRebuildError::JobTypeMismatch { job_id });
        }
        let source_id = lease
            .job
            .source_id()
            .map(SourceId::from_uuid)
            .ok_or(FeedRebuildError::JobMissingSource { job_id })?;
        let owner = lease
            .job
            .lease_owner()
            .ok_or(FeedRebuildError::JobLeaseMissingOwner { job_id })?
            .to_owned();

        self.rebuild_inner(
            source_id,
            Some(JobCompletion {
                job_id,
                owner,
                token: lease.token,
            }),
        )
        .await
    }

    async fn rebuild_inner(
        &self,
        source_id: SourceId,
        completion: Option<JobCompletion>,
    ) -> Result<FeedRebuildOutcome, FeedRebuildError> {
        if source_id.as_uuid().is_nil() {
            return Err(FeedRebuildError::InvalidSourceId);
        }

        tracing::debug!("acquiring feed rebuild lease");

        let lease = match self
            .leases
            .acquire_build(source_id, &self.owner, self.config.lease_for())
            .await
        {
            Ok(Some(lease)) => {
                tracing::debug!(
                    source_id = %source_id,
                    "acquired feed rebuild lease"
                );
                lease
            }
            Ok(None) => {
                tracing::debug!(source_id = %source_id, "feed rebuild is already active");
                return Ok(FeedRebuildOutcome::AlreadyActive);
            }
            Err(FeedBuildLeaseError::SourceNotFound { source_id }) => {
                return Err(FeedRebuildError::SourceNotFound { source_id });
            }
            Err(error) => return Err(error.into()),
        };

        let token = lease.token();
        let result = self.rebuild_with_lease(source_id, lease, completion).await;
        if result.is_err() {
            self.release_after_failure(source_id, token, &result).await;
        }
        match &result {
            Ok(outcome) => tracing::info!(
                source_id = %source_id,
                outcome = ?outcome,
                "feed rebuild finished"
            ),
            Err(error) => tracing::warn!(
                source_id = %source_id,
                error = %error,
                "feed rebuild failed"
            ),
        }
        result
    }

    async fn rebuild_with_lease(
        &self,
        source_id: SourceId,
        lease: FeedBuildLease,
        completion: Option<JobCompletion>,
    ) -> Result<FeedRebuildOutcome, FeedRebuildError> {
        let source = self
            .sources
            .find(source_id)
            .await?
            .ok_or(FeedRebuildError::SourceNotFound { source_id })?;
        let articles = self
            .articles
            .list_for_feed(source_id, source.rss_item_limit())
            .await?;
        tracing::debug!(
            source_id = %source_id,
            article_count = articles.len(),
            source_revision = ?source.feed_revision(),
            "loaded feed rebuild snapshot"
        );
        let generated_at = self.unit_of_work.database_now().await?;
        let expires_at = generated_at
            .checked_add_signed(self.config.cache_ttl())
            .ok_or(FeedRebuildConfigError::ExpiryOutOfRange)?;
        let rendered = self.renderer.render(RenderFeedInput {
            source_id,
            title: source.display_name().to_owned(),
            feed_url: self.config.feed_url().to_owned(),
            description: self.config.description().to_owned(),
            source_revision: source.feed_revision(),
            generated_at,
            expires_at,
            articles: articles
                .into_iter()
                .map(|article| render_article(article, self.config.asset_url_root()))
                .collect(),
        })?;

        let mut unit_of_work = self.unit_of_work.begin().await?;
        let publication = unit_of_work
            .publish_feed(rendered.into_candidate(), &self.owner, lease.token())
            .await;
        let publication = match publication {
            Ok(publication) => publication,
            Err(error) => {
                drop(unit_of_work);
                tracing::warn!(source_id = %source_id, error = %error, "feed cache publication failed");
                return Err(error.into());
            }
        };
        if let Some(completion) = completion {
            unit_of_work
                .apply_outcome(JobOutcome::Succeeded {
                    job_id: completion.job_id,
                    owner: completion.owner,
                    token: completion.token,
                    now: generated_at,
                })
                .await?;
        }
        unit_of_work.commit().await?;
        let outcome = match publication {
            FeedCachePublishResult::Published(cache) => FeedRebuildOutcome::Published {
                feed_revision: cache.feed_revision(),
            },
            FeedCachePublishResult::SourceRevisionChanged { current_revision } => {
                FeedRebuildOutcome::SourceRevisionChanged { current_revision }
            }
            FeedCachePublishResult::ExistingCacheNewer => FeedRebuildOutcome::ExistingCacheNewer,
        };
        tracing::debug!(source_id = %source_id, outcome = ?outcome, "published feed rebuild result");
        Ok(outcome)
    }

    async fn release_after_failure(
        &self,
        source_id: SourceId,
        token: FeedBuildLeaseToken,
        result: &Result<FeedRebuildOutcome, FeedRebuildError>,
    ) {
        if let Err(error) = self
            .leases
            .release_build(source_id, &self.owner, token)
            .await
        {
            tracing::warn!(
                %source_id,
                error = %error,
                primary_error = ?result.as_ref().err(),
                "unable to release failed feed rebuild lease"
            );
        }
    }
}

#[async_trait::async_trait]
impl<S, A, L, U> FeedRebuilder for FeedRebuildService<S, A, L, U>
where
    S: SourceReader,
    A: ArticleRepository,
    L: FeedBuildLeaseRepository,
    U: FeedRebuildUnitOfWorkFactory,
{
    async fn rebuild(&self, source_id: SourceId) -> Result<FeedRebuildOutcome, FeedRebuildError> {
        FeedRebuildService::rebuild(self, source_id).await
    }
}

struct JobCompletion {
    job_id: uuid::Uuid,
    owner: String,
    token: LeaseToken,
}

fn render_article(article: Article, asset_url_root: Option<&Url>) -> RenderArticle {
    let content_html = asset_url_root.map_or_else(
        || article.content_html().to_owned(),
        |root| absolutize_asset_urls(article.content_html(), root),
    );
    RenderArticle {
        review_id: article.review_id().to_owned(),
        title: article.title().to_owned(),
        author: article.author().map(str::to_owned),
        summary: article.summary().map(str::to_owned),
        original_url: article.original_url().map(|url| url.as_str().to_owned()),
        published_at: article.published_at(),
        content_html,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, Duration, TimeZone, Utc};
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::*;
    use crate::{
        domain::{
            article::ArticleObservationVersion,
            feed::FeedCache,
            job::{Job, JobType, LeaseToken, NewJob},
            source::{NewSource, SchedulingGate, Source},
        },
        persistence::repositories::{
            feed_cache_repository::MemoryFeedBuildLeaseRepository,
            job_repository::{
                JobLease, JobOutcome, JobQueue, JobRepositoryError, MemoryJobRepository,
            },
        },
    };

    #[derive(Clone)]
    struct FakeSources {
        source: Arc<Mutex<Option<Source>>>,
    }

    #[async_trait::async_trait]
    impl SourceReader for FakeSources {
        async fn find(&self, source_id: SourceId) -> Result<Option<Source>, SourceServiceError> {
            Ok(self
                .source
                .lock()
                .await
                .as_ref()
                .filter(|source| source.id() == source_id)
                .cloned())
        }

        async fn find_by_book_id(
            &self,
            book_id: &str,
        ) -> Result<Option<Source>, SourceServiceError> {
            let book_id = book_id.trim();
            Ok(self
                .source
                .lock()
                .await
                .as_ref()
                .filter(|source| source.book_id() == book_id)
                .cloned())
        }
    }

    #[derive(Clone, Default)]
    struct FakeArticles;

    #[async_trait::async_trait]
    impl ArticleRepository for FakeArticles {
        async fn find(
            &self,
            _source_id: SourceId,
            _review_id: &str,
        ) -> Result<Option<Article>, ArticleRepositoryError> {
            Ok(None)
        }

        async fn list_for_feed(
            &self,
            _source_id: SourceId,
            _limit: u32,
        ) -> Result<Vec<Article>, ArticleRepositoryError> {
            Ok(Vec::new())
        }

        async fn allocate_observation_version(
            &self,
        ) -> Result<ArticleObservationVersion, ArticleRepositoryError> {
            Ok(ArticleObservationVersion::from_u64(1))
        }
    }

    #[derive(Clone)]
    struct FakeUnitOfWorkFactory {
        outcome: Arc<Mutex<Option<FeedCachePublishResult>>>,
        commits: Arc<Mutex<usize>>,
        database_now: DateTime<Utc>,
        candidates: Arc<Mutex<Vec<FeedCacheCandidate>>>,
        job_outcomes: Arc<Mutex<Vec<JobOutcome>>>,
    }

    struct FakeUnitOfWork {
        outcome: Arc<Mutex<Option<FeedCachePublishResult>>>,
        commits: Arc<Mutex<usize>>,
        candidates: Arc<Mutex<Vec<FeedCacheCandidate>>>,
        job_outcomes: Arc<Mutex<Vec<JobOutcome>>>,
    }

    impl Default for FakeUnitOfWorkFactory {
        fn default() -> Self {
            Self {
                outcome: Arc::new(Mutex::new(None)),
                commits: Arc::new(Mutex::new(0)),
                database_now: timestamp(1_000),
                candidates: Arc::new(Mutex::new(Vec::new())),
                job_outcomes: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl FeedRebuildUnitOfWorkFactory for FakeUnitOfWorkFactory {
        type Transaction<'a> = FakeUnitOfWork;

        async fn begin(&self) -> Result<Self::Transaction<'_>, UnitOfWorkError> {
            Ok(FakeUnitOfWork {
                outcome: Arc::clone(&self.outcome),
                commits: Arc::clone(&self.commits),
                candidates: Arc::clone(&self.candidates),
                job_outcomes: Arc::clone(&self.job_outcomes),
            })
        }

        async fn database_now(&self) -> Result<DateTime<Utc>, UnitOfWorkError> {
            Ok(self.database_now)
        }
    }

    #[async_trait::async_trait]
    impl JobOutcomeTransaction for FakeUnitOfWork {
        async fn apply_outcome(&mut self, outcome: JobOutcome) -> Result<Job, JobRepositoryError> {
            self.job_outcomes.lock().await.push(outcome);
            Job::new(NewJob {
                job_type: JobType::FeedRebuild,
                source_id: Some(source_id().as_uuid()),
                priority: 0,
                run_after: timestamp(0),
                max_attempts: 3,
                payload: serde_json::json!({}),
                dedupe_key: "fake-feed-rebuild".to_owned(),
                now: timestamp(1_000),
            })
            .map_err(JobRepositoryError::Domain)
        }
    }

    #[async_trait::async_trait]
    impl FeedRebuildUnitOfWork for FakeUnitOfWork {
        async fn publish_feed(
            &mut self,
            candidate: FeedCacheCandidate,
            _owner: &str,
            _token: FeedBuildLeaseToken,
        ) -> Result<FeedCachePublishResult, FeedCacheRepositoryError> {
            self.candidates.lock().await.push(candidate.clone());
            let mut outcome = self.outcome.lock().await;
            Ok(outcome.take().unwrap_or_else(|| {
                FeedCachePublishResult::Published(FeedCache::from_candidate(
                    candidate,
                    timestamp(100),
                ))
            }))
        }

        async fn commit(self) -> Result<(), UnitOfWorkError> {
            *self.commits.lock().await += 1;
            Ok(())
        }
    }

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    fn source() -> Source {
        Source::new(NewSource {
            id: source_id(),
            book_id: "book-1".to_owned(),
            display_name: "Test feed".to_owned(),
            article_url: Some("https://mp.weixin.qq.com/s/test".parse().unwrap()),
            enabled: true,
            sync_interval: Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: timestamp(0),
            priority: 0,
            max_attempts: 3,
        })
        .expect("source should be valid")
    }

    fn service(
        sources: FakeSources,
        leases: MemoryFeedBuildLeaseRepository,
        unit_of_work: FakeUnitOfWorkFactory,
        config: FeedRebuildConfig,
    ) -> FeedRebuildService<
        FakeSources,
        FakeArticles,
        MemoryFeedBuildLeaseRepository,
        FakeUnitOfWorkFactory,
    > {
        FeedRebuildService::new(
            FeedRebuildDependencies::new(sources, FakeArticles, leases, unit_of_work),
            config,
            " builder-a ",
        )
        .expect("service should be valid")
    }

    #[tokio::test]
    async fn publishes_current_revision_and_commits_once() {
        let leases = MemoryFeedBuildLeaseRepository::new(timestamp(0));
        let unit_of_work = FakeUnitOfWorkFactory::default();
        let service = service(
            FakeSources {
                source: Arc::new(Mutex::new(Some(source()))),
            },
            leases.clone(),
            unit_of_work.clone(),
            FeedRebuildConfig::default(),
        );

        assert_eq!(
            service.rebuild(source_id()).await.unwrap(),
            FeedRebuildOutcome::Published {
                feed_revision: FeedRevision::zero()
            }
        );
        assert_eq!(*unit_of_work.commits.lock().await, 1);
    }

    #[tokio::test]
    async fn claimed_job_completion_shares_the_cache_publication_transaction() {
        let leases = MemoryFeedBuildLeaseRepository::new(timestamp(0));
        let unit_of_work = FakeUnitOfWorkFactory::default();
        let jobs = MemoryJobRepository::new();
        jobs.enqueue(NewJob {
            job_type: JobType::FeedRebuild,
            source_id: Some(source_id().as_uuid()),
            priority: 1,
            run_after: timestamp(0),
            max_attempts: 3,
            payload: serde_json::json!({"source_id": source_id().as_uuid()}),
            dedupe_key: "feed-rebuild-job".to_owned(),
            now: timestamp(0),
        })
        .await
        .unwrap();
        let lease = jobs
            .claim_next(
                "worker-a",
                timestamp(1),
                Duration::minutes(5),
                &[JobType::FeedRebuild],
            )
            .await
            .unwrap()
            .unwrap();
        let service = service(
            FakeSources {
                source: Arc::new(Mutex::new(Some(source()))),
            },
            leases,
            unit_of_work.clone(),
            FeedRebuildConfig::default(),
        );

        assert_eq!(
            service.rebuild_for_job(&lease).await.unwrap(),
            FeedRebuildOutcome::Published {
                feed_revision: FeedRevision::zero()
            }
        );
        let outcomes = unit_of_work.job_outcomes.lock().await;
        assert!(matches!(
            outcomes.as_slice(),
            [JobOutcome::Succeeded {
                job_id,
                owner,
                token,
                ..
            }] if *job_id == lease.job.id()
                && owner == "worker-a"
                && *token == lease.token
        ));
        assert_eq!(*unit_of_work.commits.lock().await, 1);
    }

    #[tokio::test]
    async fn claimed_job_completion_rejects_wrong_job_shape_before_acquiring_a_build_lease() {
        let leases = MemoryFeedBuildLeaseRepository::new(timestamp(0));
        let service = service(
            FakeSources {
                source: Arc::new(Mutex::new(Some(source()))),
            },
            leases.clone(),
            FakeUnitOfWorkFactory::default(),
            FeedRebuildConfig::default(),
        );
        let source_id = source_id();
        let wrong_type = JobLease {
            job: Job::new(NewJob {
                job_type: JobType::SourceSync,
                source_id: Some(source_id.as_uuid()),
                priority: 1,
                run_after: timestamp(0),
                max_attempts: 3,
                payload: serde_json::json!({}),
                dedupe_key: "wrong-type".to_owned(),
                now: timestamp(0),
            })
            .unwrap(),
            token: LeaseToken::new(),
        };
        assert!(matches!(
            service.rebuild_for_job(&wrong_type).await,
            Err(FeedRebuildError::JobTypeMismatch { job_id })
                if job_id == wrong_type.job.id()
        ));

        let missing_source = JobLease {
            job: Job::new(NewJob {
                job_type: JobType::FeedRebuild,
                source_id: None,
                priority: 1,
                run_after: timestamp(0),
                max_attempts: 3,
                payload: serde_json::json!({}),
                dedupe_key: "missing-source".to_owned(),
                now: timestamp(0),
            })
            .unwrap(),
            token: LeaseToken::new(),
        };
        assert!(matches!(
            service.rebuild_for_job(&missing_source).await,
            Err(FeedRebuildError::JobMissingSource { job_id })
                if job_id == missing_source.job.id()
        ));
        assert!(leases
            .acquire_build(source_id, "builder-b", Duration::minutes(1))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn uses_the_database_clock_for_candidate_timestamps() {
        let leases = MemoryFeedBuildLeaseRepository::new(timestamp(0));
        let unit_of_work = FakeUnitOfWorkFactory::default();
        let service = service(
            FakeSources {
                source: Arc::new(Mutex::new(Some(source()))),
            },
            leases,
            unit_of_work.clone(),
            FeedRebuildConfig::default(),
        );

        service.rebuild(source_id()).await.unwrap();
        let candidates = unit_of_work.candidates.lock().await;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].generated_at(), timestamp(1_000));
        assert_eq!(candidates[0].expires_at(), timestamp(2_800));
    }

    #[tokio::test]
    async fn preserves_non_publishing_publication_outcomes_after_commit() {
        let outcomes = [
            (
                FeedCachePublishResult::SourceRevisionChanged {
                    current_revision: FeedRevision::from_u64(7),
                },
                FeedRebuildOutcome::SourceRevisionChanged {
                    current_revision: FeedRevision::from_u64(7),
                },
            ),
            (
                FeedCachePublishResult::ExistingCacheNewer,
                FeedRebuildOutcome::ExistingCacheNewer,
            ),
        ];

        for (publication, expected) in outcomes {
            let leases = MemoryFeedBuildLeaseRepository::new(timestamp(0));
            let unit_of_work = FakeUnitOfWorkFactory {
                outcome: Arc::new(Mutex::new(Some(publication))),
                ..FakeUnitOfWorkFactory::default()
            };
            let service = service(
                FakeSources {
                    source: Arc::new(Mutex::new(Some(source()))),
                },
                leases,
                unit_of_work.clone(),
                FeedRebuildConfig::default(),
            );

            assert_eq!(service.rebuild(source_id()).await.unwrap(), expected);
            assert_eq!(*unit_of_work.commits.lock().await, 1);
        }
    }

    #[tokio::test]
    async fn missing_source_releases_the_build_lease() {
        let leases = MemoryFeedBuildLeaseRepository::new(timestamp(0));
        let service = service(
            FakeSources {
                source: Arc::new(Mutex::new(None)),
            },
            leases.clone(),
            FakeUnitOfWorkFactory::default(),
            FeedRebuildConfig::default(),
        );

        assert!(matches!(
            service.rebuild(source_id()).await,
            Err(FeedRebuildError::SourceNotFound { source_id: missing_id })
                if missing_id == source_id()
        ));
        assert!(leases
            .acquire_build(source_id(), "builder-b", Duration::minutes(1))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn renderer_failure_releases_the_build_lease() {
        let leases = MemoryFeedBuildLeaseRepository::new(timestamp(0));
        let invalid_config = FeedRebuildConfig::new(
            Duration::minutes(1),
            Duration::minutes(1),
            "https://rss.example.test/\u{0}",
            "description",
        )
        .unwrap();
        let service = service(
            FakeSources {
                source: Arc::new(Mutex::new(Some(source()))),
            },
            leases.clone(),
            FakeUnitOfWorkFactory::default(),
            invalid_config,
        );

        assert!(matches!(
            service.rebuild(source_id()).await,
            Err(FeedRebuildError::Render(RenderError::InvalidXmlCharacter {
                field: "feed_url"
            }))
        ));
        assert!(leases
            .acquire_build(source_id(), "builder-b", Duration::minutes(1))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn active_lease_is_reported_without_reading_the_source() {
        let leases = MemoryFeedBuildLeaseRepository::new(timestamp(0));
        leases
            .acquire_build(source_id(), "builder-a", Duration::minutes(5))
            .await
            .unwrap();
        let source_reads = Arc::new(Mutex::new(Some(source())));
        let service = service(
            FakeSources {
                source: Arc::clone(&source_reads),
            },
            leases,
            FakeUnitOfWorkFactory::default(),
            FeedRebuildConfig::default(),
        );

        assert_eq!(
            service.rebuild(source_id()).await.unwrap(),
            FeedRebuildOutcome::AlreadyActive
        );
        assert!(source_reads.lock().await.is_some());
    }

    #[test]
    fn rejects_invalid_configuration_and_owner() {
        assert_eq!(
            FeedRebuildConfig::new(Duration::zero(), Duration::minutes(1), "url", "description"),
            Err(FeedRebuildConfigError::InvalidLeaseDuration)
        );
        assert_eq!(
            FeedRebuildConfig::new(Duration::minutes(1), Duration::zero(), "url", "description"),
            Err(FeedRebuildConfigError::InvalidCacheTtl)
        );
        assert_eq!(
            FeedRebuildConfig::new(
                Duration::minutes(1),
                Duration::minutes(1),
                " ",
                "description"
            ),
            Err(FeedRebuildConfigError::EmptyFeedUrl)
        );
        let result = FeedRebuildService::new(
            FeedRebuildDependencies::new(
                FakeSources {
                    source: Arc::new(Mutex::new(Some(source()))),
                },
                FakeArticles,
                MemoryFeedBuildLeaseRepository::new(timestamp(0)),
                FakeUnitOfWorkFactory::default(),
            ),
            FeedRebuildConfig::default(),
            " ",
        );
        assert!(matches!(result, Err(FeedRebuildError::EmptyOwner)));
    }
}
