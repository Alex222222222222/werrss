//! Atomic application handler for claimed source-synchronization jobs.
//!
//! The handler owns the boundary between acquisition and persistence. The
//! injected [`SourceSyncAcquirer`] is responsible for authenticated WeRead
//! list access, credential-free public article fetching, and an authenticated
//! WeRead content fallback; it never receives a database transaction. Acquired
//! pages are normalized outside a
//! transaction, and the final article upserts, source revision/schedule,
//! sync-run completion, optional feed-rebuild enqueue, and fenced job outcome
//! commit together through [`UnitOfWorkFactory`].
//!
//! Runtime composition supplies the authenticated WeRead transport, browser
//! pool, and lease-backed session lifecycle through the concrete acquisition
//! adapter, while tests and future transports can implement the small port
//! without coupling this workflow to WebDriver details.

use chrono::{DateTime, Duration, Utc};
use serde_json::json;

use crate::{
    acquisition::{article_page::ExtractedArticlePage, weread::WeReadArticleReference},
    application::{
        article_backfill_handler::article_backfill_job,
        asset_archive_service::AssetArchiveService,
        source_service::SourceReader,
        sync_service::{
            classify_acquisition_error, should_preserve_cached_asset_representation,
            should_reconcile_assets, ClassifiedSyncFailure, SyncAcquisitionError, SyncService,
            SyncServiceError,
        },
        worker::{JobExecution, JobHandler},
    },
    archive::url_rewriter::rewrite_sanitized_html,
    domain::{
        credentials::WeReadAccountId,
        job::JobType,
        pacing::QuietHours,
        source::{SchedulingGate, Source, SourceId},
        sync::{NewSyncRun, SyncOutcome, SyncRunCompletion, SyncStats},
    },
    persistence::{
        repositories::{
            article_repository::{ArticleRepository, ArticleTransactionRepository},
            asset_repository::{AssetRepositoryError, AssetTransactionRepository},
            job_repository::{JobEnqueueTransaction, JobLease, JobOutcome, JobOutcomeTransaction},
            source_repository::SourceTransactionRepository,
            sync_run_repository::SyncRunTransactionRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

/// Bounds the bytes retained while a source-sync batch is prepared outside
/// the database transaction. A source can contain many articles, so retaining
/// every successful response until the final transaction must not scale
/// without a bound with the number of articles.
const MAX_PENDING_ASSET_BYTES: u64 = 256 * 1024 * 1024;

/// Article references and the account selected for their synchronization job.
///
/// The account is carried separately from the article metadata so an
/// unbound source keeps the same random account when an article falls back
/// from public WeChat HTML to authenticated WeRead content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSyncReferences {
    references: Vec<WeReadArticleReference>,
    account_id: Option<WeReadAccountId>,
}

impl SourceSyncReferences {
    /// Creates a reference batch with the account selected for its job.
    pub fn new(
        references: Vec<WeReadArticleReference>,
        account_id: Option<WeReadAccountId>,
    ) -> Self {
        Self {
            references,
            account_id,
        }
    }

    /// Splits the batch for the synchronization loop.
    pub fn into_parts(self) -> (Vec<WeReadArticleReference>, Option<WeReadAccountId>) {
        (self.references, self.account_id)
    }
}

/// Acquisition port consumed by one source-sync job.
///
/// Implementations should keep authenticated list/session work inside
/// `list_article_references` and return the account selected for that job.
/// The handler passes that account back through `fetch_article` so a public
/// page failure can reacquire the same account lease for authenticated
/// fallback work.
#[async_trait::async_trait]
pub trait SourceSyncAcquirer: Send + Sync {
    /// Lists the current normalized WeRead article references for one source.
    /// The result also carries the account selected for this synchronization
    /// job, when the implementation uses authenticated account selection.
    async fn list_article_references(
        &self,
        source: &Source,
    ) -> Result<SourceSyncReferences, SyncAcquisitionError>;

    /// Fetches and extracts one article, using public HTML first and an
    /// authenticated WeRead content fallback when necessary.
    /// `account_id` is the account selected while listing the source and must
    /// be reused by authenticated fallback implementations.
    async fn fetch_article(
        &self,
        source: &Source,
        reference: &WeReadArticleReference,
        account_id: Option<WeReadAccountId>,
    ) -> Result<ExtractedArticlePage, SyncAcquisitionError>;
}

/// Retry and source-cooldown policy for one source-sync handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSyncJobHandlerConfig {
    retry_after: Duration,
    failure_cooldown: Duration,
    quiet_hours: Option<QuietHours>,
}

impl SourceSyncJobHandlerConfig {
    /// Creates a policy with positive bounded delays.
    pub fn new(
        retry_after: Duration,
        failure_cooldown: Duration,
    ) -> Result<Self, SourceSyncJobHandlerConfigError> {
        if retry_after <= Duration::zero() {
            return Err(SourceSyncJobHandlerConfigError::InvalidRetryAfter);
        }
        if failure_cooldown <= Duration::zero() {
            return Err(SourceSyncJobHandlerConfigError::InvalidFailureCooldown);
        }
        Ok(Self {
            retry_after,
            failure_cooldown,
            quiet_hours: None,
        })
    }

    /// Adds the local quiet-hours policy checked before upstream acquisition.
    pub const fn with_quiet_hours(mut self, quiet_hours: Option<QuietHours>) -> Self {
        self.quiet_hours = quiet_hours;
        self
    }

    /// Returns the delay before a retryable queue failure is retried.
    pub const fn retry_after(self) -> Duration {
        self.retry_after
    }

    /// Returns the source cooldown applied to retryable failures.
    pub const fn failure_cooldown(self) -> Duration {
        self.failure_cooldown
    }

    /// Returns the optional local quiet-hours policy.
    pub const fn quiet_hours(self) -> Option<QuietHours> {
        self.quiet_hours
    }
}

impl Default for SourceSyncJobHandlerConfig {
    fn default() -> Self {
        Self::new(Duration::minutes(1), Duration::minutes(5))
            .expect("default source-sync delays must be valid")
    }
}

/// Invalid source-sync handler policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SourceSyncJobHandlerConfigError {
    /// A retry delay must not create an immediate polling loop.
    #[error("source-sync retry delay must be positive")]
    InvalidRetryAfter,
    /// A source cooldown must be positive so failures are not hot-looped.
    #[error("source-sync failure cooldown must be positive")]
    InvalidFailureCooldown,
}

/// Dependencies for a source-sync job handler.
pub struct SourceSyncJobHandlerDependencies<S, A, C> {
    /// Source read adapter.
    pub sources: S,
    /// Observation-version allocator.
    pub articles: A,
    /// Shared PostgreSQL transaction factory.
    pub unit_of_work: UnitOfWorkFactory,
    /// Network/browser acquisition adapter.
    pub acquirer: C,
    /// Normalization and archive policy.
    pub sync_service: SyncService,
    /// Optional best-effort anonymous asset fetcher. `None` is disabled mode.
    pub asset_archiver: Option<AssetArchiveService>,
}

/// Executes one claimed source-sync job.
pub struct SourceSyncJobHandler<S, A, C> {
    dependencies: SourceSyncJobHandlerDependencies<S, A, C>,
    config: SourceSyncJobHandlerConfig,
}

impl<S, A, C> SourceSyncJobHandler<S, A, C> {
    /// Creates a source-sync handler from injected persistence and acquisition
    /// boundaries.
    pub fn new(
        dependencies: SourceSyncJobHandlerDependencies<S, A, C>,
        config: SourceSyncJobHandlerConfig,
    ) -> Self {
        Self {
            dependencies,
            config,
        }
    }

    /// Returns the configured retry/cooldown policy.
    pub const fn config(&self) -> SourceSyncJobHandlerConfig {
        self.config
    }
}

#[async_trait::async_trait]
impl<S, A, C> JobHandler for SourceSyncJobHandler<S, A, C>
where
    S: SourceReader,
    A: ArticleRepository,
    C: SourceSyncAcquirer,
{
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        tracing::debug!(job_id = %lease.job.id(), "starting source synchronization job");
        let outcome = match self.execute_inner(lease, now).await {
            Ok(()) => {
                tracing::info!(job_id = %lease.job.id(), "source synchronization completed");
                JobExecution::Committed
            }
            Err(SourceSyncExecutionError::BeforeRun(outcome)) => {
                tracing::debug!(
                    job_id = %lease.job.id(),
                    outcome = source_job_outcome_kind(&outcome),
                    "source synchronization stopped before acquisition"
                );
                outcome
            }
            Err(SourceSyncExecutionError::AfterRun { context, failure }) => {
                if is_terminal_source_sync_failure(&failure) {
                    tracing::error!(
                        job_id = %lease.job.id(),
                        source_id = %context.source.id(),
                        outcome = ?failure.outcome(),
                        error = %failure.failure().message(),
                        "source synchronization reached a terminal failure"
                    );
                } else {
                    tracing::warn!(
                        job_id = %lease.job.id(),
                        source_id = %context.source.id(),
                        outcome = ?failure.outcome(),
                        error = %failure.failure().message(),
                        "source synchronization encountered a recoverable failure"
                    );
                }
                match self.finish_failed_run(lease, *context, failure).await {
                    Ok(()) => JobExecution::Committed,
                    Err(()) => {
                        tracing::error!(
                            job_id = %lease.job.id(),
                            "unable to persist source synchronization failure"
                        );
                        retry_execution(now, self.config.retry_after())
                    }
                }
            }
        };
        outcome
    }
}

impl<S, A, C> SourceSyncJobHandler<S, A, C>
where
    S: SourceReader,
    A: ArticleRepository,
    C: SourceSyncAcquirer,
{
    async fn execute_inner(
        &self,
        lease: &JobLease,
        worker_now: DateTime<Utc>,
    ) -> Result<(), SourceSyncExecutionError> {
        let source_id = validate_lease(lease).map_err(SourceSyncExecutionError::BeforeRun)?;
        tracing::debug!(
            job_id = %lease.job.id(),
            source_id = %source_id,
            "preparing source synchronization"
        );
        let started_at = self
            .dependencies
            .unit_of_work
            .database_now()
            .await
            .map_err(|_| {
                SourceSyncExecutionError::BeforeRun(retry_execution(
                    worker_now,
                    self.config.retry_after(),
                ))
            })?;
        if let Some(quiet_hours) = self.config.quiet_hours() {
            if quiet_hours.is_quiet_at(started_at) {
                let resume_at = quiet_hours
                    .next_allowed_at(started_at)
                    .or_else(|| started_at.checked_add_signed(self.config.retry_after()))
                    .unwrap_or(started_at);
                tracing::debug!(source_id = %source_id, resume_at = %resume_at, "source synchronization deferred during quiet hours");
                return Err(SourceSyncExecutionError::BeforeRun(
                    JobExecution::Deferred { resume_at },
                ));
            }
        }
        let source = self
            .dependencies
            .sources
            .find(source_id)
            .await
            .map_err(|_| {
                SourceSyncExecutionError::BeforeRun(retry_execution(
                    worker_now,
                    self.config.retry_after(),
                ))
            })?
            .ok_or_else(|| {
                SourceSyncExecutionError::BeforeRun(JobExecution::Failed {
                    error: "source-sync job references a missing source".to_owned(),
                })
            })?;

        let run_id = uuid::Uuid::new_v4();
        self.start_run(run_id, source_id, lease.job.id(), started_at)
            .await
            .map_err(|_| {
                SourceSyncExecutionError::BeforeRun(retry_execution(
                    worker_now,
                    self.config.retry_after(),
                ))
            })?;

        let context = SourceSyncRunContext {
            source,
            run_id,
            stats: SyncStats::default(),
            backfill_references: Vec::new(),
        };
        let references = match self
            .dependencies
            .acquirer
            .list_article_references(&context.source)
            .await
        {
            Ok(references) => references,
            Err(error) => {
                if matches!(error, SyncAcquisitionError::NoAccountEnrolled) {
                    tracing::warn!(
                        source_id = %context.source.id(),
                        "source synchronization skipped; no usable WeRead account is enrolled"
                    );
                }
                return Err(SourceSyncExecutionError::AfterRun {
                    context: Box::new(context),
                    failure: classify_acquisition_error(&error),
                });
            }
        };
        let (references, selected_account_id) = references.into_parts();
        tracing::debug!(
            source_id = %context.source.id(),
            account_id = ?selected_account_id,
            references = references.len(),
            "source synchronization listed article references"
        );
        let seen = match u32::try_from(references.len()) {
            Ok(seen) => seen,
            Err(_) => {
                return Err(SourceSyncExecutionError::AfterRun {
                    context: Box::new(context),
                    failure: ClassifiedSyncFailure::permanent("source returned too many articles"),
                });
            }
        };

        let mut context = context;
        context.stats.articles_seen = seen;
        let mut observed = Vec::with_capacity(references.len());
        for reference in references {
            let Some(_url) = reference.article_url.as_ref() else {
                tracing::debug!(
                    source_id = %context.source.id(),
                    review_id = %reference.review_id,
                    "ignoring article reference without a public URL"
                );
                context.stats.articles_failed = context.stats.articles_failed.saturating_add(1);
                continue;
            };
            let observation_version = match self
                .dependencies
                .articles
                .allocate_observation_version()
                .await
            {
                Ok(observation_version) => observation_version,
                Err(_) => {
                    return Err(SourceSyncExecutionError::AfterRun {
                        context: Box::new(context),
                        failure: retryable_persistence_failure(),
                    });
                }
            };
            match self
                .dependencies
                .acquirer
                .fetch_article(&context.source, &reference, selected_account_id)
                .await
            {
                Ok(page) => observed.push((reference, page, observation_version)),
                Err(error) => {
                    tracing::debug!(
                        source_id = %context.source.id(),
                        review_id = %reference.review_id,
                        error = %error,
                        "article acquisition failed"
                    );
                    let failure = classify_acquisition_error(&error);
                    if should_queue_article_backfill(&error) {
                        context.stats.articles_failed =
                            context.stats.articles_failed.saturating_add(1);
                        context.backfill_references.push(reference);
                        continue;
                    }
                    if failure.outcome() == SyncOutcome::Failed {
                        context.stats.articles_failed =
                            context.stats.articles_failed.saturating_add(1);
                        continue;
                    }
                    return Err(SourceSyncExecutionError::AfterRun {
                        context: Box::new(context),
                        failure,
                    });
                }
            }
        }

        let finished_at = match self.dependencies.unit_of_work.database_now().await {
            Ok(finished_at) => finished_at,
            Err(_) => {
                return Err(SourceSyncExecutionError::AfterRun {
                    context: Box::new(context),
                    failure: retryable_persistence_failure(),
                });
            }
        };
        let mut prepared = Vec::with_capacity(observed.len());
        let mut pending_asset_bytes = 0_u64;
        for (reference, page, observation_version) in observed {
            match self.dependencies.sync_service.prepare_article(
                context.source.id(),
                &reference,
                page,
                observation_version,
                finished_at,
            ) {
                Ok(article) => {
                    context.stats.archived_articles =
                        context.stats.archived_articles.saturating_add(1);
                    prepared.push(retain_asset_batch_within_memory_budget(
                        self.archive_assets(article).await,
                        &mut pending_asset_bytes,
                    ));
                }
                Err(SyncServiceError::MissingPublishedAt | SyncServiceError::Article(_)) => {
                    tracing::debug!(
                        source_id = %context.source.id(),
                        review_id = %reference.review_id,
                        "article normalization failed"
                    );
                    context.stats.articles_failed = context.stats.articles_failed.saturating_add(1);
                    context.backfill_references.push(reference);
                }
            }
        }

        let retry_context = SourceSyncRunContext {
            source: context.source.clone(),
            run_id: context.run_id,
            stats: context.stats,
            backfill_references: context.backfill_references.clone(),
        };
        self.persist_success(lease, context, prepared, finished_at)
            .await
            .map_err(|_| SourceSyncExecutionError::AfterRun {
                context: Box::new(retry_context),
                failure: retryable_persistence_failure(),
            })
    }

    async fn start_run(
        &self,
        run_id: uuid::Uuid,
        source_id: SourceId,
        job_id: uuid::Uuid,
        started_at: DateTime<Utc>,
    ) -> Result<(), ()> {
        let mut unit_of_work = self
            .dependencies
            .unit_of_work
            .begin()
            .await
            .map_err(|_| ())?;
        unit_of_work
            .sync_runs()
            .start(NewSyncRun {
                id: run_id,
                source_id,
                job_id: Some(job_id),
                started_at,
            })
            .await
            .map_err(|_| ())?;
        unit_of_work.commit().await.map_err(|_| ())
    }

    async fn archive_assets(
        &self,
        prepared: crate::application::sync_service::PreparedArticle,
    ) -> crate::application::sync_service::PreparedArticle {
        let Some(archiver) = &self.dependencies.asset_archiver else {
            return prepared;
        };
        let Some(referer) = prepared.article().original_url.as_ref() else {
            tracing::warn!(
                review_id = %prepared.article().review_id,
                "asset caching skipped because article referer is missing"
            );
            return prepared;
        };
        if prepared.external_assets().is_empty() {
            return prepared;
        }
        let fetched = archiver
            .fetch_assets(referer, prepared.external_assets())
            .await;
        prepared.with_fetched_assets(fetched)
    }

    async fn persist_success(
        &self,
        lease: &JobLease,
        context: SourceSyncRunContext,
        prepared: Vec<crate::application::sync_service::PreparedArticle>,
        finished_at: DateTime<Utc>,
    ) -> Result<(), ()> {
        let source_id = context.source.id();
        let run_id = context.run_id;
        let asset_inputs = prepared
            .iter()
            .flat_map(|article| article.fetched_assets().iter().cloned())
            .collect::<Vec<_>>();
        let mut unit_of_work = self
            .dependencies
            .unit_of_work
            .begin_with_assets(&asset_inputs)
            .await
            .map_err(|_| ())?;
        // Source-owned writes use the source row as their first lock. Source
        // deletion takes the same lock before its cascading article delete,
        // so a worker cannot hold an article row while waiting for the source.
        let current_source = {
            let mut sources = unit_of_work.source();
            sources.find_for_update(source_id).await.map_err(|_| ())?
        };
        let mut changed = false;
        let mut stats = context.stats;
        for prepared in prepared {
            let article = prepared.article().clone();
            let fetched_assets = prepared.fetched_assets().to_vec();
            let mut article_to_persist = article.clone();

            let (created, article_changed) = if let Some(archiver) =
                &self.dependencies.asset_archiver
            {
                // Lock the stored observation before mutating asset
                // relationships. The final article upsert is deliberately
                // postponed until after asset persistence so its
                // feed-visible result compares the final representation with
                // the previously published representation.
                let current_article = {
                    let mut articles = unit_of_work.articles();
                    articles
                        .find_for_update(article.source_id, &article.review_id)
                        .await
                        .map_err(|_| ())?
                };
                let accepted = current_article.as_ref().is_none_or(|current| {
                    article.observation_version >= current.observation_version()
                });
                if !accepted {
                    // A delayed observation must not replace the article or
                    // its asset relationships.
                    (false, false)
                } else {
                    let current_has_cached_representation = current_article
                        .as_ref()
                        .is_some_and(|current| current.content_html().contains("/assets/"));
                    let preserve_cached_representation =
                        should_preserve_cached_asset_representation(
                            current_article
                                .as_ref()
                                .map(|current| current.content_html()),
                            prepared.external_assets().len(),
                            fetched_assets.len(),
                        );
                    if preserve_cached_representation {
                        if let Some(current) = current_article.as_ref() {
                            tracing::warn!(
                                source_id = %source_id,
                                review_id = %article.review_id,
                                fetched_assets = fetched_assets.len(),
                                expected_assets = prepared.external_assets().len(),
                                "asset acquisition was incomplete; preserving the previously archived article"
                            );
                            article_to_persist.content_html = current.content_html().to_owned();
                            article_to_persist.content_hash =
                                current.content_hash().map(str::to_owned);
                        }
                    } else if should_reconcile_assets(&article, article.observation_version) {
                        let stored = {
                            let mut assets = unit_of_work.assets(archiver.policy());
                            assets
                                .replace_for_article(
                                    article.source_id,
                                    &article.review_id,
                                    &fetched_assets,
                                )
                                .await
                        };
                        match stored {
                            Ok(stored) => {
                                stats.archived_assets = stats.archived_assets.saturating_add(
                                    u32::try_from(stored.len()).unwrap_or(u32::MAX),
                                );
                                let replacements = stored
                                    .iter()
                                    .map(|asset| {
                                        (
                                            asset.source_url().clone(),
                                            format!("/assets/{}", asset.id()),
                                        )
                                    })
                                    .collect::<Vec<_>>();
                                let rewritten_html =
                                    rewrite_sanitized_html(&article.content_html, &replacements);
                                if rewritten_html != article.content_html {
                                    article_to_persist.content_html = rewritten_html;
                                    article_to_persist.content_hash =
                                        Some(crate::application::archive_service::sha256_hex(
                                            article_to_persist.content_html.as_bytes(),
                                        ));
                                }
                            }
                            Err(
                                error @ (AssetRepositoryError::CapacityExceeded { .. }
                                | AssetRepositoryError::AssetTooLarge { .. }
                                | AssetRepositoryError::TooManyAssets { .. }),
                            ) => {
                                if current_has_cached_representation {
                                    if let Some(current) = current_article.as_ref() {
                                        article_to_persist.content_html =
                                            current.content_html().to_owned();
                                        article_to_persist.content_hash =
                                            current.content_hash().map(str::to_owned);
                                    }
                                    tracing::warn!(
                                        source_id = %source_id,
                                        review_id = %article.review_id,
                                        error = %error,
                                        "asset replacement was rejected; preserving the previously archived article"
                                    );
                                } else {
                                    let mut assets = unit_of_work.assets(archiver.policy());
                                    assets
                                        .clear_for_article(article.source_id, &article.review_id)
                                        .await
                                        .map_err(|_| ())?;
                                    tracing::warn!(
                                        source_id = %source_id,
                                        review_id = %article.review_id,
                                        error = %error,
                                        "asset was not cached; article remains external and stale asset links were cleared"
                                    );
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    source_id = %source_id,
                                    review_id = %article.review_id,
                                    error = %error,
                                    "asset persistence failed"
                                );
                                return Err(());
                            }
                        }
                    }
                    let result = {
                        let mut articles = unit_of_work.articles();
                        articles.upsert(article_to_persist).await.map_err(|_| ())?
                    };
                    (result.created(), result.feed_visible_change())
                }
            } else {
                let result = {
                    let mut articles = unit_of_work.articles();
                    articles.upsert(article_to_persist).await.map_err(|_| ())?
                };
                (result.created(), result.feed_visible_change())
            };

            if created {
                stats.articles_created = stats.articles_created.saturating_add(1);
            } else if article_changed {
                stats.articles_updated = stats.articles_updated.saturating_add(1);
            }
            changed |= article_changed;
        }

        let feed_revision = if changed {
            let mut sources = unit_of_work.source();
            Some(
                sources
                    .bump_feed_revision(source_id, current_source.feed_revision())
                    .await
                    .map_err(|_| ())?,
            )
        } else {
            None
        };

        if changed {
            let mut queue = unit_of_work.job_enqueue();
            queue
                .enqueue_job(feed_rebuild_job(&current_source, finished_at))
                .await
                .map_err(|_| ())?;
        }

        let queued_backfills = enqueue_backfill_references(
            &mut unit_of_work,
            &context.source,
            &context.backfill_references,
            finished_at,
        )
        .await?;
        if queued_backfills > 0 {
            tracing::info!(source_id = %source_id, queued_backfills, "queued missed article backfill jobs");
        }

        let next_fetch_at = finished_at
            .checked_add_signed(current_source.sync_interval())
            .ok_or(())?;
        {
            let mut sources = unit_of_work.source();
            sources
                .update_schedule(source_id, next_fetch_at, None, None)
                .await
                .map_err(|_| ())?;
        }
        let persisted_stats = stats;
        unit_of_work
            .sync_runs()
            .finish(
                context.run_id,
                SyncRunCompletion {
                    outcome: SyncOutcome::Succeeded,
                    finished_at,
                    stats: persisted_stats,
                    failure: None,
                    feed_revision,
                },
            )
            .await
            .map_err(|_| ())?;
        apply_job_outcome(
            &mut unit_of_work,
            lease,
            JobOutcome::Succeeded {
                job_id: lease.job.id(),
                owner: lease.job.lease_owner().ok_or(())?.to_owned(),
                token: lease.token,
                now: finished_at,
            },
        )
        .await
        .map_err(|_| ())?;
        unit_of_work.commit().await.map_err(|_| ())?;
        tracing::info!(
            source_id = %source_id,
            run_id = %run_id,
            articles_seen = persisted_stats.articles_seen,
            articles_created = persisted_stats.articles_created,
            articles_updated = persisted_stats.articles_updated,
            articles_failed = persisted_stats.articles_failed,
            feed_revision = ?feed_revision,
            "persisted successful source synchronization"
        );
        Ok(())
    }

    async fn finish_failed_run(
        &self,
        lease: &JobLease,
        context: SourceSyncRunContext,
        classified: ClassifiedSyncFailure,
    ) -> Result<(), ()> {
        if is_terminal_source_sync_failure(&classified) {
            tracing::error!(
                source_id = %context.source.id(),
                run_id = %context.run_id,
                outcome = ?classified.outcome(),
                error = %classified.failure().message(),
                "persisting terminal source synchronization failure"
            );
        } else {
            tracing::warn!(
                source_id = %context.source.id(),
                run_id = %context.run_id,
                outcome = ?classified.outcome(),
                error = %classified.failure().message(),
                "persisting source synchronization failure"
            );
        }
        let finished_at = self
            .dependencies
            .unit_of_work
            .database_now()
            .await
            .map_err(|_| ())?;
        let retry_at = finished_at.checked_add_signed(self.config.retry_after);
        let cooldown_until = finished_at.checked_add_signed(self.config.failure_cooldown);
        let mut unit_of_work = self
            .dependencies
            .unit_of_work
            .begin()
            .await
            .map_err(|_| ())?;

        let queued_backfills = enqueue_backfill_references(
            &mut unit_of_work,
            &context.source,
            &context.backfill_references,
            finished_at,
        )
        .await?;
        if queued_backfills > 0 {
            tracing::info!(
                source_id = %context.source.id(),
                queued_backfills,
                "queued missed article backfill jobs while finalizing source failure"
            );
        }

        match classified.outcome() {
            SyncOutcome::AuthenticationRequired => {
                let mut sources = unit_of_work.source();
                let gated_source = sources
                    .set_scheduling_gate(
                        context.source.id(),
                        SchedulingGate::AuthenticationRequired,
                    )
                    .await
                    .map_err(|_| ())?;
                sources
                    .update_schedule(
                        context.source.id(),
                        gated_source.next_fetch_at(),
                        None,
                        None,
                    )
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::RiskControlled => {
                let mut sources = unit_of_work.source();
                let gated_source = sources
                    .set_scheduling_gate(context.source.id(), SchedulingGate::RiskControlled)
                    .await
                    .map_err(|_| ())?;
                sources
                    .update_schedule(
                        context.source.id(),
                        gated_source.next_fetch_at(),
                        None,
                        None,
                    )
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::RetryableFailure => {
                let retry_at = retry_at.ok_or(())?;
                let mut sources = unit_of_work.source();
                sources
                    .update_schedule(context.source.id(), retry_at, cooldown_until, None)
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::Blocked => {
                let mut sources = unit_of_work.source();
                let gated_source = sources
                    .set_scheduling_gate(context.source.id(), SchedulingGate::RiskControlled)
                    .await
                    .map_err(|_| ())?;
                sources
                    .update_schedule(
                        context.source.id(),
                        gated_source.next_fetch_at(),
                        None,
                        None,
                    )
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::Failed => {
                let mut sources = unit_of_work.source();
                let current_source = sources
                    .find_for_update(context.source.id())
                    .await
                    .map_err(|_| ())?;
                let next_fetch_at = finished_at
                    .checked_add_signed(current_source.sync_interval())
                    .ok_or(())?;
                sources
                    .update_schedule(context.source.id(), next_fetch_at, None, None)
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::Running | SyncOutcome::Succeeded | SyncOutcome::Deferred => return Err(()),
        }

        let stats = context.stats;
        unit_of_work
            .sync_runs()
            .finish(
                context.run_id,
                SyncRunCompletion {
                    outcome: classified.outcome(),
                    finished_at,
                    stats,
                    failure: Some(classified.failure().clone()),
                    feed_revision: None,
                },
            )
            .await
            .map_err(|_| ())?;
        let outcome = if classified.outcome() == SyncOutcome::RetryableFailure {
            JobOutcome::Retry {
                job_id: lease.job.id(),
                owner: lease.job.lease_owner().ok_or(())?.to_owned(),
                token: lease.token,
                now: finished_at,
                retry_at: retry_at.ok_or(())?,
                error: classified.failure().message().to_owned(),
            }
        } else {
            JobOutcome::Failed {
                job_id: lease.job.id(),
                owner: lease.job.lease_owner().ok_or(())?.to_owned(),
                token: lease.token,
                now: finished_at,
                error: classified.failure().message().to_owned(),
            }
        };
        apply_job_outcome(&mut unit_of_work, lease, outcome)
            .await
            .map_err(|_| ())?;
        unit_of_work.commit().await.map_err(|_| ())
    }
}

fn retain_asset_batch_within_memory_budget(
    prepared: crate::application::sync_service::PreparedArticle,
    pending_asset_bytes: &mut u64,
) -> crate::application::sync_service::PreparedArticle {
    let fetched_bytes = prepared
        .fetched_assets()
        .iter()
        .map(|asset| asset.bytes.len() as u64)
        .sum::<u64>();
    let available = MAX_PENDING_ASSET_BYTES.saturating_sub(*pending_asset_bytes);
    if fetched_bytes > available {
        tracing::warn!(
            source_id = %prepared.article().source_id,
            review_id = %prepared.article().review_id,
            fetched_bytes,
            pending_asset_bytes = *pending_asset_bytes,
            limit = MAX_PENDING_ASSET_BYTES,
            "source-sync asset memory budget exhausted; leaving this article's assets external"
        );
        prepared.with_fetched_assets(Vec::new())
    } else {
        *pending_asset_bytes = (*pending_asset_bytes).saturating_add(fetched_bytes);
        prepared
    }
}

enum SourceSyncExecutionError {
    BeforeRun(JobExecution),
    AfterRun {
        context: Box<SourceSyncRunContext>,
        failure: ClassifiedSyncFailure,
    },
}

fn source_job_outcome_kind(outcome: &JobExecution) -> &'static str {
    match outcome {
        JobExecution::Succeeded => "succeeded",
        JobExecution::Committed => "committed",
        JobExecution::Deferred { .. } => "deferred",
        JobExecution::Retry { .. } => "retry",
        JobExecution::Failed { .. } => "failed",
    }
}

fn is_terminal_source_sync_failure(failure: &ClassifiedSyncFailure) -> bool {
    failure.outcome() == SyncOutcome::Failed && failure.log_as_error()
}

struct SourceSyncRunContext {
    source: Source,
    run_id: uuid::Uuid,
    stats: SyncStats,
    backfill_references: Vec<WeReadArticleReference>,
}

async fn enqueue_backfill_references(
    unit_of_work: &mut crate::persistence::unit_of_work::UnitOfWork<'_>,
    source: &Source,
    references: &[WeReadArticleReference],
    enqueued_at: DateTime<Utc>,
) -> Result<usize, ()> {
    if references.is_empty() {
        return Ok(0);
    }

    let mut queue = unit_of_work.job_enqueue();
    let mut queued = 0;
    for reference in references {
        if let Some(job) = article_backfill_job(source, reference, enqueued_at) {
            queue.enqueue_job(job).await.map_err(|_| ())?;
            queued += 1;
        }
    }
    Ok(queued)
}

fn should_queue_article_backfill(error: &SyncAcquisitionError) -> bool {
    matches!(
        error,
        SyncAcquisitionError::ArticlePage(
            crate::acquisition::article_page::ArticlePageError::Browser(_)
                | crate::acquisition::article_page::ArticlePageError::OperationTimedOut
        ) | SyncAcquisitionError::WeRead(
            crate::acquisition::weread::WeReadAdapterError::LeaseLost { .. }
                | crate::acquisition::weread::WeReadAdapterError::LeaseBackend(_)
                | crate::acquisition::weread::WeReadAdapterError::Protocol(_)
                | crate::acquisition::weread::WeReadAdapterError::UnexpectedResponse(_)
                | crate::acquisition::weread::WeReadAdapterError::Browser(_)
        )
    )
}

fn validate_lease(lease: &JobLease) -> Result<SourceId, JobExecution> {
    if lease.job.job_type() != JobType::SourceSync {
        return Err(JobExecution::Failed {
            error: "source-sync handler received an unsupported job type".to_owned(),
        });
    }
    let source_uuid = lease.job.source_id().ok_or_else(|| JobExecution::Failed {
        error: "source-sync job is missing its source".to_owned(),
    })?;
    let payload_source_id = lease
        .job
        .payload()
        .get("source_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse::<uuid::Uuid>().ok());
    if payload_source_id != Some(source_uuid) {
        return Err(JobExecution::Failed {
            error: "source-sync job payload does not match its source".to_owned(),
        });
    }
    if lease.job.lease_owner().is_none() {
        return Err(JobExecution::Failed {
            error: "source-sync job lease is incomplete".to_owned(),
        });
    }
    Ok(SourceId::from_uuid(source_uuid))
}

fn retry_execution(now: DateTime<Utc>, retry_after: Duration) -> JobExecution {
    now.checked_add_signed(retry_after).map_or_else(
        || JobExecution::Failed {
            error: "source synchronization retry time is outside the supported range".to_owned(),
        },
        |retry_at| JobExecution::Retry {
            retry_at,
            error: "source synchronization persistence is temporarily unavailable".to_owned(),
        },
    )
}

fn retryable_persistence_failure() -> ClassifiedSyncFailure {
    ClassifiedSyncFailure::retryable(
        "source synchronization persistence is temporarily unavailable",
    )
}

fn feed_rebuild_job(source: &Source, now: DateTime<Utc>) -> crate::domain::job::NewJob {
    crate::domain::job::NewJob {
        job_type: JobType::FeedRebuild,
        source_id: Some(source.id().as_uuid()),
        priority: source.priority(),
        run_after: now,
        max_attempts: source.max_attempts(),
        payload: json!({"source_id": source.id().to_string()}),
        dedupe_key: format!("feed_rebuild:{}", source.id()),
        now,
    }
}

async fn apply_job_outcome(
    unit_of_work: &mut crate::persistence::unit_of_work::UnitOfWork<'_>,
    _lease: &JobLease,
    outcome: JobOutcome,
) -> Result<(), crate::persistence::repositories::job_repository::JobRepositoryError> {
    unit_of_work
        .job_outcomes()
        .apply_outcome(outcome)
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::*;
    use crate::domain::job::{Job, NewJob};
    use crate::domain::sync::SyncFailureClass;
    use crate::{
        acquisition::{
            article_page::ExtractedArticlePage,
            weread::{WeReadAdapterError, WeReadArticleReference},
        },
        archive::asset_store::AssetInput,
    };
    use serde_json::json;
    use url::Url;
    use uuid::Uuid;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    #[test]
    fn rejects_non_positive_delays() {
        assert_eq!(
            SourceSyncJobHandlerConfig::new(Duration::zero(), Duration::minutes(1)),
            Err(SourceSyncJobHandlerConfigError::InvalidRetryAfter)
        );
        assert_eq!(
            SourceSyncJobHandlerConfig::new(Duration::minutes(1), Duration::zero()),
            Err(SourceSyncJobHandlerConfigError::InvalidFailureCooldown)
        );
    }

    #[test]
    fn retry_time_overflow_is_bounded() {
        assert_eq!(
            retry_execution(DateTime::<Utc>::MAX_UTC, Duration::seconds(1)),
            JobExecution::Failed {
                error: "source synchronization retry time is outside the supported range"
                    .to_owned()
            }
        );
        assert_eq!(
            retry_execution(at(10), Duration::minutes(1)),
            JobExecution::Retry {
                retry_at: at(70),
                error: "source synchronization persistence is temporarily unavailable".to_owned()
            }
        );
    }

    #[test]
    fn permanent_failures_are_terminal_and_retryable_failures_are_retriable() {
        let permanent = ClassifiedSyncFailure::permanent("invalid article");
        assert_eq!(permanent.outcome(), SyncOutcome::Failed);

        let retryable = classify_acquisition_error(&SyncAcquisitionError::ArticlePage(
            crate::acquisition::article_page::ArticlePageError::OperationTimedOut,
        ));
        assert_eq!(retryable.outcome(), SyncOutcome::RetryableFailure);
    }

    #[test]
    fn terminal_source_sync_failures_are_selected_for_error_logging() {
        let failure = ClassifiedSyncFailure::permanent("invalid article");

        assert!(is_terminal_source_sync_failure(&failure));
    }

    #[test]
    fn retryable_source_sync_failures_are_selected_for_warning_logging() {
        let failure = ClassifiedSyncFailure::retryable("upstream unavailable");

        assert!(!is_terminal_source_sync_failure(&failure));
    }

    #[test]
    fn missing_account_source_sync_failures_are_selected_for_warning_logging() {
        let failure = classify_acquisition_error(&SyncAcquisitionError::NoAccountEnrolled);

        assert_eq!(failure.outcome(), SyncOutcome::Failed);
        assert!(!is_terminal_source_sync_failure(&failure));
    }

    #[test]
    fn recoverable_article_failures_are_backfill_candidates() {
        assert!(should_queue_article_backfill(
            &SyncAcquisitionError::ArticlePage(
                crate::acquisition::article_page::ArticlePageError::OperationTimedOut,
            )
        ));
        assert!(should_queue_article_backfill(
            &SyncAcquisitionError::WeRead(WeReadAdapterError::Protocol(
                "temporary response".to_owned()
            ),)
        ));
    }

    #[test]
    fn unexpected_weread_responses_are_backfill_candidates() {
        assert!(should_queue_article_backfill(
            &SyncAcquisitionError::WeRead(
                crate::acquisition::weread::WeReadAdapterError::UnexpectedResponse(
                    "data must be an array".to_owned()
                )
            )
        ));
    }

    #[test]
    fn source_wide_and_blocked_failures_are_not_backfill_candidates() {
        assert!(!should_queue_article_backfill(
            &SyncAcquisitionError::NoAccountEnrolled
        ));
        assert!(!should_queue_article_backfill(
            &SyncAcquisitionError::WeRead(WeReadAdapterError::AuthenticationExpired { code: 401 })
        ));
        assert!(!should_queue_article_backfill(
            &SyncAcquisitionError::ArticlePage(
                crate::acquisition::article_page::ArticlePageError::VerificationRequired
            )
        ));
        assert!(!should_queue_article_backfill(
            &SyncAcquisitionError::ArticlePage(
                crate::acquisition::article_page::ArticlePageError::InvalidExtraction(
                    "invalid article".to_owned()
                )
            )
        ));
    }

    #[test]
    fn a_missing_reference_url_is_classified_as_a_per_article_failure() {
        let error = classify_acquisition_error(&SyncAcquisitionError::WeRead(
            WeReadAdapterError::InvalidArticleUrl,
        ));
        assert_eq!(error.outcome(), SyncOutcome::Failed);
        assert_eq!(error.failure().class(), SyncFailureClass::Permanent);
    }

    #[test]
    fn drops_fetched_assets_that_would_exceed_the_source_batch_memory_bound() {
        let reference = WeReadArticleReference {
            review_id: "asset-memory-bound".to_owned(),
            article_url: Some(
                "https://mp.weixin.qq.com/s/asset-memory-bound"
                    .parse()
                    .unwrap(),
            ),
            title: Some("Asset memory bound".to_owned()),
            summary: None,
            author: None,
            cover_url: None,
            published_at: Some(at(1)),
        };
        let page = ExtractedArticlePage {
            canonical_url: "https://mp.weixin.qq.com/s/asset-memory-bound"
                .parse()
                .unwrap(),
            title: "Asset memory bound".to_owned(),
            author: None,
            summary: None,
            published_at: Some(at(1)),
            content_html: "<p>body</p><img src=\"https://cdn.example/image.png\">".to_owned(),
            cover_url: None,
        };
        let prepared = SyncService::new()
            .prepare_article(
                SourceId::from_uuid(Uuid::from_u128(1)),
                &reference,
                page,
                crate::domain::article::ArticleObservationVersion::from_u64(1),
                at(2),
            )
            .unwrap()
            .with_fetched_assets(vec![AssetInput::new(
                Url::parse("https://cdn.example/image.png").unwrap(),
                Url::parse("https://cdn.example/image.png").unwrap(),
                "image/png".to_owned(),
                vec![0, 1],
                0,
                Url::parse("https://mp.weixin.qq.com/s/asset-memory-bound").unwrap(),
                Some("https://mp.weixin.qq.com".to_owned()),
                None,
            )]);
        let mut pending_asset_bytes = MAX_PENDING_ASSET_BYTES - 1;

        let retained = retain_asset_batch_within_memory_budget(prepared, &mut pending_asset_bytes);

        assert!(retained.fetched_assets().is_empty());
        assert_eq!(pending_asset_bytes, MAX_PENDING_ASSET_BYTES - 1);
    }

    fn claimed_job(
        job_type: JobType,
        source_id: Option<Uuid>,
        payload: serde_json::Value,
    ) -> JobLease {
        let mut job = Job::new(NewJob {
            job_type,
            source_id,
            priority: 0,
            run_after: at(0),
            max_attempts: 3,
            payload,
            dedupe_key: "test-source-sync".to_owned(),
            now: at(0),
        })
        .expect("test job should be valid");
        let token = job
            .claim("test-worker", at(0), Duration::minutes(5))
            .expect("test job should be claimable");
        JobLease { job, token }
    }

    #[test]
    fn rejects_wrong_job_shapes_before_opening_a_source_or_database() {
        let source_uuid = Uuid::from_u128(1);
        let missing_payload = claimed_job(JobType::SourceSync, Some(source_uuid), json!({}));
        assert_eq!(
            validate_lease(&missing_payload),
            Err(JobExecution::Failed {
                error: "source-sync job payload does not match its source".to_owned()
            })
        );

        let wrong_type = claimed_job(
            JobType::FeedRebuild,
            Some(source_uuid),
            json!({"source_id": source_uuid.to_string()}),
        );
        assert_eq!(
            validate_lease(&wrong_type),
            Err(JobExecution::Failed {
                error: "source-sync handler received an unsupported job type".to_owned()
            })
        );

        let missing_source = claimed_job(JobType::SourceSync, None, json!({}));
        assert_eq!(
            validate_lease(&missing_source),
            Err(JobExecution::Failed {
                error: "source-sync job is missing its source".to_owned()
            })
        );
    }
}
