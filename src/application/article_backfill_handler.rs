//! Durable repair of article observations that were missed by source sync.
//!
//! Source synchronization intentionally keeps one article failure from
//! discarding every other article in the same batch. When a reference has a
//! stable identity and a verified public URL, the source handler enqueues an
//! `article_backfill` job. This handler retries that one article independently
//! with the normal authenticated/public acquisition adapter and commits the
//! repaired article, feed revision, feed-rebuild enqueue, and fenced job
//! completion in one PostgreSQL unit of work.
//!
//! Backfill payloads contain only article identity and non-secret metadata.
//! Account selection remains an acquisition concern, so a later attempt can
//! use the source's fixed account or a currently usable enrolled account. The
//! existing job lease and `max_attempts` budget bound retries and allow another
//! worker to recover the job after a process crash.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    acquisition::weread::WeReadArticleReference,
    application::{
        asset_archive_service::AssetArchiveService,
        source_service::SourceReader,
        source_sync_handler::SourceSyncAcquirer,
        sync_service::{
            classify_acquisition_error, should_preserve_cached_asset_representation,
            should_reconcile_assets, SyncAcquisitionError, SyncService,
        },
        worker::{JobExecution, JobHandler},
    },
    archive::url_rewriter::rewrite_sanitized_html,
    domain::{
        job::{JobType, NewJob},
        pacing::QuietHours,
        source::{Source, SourceId, VerifiedWechatArticleUrl},
        sync::SyncOutcome,
    },
    persistence::{
        repositories::{
            article_repository::{ArticleRepository, ArticleTransactionRepository},
            asset_repository::{AssetRepositoryError, AssetTransactionRepository},
            job_repository::{JobEnqueueTransaction, JobLease, JobOutcome, JobOutcomeTransaction},
            source_repository::SourceTransactionRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

/// Secret-free parameters stored in an article-backfill job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArticleBackfillPayload {
    /// Source owning the article.
    pub source_id: Uuid,
    /// Stable WeRead review identity.
    pub review_id: String,
    /// Verified public WeChat URL used by the acquisition fallback.
    pub article_url: VerifiedWechatArticleUrl,
    /// Metadata recovered from the list response.
    pub title: Option<String>,
    /// Optional list summary.
    pub summary: Option<String>,
    /// Optional list author.
    pub author: Option<String>,
    /// Optional list cover URL.
    pub cover_url: Option<String>,
    /// Optional list publication timestamp.
    pub published_at: Option<DateTime<Utc>>,
}

impl ArticleBackfillPayload {
    /// Creates a payload only when the reference can be fetched later.
    pub fn from_reference(source_id: SourceId, reference: &WeReadArticleReference) -> Option<Self> {
        let review_id = reference.review_id.trim();
        if review_id.is_empty() {
            return None;
        }
        Some(Self {
            source_id: source_id.as_uuid(),
            review_id: review_id.to_owned(),
            article_url: reference.article_url.clone()?,
            title: reference.title.clone(),
            summary: reference.summary.clone(),
            author: reference.author.clone(),
            cover_url: reference.cover_url.clone(),
            published_at: reference.published_at,
        })
    }

    fn into_reference(self) -> Result<WeReadArticleReference, &'static str> {
        let review_id = self.review_id.trim().to_owned();
        if review_id.is_empty() {
            return Err("article backfill review_id is empty");
        }
        if self.source_id.is_nil() {
            return Err("article backfill source_id is nil");
        }
        Ok(WeReadArticleReference {
            review_id,
            article_url: Some(self.article_url),
            title: self.title,
            summary: self.summary,
            author: self.author,
            cover_url: self.cover_url,
            published_at: self.published_at,
        })
    }
}

/// Returns the stable active-job key for one source/article pair.
pub fn article_backfill_dedupe_key(source_id: SourceId, review_id: &str) -> String {
    format!("article_backfill:{source_id}:{}", review_id.trim())
}

/// Builds an article-backfill job for a fetchable reference.
///
/// References without a public URL cannot be passed to the article-page or
/// authenticated content adapters and therefore do not produce a job.
pub fn article_backfill_job(
    source: &Source,
    reference: &WeReadArticleReference,
    now: DateTime<Utc>,
) -> Option<NewJob> {
    let payload = ArticleBackfillPayload::from_reference(source.id(), reference)?;
    Some(NewJob {
        job_type: JobType::ArticleBackfill,
        source_id: Some(source.id().as_uuid()),
        priority: source.priority(),
        run_after: now,
        max_attempts: source.max_attempts(),
        payload: serde_json::to_value(payload)
            .expect("article backfill payload contains only serializable values"),
        dedupe_key: article_backfill_dedupe_key(source.id(), &reference.review_id),
        now,
    })
}

/// Dependencies for one article-backfill job handler.
pub struct ArticleBackfillJobHandlerDependencies<S, A, C> {
    /// Source reader.
    pub sources: S,
    /// Observation-version allocator.
    pub articles: A,
    /// Shared transaction factory.
    pub unit_of_work: UnitOfWorkFactory,
    /// Browser/account-backed article acquisition adapter.
    pub acquirer: C,
    /// Article normalization and sanitization policy.
    pub sync_service: SyncService,
    /// Optional best-effort anonymous asset fetcher. `None` is disabled mode.
    pub asset_archiver: Option<AssetArchiveService>,
}

/// Retry policy for article-backfill persistence and upstream failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArticleBackfillJobHandlerConfig {
    retry_after: Duration,
    quiet_hours: Option<QuietHours>,
}

impl ArticleBackfillJobHandlerConfig {
    /// Creates a policy with a positive retry delay.
    pub fn new(retry_after: Duration) -> Result<Self, ArticleBackfillJobHandlerConfigError> {
        if retry_after <= Duration::zero() {
            return Err(ArticleBackfillJobHandlerConfigError::InvalidRetryAfter);
        }
        Ok(Self {
            retry_after,
            quiet_hours: None,
        })
    }

    /// Adds the local quiet-hours policy checked before upstream acquisition.
    pub const fn with_quiet_hours(mut self, quiet_hours: Option<QuietHours>) -> Self {
        self.quiet_hours = quiet_hours;
        self
    }

    /// Returns the delay before retryable failures are retried.
    pub const fn retry_after(self) -> Duration {
        self.retry_after
    }

    /// Returns the optional local quiet-hours policy.
    pub const fn quiet_hours(self) -> Option<QuietHours> {
        self.quiet_hours
    }
}

impl Default for ArticleBackfillJobHandlerConfig {
    fn default() -> Self {
        Self::new(Duration::minutes(1)).expect("default backfill retry delay must be valid")
    }
}

/// Invalid article-backfill handler policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ArticleBackfillJobHandlerConfigError {
    /// A non-positive delay would create a hot retry loop.
    #[error("article-backfill retry delay must be positive")]
    InvalidRetryAfter,
}

/// Executes one claimed article-backfill job.
pub struct ArticleBackfillJobHandler<S, A, C> {
    dependencies: ArticleBackfillJobHandlerDependencies<S, A, C>,
    config: ArticleBackfillJobHandlerConfig,
}

impl<S, A, C> ArticleBackfillJobHandler<S, A, C> {
    /// Creates a handler from injected persistence and acquisition ports.
    pub fn new(
        dependencies: ArticleBackfillJobHandlerDependencies<S, A, C>,
        config: ArticleBackfillJobHandlerConfig,
    ) -> Self {
        Self {
            dependencies,
            config,
        }
    }

    /// Returns the configured retry policy.
    pub const fn config(&self) -> ArticleBackfillJobHandlerConfig {
        self.config
    }
}

#[async_trait::async_trait]
impl<S, A, C> JobHandler for ArticleBackfillJobHandler<S, A, C>
where
    S: SourceReader,
    A: ArticleRepository,
    C: SourceSyncAcquirer,
{
    #[tracing::instrument(skip_all, level = "debug", fields(job_id = %lease.job.id()))]
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        let (source_id, reference) = match validate_lease(lease) {
            Ok(value) => value,
            Err(error) => return error,
        };

        let started_at = match self.dependencies.unit_of_work.database_now().await {
            Ok(now) => now,
            Err(error) => {
                tracing::warn!(source_id = %source_id, error = %error, "unable to read article backfill start time");
                return retry_execution(
                    now,
                    self.config.retry_after(),
                    "article backfill storage is temporarily unavailable",
                );
            }
        };
        if let Some(quiet_hours) = self.config.quiet_hours() {
            if quiet_hours.is_quiet_at(started_at) {
                let resume_at = quiet_hours
                    .next_allowed_at(started_at)
                    .or_else(|| started_at.checked_add_signed(self.config.retry_after()))
                    .unwrap_or(started_at);
                tracing::debug!(source_id = %source_id, resume_at = %resume_at, "article backfill deferred during quiet hours");
                return JobExecution::Deferred { resume_at };
            }
        }

        let source = match self.dependencies.sources.find(source_id).await {
            Ok(Some(source)) => source,
            Ok(None) => {
                tracing::error!(source_id = %source_id, "article backfill source no longer exists");
                return JobExecution::Failed {
                    error: "article backfill source is missing".to_owned(),
                };
            }
            Err(error) => {
                tracing::warn!(source_id = %source_id, error = %error, "unable to load article backfill source");
                return retry_execution(
                    now,
                    self.config.retry_after(),
                    "article backfill storage is temporarily unavailable",
                );
            }
        };

        let observation_version = match self
            .dependencies
            .articles
            .allocate_observation_version()
            .await
        {
            Ok(version) => version,
            Err(error) => {
                tracing::warn!(source_id = %source_id, error = %error, "unable to allocate article backfill observation version");
                return retry_execution(
                    now,
                    self.config.retry_after(),
                    "article backfill storage is temporarily unavailable",
                );
            }
        };

        let page = match self
            .dependencies
            .acquirer
            .fetch_article(&source, &reference, None)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                let execution =
                    classify_acquisition_failure(&error, now, self.config.retry_after());
                match &execution {
                    JobExecution::Retry { retry_at, error } => tracing::warn!(
                        source_id = %source_id,
                        review_id = %reference.review_id,
                        retry_at = %retry_at,
                        error,
                        "article backfill will be retried"
                    ),
                    JobExecution::Failed { error } => tracing::error!(
                        source_id = %source_id,
                        review_id = %reference.review_id,
                        error,
                        "article backfill reached a terminal acquisition failure"
                    ),
                    JobExecution::Succeeded
                    | JobExecution::Committed
                    | JobExecution::Deferred { .. } => {}
                }
                return execution;
            }
        };

        let finished_at = match self.dependencies.unit_of_work.database_now().await {
            Ok(now) => now,
            Err(error) => {
                tracing::warn!(source_id = %source_id, error = %error, "unable to read article backfill completion time");
                return retry_execution(
                    now,
                    self.config.retry_after(),
                    "article backfill storage is temporarily unavailable",
                );
            }
        };
        let prepared = match self.dependencies.sync_service.prepare_article(
            source_id,
            &reference,
            page,
            observation_version,
            finished_at,
        ) {
            Ok(article) => article,
            Err(error) => {
                tracing::error!(
                    source_id = %source_id,
                    review_id = %reference.review_id,
                    error = %error,
                    "article backfill normalization failed"
                );
                return JobExecution::Failed {
                    error: "article backfill data could not be normalized".to_owned(),
                };
            }
        };
        let prepared = self.archive_assets(prepared).await;

        match self
            .persist_success(lease, source_id, prepared, finished_at)
            .await
        {
            Ok(()) => {
                tracing::info!(source_id = %source_id, review_id = %reference.review_id, "article backfill committed");
                JobExecution::Committed
            }
            Err(()) => {
                tracing::warn!(source_id = %source_id, review_id = %reference.review_id, "article backfill persistence failed");
                retry_execution(
                    now,
                    self.config.retry_after(),
                    "article backfill storage is temporarily unavailable",
                )
            }
        }
    }
}

impl<S, A, C> ArticleBackfillJobHandler<S, A, C>
where
    S: SourceReader,
    A: ArticleRepository,
    C: SourceSyncAcquirer,
{
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
        source_id: SourceId,
        prepared: crate::application::sync_service::PreparedArticle,
        finished_at: DateTime<Utc>,
    ) -> Result<(), ()> {
        let asset_inputs = prepared.fetched_assets().to_vec();
        let mut unit_of_work = self
            .dependencies
            .unit_of_work
            .begin_article_acquisition_with_assets(&asset_inputs)
            .await
            .map_err(|_| ())?;
        // Source-owned writes use the source row as their first lock. Source
        // deletion takes the same lock before its cascading article delete,
        // so a worker cannot hold an article row while waiting for the source.
        let current_source = {
            let mut sources = unit_of_work.source();
            sources.find_for_update(source_id).await.map_err(|_| ())?
        };

        let article = prepared.article().clone();
        let fetched_assets = prepared.fetched_assets().to_vec();
        let mut article_to_persist = article.clone();
        let changed = if let Some(archiver) = &self.dependencies.asset_archiver {
            // Lock the stored observation before mutating asset relationships.
            // The final article upsert is deliberately postponed until after
            // asset persistence so its feed-visible result compares the final
            // representation with the previously published representation.
            let current_article = {
                let mut articles = unit_of_work.articles();
                articles
                    .find_for_update(article.source_id, &article.review_id)
                    .await
                    .map_err(|_| ())?
            };
            let accepted = current_article
                .as_ref()
                .is_none_or(|current| article.observation_version >= current.observation_version());
            if !accepted {
                // A delayed observation must not replace the article or its
                // asset relationships.
                false
            } else {
                // Only an accepted full observation owns the current set of
                // asset relationships. Partial observations preserve links
                // from the accepted article version.
                let current_has_cached_representation = current_article
                    .as_ref()
                    .is_some_and(|current| current.content_html().contains("/assets/"));
                let complete_assets = crate::application::sync_service::complete_asset_set(
                    prepared.external_assets(),
                    &fetched_assets,
                );
                let preserve_cached_representation = should_preserve_cached_asset_representation(
                    current_article
                        .as_ref()
                        .map(|current| current.content_html()),
                    prepared.external_assets(),
                    &fetched_assets,
                );
                if preserve_cached_representation {
                    if let Some(current) = current_article.as_ref() {
                        tracing::warn!(
                            source_id = %source_id,
                            review_id = %article.review_id,
                            fetched_assets = fetched_assets.len(),
                            expected_assets = prepared.external_assets().len(),
                            "asset acquisition was incomplete during backfill; preserving the previously archived article"
                        );
                        article_to_persist.content_html = current.content_html().to_owned();
                        article_to_persist.content_hash = current.content_hash().map(str::to_owned);
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
                            if complete_assets {
                                let mut assets = unit_of_work.assets(archiver.policy());
                                assets
                                    .reset_article_refresh_lineage(
                                        article.source_id,
                                        &article.review_id,
                                    )
                                    .await
                                    .map_err(|_| ())?;
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
                                    "asset replacement was rejected during backfill; preserving the previously archived article"
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
                                    "asset was not cached during backfill; article remains external and stale asset links were cleared"
                                );
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                source_id = %source_id,
                                review_id = %article.review_id,
                                error = %error,
                                "asset persistence failed during backfill"
                            );
                            return Err(());
                        }
                    }
                }
                let result = {
                    let mut articles = unit_of_work.articles();
                    articles.upsert(article_to_persist).await.map_err(|_| ())?
                };

                result.feed_visible_change()
            }
        } else {
            let mut articles = unit_of_work.articles();
            articles
                .upsert(article_to_persist)
                .await
                .map_err(|_| ())?
                .feed_visible_change()
        };

        if changed {
            let feed_revision = {
                let mut sources = unit_of_work.source();
                sources
                    .bump_feed_revision(source_id, current_source.feed_revision())
                    .await
                    .map_err(|_| ())?
            };
            tracing::debug!(source_id = %source_id, feed_revision = %feed_revision, "article backfill advanced feed revision");

            let mut queue = unit_of_work.job_enqueue();
            queue
                .enqueue_job(feed_rebuild_job(&current_source, finished_at))
                .await
                .map_err(|_| ())?;
        }

        unit_of_work
            .job_outcomes()
            .apply_outcome(JobOutcome::Succeeded {
                job_id: lease.job.id(),
                owner: lease.job.lease_owner().ok_or(())?.to_owned(),
                token: lease.token,
                now: finished_at,
            })
            .await
            .map_err(|_| ())?;
        unit_of_work.commit().await.map_err(|_| ())
    }
}

fn validate_lease(lease: &JobLease) -> Result<(SourceId, WeReadArticleReference), JobExecution> {
    if lease.job.job_type() != JobType::ArticleBackfill {
        return Err(JobExecution::Failed {
            error: "article backfill handler received an unsupported job type".to_owned(),
        });
    }
    let source_uuid = lease.job.source_id().ok_or_else(|| JobExecution::Failed {
        error: "article backfill job is missing its source".to_owned(),
    })?;
    if source_uuid.is_nil() {
        return Err(JobExecution::Failed {
            error: "article backfill job source is invalid".to_owned(),
        });
    }
    let payload = serde_json::from_value::<ArticleBackfillPayload>(lease.job.payload().clone())
        .map_err(|_| JobExecution::Failed {
            error: "article backfill job payload is invalid".to_owned(),
        })?;
    if payload.source_id != source_uuid {
        return Err(JobExecution::Failed {
            error: "article backfill job payload does not match its source".to_owned(),
        });
    }
    let source_id = SourceId::from_uuid(source_uuid);
    let reference = payload.into_reference().map_err(|_| JobExecution::Failed {
        error: "article backfill job payload is invalid".to_owned(),
    })?;
    if lease.job.dedupe_key() != article_backfill_dedupe_key(source_id, &reference.review_id) {
        return Err(JobExecution::Failed {
            error: "article backfill job dedupe key does not match its article".to_owned(),
        });
    }
    if lease.job.lease_owner().is_none() {
        return Err(JobExecution::Failed {
            error: "article backfill job lease is incomplete".to_owned(),
        });
    }
    Ok((source_id, reference))
}

fn classify_acquisition_failure(
    error: &SyncAcquisitionError,
    now: DateTime<Utc>,
    retry_after: Duration,
) -> JobExecution {
    if matches!(error, SyncAcquisitionError::NoAccountEnrolled) {
        return retry_execution(
            now,
            retry_after,
            "article backfill is waiting for a usable WeRead account",
        );
    }
    let classified = classify_acquisition_error(error);
    if classified.outcome() == SyncOutcome::RetryableFailure {
        retry_execution(
            now,
            retry_after,
            "article backfill acquisition failed temporarily",
        )
    } else {
        JobExecution::Failed {
            error: classified.failure().message().to_owned(),
        }
    }
}

fn retry_execution(
    now: DateTime<Utc>,
    retry_after: Duration,
    message: &'static str,
) -> JobExecution {
    now.checked_add_signed(retry_after).map_or_else(
        || JobExecution::Failed {
            error: format!("{message}; retry time is outside the supported range"),
        },
        |retry_at| JobExecution::Retry {
            retry_at,
            error: message.to_owned(),
        },
    )
}

fn feed_rebuild_job(source: &Source, now: DateTime<Utc>) -> NewJob {
    NewJob {
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

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::{
        acquisition::{article_page::ArticlePageError, weread::WeReadAdapterError},
        domain::job::{Job, NewJob},
    };

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    fn reference() -> WeReadArticleReference {
        WeReadArticleReference {
            review_id: "review-1".to_owned(),
            article_url: Some("https://mp.weixin.qq.com/s/review-1".parse().unwrap()),
            title: Some("title".to_owned()),
            summary: Some("summary".to_owned()),
            author: Some("author".to_owned()),
            cover_url: None,
            published_at: Some(Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()),
        }
    }

    fn lease(payload: serde_json::Value, dedupe_key: &str) -> JobLease {
        let mut job = Job::new(NewJob {
            job_type: JobType::ArticleBackfill,
            source_id: Some(source_id().as_uuid()),
            priority: 0,
            run_after: Utc.timestamp_opt(0, 0).single().unwrap(),
            max_attempts: 3,
            payload,
            dedupe_key: dedupe_key.to_owned(),
            now: Utc.timestamp_opt(0, 0).single().unwrap(),
        })
        .unwrap();
        let token = job
            .claim(
                "worker",
                Utc.timestamp_opt(0, 0).single().unwrap(),
                Duration::minutes(5),
            )
            .unwrap();
        JobLease { job, token }
    }

    #[test]
    fn payload_round_trips_all_non_secret_reference_metadata() {
        let payload = ArticleBackfillPayload::from_reference(source_id(), &reference()).unwrap();
        let value = serde_json::to_value(&payload).unwrap();
        let decoded: ArticleBackfillPayload = serde_json::from_value(value).unwrap();

        assert_eq!(decoded, payload);
        assert_eq!(
            decoded.article_url.as_str(),
            "https://mp.weixin.qq.com/s/review-1"
        );
    }

    #[test]
    fn job_is_not_created_for_a_reference_without_a_public_url() {
        let mut reference = reference();
        reference.article_url = None;

        assert!(article_backfill_job(
            &Source::new(crate::domain::source::NewSource::test_default()).unwrap(),
            &reference,
            Utc.timestamp_opt(0, 0).single().unwrap(),
        )
        .is_none());
    }

    #[test]
    fn job_is_not_created_for_a_reference_without_a_review_id() {
        let mut reference = reference();
        reference.review_id = " \n\t".to_owned();

        assert!(article_backfill_job(
            &Source::new(crate::domain::source::NewSource::test_default()).unwrap(),
            &reference,
            Utc.timestamp_opt(0, 0).single().unwrap(),
        )
        .is_none());
    }

    #[test]
    fn lease_validation_rejects_payload_source_mismatch() {
        let payload = ArticleBackfillPayload::from_reference(source_id(), &reference()).unwrap();
        let mut value = serde_json::to_value(payload).unwrap();
        value["source_id"] = json!(Uuid::from_u128(2).to_string());
        assert!(matches!(
            validate_lease(&lease(
                value,
                "article_backfill:00000000-0000-0000-0000-000000000001:review-1"
            )),
            Err(JobExecution::Failed { .. })
        ));
    }

    #[test]
    fn lease_validation_rejects_dedupe_mismatch() {
        let payload = ArticleBackfillPayload::from_reference(source_id(), &reference()).unwrap();
        let value = serde_json::to_value(payload).unwrap();
        assert!(matches!(
            validate_lease(&lease(value, "wrong-key")),
            Err(JobExecution::Failed { .. })
        ));
    }

    #[test]
    fn retryable_acquisition_is_scheduled() {
        let retry = classify_acquisition_failure(
            &SyncAcquisitionError::ArticlePage(ArticlePageError::OperationTimedOut),
            Utc.timestamp_opt(10, 0).single().unwrap(),
            Duration::seconds(5),
        );
        assert_eq!(
            retry,
            JobExecution::Retry {
                retry_at: Utc.timestamp_opt(15, 0).single().unwrap(),
                error: "article backfill acquisition failed temporarily".to_owned(),
            }
        );
    }

    #[test]
    fn permanent_acquisition_fails() {
        let failed = classify_acquisition_failure(
            &SyncAcquisitionError::WeRead(WeReadAdapterError::InvalidArticleUrl),
            Utc.timestamp_opt(10, 0).single().unwrap(),
            Duration::seconds(5),
        );
        assert_eq!(
            failed,
            JobExecution::Failed {
                error: "WeRead article identity was invalid".to_owned(),
            }
        );
    }

    #[test]
    fn missing_account_is_retried_until_panel_credentials_are_available() {
        assert_eq!(
            classify_acquisition_failure(
                &SyncAcquisitionError::NoAccountEnrolled,
                Utc.timestamp_opt(10, 0).single().unwrap(),
                Duration::seconds(5),
            ),
            JobExecution::Retry {
                retry_at: Utc.timestamp_opt(15, 0).single().unwrap(),
                error: "article backfill is waiting for a usable WeRead account".to_owned(),
            }
        );
    }

    #[test]
    fn retry_time_overflow_becomes_a_terminal_safe_failure() {
        assert_eq!(
            retry_execution(DateTime::<Utc>::MAX_UTC, Duration::seconds(1), "temporary"),
            JobExecution::Failed {
                error: "temporary; retry time is outside the supported range".to_owned(),
            }
        );
    }
}
