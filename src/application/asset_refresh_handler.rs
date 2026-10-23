//! Anonymous whole-article refresh worker.
//!
//! Refresh jobs carry only opaque article and chain identifiers. Public page
//! acquisition and article persistence are added here without authenticated
//! source acquisition or credential-bearing payload fields.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    acquisition::{
        article_page::{ArticlePageError, ArticlePageFetcher},
        browser_pool::BrowserPool,
        weread::WeReadArticleReference,
    },
    application::{
        archive_service::sha256_hex,
        asset_archive_service::AssetArchiveService,
        source_service::SourceReader,
        sync_service::SyncService,
        worker::{JobExecution, JobHandler},
    },
    archive::{
        asset_store::{AssetInput, AssetRefreshPolicy},
        url_rewriter::rewrite_sanitized_html,
    },
    domain::{
        article::ArticleObservationVersion,
        job::{JobType, NewJob},
    },
    persistence::{
        repositories::{
            article_repository::{ArticleRepository, ArticleTransactionRepository},
            asset_repository::{
                ArticleRefreshOutcome, AssetRepositoryError, AssetTransactionRepository,
            },
            job_repository::{JobEnqueueTransaction, JobLease},
            source_repository::{SourceRepositoryError, SourceTransactionRepository},
        },
        unit_of_work::UnitOfWorkFactory,
    },
};
use serde_json::json;

fn complete_asset_acquisition(
    external_assets: &[url::Url],
    fetched_assets: &[AssetInput],
    max_count: u32,
) -> bool {
    external_assets.len() <= max_count as usize
        && crate::application::sync_service::complete_asset_set(external_assets, fetched_assets)
}

/// Secret-free parameters stored in an article-refresh job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawAssetRefreshPayload", deny_unknown_fields)]
pub struct AssetRefreshPayload {
    /// Source owning the article.
    pub source_id: Uuid,
    /// Stable article identity.
    pub review_id: String,
    /// Expired asset that triggered the refresh generation.
    pub expired_asset_record_id: Uuid,
    /// Serial chain identity.
    pub chain_id: Uuid,
    /// Immutable generation served by this chain.
    pub generation: i64,
    /// Article ordinal within the generation snapshot.
    pub ordinal: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAssetRefreshPayload {
    source_id: Uuid,
    review_id: String,
    expired_asset_record_id: Uuid,
    chain_id: Uuid,
    generation: i64,
    ordinal: i64,
}

impl TryFrom<RawAssetRefreshPayload> for AssetRefreshPayload {
    type Error = &'static str;

    fn try_from(raw: RawAssetRefreshPayload) -> Result<Self, Self::Error> {
        if raw.source_id.is_nil()
            || raw.expired_asset_record_id.is_nil()
            || raw.chain_id.is_nil()
            || raw.review_id.trim().is_empty()
            || raw.generation <= 0
            || raw.ordinal <= 0
        {
            return Err("invalid asset refresh job payload");
        }
        Ok(Self {
            source_id: raw.source_id,
            review_id: raw.review_id.trim().to_owned(),
            expired_asset_record_id: raw.expired_asset_record_id,
            chain_id: raw.chain_id,
            generation: raw.generation,
            ordinal: raw.ordinal,
        })
    }
}

/// Dependencies for an anonymous whole-article refresh worker.
pub struct AssetRefreshJobHandlerDependencies<S, A, P> {
    /// Source lookup used to validate the refresh target.
    pub sources: S,
    /// Article lookup and observation-version allocator.
    pub articles: A,
    /// Clean public browser-session pool.
    pub browser_pool: BrowserPool,
    /// Public page fetcher with no authenticated fallback.
    pub page_fetcher: P,
    /// Bounded anonymous asset fetcher.
    pub asset_archiver: AssetArchiveService,
    /// Shared PostgreSQL unit-of-work factory.
    pub unit_of_work: UnitOfWorkFactory,
    /// Article sanitization and normalization service.
    pub sync_service: SyncService,
    /// Bounded article refresh policy.
    pub refresh_policy: AssetRefreshPolicy,
    /// Delay before retrying persistence failures.
    pub retry_after: Duration,
}

/// Executes one anonymous article refresh job.
pub struct AssetRefreshJobHandler<S, A, P> {
    dependencies: AssetRefreshJobHandlerDependencies<S, A, P>,
}

impl<S, A, P> AssetRefreshJobHandler<S, A, P> {
    /// Creates a refresh handler with public-only acquisition dependencies.
    pub fn new(dependencies: AssetRefreshJobHandlerDependencies<S, A, P>) -> Self {
        Self { dependencies }
    }
}

#[async_trait::async_trait]
impl<S, A, P> JobHandler for AssetRefreshJobHandler<S, A, P>
where
    S: SourceReader,
    A: ArticleRepository,
    P: ArticlePageFetcher,
{
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        if lease.job.job_type() != JobType::AssetRefresh {
            return JobExecution::Failed {
                error: "asset refresh handler received an unsupported job type".to_owned(),
            };
        }
        let payload_valid =
            serde_json::from_value::<AssetRefreshPayload>(lease.job.payload().clone()).is_ok();
        let mut validation_uow = match self
            .dependencies
            .unit_of_work
            .begin_refresh_with_assets(&[])
            .await
        {
            Ok(uow) => uow,
            Err(_) => return retry_execution(now, self.dependencies.retry_after),
        };
        let target = {
            let mut assets = validation_uow.assets(self.dependencies.asset_archiver.policy());
            assets.begin_article_refresh(lease).await
        };
        let target = match target {
            Ok(Some(target)) if payload_valid => {
                if validation_uow.commit().await.is_err() {
                    return retry_execution(now, self.dependencies.retry_after);
                }
                target
            }
            Ok(Some(_)) | Ok(None) | Err(AssetRepositoryError::InvalidMetadata) => {
                let result = {
                    let mut assets =
                        validation_uow.assets(self.dependencies.asset_archiver.policy());
                    assets.complete_unresolvable_article_refresh(lease).await
                };
                if result.is_err() || validation_uow.commit().await.is_err() {
                    return retry_execution(now, self.dependencies.retry_after);
                }
                return JobExecution::Committed;
            }
            Err(_) => return retry_execution(now, self.dependencies.retry_after),
        };
        let source_id = target.source_id;
        let version = match self
            .dependencies
            .articles
            .allocate_observation_version()
            .await
        {
            Ok(version) => version,
            Err(_) => return retry_execution(now, self.dependencies.retry_after),
        };
        let article = match self
            .dependencies
            .articles
            .find(source_id, &target.review_id)
            .await
        {
            Ok(article) => article,
            Err(_) => return retry_execution(now, self.dependencies.retry_after),
        };
        let source = match self.dependencies.sources.find(source_id).await {
            Ok(source) => source,
            Err(_) => return retry_execution(now, self.dependencies.retry_after),
        };
        let (Some(article), Some(_source)) = (article, source) else {
            return self
                .finalize(
                    lease,
                    Some(version),
                    None,
                    ArticleRefreshOutcome::Deleted,
                    now,
                )
                .await;
        };
        let Some(page_url) = article.original_url().cloned() else {
            return self
                .finalize(
                    lease,
                    Some(version),
                    None,
                    ArticleRefreshOutcome::Inconclusive,
                    now,
                )
                .await;
        };
        let reference = WeReadArticleReference {
            review_id: article.review_id().to_owned(),
            article_url: Some(page_url.clone()),
            title: Some(article.title().to_owned()),
            summary: article.summary().map(str::to_owned),
            author: article.author().map(str::to_owned),
            cover_url: article.cover_url().map(str::to_owned),
            published_at: Some(article.published_at()),
        };
        let page = match self.dependencies.browser_pool.open_public().await {
            Ok(session) => {
                self.dependencies
                    .page_fetcher
                    .fetch(page_url.clone(), session)
                    .await
            }
            Err(_) => Err(ArticlePageError::OperationTimedOut),
        };
        let prepared = match page {
            Ok(page) if !page.content_html.trim().is_empty() => {
                let fetched_at = match self.dependencies.unit_of_work.database_now().await {
                    Ok(now) => now,
                    Err(_) => return retry_execution(now, self.dependencies.retry_after),
                };
                self.dependencies
                    .sync_service
                    .prepare_article(source_id, &reference, page, version, fetched_at)
                    .ok()
            }
            Ok(_) | Err(_) => None,
        };
        let prepared = if let Some(prepared) = prepared {
            let max_count = self
                .dependencies
                .asset_archiver
                .policy()
                .max_count_per_article();
            if prepared.external_assets().len() > max_count as usize {
                None
            } else {
                let fetched = self
                    .dependencies
                    .asset_archiver
                    .fetch_assets(&page_url, prepared.external_assets())
                    .await;
                complete_asset_acquisition(prepared.external_assets(), &fetched, max_count)
                    .then(|| prepared.with_fetched_assets(fetched))
            }
        } else {
            None
        };
        let outcome = if prepared.is_some() {
            ArticleRefreshOutcome::Succeeded
        } else {
            ArticleRefreshOutcome::Inconclusive
        };
        self.finalize(lease, Some(version), prepared, outcome, now)
            .await
    }
}

impl<S, A, P> AssetRefreshJobHandler<S, A, P>
where
    S: SourceReader,
    A: ArticleRepository,
    P: ArticlePageFetcher,
{
    async fn finalize(
        &self,
        lease: &JobLease,
        reserved_version: Option<ArticleObservationVersion>,
        prepared: Option<crate::application::sync_service::PreparedArticle>,
        requested_outcome: ArticleRefreshOutcome,
        worker_now: DateTime<Utc>,
    ) -> JobExecution {
        let inputs = if requested_outcome == ArticleRefreshOutcome::Succeeded {
            prepared
                .as_ref()
                .map(|article| article.fetched_assets().to_vec())
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let mut uow = match self
            .dependencies
            .unit_of_work
            .begin_refresh_with_assets(&inputs)
            .await
        {
            Ok(uow) => uow,
            Err(_) => return retry_execution(worker_now, self.dependencies.retry_after),
        };
        let target = {
            let mut assets = uow.assets(self.dependencies.asset_archiver.policy());
            assets.begin_article_refresh(lease).await
        };
        let target = match target {
            Ok(Some(target)) => target,
            Ok(None) | Err(AssetRepositoryError::InvalidMetadata) => {
                let result = {
                    let mut assets = uow.assets(self.dependencies.asset_archiver.policy());
                    assets.complete_unresolvable_article_refresh(lease).await
                };
                if result.is_err() || uow.commit().await.is_err() {
                    return retry_execution(worker_now, self.dependencies.retry_after);
                }
                return JobExecution::Committed;
            }
            Err(_) => return retry_execution(worker_now, self.dependencies.retry_after),
        };
        let source = {
            let mut sources = uow.source();
            sources.find_for_update(target.source_id).await
        };
        let source = match source {
            Ok(source) => Some(source),
            Err(SourceRepositoryError::NotFound { .. }) => None,
            Err(_) => return retry_execution(worker_now, self.dependencies.retry_after),
        };
        let current = {
            let mut articles = uow.articles();
            articles
                .find_for_update(target.source_id, &target.review_id)
                .await
        };
        let current = match current {
            Ok(current) => current,
            Err(_) => return retry_execution(worker_now, self.dependencies.retry_after),
        };
        let mut outcome = if source.is_none() || current.is_none() {
            ArticleRefreshOutcome::Deleted
        } else if reserved_version.is_some_and(|version| {
            current
                .as_ref()
                .is_some_and(|article| article.observation_version() >= version)
        }) {
            ArticleRefreshOutcome::Superseded
        } else {
            requested_outcome
        };
        let mut feed_changed = false;
        if outcome == ArticleRefreshOutcome::Succeeded {
            let Some(prepared) = prepared else {
                return retry_execution(worker_now, self.dependencies.retry_after);
            };
            let mut article = prepared.article().clone();
            let stored = {
                let mut assets = uow.assets(self.dependencies.asset_archiver.policy());
                assets
                    .replace_for_article(target.source_id, &target.review_id, &inputs)
                    .await
            };
            let stored = match stored {
                Ok(stored) => Some(stored),
                Err(
                    AssetRepositoryError::CapacityExceeded { .. }
                    | AssetRepositoryError::AssetTooLarge { .. }
                    | AssetRepositoryError::TooManyAssets { .. },
                ) => {
                    outcome = ArticleRefreshOutcome::Inconclusive;
                    None
                }
                Err(_) => return retry_execution(worker_now, self.dependencies.retry_after),
            };
            if let Some(stored) = stored {
                let replacements = stored
                    .iter()
                    .map(|asset| {
                        (
                            asset.source_url().clone(),
                            format!("/assets/{}", asset.id()),
                        )
                    })
                    .collect::<Vec<_>>();
                let rewritten = rewrite_sanitized_html(&article.content_html, &replacements);
                if rewritten != article.content_html {
                    article.content_hash = Some(sha256_hex(rewritten.as_bytes()));
                    article.content_html = rewritten;
                }
                let upsert = {
                    let mut articles = uow.articles();
                    articles.upsert(article).await
                };
                feed_changed = match upsert {
                    Ok(upsert) => upsert.feed_visible_change(),
                    Err(_) => return retry_execution(worker_now, self.dependencies.retry_after),
                };
            }
        }
        if feed_changed {
            let source = source.expect("a live article refresh has a source row");
            let revision = {
                let mut sources = uow.source();
                sources
                    .bump_feed_revision(target.source_id, source.feed_revision())
                    .await
            };
            if revision.is_err() {
                return retry_execution(worker_now, self.dependencies.retry_after);
            }
            let now = match self.dependencies.unit_of_work.database_now().await {
                Ok(now) => now,
                Err(_) => return retry_execution(worker_now, self.dependencies.retry_after),
            };
            let mut queue = uow.job_enqueue();
            if queue
                .enqueue_job(NewJob {
                    job_type: JobType::FeedRebuild,
                    source_id: Some(target.source_id.as_uuid()),
                    priority: source.priority(),
                    run_after: now,
                    max_attempts: source.max_attempts(),
                    payload: json!({"source_id": target.source_id.to_string()}),
                    dedupe_key: format!("feed_rebuild:{}", target.source_id),
                    now,
                })
                .await
                .is_err()
            {
                return retry_execution(worker_now, self.dependencies.retry_after);
            }
        }
        let result = {
            let mut assets = uow.assets(self.dependencies.asset_archiver.policy());
            assets
                .finish_article_refresh(lease, outcome, self.dependencies.refresh_policy)
                .await
        };
        if result.is_err() {
            return retry_execution(worker_now, self.dependencies.retry_after);
        }
        match uow.commit().await {
            Ok(()) => JobExecution::Committed,
            Err(_) => retry_execution(worker_now, self.dependencies.retry_after),
        }
    }
}

fn retry_execution(now: DateTime<Utc>, retry_after: Duration) -> JobExecution {
    JobExecution::Retry {
        retry_at: now + retry_after,
        error: "asset refresh persistence is temporarily unavailable".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::AssetRefreshPayload;
    use serde_json::json;
    use uuid::Uuid;

    fn valid_payload() -> serde_json::Value {
        json!({
            "source_id": Uuid::new_v4(),
            "review_id": "review-1",
            "expired_asset_record_id": Uuid::new_v4(),
            "chain_id": Uuid::new_v4(),
            "generation": 1,
            "ordinal": 1,
        })
    }

    #[test]
    fn accepts_valid_refresh_payload_with_opaque_ids_only() {
        let value = valid_payload();
        let payload: AssetRefreshPayload =
            serde_json::from_value(value.clone()).expect("payload should decode");

        assert_eq!(payload.source_id.to_string(), value["source_id"]);
        assert_eq!(payload.review_id, "review-1");
        assert_eq!(payload.generation, 1);
        assert_eq!(payload.ordinal, 1);
    }

    #[test]
    fn rejects_refresh_payloads_with_missing_or_secret_fields() {
        let mut missing_chain = valid_payload();
        missing_chain.as_object_mut().unwrap().remove("chain_id");
        assert!(serde_json::from_value::<AssetRefreshPayload>(missing_chain).is_err());

        let mut extra_secret = valid_payload();
        extra_secret["signed_url"] = json!("https://cdn.example.test/image?signature=secret");
        assert!(serde_json::from_value::<AssetRefreshPayload>(extra_secret).is_err());
    }

    #[test]
    fn signed_images_cannot_make_refresh_look_complete_after_sanitization() {
        let sanitized = crate::archive::sanitizer::sanitize(
            r#"<p>body</p><img src="https://cdn.example/loading.gif" data-src="https://cdn.example/image?X-Amz-Signature=secret">"#,
        );
        assert_eq!(sanitized.html(), "<p>body</p>");
        assert!(!super::complete_asset_acquisition(
            sanitized.external_assets(),
            &[],
            10
        ));
    }

    #[test]
    fn refuses_to_persist_provider_signed_asset_urls() {
        let source_url = url::Url::parse(
            "https://cdn.example/1.png?X-Amz-Signature=secret&X-Amz-Credential=key",
        )
        .unwrap();
        let final_url = url::Url::parse(
            "https://cdn.example/1.png?GoogleAccessId=key&Expires=123&Signature=secret",
        )
        .unwrap();
        let referer = url::Url::parse("https://mp.weixin.qq.com/s/page").unwrap();
        let fetched = [crate::archive::asset_store::AssetInput::new(
            source_url.clone(),
            final_url,
            "image/png".to_owned(),
            vec![1],
            0,
            referer,
            None,
            None,
        )];

        assert!(!super::complete_asset_acquisition(
            &[source_url],
            &fetched,
            10
        ));
    }

    #[test]
    fn only_complete_bounded_asset_acquisition_can_replace_an_article() {
        let first = url::Url::parse("https://cdn.example/1.png").unwrap();
        let second = url::Url::parse("https://cdn.example/2.png").unwrap();
        let referer = url::Url::parse("https://mp.weixin.qq.com/s/page").unwrap();
        let fetched = vec![
            crate::archive::asset_store::AssetInput::new(
                first.clone(),
                first.clone(),
                "image/png".to_owned(),
                vec![1],
                0,
                referer.clone(),
                None,
                None,
            ),
            crate::archive::asset_store::AssetInput::new(
                second.clone(),
                second.clone(),
                "image/png".to_owned(),
                vec![2],
                1,
                referer,
                None,
                None,
            ),
        ];
        assert!(super::complete_asset_acquisition(&[], &[], 10));
        assert!(super::complete_asset_acquisition(
            &[first.clone(), second.clone()],
            &fetched,
            10
        ));
        assert!(!super::complete_asset_acquisition(
            &[first, second],
            &fetched[..1],
            10
        ));
        assert!(!super::complete_asset_acquisition(
            &fetched
                .iter()
                .map(|asset| asset.source_url.clone())
                .collect::<Vec<_>>(),
            &fetched,
            1,
        ));
        let wrong_url = url::Url::parse("https://cdn.example/different.png").unwrap();
        assert!(!super::complete_asset_acquisition(
            &[fetched[0].source_url.clone(), wrong_url],
            &fetched,
            10,
        ));
        let mut wrong_occurrence = fetched.clone();
        wrong_occurrence[1].occurrence = 0;
        assert!(!super::complete_asset_acquisition(
            &[fetched[0].source_url.clone(), fetched[1].source_url.clone()],
            &wrong_occurrence,
            10,
        ));
    }

    #[test]
    fn rejects_invalid_refresh_identity_and_chain_position() {
        for field in ["source_id", "expired_asset_record_id", "chain_id"] {
            let mut value = valid_payload();
            value[field] = json!(Uuid::nil());
            assert!(
                serde_json::from_value::<AssetRefreshPayload>(value).is_err(),
                "nil {field} should be rejected"
            );
        }

        for (field, invalid) in [
            ("review_id", json!("  ")),
            ("generation", json!(0)),
            ("generation", json!(-1)),
            ("ordinal", json!(0)),
            ("ordinal", json!(-1)),
        ] {
            let mut value = valid_payload();
            value[field] = invalid;
            assert!(serde_json::from_value::<AssetRefreshPayload>(value).is_err());
        }
    }
}
