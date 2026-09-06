//! Source synchronization orchestration.
//!
//! This service executes a claimed source-sync job. It acquires the distributed
//! WeRead account lease for authenticated article-list and URL-recovery work,
//! then releases that account session before first fetching public article
//! pages in clean, ephemeral browser sessions without credentials. A failed
//! public fetch may reacquire the source account for WeRead's content fallback.
//!
//! Browser acquisition, waits, and normalization happen outside database
//! transactions while an independent heartbeat task maintains the job and, when
//! needed, account leases. After acquisition, the service renders a candidate
//! feed outside the transaction by merging normalized changes with current RSS
//! input. A short persistence `UnitOfWork` verifies the job fencing token and
//! expected base revision, upserts records, advances the source feed revision,
//! stores the matching candidate, records the sync result and next schedule,
//! and completes the job atomically. Revision conflict discards the candidate
//! and retries from a fresh snapshot.
//!
//! Authentication expiry permits exactly one refresh and one retry. Risk
//! control and verification states stop the workflow and update source status.
//! All writes must be idempotent so expired leases and worker crashes are safe.
//!
//! The service checks quiet hours before beginning upstream work and between
//! each request/page operation. It delegates all waits and scroll decisions to
//! the acquisition pacing policy. If quiet hours begin mid-job, the current
//! bounded operation may finish, then the job exits with a non-failure
//! `deferred` outcome whose `run_after` is the next allowed instant.
//! Credentials are scoped to the authenticated WeRead adapter; the public
//! article page adapter must not receive or require them.
//!
//! The executable preparation slice is consumed by
//! [`super::source_sync_handler::SourceSyncJobHandler`]: list metadata is used
//! as a fallback for missing page metadata, public page HTML is sanitized and
//! hashed through [`ArchiveService`], and the optional external-image set is
//! retained for the database asset archiver. This service also classifies typed
//! WeRead and public-page failures into the stable sync-run vocabulary without
//! copying upstream response text into durable diagnostics. Transport and
//! concrete browser/account composition remain behind the acquisition port.
//!
//! Non-responsibilities: polling due sources, implementing WebDriver commands,
//! storing raw secrets, or serving RSS requests. The source-sync handler
//! performs acquisition outside a transaction and passes prepared values
//! through one shared `UnitOfWork` for article, source, sync-run, and job
//! writes; feed-cache publication remains the separate feed-rebuild job.

use chrono::{DateTime, Utc};
use thiserror::Error;
use url::Url;

use crate::{
    acquisition::{
        article_page::{ArticlePageError, ExtractedArticlePage},
        weread::{WeReadAdapterError, WeReadArticleReference},
    },
    application::archive_service::ArchiveService,
    archive::asset_store::AssetInput,
    domain::{
        article::{ArticleError, ArticleObservationVersion, NewArticle},
        source::SourceId,
        sync::{SyncFailure, SyncFailureClass, SyncOutcome},
    },
};

/// An acquisition failure that can be classified without exposing raw
/// protocol or browser details to the synchronization domain.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SyncAcquisitionError {
    /// No enabled account was available for the source-sync request.
    #[error("no usable WeRead account is enrolled")]
    NoAccountEnrolled,
    /// An authenticated WeRead list or account operation failed.
    #[error(transparent)]
    WeRead(#[from] WeReadAdapterError),
    /// An unauthenticated public article-page operation failed.
    #[error(transparent)]
    ArticlePage(#[from] ArticlePageError),
}

/// The durable sync-run classification selected for an acquisition failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedSyncFailure {
    outcome: SyncOutcome,
    failure: SyncFailure,
    log_as_error: bool,
}

impl ClassifiedSyncFailure {
    /// Returns the sync-run outcome to persist.
    pub const fn outcome(&self) -> SyncOutcome {
        self.outcome
    }

    /// Returns the safe failure summary to persist.
    pub fn failure(&self) -> &SyncFailure {
        &self.failure
    }

    /// Returns whether this failure should be emitted at error level.
    ///
    /// A source-sync run can finish with `Failed` while the application is
    /// still healthy and should simply try again later. The missing-account
    /// state is one such expected first-run condition.
    pub(crate) const fn log_as_error(&self) -> bool {
        self.log_as_error
    }

    /// Creates an application-owned permanent failure with a safe message.
    pub(crate) fn permanent(message: &'static str) -> Self {
        Self {
            outcome: SyncOutcome::Failed,
            failure: SyncFailure::new(SyncFailureClass::Permanent, message)
                .expect("static synchronization failure messages must be valid"),
            log_as_error: true,
        }
    }

    /// Creates an application-owned retryable failure with a safe message.
    pub(crate) fn retryable(message: &'static str) -> Self {
        Self {
            outcome: SyncOutcome::RetryableFailure,
            failure: SyncFailure::new(SyncFailureClass::Retryable, message)
                .expect("static synchronization failure messages must be valid"),
            log_as_error: false,
        }
    }
}

/// Converts an acquisition boundary error to a secret-free sync-run result.
pub fn classify_acquisition_error(error: &SyncAcquisitionError) -> ClassifiedSyncFailure {
    let (outcome, class, message) = match error {
        SyncAcquisitionError::NoAccountEnrolled => (
            SyncOutcome::Failed,
            SyncFailureClass::Permanent,
            "no usable WeRead account is enrolled",
        ),
        SyncAcquisitionError::WeRead(WeReadAdapterError::AuthenticationExpired { .. }) => (
            SyncOutcome::AuthenticationRequired,
            SyncFailureClass::AuthenticationExpired,
            "WeRead authentication expired",
        ),
        SyncAcquisitionError::WeRead(WeReadAdapterError::RiskControlled { .. }) => (
            SyncOutcome::RiskControlled,
            SyncFailureClass::RiskControlled,
            "WeRead request was risk-controlled",
        ),
        SyncAcquisitionError::WeRead(WeReadAdapterError::VerificationRequired) => (
            SyncOutcome::Blocked,
            SyncFailureClass::Blocked,
            "authenticated WeRead article acquisition was blocked",
        ),
        SyncAcquisitionError::WeRead(WeReadAdapterError::InvalidArticleUrl)
        | SyncAcquisitionError::WeRead(WeReadAdapterError::InvalidReviewId) => (
            SyncOutcome::Failed,
            SyncFailureClass::Permanent,
            "WeRead article identity was invalid",
        ),
        SyncAcquisitionError::WeRead(WeReadAdapterError::CredentialProviderNotConfigured) => (
            SyncOutcome::Failed,
            SyncFailureClass::Permanent,
            "WeRead authentication configuration is incomplete",
        ),
        SyncAcquisitionError::ArticlePage(ArticlePageError::UnsafeRedirect)
        | SyncAcquisitionError::ArticlePage(ArticlePageError::VerificationRequired) => (
            SyncOutcome::Blocked,
            SyncFailureClass::Blocked,
            "public article acquisition was blocked",
        ),
        SyncAcquisitionError::WeRead(WeReadAdapterError::LeaseLost { .. })
        | SyncAcquisitionError::WeRead(WeReadAdapterError::LeaseBackend(_))
        | SyncAcquisitionError::WeRead(WeReadAdapterError::Protocol(_))
        | SyncAcquisitionError::WeRead(WeReadAdapterError::Browser(_))
        | SyncAcquisitionError::ArticlePage(ArticlePageError::Browser(_))
        | SyncAcquisitionError::ArticlePage(ArticlePageError::OperationTimedOut) => (
            SyncOutcome::RetryableFailure,
            SyncFailureClass::Retryable,
            "upstream acquisition failed temporarily",
        ),
        SyncAcquisitionError::WeRead(WeReadAdapterError::UnexpectedResponse(_)) => (
            SyncOutcome::RetryableFailure,
            SyncFailureClass::Retryable,
            "WeRead returned an unexpected response",
        ),
        SyncAcquisitionError::ArticlePage(ArticlePageError::InvalidExtraction(_)) => (
            SyncOutcome::Failed,
            SyncFailureClass::Permanent,
            "public article content could not be extracted",
        ),
    };

    ClassifiedSyncFailure {
        outcome,
        failure: SyncFailure::new(class, message)
            .expect("static synchronization failure messages must be valid"),
        log_as_error: outcome == SyncOutcome::Failed
            && !matches!(error, SyncAcquisitionError::NoAccountEnrolled),
    }
}

/// One article prepared for the final synchronization transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedArticle {
    article: NewArticle,
    external_assets: Vec<Url>,
    fetched_assets: Vec<AssetInput>,
}

impl PreparedArticle {
    /// Returns the normalized article input for the article repository.
    pub const fn article(&self) -> &NewArticle {
        &self.article
    }

    /// Returns approved external assets for optional binary archiving.
    pub fn external_assets(&self) -> &[Url] {
        &self.external_assets
    }

    /// Returns successfully fetched asset bodies ready for the persistence
    /// transaction. Failed downloads are intentionally absent.
    pub fn fetched_assets(&self) -> &[AssetInput] {
        &self.fetched_assets
    }

    /// Adds the best-effort asset responses collected outside the database
    /// transaction.
    pub fn with_fetched_assets(mut self, fetched_assets: Vec<AssetInput>) -> Self {
        self.fetched_assets = fetched_assets;
        self
    }
}

/// Source synchronization preparation and failure-classification service.
#[derive(Debug, Clone, Copy, Default)]
pub struct SyncService {
    archive: ArchiveService,
}

/// Returns whether a successful full-content observation may reconcile its
/// article-to-asset relationships.
///
/// Empty content is a partial observation and must preserve existing asset
/// links. A lower observation version was superseded by a newer observation,
/// so applying its links would reintroduce stale data.
pub(crate) fn should_reconcile_assets(
    incoming: &NewArticle,
    persisted_version: ArticleObservationVersion,
) -> bool {
    !incoming.content_html.is_empty() && incoming.observation_version == persisted_version
}

/// Returns whether an incomplete asset observation must keep the previously
/// archived article representation.
///
/// Asset acquisition is best effort. If the stored article already points at
/// local asset routes, replacing its HTML with a partially archived response
/// would make the existing `article_assets` rows disagree with the article.
/// Keeping the old representation until a complete observation succeeds keeps
/// those two pieces of state atomic from the feed's point of view.
pub(crate) fn should_preserve_cached_asset_representation(
    current_content_html: Option<&str>,
    external_asset_count: usize,
    fetched_asset_count: usize,
) -> bool {
    external_asset_count > fetched_asset_count
        && current_content_html.is_some_and(|html| html.contains("/assets/"))
}

impl SyncService {
    /// Creates a synchronization service with the default archive policy.
    pub const fn new() -> Self {
        Self {
            archive: ArchiveService::new(),
        }
    }

    /// Merges one WeRead reference with its credential-free public page result.
    ///
    /// Page metadata wins when it is non-empty; list metadata remains a
    /// fallback for partial extraction. The page's verified canonical URL is
    /// always used as the persisted original URL. HTML is sanitized before it
    /// is placed in [`NewArticle`], and observation timestamps are supplied by
    /// the caller so persistence can allocate the monotonic version before
    /// network work begins.
    pub fn prepare_article(
        &self,
        source_id: SourceId,
        reference: &WeReadArticleReference,
        page: ExtractedArticlePage,
        observation_version: ArticleObservationVersion,
        fetched_at: DateTime<Utc>,
    ) -> Result<PreparedArticle, SyncServiceError> {
        tracing::debug!(
            source_id = %source_id,
            review_id = %reference.review_id,
            observation_version = observation_version.as_u64(),
            "preparing synchronized article"
        );
        let archived = self.archive.archive(&page.content_html);
        let result = (|| {
            let article = NewArticle {
                source_id,
                review_id: reference.review_id.clone(),
                title: preferred_text(page.title, reference.title.clone()),
                author: preferred_text_option(page.author, reference.author.clone()),
                summary: preferred_text_option(page.summary, reference.summary.clone()),
                cover_url: preferred_text_option(page.cover_url, reference.cover_url.clone()),
                original_url: Some(page.canonical_url),
                published_at: page
                    .published_at
                    .or(reference.published_at)
                    .ok_or(SyncServiceError::MissingPublishedAt)?,
                content_html: archived.html().to_owned(),
                content_hash: archived.content_hash().map(str::to_owned),
                observation_version,
                fetched_at,
            }
            .normalize()?;

            Ok(PreparedArticle {
                article,
                external_assets: archived.external_assets().to_vec(),
                fetched_assets: Vec::new(),
            })
        })();
        match &result {
            Ok(prepared) => tracing::debug!(
                source_id = %source_id,
                review_id = %reference.review_id,
                content_bytes = prepared.article.content_html.len(),
                external_assets = prepared.external_assets.len(),
                "synchronized article prepared"
            ),
            Err(error) => tracing::warn!(
                source_id = %source_id,
                review_id = %reference.review_id,
                error = %error,
                "unable to prepare synchronized article"
            ),
        }
        result
    }
}

/// Errors while preparing acquired data for persistence.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SyncServiceError {
    /// The normalized page/list data violated article invariants.
    #[error(transparent)]
    Article(#[from] ArticleError),
    /// Neither the page nor the list response supplied a publication time.
    #[error("article publication time is missing from page and list metadata")]
    MissingPublishedAt,
}

fn preferred_text(primary: String, fallback: Option<String>) -> String {
    preferred_text_option(Some(primary), fallback).unwrap_or_default()
}

fn preferred_text_option(primary: Option<String>, fallback: Option<String>) -> Option<String> {
    primary
        .filter(|value| !value.trim().is_empty())
        .or_else(|| fallback.filter(|value| !value.trim().is_empty()))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use uuid::Uuid;

    use super::*;
    use crate::domain::source::VerifiedWechatArticleUrl;

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    fn reference() -> WeReadArticleReference {
        WeReadArticleReference {
            review_id: "review-1".to_owned(),
            article_url: Some(
                "https://mp.weixin.qq.com/s/list-url"
                    .parse::<VerifiedWechatArticleUrl>()
                    .unwrap(),
            ),
            title: Some("List title".to_owned()),
            summary: Some("List summary".to_owned()),
            author: Some("List author".to_owned()),
            cover_url: Some("https://cdn.example/list.jpg".to_owned()),
            published_at: None,
        }
    }

    fn page() -> ExtractedArticlePage {
        ExtractedArticlePage {
            canonical_url: "https://mp.weixin.qq.com/s/page-url".parse().unwrap(),
            title: " Page title ".to_owned(),
            author: None,
            summary: Some(" Page summary ".to_owned()),
            published_at: Some(Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()),
            content_html: "<p>body</p><script>bad()</script>".to_owned(),
            cover_url: None,
        }
    }

    #[test]
    fn prepares_sanitized_article_and_preserves_optional_assets() {
        let prepared = SyncService::new()
            .prepare_article(
                source_id(),
                &reference(),
                page(),
                ArticleObservationVersion::from_u64(1),
                Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
            )
            .unwrap();

        assert_eq!(prepared.article().title, "Page title");
        assert_eq!(prepared.article().author.as_deref(), Some("List author"));
        assert_eq!(prepared.article().summary.as_deref(), Some("Page summary"));
        assert_eq!(
            prepared.article().original_url.as_ref().unwrap().as_str(),
            "https://mp.weixin.qq.com/s/page-url"
        );
        assert_eq!(prepared.article().content_html, "<p>body</p>");
        assert!(prepared.article().content_hash.is_some());
        assert!(prepared.external_assets().is_empty());
    }

    #[test]
    fn falls_back_from_blank_page_metadata_and_rejects_zero_version() {
        let mut page = page();
        page.title = "  ".to_owned();
        page.content_html = "<p>body</p><img src=\"https://cdn.example/image.jpg\">".to_owned();
        let prepared = SyncService::default()
            .prepare_article(
                source_id(),
                &reference(),
                page.clone(),
                ArticleObservationVersion::from_u64(2),
                Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
            )
            .unwrap();
        assert_eq!(prepared.article().title, "List title");
        assert_eq!(prepared.external_assets().len(), 1);

        assert_eq!(
            SyncService::default().prepare_article(
                source_id(),
                &reference(),
                page,
                ArticleObservationVersion::from_u64(0),
                Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
            ),
            Err(SyncServiceError::Article(
                ArticleError::InvalidObservationVersion
            ))
        );
    }

    #[test]
    fn falls_back_to_list_publication_time_when_page_omits_it() {
        let mut reference = reference();
        reference.published_at = Some(Utc.timestamp_opt(1_600_000_000, 0).single().unwrap());
        let mut page = page();
        page.published_at = None;

        let prepared = SyncService::default()
            .prepare_article(
                source_id(),
                &reference,
                page,
                ArticleObservationVersion::from_u64(1),
                Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
            )
            .unwrap();

        assert_eq!(
            prepared.article().published_at,
            Utc.timestamp_opt(1_600_000_000, 0).single().unwrap()
        );
    }

    #[test]
    fn prefers_page_publication_time_when_both_values_exist() {
        let mut reference = reference();
        reference.published_at = Some(Utc.timestamp_opt(1_600_000_000, 0).single().unwrap());

        let prepared = SyncService::default()
            .prepare_article(
                source_id(),
                &reference,
                page(),
                ArticleObservationVersion::from_u64(1),
                Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
            )
            .unwrap();

        assert_eq!(
            prepared.article().published_at,
            Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()
        );
    }

    #[test]
    fn rejects_articles_without_any_publication_time() {
        let mut page = page();
        page.published_at = None;

        assert_eq!(
            SyncService::default().prepare_article(
                source_id(),
                &reference(),
                page,
                ArticleObservationVersion::from_u64(1),
                Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
            ),
            Err(SyncServiceError::MissingPublishedAt)
        );
    }

    #[test]
    fn reconciles_assets_only_for_an_accepted_full_observation() {
        let incoming = NewArticle {
            source_id: source_id(),
            review_id: "review-1".to_owned(),
            title: "title".to_owned(),
            author: None,
            summary: None,
            cover_url: None,
            original_url: None,
            published_at: Utc.timestamp_opt(1_700_000_000, 0).single().unwrap(),
            content_html: "<p>body</p>".to_owned(),
            content_hash: Some("hash".to_owned()),
            observation_version: ArticleObservationVersion::from_u64(4),
            fetched_at: Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
        };

        assert!(should_reconcile_assets(
            &incoming,
            ArticleObservationVersion::from_u64(4)
        ));
    }

    #[test]
    fn does_not_reconcile_assets_for_partial_or_stale_observation() {
        let mut incoming = NewArticle {
            source_id: source_id(),
            review_id: "review-1".to_owned(),
            title: "title".to_owned(),
            author: None,
            summary: None,
            cover_url: None,
            original_url: None,
            published_at: Utc.timestamp_opt(1_700_000_000, 0).single().unwrap(),
            content_html: String::new(),
            content_hash: None,
            observation_version: ArticleObservationVersion::from_u64(4),
            fetched_at: Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
        };

        assert!(!should_reconcile_assets(
            &incoming,
            ArticleObservationVersion::from_u64(4)
        ));

        incoming.content_html = "<p>older</p>".to_owned();
        assert!(!should_reconcile_assets(
            &incoming,
            ArticleObservationVersion::from_u64(5)
        ));
    }

    #[test]
    fn preserves_cached_representation_when_asset_acquisition_is_incomplete() {
        assert!(should_preserve_cached_asset_representation(
            Some("<p>cached</p><img src=\"/assets/asset-id\">"),
            2,
            1,
        ));
    }

    #[test]
    fn does_not_preserve_cached_representation_after_complete_asset_acquisition() {
        assert!(!should_preserve_cached_asset_representation(
            Some("<p>cached</p><img src=\"/assets/asset-id\">"),
            1,
            1,
        ));
    }

    #[test]
    fn classifies_failures_without_retaining_upstream_details() {
        let cases = [
            (
                SyncAcquisitionError::NoAccountEnrolled,
                SyncOutcome::Failed,
                SyncFailureClass::Permanent,
                "no usable WeRead account is enrolled",
            ),
            (
                SyncAcquisitionError::WeRead(WeReadAdapterError::AuthenticationExpired {
                    code: -2012,
                }),
                SyncOutcome::AuthenticationRequired,
                SyncFailureClass::AuthenticationExpired,
                "WeRead authentication expired",
            ),
            (
                SyncAcquisitionError::ArticlePage(ArticlePageError::VerificationRequired),
                SyncOutcome::Blocked,
                SyncFailureClass::Blocked,
                "public article acquisition was blocked",
            ),
            (
                SyncAcquisitionError::WeRead(WeReadAdapterError::InvalidReviewId),
                SyncOutcome::Failed,
                SyncFailureClass::Permanent,
                "WeRead article identity was invalid",
            ),
            (
                SyncAcquisitionError::WeRead(WeReadAdapterError::CredentialProviderNotConfigured),
                SyncOutcome::Failed,
                SyncFailureClass::Permanent,
                "WeRead authentication configuration is incomplete",
            ),
            (
                SyncAcquisitionError::ArticlePage(ArticlePageError::Browser(
                    "password=secret".to_owned(),
                )),
                SyncOutcome::RetryableFailure,
                SyncFailureClass::Retryable,
                "upstream acquisition failed temporarily",
            ),
            (
                SyncAcquisitionError::ArticlePage(ArticlePageError::InvalidExtraction(
                    "secret page source".to_owned(),
                )),
                SyncOutcome::Failed,
                SyncFailureClass::Permanent,
                "public article content could not be extracted",
            ),
        ];

        for (error, outcome, class, message) in cases {
            let classified = classify_acquisition_error(&error);
            assert_eq!(classified.outcome(), outcome);
            assert_eq!(classified.failure().class(), class);
            assert_eq!(classified.failure().message(), message);
        }
    }

    #[test]
    fn classifies_authenticated_weread_verification_as_blocked() {
        let classified = classify_acquisition_error(&SyncAcquisitionError::WeRead(
            WeReadAdapterError::VerificationRequired,
        ));

        assert_eq!(classified.outcome(), SyncOutcome::Blocked);
        assert_eq!(classified.failure().class(), SyncFailureClass::Blocked);
        assert_eq!(
            classified.failure().message(),
            "authenticated WeRead article acquisition was blocked"
        );
    }
}
