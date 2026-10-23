//! Article and archived-content domain values.
//!
//! An article is identified by the pair `(source_id, review_id)`. The
//! upstream `review_id` is stable across repeated WeRead list fetches, while
//! the source component prevents two subscribed sources from accidentally
//! sharing mutable article state. URLs may be absent during list acquisition,
//! then be filled in by a later public-page lookup.
//!
//! Responsibilities:
//!
//! - preserve stable article identity and source ownership;
//! - normalize list/detail acquisition into one idempotent upsert input;
//! - distinguish feed-visible fields from observation-only timestamps; and
//! - expose immutable values to repositories and the RSS renderer.
//!
//! Non-responsibilities: browser extraction, HTML sanitization, asset
//! downloading, RSS serialization, SQL, and feed-revision mutation. The
//! `content_html` field must already be sanitized by `ArchiveService`; an
//! empty value is allowed while a list item is waiting for public article-page
//! fetching.
//!
//! Feed-cache interaction: a repository upsert reports whether any
//! feed-visible field changed. The application should bump the owning source's
//! `feed_revision` once for a changed batch in the same `UnitOfWork`; repeating
//! an identical article fetch must not create a new feed revision. The
//! A newer observation version may still refresh a feed-invisible no-op; an
//! older observation must be ignored so an out-of-order replica cannot regress
//! the stored article. The version is allocated before upstream work, while
//! `fetched_at` records completion time only.
//! Optional metadata and empty content in a new observation mean that the
//! acquisition path did not provide that field; repository upserts preserve a
//! previously known value instead of interpreting absence as deletion.
//!
//! Asset policy: the default version-one mode stores no binary asset rows.
//! When the database asset backend is enabled, the application persists
//! checksum-deduplicated binary rows and article relationships in the same
//! unit of work, then rewrites successfully archived URLs without changing
//! article identity. Local, S3, and GCS backends use the same repository
//! contract.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use super::{
    source::{SourceId, VerifiedWechatArticleUrl},
    url_policy::has_provider_signature,
};

/// Errors raised while normalizing or reconstructing an article.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ArticleError {
    /// An article must belong to a real source.
    #[error("article source id must not be nil")]
    InvalidSourceId,
    /// The upstream stable article identity is required.
    #[error("article review_id must not be empty")]
    EmptyReviewId,
    /// A feed item needs a non-empty title.
    #[error("article title must not be empty")]
    EmptyTitle,
    /// A cover URL, when supplied, must be an absolute HTTP(S) URL.
    #[error("article cover_url must be an absolute http(s) URL")]
    InvalidCoverUrl,
    /// An original URL stored by persistence was not a verified WeChat URL.
    #[error("article original_url is not a verified public WeChat URL")]
    InvalidOriginalUrl,
    /// Observation versions must be positive because zero is reserved as an
    /// invalid/uninitialized value.
    #[error("article observation version must be positive")]
    InvalidObservationVersion,
}

/// Monotonic ordering token allocated before one article observation starts.
///
/// Production callers obtain this value from the PostgreSQL observation
/// sequence before browser work begins. It is separate from `fetched_at`,
/// because a slower request can finish after a newer request and therefore
/// have a later completion timestamp despite carrying older content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArticleObservationVersion(u64);

impl ArticleObservationVersion {
    /// Wraps a trusted observation version.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the monotonic ordering value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Input produced by list/detail acquisition for one article.
///
/// The struct form keeps the interface extensible without a long constructor
/// argument list. Call [`NewArticle::normalize`] before passing it to a
/// repository. `content_html` can be empty until the unauthenticated public
/// article-page fetch completes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewArticle {
    /// Source owning this article.
    pub source_id: SourceId,
    /// Stable upstream article identity from WeRead.
    pub review_id: String,
    /// Human-readable title.
    pub title: String,
    /// Optional author shown in RSS metadata.
    pub author: Option<String>,
    /// Optional plain-text or normalized summary.
    pub summary: Option<String>,
    /// Optional cover image URL. Binary caching is outside the v1 path.
    pub cover_url: Option<String>,
    /// Verified public WeChat URL, when detail URL recovery has completed.
    pub original_url: Option<VerifiedWechatArticleUrl>,
    /// Publication time used for feed ordering.
    pub published_at: DateTime<Utc>,
    /// Sanitized article HTML, or empty while content is not fetched yet.
    pub content_html: String,
    /// Hash of normalized content when content is available.
    pub content_hash: Option<String>,
    /// Monotonic version allocated before this observation starts.
    pub observation_version: ArticleObservationVersion,
    /// Completion time of this list/detail observation.
    pub fetched_at: DateTime<Utc>,
}

impl NewArticle {
    /// Trims textual metadata and validates article-owned invariants.
    pub fn normalize(self) -> Result<Self, ArticleError> {
        if self.source_id.as_uuid().is_nil() {
            return Err(ArticleError::InvalidSourceId);
        }

        let review_id = self.review_id.trim().to_owned();
        if review_id.is_empty() {
            return Err(ArticleError::EmptyReviewId);
        }
        let title = self.title.trim().to_owned();
        if title.is_empty() {
            return Err(ArticleError::EmptyTitle);
        }
        if self.observation_version.as_u64() == 0 {
            return Err(ArticleError::InvalidObservationVersion);
        }

        let cover_url = self
            .cover_url
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        if cover_url.as_deref().is_some_and(|value| {
            Url::parse(value)
                .map(|url| {
                    !matches!(url.scheme(), "http" | "https")
                        || url.host_str().is_none()
                        || has_provider_signature(&url)
                })
                .unwrap_or(true)
        }) {
            return Err(ArticleError::InvalidCoverUrl);
        }

        Ok(Self {
            source_id: self.source_id,
            review_id,
            title,
            author: normalize_optional(self.author),
            summary: normalize_optional(self.summary),
            cover_url,
            original_url: self.original_url,
            published_at: self.published_at,
            content_html: self.content_html,
            content_hash: normalize_optional(self.content_hash),
            observation_version: self.observation_version,
            fetched_at: self.fetched_at,
        })
    }
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Immutable normalized article returned by persistence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Article {
    source_id: SourceId,
    review_id: String,
    title: String,
    author: Option<String>,
    summary: Option<String>,
    cover_url: Option<String>,
    original_url: Option<VerifiedWechatArticleUrl>,
    published_at: DateTime<Utc>,
    content_html: String,
    content_hash: Option<String>,
    observation_version: ArticleObservationVersion,
    fetched_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Trusted persisted fields used by the article repository row decoder.
#[derive(Debug)]
pub(crate) struct ArticleParts {
    pub(crate) source_id: SourceId,
    pub(crate) review_id: String,
    pub(crate) title: String,
    pub(crate) author: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) cover_url: Option<String>,
    pub(crate) original_url: Option<VerifiedWechatArticleUrl>,
    pub(crate) published_at: DateTime<Utc>,
    pub(crate) content_html: String,
    pub(crate) content_hash: Option<String>,
    pub(crate) observation_version: ArticleObservationVersion,
    pub(crate) fetched_at: DateTime<Utc>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

impl Article {
    /// Reconstructs and validates a repository result.
    pub(crate) fn from_parts(parts: ArticleParts) -> Result<Self, ArticleError> {
        let normalized = NewArticle {
            source_id: parts.source_id,
            review_id: parts.review_id,
            title: parts.title,
            author: parts.author,
            summary: parts.summary,
            cover_url: parts.cover_url,
            original_url: parts.original_url,
            published_at: parts.published_at,
            content_html: parts.content_html,
            content_hash: parts.content_hash,
            observation_version: parts.observation_version,
            fetched_at: parts.fetched_at,
        }
        .normalize()?;

        Ok(Self {
            source_id: normalized.source_id,
            review_id: normalized.review_id,
            title: normalized.title,
            author: normalized.author,
            summary: normalized.summary,
            cover_url: normalized.cover_url,
            original_url: normalized.original_url,
            published_at: normalized.published_at,
            content_html: normalized.content_html,
            content_hash: normalized.content_hash,
            observation_version: normalized.observation_version,
            fetched_at: normalized.fetched_at,
            created_at: parts.created_at,
            updated_at: parts.updated_at,
        })
    }

    /// Returns the source owning this article.
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// Returns the stable upstream identity.
    pub fn review_id(&self) -> &str {
        &self.review_id
    }

    /// Returns the normalized title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the optional author.
    pub fn author(&self) -> Option<&str> {
        self.author.as_deref()
    }

    /// Returns the optional summary.
    pub fn summary(&self) -> Option<&str> {
        self.summary.as_deref()
    }

    /// Returns the optional cover URL.
    pub fn cover_url(&self) -> Option<&str> {
        self.cover_url.as_deref()
    }

    /// Returns the verified public article URL, when known.
    pub fn original_url(&self) -> Option<&VerifiedWechatArticleUrl> {
        self.original_url.as_ref()
    }

    /// Returns the publication timestamp.
    pub const fn published_at(&self) -> DateTime<Utc> {
        self.published_at
    }

    /// Returns sanitized article HTML.
    pub fn content_html(&self) -> &str {
        &self.content_html
    }

    /// Returns the normalized content hash, when content is available.
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }

    /// Returns the version allocated before this observation started.
    pub const fn observation_version(&self) -> ArticleObservationVersion {
        self.observation_version
    }

    /// Returns when the list/detail observation was acquired.
    pub const fn fetched_at(&self) -> DateTime<Utc> {
        self.fetched_at
    }

    /// Returns when the row was first inserted.
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when the row was last persisted.
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Merges an incomplete list/detail observation with this stored article.
    ///
    /// Acquisition is intentionally incremental: a list response may contain
    /// a title and publication time without a recovered URL or page HTML.
    /// `None` optional fields and empty HTML therefore preserve existing detail
    /// data. A non-empty replacement HTML without a hash clears the old hash
    /// because it can no longer describe the replacement content. Callers must
    /// compare `observation_version` with the stored observation before
    /// invoking this merge so delayed observations cannot overwrite newer data.
    pub(crate) fn merge_observation(&self, incoming: &NewArticle) -> NewArticle {
        let content_html = if incoming.content_html.is_empty() {
            self.content_html.clone()
        } else {
            incoming.content_html.clone()
        };
        let content_hash = incoming.content_hash.clone().or_else(|| {
            (content_html == self.content_html)
                .then(|| self.content_hash.clone())
                .flatten()
        });

        NewArticle {
            source_id: self.source_id,
            review_id: self.review_id.clone(),
            title: incoming.title.clone(),
            author: incoming.author.clone().or_else(|| self.author.clone()),
            summary: incoming.summary.clone().or_else(|| self.summary.clone()),
            cover_url: incoming
                .cover_url
                .clone()
                .or_else(|| self.cover_url.clone()),
            original_url: incoming
                .original_url
                .clone()
                .or_else(|| self.original_url.clone()),
            published_at: incoming.published_at,
            content_html,
            content_hash,
            observation_version: incoming.observation_version,
            fetched_at: incoming.fetched_at,
        }
    }
}

/// Result of an idempotent article upsert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArticleUpsertResult {
    article: Article,
    feed_visible_change: bool,
    created: bool,
}

impl ArticleUpsertResult {
    /// Creates a repository result.
    pub(crate) fn new(article: Article, feed_visible_change: bool, created: bool) -> Self {
        Self {
            article,
            feed_visible_change,
            created,
        }
    }

    /// Returns the persisted article.
    pub fn article(&self) -> &Article {
        &self.article
    }

    /// Consumes the result and returns the persisted article.
    pub fn into_article(self) -> Article {
        self.article
    }

    /// Reports whether RSS-visible fields changed.
    pub const fn feed_visible_change(&self) -> bool {
        self.feed_visible_change
    }

    /// Reports whether the upsert inserted a new article row.
    pub const fn created(&self) -> bool {
        self.created
    }
}

/// Compares the fields that can alter an RSS document.
pub(crate) fn feed_visible_change(current: &Article, incoming: &NewArticle) -> bool {
    current.title() != incoming.title
        || current.author() != incoming.author.as_deref()
        || current.summary() != incoming.summary.as_deref()
        || current.cover_url() != incoming.cover_url.as_deref()
        || current.original_url() != incoming.original_url.as_ref()
        || current.published_at() != incoming.published_at
        || current.content_html() != incoming.content_html
        || current.content_hash() != incoming.content_hash.as_deref()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    use super::*;

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    fn new_article() -> NewArticle {
        NewArticle {
            source_id: source_id(),
            review_id: " review-1 ".to_owned(),
            title: " Title ".to_owned(),
            author: Some(" Author ".to_owned()),
            summary: Some(" Summary ".to_owned()),
            cover_url: Some("https://cdn.example.test/cover.jpg".to_owned()),
            original_url: Some(
                "https://mp.weixin.qq.com/s/article"
                    .parse()
                    .expect("test URL should be valid"),
            ),
            published_at: Utc.timestamp_opt(10, 0).single().unwrap(),
            content_html: "<p>content</p>".to_owned(),
            content_hash: Some(" hash ".to_owned()),
            observation_version: ArticleObservationVersion::from_u64(1),
            fetched_at: Utc.timestamp_opt(20, 0).single().unwrap(),
        }
    }

    #[test]
    fn normalization_trims_metadata_but_preserves_html() {
        let mut article = new_article();
        article.content_html = " <p>content</p> ".to_owned();
        let normalized = article.normalize().expect("article should normalize");
        assert_eq!(normalized.review_id, "review-1");
        assert_eq!(normalized.title, "Title");
        assert_eq!(normalized.author.as_deref(), Some("Author"));
        assert_eq!(normalized.content_hash.as_deref(), Some("hash"));
        assert_eq!(normalized.content_html, " <p>content</p> ");
    }

    #[test]
    fn observation_only_changes_do_not_require_feed_revision() {
        let normalized = new_article().normalize().unwrap();
        let current = Article::from_parts(ArticleParts {
            source_id: normalized.source_id,
            review_id: normalized.review_id.clone(),
            title: normalized.title.clone(),
            author: normalized.author.clone(),
            summary: normalized.summary.clone(),
            cover_url: normalized.cover_url.clone(),
            original_url: normalized.original_url.clone(),
            published_at: normalized.published_at,
            content_html: normalized.content_html.clone(),
            content_hash: normalized.content_hash.clone(),
            observation_version: normalized.observation_version,
            fetched_at: normalized.fetched_at,
            created_at: normalized.fetched_at,
            updated_at: normalized.fetched_at,
        })
        .unwrap();
        let mut observed_again = normalized;
        observed_again.observation_version = ArticleObservationVersion::from_u64(2);
        observed_again.fetched_at += chrono::Duration::hours(1);
        assert!(!feed_visible_change(&current, &observed_again));
    }

    #[test]
    fn partial_observation_preserves_known_detail_fields() {
        let normalized = new_article().normalize().unwrap();
        let current = Article::from_parts(ArticleParts {
            source_id: normalized.source_id,
            review_id: normalized.review_id.clone(),
            title: normalized.title.clone(),
            author: normalized.author.clone(),
            summary: normalized.summary.clone(),
            cover_url: normalized.cover_url.clone(),
            original_url: normalized.original_url.clone(),
            published_at: normalized.published_at,
            content_html: normalized.content_html.clone(),
            content_hash: normalized.content_hash.clone(),
            observation_version: normalized.observation_version,
            fetched_at: normalized.fetched_at,
            created_at: normalized.fetched_at,
            updated_at: normalized.fetched_at,
        })
        .unwrap();
        let partial = NewArticle {
            author: None,
            summary: None,
            cover_url: None,
            original_url: None,
            content_html: String::new(),
            content_hash: None,
            observation_version: ArticleObservationVersion::from_u64(2),
            fetched_at: current.fetched_at() + chrono::Duration::hours(1),
            ..normalized
        };

        let merged = current.merge_observation(&partial);
        assert_eq!(merged.original_url, current.original_url);
        assert_eq!(merged.content_html, current.content_html);
        assert_eq!(merged.content_hash, current.content_hash);
        assert!(!feed_visible_change(&current, &merged));
    }

    #[test]
    fn rejects_provider_signed_cover_urls_but_allows_resource_queries() {
        let mut signed = new_article();
        signed.cover_url = Some("https://cdn.example/image?X-Amz-Signature=secret".to_owned());
        assert_eq!(signed.normalize(), Err(ArticleError::InvalidCoverUrl));

        let mut resource_query = new_article();
        let ordinary = "https://cdn.example/image?width=640&token=resource-token&width=320";
        resource_query.cover_url = Some(ordinary.to_owned());
        assert_eq!(
            resource_query.normalize().unwrap().cover_url.as_deref(),
            Some(ordinary)
        );
    }

    #[test]
    fn invalid_identity_and_cover_are_rejected() {
        let mut invalid = new_article();
        invalid.source_id = SourceId::from_uuid(Uuid::nil());
        assert_eq!(invalid.normalize(), Err(ArticleError::InvalidSourceId));

        let mut invalid = new_article();
        invalid.cover_url = Some("javascript:alert(1)".to_owned());
        assert_eq!(invalid.normalize(), Err(ArticleError::InvalidCoverUrl));

        let mut invalid = new_article();
        invalid.observation_version = ArticleObservationVersion::from_u64(0);
        assert_eq!(
            invalid.normalize(),
            Err(ArticleError::InvalidObservationVersion)
        );
    }
}
