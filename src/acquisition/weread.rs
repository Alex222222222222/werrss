//! WeRead account and article-list protocol adapter boundary.
//!
//! This module defines the authenticated protocol port for QR/login state,
//! refresh-token lifecycle, article-list responses, detail-URL recovery,
//! current/legacy response-shape parsing, and rendered article-content
//! fallback. Public article fetching remains a separate credential-free
//! operation in [`super::article_page`] and is attempted before this fallback.
//!
//! A caller must obtain the one-request capability from
//! [`super::webdriver::AuthenticatedBrowserSession::prepare_request`] before
//! issuing protocol requests. That capability performs a server-clock lease
//! heartbeat, so an expired lease cannot reach the adapter. Lease loss is
//! terminal for the current operation and must not trigger token rotation.
//! Authentication expiry may be retried once by the application orchestration
//! layer, while risk-control responses remain terminal.
//!
//! The pure [`parse_article_list_payload`] boundary is executable without a
//! browser or network. It accepts the current `data` envelope, the legacy
//! `reviews[].subReviews[]` envelope, and the single-article `/api/mp/cover`
//! response, normalizes review records, and rejects unsupported or unsafe
//! values before they reach persistence. The concrete browser adapter below
//! keeps the authenticated WebDriver capability private and reads the
//! endpoint response as raw text and captures rendered HTML for the content
//! fallback; it does not parse browser-rendered JSON viewer text.
//! QR exchange remains outside this protocol adapter; credential refresh is
//! handled by the application authentication lifecycle.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde_json::{Map, Value};
use thiserror::Error;
use url::Url;

use crate::domain::{
    credentials::{WeReadAccountId, WeReadCredentials},
    source::VerifiedWechatArticleUrl,
};

use super::{
    article_page::{parse_article_html_with_fallback, ArticlePageError, ExtractedArticlePage},
    browser_pool::{AccountLeaseError, AccountLeaseStore},
    pacing::PacingController,
    webdriver::AuthenticatedRequest,
};

const WEREAD_HOST: &str = "weread.qq.com";
const WEREAD_ARTICLE_LIST_PATH: &str = "/web/mp/articles";
const WEREAD_ARTICLE_COVER_PATH: &str = "/api/mp/cover";
const WEREAD_ARTICLE_CONTENT_PATH: &str = "/web/mp/content";
const WEREAD_SHELF_PATH: &str = "/web/shelf";
const WEREAD_SHELF_URL: &str = "https://weread.qq.com/web/shelf";
const WEREAD_AUTHENTICATION_EXPIRED_CODE: i64 = -2012;

/// One normalized article-list entry returned by the WeRead adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeReadArticleReference {
    /// Stable upstream review ID used for article idempotency.
    pub review_id: String,
    /// Recovered public article URL, when the list response includes one.
    pub article_url: Option<VerifiedWechatArticleUrl>,
    /// Optional upstream title hint.
    pub title: Option<String>,
    /// Optional plain-text summary from the list response.
    pub summary: Option<String>,
    /// Optional public-account author name.
    pub author: Option<String>,
    /// Optional cover URL from the list response.
    pub cover_url: Option<String>,
    /// Optional publication timestamp from the list response.
    pub published_at: Option<DateTime<Utc>>,
}

impl WeReadArticleReference {
    /// Constructs a normalized reference and rejects an empty stable identity.
    pub fn new(
        review_id: impl Into<String>,
        article_url: Option<VerifiedWechatArticleUrl>,
        title: Option<String>,
    ) -> Result<Self, WeReadAdapterError> {
        let review_id = review_id.into().trim().to_owned();
        if review_id.is_empty() {
            return Err(WeReadAdapterError::InvalidReviewId);
        }
        Ok(Self {
            review_id,
            article_url,
            title,
            summary: None,
            author: None,
            cover_url: None,
            published_at: None,
        })
    }
}

/// Errors exposed by authenticated WeRead protocol adapters.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WeReadAdapterError {
    /// The account lease was lost before a request could be issued.
    #[error("WeRead account lease lost for {account_id}")]
    LeaseLost { account_id: WeReadAccountId },
    /// The account lease backend could not prove request ownership.
    #[error("WeRead account lease backend error: {0}")]
    LeaseBackend(String),
    /// The account session expired according to a WeRead business response.
    #[error("WeRead authentication expired (code {code})")]
    AuthenticationExpired {
        /// Stable upstream business error code.
        code: i64,
    },
    /// WeRead rejected the request for risk-control or rate-limit reasons.
    #[error("WeRead request was risk-controlled (code {code})")]
    RiskControlled {
        /// Stable upstream business error code.
        code: i64,
    },
    /// The authenticated content page requires environment verification.
    #[error("WeRead article content requires environment verification")]
    VerificationRequired,
    /// The adapter's local request or identity protocol was invalid.
    #[error("WeRead protocol error: {0}")]
    Protocol(String),
    /// WeRead returned a response that was valid JSON but did not match a
    /// supported response shape or business-error classification.
    #[error("WeRead returned an unexpected response: {0}")]
    UnexpectedResponse(String),
    /// The authenticated browser transport failed before a valid response
    /// could be parsed.
    #[error("WeRead browser operation failed: {0}")]
    Browser(String),
    /// No admin-enrolled credential provider was configured for the request.
    #[error("WeRead credential provider is not configured")]
    CredentialProviderNotConfigured,
    /// A response omitted the stable identity needed for idempotent storage.
    #[error("WeRead article review_id must not be empty")]
    InvalidReviewId,
    /// An upstream URL could not be reduced to a verified public WeChat URL.
    #[error("WeRead article URL is not a verified public WeChat URL")]
    InvalidArticleUrl,
}

/// Errors raised while loading stored browser credentials.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WeReadCredentialProviderError {
    /// No credentials exist for the requested account.
    #[error("WeRead credentials are not provisioned for this account")]
    NotProvisioned,
    /// The account is disabled or its encrypted payload is invalid.
    #[error("stored WeRead credentials are unavailable")]
    Unavailable,
}

/// Application boundary used by the browser adapter to load one account's
/// decrypted credentials for one request.
#[async_trait]
pub trait WeReadCredentialProvider: Send + Sync {
    /// Returns credentials without exposing them to the HTTP layer.
    async fn credentials(
        &self,
        account_id: WeReadAccountId,
    ) -> Result<WeReadCredentials, WeReadCredentialProviderError>;
}

/// Validation failure for the authenticated WeRead article endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WeReadEndpointError {
    /// The endpoint is not one of the supported HTTPS WeRead article APIs.
    #[error(
        "WeRead article endpoint must be HTTPS weread.qq.com/web/mp/articles or weread.qq.com/api/mp/cover without credentials, fragments, or a non-default port"
    )]
    Invalid,
}

impl From<AccountLeaseError> for WeReadAdapterError {
    fn from(error: AccountLeaseError) -> Self {
        match error {
            AccountLeaseError::LeaseLost { account_id } => Self::LeaseLost { account_id },
            other => Self::LeaseBackend(other.to_string()),
        }
    }
}

/// Parses a WeRead article-list response into validated references.
///
/// Current mobile responses use `{"data": [...]}`. Older deployments use
/// `{"reviews": [{"subReviews": [{"review": {...}}]}]}`. The current cover
/// endpoint returns one root object with `reviewId`, `title`, `name`, and
/// `pic`. Entries without a stable `reviewId` or title are ignored because
/// they cannot become useful articles. Present URLs are always converted to
/// [`VerifiedWechatArticleUrl`]; an invalid URL fails the complete response so
/// an unsafe value cannot be passed to a later browser operation.
pub fn parse_article_list_payload(
    payload: &Value,
) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
    let object = payload.as_object().ok_or_else(|| {
        WeReadAdapterError::UnexpectedResponse("response must be a JSON object".to_owned())
    })?;
    if let Some(error) = response_error(object)? {
        return Err(error);
    }

    if object.contains_key("reviewId") {
        return Ok(parse_cover_entry(object)?.into_iter().collect());
    }

    if let Some(data) = object.get("data") {
        let entries = data.as_array().ok_or_else(|| {
            WeReadAdapterError::UnexpectedResponse("data must be an array".to_owned())
        })?;
        return parse_entries(entries, None);
    }

    let Some(reviews) = object.get("reviews") else {
        return Err(WeReadAdapterError::UnexpectedResponse(
            "response has no supported article-list envelope".to_owned(),
        ));
    };
    let reviews = reviews.as_array().ok_or_else(|| {
        WeReadAdapterError::UnexpectedResponse("reviews must be an array".to_owned())
    })?;
    let mut result = Vec::new();
    for group in reviews.iter().filter_map(Value::as_object) {
        let group_time = group.get("createTime").and_then(unix_timestamp);
        let Some(sub_reviews) = group.get("subReviews") else {
            continue;
        };
        let sub_reviews = sub_reviews.as_array().ok_or_else(|| {
            WeReadAdapterError::UnexpectedResponse("subReviews must be an array".to_owned())
        })?;
        result.extend(parse_entries(sub_reviews, group_time)?);
    }
    Ok(result)
}

fn parse_cover_entry(
    object: &Map<String, Value>,
) -> Result<Option<WeReadArticleReference>, WeReadAdapterError> {
    let Some(review_id) = first_text(&[object], &["reviewId"]) else {
        return Ok(None);
    };
    let Some(title) = first_text(&[object], &["title"]) else {
        return Ok(None);
    };

    let article_url = article_url(&[object])?.or(cover_article_url(&review_id)?);
    Ok(Some(WeReadArticleReference {
        review_id,
        article_url,
        title: Some(title),
        summary: None,
        author: first_text(&[object], &["name"]),
        cover_url: first_text(&[object], &["pic"]),
        published_at: None,
    }))
}

fn parse_entries(
    entries: &[Value],
    group_time: Option<DateTime<Utc>>,
) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
    entries
        .iter()
        .filter_map(Value::as_object)
        .map(|entry| parse_entry(entry, group_time))
        .filter_map(|result| match result {
            Ok(Some(reference)) => Some(Ok(reference)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn parse_entry(
    entry: &Map<String, Value>,
    group_time: Option<DateTime<Utc>>,
) -> Result<Option<WeReadArticleReference>, WeReadAdapterError> {
    let review = entry
        .get("review")
        .and_then(Value::as_object)
        .unwrap_or(entry);
    let empty = Map::new();
    let mp_info = review
        .get("mpInfo")
        .and_then(Value::as_object)
        .unwrap_or(&empty);

    let Some(review_id) = first_text(&[review, entry], &["reviewId"]) else {
        return Ok(None);
    };
    let Some(title) = first_text(&[review, mp_info], &["title"]) else {
        return Ok(None);
    };

    let article_url = article_url(&[mp_info, review])?;
    let summary = first_text(&[mp_info, review], &["content"]);
    let author = first_text(&[mp_info], &["mp_name", "mpName"]);
    let cover_url = first_text(&[mp_info], &["pic_url", "picUrl"]);
    let published_at = first_timestamp(&[mp_info, review], &["time", "createTime"]).or(group_time);

    Ok(Some(WeReadArticleReference {
        review_id,
        article_url,
        title: Some(title),
        summary,
        author,
        cover_url,
        published_at,
    }))
}

fn response_error(
    object: &Map<String, Value>,
) -> Result<Option<WeReadAdapterError>, WeReadAdapterError> {
    let Some(value) = object.get("errcode").or_else(|| object.get("errCode")) else {
        return Ok(None);
    };
    let Some(code) = integer_value(value) else {
        return Err(WeReadAdapterError::UnexpectedResponse(
            "error code must be an integer".to_owned(),
        ));
    };
    if code == 0 {
        return Ok(None);
    }
    Ok(Some(match code {
        -2012 => WeReadAdapterError::AuthenticationExpired { code },
        -2041 | -2010 => WeReadAdapterError::RiskControlled { code },
        _ => WeReadAdapterError::UnexpectedResponse(format!("upstream error code {code}")),
    }))
}

fn article_url(
    objects: &[&Map<String, Value>],
) -> Result<Option<VerifiedWechatArticleUrl>, WeReadAdapterError> {
    let Some(value) = first_text(objects, &["doc_url", "docUrl", "url"]) else {
        let Some(original) = first_text(objects, &["originalId"]) else {
            return Ok(None);
        };
        return verify_article_url(&original);
    };
    verify_article_url(&value)
}

fn cover_article_url(
    review_id: &str,
) -> Result<Option<VerifiedWechatArticleUrl>, WeReadAdapterError> {
    let Some(review_id) = review_id.strip_prefix("MP_WXS_") else {
        return Ok(None);
    };
    let Some((_, article_id)) = review_id.rsplit_once('_') else {
        return Ok(None);
    };
    if article_id.trim().is_empty() {
        return Ok(None);
    }
    // WeRead uses `~` in review IDs where the public WeChat article URL uses `_`.
    let article_id = article_id.replace('~', "_");
    verify_article_url(&article_id)
}

fn verify_article_url(value: &str) -> Result<Option<VerifiedWechatArticleUrl>, WeReadAdapterError> {
    let value = value.trim();
    let candidate = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_owned()
    } else if value.starts_with("/s") {
        format!("https://mp.weixin.qq.com{value}")
    } else if value.starts_with('?') || value.contains("__biz=") {
        format!(
            "https://mp.weixin.qq.com/s?{}",
            value.trim_start_matches('?')
        )
    } else {
        let mut url = url::Url::parse("https://mp.weixin.qq.com/")
            .expect("static WeChat article URL must parse");
        url.path_segments_mut()
            .expect("static WeChat article URL must have path segments")
            .push("s")
            .push(value);
        url.to_string()
    };
    let article_url = VerifiedWechatArticleUrl::parse(&candidate)
        .map_err(|_| WeReadAdapterError::InvalidArticleUrl)?;
    let is_article_path = url::Url::parse(article_url.as_str())
        .is_ok_and(|url| url.path() == "/s" || url.path().starts_with("/s/"));
    if !is_article_path {
        return Err(WeReadAdapterError::InvalidArticleUrl);
    }
    Ok(Some(article_url))
}

fn first_text(objects: &[&Map<String, Value>], keys: &[&str]) -> Option<String> {
    objects.iter().find_map(|object| {
        keys.iter().find_map(|key| {
            let value = object.get(*key)?;
            match value {
                Value::String(value) => {
                    let value = value.trim();
                    (!value.is_empty()).then(|| value.to_owned())
                }
                Value::Number(value) => Some(value.to_string()),
                _ => None,
            }
        })
    })
}

fn first_timestamp(objects: &[&Map<String, Value>], keys: &[&str]) -> Option<DateTime<Utc>> {
    objects.iter().find_map(|object| {
        keys.iter()
            .find_map(|key| object.get(*key).and_then(unix_timestamp))
    })
}

fn unix_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    integer_value(value).and_then(|seconds| DateTime::from_timestamp(seconds, 0))
}

fn integer_value(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str()?.trim().parse().ok())
}

/// Thirtyfour-backed authenticated WeRead article adapter.
#[derive(Clone)]
pub struct BrowserWeReadAdapter {
    endpoint: Url,
    pacing: Option<PacingController>,
    credential_provider: Option<Arc<dyn WeReadCredentialProvider>>,
    timezone: Tz,
}

impl std::fmt::Debug for BrowserWeReadAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrowserWeReadAdapter")
            .field("endpoint", &self.endpoint)
            .field("pacing", &self.pacing)
            .field("credential_provider", &self.credential_provider.is_some())
            .field("timezone", &self.timezone)
            .finish()
    }
}

impl BrowserWeReadAdapter {
    /// Creates an adapter for a validated WeRead article endpoint.
    pub fn new(mut endpoint: Url) -> Result<Self, WeReadEndpointError> {
        validate_article_list_endpoint(&endpoint)?;
        if endpoint.port() == Some(443) {
            endpoint
                .set_port(None)
                .map_err(|_| WeReadEndpointError::Invalid)?;
        }
        Ok(Self {
            endpoint,
            pacing: None,
            credential_provider: None,
            timezone: chrono_tz::UTC,
        })
    }

    /// Adds the shared pacing controller used before authenticated requests.
    pub fn with_pacing(mut self, pacing: PacingController) -> Self {
        self.pacing = Some(pacing);
        self
    }

    /// Uses encrypted credentials supplied by the application boundary.
    pub fn with_credential_provider(mut self, provider: Arc<dyn WeReadCredentialProvider>) -> Self {
        self.credential_provider = Some(provider);
        self
    }

    /// Sets the timezone used when the authenticated content page exposes a
    /// local publication timestamp.
    pub const fn with_timezone(mut self, timezone: Tz) -> Self {
        self.timezone = timezone;
        self
    }

    /// Returns the configured endpoint without exposing browser credentials.
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    async fn authenticate_request<R>(
        &self,
        request: &mut AuthenticatedRequest<'_, R>,
    ) -> Result<(), WeReadAdapterError>
    where
        R: AccountLeaseStore,
    {
        tracing::debug!(account_id = %request.account_id(), "authenticating WeRead browser request");
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        let provider = self
            .credential_provider
            .as_ref()
            .ok_or(WeReadAdapterError::CredentialProviderNotConfigured)?;
        let credentials = provider
            .credentials(request.account_id())
            .await
            .map_err(|error| {
                tracing::warn!(account_id = %request.account_id(), error = %error, "unable to load stored WeRead credentials");
                WeReadAdapterError::Browser(error.to_string())
            })?;
        let cookie = credentials.web_cookie().ok_or_else(|| {
            WeReadAdapterError::Browser(
                "stored WeRead credentials do not contain a web cookie".to_owned(),
            )
        })?;
        request
            .goto(WEREAD_SHELF_URL)
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.safe_message()))?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        request
            .install_cookie_header(cookie)
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.safe_message()))?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        // The initial shelf navigation establishes the origin required for
        // WebDriver cookie injection. Navigate to it again after installing
        // credentials so WeRead can apply the cookies and redirect an
        // unauthenticated account to its login page before any protected
        // operation is attempted.
        request
            .goto(WEREAD_SHELF_URL)
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.safe_message()))?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        let shelf_url = request
            .current_url()
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.safe_message()))?;
        let result = ensure_authenticated_shelf_url(&shelf_url);
        match &result {
            Ok(()) => {
                tracing::debug!(account_id = %request.account_id(), "WeRead browser session is authenticated")
            }
            Err(error) => {
                tracing::warn!(account_id = %request.account_id(), error = %error, "WeRead browser session is not authenticated")
            }
        }
        result
    }
}

#[async_trait]
impl<R> WeReadAdapter<R> for BrowserWeReadAdapter
where
    R: AccountLeaseStore,
{
    async fn list_articles(
        &self,
        book_id: &str,
        mut request: AuthenticatedRequest<'_, R>,
    ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
        tracing::debug!(account_id = %request.account_id(), book_id, "requesting WeRead article list");
        let endpoint = article_list_endpoint(&self.endpoint, book_id)?;
        self.authenticate_request(&mut request).await?;
        if let Some(pacing) = &self.pacing {
            pacing.wait(crate::domain::pacing::DelayKind::Request).await;
        }
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        let body = request
            .fetch_text(endpoint.as_str())
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.safe_message()))?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        let result = parse_article_list_body(&body);
        match &result {
            Ok(references) => tracing::info!(
                account_id = %request.account_id(),
                references = references.len(),
                "parsed WeRead article list"
            ),
            Err(error) => tracing::warn!(
                account_id = %request.account_id(),
                response_bytes = body.len(),
                error = %error,
                "unable to parse WeRead article list"
            ),
        }
        result
    }

    async fn fetch_article_content(
        &self,
        reference: &WeReadArticleReference,
        mut request: AuthenticatedRequest<'_, R>,
    ) -> Result<ExtractedArticlePage, WeReadAdapterError> {
        tracing::debug!(account_id = %request.account_id(), review_id = %reference.review_id, "requesting authenticated WeRead article content");
        let canonical_url = reference
            .article_url
            .clone()
            .ok_or(WeReadAdapterError::InvalidArticleUrl)?;
        let endpoint = article_content_endpoint(&reference.review_id)?;
        self.authenticate_request(&mut request).await?;
        if let Some(pacing) = &self.pacing {
            pacing.wait(crate::domain::pacing::DelayKind::Request).await;
        }
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        request
            .goto(endpoint.as_str())
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.safe_message()))?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        let content_url = request
            .current_url()
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.safe_message()))?;
        ensure_authenticated_content_url(&content_url)?;
        let html = request
            .source()
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.safe_message()))?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        let result = parse_article_html_with_fallback(
            &html,
            canonical_url,
            self.timezone,
            reference.title.as_deref(),
            reference.published_at,
        )
        .map_err(map_article_content_error);
        match &result {
            Ok(_) => {
                tracing::info!(account_id = %request.account_id(), review_id = %reference.review_id, "parsed authenticated WeRead article content")
            }
            Err(error) => {
                tracing::warn!(account_id = %request.account_id(), review_id = %reference.review_id, error = %error, "unable to parse authenticated WeRead article content")
            }
        }
        result
    }
}

fn map_article_content_error(error: ArticlePageError) -> WeReadAdapterError {
    match error {
        ArticlePageError::VerificationRequired => WeReadAdapterError::VerificationRequired,
        error => WeReadAdapterError::Protocol(error.to_string()),
    }
}

fn ensure_authenticated_shelf_url(url: &Url) -> Result<(), WeReadAdapterError> {
    let is_shelf = url.scheme() == "https"
        && url.host_str() == Some(WEREAD_HOST)
        && (url.path() == WEREAD_SHELF_PATH || url.path() == "/web/shelf/")
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url.port().is_none_or(|port| port == 443);
    if is_shelf {
        Ok(())
    } else {
        Err(WeReadAdapterError::AuthenticationExpired {
            code: WEREAD_AUTHENTICATION_EXPIRED_CODE,
        })
    }
}

fn ensure_authenticated_content_url(url: &Url) -> Result<(), WeReadAdapterError> {
    let is_content = url.scheme() == "https"
        && url.host_str() == Some(WEREAD_HOST)
        && (url.path() == WEREAD_ARTICLE_CONTENT_PATH || url.path() == "/web/mp/content/")
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && url.port().is_none_or(|port| port == 443);
    if is_content {
        Ok(())
    } else {
        Err(WeReadAdapterError::AuthenticationExpired {
            code: WEREAD_AUTHENTICATION_EXPIRED_CODE,
        })
    }
}

fn validate_article_list_endpoint(endpoint: &Url) -> Result<(), WeReadEndpointError> {
    if endpoint.scheme() != "https"
        || endpoint.host_str() != Some(WEREAD_HOST)
        || !matches!(
            endpoint.path(),
            WEREAD_ARTICLE_LIST_PATH | WEREAD_ARTICLE_COVER_PATH
        )
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
        || endpoint.port().is_some_and(|port| port != 443)
    {
        return Err(WeReadEndpointError::Invalid);
    }
    Ok(())
}

fn article_list_endpoint(endpoint: &Url, book_id: &str) -> Result<Url, WeReadAdapterError> {
    let book_id = book_id.trim();
    if book_id.is_empty() {
        return Err(WeReadAdapterError::Protocol(
            "book_id must not be empty".to_owned(),
        ));
    }
    let mut endpoint = endpoint.clone();
    let is_article_list = endpoint.path() == WEREAD_ARTICLE_LIST_PATH;
    let configured_query = endpoint
        .query_pairs()
        .filter(|(key, _)| key != "bookId" && key != "offset")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    endpoint.set_query(None);
    {
        let mut query = endpoint.query_pairs_mut();
        for (key, value) in configured_query {
            query.append_pair(&key, &value);
        }
        query.append_pair("bookId", book_id);
        if is_article_list {
            query.append_pair("offset", "0");
        }
    }
    Ok(endpoint)
}

fn article_content_endpoint(review_id: &str) -> Result<Url, WeReadAdapterError> {
    let review_id = review_id.trim();
    if review_id.is_empty() {
        return Err(WeReadAdapterError::InvalidReviewId);
    }
    let mut endpoint = Url::parse("https://weread.qq.com/web/mp/content")
        .expect("static WeRead article-content URL must parse");
    endpoint
        .query_pairs_mut()
        .append_pair("reviewId", review_id);
    Ok(endpoint)
}

fn parse_article_list_body(body: &str) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
    let payload = serde_json::from_str::<Value>(body).map_err(|error| {
        WeReadAdapterError::UnexpectedResponse(format!("response was not JSON: {error}"))
    })?;
    parse_article_list_payload(&payload)
}

/// Port for authenticated WeRead account/list operations.
#[async_trait::async_trait]
pub trait WeReadAdapter<R>: Send + Sync
where
    R: AccountLeaseStore,
{
    /// Lists normalized article references using a freshly heartbeated request.
    async fn list_articles(
        &self,
        book_id: &str,
        request: AuthenticatedRequest<'_, R>,
    ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError>;

    /// Fetches and extracts one article through the authenticated WeRead
    /// content endpoint. Implementations that only support listing may retain
    /// the default error; the runtime uses this operation only as a fallback
    /// after public article extraction fails.
    async fn fetch_article_content(
        &self,
        _reference: &WeReadArticleReference,
        _request: AuthenticatedRequest<'_, R>,
    ) -> Result<ExtractedArticlePage, WeReadAdapterError> {
        Err(WeReadAdapterError::Protocol(
            "authenticated WeRead article-content fallback is not supported".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        acquisition::browser_pool::BrowserPool,
        persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository,
    };

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    struct FakeAdapter;

    #[async_trait]
    impl<R> WeReadAdapter<R> for FakeAdapter
    where
        R: AccountLeaseStore,
    {
        async fn list_articles(
            &self,
            book_id: &str,
            request: AuthenticatedRequest<'_, R>,
        ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
            assert_eq!(book_id, "book-1");
            let _account_id = request.account_id();
            Ok(vec![
                WeReadArticleReference::new("review-1", None, None).unwrap()
            ])
        }
    }

    #[test]
    fn rejects_missing_stable_article_identity() {
        assert_eq!(
            WeReadArticleReference::new("  ", None, None),
            Err(WeReadAdapterError::InvalidReviewId)
        );
    }

    #[test]
    fn trims_stable_article_identity() {
        let reference = WeReadArticleReference::new(" review-1 ", None, None).unwrap();
        assert_eq!(reference.review_id, "review-1");
        assert!(reference.published_at.is_none());
    }

    #[test]
    fn parses_current_response_and_normalizes_nested_metadata() {
        let payload = json!({
            "data": [
                {
                    "reviewId": " review-current ",
                    "title": " Current title ",
                    "createTime": "1710000000",
                    "mpInfo": {
                        "content": " summary ",
                        "mpName": " Account ",
                        "picUrl": "https://mp.weixin.qq.com/logo.png",
                        "docUrl": "https://mp.weixin.qq.com/s/current"
                    }
                },
                null,
                {"reviewId": "without-title"}
            ]
        });

        let articles = parse_article_list_payload(&payload).expect("current response should parse");
        assert_eq!(articles.len(), 1);
        let article = &articles[0];
        assert_eq!(article.review_id, "review-current");
        assert_eq!(article.title.as_deref(), Some("Current title"));
        assert_eq!(article.summary.as_deref(), Some("summary"));
        assert_eq!(article.author.as_deref(), Some("Account"));
        assert_eq!(
            article.cover_url.as_deref(),
            Some("https://mp.weixin.qq.com/logo.png")
        );
        assert_eq!(
            article.published_at,
            DateTime::from_timestamp(1_710_000_000, 0)
        );
        assert_eq!(
            article
                .article_url
                .as_ref()
                .map(VerifiedWechatArticleUrl::as_str),
            Some("https://mp.weixin.qq.com/s/current")
        );
    }

    #[test]
    fn parses_legacy_response_and_uses_group_time_as_fallback() {
        let payload = json!({
            "reviews": [{
                "createTime": 1700000000,
                "subReviews": [
                    {
                        "review": {
                            "reviewId": "legacy-1",
                            "mpInfo": {
                                "title": "Legacy title",
                                "originalId": "legacy-token"
                            }
                        }
                    }
                ]
            }]
        });

        let articles = parse_article_list_payload(&payload).expect("legacy response should parse");
        assert_eq!(articles.len(), 1);
        assert_eq!(
            articles[0].published_at,
            DateTime::from_timestamp(1_700_000_000, 0)
        );
        assert_eq!(
            articles[0]
                .article_url
                .as_ref()
                .map(VerifiedWechatArticleUrl::as_str),
            Some("https://mp.weixin.qq.com/s/legacy-token")
        );
    }

    #[test]
    fn parses_the_cover_response_and_recovers_its_public_article_url() {
        let payload = json!({
            "avatar": "http://wx.qlogo.cn/avatar",
            "name": " 人物 ",
            "title": " 影子和影子，苹果CEO的接力赛 ",
            "pic": "https://mmbiz.qpic.cn/cover.jpg",
            "reviewId": " MP_WXS_2103095721_1V0fvyRTje-N7TWQunyLJA "
        });

        let articles = parse_article_list_payload(&payload).expect("cover response should parse");

        assert_eq!(articles.len(), 1);
        let article = &articles[0];
        assert_eq!(
            article.review_id,
            "MP_WXS_2103095721_1V0fvyRTje-N7TWQunyLJA"
        );
        assert_eq!(
            article.title.as_deref(),
            Some("影子和影子，苹果CEO的接力赛")
        );
        assert_eq!(article.author.as_deref(), Some("人物"));
        assert_eq!(
            article.cover_url.as_deref(),
            Some("https://mmbiz.qpic.cn/cover.jpg")
        );
        assert_eq!(
            article
                .article_url
                .as_ref()
                .map(VerifiedWechatArticleUrl::as_str),
            Some("https://mp.weixin.qq.com/s/1V0fvyRTje-N7TWQunyLJA")
        );
    }

    #[test]
    fn converts_we_read_tilde_separator_to_wechat_article_underscore() {
        let payload = json!({
            "reviewId": "MP_WXS_2103095721_8X5pfeUgOhJVK~8cFnjx0A",
            "title": "Article title"
        });

        let articles = parse_article_list_payload(&payload).expect("cover payload should parse");

        assert_eq!(
            articles[0].article_url.as_ref().unwrap().as_str(),
            "https://mp.weixin.qq.com/s/8X5pfeUgOhJVK_8cFnjx0A"
        );
    }

    #[test]
    fn supports_article_url_variants_but_rejects_unsafe_hosts() {
        for (value, expected) in [
            ("/s/path-token", "https://mp.weixin.qq.com/s/path-token"),
            (
                "?__biz=MzA==&mid=1",
                "https://mp.weixin.qq.com/s?__biz=MzA==&mid=1",
            ),
            (
                "token with spaces",
                "https://mp.weixin.qq.com/s/token%20with%20spaces",
            ),
        ] {
            let payload = json!({
                "data": [{
                    "reviewId": "review",
                    "title": "title",
                    "mpInfo": {"originalId": value}
                }]
            });
            let articles = parse_article_list_payload(&payload).expect("URL variant should parse");
            assert_eq!(articles[0].article_url.as_ref().unwrap().as_str(), expected);
        }

        let unsafe_payload = json!({
            "data": [{
                "reviewId": "review",
                "title": "title",
                "mpInfo": {"docUrl": "https://example.com/not-wechat"}
            }]
        });
        assert_eq!(
            parse_article_list_payload(&unsafe_payload),
            Err(WeReadAdapterError::InvalidArticleUrl)
        );

        for value in [
            "https://mp.weixin.qq.com/cgi-bin/appmsg",
            "https://mp.weixin.qq.com/script",
            "/script",
        ] {
            let payload = json!({
                "data": [{
                    "reviewId": "review",
                    "title": "title",
                    "mpInfo": {"docUrl": value}
                }]
            });
            assert_eq!(
                parse_article_list_payload(&payload),
                Err(WeReadAdapterError::InvalidArticleUrl),
                "non-article path should be rejected: {value}"
            );
        }
    }

    #[test]
    fn classifies_business_errors_without_exposing_response_text() {
        assert_eq!(
            parse_article_list_payload(&json!({"errCode": "-2012", "errMsg": "secret"})),
            Err(WeReadAdapterError::AuthenticationExpired { code: -2012 })
        );
        assert_eq!(
            parse_article_list_payload(&json!({"errcode": -2041})),
            Err(WeReadAdapterError::RiskControlled { code: -2041 })
        );
        assert_eq!(
            parse_article_list_payload(&json!({"errcode": 1234})),
            Err(WeReadAdapterError::UnexpectedResponse(
                "upstream error code 1234".to_owned()
            ))
        );
    }

    #[test]
    fn rejects_malformed_envelopes_instead_of_returning_an_empty_success() {
        assert_eq!(
            parse_article_list_payload(&json!([])),
            Err(WeReadAdapterError::UnexpectedResponse(
                "response must be a JSON object".to_owned()
            ))
        );
        assert_eq!(
            parse_article_list_payload(&json!({"data": {}})),
            Err(WeReadAdapterError::UnexpectedResponse(
                "data must be an array".to_owned()
            ))
        );
        assert_eq!(
            parse_article_list_payload(&json!({"reviews": [{"subReviews": {}}]})),
            Err(WeReadAdapterError::UnexpectedResponse(
                "subReviews must be an array".to_owned()
            ))
        );
    }

    #[test]
    fn maps_non_json_browser_bodies_to_unexpected_response_errors() {
        assert!(matches!(
            parse_article_list_body("<html>login required</html>"),
            Err(WeReadAdapterError::UnexpectedResponse(message)) if message.starts_with("response was not JSON:")
        ));
    }

    #[test]
    fn accepts_the_authenticated_canonical_shelf_url() {
        for value in [
            "https://weread.qq.com/web/shelf",
            "https://weread.qq.com/web/shelf/",
            "https://weread.qq.com/web/shelf?tab=recent",
        ] {
            let url = value.parse().expect("test URL should parse");
            assert!(
                ensure_authenticated_shelf_url(&url).is_ok(),
                "canonical shelf URL should be accepted: {value}"
            );
        }
    }

    #[test]
    fn rejects_a_login_redirect_before_article_list_navigation() {
        let url: Url = "https://weread.qq.com/web/login".parse().unwrap();
        assert_eq!(
            ensure_authenticated_shelf_url(&url),
            Err(WeReadAdapterError::AuthenticationExpired {
                code: WEREAD_AUTHENTICATION_EXPIRED_CODE,
            })
        );
    }

    #[test]
    fn rejects_a_shelf_url_on_an_untrusted_host() {
        let url: Url = "https://i.weread.qq.com/web/shelf".parse().unwrap();
        assert_eq!(
            ensure_authenticated_shelf_url(&url),
            Err(WeReadAdapterError::AuthenticationExpired {
                code: WEREAD_AUTHENTICATION_EXPIRED_CODE,
            })
        );
    }

    #[test]
    fn appends_book_identity_without_discarding_configured_endpoint_query() {
        let adapter = BrowserWeReadAdapter::new(
            "https://weread.qq.com/web/mp/articles?count=100"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let endpoint = article_list_endpoint(adapter.endpoint(), "book/with spaces").unwrap();
        assert_eq!(
            endpoint.as_str(),
            "https://weread.qq.com/web/mp/articles?count=100&bookId=book%2Fwith+spaces&offset=0"
        );
    }

    #[test]
    fn replaces_configured_book_identity_and_offset() {
        let endpoint: Url =
            "https://weread.qq.com/web/mp/articles?bookId=wrong&offset=99&count=100"
                .parse()
                .unwrap();
        let endpoint = article_list_endpoint(&endpoint, "right").unwrap();

        assert_eq!(
            endpoint.as_str(),
            "https://weread.qq.com/web/mp/articles?count=100&bookId=right&offset=0"
        );
    }

    #[test]
    fn cover_endpoint_adds_book_identity_without_a_list_offset() {
        let endpoint: Url = "https://weread.qq.com/api/mp/cover?bookId=wrong&offset=99&count=1"
            .parse()
            .unwrap();
        let endpoint = article_list_endpoint(&endpoint, "right").unwrap();

        assert_eq!(
            endpoint.as_str(),
            "https://weread.qq.com/api/mp/cover?count=1&bookId=right"
        );
    }

    #[test]
    fn builds_an_encoded_authenticated_content_endpoint() {
        let endpoint = article_content_endpoint(" review/id with spaces ")
            .expect("a non-empty review ID should produce an endpoint");
        assert_eq!(
            endpoint.as_str(),
            "https://weread.qq.com/web/mp/content?reviewId=review%2Fid+with+spaces"
        );
    }

    #[test]
    fn rejects_an_empty_authenticated_content_review_id() {
        assert_eq!(
            article_content_endpoint("  "),
            Err(WeReadAdapterError::InvalidReviewId)
        );
    }

    #[test]
    fn accepts_only_the_authenticated_content_path_after_navigation() {
        for value in [
            "https://weread.qq.com/web/mp/content?reviewId=review",
            "https://weread.qq.com/web/mp/content/?reviewId=review",
        ] {
            let url = value.parse().expect("test URL should parse");
            assert!(ensure_authenticated_content_url(&url).is_ok());
        }
    }

    #[test]
    fn treats_a_content_login_redirect_as_expired_authentication() {
        let url: Url = "https://weread.qq.com/web/login".parse().unwrap();
        assert_eq!(
            ensure_authenticated_content_url(&url),
            Err(WeReadAdapterError::AuthenticationExpired {
                code: WEREAD_AUTHENTICATION_EXPIRED_CODE,
            })
        );
    }

    #[test]
    fn preserves_verification_errors_from_authenticated_content_parsing() {
        assert_eq!(
            map_article_content_error(ArticlePageError::VerificationRequired),
            WeReadAdapterError::VerificationRequired
        );
    }

    #[test]
    fn rejects_an_empty_book_identity_before_touching_the_browser() {
        let endpoint: Url = "https://weread.qq.com/web/mp/articles".parse().unwrap();
        assert_eq!(
            article_list_endpoint(&endpoint, "  "),
            Err(WeReadAdapterError::Protocol(
                "book_id must not be empty".to_owned()
            ))
        );
    }

    #[test]
    fn rejects_untrusted_article_list_endpoints_at_construction() {
        for value in [
            "http://weread.qq.com/web/mp/articles",
            "https://example.test/web/mp/articles",
            "https://weread.qq.com/web/mp/other",
            "https://weread.qq.com:8443/web/mp/articles",
            "https://user:password@weread.qq.com/web/mp/articles",
            "https://weread.qq.com/web/mp/articles#fragment",
            "https://i.weread.qq.com/web/mp/articles",
        ] {
            let endpoint = value.parse().expect("test URL should parse");
            assert!(
                matches!(
                    BrowserWeReadAdapter::new(endpoint),
                    Err(WeReadEndpointError::Invalid)
                ),
                "unsafe endpoint should be rejected: {value}"
            );
        }
    }

    #[test]
    fn normalizes_the_default_https_port_for_the_article_list_endpoint() {
        let adapter =
            BrowserWeReadAdapter::new("https://weread.qq.com:443/web/mp/articles".parse().unwrap())
                .unwrap();
        assert_eq!(
            adapter.endpoint().as_str(),
            "https://weread.qq.com/web/mp/articles"
        );
    }

    #[tokio::test]
    async fn request_pacing_waits_before_authenticated_navigation() {
        use crate::domain::pacing::{DelayDistribution, PacingPolicy};

        let delay = DelayDistribution::new(10.0, 0.0, 10.0, 10.0).unwrap();
        let policy = PacingPolicy::new(
            delay,
            delay,
            delay,
            delay,
            1,
            1,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        let pacing = PacingController::from_seed(policy, 1);
        let started = tokio::time::Instant::now();
        pacing.wait(crate::domain::pacing::DelayKind::Request).await;
        assert!(started.elapsed() >= std::time::Duration::from_millis(8));
    }

    #[tokio::test]
    async fn adapter_receives_only_an_authenticated_session() {
        let repository = MemoryAccountLeaseRepository::new(Utc::now());
        let pool = BrowserPool::new(1).unwrap();
        let mut session = pool
            .open_authenticated(
                repository,
                account_id(),
                "worker-a",
                chrono::Duration::seconds(30),
            )
            .await
            .unwrap()
            .unwrap();

        let request = session
            .prepare_request(chrono::Duration::seconds(30))
            .await
            .unwrap();
        let entries = FakeAdapter.list_articles("book-1", request).await.unwrap();
        assert_eq!(entries[0].review_id, "review-1");
    }
}
