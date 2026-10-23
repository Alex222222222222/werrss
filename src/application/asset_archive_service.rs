//! Bounded anonymous HTTP acquisition for article image assets.
//!
//! WebDriver DOM access cannot reliably recover the original response body.
//! This service performs a separate request with the article page's
//! `Referer`, derived `Origin`, and optional browser User-Agent. WeChat public
//! article media is treated as public in the first implementation, so no
//! WeRead cookie or browser cookie jar is ever sent here.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use reqwest::{header, redirect::Policy, Client, Response, StatusCode};
use thiserror::Error;
use tokio::time::{timeout, Instant};
use url::{Host, Url};

use crate::{
    archive::asset_store::{AssetCachePolicy, AssetInput},
    domain::{source::VerifiedWechatArticleUrl, url_policy::has_provider_signature},
};

/// Errors raised while constructing the asset HTTP client.
#[derive(Debug, Error)]
pub enum AssetArchiveServiceError {
    /// Reqwest could not construct a client from the validated policy.
    #[error("asset HTTP client could not be constructed: {0}")]
    Client(String),
}

/// Safe failure categories for one repair-specific asset fetch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairAssetFetchFailureKind {
    /// Exact provider evidence matched the repair-only policy.
    ConfirmedSignedUrlExpiry,
    /// The provider returned an unsuccessful HTTP response without exact expiry evidence.
    Http,
    /// The request could not be completed.
    Transport,
    /// The redirect chain could not be followed.
    Redirect,
    /// A source or final destination failed outbound URL validation.
    UnsafeDestination,
    /// The response media type or signature was invalid.
    MediaValidation,
    /// The response body could not be read safely.
    BodyRead,
    /// The response exceeded a configured fetch budget.
    Budget,
}

/// Failure details that are safe to retain or log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("asset fetch failed ({kind:?}, status={status:?})")]
pub struct RepairAssetFetchFailure {
    /// Bounded, secret-free failure classification.
    pub kind: RepairAssetFetchFailureKind,
    /// HTTP status when a response was received.
    pub status: Option<u16>,
}

impl RepairAssetFetchFailure {
    fn new(kind: RepairAssetFetchFailureKind, status: Option<StatusCode>) -> Self {
        Self {
            kind,
            status: status.map(|status| status.as_u16()),
        }
    }
}

/// Result of fetching one asset for a repair job.
pub enum RepairAssetFetchOutcome {
    /// A complete validated input ready for persistence.
    Fetched(Box<AssetInput>),
    /// A bounded failure without request URLs, bodies, or raw errors.
    Failed(RepairAssetFetchFailure),
}

/// Exact response marker required to classify a provider-confirmed expiry.
#[derive(Clone, Copy)]
struct ProviderExpiryRule {
    host: &'static str,
    status: StatusCode,
    code: &'static str,
}

const PRODUCTION_EXPIRY_RULES: &[ProviderExpiryRule] = &[];
const MAX_EXPIRY_EVIDENCE_BYTES: usize = 4096;

fn classify_expiry_evidence(
    final_url: &Url,
    status: StatusCode,
    evidence: &[u8],
    rules: &[ProviderExpiryRule],
) -> bool {
    if evidence.len() > MAX_EXPIRY_EVIDENCE_BYTES {
        return false;
    }
    let Some(host) = final_url.host_str() else {
        return false;
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(evidence) else {
        return false;
    };
    let Some(code) = value.get("code").and_then(serde_json::Value::as_str) else {
        return false;
    };
    rules
        .iter()
        .any(|rule| host == rule.host && status == rule.status && code == rule.code)
}

fn redirect_failure_kind(response: &Response) -> RepairAssetFetchFailureKind {
    let Some(location) = response
        .headers()
        .get(header::LOCATION)
        .and_then(|value| value.to_str().ok())
    else {
        return RepairAssetFetchFailureKind::Redirect;
    };
    let Some(destination) = response.url().join(location).ok() else {
        return RepairAssetFetchFailureKind::UnsafeDestination;
    };
    if validate_asset_url(&destination).is_none() {
        RepairAssetFetchFailureKind::UnsafeDestination
    } else {
        RepairAssetFetchFailureKind::Redirect
    }
}

async fn response_has_expiry_evidence(
    mut response: Response,
    started: Instant,
    max_fetch_time: Duration,
    request_timeout: Duration,
    rules: &[ProviderExpiryRule],
) -> Result<bool, RepairAssetFetchFailureKind> {
    use RepairAssetFetchFailureKind as Failure;

    let status = response.status();
    let Some(final_url) = validate_asset_url(response.url()) else {
        return Err(Failure::UnsafeDestination);
    };
    if response
        .content_length()
        .is_some_and(|length| length > MAX_EXPIRY_EVIDENCE_BYTES as u64)
    {
        return Ok(false);
    }

    let mut evidence = Vec::new();
    loop {
        let remaining_time = request_timeout
            .min(max_fetch_time)
            .checked_sub(started.elapsed())
            .ok_or(Failure::Budget)?;
        let chunk = match timeout(remaining_time, response.chunk()).await {
            Ok(Ok(Some(chunk))) => chunk,
            Ok(Ok(None)) => break,
            Ok(Err(error)) if error.is_timeout() => return Err(Failure::Budget),
            Ok(Err(_)) => return Err(Failure::BodyRead),
            Err(_) => return Err(Failure::Budget),
        };
        let remaining = MAX_EXPIRY_EVIDENCE_BYTES
            .saturating_add(1)
            .saturating_sub(evidence.len());
        evidence.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if evidence.len() > MAX_EXPIRY_EVIDENCE_BYTES {
            return Ok(false);
        }
    }

    Ok(classify_expiry_evidence(
        &final_url, status, &evidence, rules,
    ))
}

async fn read_repair_body(
    mut response: Response,
    limit: u64,
    started: Instant,
    max_fetch_time: Duration,
    request_timeout: Duration,
) -> Result<Vec<u8>, RepairAssetFetchFailureKind> {
    use RepairAssetFetchFailureKind as Failure;

    let mut bytes = Vec::new();
    loop {
        let remaining_time = request_timeout
            .min(max_fetch_time)
            .checked_sub(started.elapsed())
            .ok_or(Failure::Budget)?;
        let chunk = match timeout(remaining_time, response.chunk()).await {
            Ok(Ok(chunk)) => chunk,
            Ok(Err(error)) => {
                if error.is_timeout() {
                    return Err(Failure::Budget);
                }
                return Err(Failure::BodyRead);
            }
            Err(_) => return Err(Failure::Budget),
        };
        let Some(chunk) = chunk else { break };
        let chunk_size = u64::try_from(chunk.len()).map_err(|_| Failure::Budget)?;
        if chunk_size > limit.saturating_sub(bytes.len() as u64) {
            return Err(Failure::Budget);
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.is_empty() {
        return Err(Failure::BodyRead);
    }
    Ok(bytes)
}

/// Bounded, anonymous asset fetcher used before article persistence.
#[derive(Clone)]
pub struct AssetArchiveService {
    client: Client,
    policy: AssetCachePolicy,
    user_agent: Option<String>,
    #[cfg(test)]
    test_expiry_rules: Vec<ProviderExpiryRule>,
}

impl std::fmt::Debug for AssetArchiveService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssetArchiveService")
            .field("policy", &self.policy)
            .field("user_agent_configured", &self.user_agent.is_some())
            .finish()
    }
}

impl AssetArchiveService {
    /// Builds an asset client with a bounded timeout and redirect policy.
    pub fn new(
        policy: AssetCachePolicy,
        user_agent: Option<String>,
    ) -> Result<Self, AssetArchiveServiceError> {
        let max_redirects = policy.max_redirects();
        let client = asset_client_builder()
            .timeout(policy.fetch_timeout())
            .dns_resolver(SafeDnsResolver)
            .redirect(Policy::custom(move |attempt| {
                if !can_follow_redirect(attempt.previous().len(), max_redirects, attempt.url()) {
                    attempt.stop()
                } else {
                    attempt.follow()
                }
            }))
            .build()
            .map_err(|error| AssetArchiveServiceError::Client(error.to_string()))?;

        Ok(Self {
            client,
            policy,
            user_agent: user_agent.filter(|value| !value.is_empty()),
            #[cfg(test)]
            test_expiry_rules: Vec::new(),
        })
    }

    /// Returns the policy enforced by this fetcher.
    pub const fn policy(&self) -> AssetCachePolicy {
        self.policy
    }

    /// Builds the fetcher with an injected HTTP client for integration tests.
    ///
    /// The injected client is intentionally explicit so tests can resolve a
    /// controlled hostname to a local fixture without weakening the
    /// production safe-DNS resolver used by [`Self::new`].
    #[doc(hidden)]
    pub fn with_client_for_test(
        policy: AssetCachePolicy,
        user_agent: Option<String>,
        client: Client,
    ) -> Self {
        Self {
            client,
            policy,
            user_agent,
            #[cfg(test)]
            test_expiry_rules: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_expiry_rule_for_test(
        mut self,
        host: &'static str,
        status: StatusCode,
        code: &'static str,
    ) -> Self {
        self.test_expiry_rules
            .push(ProviderExpiryRule { host, status, code });
        self
    }

    /// Fetches distinct approved image URLs on a best-effort basis.
    ///
    /// A failed or invalid asset is logged and omitted. The article remains
    /// persistable with its original external URL. The returned occurrence is
    /// the index in the sanitizer's first-seen URL list, which lets the
    /// persistence layer attach the stable route to the correct image.
    pub async fn fetch_assets(
        &self,
        referer: &VerifiedWechatArticleUrl,
        urls: &[Url],
    ) -> Vec<AssetInput> {
        self.fetch_assets_with_context(referer, urls, None, None)
            .await
    }

    /// Fetches assets using request context captured with the article
    /// relationship. The optional values are used by repair jobs; initial
    /// acquisition derives the origin and uses the configured browser profile.
    pub async fn fetch_assets_with_context(
        &self,
        referer: &VerifiedWechatArticleUrl,
        urls: &[Url],
        captured_origin: Option<&str>,
        captured_user_agent: Option<&str>,
    ) -> Vec<AssetInput> {
        let origin = captured_origin
            .map(str::to_owned)
            .or_else(|| origin_for(referer.as_str()));
        let user_agent = captured_user_agent.or(self.user_agent.as_deref());
        let started = Instant::now();
        let mut fetched_bytes = 0_u64;
        let mut fetched = Vec::new();

        for (occurrence, source_url) in urls.iter().enumerate() {
            if occurrence >= self.policy.max_count_per_article() as usize {
                tracing::warn!(
                    attempted = occurrence,
                    limit = self.policy.max_count_per_article(),
                    "asset count budget exhausted"
                );
                break;
            }
            let Some(remaining_time) = self.remaining_fetch_time(started) else {
                tracing::warn!(attempted = occurrence, "asset fetch time budget exhausted");
                break;
            };
            let remaining = self
                .policy
                .max_fetch_bytes_per_article()
                .saturating_sub(fetched_bytes);
            if remaining == 0 {
                tracing::warn!("asset fetch byte budget exhausted");
                break;
            }

            let Some(source_url) = validate_asset_url(source_url) else {
                tracing::warn!(
                    occurrence,
                    host = ?source_url.host_str(),
                    "asset URL rejected by outbound policy"
                );
                continue;
            };
            if has_provider_signature(&source_url) {
                tracing::warn!(occurrence, "signed asset URL omitted from acquisition");
                continue;
            }
            tracing::debug!(occurrence, host = ?source_url.host_str(), "fetching article asset");

            let mut request = self
                .client
                .get(source_url.clone())
                .header(header::REFERER, referer.as_str())
                .header(
                    header::ACCEPT,
                    "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                );
            if let Some(origin) = origin.as_deref() {
                request = request.header(header::ORIGIN, origin);
            }
            if let Some(user_agent) = user_agent {
                request = request.header(header::USER_AGENT, user_agent);
            }
            let response = match timeout(remaining_time, request.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => {
                    tracing::warn!(occurrence, "asset request failed");
                    continue;
                }
                Err(_) => {
                    tracing::warn!(
                        occurrence,
                        host = ?source_url.host_str(),
                        "asset request exceeded the article fetch time budget"
                    );
                    break;
                }
            };
            if !response.status().is_success() {
                tracing::warn!(
                    occurrence,
                    status = response.status().as_u16(),
                    host = ?source_url.host_str(),
                    "asset request returned a non-success status"
                );
                continue;
            }

            let Some(media_type) = response_media_type(&response) else {
                tracing::warn!(
                    occurrence,
                    "asset response did not have a supported image type"
                );
                continue;
            };
            let per_asset_limit = self.policy.max_asset_size_bytes().min(remaining);
            if response
                .content_length()
                .is_some_and(|length| length > per_asset_limit)
            {
                tracing::warn!(
                    occurrence,
                    limit = per_asset_limit,
                    "asset response exceeds its configured byte budget"
                );
                continue;
            }

            let Some(final_url) = validate_asset_url(response.url()) else {
                tracing::warn!(occurrence, "asset redirect ended at an unsafe URL");
                continue;
            };
            if has_provider_signature(&final_url) {
                tracing::warn!(occurrence, "signed asset redirect omitted from acquisition");
                continue;
            }
            let Some(bytes) = self
                .read_body(response, per_asset_limit, &mut fetched_bytes, started)
                .await
            else {
                continue;
            };
            if !signature_matches(&media_type, &bytes) {
                tracing::warn!(
                    occurrence,
                    media_type,
                    "asset signature did not match media type"
                );
                continue;
            }
            tracing::debug!(
                occurrence,
                host = ?source_url.host_str(),
                bytes = bytes.len(),
                "article asset fetched"
            );
            fetched.push(AssetInput::new(
                source_url,
                final_url,
                media_type,
                bytes,
                occurrence as u32,
                Url::parse(referer.as_str()).expect("verified article URL must remain parseable"),
                origin.clone(),
                self.user_agent.clone(),
            ));
        }

        fetched
    }

    /// Fetches one asset with a typed, secret-free outcome for repair workers.
    pub async fn fetch_asset_for_repair(
        &self,
        referer: &VerifiedWechatArticleUrl,
        source_url: &Url,
        captured_origin: Option<&str>,
        captured_user_agent: Option<&str>,
    ) -> RepairAssetFetchOutcome {
        use RepairAssetFetchFailureKind as Failure;

        let Some(source_url) = validate_asset_url(source_url) else {
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                Failure::UnsafeDestination,
                None,
            ));
        };
        if self.policy.max_count_per_article() == 0 {
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                Failure::Budget,
                None,
            ));
        }
        let started = Instant::now();
        let Some(remaining_time) = self.remaining_fetch_time(started) else {
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                Failure::Budget,
                None,
            ));
        };
        let origin = captured_origin
            .map(str::to_owned)
            .or_else(|| origin_for(referer.as_str()));
        let user_agent = captured_user_agent.or(self.user_agent.as_deref());
        let mut request = self
            .client
            .get(source_url.clone())
            .header(header::REFERER, referer.as_str())
            .header(
                header::ACCEPT,
                "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            );
        if let Some(origin) = origin.as_deref() {
            request = request.header(header::ORIGIN, origin);
        }
        if let Some(user_agent) = user_agent {
            request = request.header(header::USER_AGENT, user_agent);
        }

        let response = match timeout(remaining_time, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let kind = if error.is_timeout() {
                    Failure::Budget
                } else if error.is_redirect() {
                    if error.url().is_some_and(|url| !is_safe_asset_url(url)) {
                        Failure::UnsafeDestination
                    } else {
                        Failure::Redirect
                    }
                } else {
                    Failure::Transport
                };
                return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(kind, None));
            }
            Err(_) => {
                return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                    Failure::Budget,
                    None,
                ));
            }
        };
        let status = response.status();
        if !status.is_success() {
            if status.is_redirection() {
                return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                    redirect_failure_kind(&response),
                    Some(status),
                ));
            }
            if validate_asset_url(response.url()).is_none() {
                return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                    Failure::UnsafeDestination,
                    Some(status),
                ));
            }
            #[cfg(test)]
            let expiry_rules = PRODUCTION_EXPIRY_RULES
                .iter()
                .chain(self.test_expiry_rules.iter())
                .copied()
                .collect::<Vec<_>>();
            #[cfg(test)]
            let expiry_rules = expiry_rules.as_slice();
            #[cfg(not(test))]
            let expiry_rules = PRODUCTION_EXPIRY_RULES;
            let kind = if expiry_rules.is_empty() {
                Failure::Http
            } else {
                match response_has_expiry_evidence(
                    response,
                    started,
                    self.policy.max_fetch_time_per_article(),
                    self.policy.fetch_timeout(),
                    expiry_rules,
                )
                .await
                {
                    Ok(true) => Failure::ConfirmedSignedUrlExpiry,
                    Ok(false) => Failure::Http,
                    Err(kind) => kind,
                }
            };
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                kind,
                Some(status),
            ));
        }
        let Some(final_url) = validate_asset_url(response.url()) else {
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                Failure::UnsafeDestination,
                Some(status),
            ));
        };
        if has_provider_signature(&source_url) || has_provider_signature(&final_url) {
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                Failure::UnsafeDestination,
                Some(status),
            ));
        }

        let Some(media_type) = response_media_type(&response) else {
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                Failure::MediaValidation,
                Some(status),
            ));
        };
        let limit = self
            .policy
            .max_asset_size_bytes()
            .min(self.policy.max_fetch_bytes_per_article());
        if limit == 0
            || response
                .content_length()
                .is_some_and(|length| length > limit)
        {
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                Failure::Budget,
                Some(status),
            ));
        }
        let bytes = match read_repair_body(
            response,
            limit,
            started,
            self.policy.max_fetch_time_per_article(),
            self.policy.fetch_timeout(),
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(kind) => {
                return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                    kind,
                    Some(status),
                ));
            }
        };
        if !signature_matches(&media_type, &bytes) {
            return RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure::new(
                Failure::MediaValidation,
                Some(status),
            ));
        }

        RepairAssetFetchOutcome::Fetched(Box::new(AssetInput::new(
            source_url,
            final_url,
            media_type,
            bytes,
            0,
            Url::parse(referer.as_str()).expect("verified article URL must remain parseable"),
            origin,
            user_agent.map(str::to_owned),
        )))
    }

    fn remaining_fetch_time(&self, started: Instant) -> Option<Duration> {
        self.policy
            .max_fetch_time_per_article()
            .checked_sub(started.elapsed())
    }

    async fn read_body(
        &self,
        mut response: Response,
        limit: u64,
        fetched_bytes: &mut u64,
        started: Instant,
    ) -> Option<Vec<u8>> {
        let mut bytes = Vec::new();
        loop {
            let Some(remaining_time) = self.remaining_fetch_time(started) else {
                tracing::warn!("asset fetch time budget exhausted during response body");
                return None;
            };
            let chunk = match timeout(remaining_time, response.chunk()).await {
                Ok(Ok(chunk)) => chunk,
                Err(_) => {
                    tracing::warn!("asset response body exceeded the article fetch time budget");
                    return None;
                }
                Ok(Err(_)) => {
                    tracing::warn!("asset response body could not be read");
                    return None;
                }
            };
            if self.remaining_fetch_time(started).is_none() {
                tracing::warn!("asset fetch time budget exhausted during response body");
                return None;
            }
            let Some(chunk) = chunk else { break };
            let chunk_size = u64::try_from(chunk.len()).ok()?;
            if chunk_size > limit.saturating_sub(bytes.len() as u64) {
                // Count the chunk that was already received before stopping.
                // Otherwise every oversized response leaves the aggregate
                // budget unchanged and the caller can fetch one oversized
                // body for every URL in the article.
                *fetched_bytes = fetched_bytes.saturating_add(chunk_size);
                tracing::warn!(limit, "asset response body exceeded its byte budget");
                return None;
            }
            bytes.extend_from_slice(&chunk);
            *fetched_bytes = fetched_bytes.saturating_add(chunk_size);
        }
        if bytes.is_empty() {
            tracing::warn!("asset response body was empty");
            return None;
        }
        Some(bytes)
    }
}

fn asset_client_builder() -> reqwest::ClientBuilder {
    // The URL and DNS checks below are the SSRF boundary for public article
    // media. An ambient HTTP(S)_PROXY/ALL_PROXY setting would otherwise move
    // destination resolution into an untrusted proxy and bypass SafeDnsResolver.
    Client::builder().no_proxy()
}

fn response_media_type(response: &Response) -> Option<String> {
    let value = response
        .headers()
        .get(header::CONTENT_TYPE)?
        .to_str()
        .ok()?;
    let media_type = value.split(';').next()?.trim().to_ascii_lowercase();
    matches!(
        media_type.as_str(),
        "image/png" | "image/jpeg" | "image/gif" | "image/webp"
    )
    .then_some(media_type)
}

fn signature_matches(media_type: &str, bytes: &[u8]) -> bool {
    match media_type {
        "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => bytes.starts_with(b"\xff\xd8\xff"),
        "image/gif" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "image/webp" => bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP",
        _ => false,
    }
}

fn validate_asset_url(url: &Url) -> Option<Url> {
    if !is_safe_asset_url(url) {
        return None;
    }
    let mut normalized = url.clone();
    normalized.set_fragment(None);
    Some(normalized)
}

fn is_safe_asset_url(url: &Url) -> bool {
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !is_allowed_asset_port(url)
    {
        return false;
    }
    let Some(host) = url.host() else {
        return false;
    };
    match host {
        Host::Ipv4(address) => !is_private_ip(IpAddr::V4(address)),
        Host::Ipv6(address) => !is_private_ip(IpAddr::V6(address)),
        Host::Domain(host) => {
            let host_lower = host.to_ascii_lowercase();
            host_lower != "localhost"
                && !host_lower.ends_with(".localhost")
                && !host_lower.ends_with(".local")
        }
    }
}

fn is_allowed_asset_port(url: &Url) -> bool {
    matches!(
        (url.scheme(), url.port()),
        ("http", None | Some(80)) | ("https", None | Some(443))
    )
}

fn can_follow_redirect(previous_count: usize, max_redirects: u32, url: &Url) -> bool {
    previous_count <= max_redirects as usize && is_safe_asset_url(url)
}

fn is_private_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_broadcast()
                || address.is_multicast()
                || ipv4_in_cidr(address, Ipv4Addr::new(0, 0, 0, 0), 8)
                || ipv4_in_cidr(address, Ipv4Addr::new(100, 64, 0, 0), 10)
                || ipv4_in_cidr(address, Ipv4Addr::new(192, 0, 0, 0), 24)
                || ipv4_in_cidr(address, Ipv4Addr::new(192, 0, 2, 0), 24)
                || ipv4_in_cidr(address, Ipv4Addr::new(198, 18, 0, 0), 15)
                || ipv4_in_cidr(address, Ipv4Addr::new(198, 51, 100, 0), 24)
                || ipv4_in_cidr(address, Ipv4Addr::new(203, 0, 113, 0), 24)
                || ipv4_in_cidr(address, Ipv4Addr::new(240, 0, 0, 0), 4)
        }
        IpAddr::V6(address) => {
            let segments = address.segments();
            mapped_ipv4(address).is_some_and(|mapped| is_private_ip(mapped.into()))
                || address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || (segments[0] & 0xfe00) == 0xfc00
                || (segments[0] & 0xffc0) == 0xfe80
                || ipv6_in_cidr(address, Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 0), 10)
                || ipv6_in_cidr(address, Ipv6Addr::new(0x2001, 0x0002, 0, 0, 0, 0, 0, 0), 48)
                || ipv6_in_cidr(address, Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0), 32)
                || ipv6_in_cidr(address, Ipv6Addr::new(0x2001, 0x0010, 0, 0, 0, 0, 0, 0), 28)
        }
    }
}

fn ipv4_in_cidr(address: Ipv4Addr, network: Ipv4Addr, prefix: u32) -> bool {
    let mask = if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    };
    u32::from(address) & mask == u32::from(network) & mask
}

fn ipv6_in_cidr(address: Ipv6Addr, network: Ipv6Addr, prefix: u32) -> bool {
    let mask = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    };
    u128::from(address) & mask == u128::from(network) & mask
}

fn mapped_ipv4(address: Ipv6Addr) -> Option<Ipv4Addr> {
    let segments = address.segments();
    (segments[..5].iter().all(|segment| *segment == 0) && segments[5] == u16::MAX).then(|| {
        Ipv4Addr::new(
            (segments[6] >> 8) as u8,
            segments[6] as u8,
            (segments[7] >> 8) as u8,
            segments[7] as u8,
        )
    })
}

/// Resolver used by the asset client to prevent a hostname from resolving to
/// an internal address after its URL-level validation has passed. Explicit
/// client test overrides still take precedence over this resolver.
#[derive(Debug, Clone, Copy)]
struct SafeDnsResolver;

impl reqwest::dns::Resolve for SafeDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let addresses = filter_resolved_addresses(addresses);
            if addresses.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "asset host resolved only to blocked addresses",
                )
                .into());
            }
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn filter_resolved_addresses(addresses: impl IntoIterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    addresses
        .into_iter()
        .filter(|address| !is_private_ip(address.ip()))
        .collect()
}

fn origin_for(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let host = url.host_str()?;
    let mut origin = format!("{}://{host}", url.scheme());
    if let Some(port) = url.port() {
        origin.push(':');
        origin.push_str(&port.to_string());
    }
    Some(origin)
}

#[cfg(test)]
mod tests {
    use std::{env, ffi::OsString, sync::Mutex, time::Duration};

    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    static PROXY_ENV_LOCK: Mutex<()> = Mutex::new(());
    const PROXY_ENV_NAMES: [&str; 8] = [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ];

    struct ProxyEnvironmentGuard {
        previous: Vec<(&'static str, Option<OsString>)>,
    }

    impl Drop for ProxyEnvironmentGuard {
        fn drop(&mut self) {
            for (name, value) in &self.previous {
                match value {
                    Some(value) => env::set_var(name, value),
                    None => env::remove_var(name),
                }
            }
        }
    }

    fn force_proxy_environment(proxy: &str) -> ProxyEnvironmentGuard {
        let previous = PROXY_ENV_NAMES
            .iter()
            .map(|name| (*name, env::var_os(name)))
            .collect();
        for name in PROXY_ENV_NAMES {
            env::remove_var(name);
        }
        env::set_var("HTTP_PROXY", proxy);
        ProxyEnvironmentGuard { previous }
    }

    #[test]
    fn derives_origin_without_path_or_credentials() {
        assert_eq!(
            origin_for("https://mp.weixin.qq.com/s/article?x=1"),
            Some("https://mp.weixin.qq.com".to_owned())
        );
    }

    #[test]
    fn rejects_private_local_and_credential_bearing_asset_urls() {
        for value in [
            "http://127.0.0.1/image.png",
            "http://10.0.0.1/image.png",
            "http://localhost/image.png",
            "https://user:password@cdn.example/image.png",
            "file:///tmp/image.png",
        ] {
            assert!(
                validate_asset_url(&Url::parse(value).unwrap()).is_none(),
                "{value}"
            );
        }
    }

    #[test]
    fn rejects_non_default_asset_ports() {
        for value in [
            "http://cdn.example:8080/image.png",
            "https://cdn.example:8443/image.png",
        ] {
            let url = Url::parse(value).unwrap();
            assert!(
                !is_safe_asset_url(&url),
                "unsafe port was accepted: {value}"
            );
        }
    }

    #[test]
    fn permits_the_configured_redirect_limit_at_the_boundary() {
        let url = Url::parse("https://cdn.example/image.png").unwrap();

        assert!(can_follow_redirect(5, 5, &url));
        assert!(!can_follow_redirect(6, 5, &url));
    }

    #[test]
    fn rejects_the_first_redirect_when_the_limit_is_zero() {
        let url = Url::parse("https://cdn.example/image.png").unwrap();

        assert!(!can_follow_redirect(1, 0, &url));
    }

    #[test]
    fn rejects_mapped_ipv4_and_multicast_addresses() {
        for address in [
            "http://[::ffff:127.0.0.1]/image.png",
            "http://[ff02::1]/image.png",
            "http://224.0.0.1/image.png",
            "http://100.64.0.1/image.png",
            "http://192.0.2.1/image.png",
            "http://198.18.0.1/image.png",
            "http://203.0.113.1/image.png",
            "http://[2001:db8::1]/image.png",
        ] {
            let url = Url::parse(address).unwrap();
            assert!(
                !is_safe_asset_url(&url),
                "unsafe address was accepted: {url}"
            );
        }
    }

    #[test]
    fn rejects_ipv6_site_local_and_benchmarking_addresses() {
        for address in [
            "http://[fec0::1]/image.png",
            "http://[feff::ffff]/image.png",
            "http://[2001:2::1]/image.png",
            "http://[2001:2:0:ffff::1]/image.png",
        ] {
            let url = Url::parse(address).unwrap();
            assert!(
                !is_safe_asset_url(&url),
                "unsafe address was accepted: {url}"
            );
        }
    }

    #[test]
    fn filters_private_addresses_returned_by_dns() {
        let addresses = filter_resolved_addresses([
            "127.0.0.1:80".parse().unwrap(),
            "10.0.0.1:80".parse().unwrap(),
            "100.64.0.1:80".parse().unwrap(),
            "198.18.0.1:80".parse().unwrap(),
            "224.0.0.1:80".parse().unwrap(),
            "[fec0::1]:80".parse().unwrap(),
            "[2001:2::1]:80".parse().unwrap(),
            "93.184.216.34:80".parse().unwrap(),
        ]);

        assert_eq!(addresses, ["93.184.216.34:80".parse().unwrap()]);
    }

    #[tokio::test]
    async fn asset_client_ignores_ambient_proxy_configuration() {
        let target_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("asset target listener should bind");
        let target_address = target_listener
            .local_addr()
            .expect("asset target listener should have an address");
        let proxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("asset proxy listener should bind");
        let proxy_address = proxy_listener
            .local_addr()
            .expect("asset proxy listener should have an address");

        let target = tokio::spawn(async move {
            let (mut stream, _) = target_listener
                .accept()
                .await
                .expect("asset target should receive a request");
            let mut request = [0_u8; 4096];
            let bytes_read = stream
                .read(&mut request)
                .await
                .expect("asset target request should be readable");
            assert!(bytes_read > 0, "asset target request should not be empty");
            let body = b"target";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("asset target response headers should be writable");
            stream
                .write_all(body)
                .await
                .expect("asset target response body should be writable");
            stream
                .shutdown()
                .await
                .expect("asset target connection should shut down cleanly");
        });
        let proxy = tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_millis(500), proxy_listener.accept()).await {
                Ok(Ok((mut stream, _))) => {
                    stream
                        .write_all(
                            b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .expect("proxy response should be writable");
                    true
                }
                _ => false,
            }
        });

        let client = {
            let _environment_lock = PROXY_ENV_LOCK.lock().expect("proxy environment lock");
            let _environment = force_proxy_environment(&format!("http://{proxy_address}"));
            asset_client_builder()
                .resolve("assets.example.test", target_address)
                .build()
                .expect("asset client should be constructible")
        };

        let response = client
            .get("http://assets.example.test/image.png")
            .send()
            .await
            .expect("asset client should reach the target directly");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), b"target");
        target.await.expect("asset target should finish");
        assert!(!proxy.await.expect("asset proxy should finish"));
    }

    #[test]
    fn accepts_only_matching_common_image_signatures() {
        assert!(signature_matches("image/png", b"\x89PNG\r\n\x1a\nbody"));
        assert!(!signature_matches("image/png", b"not png"));
        assert!(signature_matches("image/jpeg", b"\xff\xd8\xffbody"));
        assert!(signature_matches("image/gif", b"GIF89abody"));
        assert!(signature_matches("image/webp", b"RIFF1234WEBPbody"));
    }

    #[tokio::test]
    async fn fetches_an_image_with_article_context_without_cookies() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("asset test listener should bind");
        let address = listener
            .local_addr()
            .expect("asset test listener should have an address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("asset request should connect");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream
                    .read(&mut buffer)
                    .await
                    .expect("asset request should be readable");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request)
                .expect("asset request should be HTTP text")
                .to_ascii_lowercase();
            assert!(request.contains("referer: https://mp.weixin.qq.com/s/article\r\n"));
            assert!(request.contains("origin: https://mp.weixin.qq.com\r\n"));
            assert!(request.contains("user-agent: werrss-asset-test\r\n"));
            assert!(!request.contains("cookie:"));

            let body = b"\x89PNG\r\n\x1a\nserver-body";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png; charset=binary\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("asset response headers should be writable");
            stream
                .write_all(body)
                .await
                .expect("asset response body should be writable");
        });

        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("test asset client should be constructible");
        let policy = AssetCachePolicy::default();
        let service = AssetArchiveService::with_client_for_test(
            policy,
            Some("werrss-asset-test".to_owned()),
            client,
        );
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let url = Url::parse("http://assets.example.test/image.png")
            .expect("test asset URL should be valid");

        let assets = service.fetch_assets(&referer, &[url]).await;
        server.await.expect("asset test server should finish");

        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].media_type, "image/png");
        assert_eq!(assets[0].bytes, b"\x89PNG\r\n\x1a\nserver-body");
        assert_eq!(assets[0].occurrence, 0);
    }

    #[tokio::test]
    async fn stops_after_an_oversized_chunk_consumes_the_article_budget() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("asset budget listener should bind");
        let address = listener
            .local_addr()
            .expect("asset budget listener should have an address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("first asset request should connect");
            let mut request = [0_u8; 1024];
            let _ = stream
                .read(&mut request)
                .await
                .expect("first asset request should be readable");
            let body = b"\x89PNG\r\n\x1a\nbody-that-is-larger-than-the-budget";
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("asset budget response headers should be writable");
            stream
                .write_all(body)
                .await
                .expect("asset budget response body should be writable");

            assert!(
                tokio::time::timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err(),
                "the aggregate budget should prevent a second oversized request"
            );
        });

        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("test asset budget client should be constructible");
        let policy = AssetCachePolicy::new(
            0,
            Duration::from_secs(10),
            1024,
            10,
            1,
            Duration::from_secs(10),
            Duration::from_secs(2),
            2,
        )
        .expect("test asset budget policy should be valid");
        let service = AssetArchiveService::with_client_for_test(policy, None, client);
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let url = |path: &str| {
            Url::parse(&format!("http://assets.example.test/{path}"))
                .expect("test asset URL should be valid")
        };

        let assets = service
            .fetch_assets(&referer, &[url("first.png"), url("second.png")])
            .await;
        server
            .await
            .expect("asset budget test server should finish");

        assert!(assets.is_empty());
    }

    #[tokio::test]
    async fn enforces_the_article_deadline_across_headers_and_body_requests() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("asset deadline listener should bind");
        let address = listener
            .local_addr()
            .expect("asset deadline listener should have an address");
        let server = tokio::spawn(async move {
            for request_number in 0..2 {
                let (mut stream, _) = listener
                    .accept()
                    .await
                    .expect("asset deadline request should connect");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream
                        .read(&mut buffer)
                        .await
                        .expect("asset deadline request should be readable");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }

                if request_number == 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let body = b"\x89PNG\r\n\x1a\nfirst";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .await
                        .expect("first deadline response headers should be writable");
                    stream
                        .write_all(body)
                        .await
                        .expect("first deadline response body should be writable");
                } else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });

        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("test asset deadline client should be constructible");
        let policy = AssetCachePolicy::new(
            0,
            Duration::from_secs(10),
            1024,
            2,
            1024 * 1024,
            Duration::from_millis(150),
            Duration::from_millis(150),
            2,
        )
        .expect("test asset deadline policy should be valid");
        let service = AssetArchiveService::with_client_for_test(policy, None, client);
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let url = |path: &str| {
            Url::parse(&format!("http://assets.example.test/{path}"))
                .expect("test asset deadline URL should be valid")
        };

        let assets = tokio::time::timeout(
            Duration::from_millis(220),
            service.fetch_assets(&referer, &[url("first.png"), url("second.png")]),
        )
        .await
        .expect("the article deadline should stop the second request");
        server.abort();
        let _ = server.await;

        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].occurrence, 0);
    }

    #[tokio::test]
    async fn signed_source_urls_are_not_requested_by_best_effort_fetch() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("signed source fixture should bind");
        let address = listener
            .local_addr()
            .expect("signed source fixture should have an address");
        let server = tokio::spawn(async move {
            let mut requests = 0;
            while requests < 2 {
                let Ok(Ok((mut stream, _))) =
                    timeout(Duration::from_millis(200), listener.accept()).await
                else {
                    break;
                };
                requests += 1;
                let mut request = [0_u8; 1024];
                let _ = stream
                    .read(&mut request)
                    .await
                    .expect("signed source request should be readable");
                let body = b"\x89PNG\r\n\x1a\nvalid-signed-response";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("signed source response headers should be writable");
                stream
                    .write_all(body)
                    .await
                    .expect("signed source response should be writable");
            }
            requests
        });
        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("signed source client should be constructible");
        let service =
            AssetArchiveService::with_client_for_test(AssetCachePolicy::default(), None, client);
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let source_url =
            Url::parse("http://assets.example.test/image.png?X-Amz-Signature=signed-source-secret")
                .expect("signed source URL should be valid");

        let assets = service
            .fetch_assets(&referer, std::slice::from_ref(&source_url))
            .await;
        let requests = server.await.expect("signed source fixture should finish");

        assert!(assets.is_empty());
        assert_eq!(requests, 0);
    }

    #[tokio::test]
    async fn repair_fetch_rejects_successful_signed_source_without_exposing_it() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("signed repair fixture should bind");
        let address = listener
            .local_addr()
            .expect("signed repair fixture should have an address");
        let server = tokio::spawn(async move {
            for request_number in 0..2 {
                let (mut stream, _) = listener
                    .accept()
                    .await
                    .expect("signed repair request should connect");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream
                        .read(&mut buffer)
                        .await
                        .expect("signed repair request should be readable");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                if request_number == 0 {
                    stream
                        .write_all(b"HTTP/1.1 302 Found\r\nLocation: http://assets.example.test/final.png\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await
                        .expect("signed repair redirect should be writable");
                } else {
                    let body = b"\x89PNG\r\n\x1a\nvalid-signed-response";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .await
                        .expect("signed repair response headers should be writable");
                    stream
                        .write_all(body)
                        .await
                        .expect("signed repair response body should be writable");
                }
            }
        });
        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("signed repair client should be constructible");
        let service =
            AssetArchiveService::with_client_for_test(AssetCachePolicy::default(), None, client);
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let source_url =
            Url::parse("http://assets.example.test/image.png?X-Amz-Signature=signed-source-secret")
                .expect("signed source URL should be valid");

        let outcome = service
            .fetch_asset_for_repair(&referer, &source_url, None, None)
            .await;
        server.await.expect("signed repair fixture should finish");

        let RepairAssetFetchOutcome::Failed(failure) = outcome else {
            panic!("a successful signed source must not produce a persistent input");
        };
        assert_eq!(failure.kind, RepairAssetFetchFailureKind::UnsafeDestination);
        assert_eq!(failure.status, Some(200));
        let diagnostics = format!("{failure:?} {failure}");
        assert!(!diagnostics.contains("signed-source-secret"));
    }

    #[tokio::test]
    async fn repair_fetch_preserves_test_only_signed_url_expiry_classification() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("signed expiry fixture should bind");
        let address = listener
            .local_addr()
            .expect("signed expiry fixture should have an address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("signed expiry request should connect");
            let mut request = [0_u8; 1024];
            let _ = stream
                .read(&mut request)
                .await
                .expect("signed expiry request should be readable");
            let body = br#"{"code":"ExpiredToken","message":"expiry-secret"}"#;
            let response = format!(
                "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("signed expiry response headers should be writable");
            stream
                .write_all(body)
                .await
                .expect("signed expiry response body should be writable");
        });
        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("signed expiry client should be constructible");
        let service =
            AssetArchiveService::with_client_for_test(AssetCachePolicy::default(), None, client)
                .with_expiry_rule_for_test(
                    "assets.example.test",
                    StatusCode::FORBIDDEN,
                    "ExpiredToken",
                );
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let source_url =
            Url::parse("http://assets.example.test/image.png?X-Amz-Signature=signed-source-secret")
                .expect("signed source URL should be valid");

        let outcome = service
            .fetch_asset_for_repair(&referer, &source_url, None, None)
            .await;
        server.await.expect("signed expiry fixture should finish");

        assert!(matches!(
            outcome,
            RepairAssetFetchOutcome::Failed(RepairAssetFetchFailure {
                kind: RepairAssetFetchFailureKind::ConfirmedSignedUrlExpiry,
                status: Some(403)
            })
        ));
    }

    #[tokio::test]
    async fn successful_redirects_to_signed_urls_are_not_returned_by_either_fetch_path() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("signed redirect fixture should bind");
        let address = listener
            .local_addr()
            .expect("signed redirect fixture should have an address");
        let redirect_target =
            "http://assets.example.test/final.png?X-Amz-Signature=redirect-secret";
        let server = tokio::spawn(async move {
            let mut signed_responses = 0;
            for request_number in 0..4 {
                let accepted = timeout(Duration::from_millis(500), listener.accept()).await;
                let Ok(Ok((mut stream, _))) = accepted else {
                    break;
                };
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream
                        .read(&mut buffer)
                        .await
                        .expect("signed redirect request should be readable");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                if request_number % 2 == 0 {
                    let response = format!(
                        "HTTP/1.1 302 Found\r\nLocation: {redirect_target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    stream
                        .write_all(response.as_bytes())
                        .await
                        .expect("signed redirect response should be writable");
                } else {
                    signed_responses += 1;
                    let body = b"\x89PNG\r\n\x1a\nredirected-image";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .await
                        .expect("signed response headers should be writable");
                    stream
                        .write_all(body)
                        .await
                        .expect("signed response body should be writable");
                }
            }
            signed_responses
        });
        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("signed redirect client should be constructible");
        let service =
            AssetArchiveService::with_client_for_test(AssetCachePolicy::default(), None, client);
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let source_url = Url::parse("http://assets.example.test/image.png")
            .expect("unsigned source URL should be valid");

        let assets = service
            .fetch_assets(&referer, std::slice::from_ref(&source_url))
            .await;
        let repair = service
            .fetch_asset_for_repair(&referer, &source_url, None, None)
            .await;
        let signed_responses = server.await.expect("signed redirect fixture should finish");

        assert!(assets.is_empty());
        let RepairAssetFetchOutcome::Failed(failure) = repair else {
            panic!("a signed final URL must not produce a persistent input");
        };
        assert_eq!(failure.kind, RepairAssetFetchFailureKind::UnsafeDestination);
        assert_eq!(failure.status, Some(200));
        let diagnostics = format!("{failure:?} {failure}");
        assert!(!diagnostics.contains("redirect-secret"));
        assert_eq!(signed_responses, 2);
    }

    #[tokio::test]
    async fn repair_fetch_returns_validated_asset_input() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("repair success listener should bind");
        let address = listener
            .local_addr()
            .expect("repair success listener should have an address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("repair success request should connect");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream
                    .read(&mut buffer)
                    .await
                    .expect("repair success request should be readable");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let body = b"\x89PNG\r\n\x1a\nvalidated-image";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("repair success headers should be writable");
            stream
                .write_all(body)
                .await
                .expect("repair success body should be writable");
        });
        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("repair success client should be constructible");
        let service = AssetArchiveService::with_client_for_test(
            AssetCachePolicy::default(),
            Some("repair-test-agent".to_owned()),
            client,
        );
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let source_url = Url::parse("http://assets.example.test/image.png")
            .expect("repair source URL should be valid");

        let outcome = service
            .fetch_asset_for_repair(&referer, &source_url, None, None)
            .await;
        server.await.expect("repair success server should finish");

        let RepairAssetFetchOutcome::Fetched(input) = outcome else {
            panic!("a valid image response should produce an asset input");
        };
        assert_eq!(input.source_url, source_url);
        assert_eq!(input.final_url, source_url);
        assert_eq!(input.media_type, "image/png");
        assert_eq!(input.bytes, b"\x89PNG\r\n\x1a\nvalidated-image");
        assert_eq!(input.occurrence, 0);
        assert_eq!(input.user_agent.as_deref(), Some("repair-test-agent"));
    }

    #[tokio::test]
    async fn repair_fetch_rejects_unsafe_redirect_destination() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("unsafe redirect listener should bind");
        let address = listener
            .local_addr()
            .expect("unsafe redirect listener should have an address");
        let redirect_target = format!(
            "http://127.0.0.1:{}/image?signature=redirect-secret",
            address.port()
        );
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("unsafe redirect request should connect");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream
                    .read(&mut buffer)
                    .await
                    .expect("unsafe redirect request should be readable");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: {redirect_target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("unsafe redirect response should be writable");
        });
        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .redirect(Policy::custom(|attempt| {
                if can_follow_redirect(attempt.previous().len(), 5, attempt.url()) {
                    attempt.follow()
                } else {
                    attempt.stop()
                }
            }))
            .build()
            .expect("unsafe redirect client should be constructible");
        let service =
            AssetArchiveService::with_client_for_test(AssetCachePolicy::default(), None, client);
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let source_url = Url::parse("http://assets.example.test/image.png?signature=source-secret")
            .expect("repair source URL should be valid");

        let outcome = service
            .fetch_asset_for_repair(&referer, &source_url, None, None)
            .await;
        server.await.expect("unsafe redirect server should finish");

        let RepairAssetFetchOutcome::Failed(failure) = outcome else {
            panic!("an unsafe redirect target must not be fetched");
        };
        assert_eq!(failure.kind, RepairAssetFetchFailureKind::UnsafeDestination);
        assert_eq!(failure.status, Some(302));
        let diagnostics = format!("{failure:?} {failure}");
        assert!(!diagnostics.contains("source-secret"));
        assert!(!diagnostics.contains("redirect-secret"));
    }

    #[tokio::test]
    async fn repair_fetch_returns_safe_status_only_failure() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("repair test listener should bind");
        let address = listener
            .local_addr()
            .expect("repair test listener should have an address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("repair request should connect");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream
                    .read(&mut buffer)
                    .await
                    .expect("repair request should be readable");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let body = br#"{"code":"ExpiredToken","message":"secret-response-body"}"#;
            let response = format!(
                "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("repair response headers should be writable");
            stream
                .write_all(body)
                .await
                .expect("repair response body should be writable");
        });
        let client = Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .expect("repair test client should be constructible");
        let service =
            AssetArchiveService::with_client_for_test(AssetCachePolicy::default(), None, client);
        let referer = VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/article")
            .expect("test referer should be valid");
        let url = Url::parse("http://assets.example.test/image?signature=secret-signed-url")
            .expect("repair test URL should be valid");

        let outcome = service
            .fetch_asset_for_repair(&referer, &url, None, None)
            .await;
        server.await.expect("repair test server should finish");

        let RepairAssetFetchOutcome::Failed(failure) = outcome else {
            panic!("403 without exact provider evidence must fail");
        };
        assert_eq!(failure.kind, RepairAssetFetchFailureKind::Http);
        assert_eq!(failure.status, Some(403));
        let diagnostics = format!("{failure:?} {failure}");
        assert!(!diagnostics.contains("secret-signed-url"));
        assert!(!diagnostics.contains("secret-response-body"));
    }

    #[tokio::test]
    async fn response_expiry_classifier_matches_only_exact_bounded_provider_evidence() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("expiry fixture listener should bind");
        let address = listener
            .local_addr()
            .expect("expiry fixture listener should have an address");
        let server = tokio::spawn(async move {
            for request_number in 0..2 {
                let (mut stream, _) = listener
                    .accept()
                    .await
                    .expect("expiry fixture request should connect");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream
                        .read(&mut buffer)
                        .await
                        .expect("expiry fixture request should be readable");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let body = if request_number == 0 {
                    br#"{"code":"ExpiredToken"}"#.to_vec()
                } else {
                    vec![b' '; MAX_EXPIRY_EVIDENCE_BYTES + 8]
                };
                let length = if request_number == 0 {
                    format!("Content-Length: {}\r\n", body.len())
                } else {
                    String::new()
                };
                let response = format!(
                    "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\n{length}Connection: close\r\n\r\n"
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("expiry fixture headers should be writable");
                stream
                    .write_all(&body)
                    .await
                    .expect("expiry fixture body should be writable");
            }
        });
        let client = Client::builder()
            .no_proxy()
            .resolve("cdn.example.test", address)
            .build()
            .expect("expiry fixture client should be constructible");
        let response = client
            .get("http://cdn.example.test/image")
            .send()
            .await
            .expect("expiry fixture response should arrive");
        let rules = [ProviderExpiryRule {
            host: "cdn.example.test",
            status: reqwest::StatusCode::FORBIDDEN,
            code: "ExpiredToken",
        }];

        let expired = response_has_expiry_evidence(
            response,
            Instant::now(),
            Duration::from_secs(2),
            Duration::from_secs(2),
            &rules,
        )
        .await
        .expect("response body should be read within the configured budget");
        let oversized = response_has_expiry_evidence(
            client
                .get("http://cdn.example.test/image")
                .send()
                .await
                .expect("second expiry fixture response should arrive"),
            Instant::now(),
            Duration::from_secs(2),
            Duration::from_secs(2),
            &rules,
        )
        .await
        .expect("oversized evidence should be rejected without a body-read error");
        server.await.expect("expiry fixture server should finish");

        assert!(expired);
        assert!(!oversized);
    }

    #[test]
    fn signed_url_expiry_requires_injected_provider_status_and_machine_code() {
        let url = Url::parse("https://cdn.example.test/image").unwrap();
        let rules = [ProviderExpiryRule {
            host: "cdn.example.test",
            status: reqwest::StatusCode::FORBIDDEN,
            code: "ExpiredToken",
        }];
        let evidence = br#"{"code":"ExpiredToken"}"#;

        assert!(classify_expiry_evidence(
            &url,
            reqwest::StatusCode::FORBIDDEN,
            evidence,
            &rules
        ));
        assert!(!classify_expiry_evidence(
            &url,
            reqwest::StatusCode::FORBIDDEN,
            b"",
            &rules
        ));
        assert!(!classify_expiry_evidence(
            &Url::parse("https://other.example.test/image").unwrap(),
            reqwest::StatusCode::FORBIDDEN,
            evidence,
            &rules
        ));
        assert!(!classify_expiry_evidence(
            &url,
            reqwest::StatusCode::NOT_FOUND,
            evidence,
            &rules
        ));
        assert!(!classify_expiry_evidence(
            &url,
            reqwest::StatusCode::FORBIDDEN,
            b"AccessDenied",
            &rules
        ));
        assert!(!classify_expiry_evidence(
            &url,
            reqwest::StatusCode::FORBIDDEN,
            br#"{"error":{"code":"ExpiredToken"}}"#,
            &rules
        ));
        assert!(!classify_expiry_evidence(
            &url,
            reqwest::StatusCode::FORBIDDEN,
            &vec![b' '; MAX_EXPIRY_EVIDENCE_BYTES + 1],
            &rules
        ));
        assert!(PRODUCTION_EXPIRY_RULES.is_empty());
        assert!(!classify_expiry_evidence(
            &url,
            reqwest::StatusCode::FORBIDDEN,
            evidence,
            PRODUCTION_EXPIRY_RULES
        ));
    }
}
