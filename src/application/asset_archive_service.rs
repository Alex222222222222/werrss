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

use reqwest::{header, redirect::Policy, Client, Response};
use thiserror::Error;
use tokio::time::{timeout, Instant};
use url::{Host, Url};

use crate::{
    archive::asset_store::{AssetCachePolicy, AssetInput},
    domain::source::VerifiedWechatArticleUrl,
};

/// Errors raised while constructing the asset HTTP client.
#[derive(Debug, Error)]
pub enum AssetArchiveServiceError {
    /// Reqwest could not construct a client from the validated policy.
    #[error("asset HTTP client could not be constructed: {0}")]
    Client(String),
}

/// Bounded, anonymous asset fetcher used before article persistence.
#[derive(Clone)]
pub struct AssetArchiveService {
    client: Client,
    policy: AssetCachePolicy,
    user_agent: Option<String>,
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
        }
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
                Ok(Err(error)) => {
                    tracing::warn!(
                        occurrence,
                        host = ?source_url.host_str(),
                        error = %error,
                        "asset request failed"
                    );
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
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "asset response body exceeded the article fetch time budget"
                    );
                    return None;
                }
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "asset response body could not be read");
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
}
