//! Thirtyfour/WebDriver adapter boundary.
//!
//! This module encapsulates connections to Chromium/ChromeDriver or
//! Firefox/GeckoDriver, navigation, waits, DOM/script evaluation, page-source
//! capture, and browser-session cleanup. Thirtyfour and the sidecar endpoint
//! remain private implementation details; application services receive
//! capability-specific session types instead of a general driver handle.
//!
//! [`PublicBrowserSession`] is a clean, non-cloneable capability for public
//! WeChat article pages. It owns one local browser-pool permit and has no API
//! for cookies, storage import, credentials, or account leases.
//! [`AuthenticatedBrowserSession`] is a separate non-cloneable capability for
//! WeRead account/list operations. It carries one account lease guard and an
//! authenticated WebDriver client, and cannot be converted into a public
//! session. Dropping either session releases only local capacity;
//! authenticated callers should explicitly call `release()` so the durable
//! account lease and remote browser session are cleaned up promptly.
//!
//! The concrete public adapter now connects a fresh WebDriver session and
//! exposes only safe navigation, current-URL, source, bounded scrolling, close,
//! and environment diagnostic operations to sibling acquisition code. It does
//! not expose cookies, storage, raw protocol commands, or an authenticated
//! session. Authenticated sessions receive credentials from the WeRead adapter
//! through the admin-enrolled credential provider. The factory asks the
//! browser's `Intl` implementation to
//! canonicalize the configured IANA timezone and compares that result with the
//! browser diagnostic when the profile declares an expected timezone; public
//! article pacing and bounded scroll execution are coordinated by
//! [`super::pacing`].
//! Browser timeouts, sidecar loss, verification pages, and session loss must
//! map to typed acquisition errors. Browser failures must not prevent API
//! replicas from serving persisted RSS cache bytes.
//!
//! The concrete session lifecycle, profile application, browser timezone
//! validation, environment diagnostic, sidecar health probe, and low-level
//! scroll operation are implemented here; article extraction and pacing
//! orchestration remain in [`super::article_page`] and [`super::pacing`].

use crate::config::BrowserEngine;
use chrono_tz::Tz;
use serde::Deserialize;
#[cfg(test)]
use serde_json::{json, Value};
use thirtyfour::common::capabilities::firefox::FirefoxPreferences;
use thirtyfour::prelude::{ChromiumLikeCapabilities, DesiredCapabilities};
use thirtyfour::{Capabilities, Cookie, WebDriver};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use super::browser_pool::{
    AccountLeaseError, AccountLeaseGuard, AccountLeaseHeartbeat, AccountLeaseStore, BrowserPool,
    BrowserPoolError,
};

const WEBDRIVER_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
const WEBDRIVER_PAGE_LOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const WEBDRIVER_CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const WEBDRIVER_HEALTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const WEBDRIVER_HEALTH_SESSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Errors raised while creating or operating a WebDriver session.
#[derive(Debug, Error)]
pub enum WebDriverError {
    /// The process-local browser capacity could not be acquired.
    #[error(transparent)]
    Pool(#[from] BrowserPoolError),
    /// The sidecar rejected or failed a new browser session.
    #[error("WebDriver session could not be created: {0}")]
    Connect(String),
    /// An existing browser session rejected a command or disconnected.
    #[error("WebDriver command failed: {0}")]
    Command(String),
    /// A command was requested on a pool capability that has no client.
    #[error("browser session is not connected to WebDriver")]
    NotConnected,
    /// The browser-visible environment did not match the configured profile.
    #[error("browser environment field {field} mismatch: expected {expected:?}, got {actual:?}")]
    EnvironmentMismatch {
        /// Profile field that failed validation.
        field: &'static str,
        /// Configured value.
        expected: String,
        /// Browser-reported value.
        actual: String,
    },
}

/// Configuration for one private WebDriver sidecar endpoint.
#[derive(Debug, Clone)]
pub struct WebDriverFactory {
    endpoint: Url,
    engine: BrowserEngine,
    profile: BrowserProfile,
}

/// Browser settings that are applied consistently to one fresh session.
///
/// The values are deliberately explicit so a real-browser diagnostic can
/// compare the effective User-Agent, viewport, locale, timezone, and browser
/// arguments as one profile. If `expected_timezone` is set, session creation
/// validates the browser-reported timezone; the browser sidecar must still set
/// its own `TZ` and timezone data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserProfile {
    /// Optional page User-Agent override. `None` preserves the browser default.
    pub user_agent: Option<String>,
    /// Requested outer browser window dimensions.
    pub viewport: BrowserViewport,
    /// Browser locale used for language negotiation.
    pub locale: String,
    /// Optional browser-visible timezone expected from the sidecar.
    pub expected_timezone: Option<Tz>,
    /// Additional operator-selected browser arguments.
    pub extra_args: Vec<String>,
}

impl Default for BrowserProfile {
    fn default() -> Self {
        Self {
            user_agent: None,
            viewport: BrowserViewport::new(1_280, 2_000),
            locale: "zh-CN".to_owned(),
            expected_timezone: None,
            extra_args: Vec::new(),
        }
    }
}

impl WebDriverError {
    /// Returns a bounded message safe to propagate into application errors
    /// and logs. Thirtyfour error values may contain the complete response
    /// body from the WebDriver sidecar.
    pub(crate) fn safe_message(&self) -> String {
        match self {
            Self::Pool(_) => "browser pool operation failed".to_owned(),
            Self::Connect(_) => "WebDriver session could not be created".to_owned(),
            Self::Command(_) => "WebDriver command failed".to_owned(),
            Self::NotConnected => "browser session is not connected to WebDriver".to_owned(),
            Self::EnvironmentMismatch { field, .. } => {
                format!("browser environment field {field} did not match the configured profile")
            }
        }
    }

    /// Returns a stable category suitable for diagnostic log fields.
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::Pool(_) => "pool",
            Self::Connect(_) => "connect",
            Self::Command(_) => "command",
            Self::NotConnected => "not_connected",
            Self::EnvironmentMismatch { .. } => "environment_mismatch",
        }
    }
}

/// Requested browser window dimensions in CSS pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrowserViewport {
    /// Requested outer window width.
    pub width: u32,
    /// Requested outer window height.
    pub height: u32,
}

impl BrowserViewport {
    /// Creates viewport dimensions. Configuration validation happens before
    /// the profile reaches the WebDriver adapter.
    pub const fn new(width: u32, height: u32) -> Self {
        Self { width, height }
    }
}

/// Browser-visible values captured by an opt-in integration diagnostic.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BrowserEnvironment {
    /// Effective page User-Agent.
    #[serde(rename = "userAgent")]
    pub user_agent: String,
    /// Effective primary locale.
    pub language: String,
    /// Effective ordered language list.
    pub languages: Vec<String>,
    /// IANA timezone reported by `Intl`.
    pub timezone: String,
    /// WebDriver exposure, when the browser reports it.
    pub webdriver: Option<bool>,
    /// Effective inner viewport width. The driver may report a value smaller
    /// than the requested outer window width.
    #[serde(rename = "innerWidth")]
    pub inner_width: u32,
    /// Effective inner viewport height. The driver may report a value smaller
    /// than the requested outer window height.
    #[serde(rename = "innerHeight")]
    pub inner_height: u32,
}

/// Result of probing the sidecar status endpoint and one fresh browser
/// session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebDriverHealthCheck {
    /// The sidecar answered but reported that it cannot accept sessions.
    NotReady,
    /// The local browser pool is busy with real work, so this probe was
    /// skipped without changing the previous health state.
    Busy,
    /// The sidecar accepted a session and its browser environment was read.
    Ready {
        /// Values observed from the fresh probe session.
        environment: BrowserEnvironment,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WebDriverSidecarStatus {
    Ready,
    Busy,
    NotReady,
}

impl WebDriverFactory {
    /// Creates a factory for the configured browser sidecar.
    pub fn new(endpoint: Url, engine: BrowserEngine) -> Self {
        Self {
            endpoint,
            engine,
            profile: BrowserProfile::default(),
        }
    }

    /// Replaces the default browser profile with an explicit diagnostic or
    /// runtime profile.
    pub fn with_profile(mut self, mut profile: BrowserProfile) -> Self {
        if profile.locale.is_empty() {
            profile.locale = "zh-CN".to_owned();
        }
        self.profile = profile;
        self
    }

    /// Probes the sidecar without exposing its status endpoint to callers.
    ///
    /// The status request avoids creating a session when Selenium is still
    /// starting. Once it reports ready, a short-lived public session verifies
    /// the browser-visible profile (including the configured timezone) before
    /// the session is closed again. No account lease or credential is involved.
    pub async fn health_check(
        &self,
        pool: &BrowserPool,
    ) -> Result<WebDriverHealthCheck, WebDriverError> {
        match self.sidecar_status().await? {
            WebDriverSidecarStatus::NotReady => return Ok(WebDriverHealthCheck::NotReady),
            WebDriverSidecarStatus::Busy => return Ok(WebDriverHealthCheck::Busy),
            WebDriverSidecarStatus::Ready => {}
        }

        let Some(session) = self
            .try_open_public_with_timeout(pool, WEBDRIVER_HEALTH_SESSION_TIMEOUT)
            .await?
        else {
            return Ok(WebDriverHealthCheck::Busy);
        };
        let environment = session.environment().await;
        let close = session.close().await;
        match (environment, close) {
            (Ok(environment), Ok(())) => Ok(WebDriverHealthCheck::Ready { environment }),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    /// Returns the expected timezone used by health-monitor classification.
    pub(crate) const fn expected_timezone(&self) -> Option<Tz> {
        self.profile.expected_timezone
    }

    /// Opens a clean public-page browser session using one pool permit.
    ///
    /// If WebDriver session creation or the configured browser-environment
    /// validation fails, the partially acquired pool capability is dropped
    /// before the error is returned. No account lease is involved in this
    /// path.
    pub async fn open_public(
        &self,
        pool: &BrowserPool,
    ) -> Result<PublicBrowserSession, WebDriverError> {
        self.open_public_with_timeout(pool, WEBDRIVER_REQUEST_TIMEOUT)
            .await
    }

    async fn open_public_with_timeout(
        &self,
        pool: &BrowserPool,
        request_timeout: std::time::Duration,
    ) -> Result<PublicBrowserSession, WebDriverError> {
        tracing::debug!(engine = ?self.engine, endpoint_host = ?self.endpoint.host_str(), "opening public WebDriver session");
        let session = pool.open_public().await?;
        self.attach_public_client(session, request_timeout).await
    }

    async fn try_open_public_with_timeout(
        &self,
        pool: &BrowserPool,
        request_timeout: std::time::Duration,
    ) -> Result<Option<PublicBrowserSession>, WebDriverError> {
        tracing::debug!(engine = ?self.engine, endpoint_host = ?self.endpoint.host_str(), "trying to open public WebDriver health session");
        let Some(session) = pool.try_open_public().await? else {
            tracing::debug!("skipping WebDriver health session while browser capacity is busy");
            return Ok(None);
        };
        self.attach_public_client(session, request_timeout)
            .await
            .map(Some)
    }

    async fn attach_public_client(
        &self,
        session: PublicBrowserSession,
        request_timeout: std::time::Duration,
    ) -> Result<PublicBrowserSession, WebDriverError> {
        let client = match connect(
            self.endpoint.clone(),
            self.engine,
            &self.profile,
            request_timeout,
        )
        .await
        {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(engine = ?self.engine, error_kind = "connect", "unable to create public WebDriver session");
                return Err(WebDriverError::Connect(error));
            }
        };
        if let Err(error) = self.validate_environment(&client).await {
            tracing::warn!(engine = ?self.engine, error_kind = error.kind(), "public WebDriver environment validation failed");
            if let Err(_cleanup_error) = close_webdriver(client).await {
                tracing::warn!(
                    error_kind = "cleanup",
                    "browser cleanup failed after environment validation error"
                );
            }
            return Err(error);
        }
        tracing::info!(session_id = %session.session_id(), engine = ?self.engine, "public WebDriver session opened");
        Ok(session.attach_client(client))
    }

    /// Opens an authenticated browser session fenced by one account lease.
    ///
    /// The authenticated protocol adapter injects the account cookie after
    /// the session is created. The lease is acquired before WebDriver creation
    /// and released again if session creation fails, so a broken sidecar cannot
    /// strand account ownership until expiry.
    pub async fn open_authenticated<R>(
        &self,
        pool: &BrowserPool,
        leases: R,
        account_id: crate::domain::credentials::WeReadAccountId,
        owner: &str,
        lease_for: chrono::Duration,
    ) -> Result<Option<AuthenticatedBrowserSession<R>>, WebDriverError>
    where
        R: AccountLeaseStore,
    {
        tracing::debug!(account_id = %account_id, engine = ?self.engine, endpoint_host = ?self.endpoint.host_str(), "opening authenticated WebDriver session");
        let Some(session) = pool
            .open_authenticated(leases, account_id, owner, lease_for)
            .await?
        else {
            return Ok(None);
        };
        let client = match connect(
            self.endpoint.clone(),
            self.engine,
            &self.profile,
            WEBDRIVER_REQUEST_TIMEOUT,
        )
        .await
        {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(account_id = %account_id, engine = ?self.engine, error_kind = "connect", "unable to create authenticated WebDriver session");
                let _ = session.release().await;
                return Err(WebDriverError::Connect(error));
            }
        };
        if let Err(error) = self.validate_environment(&client).await {
            tracing::warn!(account_id = %account_id, error_kind = error.kind(), "authenticated WebDriver environment validation failed");
            if let Err(_cleanup_error) = close_webdriver(client).await {
                tracing::warn!(
                    error_kind = "cleanup",
                    "browser cleanup failed after authenticated environment validation error"
                );
            }
            let _ = session.release().await;
            return Err(error);
        }
        tracing::info!(session_id = %session.session_id(), account_id = %account_id, engine = ?self.engine, "authenticated WebDriver session opened");
        Ok(Some(session.attach_client(client)))
    }

    async fn validate_environment(&self, client: &WebDriver) -> Result<(), WebDriverError> {
        let Some(expected_timezone) = self.profile.expected_timezone else {
            return Ok(());
        };
        let environment = browser_environment(client).await?;
        let canonical_expected_timezone = canonical_timezone(client, expected_timezone).await?;
        validate_expected_environment(&canonical_expected_timezone, &environment)
    }

    async fn sidecar_status(&self) -> Result<WebDriverSidecarStatus, WebDriverError> {
        let status_url = webdriver_status_url(&self.endpoint)?;
        let client = reqwest::Client::builder()
            .timeout(WEBDRIVER_HEALTH_TIMEOUT)
            .build()
            .map_err(|_| WebDriverError::Connect("WebDriver health probe failed".to_owned()))?;
        let response = client
            .get(status_url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| WebDriverError::Connect("WebDriver health probe failed".to_owned()))?;
        if !response.status().is_success() {
            return Err(WebDriverError::Connect(
                "WebDriver health probe returned an unsuccessful status".to_owned(),
            ));
        }
        let payload = response
            .json::<serde_json::Value>()
            .await
            .map_err(|_| WebDriverError::Connect("WebDriver health probe failed".to_owned()))?;
        parse_webdriver_status(&payload)
    }
}

fn webdriver_status_url(endpoint: &Url) -> Result<Url, WebDriverError> {
    endpoint
        .join("/status")
        .map_err(|_| WebDriverError::Connect("WebDriver health probe failed".to_owned()))
}

fn parse_webdriver_status(
    payload: &serde_json::Value,
) -> Result<WebDriverSidecarStatus, WebDriverError> {
    let value = payload.get("value").unwrap_or(payload);
    let ready = value
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| WebDriverError::Connect("invalid WebDriver health response".to_owned()))?;
    if ready {
        return Ok(WebDriverSidecarStatus::Ready);
    }

    if value
        .get("nodes")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|nodes| {
            let is_up = |node: &serde_json::Value| {
                node.get("availability").and_then(serde_json::Value::as_str) == Some("UP")
            };
            nodes.iter().any(is_up)
                && nodes.iter().filter(|node| is_up(node)).all(|node| {
                    node.get("slots")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|slots| {
                            !slots.is_empty()
                                && slots.iter().all(|slot| {
                                    slot.get("session")
                                        .is_some_and(|session| !session.is_null())
                                })
                        })
                })
        })
    {
        return Ok(WebDriverSidecarStatus::Busy);
    }

    Ok(WebDriverSidecarStatus::NotReady)
}

fn validate_expected_environment(
    expected_timezone: &str,
    environment: &BrowserEnvironment,
) -> Result<(), WebDriverError> {
    if environment.timezone != expected_timezone {
        return Err(WebDriverError::EnvironmentMismatch {
            field: "timezone",
            expected: expected_timezone.to_owned(),
            actual: environment.timezone.clone(),
        });
    }
    Ok(())
}

async fn connect(
    endpoint: Url,
    engine: BrowserEngine,
    profile: &BrowserProfile,
    request_timeout: std::time::Duration,
) -> Result<WebDriver, String> {
    tracing::trace!(engine = ?engine, endpoint_host = ?endpoint.host_str(), "connecting to WebDriver sidecar");
    let capabilities = capabilities_for(engine, profile)?;
    let driver = WebDriver::builder(endpoint.as_str(), capabilities)
        .request_timeout(request_timeout)
        .connect()
        .await
        .map_err(|error| error.to_string())?;
    for result in [
        driver
            .set_page_load_timeout(WEBDRIVER_PAGE_LOAD_TIMEOUT)
            .await,
        driver
            .set_window_rect(0, 0, profile.viewport.width, profile.viewport.height)
            .await,
    ] {
        if let Err(error) = result {
            let message = error.to_string();
            tracing::warn!(engine = ?engine, error_kind = "command", "WebDriver session initialization command failed");
            let _ = close_webdriver(driver).await;
            return Err(message);
        }
    }
    tracing::debug!(engine = ?engine, "connected to WebDriver sidecar");
    Ok(driver)
}

async fn browser_environment(client: &WebDriver) -> Result<BrowserEnvironment, WebDriverError> {
    let result = client
        .execute(
            r#"return {
                userAgent: navigator.userAgent,
                language: navigator.language,
                languages: Array.from(navigator.languages || []),
                timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "",
                webdriver: navigator.webdriver === undefined ? null : navigator.webdriver,
                innerWidth: window.innerWidth,
                innerHeight: window.innerHeight
            };"#,
            Vec::new(),
        )
        .await
        .map_err(|error| WebDriverError::Command(error.to_string()))?;
    result
        .convert()
        .map_err(|error| WebDriverError::Command(error.to_string()))
}

async fn canonical_timezone(client: &WebDriver, timezone: Tz) -> Result<String, WebDriverError> {
    let result = client
        .execute(
            r#"return new Intl.DateTimeFormat("en-US", { timeZone: arguments[0] })
                .resolvedOptions().timeZone;"#,
            vec![serde_json::json!(timezone.name())],
        )
        .await
        .map_err(|error| WebDriverError::Command(error.to_string()))?;
    result
        .convert()
        .map_err(|error| WebDriverError::Command(error.to_string()))
}

#[derive(Debug, Deserialize)]
struct BrowserFetchTextResult {
    body: Option<String>,
    error: Option<String>,
}

fn capabilities_for(
    engine: BrowserEngine,
    profile: &BrowserProfile,
) -> Result<Capabilities, String> {
    validate_profile(profile)?;
    match engine {
        BrowserEngine::Chromium => {
            let mut capabilities = DesiredCapabilities::chrome();
            for argument in [
                "--headless=new",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                &format!(
                    "--window-size={},{}",
                    profile.viewport.width, profile.viewport.height
                ),
                &format!("--lang={}", profile.locale),
            ] {
                capabilities
                    .add_arg(argument)
                    .map_err(|error| error.to_string())?;
            }
            if let Some(user_agent) = &profile.user_agent {
                capabilities
                    .add_arg(&format!("--user-agent={user_agent}"))
                    .map_err(|error| error.to_string())?;
            }
            for argument in &profile.extra_args {
                capabilities
                    .add_arg(argument)
                    .map_err(|error| error.to_string())?;
            }
            Ok(capabilities.into())
        }
        BrowserEngine::Firefox => {
            let mut capabilities = DesiredCapabilities::firefox();
            capabilities
                .add_arg("-headless")
                .map_err(|error| error.to_string())?;
            let mut preferences = FirefoxPreferences::new();
            preferences
                .set("intl.accept_languages", &profile.locale)
                .map_err(|error| error.to_string())?;
            preferences
                .set("intl.locale.requested", &profile.locale)
                .map_err(|error| error.to_string())?;
            if let Some(user_agent) = &profile.user_agent {
                preferences
                    .set("general.useragent.override", user_agent)
                    .map_err(|error| error.to_string())?;
            }
            capabilities
                .set_preferences(preferences)
                .map_err(|error| error.to_string())?;
            for argument in &profile.extra_args {
                capabilities
                    .add_arg(argument)
                    .map_err(|error| error.to_string())?;
            }
            Ok(capabilities.into())
        }
    }
}

fn validate_profile(profile: &BrowserProfile) -> Result<(), String> {
    if profile.viewport.width == 0 || profile.viewport.height == 0 {
        return Err("viewport dimensions must be positive".to_owned());
    }
    if profile.locale.trim().is_empty()
        || profile
            .locale
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err("locale must be a non-empty token without whitespace".to_owned());
    }
    if profile.user_agent.as_deref().is_some_and(|user_agent| {
        user_agent.trim().is_empty() || user_agent.chars().any(char::is_control)
    }) {
        return Err("user agent must be non-empty and free of control characters".to_owned());
    }
    if profile.extra_args.iter().any(|argument| {
        browser_argument_name(argument).is_none_or(|name| {
            matches!(
                name,
                "--lang"
                    | "--user-agent"
                    | "--window-size"
                    | "--user-data-dir"
                    | "--headless"
                    | "-headless"
                    | "-profile"
            )
        })
    }) {
        return Err(
            "extra arguments must not override controlled browser profile arguments".to_owned(),
        );
    }
    Ok(())
}

fn browser_argument_name(argument: &str) -> Option<&str> {
    argument
        .split_once('=')
        .map_or(Some(argument), |(name, _)| {
            (!name.is_empty()).then_some(name)
        })
}

/// A clean, unauthenticated public-page browser session.
pub struct PublicBrowserSession {
    _permit: tokio::sync::OwnedSemaphorePermit,
    session_id: Uuid,
    client: Option<WebDriver>,
}

impl PublicBrowserSession {
    pub(crate) fn from_permit(permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        Self {
            _permit: permit,
            session_id: Uuid::new_v4(),
            client: None,
        }
    }

    fn attach_client(mut self, client: WebDriver) -> Self {
        self.client = Some(client);
        self
    }

    /// Returns a non-secret identifier useful for tracing one local session.
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Navigates the clean session to a validated public-page destination.
    pub(crate) async fn goto(&self, url: &str) -> Result<(), WebDriverError> {
        tracing::trace!(session_id = %self.session_id, "navigating public browser session");
        self.client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .goto(url)
            .await
            .map_err(|error| {
                tracing::warn!(session_id = %self.session_id, error_kind = "command", "public browser navigation failed");
                WebDriverError::Command(error.to_string())
            })
    }

    /// Returns the browser-observed URL after navigation and redirects.
    pub(crate) async fn current_url(&self) -> Result<Url, WebDriverError> {
        self.client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .current_url()
            .await
            .map_err(|error| WebDriverError::Command(error.to_string()))
    }

    /// Captures the current rendered page source.
    pub(crate) async fn source(&self) -> Result<String, WebDriverError> {
        tracing::trace!(session_id = %self.session_id, "capturing public browser page source");
        self.client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .source()
            .await
            .map_err(|error| {
                tracing::warn!(session_id = %self.session_id, error_kind = "command", "public browser page-source capture failed");
                WebDriverError::Command(error.to_string())
            })
    }

    /// Returns the current CSS viewport height for bounded scroll planning.
    pub(crate) async fn viewport_height(&self) -> Result<u32, WebDriverError> {
        let result = self
            .client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .execute(
                "return Math.max(1, Math.floor(window.innerHeight || 1));",
                Vec::new(),
            )
            .await
            .map_err(|error| WebDriverError::Command(error.to_string()))?;
        result
            .convert()
            .map_err(|error| WebDriverError::Command(error.to_string()))
    }

    /// Scrolls down by a bounded number of CSS pixels.
    pub(crate) async fn scroll_by(&self, distance: u32) -> Result<(), WebDriverError> {
        tracing::trace!(session_id = %self.session_id, distance, "scrolling public browser session");
        self.client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .execute(
                "window.scrollBy(0, arguments[0]);",
                vec![serde_json::json!(distance)],
            )
            .await
            .map_err(|error| {
                tracing::warn!(session_id = %self.session_id, error_kind = "command", "public browser scroll failed");
                WebDriverError::Command(error.to_string())
            })?;
        Ok(())
    }

    /// Captures browser-visible profile values for diagnostics and health
    /// checks. It does not attempt to hide automation signals.
    pub async fn environment(&self) -> Result<BrowserEnvironment, WebDriverError> {
        browser_environment(self.client.as_ref().ok_or(WebDriverError::NotConnected)?).await
    }

    /// Returns the browser's canonical IANA name for a configured timezone.
    ///
    /// This asks the browser's own `Intl` implementation to resolve aliases,
    /// keeping validation aligned with the ICU timezone database used by the
    /// sidecar instead of maintaining a second alias table in the service.
    pub async fn canonical_timezone(&self, timezone: Tz) -> Result<String, WebDriverError> {
        canonical_timezone(
            self.client.as_ref().ok_or(WebDriverError::NotConnected)?,
            timezone,
        )
        .await
    }

    pub(crate) async fn close_client(&mut self) -> Result<(), WebDriverError> {
        if let Some(client) = self.client.take() {
            close_webdriver(client)
                .await
                .map_err(|error| WebDriverError::Command(error.to_owned()))?;
        }
        Ok(())
    }

    /// Closes the remote browser session and releases the local permit.
    pub async fn close(mut self) -> Result<(), WebDriverError> {
        self.close_client().await
    }
}

impl std::fmt::Debug for PublicBrowserSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublicBrowserSession")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

/// An authenticated browser capability fenced to one WeRead account lease.
pub struct AuthenticatedBrowserSession<R>
where
    R: AccountLeaseStore,
{
    _permit: tokio::sync::OwnedSemaphorePermit,
    session_id: Uuid,
    lease: AccountLeaseGuard<R>,
    lease_heartbeat: Option<AccountLeaseHeartbeat>,
    client: Option<WebDriver>,
}

impl<R> AuthenticatedBrowserSession<R>
where
    R: AccountLeaseStore,
{
    pub(crate) fn from_permit(
        permit: tokio::sync::OwnedSemaphorePermit,
        lease: AccountLeaseGuard<R>,
    ) -> Self {
        Self {
            _permit: permit,
            session_id: Uuid::new_v4(),
            lease,
            lease_heartbeat: None,
            client: None,
        }
    }

    fn attach_client(mut self, client: WebDriver) -> Self {
        self.client = Some(client);
        self
    }

    /// Returns a non-secret identifier useful for tracing one local session.
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the stable account identity used by this session.
    pub const fn account_id(&self) -> crate::domain::credentials::WeReadAccountId {
        self.lease.account_id()
    }

    /// Heartbeats the account lease before more authenticated work.
    pub async fn heartbeat(
        &mut self,
        lease_for: chrono::Duration,
    ) -> Result<(), AccountLeaseError> {
        self.lease.heartbeat(lease_for).await
    }

    /// Starts the periodic lease heartbeat for a long-running authenticated
    /// operation. The heartbeat is owned by this session and is stopped by
    /// [`Self::release`], including when the caller forgets to stop it after
    /// an operation error.
    pub(crate) fn start_lease_heartbeat(
        &mut self,
        heartbeat_for: chrono::Duration,
        lease_for: chrono::Duration,
    ) -> Result<(), AccountLeaseError>
    where
        R: Clone + 'static,
    {
        if self.lease_heartbeat.is_some() {
            return Err(AccountLeaseError::Backend(
                "authenticated lease heartbeat is already running".to_owned(),
            ));
        }
        self.lease_heartbeat = Some(self.lease.start_heartbeat(heartbeat_for, lease_for)?);
        Ok(())
    }

    /// Stops the periodic lease heartbeat and reports a heartbeat failure.
    pub(crate) async fn stop_lease_heartbeat(&mut self) -> Result<(), AccountLeaseError> {
        if let Some(mut heartbeat) = self.lease_heartbeat.take() {
            heartbeat.stop().await
        } else {
            Ok(())
        }
    }

    /// Proves that no background heartbeat has lost this session's lease.
    pub(crate) fn ensure_usable(&self) -> Result<(), AccountLeaseError> {
        self.lease.ensure_usable()
    }

    /// Proves account-lease liveness immediately before one authenticated
    /// protocol request.
    ///
    /// The heartbeat uses the lease store's authoritative clock, so this
    /// cannot succeed for an already-expired durable lease. The returned
    /// capability is the only session value accepted by authenticated protocol
    /// adapters; callers cannot pass the raw session directly to them.
    pub async fn prepare_request(
        &mut self,
        lease_for: chrono::Duration,
    ) -> Result<AuthenticatedRequest<'_, R>, AccountLeaseError> {
        self.lease.heartbeat(lease_for).await?;
        Ok(AuthenticatedRequest { session: self })
    }

    /// Releases the durable account lease and then drops local browser capacity.
    pub async fn release(self) -> Result<(), AccountLeaseError> {
        let Self {
            _permit,
            mut client,
            lease,
            lease_heartbeat,
            ..
        } = self;
        let heartbeat_error = if let Some(mut heartbeat) = lease_heartbeat {
            heartbeat.stop().await.err()
        } else {
            None
        };
        let cleanup_error = if let Some(client) = client.take() {
            close_webdriver(client).await.err().map(|error| {
                AccountLeaseError::Backend(format!("authenticated browser cleanup failed: {error}"))
            })
        } else {
            None
        };
        let lease_result = lease.release().await;
        match (lease_result, cleanup_error) {
            (Err(error), _) => Err(error),
            (Ok(()), Some(error)) => Err(heartbeat_error.unwrap_or(error)),
            (Ok(()), None) => heartbeat_error.map_or(Ok(()), Err),
        }
    }
}

impl<R> std::fmt::Debug for AuthenticatedBrowserSession<R>
where
    R: AccountLeaseStore,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticatedBrowserSession")
            .field("session_id", &self.session_id)
            .field("account_id", &self.account_id())
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

/// A one-request authenticated capability created only after a successful
/// server-clock account-lease heartbeat.
pub struct AuthenticatedRequest<'a, R>
where
    R: AccountLeaseStore,
{
    session: &'a mut AuthenticatedBrowserSession<R>,
}

/// Closes a WebDriver client without allowing Thirtyfour's synchronous Drop
/// fallback to run on the Tokio executor after a failed or timed-out request.
/// The cloned handle is leaked in every non-success path, which marks the
/// remote session as abandoned while the cleanup task may still finish.
async fn close_webdriver(client: WebDriver) -> Result<(), &'static str> {
    let abandon_guard = client.clone();
    let cleanup = tokio::spawn(async move { client.quit().await });
    match tokio::time::timeout(WEBDRIVER_CLEANUP_TIMEOUT, cleanup).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(_error))) => {
            let _ = abandon_guard.leak();
            Err("WebDriver quit command failed")
        }
        Ok(Err(_error)) => {
            let _ = abandon_guard.leak();
            Err("WebDriver cleanup task failed")
        }
        Err(_) => {
            let _ = abandon_guard.leak();
            Err("WebDriver cleanup timed out")
        }
    }
}

impl<R> AuthenticatedRequest<'_, R>
where
    R: AccountLeaseStore,
{
    pub(crate) fn ensure_usable(&self) -> Result<(), AccountLeaseError> {
        self.session.lease.ensure_usable()
    }

    /// Returns the account identity whose lease was just proven live.
    pub const fn account_id(&self) -> crate::domain::credentials::WeReadAccountId {
        self.session.account_id()
    }

    /// Returns the local browser-session identifier for tracing.
    pub const fn session_id(&self) -> Uuid {
        self.session.session_id()
    }

    /// Navigates the authenticated browser to one protocol endpoint.
    pub(crate) async fn goto(&mut self, url: &str) -> Result<(), WebDriverError> {
        tracing::trace!(
            session_id = %self.session_id(),
            account_id = %self.account_id(),
            "navigating authenticated browser session"
        );
        self.session
            .client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .goto(url)
            .await
            .map_err(|error| {
                tracing::warn!(
                    session_id = %self.session_id(),
                    account_id = %self.account_id(),
                    error_kind = "command",
                    "authenticated browser navigation failed"
                );
                WebDriverError::Command(error.to_string())
            })
    }

    /// Returns the browser-observed URL after navigation and redirects.
    pub(crate) async fn current_url(&self) -> Result<Url, WebDriverError> {
        self.session
            .client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .current_url()
            .await
            .map_err(|error| WebDriverError::Command(error.to_string()))
    }

    /// Captures the authenticated browser's current rendered page source.
    pub(crate) async fn source(&self) -> Result<String, WebDriverError> {
        tracing::trace!(
            session_id = %self.session_id(),
            account_id = %self.account_id(),
            "capturing authenticated browser page source"
        );
        self.session
            .client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .source()
            .await
            .map_err(|error| {
                tracing::warn!(
                    session_id = %self.session_id(),
                    account_id = %self.account_id(),
                    error_kind = "command",
                    "authenticated browser page-source capture failed"
                );
                WebDriverError::Command(error.to_string())
            })
    }

    /// Installs a validated WeRead cookie header into the current browser
    /// origin. The caller must navigate to `weread.qq.com` first because
    /// WebDriver rejects cookies added while no matching origin is loaded.
    pub(crate) async fn install_cookie_header(
        &mut self,
        cookie_header: &str,
    ) -> Result<(), WebDriverError> {
        tracing::trace!(
            session_id = %self.session_id(),
            account_id = %self.account_id(),
            "installing authenticated browser cookies"
        );
        let client = self
            .session
            .client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?;
        let mut cookie_count = 0_u32;
        for part in cookie_header.split(';') {
            let part = part.trim();
            let Some((name, value)) = part.split_once('=') else {
                return Err(WebDriverError::Command(
                    "WeRead cookie header contains an invalid pair".to_owned(),
                ));
            };
            let mut cookie = Cookie::new(name.trim(), value.trim());
            cookie.set_path("/");
            cookie.set_domain("weread.qq.com");
            cookie.set_secure(true);
            client.add_cookie(cookie).await.map_err(|error| {
                tracing::warn!(
                    session_id = %self.session_id(),
                    account_id = %self.account_id(),
                    error_kind = "command",
                    "authenticated browser cookie installation failed"
                );
                WebDriverError::Command(error.to_string())
            })?;
            cookie_count = cookie_count.saturating_add(1);
        }
        tracing::debug!(
            session_id = %self.session_id(),
            account_id = %self.account_id(),
            cookies = cookie_count,
            "installed authenticated browser cookies"
        );
        Ok(())
    }

    /// Fetches a same-origin response and returns its raw text body.
    ///
    /// Navigating a browser directly to a JSON endpoint is not equivalent to
    /// reading the response bytes: Firefox may replace the document with its
    /// JSON viewer, whose visible text is a formatted tree. The async script
    /// keeps the authenticated page origin and asks the browser Fetch API for
    /// the response text before any document rendering can change it.
    pub(crate) async fn fetch_text(&self, url: &str) -> Result<String, WebDriverError> {
        tracing::trace!(
            session_id = %self.session_id(),
            account_id = %self.account_id(),
            "fetching authenticated browser response text"
        );
        let result = self
            .session
            .client
            .as_ref()
            .ok_or(WebDriverError::NotConnected)?
            .execute_async(
                r#"const done = arguments[arguments.length - 1];
fetch(arguments[0], { credentials: "include" })
    .then(response => response.text())
    .then(body => done({ body }))
    .catch(error => done({ error: String(error) }));"#,
                vec![serde_json::json!(url)],
            )
            .await
            .map_err(|error| {
                tracing::warn!(
                    session_id = %self.session_id(),
                    account_id = %self.account_id(),
                    error_kind = "command",
                    "authenticated browser response fetch failed"
                );
                WebDriverError::Command(error.to_string())
            })?;
        let response: BrowserFetchTextResult = result
            .convert()
            .map_err(|error| WebDriverError::Command(error.to_string()))?;
        if let Some(error) = response.error {
            return Err(WebDriverError::Command(format!(
                "browser fetch failed: {error}"
            )));
        }
        let body = response.body.ok_or_else(|| {
            WebDriverError::Command("browser fetch returned no response body".to_owned())
        });
        match &body {
            Ok(body) => tracing::debug!(
                session_id = %self.session_id(),
                account_id = %self.account_id(),
                bytes = body.len(),
                "received authenticated browser response text"
            ),
            Err(error) => tracing::warn!(
                session_id = %self.session_id(),
                account_id = %self.account_id(),
                error = %error,
                "authenticated browser response had no body"
            ),
        }
        body
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use axum::{
        body::Bytes,
        http::{Method, Request, Response, StatusCode},
    };
    use thirtyfour::{
        prelude::{DesiredCapabilities, WebDriverError as ThirtyfourError, WebDriverResult},
        session::http::{Body, HttpClient},
    };

    use super::*;

    #[derive(Debug)]
    struct FailingQuitHttpClient;

    #[async_trait::async_trait]
    impl HttpClient for FailingQuitHttpClient {
        async fn send(&self, request: Request<Body<'_>>) -> WebDriverResult<Response<Bytes>> {
            if request.method() == Method::DELETE {
                return Err(ThirtyfourError::RequestFailed("quit failed".to_owned()));
            }
            Response::builder()
                .status(StatusCode::OK)
                .body(Bytes::from_static(
                    br#"{"value":{"sessionId":"test-session","capabilities":{}}}"#,
                ))
                .map_err(|error| ThirtyfourError::RequestFailed(error.to_string()))
        }

        async fn new(&self) -> Arc<dyn HttpClient> {
            Arc::new(Self)
        }
    }

    async fn failing_quit_driver() -> WebDriver {
        WebDriver::builder("http://webdriver.test", DesiredCapabilities::firefox())
            .client(FailingQuitHttpClient)
            .connect()
            .await
            .expect("the fake WebDriver should accept session setup")
    }

    #[derive(Debug)]
    struct EnvironmentHttpClient {
        execute_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl HttpClient for EnvironmentHttpClient {
        async fn send(&self, request: Request<Body<'_>>) -> WebDriverResult<Response<Bytes>> {
            let body = if request.uri().path().ends_with("/execute/sync") {
                if self.execute_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                    br#"{"value":{"userAgent":"test-agent","language":"zh-CN","languages":["zh-CN"],"timezone":"UTC","webdriver":true,"innerWidth":1280,"innerHeight":2000}}"#.to_vec()
                } else {
                    br#"{"value":"Asia/Shanghai"}"#.to_vec()
                }
            } else if request.uri().path().ends_with("/session") {
                br#"{"value":{"sessionId":"test-session","capabilities":{}}}"#.to_vec()
            } else {
                br#"{"value":null}"#.to_vec()
            };
            Response::builder()
                .status(StatusCode::OK)
                .body(Bytes::from(body))
                .map_err(|error| ThirtyfourError::RequestFailed(error.to_string()))
        }

        async fn new(&self) -> Arc<dyn HttpClient> {
            Arc::new(Self {
                execute_calls: AtomicUsize::new(self.execute_calls.load(Ordering::Relaxed)),
            })
        }
    }

    async fn environment_driver() -> WebDriver {
        WebDriver::builder("http://webdriver.test", DesiredCapabilities::firefox())
            .client(EnvironmentHttpClient {
                execute_calls: AtomicUsize::new(0),
            })
            .connect()
            .await
            .expect("the fake WebDriver should accept session setup")
    }

    const TEST_WEREAD_SHELF_URL: &str = "https://weread.qq.com/web/shelf";

    #[derive(Debug)]
    struct WeReadBrowserState {
        navigations: Mutex<Vec<String>>,
        cookie_domains: Mutex<Vec<Option<String>>>,
        raw_fetch_urls: Mutex<Vec<String>>,
        raw_fetch_body: String,
        page_source: String,
        current_url: Mutex<String>,
        redirect_shelf_to_login: bool,
    }

    #[derive(Debug, Clone)]
    struct WeReadHttpClient {
        state: Arc<WeReadBrowserState>,
    }

    #[async_trait::async_trait]
    impl HttpClient for WeReadHttpClient {
        async fn send(&self, request: Request<Body<'_>>) -> WebDriverResult<Response<Bytes>> {
            let method = request.method().clone();
            let path = request.uri().path().to_owned();
            let body = match request.body() {
                Body::Json(value) => Some((*value).clone()),
                Body::Empty => None,
            };

            if method == Method::POST && path.ends_with("/session") {
                return webdriver_json_response(json!({
                    "sessionId": "test-session",
                    "capabilities": {}
                }));
            }
            if method == Method::POST && path.ends_with("/url") {
                let target = body
                    .as_ref()
                    .and_then(|value| value.get("url"))
                    .and_then(Value::as_str)
                    .expect("navigation command should contain a URL");
                self.state
                    .navigations
                    .lock()
                    .unwrap()
                    .push(target.to_owned());
                let current_url =
                    if self.state.redirect_shelf_to_login && target == TEST_WEREAD_SHELF_URL {
                        "https://weread.qq.com/web/login"
                    } else {
                        target
                    };
                *self.state.current_url.lock().unwrap() = current_url.to_owned();
                return webdriver_json_response(Value::Null);
            }
            if method == Method::GET && path.ends_with("/url") {
                let current_url = self.state.current_url.lock().unwrap().clone();
                return webdriver_json_response(json!(current_url));
            }
            if method == Method::POST && path.ends_with("/cookie") {
                self.state.cookie_domains.lock().unwrap().push(
                    body.as_ref()
                        .and_then(|value| value.get("cookie"))
                        .and_then(|cookie| cookie.get("domain"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                );
                return webdriver_json_response(Value::Null);
            }
            if method == Method::POST && path.ends_with("/execute/sync") {
                return webdriver_json_response(json!(r#"{"data":[]}"#));
            }
            if method == Method::GET && path.ends_with("/source") {
                return webdriver_json_response(json!(self.state.page_source.clone()));
            }
            if method == Method::POST && path.ends_with("/execute/async") {
                let target = body
                    .as_ref()
                    .and_then(|value| value.get("args"))
                    .and_then(Value::as_array)
                    .and_then(|args| args.first())
                    .and_then(Value::as_str)
                    .expect("raw fetch should contain its target URL as an argument");
                self.state
                    .raw_fetch_urls
                    .lock()
                    .unwrap()
                    .push(target.to_owned());
                return webdriver_json_response(json!({
                    "body": self.state.raw_fetch_body.clone()
                }));
            }
            webdriver_json_response(Value::Null)
        }

        async fn new(&self) -> Arc<dyn HttpClient> {
            Arc::new(self.clone())
        }
    }

    fn webdriver_json_response(value: Value) -> WebDriverResult<Response<Bytes>> {
        Response::builder()
            .status(StatusCode::OK)
            .body(Bytes::from(json!({"value": value}).to_string()))
            .map_err(|error| ThirtyfourError::RequestFailed(error.to_string()))
    }

    async fn weread_driver(state: Arc<WeReadBrowserState>) -> WebDriver {
        WebDriver::builder("http://webdriver.test", DesiredCapabilities::firefox())
            .client(WeReadHttpClient { state })
            .connect()
            .await
            .expect("the fake WeRead WebDriver should accept session setup")
    }

    #[derive(Debug)]
    struct TestWeReadCredentialProvider;

    #[async_trait::async_trait]
    impl crate::acquisition::weread::WeReadCredentialProvider for TestWeReadCredentialProvider {
        async fn credentials(
            &self,
            _account_id: crate::domain::credentials::WeReadAccountId,
        ) -> Result<
            crate::domain::credentials::WeReadCredentials,
            crate::acquisition::weread::WeReadCredentialProviderError,
        > {
            let credentials = crate::domain::credentials::WeReadCredentials::new(
                "access",
                "refresh",
                chrono::Utc::now() + chrono::Duration::hours(1),
                chrono::Utc::now(),
            )
            .expect("test credentials should be valid");
            Ok(credentials
                .with_web_cookie("wr_skey=access; wr_rt=refresh")
                .expect("test cookie should be valid"))
        }
    }

    #[test]
    fn chromium_capabilities_request_a_headless_chrome_without_credentials() {
        let capabilities =
            capabilities_for(BrowserEngine::Chromium, &BrowserProfile::default()).unwrap();
        assert_eq!(capabilities.get("browserName"), Some(&json!("chrome")));
        assert_eq!(
            capabilities
                .get("goog:chromeOptions")
                .and_then(|options| options.get("args"))
                .and_then(Value::as_array)
                .and_then(|args| args.first())
                .and_then(Value::as_str),
            Some("--headless=new")
        );
        assert!(!capabilities.contains_key("proxy"));
    }

    #[test]
    fn firefox_capabilities_request_headless_firefox() {
        let capabilities =
            capabilities_for(BrowserEngine::Firefox, &BrowserProfile::default()).unwrap();
        assert_eq!(capabilities.get("browserName"), Some(&json!("firefox")));
        assert_eq!(
            capabilities
                .get("moz:firefoxOptions")
                .and_then(|options| options.get("args"))
                .and_then(Value::as_array)
                .and_then(|args| args.first())
                .and_then(Value::as_str),
            Some("-headless")
        );
    }

    #[test]
    fn capabilities_apply_profile_values_without_credentials() {
        let profile = BrowserProfile {
            user_agent: Some("Mozilla/5.0 TestBrowser/1.0".to_owned()),
            viewport: BrowserViewport::new(1_440, 900),
            locale: "en-US".to_owned(),
            expected_timezone: Some(chrono_tz::Asia::Shanghai),
            extra_args: vec!["--disable-features=SomeFeature".to_owned()],
        };
        let capabilities = capabilities_for(BrowserEngine::Chromium, &profile).unwrap();
        let args = capabilities
            .get("goog:chromeOptions")
            .and_then(|options| options.get("args"))
            .and_then(Value::as_array)
            .unwrap();
        let args = args.iter().filter_map(Value::as_str).collect::<Vec<_>>();
        assert!(args.contains(&"--window-size=1440,900"));
        assert!(args.contains(&"--lang=en-US"));
        assert!(args.contains(&"--user-agent=Mozilla/5.0 TestBrowser/1.0"));
        assert!(args.contains(&"--disable-features=SomeFeature"));
        assert!(!capabilities.contains_key("proxy"));

        let firefox = capabilities_for(BrowserEngine::Firefox, &profile).unwrap();
        assert_eq!(
            firefox
                .get("moz:firefoxOptions")
                .and_then(|options| options.get("prefs"))
                .and_then(|prefs| prefs.get("intl.accept_languages")),
            Some(&json!("en-US"))
        );
        assert_eq!(
            firefox
                .get("moz:firefoxOptions")
                .and_then(|options| options.get("prefs"))
                .and_then(|prefs| prefs.get("general.useragent.override")),
            Some(&json!("Mozilla/5.0 TestBrowser/1.0"))
        );
    }

    #[test]
    fn rejects_extra_arguments_that_override_controlled_profile_values() {
        for argument in [
            "--lang=en-US",
            "--user-agent=other",
            "--window-size=1,1",
            "--user-data-dir=/tmp/other-profile",
            "--headless=false",
            "-profile=/tmp/other-profile",
        ] {
            let profile = BrowserProfile {
                extra_args: vec![argument.to_owned()],
                ..BrowserProfile::default()
            };
            let error = capabilities_for(BrowserEngine::Chromium, &profile)
                .expect_err("conflicting profile argument should be rejected");
            assert!(error.contains("controlled browser profile arguments"));
        }
    }

    fn browser_environment(timezone: &str) -> BrowserEnvironment {
        BrowserEnvironment {
            user_agent: "test-agent".to_owned(),
            language: "zh-CN".to_owned(),
            languages: vec!["zh-CN".to_owned()],
            timezone: timezone.to_owned(),
            webdriver: Some(true),
            inner_width: 1_280,
            inner_height: 2_000,
        }
    }

    #[test]
    fn expected_timezone_mismatch_is_rejected() {
        assert!(matches!(
            validate_expected_environment("UTC", &browser_environment("Asia/Shanghai")),
            Err(WebDriverError::EnvironmentMismatch {
                field: "timezone",
                expected,
                actual,
            }) if expected == "UTC" && actual == "Asia/Shanghai"
        ));
    }

    #[test]
    fn canonical_expected_timezone_is_accepted() {
        assert!(validate_expected_environment(
            "America/Los_Angeles",
            &browser_environment("America/Los_Angeles")
        )
        .is_ok());
    }

    #[test]
    fn safe_webdriver_error_messages_hide_response_bodies() {
        let error = WebDriverError::Command(
            "The WebDriver server returned an article body that must stay private".to_owned(),
        );

        assert_eq!(error.safe_message(), "WebDriver command failed");
    }

    #[test]
    fn webdriver_health_parser_accepts_selenium_envelopes_and_legacy_payloads() {
        assert_eq!(
            parse_webdriver_status(&json!({
                "value": {"ready": true, "message": "ready", "build": {}}
            }))
            .unwrap(),
            WebDriverSidecarStatus::Ready
        );
        assert_eq!(
            parse_webdriver_status(&json!({
                "ready": false,
                "message": "starting"
            }))
            .unwrap(),
            WebDriverSidecarStatus::NotReady
        );
    }

    #[test]
    fn webdriver_health_parser_treats_an_up_node_with_all_slots_reserved_as_busy() {
        assert_eq!(
            parse_webdriver_status(&json!({
                "value": {
                    "ready": false,
                    "nodes": [{
                        "availability": "UP",
                        "slots": [{"session": {"sessionId": "reserved"}}]
                    }, {
                        "availability": "DOWN",
                        "slots": [{"session": null}]
                    }]
                }
            }))
            .unwrap(),
            WebDriverSidecarStatus::Busy
        );
    }

    #[test]
    fn webdriver_health_parser_keeps_an_up_node_with_a_free_slot_not_ready() {
        assert_eq!(
            parse_webdriver_status(&json!({
                "value": {
                    "ready": false,
                    "nodes": [{
                        "availability": "UP",
                        "slots": [{"session": null}]
                    }, {
                        "availability": "UP",
                        "slots": [{"session": {"sessionId": "reserved"}}]
                    }]
                }
            }))
            .unwrap(),
            WebDriverSidecarStatus::NotReady
        );
    }

    #[test]
    fn webdriver_health_parser_rejects_malformed_status_without_echoing_body() {
        let error = parse_webdriver_status(&json!({
            "value": {"message": "private response body"}
        }))
        .expect_err("ready must be a boolean");

        assert_eq!(
            error.safe_message(),
            "WebDriver session could not be created"
        );
        assert!(!error.to_string().contains("private response body"));
    }

    #[test]
    fn webdriver_health_always_targets_the_sidecar_status_endpoint() {
        let endpoint = "http://webdriver.test/wd/hub/".parse::<Url>().unwrap();
        assert_eq!(
            webdriver_status_url(&endpoint).unwrap().as_str(),
            "http://webdriver.test/status"
        );
    }

    #[tokio::test]
    async fn authenticated_environment_validation_rejects_a_timezone_mismatch() {
        let client = environment_driver().await;
        let factory = WebDriverFactory::new(
            "http://webdriver.test".parse().unwrap(),
            BrowserEngine::Firefox,
        )
        .with_profile(BrowserProfile {
            expected_timezone: Some(chrono_tz::Asia::Shanghai),
            ..BrowserProfile::default()
        });

        assert!(matches!(
            factory.validate_environment(&client).await,
            Err(WebDriverError::EnvironmentMismatch {
                field: "timezone",
                expected,
                actual,
            }) if expected == "Asia/Shanghai" && actual == "UTC"
        ));
        let _ = close_webdriver(client).await;
    }

    #[tokio::test]
    async fn an_unconnected_pool_session_cannot_issue_browser_commands() {
        let pool = BrowserPool::new(1).unwrap();
        let session = pool.open_public().await.unwrap();
        assert!(matches!(
            session.current_url().await,
            Err(WebDriverError::NotConnected)
        ));
        assert!(matches!(
            session.viewport_height().await,
            Err(WebDriverError::NotConnected)
        ));
        assert!(matches!(
            session.scroll_by(100).await,
            Err(WebDriverError::NotConnected)
        ));
        assert!(matches!(
            session.canonical_timezone(chrono_tz::UTC).await,
            Err(WebDriverError::NotConnected)
        ));
    }

    #[tokio::test]
    async fn authenticated_release_releases_the_lease_when_browser_cleanup_fails() {
        let repository = crate::persistence::repositories::account_lease_repository::
            MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let account_id =
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1));
        let pool = BrowserPool::new(1).unwrap();
        let session = pool
            .open_authenticated(
                repository.clone(),
                account_id,
                "worker-a",
                chrono::Duration::seconds(30),
            )
            .await
            .unwrap()
            .expect("the test worker should acquire the account");
        let session = session.attach_client(failing_quit_driver().await);

        let error = session
            .release()
            .await
            .expect_err("cleanup failure should be reported");
        assert!(error
            .to_string()
            .contains("authenticated browser cleanup failed"));
        assert!(repository
            .acquire(account_id, "worker-b", chrono::Duration::seconds(30),)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn weread_cover_fetch_reads_raw_json_without_navigating_to_the_endpoint() {
        use crate::acquisition::weread::{BrowserWeReadAdapter, WeReadAdapter};

        let state = Arc::new(WeReadBrowserState {
            navigations: Mutex::new(Vec::new()),
            cookie_domains: Mutex::new(Vec::new()),
            raw_fetch_urls: Mutex::new(Vec::new()),
            raw_fetch_body: r#"{"reviewId":"MP_WXS_book-1_article-1","title":"Cover title","name":"Account","pic":"https://mmbiz.qpic.cn/cover.jpg"}"#.to_owned(),
            page_source: String::new(),
            current_url: Mutex::new(TEST_WEREAD_SHELF_URL.to_owned()),
            redirect_shelf_to_login: false,
        });
        let repository = crate::persistence::repositories::account_lease_repository::
            MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let account_id =
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1));
        let pool = BrowserPool::new(1).unwrap();
        let mut session = pool
            .open_authenticated(
                repository,
                account_id,
                "worker-a",
                chrono::Duration::seconds(30),
            )
            .await
            .unwrap()
            .expect("the test worker should acquire the account")
            .attach_client(weread_driver(Arc::clone(&state)).await);
        let request = session
            .prepare_request(chrono::Duration::seconds(30))
            .await
            .unwrap();
        let adapter =
            BrowserWeReadAdapter::new("https://weread.qq.com/api/mp/cover".parse().unwrap())
                .unwrap()
                .with_credential_provider(Arc::new(TestWeReadCredentialProvider));

        let references = adapter.list_articles("book-1", request).await.unwrap();

        assert_eq!(references.len(), 1);
        assert_eq!(references[0].review_id, "MP_WXS_book-1_article-1");
        let navigations = state.navigations.lock().unwrap().clone();
        assert_eq!(
            navigations,
            vec![
                TEST_WEREAD_SHELF_URL.to_owned(),
                TEST_WEREAD_SHELF_URL.to_owned(),
            ]
        );
        assert_eq!(
            state.raw_fetch_urls.lock().unwrap().clone(),
            vec!["https://weread.qq.com/api/mp/cover?bookId=book-1".to_owned()]
        );
        let cookie_domains = state.cookie_domains.lock().unwrap().clone();
        assert_eq!(
            cookie_domains,
            vec![
                Some("weread.qq.com".to_owned()),
                Some("weread.qq.com".to_owned()),
            ]
        );
        session
            .release()
            .await
            .expect("the test account lease should be released");
    }

    #[tokio::test]
    async fn weread_listing_stops_when_the_shelf_redirects_to_login() {
        use crate::acquisition::weread::{BrowserWeReadAdapter, WeReadAdapter, WeReadAdapterError};

        let state = Arc::new(WeReadBrowserState {
            navigations: Mutex::new(Vec::new()),
            cookie_domains: Mutex::new(Vec::new()),
            raw_fetch_urls: Mutex::new(Vec::new()),
            raw_fetch_body: r#"{"data":[]}"#.to_owned(),
            page_source: String::new(),
            current_url: Mutex::new(TEST_WEREAD_SHELF_URL.to_owned()),
            redirect_shelf_to_login: true,
        });
        let repository = crate::persistence::repositories::account_lease_repository::
            MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let account_id =
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1));
        let pool = BrowserPool::new(1).unwrap();
        let mut session = pool
            .open_authenticated(
                repository,
                account_id,
                "worker-a",
                chrono::Duration::seconds(30),
            )
            .await
            .unwrap()
            .expect("the test worker should acquire the account")
            .attach_client(weread_driver(Arc::clone(&state)).await);
        let request = session
            .prepare_request(chrono::Duration::seconds(30))
            .await
            .unwrap();
        let adapter =
            BrowserWeReadAdapter::new("https://weread.qq.com/web/mp/articles".parse().unwrap())
                .unwrap()
                .with_credential_provider(Arc::new(TestWeReadCredentialProvider));

        let error = adapter
            .list_articles("book-1", request)
            .await
            .expect_err("a login redirect must stop article-list navigation");

        assert_eq!(
            error,
            WeReadAdapterError::AuthenticationExpired { code: -2012 }
        );
        let navigations = state.navigations.lock().unwrap().clone();
        assert_eq!(
            navigations,
            vec![
                TEST_WEREAD_SHELF_URL.to_owned(),
                TEST_WEREAD_SHELF_URL.to_owned(),
            ]
        );
        session
            .release()
            .await
            .expect("the test account lease should be released");
    }

    #[tokio::test]
    async fn weread_content_fetch_extracts_js_content_in_the_authenticated_session() {
        use crate::acquisition::weread::{
            BrowserWeReadAdapter, WeReadAdapter, WeReadArticleReference,
        };

        let state = Arc::new(WeReadBrowserState {
            navigations: Mutex::new(Vec::new()),
            cookie_domains: Mutex::new(Vec::new()),
            raw_fetch_urls: Mutex::new(Vec::new()),
            raw_fetch_body: String::new(),
            page_source: r#"
                <html><head>
                  <meta property="og:title" content="Content title">
                </head><body>
                  <div id="js_content"><p>Authenticated body</p></div>
                </body></html>
            "#
            .to_owned(),
            current_url: Mutex::new(TEST_WEREAD_SHELF_URL.to_owned()),
            redirect_shelf_to_login: false,
        });
        let repository = crate::persistence::repositories::account_lease_repository::
            MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let account_id =
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1));
        let pool = BrowserPool::new(1).unwrap();
        let mut session = pool
            .open_authenticated(
                repository,
                account_id,
                "worker-a",
                chrono::Duration::seconds(30),
            )
            .await
            .unwrap()
            .expect("the test worker should acquire the account")
            .attach_client(weread_driver(Arc::clone(&state)).await);
        let request = session
            .prepare_request(chrono::Duration::seconds(30))
            .await
            .unwrap();
        let reference = WeReadArticleReference::new(
            "MP_WXS_book-1_article-1",
            Some(
                "https://mp.weixin.qq.com/s/article-1"
                    .parse()
                    .expect("test article URL should be valid"),
            ),
            Some("List title".to_owned()),
        )
        .unwrap();
        let adapter = BrowserWeReadAdapter::new(
            "https://weread.qq.com/api/mp/cover"
                .parse()
                .expect("test endpoint should be valid"),
        )
        .unwrap()
        .with_credential_provider(Arc::new(TestWeReadCredentialProvider));

        let page = adapter
            .fetch_article_content(&reference, request)
            .await
            .expect("authenticated content should parse");

        assert_eq!(page.title, "Content title");
        assert_eq!(page.content_html, "<p>Authenticated body</p>");
        assert_eq!(
            state.navigations.lock().unwrap().clone(),
            vec![
                TEST_WEREAD_SHELF_URL.to_owned(),
                TEST_WEREAD_SHELF_URL.to_owned(),
                "https://weread.qq.com/web/mp/content?reviewId=MP_WXS_book-1_article-1".to_owned(),
            ]
        );
        assert!(state.raw_fetch_urls.lock().unwrap().is_empty());
        session
            .release()
            .await
            .expect("the test account lease should be released");
    }
}
