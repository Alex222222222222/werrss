//! Runtime composition for authenticated WeRead listing and public article
//! acquisition.
//!
//! The source-sync handler depends on [`super::source_sync_handler::SourceSyncAcquirer`]
//! rather than on browser details. This adapter is the executable bridge: it
//! acquires one local browser permit and one durable account lease for the
//! authenticated list request, then releases both before each public article
//! is fetched with a clean browser session. The selected account is returned
//! with the list references so that, if a public fetch fails, the adapter can
//! reacquire that same account and try the authenticated WeRead content
//! endpoint before reporting the acquisition error.
//!
//! Authentication uses an encrypted cookie header injected by the application
//! authentication boundary. The account is selected at job time from the
//! source relationship or the enabled account records, so admin-panel
//! enrollment does not require a process restart. Login and QR exchange remain
//! outside this first runtime slice. Lease contention is reported as a
//! retryable acquisition failure, and a lost lease is never followed by
//! another upstream request.

use chrono::{DateTime, Duration, Utc};
use rand::seq::IteratorRandom;

use crate::{
    acquisition::{
        article_page::{ArticlePageError, ArticlePageFetcher, ExtractedArticlePage},
        browser_pool::{AccountLeaseError, AccountLeaseStore, BrowserPool},
        webdriver::{AuthenticatedBrowserSession, WebDriverError, WebDriverFactory},
        weread::{WeReadAdapter, WeReadAdapterError, WeReadArticleReference},
    },
    domain::{credentials::WeReadAccountId, source::Source},
    persistence::repositories::credential_repository::{CredentialRecord, CredentialRepository},
};

use super::{
    source_sync_handler::{SourceSyncAcquirer, SourceSyncReferences},
    sync_service::SyncAcquisitionError,
};

/// Resolves the account to use for one source-sync request.
///
/// A requested account comes from the source relationship when present. When
/// it is absent, implementations select a random enabled, usable account from
/// their durable store. Selection happens immediately before browser work so
/// accounts enrolled through the admin panel become available without a
/// process restart.
#[async_trait::async_trait]
pub trait WeReadAccountSelector: Send + Sync {
    /// Returns the requested usable account, a random usable account when no
    /// account was requested, or `None` when no account is currently enrolled
    /// and enabled.
    async fn select_account(
        &self,
        requested: Option<WeReadAccountId>,
    ) -> Result<Option<WeReadAccountId>, WeReadAdapterError>;
}

/// PostgreSQL- or memory-backed account selector for runtime source jobs.
#[derive(Clone)]
pub struct CredentialRepositoryAccountSelector<R> {
    accounts: R,
}

impl<R> CredentialRepositoryAccountSelector<R> {
    /// Creates a selector backed by the supplied credential repository.
    pub const fn new(accounts: R) -> Self {
        Self { accounts }
    }
}

#[async_trait::async_trait]
impl<R> WeReadAccountSelector for CredentialRepositoryAccountSelector<R>
where
    R: CredentialRepository + Clone,
{
    async fn select_account(
        &self,
        requested: Option<WeReadAccountId>,
    ) -> Result<Option<WeReadAccountId>, WeReadAdapterError> {
        tracing::debug!(
            requested_account_id = ?requested,
            "selecting a usable WeRead account"
        );
        let now = self
            .accounts
            .database_now()
            .await
            .map_err(|error| WeReadAdapterError::LeaseBackend(error.to_string()))?;
        let record = match requested {
            Some(account_id) => self
                .accounts
                .find(account_id)
                .await
                .map_err(|error| WeReadAdapterError::LeaseBackend(error.to_string()))?,
            None => {
                let records = self
                    .accounts
                    .list()
                    .await
                    .map_err(|error| WeReadAdapterError::LeaseBackend(error.to_string()))?;
                let selected = choose_random_usable_account(records, now, &mut rand::rng());
                tracing::debug!(selected_account_id = ?selected, "selected a random usable WeRead account");
                return Ok(selected);
            }
        };

        let selected = record
            .filter(|record| {
                !record.account().disabled()
                    && !record.account().setup_required()
                    && record
                        .account()
                        .access_expires_at()
                        .is_some_and(|expires_at| expires_at > now)
            })
            .map(|record| record.account().account_id());
        tracing::debug!(selected_account_id = ?selected, "resolved requested WeRead account");
        Ok(selected)
    }
}

fn choose_random_usable_account<R: rand::Rng + ?Sized>(
    records: Vec<CredentialRecord>,
    now: DateTime<Utc>,
    rng: &mut R,
) -> Option<WeReadAccountId> {
    records
        .into_iter()
        .filter(|record| {
            !record.account().disabled()
                && !record.account().setup_required()
                && record
                    .account()
                    .access_expires_at()
                    .is_some_and(|expires_at| expires_at > now)
        })
        .map(|record| record.account().account_id())
        .choose(rng)
}

/// Dependencies for the concrete source-sync acquisition adapter.
#[derive(Clone)]
pub struct BrowserSourceSyncAcquirerDependencies<R, W, A, S> {
    /// Process-local browser capacity shared by worker tasks.
    pub browser_pool: BrowserPool,
    /// WebDriver connection and browser-profile policy.
    pub webdriver: WebDriverFactory,
    /// Durable account lease store.
    pub account_leases: R,
    /// Authenticated WeRead article-list adapter.
    pub weread: W,
    /// Credential-free public article-page fetcher.
    pub article_pages: A,
    /// Durable account selector used when the source has no fixed account.
    pub account_selector: S,
}

/// Runtime policy for authenticated source synchronization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserSourceSyncAcquirerConfig {
    account_id: Option<WeReadAccountId>,
    owner: String,
    lease_for: Duration,
    heartbeat_for: Duration,
}

impl BrowserSourceSyncAcquirerConfig {
    /// Creates a policy with an optional environment-configured default
    /// account. When it is absent, the account selector is consulted for each
    /// source-sync job.
    pub fn new(
        account_id: Option<WeReadAccountId>,
        owner: impl Into<String>,
        lease_for: Duration,
        heartbeat_for: Duration,
    ) -> Result<Self, BrowserSourceSyncAcquirerConfigError> {
        if account_id.is_some_and(|account_id| account_id.as_uuid().is_nil()) {
            return Err(BrowserSourceSyncAcquirerConfigError::InvalidAccountId);
        }
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(BrowserSourceSyncAcquirerConfigError::EmptyOwner);
        }
        if lease_for <= Duration::zero() || heartbeat_for <= Duration::zero() {
            return Err(BrowserSourceSyncAcquirerConfigError::InvalidLeaseTiming);
        }
        if heartbeat_for >= lease_for {
            return Err(BrowserSourceSyncAcquirerConfigError::HeartbeatNotShorterThanLease);
        }
        Ok(Self {
            account_id,
            owner,
            lease_for,
            heartbeat_for,
        })
    }

    /// Returns the default account used when a source has no account override.
    pub const fn account_id(&self) -> Option<WeReadAccountId> {
        self.account_id
    }

    /// Returns the account lease duration.
    pub const fn lease_for(&self) -> Duration {
        self.lease_for
    }

    /// Returns the account heartbeat duration passed to the authenticated
    /// request preparation boundary.
    pub const fn heartbeat_for(&self) -> Duration {
        self.heartbeat_for
    }

    fn owner_for(&self, worker_index: u32) -> String {
        format!("{}-{worker_index}", self.owner)
    }
}

/// Invalid concrete source-sync acquisition policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BrowserSourceSyncAcquirerConfigError {
    /// A nil UUID cannot identify an account.
    #[error("WeRead account id must not be nil")]
    InvalidAccountId,
    /// Lease ownership must identify a worker instance.
    #[error("source-sync account lease owner must not be empty")]
    EmptyOwner,
    /// Both lease durations must be positive.
    #[error("source-sync account lease timings must be positive")]
    InvalidLeaseTiming,
    /// Heartbeats must leave time to reach the database before expiry.
    #[error("source-sync account heartbeat must be shorter than its lease")]
    HeartbeatNotShorterThanLease,
}

/// Thirtyfour-backed source-sync acquisition implementation.
#[derive(Clone)]
pub struct BrowserSourceSyncAcquirer<R, W, A, S> {
    dependencies: BrowserSourceSyncAcquirerDependencies<R, W, A, S>,
    config: BrowserSourceSyncAcquirerConfig,
    worker_index: u32,
}

impl<R, W, A, S> BrowserSourceSyncAcquirer<R, W, A, S> {
    /// Creates one worker-local adapter. The browser pool and lease store may
    /// be cloned into other workers; the durable account lease serializes use
    /// of the selected account across them.
    pub fn new(
        dependencies: BrowserSourceSyncAcquirerDependencies<R, W, A, S>,
        config: BrowserSourceSyncAcquirerConfig,
        worker_index: u32,
    ) -> Self {
        Self {
            dependencies,
            config,
            worker_index,
        }
    }

    async fn open_authenticated_session(
        &self,
        source: &Source,
        selected_account_id: Option<WeReadAccountId>,
    ) -> Result<AuthenticatedBrowserSession<R>, SyncAcquisitionError>
    where
        R: AccountLeaseStore + Clone + 'static,
        S: WeReadAccountSelector,
    {
        tracing::debug!(
            source_id = %source.id(),
            requested_account_id = ?selected_account_id.or(source.account_id()).or(self.config.account_id()),
            "opening authenticated WeRead browser session"
        );
        let requested_account = selected_account_id
            .or(source.account_id())
            .or(self.config.account_id());
        let account_id = self
            .dependencies
            .account_selector
            .select_account(requested_account)
            .await
            .map_err(SyncAcquisitionError::WeRead)?;
        let Some(account_id) = account_id else {
            tracing::warn!(
                source_id = %source.id(),
                "source synchronization has no usable WeRead account"
            );
            return Err(SyncAcquisitionError::NoAccountEnrolled);
        };
        let owner = self.config.owner_for(self.worker_index);
        let session = self
            .dependencies
            .webdriver
            .open_authenticated(
                &self.dependencies.browser_pool,
                self.dependencies.account_leases.clone(),
                account_id,
                &owner,
                self.config.lease_for(),
            )
            .await
            .map_err(map_authenticated_webdriver_error)?;
        let Some(mut session) = session else {
            tracing::warn!(account_id = %account_id, "WeRead account lease is already held");
            return Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::LeaseBackend(
                    "the configured WeRead account is already in use".to_owned(),
                ),
            ));
        };

        if let Err(error) =
            session.start_lease_heartbeat(self.config.heartbeat_for(), self.config.lease_for())
        {
            tracing::warn!(account_id = %account_id, error = %error, "unable to start WeRead account lease heartbeat");
            let _ = session.release().await;
            return Err(SyncAcquisitionError::WeRead(error.into()));
        }
        tracing::info!(
            account_id = %account_id,
            session_id = %session.session_id(),
            "opened authenticated WeRead browser session"
        );
        Ok(session)
    }

    async fn fetch_article_with_weread(
        &self,
        source: &Source,
        reference: &WeReadArticleReference,
        selected_account_id: Option<WeReadAccountId>,
    ) -> Result<ExtractedArticlePage, SyncAcquisitionError>
    where
        R: AccountLeaseStore + Clone + 'static,
        W: WeReadAdapter<R>,
        S: WeReadAccountSelector,
    {
        tracing::debug!(
            source_id = %source.id(),
            review_id = %reference.review_id,
            "fetching article through authenticated WeRead fallback"
        );
        let mut session = self
            .open_authenticated_session(source, selected_account_id)
            .await?;
        let result: Result<ExtractedArticlePage, WeReadAdapterError> = async {
            let request = session
                .prepare_request(self.config.lease_for())
                .await
                .map_err(WeReadAdapterError::from)?;
            self.dependencies
                .weread
                .fetch_article_content(reference, request)
                .await
        }
        .await;
        let heartbeat = session.stop_lease_heartbeat().await;
        let release = session.release().await;
        match (result, heartbeat, release) {
            (Err(error), _, _) => {
                tracing::warn!(source_id = %source.id(), review_id = %reference.review_id, error = %error, "authenticated WeRead article fallback failed");
                Err(SyncAcquisitionError::WeRead(error))
            }
            (Ok(_), Err(error), _) => {
                tracing::warn!(source_id = %source.id(), review_id = %reference.review_id, error = %error, "authenticated WeRead lease heartbeat failed after article fetch");
                Err(SyncAcquisitionError::WeRead(error.into()))
            }
            (Ok(_), Ok(()), Err(error)) => {
                tracing::warn!(source_id = %source.id(), review_id = %reference.review_id, error = %error, "authenticated WeRead session release failed after article fetch");
                Err(SyncAcquisitionError::WeRead(error.into()))
            }
            (Ok(page), Ok(()), Ok(())) => {
                tracing::info!(source_id = %source.id(), review_id = %reference.review_id, "authenticated WeRead article fallback succeeded");
                Ok(page)
            }
        }
    }
}

#[async_trait::async_trait]
impl<R, W, A, S> SourceSyncAcquirer for BrowserSourceSyncAcquirer<R, W, A, S>
where
    R: AccountLeaseStore + Clone + 'static,
    W: WeReadAdapter<R>,
    A: ArticlePageFetcher,
    S: WeReadAccountSelector,
{
    async fn list_article_references(
        &self,
        source: &Source,
    ) -> Result<SourceSyncReferences, SyncAcquisitionError> {
        tracing::debug!(source_id = %source.id(), book_id = %source.book_id(), "listing WeRead article references");
        let mut session = self.open_authenticated_session(source, None).await?;
        let selected_account_id = session.account_id();
        let result = async {
            let references = {
                let request = session
                    .prepare_request(self.config.lease_for())
                    .await
                    .map_err(WeReadAdapterError::from)?;
                self.dependencies
                    .weread
                    .list_articles(source.book_id(), request)
                    .await?
            };
            session.ensure_usable().map_err(WeReadAdapterError::from)?;
            Ok(references)
        }
        .await;
        let heartbeat = session.stop_lease_heartbeat().await;
        let release = session.release().await;
        match (result, heartbeat, release) {
            (Err(error), _, _) => {
                tracing::warn!(source_id = %source.id(), account_id = %selected_account_id, error = %error, "WeRead article listing failed");
                Err(SyncAcquisitionError::WeRead(error))
            }
            (Ok(_), Err(error), _) => {
                tracing::warn!(source_id = %source.id(), account_id = %selected_account_id, error = %error, "WeRead account lease heartbeat failed after listing");
                Err(SyncAcquisitionError::WeRead(error.into()))
            }
            (Ok(_), Ok(()), Err(error)) => {
                tracing::warn!(source_id = %source.id(), account_id = %selected_account_id, error = %error, "WeRead session release failed after listing");
                Err(SyncAcquisitionError::WeRead(error.into()))
            }
            (Ok(references), Ok(()), Ok(())) => {
                tracing::info!(source_id = %source.id(), account_id = %selected_account_id, references = references.len(), "listed WeRead article references");
                Ok(SourceSyncReferences::new(
                    references,
                    Some(selected_account_id),
                ))
            }
        }
    }

    async fn fetch_article(
        &self,
        source: &Source,
        reference: &WeReadArticleReference,
        selected_account_id: Option<WeReadAccountId>,
    ) -> Result<ExtractedArticlePage, SyncAcquisitionError> {
        tracing::debug!(source_id = %source.id(), review_id = %reference.review_id, "fetching public article");
        let url = reference.article_url.clone().ok_or_else(|| {
            SyncAcquisitionError::ArticlePage(ArticlePageError::InvalidExtraction(
                "WeRead article reference has no public URL".to_owned(),
            ))
        })?;
        let public_result = self
            .dependencies
            .webdriver
            .open_public(&self.dependencies.browser_pool)
            .await;
        let public_result = match public_result {
            Ok(session) => self
                .dependencies
                .article_pages
                .fetch(url, session)
                .await
                .map_err(SyncAcquisitionError::ArticlePage),
            Err(error) => Err(map_public_webdriver_error(error)),
        };
        match public_result {
            Ok(page) => {
                tracing::info!(source_id = %source.id(), review_id = %reference.review_id, "public article fetch succeeded");
                Ok(page)
            }
            Err(public_error) => {
                tracing::warn!(
                    source_id = %source.id(),
                    review_id = %reference.review_id,
                    error = %public_error,
                    "public article fetch failed; trying authenticated WeRead content fallback"
                );
                self.fetch_article_with_weread(source, reference, selected_account_id)
                    .await
            }
        }
    }
}

fn map_authenticated_webdriver_error(error: WebDriverError) -> SyncAcquisitionError {
    SyncAcquisitionError::WeRead(WeReadAdapterError::Browser(error.safe_message()))
}

fn map_public_webdriver_error(error: WebDriverError) -> SyncAcquisitionError {
    SyncAcquisitionError::ArticlePage(ArticlePageError::Browser(error.safe_message()))
}

impl From<AccountLeaseError> for SyncAcquisitionError {
    fn from(error: AccountLeaseError) -> Self {
        SyncAcquisitionError::WeRead(WeReadAdapterError::from(error))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{
        body::{to_bytes, Body},
        extract::State,
        http::{Method, Request, Response, StatusCode},
        Router,
    };
    use serde_json::{json, Value};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        acquisition::{
            article_page::{ArticlePageError, ArticlePageFetcher},
            browser_pool::{AccountLeaseStore, BrowserPool},
            webdriver::{AuthenticatedRequest, PublicBrowserSession},
            weread::{WeReadCredentialProvider, WeReadCredentialProviderError},
        },
        config::BrowserEngine,
        domain::credentials::WeReadCredentials,
        domain::source::{NewSource, Source},
        persistence::repositories::{
            account_lease_repository::MemoryAccountLeaseRepository,
            credential_repository::{CredentialRepository, MemoryCredentialRepository},
        },
    };
    use uuid::Uuid;

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    fn source(account_id: Option<WeReadAccountId>) -> Source {
        let mut spec = NewSource::test_default();
        spec.account_id = account_id;
        Source::new(spec).expect("test source should be valid")
    }

    #[derive(Clone)]
    struct UnusedWeReadAdapter;

    #[async_trait::async_trait]
    impl<R> WeReadAdapter<R> for UnusedWeReadAdapter
    where
        R: AccountLeaseStore,
    {
        async fn list_articles(
            &self,
            _book_id: &str,
            _request: AuthenticatedRequest<'_, R>,
        ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
            unreachable!("the test must stop before browser acquisition")
        }
    }

    #[derive(Clone)]
    struct UnusedArticlePageFetcher;

    #[async_trait::async_trait]
    impl ArticlePageFetcher for UnusedArticlePageFetcher {
        async fn fetch(
            &self,
            _url: crate::domain::source::VerifiedWechatArticleUrl,
            _session: PublicBrowserSession,
        ) -> Result<ExtractedArticlePage, ArticlePageError> {
            unreachable!("the test must stop before browser acquisition")
        }
    }

    #[derive(Clone)]
    struct StaticAccountSelector {
        account_id: Option<WeReadAccountId>,
    }

    #[async_trait::async_trait]
    impl WeReadAccountSelector for StaticAccountSelector {
        async fn select_account(
            &self,
            requested: Option<WeReadAccountId>,
        ) -> Result<Option<WeReadAccountId>, WeReadAdapterError> {
            Ok(requested.or(self.account_id))
        }
    }

    #[derive(Clone)]
    struct AlternatingAccountSelector {
        first: WeReadAccountId,
        second: WeReadAccountId,
        calls: Arc<Mutex<Vec<Option<WeReadAccountId>>>>,
    }

    #[async_trait::async_trait]
    impl WeReadAccountSelector for AlternatingAccountSelector {
        async fn select_account(
            &self,
            requested: Option<WeReadAccountId>,
        ) -> Result<Option<WeReadAccountId>, WeReadAdapterError> {
            let call_index = {
                let mut calls = self.calls.lock().unwrap();
                let call_index = calls.len();
                calls.push(requested);
                call_index
            };
            Ok(requested.or(Some(if call_index == 0 {
                self.first
            } else {
                self.second
            })))
        }
    }

    #[derive(Clone)]
    struct RecordingFallbackAdapter {
        reference: WeReadArticleReference,
        list_account: Arc<Mutex<Option<WeReadAccountId>>>,
        fallback_account: Arc<Mutex<Option<WeReadAccountId>>>,
    }

    #[async_trait::async_trait]
    impl<R> WeReadAdapter<R> for RecordingFallbackAdapter
    where
        R: AccountLeaseStore,
    {
        async fn list_articles(
            &self,
            _book_id: &str,
            request: AuthenticatedRequest<'_, R>,
        ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
            *self.list_account.lock().unwrap() = Some(request.account_id());
            Ok(vec![self.reference.clone()])
        }

        async fn fetch_article_content(
            &self,
            reference: &WeReadArticleReference,
            request: AuthenticatedRequest<'_, R>,
        ) -> Result<ExtractedArticlePage, WeReadAdapterError> {
            *self.fallback_account.lock().unwrap() = Some(request.account_id());
            Ok(ExtractedArticlePage {
                canonical_url: reference
                    .article_url
                    .clone()
                    .expect("the fallback test reference should have a public URL"),
                title: "Fallback title".to_owned(),
                author: None,
                summary: None,
                published_at: Some(Utc::now()),
                content_html: "<p>Fallback body</p>".to_owned(),
                cover_url: None,
            })
        }
    }

    #[derive(Debug, Default)]
    struct FallbackWebDriverState {
        current_url: Mutex<String>,
        navigations: Mutex<Vec<String>>,
        page_source: String,
    }

    async fn fallback_webdriver_handler(
        State(state): State<Arc<FallbackWebDriverState>>,
        request: Request<Body>,
    ) -> Response<Body> {
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        if method == Method::POST && path.ends_with("/session") {
            return webdriver_response(json!({
                "sessionId": "fallback-session",
                "capabilities": {}
            }));
        }
        if method == Method::POST && path.ends_with("/url") {
            let body = to_bytes(request.into_body(), 1_000_000)
                .await
                .expect("WebDriver navigation body should be readable");
            let payload: Value =
                serde_json::from_slice(&body).expect("WebDriver navigation body should be JSON");
            let target = payload
                .get("url")
                .and_then(Value::as_str)
                .expect("WebDriver navigation should contain a URL")
                .to_owned();
            state.navigations.lock().unwrap().push(target.clone());
            *state.current_url.lock().unwrap() = target;
            return webdriver_response(Value::Null);
        }
        if method == Method::GET && path.ends_with("/url") {
            return webdriver_response(json!(state.current_url.lock().unwrap().clone()));
        }
        if method == Method::GET && path.ends_with("/source") {
            return webdriver_response(json!(state.page_source.clone()));
        }
        if method == Method::DELETE && path.contains("/session/") {
            return webdriver_response(Value::Null);
        }
        webdriver_response(Value::Null)
    }

    fn webdriver_response(value: Value) -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(json!({ "value": value }).to_string()))
            .expect("test WebDriver response should be valid")
    }

    async fn start_fallback_webdriver() -> (
        url::Url,
        Arc<FallbackWebDriverState>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test WebDriver listener should bind");
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let state = Arc::new(FallbackWebDriverState {
            current_url: Mutex::new("https://weread.qq.com/web/shelf".to_owned()),
            navigations: Mutex::new(Vec::new()),
            page_source: r#"
                <html><head><meta property="og:title" content="Fallback title"></head>
                <body><div id="js_content"><p>WeRead content</p></div></body></html>
            "#
            .to_owned(),
        });
        let app = Router::new()
            .fallback(fallback_webdriver_handler)
            .with_state(Arc::clone(&state));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test WebDriver server should run");
        });
        (
            endpoint.parse().expect("test WebDriver URL should parse"),
            state,
            server,
        )
    }

    #[derive(Clone)]
    struct FallbackCredentialProvider;

    #[async_trait::async_trait]
    impl WeReadCredentialProvider for FallbackCredentialProvider {
        async fn credentials(
            &self,
            _account_id: WeReadAccountId,
        ) -> Result<WeReadCredentials, WeReadCredentialProviderError> {
            WeReadCredentials::new(
                "access",
                "refresh",
                Utc::now() + Duration::hours(1),
                Utc::now(),
            )
            .expect("test credentials should be valid")
            .with_web_cookie("wr_vid=test; wr_skey=test")
            .map_err(|_| WeReadCredentialProviderError::Unavailable)
        }
    }

    #[derive(Clone)]
    struct BlockedPublicFetcher;

    #[async_trait::async_trait]
    impl ArticlePageFetcher for BlockedPublicFetcher {
        async fn fetch(
            &self,
            _url: crate::domain::source::VerifiedWechatArticleUrl,
            session: PublicBrowserSession,
        ) -> Result<ExtractedArticlePage, ArticlePageError> {
            session
                .close()
                .await
                .expect("the failed public session should be cleaned up");
            Err(ArticlePageError::VerificationRequired)
        }
    }

    fn acquirer_with_account_and_selector(
        repository: MemoryAccountLeaseRepository,
        account_id: Option<WeReadAccountId>,
        selected_account_id: Option<WeReadAccountId>,
    ) -> BrowserSourceSyncAcquirer<
        MemoryAccountLeaseRepository,
        UnusedWeReadAdapter,
        UnusedArticlePageFetcher,
        StaticAccountSelector,
    > {
        let config = BrowserSourceSyncAcquirerConfig::new(
            account_id,
            "source-sync-test",
            Duration::seconds(30),
            Duration::seconds(5),
        )
        .expect("test acquisition policy should be valid");
        BrowserSourceSyncAcquirer::new(
            BrowserSourceSyncAcquirerDependencies {
                browser_pool: BrowserPool::new(1).expect("test pool should be valid"),
                webdriver: WebDriverFactory::new(
                    "http://127.0.0.1:9"
                        .parse()
                        .expect("test URL should be valid"),
                    BrowserEngine::Firefox,
                ),
                account_leases: repository,
                weread: UnusedWeReadAdapter,
                article_pages: UnusedArticlePageFetcher,
                account_selector: StaticAccountSelector {
                    account_id: selected_account_id,
                },
            },
            config,
            0,
        )
    }

    fn acquirer(
        repository: MemoryAccountLeaseRepository,
        account_id: WeReadAccountId,
    ) -> BrowserSourceSyncAcquirer<
        MemoryAccountLeaseRepository,
        UnusedWeReadAdapter,
        UnusedArticlePageFetcher,
        StaticAccountSelector,
    > {
        acquirer_with_account_and_selector(repository, Some(account_id), Some(account_id))
    }

    #[test]
    fn rejects_invalid_lease_policies_before_browser_work() {
        assert_eq!(
            BrowserSourceSyncAcquirerConfig::new(
                Some(WeReadAccountId::from_uuid(Uuid::nil())),
                "worker",
                Duration::seconds(30),
                Duration::seconds(5),
            ),
            Err(BrowserSourceSyncAcquirerConfigError::InvalidAccountId)
        );
        assert_eq!(
            BrowserSourceSyncAcquirerConfig::new(
                Some(account_id()),
                " ",
                Duration::seconds(30),
                Duration::seconds(5),
            ),
            Err(BrowserSourceSyncAcquirerConfigError::EmptyOwner)
        );
        assert_eq!(
            BrowserSourceSyncAcquirerConfig::new(
                Some(account_id()),
                "worker",
                Duration::seconds(5),
                Duration::seconds(5),
            ),
            Err(BrowserSourceSyncAcquirerConfigError::HeartbeatNotShorterThanLease)
        );
    }

    #[test]
    fn derives_distinct_worker_lease_owners() {
        let config = BrowserSourceSyncAcquirerConfig::new(
            Some(account_id()),
            "instance-source-worker",
            Duration::seconds(30),
            Duration::seconds(5),
        )
        .unwrap();
        assert_eq!(config.owner_for(0), "instance-source-worker-0");
        assert_eq!(config.owner_for(7), "instance-source-worker-7");
    }

    #[tokio::test]
    async fn lease_contention_stops_before_authenticated_browser_work() {
        let repository = MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let account_id = account_id();
        let _held = repository
            .acquire(account_id, "another-worker", Duration::seconds(30))
            .await
            .unwrap()
            .expect("the competing worker should acquire the account");

        let result = acquirer(repository, account_id)
            .list_article_references(&source(None))
            .await;

        assert!(matches!(
            result,
            Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::LeaseBackend(message)
            )) if message.contains("already in use")
        ));
    }

    #[tokio::test]
    async fn no_enrolled_account_fails_before_opening_authenticated_browser_work() {
        let result = acquirer_with_account_and_selector(
            MemoryAccountLeaseRepository::new(chrono::Utc::now()),
            None,
            None,
        )
        .list_article_references(&source(None))
        .await;

        assert_eq!(result, Err(SyncAcquisitionError::NoAccountEnrolled));
    }

    #[tokio::test]
    async fn selects_the_source_account_from_the_durable_account_selector() {
        let selected = account_id();
        let repository = MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let _held = repository
            .acquire(selected, "another-worker", Duration::seconds(30))
            .await
            .unwrap()
            .expect("the competing worker should acquire the selected account");

        let result = acquirer_with_account_and_selector(repository, None, Some(selected))
            .list_article_references(&source(None))
            .await;

        assert!(matches!(
            result,
            Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::LeaseBackend(message)
            )) if message.contains("already in use")
        ));
    }

    #[tokio::test]
    async fn selects_a_random_unexpired_enrolled_account_and_honors_overrides() {
        let now = chrono::Utc::now();
        let expired = WeReadAccountId::from_uuid(Uuid::from_u128(2));
        let enrolled = account_id();
        let another_enrolled = WeReadAccountId::from_uuid(Uuid::from_u128(3));
        let repository = MemoryCredentialRepository::new(now);
        repository
            .insert(
                expired,
                "expired",
                b"expired-ciphertext",
                now - chrono::Duration::seconds(1),
            )
            .await
            .unwrap();
        repository
            .insert(
                enrolled,
                "enrolled",
                b"enrolled-ciphertext",
                now + chrono::Duration::hours(1),
            )
            .await
            .unwrap();
        repository
            .insert(
                another_enrolled,
                "another enrolled",
                b"another-enrolled-ciphertext",
                now + chrono::Duration::hours(2),
            )
            .await
            .unwrap();

        let selector = CredentialRepositoryAccountSelector::new(repository);
        assert!(matches!(
            selector.select_account(None).await.unwrap(),
            Some(selected) if selected == enrolled || selected == another_enrolled
        ));
        assert_eq!(selector.select_account(Some(expired)).await.unwrap(), None);
        assert_eq!(
            selector.select_account(Some(enrolled)).await.unwrap(),
            Some(enrolled)
        );
    }

    #[tokio::test]
    async fn random_selection_skips_expired_accounts_and_returns_none_when_all_are_expired() {
        use std::{collections::HashSet, iter::FromIterator};

        use rand::{rngs::StdRng, SeedableRng};

        let now = chrono::Utc::now();
        let expired = WeReadAccountId::from_uuid(Uuid::from_u128(2));
        let enrolled = account_id();
        let another_enrolled = WeReadAccountId::from_uuid(Uuid::from_u128(3));
        let repository = MemoryCredentialRepository::new(now);
        repository
            .insert(
                expired,
                "expired",
                b"expired-ciphertext",
                now - chrono::Duration::seconds(1),
            )
            .await
            .unwrap();
        repository
            .insert(
                enrolled,
                "enrolled",
                b"enrolled-ciphertext",
                now + chrono::Duration::hours(1),
            )
            .await
            .unwrap();
        repository
            .insert(
                another_enrolled,
                "another enrolled",
                b"another-enrolled-ciphertext",
                now + chrono::Duration::hours(2),
            )
            .await
            .unwrap();

        let records = repository.list().await.unwrap();
        let mut selected = HashSet::new();
        for seed in 0..64 {
            let mut rng = StdRng::seed_from_u64(seed);
            selected.insert(choose_random_usable_account(records.clone(), now, &mut rng));
        }
        assert_eq!(
            selected,
            HashSet::from_iter([Some(enrolled), Some(another_enrolled)])
        );

        assert_eq!(
            choose_random_usable_account(
                vec![records
                    .into_iter()
                    .find(|record| record.account().account_id() == expired)
                    .expect("expired record should be present")],
                now,
                &mut StdRng::seed_from_u64(0),
            ),
            None
        );
    }

    #[tokio::test]
    async fn source_account_override_wins_over_configured_default() {
        let configured_account = account_id();
        let source_account = WeReadAccountId::from_uuid(Uuid::from_u128(2));
        let repository = MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let _held = repository
            .acquire(source_account, "another-worker", Duration::seconds(30))
            .await
            .unwrap()
            .expect("the source account should be held by the competing worker");
        let result = acquirer(repository, configured_account)
            .list_article_references(&source(Some(source_account)))
            .await;

        assert!(matches!(
            result,
            Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::LeaseBackend(message)
            )) if message.contains("already in use")
        ));
    }

    #[tokio::test]
    async fn public_article_failure_falls_back_to_authenticated_weread_content() {
        let (webdriver_url, state, server) = start_fallback_webdriver().await;
        let repository = MemoryAccountLeaseRepository::new(Utc::now());
        let selected_account = account_id();
        let config = BrowserSourceSyncAcquirerConfig::new(
            None,
            "source-sync-fallback-test",
            Duration::seconds(30),
            Duration::seconds(5),
        )
        .expect("test acquisition policy should be valid");
        let acquirer = BrowserSourceSyncAcquirer::new(
            BrowserSourceSyncAcquirerDependencies {
                browser_pool: BrowserPool::new(1).expect("test pool should be valid"),
                webdriver: WebDriverFactory::new(webdriver_url, BrowserEngine::Firefox),
                account_leases: repository.clone(),
                weread: crate::acquisition::weread::BrowserWeReadAdapter::new(
                    "https://weread.qq.com/api/mp/cover"
                        .parse()
                        .expect("test endpoint should be valid"),
                )
                .expect("test endpoint should be accepted")
                .with_credential_provider(Arc::new(FallbackCredentialProvider)),
                article_pages: BlockedPublicFetcher,
                account_selector: StaticAccountSelector {
                    account_id: Some(selected_account),
                },
            },
            config,
            0,
        );
        let reference = WeReadArticleReference::new(
            "MP_WXS_book-1_article-1",
            Some(
                "https://mp.weixin.qq.com/s/article-1"
                    .parse()
                    .expect("test article URL should be valid"),
            ),
            Some("List title".to_owned()),
        )
        .expect("test reference should be valid");

        let page = acquirer
            .fetch_article(&source(None), &reference, Some(selected_account))
            .await
            .expect("authenticated content should recover a blocked public article");

        assert_eq!(page.title, "Fallback title");
        assert_eq!(page.content_html, "<p>WeRead content</p>");
        assert_eq!(
            state.navigations.lock().unwrap().clone(),
            vec![
                "https://weread.qq.com/web/shelf".to_owned(),
                "https://weread.qq.com/web/shelf".to_owned(),
                "https://weread.qq.com/web/mp/content?reviewId=MP_WXS_book-1_article-1".to_owned(),
            ]
        );
        assert!(repository
            .acquire(selected_account, "after-fallback", Duration::seconds(30))
            .await
            .expect("the fallback lease should be released")
            .is_some());
        server.abort();
    }

    #[tokio::test]
    async fn authenticated_fallback_reuses_the_account_selected_for_listing() {
        let (webdriver_url, _state, server) = start_fallback_webdriver().await;
        let repository = MemoryAccountLeaseRepository::new(Utc::now());
        let first_account = account_id();
        let second_account = WeReadAccountId::from_uuid(Uuid::from_u128(2));
        let list_account = Arc::new(Mutex::new(None));
        let fallback_account = Arc::new(Mutex::new(None));
        let selector_calls = Arc::new(Mutex::new(Vec::new()));
        let reference = WeReadArticleReference::new(
            "MP_WXS_book-1_article-1",
            Some(
                "https://mp.weixin.qq.com/s/article-1"
                    .parse()
                    .expect("test article URL should be valid"),
            ),
            Some("List title".to_owned()),
        )
        .expect("test reference should be valid");
        let config = BrowserSourceSyncAcquirerConfig::new(
            None,
            "source-sync-account-context-test",
            Duration::seconds(30),
            Duration::seconds(5),
        )
        .expect("test acquisition policy should be valid");
        let acquirer = BrowserSourceSyncAcquirer::new(
            BrowserSourceSyncAcquirerDependencies {
                browser_pool: BrowserPool::new(1).expect("test pool should be valid"),
                webdriver: WebDriverFactory::new(webdriver_url, BrowserEngine::Firefox),
                account_leases: repository,
                weread: RecordingFallbackAdapter {
                    reference,
                    list_account: Arc::clone(&list_account),
                    fallback_account: Arc::clone(&fallback_account),
                },
                article_pages: BlockedPublicFetcher,
                account_selector: AlternatingAccountSelector {
                    first: first_account,
                    second: second_account,
                    calls: Arc::clone(&selector_calls),
                },
            },
            config,
            0,
        );

        let selected = acquirer
            .list_article_references(&source(None))
            .await
            .expect("listing should select the first account");
        let (references, selected_account) = selected.into_parts();
        acquirer
            .fetch_article(&source(None), &references[0], selected_account)
            .await
            .expect("the fallback should use the selected account");

        assert_eq!(*list_account.lock().unwrap(), Some(first_account));
        assert_eq!(*fallback_account.lock().unwrap(), Some(first_account));
        assert_eq!(
            *selector_calls.lock().unwrap(),
            vec![None, Some(first_account)]
        );
        server.abort();
    }

    #[tokio::test]
    async fn missing_public_url_is_rejected_before_opening_a_public_browser() {
        let reference = WeReadArticleReference::new("review-1", None, Some("title".to_owned()))
            .expect("reference should be valid");
        let result = acquirer(
            MemoryAccountLeaseRepository::new(chrono::Utc::now()),
            account_id(),
        )
        .fetch_article(&source(None), &reference, None)
        .await;

        assert!(matches!(
            result,
            Err(SyncAcquisitionError::ArticlePage(
                ArticlePageError::InvalidExtraction(message)
            )) if message.contains("no public URL")
        ));
    }
}
