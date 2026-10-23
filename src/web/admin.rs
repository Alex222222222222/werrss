//! Authenticated single-administrator API and panel.
//!
//! This module is intentionally a thin HTTP adapter. Source mutations and
//! feed-token lifecycle stay in application services; synchronization history
//! stays in its repository read port. All state-changing routes require both
//! the signed admin session and its CSRF token.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    acquisition::identity::{ArticleIdentityResolver, IdentityError, UrlArticleIdentityResolver},
    acquisition::weread_qr::WereadQrHttpTransport,
    application::{
        auth_service::{
            AuthService, AuthServiceError, CredentialProvision, ManualCredentialRefresher,
            RingCredentialCipher,
        },
        config_transfer::{
            ConfigExportSelection, ConfigTransferValidationError, ValidatedConfigBundle,
        },
        feed_token_service::{FeedTokenService, FeedTokenServiceError},
        qr_login::{
            QrAuthenticatedSession, QrLoginError, QrLoginManager, QrLoginPollResult,
            QrLoginService, QrLoginState,
        },
        source_service::{SourceService, SourceServiceError},
    },
    domain::{
        credentials::WeReadAccountId,
        source::{NewSource, SchedulingGate, Source, SourceId, SourcePatch},
        sync::SyncRun,
    },
    persistence::repositories::{
        account_lease_repository::PostgresAccountLeaseRepository,
        config_transfer_repository::{
            ConfigTransferRepositoryError, PostgresConfigTransferRepository,
        },
        credential_repository::PostgresCredentialRepository,
        feed_token_repository::{FeedTokenRepositoryError, PostgresFeedTokenRepository},
        source_repository::{PostgresSourceRepository, SourceRepositoryError},
        sync_run_repository::{
            PostgresSyncRunRepository, SyncRunRepository, SyncRunRepositoryError,
        },
    },
    persistence::unit_of_work::UnitOfWorkFactory,
    web::{
        auth::{AdminAuthenticator, AdminSession, AuthError},
        i18n, ui,
    },
};

type AdminSourceService = SourceService<PostgresSourceRepository, UnitOfWorkFactory>;
type AdminTokenService = FeedTokenService<PostgresFeedTokenRepository>;
type AdminAuthService = AuthService<
    PostgresCredentialRepository,
    PostgresAccountLeaseRepository,
    ManualCredentialRefresher,
    RingCredentialCipher,
>;

/// Shared application dependencies used to build administration routes.
pub struct AdminRouterDependencies {
    auth: AdminAuthenticator,
    sources: AdminSourceService,
    feed_tokens: AdminTokenService,
    sync_runs: PostgresSyncRunRepository,
    config_transfer: PostgresConfigTransferRepository,
    weread_auth: AdminAuthService,
}

impl AdminRouterDependencies {
    /// Creates the shared application dependencies for administration routes.
    pub fn new(
        auth: AdminAuthenticator,
        sources: AdminSourceService,
        feed_tokens: AdminTokenService,
        sync_runs: PostgresSyncRunRepository,
        config_transfer: PostgresConfigTransferRepository,
        weread_auth: AdminAuthService,
    ) -> Self {
        Self {
            auth,
            sources,
            feed_tokens,
            sync_runs,
            config_transfer,
            weread_auth,
        }
    }
}

/// Builds the authenticated administration API and HTML panel routes.
pub fn admin_router(dependencies: AdminRouterDependencies) -> Router {
    admin_router_with_identity_resolver_and_server_root_url(
        dependencies,
        Arc::new(UrlArticleIdentityResolver),
        None,
    )
}

/// Builds the administration routes with the URL-only identity resolver and
/// an optional public server root used for generated feed links.
pub fn admin_router_with_server_root_url(
    dependencies: AdminRouterDependencies,
    server_root_url: Option<url::Url>,
) -> Router {
    admin_router_with_identity_resolver_and_server_root_url(
        dependencies,
        Arc::new(UrlArticleIdentityResolver),
        server_root_url,
    )
}

/// Builds the administration routes with an injected QR-login service and the
/// compatibility URL-only identity resolver.
pub fn admin_router_with_server_root_url_and_qr_login(
    dependencies: AdminRouterDependencies,
    server_root_url: Option<url::Url>,
    qr_login: Arc<dyn QrLoginService>,
) -> Router {
    admin_router_with_identity_resolver_and_server_root_url_and_qr_login(
        dependencies,
        Arc::new(UrlArticleIdentityResolver),
        server_root_url,
        qr_login,
    )
}

/// Builds the administration routes with a configured article identity
/// resolver. Runtime deployments should provide the browser-backed resolver;
/// the compatibility constructor above remains useful for long URLs and tests.
pub fn admin_router_with_identity_resolver(
    dependencies: AdminRouterDependencies,
    identity_resolver: Arc<dyn ArticleIdentityResolver>,
) -> Router {
    admin_router_with_identity_resolver_and_server_root_url(dependencies, identity_resolver, None)
}

/// Builds the administration routes with a configured identity resolver and
/// optional public server root for generated feed links.
pub fn admin_router_with_identity_resolver_and_server_root_url(
    dependencies: AdminRouterDependencies,
    identity_resolver: Arc<dyn ArticleIdentityResolver>,
    server_root_url: Option<url::Url>,
) -> Router {
    admin_router_with_identity_resolver_and_server_root_url_and_qr_login(
        dependencies,
        identity_resolver,
        server_root_url,
        Arc::new(QrLoginManager::new(WereadQrHttpTransport::default())),
    )
}

/// Builds the administration routes with an injected QR-login service.
///
/// The injected boundary keeps HTTP integration tests independent of WeRead
/// and allows deployments to replace the upstream transport with an approved
/// adapter without changing the account API.
pub fn admin_router_with_identity_resolver_and_server_root_url_and_qr_login(
    dependencies: AdminRouterDependencies,
    identity_resolver: Arc<dyn ArticleIdentityResolver>,
    server_root_url: Option<url::Url>,
    qr_login: Arc<dyn QrLoginService>,
) -> Router {
    let AdminRouterDependencies {
        auth,
        sources,
        feed_tokens,
        sync_runs,
        config_transfer,
        weread_auth,
    } = dependencies;
    let state = Arc::new(AdminApiState {
        auth,
        sources,
        feed_tokens,
        sync_runs,
        config_transfer,
        weread_auth,
        identity_resolver,
        server_root_url,
        qr_login,
    });
    Router::new()
        .route("/", get(root_redirect))
        .route("/admin/login", get(login_page))
        .route("/admin", get(admin_page))
        .route("/admin/", get(admin_page))
        .route("/admin/sources/{source_id}", get(source_page))
        .route("/admin/weread/accounts", get(weread_accounts_page))
        .route(
            "/admin/weread/accounts/{account_id}",
            get(weread_account_page),
        )
        .route("/api/admin/login", post(login))
        .route("/api/admin/logout", post(logout))
        .route("/api/admin/config/export", get(export_config))
        .route(
            "/api/admin/config/import/preview",
            post(preview_config_import),
        )
        .route("/api/admin/config/import", post(apply_config_import))
        .route(
            "/api/admin/weread/accounts",
            get(list_weread_accounts).post(provision_weread_account),
        )
        .route(
            "/api/admin/weread/accounts/{account_id}",
            get(get_weread_account)
                .put(replace_weread_account)
                .delete(delete_weread_account),
        )
        .route(
            "/api/admin/weread/accounts/{account_id}/enabled",
            post(set_weread_account_enabled),
        )
        .route("/api/admin/weread/qr", post(start_weread_qr_login))
        .route(
            "/api/admin/weread/qr/{attempt_id}",
            get(poll_weread_qr_login).delete(cancel_weread_qr_login),
        )
        .route("/api/admin/sources", get(list_sources).post(create_source))
        .route(
            "/api/admin/sources/{source_id}",
            get(get_source).put(update_source).delete(delete_source),
        )
        .route("/api/admin/sources/{source_id}/enabled", post(set_enabled))
        .route("/api/admin/sources/{source_id}/gate", post(set_gate))
        .route(
            "/api/admin/sources/{source_id}/feed-token",
            post(issue_feed_token),
        )
        .route(
            "/api/admin/sources/{source_id}/sync-runs",
            get(list_sync_runs),
        )
        .with_state(state)
}

struct AdminApiState {
    auth: AdminAuthenticator,
    sources: AdminSourceService,
    feed_tokens: AdminTokenService,
    sync_runs: PostgresSyncRunRepository,
    config_transfer: PostgresConfigTransferRepository,
    weread_auth: AdminAuthService,
    identity_resolver: Arc<dyn ArticleIdentityResolver>,
    server_root_url: Option<url::Url>,
    qr_login: Arc<dyn QrLoginService>,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct LoginResponse {
    username: String,
    csrf_token: String,
    expires_at: String,
}

/// Manual, non-interactive credential enrollment request.
///
/// This type intentionally does not implement `Debug`: access and refresh
/// tokens must not be included in accidental request logging.
#[derive(Deserialize)]
struct ProvisionWeReadAccountRequest {
    account_id: Option<Uuid>,
    display_name: Option<String>,
    cookie_header: String,
    access_expires_at: String,
}

#[derive(Debug, Serialize)]
struct WeReadAccountResponse {
    account_id: Uuid,
    display_name: String,
    credential_version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_expires_at: Option<String>,
    disabled: bool,
    setup_required: bool,
}

#[derive(Debug, Serialize)]
struct WeReadAccountListResponse {
    account_id: Uuid,
    display_name: String,
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct SetEnabledRequest {
    enabled: bool,
}

#[derive(Deserialize)]
struct StartQrLoginRequest {
    account_id: Option<Uuid>,
    display_name: Option<String>,
}

#[derive(Serialize)]
struct QrLoginStartResponse {
    attempt_id: Uuid,
    account_id: Uuid,
    qr_svg: String,
    expires_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct QrLoginCompletionResponse {
    status: QrLoginState,
    account: WeReadAccountResponse,
}

#[derive(Debug, Deserialize)]
struct ConfigExportQuery {
    sources: bool,
    weread_accounts: bool,
}

#[tracing::instrument(skip_all, level = "debug")]
async fn export_config(
    State(state): State<Arc<AdminApiState>>,
    query: Result<Query<ConfigExportQuery>, axum::extract::rejection::QueryRejection>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return validation_error("invalid configuration export selection"),
    };
    let selection = match ConfigExportSelection::new(query.sources, query.weread_accounts) {
        Ok(selection) => selection,
        Err(error) => return config_transfer_error_response(error.into()),
    };
    match state.config_transfer.export(selection).await {
        Ok(bundle) => match bundle.serialize_limited() {
            Ok(bytes) => {
                tracing::info!(
                    sources = bundle.sources.as_ref().map_or(0, Vec::len),
                    weread_accounts = bundle.weread_accounts.as_ref().map_or(0, Vec::len),
                    "administrator exported configuration"
                );
                let mut response = Response::new(Body::from(bytes));
                *response.status_mut() = StatusCode::OK;
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                response.headers_mut().insert(
                    header::CONTENT_DISPOSITION,
                    HeaderValue::from_static("attachment; filename=\"werrss-config.json\""),
                );
                response
                    .headers_mut()
                    .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
                response
            }
            Err(error) => config_transfer_error_response(error.into()),
        },
        Err(error) => config_transfer_error_response(error),
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn preview_config_import(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if let Err(response) = protected_mutation(&state, &headers) {
        return *response;
    }
    let bytes = match bounded_config_body(body).await {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };
    let bundle = match ValidatedConfigBundle::parse_limited(&bytes) {
        Ok(bundle) => bundle,
        Err(error) => return config_transfer_error_response(error.into()),
    };
    match state.config_transfer.preview(&bundle).await {
        Ok(preview) => {
            tracing::info!(
                sources = preview.sources_created + preview.sources_updated,
                accounts = preview.accounts_created + preview.accounts_updated,
                conflicts = preview.conflicts.len(),
                "administrator previewed configuration import"
            );
            Json(preview).into_response()
        }
        Err(error) => config_transfer_error_response(error),
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn apply_config_import(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let bytes = match bounded_config_body(body).await {
        Ok(bytes) => bytes,
        Err(response) => return response,
    };
    let bundle = match ValidatedConfigBundle::parse_limited(&bytes) {
        Ok(bundle) => bundle,
        Err(error) => return config_transfer_error_response(error.into()),
    };
    let source_count = bundle.sources().map_or(0, <[_]>::len);
    let account_count = bundle.weread_accounts().map_or(0, <[_]>::len);
    match state.config_transfer.apply(bundle).await {
        Ok(result) => {
            tracing::info!(
                username = %session.username(),
                sources = source_count,
                accounts = account_count,
                "administrator applied configuration import"
            );
            Json(result).into_response()
        }
        Err(error) => config_transfer_error_response(error),
    }
}

async fn bounded_config_body(body: Body) -> Result<axum::body::Bytes, Response> {
    match to_bytes(body, crate::application::config_transfer::MAX_BUNDLE_BYTES).await {
        Ok(bytes) => Ok(bytes),
        Err(_) => Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({ "error": "configuration bundle exceeds the 2 MiB limit" })),
        )
            .into_response()),
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn root_redirect() -> Redirect {
    Redirect::temporary("/admin/")
}

#[tracing::instrument(skip_all, level = "debug")]
async fn start_weread_qr_login(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
    Json(request): Json<StartQrLoginRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = match request.account_id {
        Some(value) if !value.is_nil() => Some(WeReadAccountId::from_uuid(value)),
        Some(_) => return validation_error("account_id must be a non-nil UUID"),
        None => None,
    };
    match state.qr_login.start(account_id, request.display_name).await {
        Ok(started) => {
            tracing::info!(account_id = %started.account_id, "administrator started WeRead QR login");
            let mut response = Json(QrLoginStartResponse {
                attempt_id: started.attempt_id,
                account_id: started.account_id.as_uuid(),
                qr_svg: started.qr_svg,
                expires_at: started.expires_at,
            })
            .into_response();
            // The SVG contains a short-lived login capability. Prevent
            // browsers and reverse proxies from storing the start response.
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        }
        Err(error) => {
            tracing::warn!(error = %error, "administrator could not start WeRead QR login");
            qr_login_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn poll_weread_qr_login(
    State(state): State<Arc<AdminApiState>>,
    Path(attempt_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let attempt_id = match Uuid::parse_str(&attempt_id) {
        Ok(value) if !value.is_nil() => value,
        _ => return validation_error("attempt_id must be a non-nil UUID"),
    };
    match state.qr_login.poll(attempt_id).await {
        Ok(QrLoginPollResult::Status(status)) => Json(status).into_response(),
        Ok(QrLoginPollResult::Authenticated {
            account_id,
            requested_display_name,
            session,
        }) => complete_weread_qr_login(&state, account_id, requested_display_name, session).await,
        Err(error) => {
            tracing::warn!(attempt_id = %attempt_id, error = %error, "administrator QR login poll failed");
            qr_login_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn cancel_weread_qr_login(
    State(state): State<Arc<AdminApiState>>,
    Path(attempt_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let attempt_id = match Uuid::parse_str(&attempt_id) {
        Ok(value) if !value.is_nil() => value,
        _ => return validation_error("attempt_id must be a non-nil UUID"),
    };
    match state.qr_login.cancel(attempt_id).await {
        Ok(status) => {
            tracing::info!(attempt_id = %attempt_id, "administrator cancelled WeRead QR login");
            Json(status).into_response()
        }
        Err(error) => {
            tracing::warn!(attempt_id = %attempt_id, error = %error, "administrator could not cancel WeRead QR login");
            qr_login_error_response(error)
        }
    }
}

async fn complete_weread_qr_login(
    state: &AdminApiState,
    account_id: WeReadAccountId,
    requested_display_name: Option<String>,
    session: QrAuthenticatedSession,
) -> Response {
    let display_name = match display_name_from_values(
        requested_display_name.as_deref(),
        session.display_name(),
        session.cookie_header(),
    ) {
        Ok(display_name) => display_name,
        Err(error) => return validation_error(error),
    };
    let credentials = match crate::domain::credentials::WeReadCredentials::new(
        session.access_token().to_owned(),
        session.refresh_token().to_owned(),
        session.access_expires_at(),
        Utc::now(),
    ) {
        Ok(credentials) => match credentials.with_web_cookie(session.cookie_header().to_owned()) {
            Ok(credentials) => credentials,
            Err(_) => return validation_error("QR login returned invalid credentials"),
        },
        Err(_) => return validation_error("QR login returned invalid credentials"),
    };
    let provision = CredentialProvision {
        account_id,
        display_name,
        credentials,
    };
    let existing = match state.weread_auth.account(account_id).await {
        Ok(_) => true,
        Err(AuthServiceError::AccountNotFound { .. }) => false,
        Err(error) => return auth_service_error_response(error),
    };
    let result = if existing {
        state
            .weread_auth
            .replace(provision, "admin:qr-login")
            .await
            .map(|account| (StatusCode::OK, account))
    } else {
        state
            .weread_auth
            .provision(provision)
            .await
            .map(|account| (StatusCode::CREATED, account))
    };
    match result {
        Ok((status, account)) => {
            tracing::info!(account_id = %account.account_id(), status = ?status, "WeRead QR login credentials stored");
            (
                status,
                Json(QrLoginCompletionResponse {
                    status: QrLoginState::Completed,
                    account: WeReadAccountResponse::from(account),
                }),
            )
                .into_response()
        }
        Err(error) => {
            tracing::warn!(account_id = %account_id, error = %error, "WeRead QR login credentials could not be stored");
            auth_service_error_response(error)
        }
    }
}

impl From<crate::domain::credentials::WeReadAccount> for WeReadAccountResponse {
    fn from(account: crate::domain::credentials::WeReadAccount) -> Self {
        Self {
            account_id: account.account_id().as_uuid(),
            display_name: account.display_name().to_owned(),
            credential_version: account.credential_version(),
            access_expires_at: account
                .access_expires_at()
                .map(|expires_at| expires_at.to_rfc3339()),
            disabled: account.disabled(),
            setup_required: account.setup_required(),
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn login(
    State(state): State<Arc<AdminApiState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(request): Json<LoginRequest>,
) -> Response {
    let client_key = client_key(peer);
    match state.auth.login(
        &request.username,
        &request.password,
        &client_key,
        Utc::now(),
    ) {
        Ok((session, cookie)) => {
            tracing::info!("administrator login succeeded");
            let mut response = Json(LoginResponse {
                username: session.username().to_owned(),
                csrf_token: session.csrf_token().to_owned(),
                expires_at: session.expires_at().to_rfc3339(),
            })
            .into_response();
            response.headers_mut().insert(
                header::SET_COOKIE,
                HeaderValue::try_from(AdminAuthenticator::session_cookie(&cookie))
                    .expect("session cookie should be a valid header"),
            );
            response
        }
        Err(error) => {
            tracing::warn!(error = %error, "administrator login failed");
            auth_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn logout(State(state): State<Arc<AdminApiState>>, headers: HeaderMap) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(error) => return auth_error_response(error),
    };
    if let Err(error) = csrf(&state.auth, &session, &headers) {
        return auth_error_response(error);
    }
    let mut response = Json(json!({ "status": "ok" })).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static(AdminAuthenticator::clear_cookie()),
    );
    tracing::info!("administrator logout succeeded");
    response
}

#[tracing::instrument(skip_all, level = "debug")]
async fn provision_weread_account(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
    Json(request): Json<ProvisionWeReadAccountRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = request
        .account_id
        .map(WeReadAccountId::from_uuid)
        .unwrap_or_else(|| WeReadAccountId::from_uuid(Uuid::new_v4()));
    let credentials = match credentials_from_request(&request) {
        Ok(credentials) => credentials,
        Err(response) => return *response,
    };
    let display_name = match display_name_from_request(&request) {
        Ok(display_name) => display_name,
        Err(response) => return *response,
    };
    match state
        .weread_auth
        .provision(CredentialProvision {
            account_id,
            display_name,
            credentials,
        })
        .await
    {
        Ok(account) => {
            tracing::info!(account_id = %account.account_id(), "administrator provisioned a WeRead account");
            (
                StatusCode::CREATED,
                Json(WeReadAccountResponse::from(account)),
            )
                .into_response()
        }
        Err(error) => {
            tracing::warn!(error = %error, "administrator could not provision a WeRead account");
            auth_service_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn replace_weread_account(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ProvisionWeReadAccountRequest>,
) -> Response {
    let session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = match Uuid::parse_str(&account_id) {
        Ok(value) if !value.is_nil() => WeReadAccountId::from_uuid(value),
        _ => return validation_error("account_id must be a non-nil UUID"),
    };
    if request
        .account_id
        .is_some_and(|value| value != account_id.as_uuid())
    {
        return validation_error("account_id in the request must match the URL");
    }
    let credentials = match credentials_from_request(&request) {
        Ok(credentials) => credentials,
        Err(response) => return *response,
    };
    let display_name = match display_name_from_request(&request) {
        Ok(display_name) => display_name,
        Err(response) => return *response,
    };
    match state
        .weread_auth
        .replace(
            CredentialProvision {
                account_id,
                display_name,
                credentials,
            },
            &format!("admin:{}", session.username()),
        )
        .await
    {
        Ok(account) => {
            tracing::info!(account_id = %account.account_id(), "administrator replaced a WeRead account credential");
            Json(WeReadAccountResponse::from(account)).into_response()
        }
        Err(error) => {
            tracing::warn!(error = %error, "administrator could not replace a WeRead account credential");
            auth_service_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn list_weread_accounts(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let now = Utc::now();
    match state.weread_auth.list_accounts().await {
        Ok(accounts) => {
            tracing::debug!(
                accounts = accounts.len(),
                "administrator listed WeRead accounts"
            );
            Json(
                accounts
                    .into_iter()
                    .map(|account| WeReadAccountListResponse {
                        account_id: account.account_id().as_uuid(),
                        display_name: account.display_name().to_owned(),
                        status: weread_account_status(&account, now),
                    })
                    .collect::<Vec<_>>(),
            )
            .into_response()
        }
        Err(error) => {
            tracing::warn!(error = %error, "administrator could not list WeRead accounts");
            auth_service_error_response(error)
        }
    }
}

fn weread_account_status(
    account: &crate::domain::credentials::WeReadAccount,
    now: DateTime<Utc>,
) -> &'static str {
    if account.setup_required() {
        "setup_required"
    } else if account.disabled() {
        "disabled"
    } else if account
        .access_expires_at()
        .is_some_and(|expires_at| expires_at <= now)
    {
        "expired"
    } else {
        "active"
    }
}

fn credentials_from_request(
    request: &ProvisionWeReadAccountRequest,
) -> Result<crate::domain::credentials::WeReadCredentials, Box<Response>> {
    let cookie_header = request.cookie_header.trim();
    let access_expires_at = match DateTime::parse_from_rfc3339(&request.access_expires_at) {
        Ok(value) => value.with_timezone(&Utc),
        Err(_) => {
            return Err(Box::new(validation_error(
                "access_expires_at must be an RFC3339 timestamp",
            )))
        }
    };
    if cookie_value(cookie_header, "wr_vid").is_none() {
        return Err(Box::new(validation_error(
            "cookie_header must contain wr_vid",
        )));
    }
    let access_token = match cookie_value(cookie_header, "wr_skey") {
        Some(value) => value,
        None => {
            return Err(Box::new(validation_error(
                "cookie_header must contain wr_skey",
            )))
        }
    };
    let refresh_token = match cookie_value(cookie_header, "wr_rt") {
        Some(value) => value,
        None => {
            return Err(Box::new(validation_error(
                "cookie_header must contain wr_rt",
            )))
        }
    };
    let issued_at = Utc::now();
    let credentials = match crate::domain::credentials::WeReadCredentials::new(
        access_token,
        refresh_token,
        access_expires_at,
        issued_at,
    ) {
        Ok(credentials) => credentials,
        Err(error) => return Err(Box::new(validation_error(error.to_string()))),
    };
    credentials
        .with_web_cookie(cookie_header.to_owned())
        .map_err(|error| Box::new(validation_error(error.to_string())))
}

fn display_name_from_request(
    request: &ProvisionWeReadAccountRequest,
) -> Result<String, Box<Response>> {
    display_name_from_values(
        request.display_name.as_deref(),
        None,
        request.cookie_header.trim(),
    )
    .map_err(|error| Box::new(validation_error(error)))
}

fn display_name_from_values(
    requested: Option<&str>,
    upstream: Option<&str>,
    cookie_header: &str,
) -> Result<String, &'static str> {
    if let Some(display_name) = requested
        .map(str::trim)
        .filter(|display_name| !display_name.is_empty())
    {
        return Ok(display_name.to_owned());
    }
    if let Some(display_name) = upstream
        .map(str::trim)
        .filter(|display_name| !display_name.is_empty())
    {
        return Ok(display_name.to_owned());
    }

    let encoded_name = cookie_value(cookie_header, "wr_name")
        .ok_or("display_name must be provided or cookie_header must contain a non-empty wr_name")?;
    let display_name = percent_decode_str(&encoded_name)
        .decode_utf8()
        .map_err(|_| "cookie_header wr_name is not valid UTF-8")?;
    let display_name = display_name.trim();
    if display_name.is_empty() {
        return Err(
            "display_name must be provided or cookie_header must contain a non-empty wr_name",
        );
    }
    Ok(display_name.to_owned())
}

#[tracing::instrument(skip_all, level = "debug")]
async fn get_weread_account(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let account_id = match Uuid::parse_str(&account_id) {
        Ok(value) if !value.is_nil() => WeReadAccountId::from_uuid(value),
        _ => return validation_error("account_id must be a non-nil UUID"),
    };
    match state.weread_auth.account(account_id).await {
        Ok(account) => {
            tracing::debug!(account_id = %account.account_id(), "administrator loaded WeRead account");
            Json(WeReadAccountResponse::from(account)).into_response()
        }
        Err(AuthServiceError::AccountNotFound { .. }) => not_found("WeRead account"),
        Err(error) => {
            tracing::warn!(account_id = %account_id, error = %error, "administrator could not load WeRead account");
            auth_service_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn set_weread_account_enabled(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<SetEnabledRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = match parse_account_id(&account_id) {
        Ok(account_id) => account_id,
        Err(response) => return *response,
    };
    match state
        .weread_auth
        .set_disabled(account_id, !request.enabled)
        .await
    {
        Ok(account) => {
            tracing::info!(account_id = %account.account_id(), enabled = request.enabled, "administrator changed WeRead account enabled state");
            Json(WeReadAccountResponse::from(account)).into_response()
        }
        Err(error) => {
            tracing::warn!(account_id = %account_id, error = %error, "administrator could not change WeRead account enabled state");
            auth_service_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn delete_weread_account(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = match parse_account_id(&account_id) {
        Ok(account_id) => account_id,
        Err(response) => return *response,
    };
    match state.weread_auth.delete(account_id).await {
        Ok(()) => {
            tracing::info!(account_id = %account_id, "administrator deleted WeRead account");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => {
            tracing::warn!(account_id = %account_id, error = %error, "administrator could not delete WeRead account");
            auth_service_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn weread_account_page(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(_) => {
            return (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin/login")]).into_response()
        }
    };
    let account_id = match parse_account_id(&account_id) {
        Ok(account_id) => account_id,
        Err(response) => return *response,
    };
    match state.weread_auth.account(account_id).await {
        Ok(account) => ui::weread_account_page(&session, &account, i18n::from_headers(&headers))
            .into_response(),
        Err(AuthServiceError::AccountNotFound { .. }) => not_found("WeRead account"),
        Err(error) => auth_service_error_response(error),
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn weread_accounts_page(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(_) => {
            return (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin/login")]).into_response()
        }
    };
    ui::weread_accounts_page(&session, i18n::from_headers(&headers)).into_response()
}

fn parse_account_id(value: &str) -> Result<WeReadAccountId, Box<Response>> {
    match Uuid::parse_str(value) {
        Ok(value) if !value.is_nil() => Ok(WeReadAccountId::from_uuid(value)),
        _ => Err(Box::new(validation_error(
            "account_id must be a non-nil UUID",
        ))),
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn list_sources(State(state): State<Arc<AdminApiState>>, headers: HeaderMap) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    match state.sources.list().await {
        Ok(sources) => {
            tracing::debug!(sources = sources.len(), "administrator listed sources");
            Json(
                sources
                    .into_iter()
                    .map(SourceResponse::from)
                    .collect::<Vec<_>>(),
            )
            .into_response()
        }
        Err(error) => {
            tracing::warn!(error = %error, "administrator could not list sources");
            application_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn get_source(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.sources.find(source_id).await {
        Ok(Some(source)) => {
            tracing::debug!(source_id = %source_id, "administrator loaded source");
            Json(SourceResponse::from(source)).into_response()
        }
        Ok(None) => not_found("source"),
        Err(error) => {
            tracing::warn!(source_id = %source_id, error = %error, "administrator could not load source");
            application_error_response(error)
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateSourceRequest {
    book_id: Option<String>,
    display_name: Option<String>,
    article_url: Option<String>,
    sync_interval_seconds: Option<i64>,
    rss_item_limit: Option<u32>,
    account_id: Option<Uuid>,
    priority: Option<i32>,
    max_attempts: Option<u32>,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct UpdateSourceRequest {
    /// Omitted fields retain their existing value; an empty string is passed
    /// through to domain validation so clients receive a useful error.
    book_id: Option<String>,
    display_name: Option<String>,
    /// `None` means retain the URL, while `Some(None)` clears it. The custom
    /// deserializer preserves that distinction for explicit JSON `null`.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    article_url: Option<Option<String>>,
    sync_interval_seconds: Option<i64>,
    rss_item_limit: Option<u32>,
    /// `None` means retain the binding, while `Some(None)` clears it. The
    /// custom deserializer preserves that distinction for explicit JSON null.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    account_id: Option<Option<Uuid>>,
    priority: Option<i32>,
    max_attempts: Option<u32>,
}

#[tracing::instrument(skip_all, level = "debug")]
async fn create_source(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
    Json(request): Json<CreateSourceRequest>,
) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(error) => return auth_error_response(error),
    };
    if let Err(error) = csrf(&state.auth, &session, &headers) {
        return auth_error_response(error);
    }
    let article_url = match parse_article_url(request.article_url.as_deref()) {
        Ok(article_url) => article_url,
        Err(error) => return validation_error(error),
    };
    let supplied_book_id = request
        .book_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if supplied_book_id.is_none() && article_url.is_none() {
        return validation_error("book_id or article_url must be provided");
    }
    let identity = if supplied_book_id.is_none() {
        match state
            .identity_resolver
            .resolve(article_url.clone().expect("article URL was checked"))
            .await
        {
            Ok(identity) => Some(identity),
            Err(error) => return identity_error_response(error),
        }
    } else {
        None
    };
    let book_id = supplied_book_id
        .or_else(|| identity.as_ref().map(|value| value.book_id().to_owned()))
        .expect("book ID or identity must exist");
    let display_name = request
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            identity
                .as_ref()
                .and_then(|value| value.account_name().map(str::to_owned))
        })
        .unwrap_or_else(|| book_id.clone());
    let sync_interval = match Duration::try_seconds(request.sync_interval_seconds.unwrap_or(3_600))
    {
        Some(value) => value,
        None => return validation_error("sync_interval_seconds is outside the supported range"),
    };
    let account_id = request.account_id.map(WeReadAccountId::from_uuid);
    let source = NewSource {
        id: SourceId::from_uuid(Uuid::new_v4()),
        book_id,
        display_name,
        article_url,
        enabled: request.enabled.unwrap_or(true),
        sync_interval,
        rss_item_limit: request.rss_item_limit.unwrap_or(20),
        account_id,
        scheduling_gate: SchedulingGate::Ready,
        next_fetch_at: Utc::now(),
        priority: request.priority.unwrap_or(0),
        max_attempts: request.max_attempts.unwrap_or(3),
    };
    match state.sources.create(source).await {
        Ok(source) => {
            tracing::info!(source_id = %source.id(), "administrator created source");
            (StatusCode::CREATED, Json(SourceResponse::from(source))).into_response()
        }
        Err(error) => {
            tracing::warn!(error = %error, "administrator could not create source");
            application_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn update_source(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<UpdateSourceRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let article_url = match request.article_url {
        None => None,
        Some(value) => match parse_article_url(value.as_deref()) {
            Ok(article_url) => Some(article_url),
            Err(error) => return validation_error(error),
        },
    };
    let sync_interval = match request.sync_interval_seconds {
        Some(value) => match Duration::try_seconds(value) {
            Some(value) => Some(value),
            None => {
                return validation_error("sync_interval_seconds is outside the supported range")
            }
        },
        None => None,
    };
    let account_id = request
        .account_id
        .map(|value| value.map(WeReadAccountId::from_uuid));
    let patch = SourcePatch {
        book_id: request.book_id,
        display_name: request.display_name,
        article_url,
        sync_interval,
        rss_item_limit: request.rss_item_limit,
        account_id,
        priority: request.priority,
        max_attempts: request.max_attempts,
    };
    match state.sources.update(source_id, patch).await {
        Ok(source) => {
            tracing::info!(source_id = %source_id, "administrator updated source");
            Json(SourceResponse::from(source)).into_response()
        }
        Err(error) => {
            tracing::warn!(source_id = %source_id, error = %error, "administrator could not update source");
            application_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn delete_source(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.sources.delete(source_id).await {
        Ok(()) => {
            tracing::info!(source_id = %source_id, "administrator deleted source");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => {
            tracing::warn!(source_id = %source_id, error = %error, "administrator could not delete source");
            application_error_response(error)
        }
    }
}

fn parse_article_url(
    value: Option<&str>,
) -> Result<Option<crate::domain::source::VerifiedWechatArticleUrl>, String> {
    match value {
        Some(value) if !value.trim().is_empty() => value
            .parse::<crate::domain::source::VerifiedWechatArticleUrl>()
            .map(Some)
            .map_err(|error| error.to_string()),
        _ => Ok(None),
    }
}

fn deserialize_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

#[derive(Debug, Deserialize)]
struct EnabledRequest {
    enabled: bool,
}

#[tracing::instrument(skip_all, level = "debug")]
async fn set_enabled(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<EnabledRequest>,
) -> Response {
    let session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let _ = session;
    match state.sources.set_enabled(source_id, request.enabled).await {
        Ok(source) => {
            tracing::info!(source_id = %source_id, enabled = request.enabled, "administrator changed source enabled state");
            Json(SourceResponse::from(source)).into_response()
        }
        Err(error) => {
            tracing::warn!(source_id = %source_id, error = %error, "administrator could not change source enabled state");
            application_error_response(error)
        }
    }
}

#[derive(Debug, Deserialize)]
struct GateRequest {
    gate: SchedulingGate,
}

#[tracing::instrument(skip_all, level = "debug")]
async fn set_gate(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<GateRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state
        .sources
        .set_scheduling_gate(source_id, request.gate)
        .await
    {
        Ok(source) => {
            tracing::info!(source_id = %source_id, gate = ?request.gate, "administrator changed source scheduling gate");
            Json(SourceResponse::from(source)).into_response()
        }
        Err(error) => {
            tracing::warn!(source_id = %source_id, error = %error, "administrator could not change source scheduling gate");
            application_error_response(error)
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn issue_feed_token(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.feed_tokens.issue(source_id).await {
        Ok(token) => {
            tracing::info!(source_id = %source_id, "administrator issued a feed token");
            let feed_path = format!("/feeds/{}.xml", token.as_str());
            let feed_url = state
                .server_root_url
                .as_ref()
                .map(|root| absolute_feed_url(root, &feed_path));
            Json(json!({
                "feed_path": feed_path,
                "feed_url": feed_url,
            }))
            .into_response()
        }
        Err(error) => {
            tracing::warn!(source_id = %source_id, error = %error, "administrator could not issue a feed token");
            feed_token_error_response(error)
        }
    }
}

fn absolute_feed_url(root: &url::Url, feed_path: &str) -> String {
    let mut base = root.clone();
    let path = base.path().trim_end_matches('/');
    base.set_path(&format!("{path}/"));
    base.join(feed_path.trim_start_matches('/'))
        .expect("static feed path should join with a valid server root URL")
        .to_string()
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    limit: Option<u32>,
}

#[tracing::instrument(skip_all, level = "debug")]
async fn list_sync_runs(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    Query(query): Query<HistoryQuery>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let limit = query.limit.unwrap_or(20);
    if limit == 0 || limit > 100 {
        return validation_error("limit must be between 1 and 100");
    }
    match state.sources.find(source_id).await {
        Ok(None) => not_found("source"),
        Err(error) => application_error_response(error),
        Ok(Some(_)) => match state.sync_runs.list_for_source(source_id, limit).await {
            Ok(runs) => {
                tracing::debug!(source_id = %source_id, runs = runs.len(), "administrator listed synchronization runs");
                Json(
                    runs.into_iter()
                        .map(SyncRunResponse::from)
                        .collect::<Vec<_>>(),
                )
                .into_response()
            }
            Err(error) => {
                tracing::warn!(source_id = %source_id, error = %error, "administrator could not list synchronization runs");
                sync_run_error_response(error)
            }
        },
    }
}

#[tracing::instrument(skip_all, level = "trace")]
async fn login_page(headers: HeaderMap) -> impl IntoResponse {
    ui::login_page(i18n::from_headers(&headers))
}

#[tracing::instrument(skip_all, level = "debug")]
async fn admin_page(State(state): State<Arc<AdminApiState>>, headers: HeaderMap) -> Response {
    match authenticate(&state.auth, &headers) {
        Ok(session) => ui::admin_page(&session, i18n::from_headers(&headers)).into_response(),
        Err(_) => (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin/login")]).into_response(),
    }
}

#[tracing::instrument(skip_all, level = "debug")]
async fn source_page(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(_) => {
            return (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin/login")]).into_response()
        }
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.sources.find(source_id).await {
        Ok(Some(source)) => {
            ui::source_page(&session, &source, i18n::from_headers(&headers)).into_response()
        }
        Ok(None) => not_found("source"),
        Err(error) => application_error_response(error),
    }
}

fn protected_mutation(
    state: &AdminApiState,
    headers: &HeaderMap,
) -> Result<AdminSession, Box<Response>> {
    let session =
        authenticate(&state.auth, headers).map_err(|error| Box::new(auth_error_response(error)))?;
    csrf(&state.auth, &session, headers).map_err(|error| Box::new(auth_error_response(error)))?;
    Ok(session)
}

fn authenticate(auth: &AdminAuthenticator, headers: &HeaderMap) -> Result<AdminSession, AuthError> {
    let result = auth.authenticate_cookie(
        headers
            .get(header::COOKIE)
            .and_then(|value| value.to_str().ok()),
        Utc::now(),
    );
    if let Err(error) = &result {
        tracing::debug!(error = %error, "admin request did not have a valid session");
    }
    result
}

fn csrf(
    auth: &AdminAuthenticator,
    session: &AdminSession,
    headers: &HeaderMap,
) -> Result<(), AuthError> {
    let result = auth.verify_csrf(
        session,
        headers
            .get("x-csrf-token")
            .and_then(|value| value.to_str().ok()),
    );
    if let Err(error) = &result {
        tracing::warn!(error = %error, "admin mutation failed CSRF validation");
    }
    result
}

fn client_key(peer: SocketAddr) -> String {
    peer.ip().to_string()
}

fn parse_source_id(value: &str) -> Result<SourceId, Box<Response>> {
    Uuid::parse_str(value)
        .map(SourceId::from_uuid)
        .map_err(|_| Box::new(validation_error("source_id must be a UUID")))
}

#[derive(Debug, Serialize)]
struct SourceResponse {
    id: SourceId,
    book_id: String,
    display_name: String,
    article_url: Option<String>,
    enabled: bool,
    sync_interval_seconds: i64,
    rss_item_limit: u32,
    account_id: Option<Uuid>,
    scheduling_gate: SchedulingGate,
    feed_revision: u64,
    next_fetch_at: String,
    failure_cooldown_until: Option<String>,
    schedule_reserved_until: Option<String>,
    priority: i32,
    max_attempts: u32,
}

impl From<Source> for SourceResponse {
    fn from(source: Source) -> Self {
        Self {
            id: source.id(),
            book_id: source.book_id().to_owned(),
            display_name: source.display_name().to_owned(),
            article_url: source.article_url().map(ToString::to_string),
            enabled: source.enabled(),
            sync_interval_seconds: source.sync_interval().num_seconds(),
            rss_item_limit: source.rss_item_limit(),
            account_id: source.account_id().map(WeReadAccountId::as_uuid),
            scheduling_gate: source.scheduling_gate(),
            feed_revision: source.feed_revision().as_u64(),
            next_fetch_at: source.next_fetch_at().to_rfc3339(),
            failure_cooldown_until: source
                .failure_cooldown_until()
                .map(|value| value.to_rfc3339()),
            schedule_reserved_until: source
                .schedule_reserved_until()
                .map(|value| value.to_rfc3339()),
            priority: source.priority(),
            max_attempts: source.max_attempts(),
        }
    }
}

#[derive(Debug, Serialize)]
struct SyncRunResponse {
    id: Uuid,
    source_id: SourceId,
    job_id: Option<Uuid>,
    outcome: &'static str,
    articles_seen: u32,
    articles_created: u32,
    articles_updated: u32,
    articles_failed: u32,
    archived_articles: u32,
    archived_assets: u32,
    failure_class: Option<&'static str>,
    failure_message: Option<String>,
    feed_revision: Option<u64>,
    started_at: String,
    finished_at: Option<String>,
}

impl From<SyncRun> for SyncRunResponse {
    fn from(run: SyncRun) -> Self {
        let stats = run.stats();
        Self {
            id: run.id(),
            source_id: run.source_id(),
            job_id: run.job_id(),
            outcome: run.outcome().as_str(),
            articles_seen: stats.articles_seen,
            articles_created: stats.articles_created,
            articles_updated: stats.articles_updated,
            articles_failed: stats.articles_failed,
            archived_articles: stats.archived_articles,
            archived_assets: stats.archived_assets,
            failure_class: run.failure().map(|failure| failure.class().as_str()),
            failure_message: run.failure().map(|failure| failure.message().to_owned()),
            feed_revision: run.feed_revision().map(|revision| revision.as_u64()),
            started_at: run.started_at().to_rfc3339(),
            finished_at: run.finished_at().map(|value| value.to_rfc3339()),
        }
    }
}

fn auth_error_response(error: AuthError) -> Response {
    tracing::debug!(error = %error, "returning admin authentication error");
    let status = match error {
        AuthError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        AuthError::InvalidCsrf => StatusCode::FORBIDDEN,
        _ => StatusCode::UNAUTHORIZED,
    };
    (status, Json(json!({ "error": error.to_string() }))).into_response()
}

fn config_transfer_error_response(error: ConfigTransferRepositoryError) -> Response {
    let (status, message) = match error {
        ConfigTransferRepositoryError::Conflict(conflict) => {
            tracing::warn!(kind = %conflict.kind, "configuration transfer conflict");
            (
                StatusCode::CONFLICT,
                "configuration conflicts with existing data",
            )
        }
        ConfigTransferRepositoryError::Validation(error) => {
            let status = match error {
                ConfigTransferValidationError::PayloadTooLarge { .. } => {
                    StatusCode::PAYLOAD_TOO_LARGE
                }
                _ => StatusCode::UNPROCESSABLE_ENTITY,
            };
            tracing::warn!(status = %status, "configuration transfer validation failed");
            (status, "invalid configuration bundle")
        }
        ConfigTransferRepositoryError::Storage(_) => {
            tracing::warn!("configuration transfer storage operation failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "configuration transfer temporarily unavailable",
            )
        }
    };
    (status, Json(json!({ "error": message }))).into_response()
}

fn validation_error(message: impl Into<String>) -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({ "error": message.into() })),
    )
        .into_response()
}

fn identity_error_response(error: IdentityError) -> Response {
    tracing::warn!(error = %error, "returning article identity resolution error");
    let status = match error {
        IdentityError::Browser(_) => StatusCode::SERVICE_UNAVAILABLE,
        IdentityError::InvalidArticleUrl(_)
        | IdentityError::InvalidBiz
        | IdentityError::MissingIdentity
        | IdentityError::UnsafeRedirect
        | IdentityError::VerificationRequired => StatusCode::UNPROCESSABLE_ENTITY,
    };
    (status, Json(json!({ "error": error.to_string() }))).into_response()
}

fn auth_service_error_response(error: AuthServiceError) -> Response {
    tracing::warn!(error = %error, "returning WeRead account service error");
    let status = match error {
        AuthServiceError::AccountNotFound { .. } => StatusCode::NOT_FOUND,
        AuthServiceError::Repository(
            crate::persistence::repositories::credential_repository::CredentialRepositoryError::Conflict { .. },
        ) => StatusCode::CONFLICT,
        AuthServiceError::Repository(
            crate::persistence::repositories::credential_repository::CredentialRepositoryError::NotFound { .. },
        ) => StatusCode::NOT_FOUND,
        AuthServiceError::Repository(
            crate::persistence::repositories::credential_repository::CredentialRepositoryError::Invalid(_),
        ) => StatusCode::UNPROCESSABLE_ENTITY,
        AuthServiceError::Credentials(_) => StatusCode::UNPROCESSABLE_ENTITY,
        AuthServiceError::Cipher(_) | AuthServiceError::Repository(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        AuthServiceError::AccountDisabled { .. }
        | AuthServiceError::SetupRequired { .. }
        | AuthServiceError::AccountBusy { .. }
        | AuthServiceError::Lease(_)
        | AuthServiceError::Refresh(_) => StatusCode::SERVICE_UNAVAILABLE,
    };
    let message = match status {
        StatusCode::NOT_FOUND => "WeRead account not found",
        StatusCode::CONFLICT => "a WeRead account with this account_id already exists",
        StatusCode::UNPROCESSABLE_ENTITY => "invalid WeRead credentials",
        _ => "WeRead account service temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

fn qr_login_error_response(error: QrLoginError) -> Response {
    tracing::warn!(error = %error, "returning WeRead QR login error");
    let status = match error {
        QrLoginError::AttemptNotFound => StatusCode::NOT_FOUND,
        QrLoginError::TooManyActiveAttempts => StatusCode::TOO_MANY_REQUESTS,
        QrLoginError::Transport(_) | QrLoginError::InvalidDeadline | QrLoginError::QrCode => {
            StatusCode::SERVICE_UNAVAILABLE
        }
    };
    let message = match status {
        StatusCode::NOT_FOUND => "QR login attempt not found or already consumed",
        StatusCode::TOO_MANY_REQUESTS => "too many QR login attempts are active",
        _ => "WeRead QR login is temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

fn cookie_value(header: &str, name: &str) -> Option<String> {
    header.split(';').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        (key.trim() == name && !value.trim().is_empty()).then(|| value.trim().to_owned())
    })
}

fn not_found(resource: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("{resource} not found") })),
    )
        .into_response()
}

fn application_error_response(error: SourceServiceError) -> Response {
    tracing::warn!(error = %error, "returning source service error");
    let status = match &error {
        SourceServiceError::Source(SourceRepositoryError::BookIdConflict { .. }) => {
            StatusCode::CONFLICT
        }
        SourceServiceError::Source(SourceRepositoryError::NotFound { .. }) => StatusCode::NOT_FOUND,
        SourceServiceError::Source(SourceRepositoryError::Domain(_)) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    let message = match &error {
        SourceServiceError::Source(SourceRepositoryError::BookIdConflict { .. }) => {
            "a source with this book_id already exists"
        }
        SourceServiceError::Source(SourceRepositoryError::NotFound { .. }) => "source not found",
        SourceServiceError::Source(SourceRepositoryError::Domain(error)) => {
            return (status, Json(json!({ "error": error.to_string() }))).into_response()
        }
        _ => "source service temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

fn feed_token_error_response(error: FeedTokenServiceError) -> Response {
    tracing::warn!(error = %error, "returning feed token service error");
    let status = match &error {
        FeedTokenServiceError::Repository(FeedTokenRepositoryError::SourceNotFound { .. }) => {
            StatusCode::NOT_FOUND
        }
        FeedTokenServiceError::Repository(FeedTokenRepositoryError::InvalidSourceId) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        FeedTokenServiceError::Repository(_) => StatusCode::SERVICE_UNAVAILABLE,
        FeedTokenServiceError::InvalidToken => StatusCode::UNPROCESSABLE_ENTITY,
    };
    let message = match error {
        FeedTokenServiceError::Repository(FeedTokenRepositoryError::SourceNotFound { .. }) => {
            "source not found"
        }
        FeedTokenServiceError::Repository(FeedTokenRepositoryError::InvalidSourceId) => {
            "source_id must not be nil"
        }
        FeedTokenServiceError::InvalidToken => "invalid feed token",
        FeedTokenServiceError::Repository(_) => "feed token service temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

fn sync_run_error_response(error: SyncRunRepositoryError) -> Response {
    tracing::warn!(error = %error, "returning synchronization history error");
    let status = match error {
        SyncRunRepositoryError::InvalidLimit => StatusCode::UNPROCESSABLE_ENTITY,
        SyncRunRepositoryError::NotFound { .. } => StatusCode::NOT_FOUND,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    let message = match error {
        SyncRunRepositoryError::InvalidLimit => "invalid synchronization history limit",
        SyncRunRepositoryError::NotFound { .. } => "synchronization run not found",
        _ => "synchronization history temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[tokio::test]
    async fn config_transfer_storage_errors_are_not_returned_to_admin_clients() {
        let response = config_transfer_error_response(ConfigTransferRepositoryError::Storage(
            "postgres://user:password@internal/db private-data".to_owned(),
        ));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error response should be readable");
        let body = String::from_utf8_lossy(&body);
        assert!(!body.contains("password"));
        assert!(!body.contains("private-data"));
        assert!(body.contains("temporarily unavailable"));
    }

    #[tokio::test]
    async fn storage_errors_are_not_returned_to_admin_clients() {
        let response = application_error_response(SourceServiceError::Source(
            SourceRepositoryError::Storage("postgres://user:password@internal/db".to_owned()),
        ));
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error response should be readable");
        let body = String::from_utf8_lossy(&body);
        assert!(!body.contains("password"));
        assert!(body.contains("temporarily unavailable"));
    }

    #[test]
    fn login_rate_limit_key_uses_peer_ip_instead_of_forwarded_headers() {
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 43_210);
        assert_eq!(client_key(peer), "127.0.0.1");
    }

    #[test]
    fn missing_feed_sources_are_not_reported_as_storage_outages() {
        let response = feed_token_error_response(FeedTokenServiceError::Repository(
            FeedTokenRepositoryError::SourceNotFound {
                source_id: SourceId::from_uuid(Uuid::from_u128(42)),
            },
        ));
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn absolute_feed_url_appends_to_a_server_root_path() {
        let root = "https://feeds.example.test/werrss"
            .parse::<url::Url>()
            .expect("test server root URL should parse");

        assert_eq!(
            absolute_feed_url(&root, "/feeds/token.xml"),
            "https://feeds.example.test/werrss/feeds/token.xml"
        );
    }

    fn provisioning_request(
        display_name: Option<&str>,
        cookie_header: &str,
    ) -> ProvisionWeReadAccountRequest {
        ProvisionWeReadAccountRequest {
            account_id: None,
            display_name: display_name.map(str::to_owned),
            cookie_header: cookie_header.to_owned(),
            access_expires_at: "2030-01-01T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn derives_display_name_from_percent_encoded_weread_cookie_name() {
        let request = provisioning_request(None, " \nwr_name=Alex%20Hua; wr_vid=vid\n ");
        assert_eq!(
            display_name_from_request(&request).ok(),
            Some("Alex Hua".to_owned())
        );
    }

    #[test]
    fn explicit_display_name_takes_precedence_over_weread_cookie_name() {
        let request = provisioning_request(Some("  Chosen name \n"), "wr_name=Cookie%20name");
        assert_eq!(
            display_name_from_request(&request).ok(),
            Some("Chosen name".to_owned())
        );
    }

    #[test]
    fn credentials_trim_outer_cookie_header_whitespace_before_storage() {
        let request = provisioning_request(
            Some("Display name"),
            " \nwr_vid=vid; wr_skey=access; wr_rt=refresh\n ",
        );
        let credentials = credentials_from_request(&request).expect("trimmed cookie is valid");

        assert_eq!(
            credentials.web_cookie(),
            Some("wr_vid=vid; wr_skey=access; wr_rt=refresh")
        );
    }

    #[tokio::test]
    async fn blank_display_name_requires_a_valid_nonempty_weread_cookie_name() {
        for cookie_header in ["wr_vid=vid", "wr_name="] {
            let request = provisioning_request(Some("   "), cookie_header);
            let response = display_name_from_request(&request).unwrap_err();
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("validation body should be readable");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"],
                "display_name must be provided or cookie_header must contain a non-empty wr_name"
            );
        }

        let request = provisioning_request(Some("   "), "wr_name=%FF");
        let response = display_name_from_request(&request).unwrap_err();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn account_status_distinguishes_active_expired_and_disabled_accounts() {
        let now = "2026-09-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let active = crate::domain::credentials::WeReadAccount::from_parts(
            WeReadAccountId::from_uuid(Uuid::from_u128(1)),
            "active".to_owned(),
            1,
            Some(now + Duration::hours(1)),
            false,
            false,
        );
        let expired = crate::domain::credentials::WeReadAccount::from_parts(
            WeReadAccountId::from_uuid(Uuid::from_u128(2)),
            "expired".to_owned(),
            1,
            Some(now),
            false,
            false,
        );
        let disabled = crate::domain::credentials::WeReadAccount::from_parts(
            WeReadAccountId::from_uuid(Uuid::from_u128(3)),
            "disabled".to_owned(),
            1,
            Some(now + Duration::hours(1)),
            true,
            false,
        );

        assert_eq!(weread_account_status(&active, now), "active");
        assert_eq!(weread_account_status(&expired, now), "expired");
        assert_eq!(weread_account_status(&disabled, now), "disabled");
    }
}
