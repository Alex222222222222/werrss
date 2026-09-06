//! REST API route and DTO boundary.
//!
//! Documents administrative endpoints for authentication, sources, articles,
//! manual sync, backfill, health/readiness, and job status. It also documents
//! the public/tokenized feed and the optional media route used by archived
//! binary assets.
//!
//! Request DTO validation happens here; use-case sequencing belongs to
//! application services. Access and refresh tokens must never appear in
//! response DTOs, tracing fields, or error messages.
//!
//! The feed handler delegates to `FeedService`, emits its ETag/Last-Modified
//! and freshness metadata, and supports conditional requests. A missing or
//! expired cache is rebuilt synchronously from normalized database records
//! before the response is returned; if another live feed-build lease owns the
//! work, the service waits only for its configured bound and maps an
//! unavailable miss to `503 Service Unavailable` with `Retry-After`.
//!
//! Liveness is process-only. API readiness requires PostgreSQL but remains ready
//! to serve persisted feeds when the browser component is degraded; browser
//! health is reported separately and gates worker claims.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::Duration;
use chrono_tz::Tz;
use serde_json::json;
use sqlx::PgPool;

use crate::{
    application::{
        browser_health::BrowserHealth,
        feed_rebuild_service::FeedRebuilder,
        feed_service::{
            FeedCacheStatus, FeedDelivery, FeedRebuildQueue, FeedService, FeedServiceError,
        },
        feed_token_service::{FeedTokenService, FeedTokenServiceError},
    },
    domain::feed::FeedCache,
    persistence::repositories::{
        asset_repository::{AssetRepairEnqueueResult, PostgresAssetStore},
        feed_cache_repository::FeedCacheRepository,
        feed_token_repository::FeedTokenRepository,
    },
};

/// Builds the public, tokenized RSS route.
///
/// Token resolution and cache delivery remain separate application calls. A
/// syntactically invalid, unknown, or revoked token always receives the same
/// `404` response, while storage failures receive `503` without exposing
/// repository details. The route never starts browser work; a cache rebuild,
/// when needed, uses only normalized database records.
pub fn feed_router<R, C, Q, B>(
    token_service: FeedTokenService<R>,
    feed_service: FeedService<C, Q, B>,
    pool: PgPool,
    timezone: Tz,
) -> Router
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
    B: FeedRebuilder + 'static,
{
    feed_router_with_browser_health_and_assets(
        token_service,
        feed_service,
        pool,
        timezone,
        BrowserHealth::new(timezone),
        None,
    )
}

/// Builds the public feed route with shared browser-health diagnostics.
pub fn feed_router_with_browser_health<R, C, Q, B>(
    token_service: FeedTokenService<R>,
    feed_service: FeedService<C, Q, B>,
    pool: PgPool,
    timezone: Tz,
    browser_health: BrowserHealth,
) -> Router
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
    B: FeedRebuilder + 'static,
{
    feed_router_with_browser_health_and_assets(
        token_service,
        feed_service,
        pool,
        timezone,
        browser_health,
        None,
    )
}

/// Builds the public routes with optional database-backed asset serving.
pub fn feed_router_with_browser_health_and_assets<R, C, Q, B>(
    token_service: FeedTokenService<R>,
    feed_service: FeedService<C, Q, B>,
    pool: PgPool,
    timezone: Tz,
    browser_health: BrowserHealth,
    asset_store: Option<PostgresAssetStore>,
) -> Router
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
    B: FeedRebuilder + 'static,
{
    let state = Arc::new(FeedApiState {
        token_service,
        feed_service,
        pool,
        timezone,
        browser_health,
        asset_store,
    });
    Router::new()
        .route("/api/health", get(liveness))
        .route("/api/ready", get(readiness::<R, C, Q, B>))
        .route("/api/worker/ready", get(worker_readiness::<R, C, Q, B>))
        // Axum does not allow a literal suffix in the same segment as a path
        // parameter, so the handler validates the exact `.xml` shape below.
        .route("/feeds/{*feed_path}", get(feed::<R, C, Q, B>))
        .route("/assets/{asset_id}", get(asset::<R, C, Q, B>))
        .with_state(state)
}

struct FeedApiState<R, C, Q, B> {
    token_service: FeedTokenService<R>,
    feed_service: FeedService<C, Q, B>,
    pool: PgPool,
    timezone: Tz,
    browser_health: BrowserHealth,
    asset_store: Option<PostgresAssetStore>,
}

/// Reports that this process is alive without contacting any dependency.
#[tracing::instrument(skip_all, level = "trace")]
async fn liveness() -> impl IntoResponse {
    tracing::trace!("liveness probe succeeded");
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

/// Reports whether the API can reach its required PostgreSQL dependency.
///
/// The response intentionally contains only stable component status and the
/// configured timezone. Database errors are not returned to callers because
/// they may contain connection details.
#[tracing::instrument(skip_all, level = "debug")]
async fn readiness<R, C, Q, B>(State(state): State<Arc<FeedApiState<R, C, Q, B>>>) -> Response
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
    B: FeedRebuilder + 'static,
{
    let database_check = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await;
    let database_available = database_check.as_ref().is_ok_and(|value| *value == 1);
    if let Err(error) = &database_check {
        tracing::warn!(error = %error, "readiness database check failed");
    }
    let (status, database, status_code) = if database_available {
        ("ready", "ready", StatusCode::OK)
    } else {
        ("not_ready", "unavailable", StatusCode::SERVICE_UNAVAILABLE)
    };
    tracing::debug!(status, database, "readiness probe completed");

    (
        status_code,
        Json(json!({
            "status": status,
            "database": database,
            "timezone": state.timezone.to_string(),
        })),
    )
        .into_response()
}

/// Reports browser-sidecar and browser-timezone readiness independently from
/// process liveness and PostgreSQL API readiness.
#[tracing::instrument(skip_all, level = "debug")]
async fn worker_readiness<R, C, Q, B>(
    State(state): State<Arc<FeedApiState<R, C, Q, B>>>,
) -> Response
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
    B: FeedRebuilder + 'static,
{
    let snapshot = state.browser_health.snapshot();
    let ready = snapshot.worker_ready();
    let status = if ready { "ready" } else { "not_ready" };
    let status_code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    tracing::debug!(
        status,
        webdriver = ?snapshot.webdriver,
        timezone = ?snapshot.timezone,
        "worker readiness probe completed"
    );
    (
        status_code,
        Json(json!({
            "status": status,
            "webdriver": snapshot.webdriver,
            "timezone": snapshot.timezone,
            "configured_timezone": snapshot.configured_timezone,
            "observed_timezone": snapshot.observed_timezone,
        })),
    )
        .into_response()
}

async fn feed<R, C, Q, B>(
    State(state): State<Arc<FeedApiState<R, C, Q, B>>>,
    Path(feed_path): Path<String>,
    headers: HeaderMap,
) -> Response
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
    B: FeedRebuilder + 'static,
{
    tracing::trace!("handling feed request");
    let Some(feed_token) = feed_path
        .strip_suffix(".xml")
        .filter(|token| !token.is_empty() && !token.contains('/'))
    else {
        return empty_response(StatusCode::NOT_FOUND);
    };

    let source_id = match state.token_service.resolve(feed_token).await {
        Ok(Some(source_id)) => source_id,
        Ok(None) | Err(FeedTokenServiceError::InvalidToken) => {
            tracing::debug!("feed request used an unknown or invalid token");
            return empty_response(StatusCode::NOT_FOUND);
        }
        Err(FeedTokenServiceError::Repository(_)) => {
            tracing::warn!("feed token lookup failed");
            return empty_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    tracing::debug!(source_id = %source_id, "resolved feed token");

    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let request = crate::application::feed_service::FeedRequest::new(source_id, if_none_match);

    match state.feed_service.get_feed(request).await {
        Ok(FeedDelivery::Cached { cache, status, .. }) => {
            tracing::debug!(source_id = %source_id, status = ?status, "serving cached feed");
            cached_response(
                StatusCode::OK,
                cache,
                status,
                state.feed_service.stale_while_revalidate(),
            )
        }
        Ok(FeedDelivery::NotModified { cache, status, .. }) => {
            tracing::debug!(source_id = %source_id, status = ?status, "feed is not modified");
            cached_response(
                StatusCode::NOT_MODIFIED,
                cache,
                status,
                state.feed_service.stale_while_revalidate(),
            )
        }
        Ok(FeedDelivery::Unavailable { retry_after, .. }) => {
            tracing::debug!(source_id = %source_id, retry_after_seconds = retry_after.num_seconds(), "feed cache is unavailable");
            let mut response = empty_response(StatusCode::SERVICE_UNAVAILABLE);
            let value = HeaderValue::try_from(retry_after_seconds(retry_after).to_string())
                .expect("numeric Retry-After values are valid headers");
            response.headers_mut().insert(header::RETRY_AFTER, value);
            response
        }
        Err(FeedServiceError::InvalidSourceId | FeedServiceError::Cache(_)) => {
            tracing::warn!(source_id = %source_id, "feed delivery failed");
            empty_response(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

async fn asset<R, C, Q, B>(
    State(state): State<Arc<FeedApiState<R, C, Q, B>>>,
    Path(asset_id): Path<String>,
    headers: HeaderMap,
) -> Response
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
    B: FeedRebuilder + 'static,
{
    let Ok(asset_id) = asset_id.parse::<uuid::Uuid>() else {
        return empty_response(StatusCode::NOT_FOUND);
    };
    let Some(store) = &state.asset_store else {
        return empty_response(StatusCode::NOT_FOUND);
    };
    let asset = match store.read_and_touch(asset_id).await {
        Ok(Some(asset)) => asset,
        Ok(None) => return empty_response(StatusCode::NOT_FOUND),
        Err(error) => {
            tracing::warn!(asset_id = %asset_id, error = %error, "asset read failed");
            return empty_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };
    let asset = if matches!(
        asset,
        crate::archive::asset_store::AssetRead::Missing { .. }
    ) {
        tracing::debug!(asset_id = %asset_id, "asset bytes are missing");
        let reread = match store.enqueue_repair(asset_id).await {
            Ok(AssetRepairEnqueueResult::NotFound | AssetRepairEnqueueResult::Exhausted) => {
                return empty_response(StatusCode::NOT_FOUND)
            }
            Ok(AssetRepairEnqueueResult::Enqueued { job_id }) => {
                tracing::info!(asset_id = %asset_id, job_id = %job_id, "asset repair admitted");
                None
            }
            Ok(AssetRepairEnqueueResult::AlreadyActive { job_id }) => {
                tracing::debug!(asset_id = %asset_id, job_id = %job_id, "asset repair already active");
                None
            }
            Ok(AssetRepairEnqueueResult::AlreadyAvailable) => {
                tracing::debug!(asset_id = %asset_id, "asset was restored while serving cache miss");
                Some(store.read_and_touch(asset_id).await)
            }
            Ok(AssetRepairEnqueueResult::CapacityFull) => {
                tracing::warn!(asset_id = %asset_id, "asset repair capacity is full");
                None
            }
            Ok(AssetRepairEnqueueResult::Backoff) => {
                tracing::debug!(asset_id = %asset_id, "asset repair is in backoff");
                None
            }
            Err(error) => {
                tracing::warn!(asset_id = %asset_id, error = %error, "asset repair admission failed");
                None
            }
        };
        let Some(reread) = reread else {
            return asset_retry_response(store);
        };
        match reread {
            Ok(Some(asset @ crate::archive::asset_store::AssetRead::Available { .. })) => asset,
            Ok(Some(crate::archive::asset_store::AssetRead::Missing { .. })) => {
                tracing::warn!(asset_id = %asset_id, "asset remained unavailable after restoration race");
                return asset_retry_response(store);
            }
            Ok(None) => return empty_response(StatusCode::NOT_FOUND),
            Err(error) => {
                tracing::warn!(asset_id = %asset_id, error = %error, "asset reread failed after restoration race");
                return empty_response(StatusCode::SERVICE_UNAVAILABLE);
            }
        }
    } else {
        asset
    };

    let crate::archive::asset_store::AssetRead::Available {
        media_type,
        checksum,
        bytes,
    } = asset
    else {
        return asset_retry_response(store);
    };

    let etag_value = format!("\"sha256:{checksum}\"");
    let not_modified = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == etag_value);
    let etag = match HeaderValue::try_from(etag_value) {
        Ok(value) => value,
        Err(_) => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let status = if not_modified {
        StatusCode::NOT_MODIFIED
    } else {
        StatusCode::OK
    };
    let content_length = bytes.len();
    let mut response = Response::new(Body::from(if not_modified { Vec::new() } else { bytes }));
    *response.status_mut() = status;
    let response_headers = response.headers_mut();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&media_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    response_headers.insert(header::ETAG, etag);
    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=86400"),
    );
    response_headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    if let Ok(value) = HeaderValue::try_from(content_length.to_string()) {
        // A 304 may carry Content-Length, but it must describe the selected
        // 200 representation rather than the empty response body.
        response_headers.insert(header::CONTENT_LENGTH, value);
    }
    response
}

fn asset_retry_response(store: &PostgresAssetStore) -> Response {
    let mut response = empty_response(StatusCode::SERVICE_UNAVAILABLE);
    let retry_after = store.repair_policy().requeue_after().as_secs().to_string();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        HeaderValue::try_from(retry_after).expect("numeric Retry-After values are valid"),
    );
    response
}

fn cached_response(
    status_code: StatusCode,
    cache: FeedCache,
    status: FeedCacheStatus,
    stale_while_revalidate: Duration,
) -> Response {
    let etag = match HeaderValue::try_from(format!("\"{}\"", cache.etag())) {
        Ok(value) => value,
        Err(_) => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let last_modified = cache
        .updated_at()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let cache_control = match status {
        // The cache repository currently exposes freshness as a boolean. A
        // zero max-age is conservative until it also returns the exact
        // database-clocked remaining lifetime.
        FeedCacheStatus::Fresh => "public, max-age=0, must-revalidate".to_owned(),
        FeedCacheStatus::Stale => format!(
            "public, max-age=0, stale-while-revalidate={}",
            retry_after_seconds(stale_while_revalidate)
        ),
    };
    let mut response = Response::new(Body::from(if status_code == StatusCode::OK {
        cache.xml_bytes().to_vec()
    } else {
        Vec::new()
    }));
    *response.status_mut() = status_code;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/rss+xml; charset=utf-8"),
    );
    headers.insert(header::ETAG, etag);
    if let Ok(value) = HeaderValue::try_from(last_modified) {
        headers.insert(header::LAST_MODIFIED, value);
    }
    if let Ok(value) = HeaderValue::try_from(cache_control) {
        headers.insert(header::CACHE_CONTROL, value);
    }
    response
}

fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

fn retry_after_seconds(delay: Duration) -> u64 {
    delay
        .to_std()
        .map(|delay| {
            delay
                .as_secs()
                .saturating_add(u64::from(delay.subsec_nanos() != 0))
                .max(1)
        })
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, response::IntoResponse};
    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn retry_after_rounds_up_and_never_returns_zero() {
        assert_eq!(retry_after_seconds(Duration::zero()), 1);
        assert_eq!(retry_after_seconds(Duration::milliseconds(1_001)), 2);
        assert_eq!(retry_after_seconds(Duration::seconds(-1)), 1);
    }

    #[test]
    fn http_date_is_rendered_in_a_header_safe_rfc_style() {
        let timestamp = DateTime::<Utc>::from_timestamp(0, 0).expect("epoch should be valid");
        assert_eq!(
            timestamp.format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            "Thu, 01 Jan 1970 00:00:00 GMT"
        );
    }

    #[tokio::test]
    async fn liveness_does_not_require_a_database_connection() {
        let response = liveness().await.into_response();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("liveness body should be readable"),
            r#"{"status":"ok"}"#
        );
    }
}
