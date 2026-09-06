//! PostgreSQL-backed integration coverage for the public feed HTTP boundary.

use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    extract::ConnectInfo,
    http::{header, Request, StatusCode},
    Extension,
};
use chrono::{Duration, Utc};
use chrono_tz::UTC;
use secrecy::SecretString;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use uuid::Uuid;
use werrss::{
    application::{
        auth_service::{
            AuthService, AuthServiceConfig, AuthServiceDependencies, ManualCredentialRefresher,
            RingCredentialCipher,
        },
        browser_health::BrowserHealth,
        feed_rebuild_service::{FeedRebuildConfig, FeedRebuildDependencies, FeedRebuildService},
        feed_service::{
            FeedRebuildJobConfig, FeedService, FeedServiceConfig, PostgresFeedRebuildQueue,
        },
        feed_token_service::FeedTokenService,
        qr_login::{
            QrAuthenticatedSession, QrLoginChallenge, QrLoginManager, QrLoginService,
            QrLoginTransport, QrLoginTransportError, QrLoginTransportPoll,
        },
        source_service::SourceService,
    },
    archive::asset_store::{AssetCachePolicy, AssetRepairPolicy},
    domain::source::{FeedRevision, SourceId},
    persistence::{
        repositories::{
            account_lease_repository::PostgresAccountLeaseRepository,
            article_repository::PostgresArticleRepository,
            asset_repository::PostgresAssetStore,
            credential_repository::PostgresCredentialRepository,
            feed_cache_repository::{
                FeedBuildLeaseRepository, FeedCachePublishResult, FeedCacheRepository,
                FeedCacheTransactionRepository, PostgresFeedBuildLeaseRepository,
                PostgresFeedCacheRepository,
            },
            feed_token_repository::PostgresFeedTokenRepository,
            job_repository::PostgresJobRepository,
            source_repository::PostgresSourceRepository,
            sync_run_repository::PostgresSyncRunRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
    rss::renderer::{RenderArticle, RenderFeedInput, RssRenderer},
    web::{
        admin::{
            admin_router_with_server_root_url, admin_router_with_server_root_url_and_qr_login,
        },
        api::{feed_router, feed_router_with_browser_health_and_assets},
        auth::AdminAuthenticator,
    },
};

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_login_requires_authentication_and_csrf_for_mutations(pool: PgPool) {
    let app = admin_app(&pool);
    let root = app
        .clone()
        .oneshot(get_request("/", None))
        .await
        .expect("root request should complete");
    assert_eq!(root.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(root.headers()[header::LOCATION], "/admin/");

    let slash_admin = app
        .clone()
        .oneshot(get_request("/admin/", None))
        .await
        .expect("slash-admin request should complete");
    assert_eq!(slash_admin.status(), StatusCode::SEE_OTHER);
    assert_eq!(slash_admin.headers()[header::LOCATION], "/admin/login");

    let login = app
        .clone()
        .oneshot(json_request(
            "/api/admin/login",
            serde_json::json!({"username": "admin", "password": "wrong"}),
        ))
        .await
        .expect("login request should complete");
    assert_eq!(login.status(), StatusCode::UNAUTHORIZED);
    assert!(!login.headers().contains_key(header::SET_COOKIE));

    let page = app
        .clone()
        .oneshot(get_request("/admin", None))
        .await
        .expect("admin page request should complete");
    assert_eq!(page.status(), StatusCode::SEE_OTHER);
    assert_eq!(page.headers()[header::LOCATION], "/admin/login");

    let accounts_page = app
        .clone()
        .oneshot(get_request("/admin/weread/accounts", None))
        .await
        .expect("unauthenticated accounts page request should complete");
    assert_eq!(accounts_page.status(), StatusCode::SEE_OTHER);
    assert_eq!(accounts_page.headers()[header::LOCATION], "/admin/login");

    let login = app
        .clone()
        .oneshot(json_request(
            "/api/admin/login",
            serde_json::json!({"username": "admin", "password": "correct horse"}),
        ))
        .await
        .expect("valid login request should complete");
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()[header::SET_COOKIE]
        .to_str()
        .expect("session cookie should be valid")
        .split(';')
        .next()
        .expect("cookie should contain a value")
        .to_owned();
    let login_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(login.into_body(), usize::MAX)
            .await
            .expect("login body should be readable"),
    )
    .expect("login body should be JSON");
    let csrf = login_body["csrf_token"]
        .as_str()
        .expect("CSRF should be returned");

    let page = app
        .clone()
        .oneshot(admin_request("/admin", &cookie, None, None))
        .await
        .expect("authenticated admin page request should complete");
    assert_eq!(page.status(), StatusCode::OK);
    let page_body = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .expect("admin page body should be readable")
            .to_vec(),
    )
    .expect("admin page should be UTF-8");
    assert!(page_body.contains("Werrss admin"));
    assert!(!page_body.contains("correct horse"));
    assert!(page_body.contains("href=\"/admin/weread/accounts\""));

    let accounts_page = app
        .clone()
        .oneshot(admin_request("/admin/weread/accounts", &cookie, None, None))
        .await
        .expect("authenticated accounts page request should complete");
    assert_eq!(accounts_page.status(), StatusCode::OK);
    let accounts_page_body = to_bytes(accounts_page.into_body(), usize::MAX)
        .await
        .expect("accounts page body should be readable");
    let accounts_page_body = String::from_utf8_lossy(&accounts_page_body);
    assert!(
        accounts_page_body.contains("<h1 data-i18n=\"account.list_heading\">WeRead accounts</h1>")
    );
    assert!(accounts_page_body.contains("/api/admin/weread/accounts"));
    assert!(!accounts_page_body.contains("correct horse"));

    let sources = app
        .clone()
        .oneshot(admin_request("/api/admin/sources", &cookie, None, None))
        .await
        .expect("source list request should complete");
    assert_eq!(sources.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(sources.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap(),
        serde_json::json!([])
    );

    let account_id = Uuid::new_v4();
    let account_payload = serde_json::json!({
        "account_id": account_id,
        "display_name": "Primary WeRead",
        "cookie_header": "wr_vid=vid-secret; wr_skey=access-secret-from-form; wr_rt=refresh-secret-from-form",
        "access_expires_at": "2099-01-01T00:00:00Z"
    });
    let missing_account_csrf = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            None,
            Some(account_payload.clone()),
        ))
        .await
        .expect("account mutation should complete");
    assert_eq!(missing_account_csrf.status(), StatusCode::FORBIDDEN);

    let account = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(csrf),
            Some(account_payload),
        ))
        .await
        .expect("account provisioning should complete");
    assert_eq!(account.status(), StatusCode::CREATED);
    let account_body = to_bytes(account.into_body(), usize::MAX)
        .await
        .expect("account response should be readable");
    let account_response: serde_json::Value =
        serde_json::from_slice(&account_body).expect("account response should be JSON");
    assert_eq!(account_response["account_id"], account_id.to_string());
    assert_eq!(account_response["credential_version"], 1);
    assert!(!String::from_utf8_lossy(&account_body).contains("access-secret-from-form"));
    assert!(!String::from_utf8_lossy(&account_body).contains("refresh-secret-from-form"));

    let unauthenticated_accounts = app
        .clone()
        .oneshot(get_request("/api/admin/weread/accounts", None))
        .await
        .expect("unauthenticated account list request should complete");
    assert_eq!(unauthenticated_accounts.status(), StatusCode::UNAUTHORIZED);

    let accounts = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            None,
            None,
        ))
        .await
        .expect("account list request should complete");
    assert_eq!(accounts.status(), StatusCode::OK);
    let accounts_body = to_bytes(accounts.into_body(), usize::MAX)
        .await
        .expect("account list body should be readable");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&accounts_body)
            .expect("account list should be JSON"),
        serde_json::json!([{
            "account_id": account_id,
            "display_name": "Primary WeRead",
            "status": "active"
        }])
    );

    let account_status = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/weread/accounts/{account_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("account status request should complete");
    assert_eq!(account_status.status(), StatusCode::OK);
    let account_status_body = to_bytes(account_status.into_body(), usize::MAX)
        .await
        .expect("account status response should be readable");
    assert!(!String::from_utf8_lossy(&account_status_body).contains("secret-from-form"));

    let account_page = app
        .clone()
        .oneshot(admin_request(
            &format!("/admin/weread/accounts/{account_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("account page request should complete");
    assert_eq!(account_page.status(), StatusCode::OK);
    let account_page_body = to_bytes(account_page.into_body(), usize::MAX)
        .await
        .expect("account page body should be readable");
    let account_page_body = String::from_utf8_lossy(&account_page_body);
    assert!(account_page_body.contains(&account_id.to_string()));
    assert!(account_page_body.contains("/api/admin/weread/accounts/${accountId}/enabled"));
    assert!(!account_page_body.contains("access-secret-from-form"));

    let replacement = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/weread/accounts/{account_id}"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "account_id": account_id,
                "display_name": "Primary WeRead",
                "cookie_header": "wr_vid=vid-secret; wr_skey=rotated-access; wr_rt=rotated-refresh",
                "access_expires_at": "2099-02-01T00:00:00Z"
            })),
            "PUT",
        ))
        .await
        .expect("account replacement should complete");
    assert_eq!(replacement.status(), StatusCode::OK);
    let replacement_body = to_bytes(replacement.into_body(), usize::MAX)
        .await
        .expect("replacement response should be readable");
    let replacement_response: serde_json::Value =
        serde_json::from_slice(&replacement_body).expect("replacement response should be JSON");
    assert_eq!(replacement_response["account_id"], account_id.to_string());
    assert_eq!(replacement_response["credential_version"], 2);
    assert!(!String::from_utf8_lossy(&replacement_body).contains("rotated-access"));

    let disabled = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/weread/accounts/{account_id}/enabled"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({"enabled": false})),
        ))
        .await
        .expect("account disable request should complete");
    assert_eq!(disabled.status(), StatusCode::OK);
    let disabled_body = to_bytes(disabled.into_body(), usize::MAX).await.unwrap();
    let disabled_response: serde_json::Value = serde_json::from_slice(&disabled_body).unwrap();
    assert_eq!(disabled_response["disabled"], true);

    let disabled_list = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            None,
            None,
        ))
        .await
        .expect("disabled account list request should complete");
    let disabled_list_body = to_bytes(disabled_list.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&disabled_list_body).unwrap(),
        serde_json::json!([{
            "account_id": account_id,
            "display_name": "Primary WeRead",
            "status": "disabled"
        }])
    );

    let disabled_replacement = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/weread/accounts/{account_id}"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "account_id": account_id,
                "display_name": "Renamed WeRead",
                "cookie_header": "wr_vid=vid-new; wr_skey=access-new; wr_rt=refresh-new",
                "access_expires_at": "2099-03-01T00:00:00Z"
            })),
            "PUT",
        ))
        .await
        .expect("disabled account replacement should complete");
    assert_eq!(disabled_replacement.status(), StatusCode::OK);
    let disabled_replacement_body = to_bytes(disabled_replacement.into_body(), usize::MAX)
        .await
        .unwrap();
    let disabled_replacement_response: serde_json::Value =
        serde_json::from_slice(&disabled_replacement_body).unwrap();
    assert_eq!(
        disabled_replacement_response["display_name"],
        "Renamed WeRead"
    );
    assert_eq!(disabled_replacement_response["disabled"], true);

    let enabled = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/weread/accounts/{account_id}/enabled"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({"enabled": true})),
        ))
        .await
        .expect("account enable request should complete");
    assert_eq!(enabled.status(), StatusCode::OK);
    let enabled_body = to_bytes(enabled.into_body(), usize::MAX).await.unwrap();
    let enabled_response: serde_json::Value = serde_json::from_slice(&enabled_body).unwrap();
    assert_eq!(enabled_response["disabled"], false);

    let duplicate_account = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "account_id": account_id,
                "display_name": "Duplicate",
                "cookie_header": "wr_vid=vid-secret; wr_skey=duplicate-access; wr_rt=duplicate-refresh",
                "access_expires_at": "2099-04-01T00:00:00Z"
            })),
        ))
        .await
        .expect("duplicate account request should complete");
    assert_eq!(duplicate_account.status(), StatusCode::CONFLICT);

    let invalid_expiry = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "display_name": "Invalid",
                "cookie_header": "wr_vid=vid-invalid; wr_skey=access-invalid; wr_rt=refresh-invalid",
                "access_expires_at": "not-a-timestamp"
            })),
        ))
        .await
        .expect("invalid account request should complete");
    assert_eq!(invalid_expiry.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let invalid_cookie = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "display_name": "Invalid cookie",
                "cookie_header": "wr_vid=vid-only; wr_skey=access-only",
                "access_expires_at": "2099-01-01T00:00:00Z"
            })),
        ))
        .await
        .expect("invalid cookie request should complete");
    assert_eq!(invalid_cookie.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let invalid_account_id = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "account_id": Uuid::nil(),
                "display_name": "Invalid account ID",
                "cookie_header": "wr_vid=vid-invalid; wr_skey=access-invalid; wr_rt=refresh-invalid",
                "access_expires_at": "2099-05-01T00:00:00Z"
            })),
        ))
        .await
        .expect("nil account ID request should complete");
    assert_eq!(
        invalid_account_id.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let invalid_display_name = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "display_name": "   ",
                "cookie_header": "wr_vid=vid-invalid; wr_skey=access-invalid; wr_rt=refresh-invalid",
                "access_expires_at": "2099-06-01T00:00:00Z"
            })),
        ))
        .await
        .expect("blank display name request should complete");
    assert_eq!(
        invalid_display_name.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let mismatched_replacement = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/weread/accounts/{account_id}"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "account_id": Uuid::new_v4(),
                "display_name": "Primary WeRead",
                "cookie_header": "wr_vid=vid-secret; wr_skey=other-access; wr_rt=other-refresh",
                "access_expires_at": "2099-03-01T00:00:00Z"
            })),
            "PUT",
        ))
        .await
        .expect("mismatched replacement request should complete");
    assert_eq!(
        mismatched_replacement.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let deleted = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/weread/accounts/{account_id}"),
            &cookie,
            Some(csrf),
            None,
            "DELETE",
        ))
        .await
        .expect("account delete request should complete");
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

    let deleted_status = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/weread/accounts/{account_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("deleted account status request should complete");
    assert_eq!(deleted_status.status(), StatusCode::NOT_FOUND);

    let deleted_again = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/weread/accounts/{account_id}"),
            &cookie,
            Some(csrf),
            None,
            "DELETE",
        ))
        .await
        .expect("repeated account delete request should complete");
    assert_eq!(deleted_again.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn admin_login_page_negotiates_the_browser_locale() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://user:pass@localhost/werrss")
        .expect("lazy test pool should be constructible");
    let app = admin_app(&pool);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/admin/login")
                .header(header::ACCEPT_LANGUAGE, "fr-FR, en;q=0.8")
                .body(Body::empty())
                .expect("locale request should be valid"),
        )
        .await
        .expect("login page request should complete");

    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("login page body should be readable")
            .to_vec(),
    )
    .expect("login page should be UTF-8");
    assert!(body.contains("document.documentElement.lang='fr'"));
    assert!(body.contains("Bienvenue"));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_mutations_require_csrf(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, _) = admin_session(&app).await;

    let missing_csrf = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            None,
            Some(serde_json::json!({
                "book_id": "book-csrf", "display_name": "CSRF", "article_url": "https://mp.weixin.qq.com/s/csrf"
            })),
        ))
        .await
        .expect("mutation request should complete");
    assert_eq!(missing_csrf.status(), StatusCode::FORBIDDEN);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_update_endpoints(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();

    let created = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "book_id": "book-admin", "display_name": "Admin source", "article_url": "https://mp.weixin.qq.com/s/admin"
            })),
        ))
        .await
        .expect("source creation request should complete");
    assert_eq!(created.status(), StatusCode::CREATED);
    let created_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap()).unwrap();
    let source_id = created_body["id"].as_str().unwrap().to_owned();

    let source_detail = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("source detail request should complete");
    assert_eq!(source_detail.status(), StatusCode::OK);
    let source_detail_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(source_detail.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(source_detail_body["book_id"], "book-admin");
    assert_eq!(source_detail_body["display_name"], "Admin source");

    let source_page = app
        .clone()
        .oneshot(admin_request(
            &format!("/admin/sources/{source_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("source page request should complete");
    assert_eq!(source_page.status(), StatusCode::OK);
    let source_page_body =
        String::from_utf8_lossy(&to_bytes(source_page.into_body(), usize::MAX).await.unwrap())
            .to_string();
    assert!(source_page_body.contains("<h1 data-i18n=\"source.edit_heading\">Edit source</h1>"));
    assert!(source_page_body.contains("name=\"book_id\" value=\"book-admin\""));

    let updated = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/sources/{source_id}"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "book_id": " book-updated ",
                "display_name": " Updated source ",
                "article_url": null,
                "account_id": null,
                "sync_interval_seconds": 7200,
                "rss_item_limit": 10,
                "priority": 4,
                "max_attempts": 5
            })),
            "PUT",
        ))
        .await
        .expect("source update request should complete");
    assert_eq!(updated.status(), StatusCode::OK);
    let updated_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(updated.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(updated_body["book_id"], "book-updated");
    assert_eq!(updated_body["display_name"], "Updated source");
    assert!(updated_body["article_url"].is_null());
    assert_eq!(updated_body["sync_interval_seconds"], 7200);
    assert_eq!(updated_body["rss_item_limit"], 10);
    assert!(updated_body["account_id"].is_null());
    assert_eq!(updated_body["priority"], 4);
    assert_eq!(updated_body["max_attempts"], 5);
    assert_eq!(updated_body["feed_revision"], 1);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_creation_infers_identity_from_article_url(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();

    let inferred = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "article_url": "https://mp.weixin.qq.com/s/inferred?__biz=MTIzNDU%3D&mid=1"
            })),
        ))
        .await
        .expect("source identity inference request should complete");
    assert_eq!(inferred.status(), StatusCode::CREATED);
    let inferred_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(inferred.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(inferred_body["book_id"], "MP_WXS_12345");
    assert_eq!(inferred_body["display_name"], "MP_WXS_12345");
    assert_eq!(
        inferred_body["article_url"],
        "https://mp.weixin.qq.com/s/inferred?__biz=MTIzNDU%3D&mid=1"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_creation_prefers_explicit_book_id(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();

    let explicit_identity = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "book_id": "book-explicit",
                "display_name": "Explicit source",
                "article_url": "https://mp.weixin.qq.com/s/short-without-biz"
            })),
        ))
        .await
        .expect("explicit source identity request should complete");
    assert_eq!(explicit_identity.status(), StatusCode::CREATED);
    let explicit_identity_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(explicit_identity.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(explicit_identity_body["book_id"], "book-explicit");
    assert_eq!(explicit_identity_body["display_name"], "Explicit source");
    assert_eq!(
        explicit_identity_body["article_url"],
        "https://mp.weixin.qq.com/s/short-without-biz"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_update_rejects_conflicting_book_id_without_mutating_source(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();
    let created = create_admin_source(
        &app,
        &cookie,
        csrf,
        serde_json::json!({
            "book_id": "book-updated",
            "display_name": "Admin source",
            "article_url": "https://mp.weixin.qq.com/s/admin"
        }),
    )
    .await;
    let source_id = created["id"].as_str().unwrap().to_owned();

    create_admin_source(
        &app,
        &cookie,
        csrf,
        serde_json::json!({
            "book_id": "book-explicit",
            "display_name": "Existing source",
            "article_url": "https://mp.weixin.qq.com/s/existing"
        }),
    )
    .await;

    let conflicting_update = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/sources/{source_id}"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({"book_id": "book-explicit"})),
            "PUT",
        ))
        .await
        .expect("conflicting source update request should complete");
    assert_eq!(conflicting_update.status(), StatusCode::CONFLICT);

    let after_conflict = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("source lookup after conflicting update should complete");
    let after_conflict_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(after_conflict.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(after_conflict_body["book_id"], "book-updated");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_creation_allows_book_only_sources(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();

    let book_only = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({"book_id": "book-only"})),
        ))
        .await
        .expect("book-only source request should complete");
    assert_eq!(book_only.status(), StatusCode::CREATED);
    let book_only_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(book_only.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(book_only_body["book_id"], "book-only");
    assert_eq!(book_only_body["display_name"], "book-only");
    assert!(book_only_body["article_url"].is_null());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_creation_requires_book_id_or_article_url(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();

    let missing_identity = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({})),
        ))
        .await
        .expect("missing source identity request should complete");
    assert_eq!(missing_identity.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_scheduling_and_feed_token_endpoints(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();
    let created = create_admin_source(
        &app,
        &cookie,
        csrf,
        serde_json::json!({
            "book_id": "book-scheduled",
            "display_name": "Scheduled source",
            "article_url": "https://mp.weixin.qq.com/s/scheduled"
        }),
    )
    .await;
    let source_id = created["id"].as_str().unwrap().to_owned();

    let disabled = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/enabled"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({"enabled": false})),
        ))
        .await
        .expect("source enable mutation should complete");
    assert_eq!(disabled.status(), StatusCode::OK);
    let gated = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/gate"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({"gate": "risk_controlled"})),
        ))
        .await
        .expect("source gate mutation should complete");
    assert_eq!(gated.status(), StatusCode::OK);

    let feed_token = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/feed-token"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({})),
        ))
        .await
        .expect("feed token request should complete");
    assert_eq!(feed_token.status(), StatusCode::OK);
    let feed_body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(feed_token.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    let feed_path = feed_body["feed_path"].as_str().unwrap().to_owned();
    assert!(feed_path.starts_with("/feeds/"));
    assert!(feed_path.ends_with(".xml"));
    assert!(feed_body["feed_url"].is_null());

    let missing_feed_token = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{}/feed-token", Uuid::from_u128(99_999)),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({})),
        ))
        .await
        .expect("missing source token request should complete");
    assert_eq!(missing_feed_token.status(), StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_feed_token_includes_absolute_url_when_server_root_url_is_configured(pool: PgPool) {
    let app = admin_app_with_server_root_url(&pool, Some("https://feeds.example.test/werrss/"));
    let (cookie, csrf_token) = admin_session(&app).await;
    let source = create_admin_source(
        &app,
        &cookie,
        &csrf_token,
        serde_json::json!({
            "book_id": "book-absolute-feed-url",
            "display_name": "Absolute feed URL source",
            "article_url": "https://mp.weixin.qq.com/s/absolute-feed-url"
        }),
    )
    .await;
    let source_id = source["id"].as_str().unwrap();

    let response = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/feed-token"),
            &cookie,
            Some(&csrf_token),
            Some(serde_json::json!({})),
        ))
        .await
        .expect("feed token request should complete");
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .expect("feed token response should be JSON");
    let feed_path = body["feed_path"]
        .as_str()
        .expect("feed path should be returned");
    assert_eq!(
        body["feed_url"],
        format!("https://feeds.example.test/werrss{feed_path}")
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_duplicate_identity_is_rejected(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();
    create_admin_source(
        &app,
        &cookie,
        csrf,
        serde_json::json!({
            "book_id": "book-updated",
            "display_name": "Existing source",
            "article_url": "https://mp.weixin.qq.com/s/existing"
        }),
    )
    .await;

    let duplicate = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "book_id": "book-updated", "display_name": "Duplicate", "article_url": "https://mp.weixin.qq.com/s/duplicate"
            })),
        ))
        .await
        .expect("duplicate source request should complete");
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_history_endpoint_validates_limit(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();
    let created = create_admin_source(
        &app,
        &cookie,
        csrf,
        serde_json::json!({
            "book_id": "book-history",
            "display_name": "History source",
            "article_url": "https://mp.weixin.qq.com/s/history"
        }),
    )
    .await;
    let source_id = created["id"].as_str().unwrap().to_owned();

    let invalid_history = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/sync-runs?limit=0"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("invalid history request should complete");
    assert_eq!(invalid_history.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_source_delete_endpoint_removes_the_source(pool: PgPool) {
    let app = admin_app(&pool);
    let (cookie, csrf_token) = admin_session(&app).await;
    let csrf = csrf_token.as_str();
    let created = create_admin_source(
        &app,
        &cookie,
        csrf,
        serde_json::json!({
            "book_id": "book-delete",
            "display_name": "Delete source",
            "article_url": "https://mp.weixin.qq.com/s/delete"
        }),
    )
    .await;
    let source_id = created["id"].as_str().unwrap().to_owned();

    let deleted = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/sources/{source_id}"),
            &cookie,
            Some(csrf),
            None,
            "DELETE",
        ))
        .await
        .expect("source deletion request should complete");
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

    let deleted_detail = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("deleted source detail request should complete");
    assert_eq!(deleted_detail.status(), StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn every_protected_admin_route_rejects_an_unauthenticated_request(pool: PgPool) {
    let app = admin_app(&pool);
    let account_id = Uuid::from_u128(1);
    let account_payload = serde_json::json!({
        "account_id": account_id,
        "display_name": "test account",
        "cookie_header": "wr_vid=vid; wr_skey=skey; wr_rt=rt",
        "access_expires_at": "2099-01-01T00:00:00Z"
    });
    let source_payload = serde_json::json!({
        "book_id": "test-book",
        "display_name": "test source",
        "article_url": "https://mp.weixin.qq.com/s/test"
    });
    let source_update_payload = serde_json::json!({
        "book_id": "test-book",
        "display_name": "test source",
        "article_url": null,
        "account_id": null,
        "sync_interval_seconds": 3600,
        "rss_item_limit": 20,
        "priority": 0,
        "max_attempts": 3
    });
    let protected_routes = vec![
        ("GET", "/admin", None, StatusCode::SEE_OTHER),
        ("GET", "/admin/", None, StatusCode::SEE_OTHER),
        ("GET", "/admin/weread/accounts", None, StatusCode::SEE_OTHER),
        (
            "GET",
            "/admin/weread/accounts/00000000-0000-0000-0000-000000000001",
            None,
            StatusCode::SEE_OTHER,
        ),
        (
            "GET",
            "/admin/sources/00000000-0000-0000-0000-000000000002",
            None,
            StatusCode::SEE_OTHER,
        ),
        (
            "GET",
            "/api/admin/weread/accounts",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/api/admin/weread/accounts",
            Some(account_payload.clone()),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            "/api/admin/weread/accounts/00000000-0000-0000-0000-000000000001",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "PUT",
            "/api/admin/weread/accounts/00000000-0000-0000-0000-000000000001",
            Some(account_payload),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "DELETE",
            "/api/admin/weread/accounts/00000000-0000-0000-0000-000000000001",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/api/admin/weread/accounts/00000000-0000-0000-0000-000000000001/enabled",
            Some(serde_json::json!({"enabled": true})),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/api/admin/weread/qr",
            Some(serde_json::json!({"account_id": null, "display_name": null})),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            "/api/admin/weread/qr/00000000-0000-0000-0000-000000000001",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "DELETE",
            "/api/admin/weread/qr/00000000-0000-0000-0000-000000000001",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        ("GET", "/api/admin/sources", None, StatusCode::UNAUTHORIZED),
        (
            "POST",
            "/api/admin/sources",
            Some(source_payload),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            "/api/admin/sources/00000000-0000-0000-0000-000000000002",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "PUT",
            "/api/admin/sources/00000000-0000-0000-0000-000000000002",
            Some(source_update_payload),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "DELETE",
            "/api/admin/sources/00000000-0000-0000-0000-000000000002",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/api/admin/sources/00000000-0000-0000-0000-000000000002/enabled",
            Some(serde_json::json!({"enabled": true})),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/api/admin/sources/00000000-0000-0000-0000-000000000002/gate",
            Some(serde_json::json!({"gate": "ready"})),
            StatusCode::UNAUTHORIZED,
        ),
        (
            "POST",
            "/api/admin/sources/00000000-0000-0000-0000-000000000002/feed-token",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        (
            "GET",
            "/api/admin/sources/00000000-0000-0000-0000-000000000002/sync-runs",
            None,
            StatusCode::UNAUTHORIZED,
        ),
        ("POST", "/api/admin/logout", None, StatusCode::UNAUTHORIZED),
    ];

    for (method, path, body, expected_status) in protected_routes {
        let response = app
            .clone()
            .oneshot(admin_request_with_method(path, "", None, body, method))
            .await
            .expect("unauthenticated admin request should complete");
        assert_eq!(response.status(), expected_status, "{method} {path}");
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_can_provision_weread_account_with_empty_optional_cookie_values(pool: PgPool) {
    let app = admin_app(&pool);
    let login = app
        .clone()
        .oneshot(json_request(
            "/api/admin/login",
            serde_json::json!({"username": "admin", "password": "correct horse"}),
        ))
        .await
        .expect("valid login request should complete");
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()[header::SET_COOKIE]
        .to_str()
        .expect("session cookie should be valid")
        .split(';')
        .next()
        .expect("cookie should contain a value")
        .to_owned();
    let login_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(login.into_body(), usize::MAX)
            .await
            .expect("login body should be readable"),
    )
    .expect("login body should be JSON");
    let csrf = login_body["csrf_token"]
        .as_str()
        .expect("CSRF should be returned");
    let expiry = (Utc::now() + Duration::days(30)).to_rfc3339();

    let missing_name = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "account_id": null,
                "display_name": null,
                "cookie_header": "wr_vid=12983214; wr_skey=access; wr_rt=refresh; wr_name=",
                "access_expires_at": expiry,
            })),
        ))
        .await
        .expect("invalid account provisioning request should complete");
    assert_eq!(missing_name.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let missing_name_body = to_bytes(missing_name.into_body(), usize::MAX)
        .await
        .expect("validation body should be readable");
    let missing_name_body: serde_json::Value =
        serde_json::from_slice(&missing_name_body).expect("validation body should be JSON");
    assert_eq!(
        missing_name_body["error"],
        "display_name must be provided or cookie_header must contain a non-empty wr_name"
    );

    let response = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "account_id": null,
                "display_name": null,
                "cookie_header": " \nwr_avatar=; wr_fp=1237823; wr_gender=0; wr_localvid=awefhiauef; wr_name=Alex%20Hua; wr_ql=0; wr_rt=web%400H5swX9Mm95b1~YWmDF_AD; wr_skey=aewi238; wr_vid=12983214; wr_gid=12412424; _qimei_fingerprint=aefhiuawef; _qimei_h38=; _qimei_i_1=aewaefuhi; _qimei_i_2=aewufhiew; _qimei_i_3=awefhbieuwh2839ifde; _qimei_q32=; _qimei_q36=; yybsdk-webId=ahweiufhwiu3829hcf; _qimei_uuid42=wefhu3289hf\n ",
                "access_expires_at": expiry,
            })),
        ))
        .await
        .expect("account provisioning request should complete");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("account response should be readable");
    let account: serde_json::Value =
        serde_json::from_slice(&body).expect("account response should be JSON");
    let account_id = account["account_id"]
        .as_str()
        .expect("new account should receive an ID")
        .parse::<Uuid>()
        .expect("account ID should be a UUID");
    assert!(!account_id.is_nil());
    assert_eq!(account["display_name"], "Alex Hua");
    assert_eq!(account["credential_version"], 1);
    assert_eq!(account["access_expires_at"], expiry);
    assert!(!String::from_utf8_lossy(&body).contains("aewi238"));

    let status = app
        .oneshot(admin_request(
            &format!("/api/admin/weread/accounts/{account_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("account status request should complete");
    assert_eq!(status.status(), StatusCode::OK);
    let status_body = to_bytes(status.into_body(), usize::MAX)
        .await
        .expect("account status response should be readable");
    let status: serde_json::Value =
        serde_json::from_slice(&status_body).expect("account status should be JSON");
    assert_eq!(status["account_id"], account_id.to_string());
    assert_eq!(status["display_name"], "Alex Hua");
    assert_eq!(status["disabled"], false);
}

#[derive(Clone)]
struct TestQrTransport {
    result: Arc<Mutex<Option<Result<QrLoginTransportPoll, QrLoginTransportError>>>>,
}

impl TestQrTransport {
    fn new(result: Result<QrLoginTransportPoll, QrLoginTransportError>) -> Self {
        Self {
            result: Arc::new(Mutex::new(Some(result))),
        }
    }
}

#[async_trait]
impl QrLoginTransport for TestQrTransport {
    async fn begin(&self) -> Result<QrLoginChallenge, QrLoginTransportError> {
        QrLoginChallenge::new("integration-test-uid")
    }

    async fn poll(
        &self,
        _challenge: &QrLoginChallenge,
    ) -> Result<QrLoginTransportPoll, QrLoginTransportError> {
        self.result
            .lock()
            .expect("test QR result mutex should not be poisoned")
            .take()
            .unwrap_or(Ok(QrLoginTransportPoll::Waiting))
    }

    async fn cancel(&self, _challenge: &QrLoginChallenge) -> Result<(), QrLoginTransportError> {
        Ok(())
    }
}

fn test_qr_session() -> QrAuthenticatedSession {
    QrAuthenticatedSession::new(
        "qr-access-secret",
        "qr-refresh-secret",
        "wr_vid=qr-vid; wr_skey=qr-access-secret; wr_rt=qr-refresh-secret; wr_name=QR%20User",
        Utc::now() + Duration::hours(1),
        None,
    )
    .expect("test QR session should be valid")
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_qr_login_requires_admin_authentication_and_csrf(pool: PgPool) {
    let manager = Arc::new(QrLoginManager::new(TestQrTransport::new(Ok(
        QrLoginTransportPoll::Waiting,
    ))));
    let app = admin_app_with_qr_login(&pool, None, Some(manager));

    let unauthenticated = app
        .clone()
        .oneshot(json_request(
            "/api/admin/weread/qr",
            serde_json::json!({"account_id": null, "display_name": null}),
        ))
        .await
        .expect("unauthenticated QR start should complete");
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let (cookie, _) = admin_session(&app).await;
    let missing_csrf = app
        .oneshot(admin_request(
            "/api/admin/weread/qr",
            &cookie,
            None,
            Some(serde_json::json!({"account_id": null, "display_name": null})),
        ))
        .await
        .expect("QR start without CSRF should complete");
    assert_eq!(missing_csrf.status(), StatusCode::FORBIDDEN);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_qr_login_poll_requires_csrf(pool: PgPool) {
    let manager = Arc::new(QrLoginManager::new(TestQrTransport::new(Ok(
        QrLoginTransportPoll::Waiting,
    ))));
    let app = admin_app_with_qr_login(&pool, None, Some(manager));
    let (cookie, csrf) = admin_session(&app).await;
    let start = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/qr",
            &cookie,
            Some(&csrf),
            Some(serde_json::json!({"account_id": null, "display_name": null})),
        ))
        .await
        .expect("QR start should complete");
    assert_eq!(start.status(), StatusCode::OK);
    let start: serde_json::Value = serde_json::from_slice(
        &to_bytes(start.into_body(), usize::MAX)
            .await
            .expect("QR start body should be readable"),
    )
    .expect("QR start should return JSON");
    let attempt_id = start["attempt_id"]
        .as_str()
        .expect("QR start should return an attempt ID")
        .to_owned();

    let missing_csrf = app
        .oneshot(admin_request(
            &format!("/api/admin/weread/qr/{attempt_id}"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("QR poll without CSRF should complete");
    assert_eq!(missing_csrf.status(), StatusCode::FORBIDDEN);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_qr_login_provisions_a_new_account_without_returning_secrets(pool: PgPool) {
    let manager = Arc::new(QrLoginManager::new(TestQrTransport::new(Ok(
        QrLoginTransportPoll::Authenticated(test_qr_session()),
    ))));
    let app = admin_app_with_qr_login(&pool, None, Some(manager));
    let (cookie, csrf) = admin_session(&app).await;

    let start = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/qr",
            &cookie,
            Some(&csrf),
            Some(serde_json::json!({"account_id": null, "display_name": null})),
        ))
        .await
        .expect("QR start should complete");
    assert_eq!(start.status(), StatusCode::OK);
    assert_eq!(
        start
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let start_body = to_bytes(start.into_body(), usize::MAX)
        .await
        .expect("QR start body should be readable");
    let start: serde_json::Value =
        serde_json::from_slice(&start_body).expect("QR start should return JSON");
    assert!(start["qr_svg"].as_str().unwrap().contains("<svg"));
    assert!(!String::from_utf8_lossy(&start_body).contains("qr-access-secret"));
    assert!(!String::from_utf8_lossy(&start_body).contains("integration-test-uid"));
    let attempt_id = start["attempt_id"]
        .as_str()
        .expect("QR start should return an attempt ID")
        .to_owned();

    let completed = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/weread/qr/{attempt_id}"),
            &cookie,
            Some(&csrf),
            None,
        ))
        .await
        .expect("QR poll should complete");
    assert_eq!(completed.status(), StatusCode::CREATED);
    let completed_body = to_bytes(completed.into_body(), usize::MAX)
        .await
        .expect("QR completion body should be readable");
    let completed: serde_json::Value =
        serde_json::from_slice(&completed_body).expect("QR completion should return JSON");
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["account"]["display_name"], "QR User");
    assert!(!String::from_utf8_lossy(&completed_body).contains("qr-access-secret"));
    assert!(!String::from_utf8_lossy(&completed_body).contains("qr-refresh-secret"));

    let account_id = completed["account"]["account_id"]
        .as_str()
        .expect("completion should return account ID")
        .parse::<Uuid>()
        .expect("account ID should be a UUID");
    let (stored_name, ciphertext): (String, Vec<u8>) = sqlx::query_as(
        "SELECT display_name, credentials_ciphertext FROM weread_accounts WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_one(&pool)
    .await
    .expect("QR login should persist an account");
    assert_eq!(stored_name, "QR User");
    assert!(!String::from_utf8_lossy(&ciphertext).contains("qr-access-secret"));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_qr_login_replaces_an_existing_account_without_changing_its_id(pool: PgPool) {
    let manager = Arc::new(QrLoginManager::new(TestQrTransport::new(Ok(
        QrLoginTransportPoll::Authenticated(test_qr_session()),
    ))));
    let app = admin_app_with_qr_login(&pool, None, Some(manager));
    let (cookie, csrf) = admin_session(&app).await;
    let account_id = Uuid::from_u128(42);
    let initial = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/accounts",
            &cookie,
            Some(&csrf),
            Some(serde_json::json!({
                "account_id": account_id,
                "display_name": "Initial name",
                "cookie_header": "wr_vid=old-vid; wr_skey=old-access; wr_rt=old-refresh",
                "access_expires_at": (Utc::now() + Duration::days(30)).to_rfc3339(),
            })),
        ))
        .await
        .expect("initial account provisioning should complete");
    assert_eq!(initial.status(), StatusCode::CREATED);

    let start = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/qr",
            &cookie,
            Some(&csrf),
            Some(serde_json::json!({
                "account_id": account_id,
                "display_name": "QR replacement",
            })),
        ))
        .await
        .expect("QR start should complete");
    assert_eq!(start.status(), StatusCode::OK);
    let start: serde_json::Value = serde_json::from_slice(
        &to_bytes(start.into_body(), usize::MAX)
            .await
            .expect("QR start body should be readable"),
    )
    .expect("QR start should return JSON");
    let attempt_id = start["attempt_id"]
        .as_str()
        .expect("QR start should return an attempt ID")
        .to_owned();

    let completed = app
        .oneshot(admin_request(
            &format!("/api/admin/weread/qr/{attempt_id}"),
            &cookie,
            Some(&csrf),
            None,
        ))
        .await
        .expect("QR poll should complete");
    assert_eq!(completed.status(), StatusCode::OK);
    let completed: serde_json::Value = serde_json::from_slice(
        &to_bytes(completed.into_body(), usize::MAX)
            .await
            .expect("QR completion body should be readable"),
    )
    .expect("QR completion should return JSON");
    assert_eq!(completed["account"]["account_id"], account_id.to_string());
    assert_eq!(completed["account"]["display_name"], "QR replacement");
    assert_eq!(completed["account"]["credential_version"], 2);

    let (stored_name, stored_version): (String, i64) = sqlx::query_as(
        "SELECT display_name, credential_version FROM weread_accounts WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_one(&pool)
    .await
    .expect("replaced account should remain queryable");
    assert_eq!(stored_name, "QR replacement");
    assert_eq!(stored_version, 2);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_qr_login_cancellation_consumes_the_attempt(pool: PgPool) {
    let manager = Arc::new(QrLoginManager::new(TestQrTransport::new(Ok(
        QrLoginTransportPoll::Waiting,
    ))));
    let app = admin_app_with_qr_login(&pool, None, Some(manager));
    let (cookie, csrf) = admin_session(&app).await;
    let start = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/weread/qr",
            &cookie,
            Some(&csrf),
            Some(serde_json::json!({"account_id": null, "display_name": "To cancel"})),
        ))
        .await
        .expect("QR start should complete");
    let start: serde_json::Value = serde_json::from_slice(
        &to_bytes(start.into_body(), usize::MAX)
            .await
            .expect("QR start body should be readable"),
    )
    .expect("QR start should return JSON");
    let attempt_id = start["attempt_id"].as_str().unwrap();

    let cancelled = app
        .clone()
        .oneshot(admin_request_with_method(
            &format!("/api/admin/weread/qr/{attempt_id}"),
            &cookie,
            Some(&csrf),
            None,
            "DELETE",
        ))
        .await
        .expect("QR cancellation should complete");
    assert_eq!(cancelled.status(), StatusCode::OK);
    let cancelled: serde_json::Value = serde_json::from_slice(
        &to_bytes(cancelled.into_body(), usize::MAX)
            .await
            .expect("cancellation body should be readable"),
    )
    .expect("cancellation should return JSON");
    assert_eq!(cancelled["status"], "cancelled");

    let after_cancel = app
        .oneshot(admin_request(
            &format!("/api/admin/weread/qr/{attempt_id}"),
            &cookie,
            Some(&csrf),
            None,
        ))
        .await
        .expect("poll after cancellation should complete");
    assert_eq!(after_cancel.status(), StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_route_serves_xml_and_honors_conditional_requests(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    publish_cache(&pool, source_id).await;
    let token = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()))
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let app = router(&pool);
    let path = format!("/feeds/{}.xml", token.as_str());

    let response = app
        .clone()
        .oneshot(get_request(&path, None))
        .await
        .expect("feed request should complete");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/rss+xml; charset=utf-8")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("public, max-age=0, must-revalidate")
    );
    assert!(response.headers().contains_key(header::LAST_MODIFIED));
    let etag = response
        .headers()
        .get(header::ETAG)
        .expect("feed response should include an ETag")
        .to_str()
        .expect("ETag should be valid text")
        .to_owned();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("feed body should be readable");
    assert!(String::from_utf8_lossy(&body).contains("API integration article"));

    let response = app
        .oneshot(get_request(&path, Some(&etag)))
        .await
        .expect("conditional feed request should complete");
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("304 body should be readable")
            .len(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn health_routes_report_liveness_and_database_readiness(pool: PgPool) {
    let app = router(&pool);

    let liveness = app
        .clone()
        .oneshot(get_request("/api/health", None))
        .await
        .expect("liveness request should complete");
    assert_eq!(liveness.status(), StatusCode::OK);
    assert_eq!(liveness.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(
        to_bytes(liveness.into_body(), usize::MAX)
            .await
            .expect("liveness body should be readable"),
        r#"{"status":"ok"}"#
    );

    let readiness = app
        .oneshot(get_request("/api/ready", None))
        .await
        .expect("readiness request should complete");
    assert_eq!(readiness.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(readiness.into_body(), usize::MAX)
                .await
                .expect("readiness body should be readable"),
        )
        .expect("readiness body should be JSON"),
        serde_json::json!({
            "status": "ready",
            "database": "ready",
            "timezone": "UTC"
        })
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn readiness_returns_service_unavailable_when_database_is_closed(pool: PgPool) {
    let app = router(&pool);
    pool.close().await;

    let response = app
        .oneshot(get_request("/api/ready", None))
        .await
        .expect("readiness request should complete even when the database is closed");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("readiness body should be readable"),
        )
        .expect("readiness body should be JSON"),
        serde_json::json!({
            "status": "not_ready",
            "database": "unavailable",
            "timezone": "UTC"
        })
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn worker_readiness_reports_browser_components_before_the_first_probe(pool: PgPool) {
    let response = router(&pool)
        .oneshot(get_request("/api/worker/ready", None))
        .await
        .expect("worker readiness request should complete");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("worker readiness body should be readable"),
        )
        .expect("worker readiness body should be JSON"),
        serde_json::json!({
            "status": "not_ready",
            "webdriver": "unknown",
            "timezone": "unknown",
            "configured_timezone": "UTC",
            "observed_timezone": null
        })
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_route_does_not_enumerate_invalid_unknown_or_revoked_tokens(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    let token_service = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()));
    let token = token_service
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let unknown = werrss::domain::feed_token::FeedToken::generate();
    let app = router(&pool);

    let invalid_status = status(&app, "/feeds/not-a-token.xml").await;
    let unknown_status = status(&app, &format!("/feeds/{}.xml", unknown.as_str())).await;
    token_service
        .revoke(source_id)
        .await
        .expect("token should be revoked");
    let revoked_status = status(&app, &format!("/feeds/{}.xml", token.as_str())).await;

    assert_eq!(invalid_status, StatusCode::NOT_FOUND);
    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    assert_eq!(revoked_status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_route_rejects_non_xml_and_nested_paths(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    let token = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()))
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let app = router(&pool);
    let raw_token = token.as_str();

    assert_eq!(
        status(&app, &format!("/feeds/{raw_token}")).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(&app, &format!("/feeds/{raw_token}.json")).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(&app, &format!("/feeds/{raw_token}.xml/extra")).await,
        StatusCode::NOT_FOUND
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_route_builds_and_returns_a_feed_on_a_cache_miss(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    let token = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()))
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let app = router(&pool);
    let path = format!("/feeds/{}.xml", token.as_str());

    let response = app
        .oneshot(get_request(&path, None))
        .await
        .expect("cache-miss request should complete");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/rss+xml; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("cache-miss body should be readable");
    let body = String::from_utf8(body.to_vec()).expect("feed body should be UTF-8");
    assert!(body.contains("<rss"));
    assert!(body.contains("API integration source"));

    let cached = PostgresFeedCacheRepository::new(pool.clone())
        .get(source_id)
        .await
        .expect("rebuilt cache should be queryable")
        .expect("cache miss should publish a cache row");
    assert!(cached.is_fresh());
    assert_eq!(cached.cache().xml_bytes(), body.as_bytes());

    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE source_id = $1 AND job_type = 'feed_rebuild'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("rebuild job should be queryable");
    assert_eq!(queued, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn disabled_asset_cache_does_not_expose_asset_route(pool: PgPool) {
    let app = router(&pool);
    let response = app
        .oneshot(get_request(&format!("/assets/{}", Uuid::new_v4()), None))
        .await
        .expect("disabled asset request should complete");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_route_serves_cached_bytes_and_honors_etag(pool: PgPool) {
    let asset_id = insert_asset_fixture(&pool).await;
    let app = router_with_assets(
        &pool,
        Some(PostgresAssetStore::new(
            pool.clone(),
            AssetCachePolicy::default(),
        )),
    );
    let path = format!("/assets/{asset_id}");

    let response = app
        .clone()
        .oneshot(get_request(&path, None))
        .await
        .expect("asset request should complete");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "image/png");
    assert_eq!(response.headers()[header::CONTENT_LENGTH], "15");
    assert_eq!(
        response.headers()[header::ETAG],
        "\"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""
    );
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    let etag = response.headers()[header::ETAG]
        .to_str()
        .expect("asset ETag should be valid")
        .to_owned();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("asset body should be readable");
    assert_eq!(body.as_ref(), b"\x89PNG\r\n\x1a\nfixture");

    let not_modified = app
        .oneshot(get_request(&path, Some(&etag)))
        .await
        .expect("conditional asset request should complete");
    assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(not_modified.headers()[header::CONTENT_LENGTH], "15");
    assert_eq!(
        to_bytes(not_modified.into_body(), usize::MAX)
            .await
            .expect("empty conditional body should be readable")
            .len(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_route_reports_a_repairable_cache_miss_and_retains_metadata(pool: PgPool) {
    let asset_id = insert_asset_fixture(&pool).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL")
        .execute(&pool)
        .await
        .expect("asset data should be evictable");
    let app = router_with_assets(
        &pool,
        Some(
            PostgresAssetStore::new(pool.clone(), AssetCachePolicy::default()).with_repair_policy(
                AssetRepairPolicy::new(10, 10, std::time::Duration::from_secs(37), 3).unwrap(),
            ),
        ),
    );

    let response = app
        .oneshot(get_request(&format!("/assets/{asset_id}"), None))
        .await
        .expect("missing asset request should complete");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "37");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(asset_id)
            .fetch_one(&pool)
            .await
            .expect("asset metadata should remain queryable"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_repair_jobs AS arj
             JOIN jobs AS j ON j.id = arj.job_id
             WHERE arj.asset_record_id = $1 AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .expect("repair job should be queryable"),
        1
    );

    let second = router_with_assets(
        &pool,
        Some(PostgresAssetStore::new(
            pool.clone(),
            AssetCachePolicy::default(),
        )),
    )
    .oneshot(get_request(&format!("/assets/{asset_id}"), None))
    .await
    .expect("repeated missing asset request should complete");
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM asset_repair_jobs AS arj
             JOIN jobs AS j ON j.id = arj.job_id
             WHERE arj.asset_record_id = $1 AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')",
        )
        .bind(asset_id)
        .fetch_one(&pool)
        .await
        .expect("deduplicated repair job should be queryable"),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_route_serves_bytes_when_repair_race_restores_cache(pool: PgPool) {
    let asset_id = insert_asset_fixture(&pool).await;
    sqlx::query("UPDATE asset_blobs SET data = NULL")
        .execute(&pool)
        .await
        .expect("asset data should be evictable");

    let mut repair_lock = pool
        .begin()
        .await
        .expect("repair admission lock transaction should start");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0))")
        .execute(&mut *repair_lock)
        .await
        .expect("repair admission lock should be acquired");

    let mut read_lock = pool
        .begin()
        .await
        .expect("asset read lock transaction should start");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *read_lock)
        .await
        .expect("asset capacity lock should be acquired");

    let app = router_with_assets(
        &pool,
        Some(
            PostgresAssetStore::new(pool.clone(), AssetCachePolicy::default()).with_repair_policy(
                AssetRepairPolicy::new(10, 10, std::time::Duration::from_secs(37), 3).unwrap(),
            ),
        ),
    );
    let request = tokio::spawn(async move {
        app.oneshot(get_request(&format!("/assets/{asset_id}"), None))
            .await
            .expect("asset request should complete")
    });

    read_lock
        .commit()
        .await
        .expect("asset capacity lock should be released");
    wait_for_advisory_waiter(&pool, "asset-repair-admission").await;

    sqlx::query("UPDATE asset_blobs SET data = $1")
        .bind(b"\x89PNG\r\n\x1a\nfixture".as_slice())
        .execute(&pool)
        .await
        .expect("restored asset data should be written");
    repair_lock
        .commit()
        .await
        .expect("repair admission lock should be released");

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), request)
        .await
        .expect("asset request should not remain blocked")
        .expect("asset request task should not panic");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("restored asset body should be readable")
            .as_ref(),
        b"\x89PNG\r\n\x1a\nfixture"
    );
}

fn router(pool: &PgPool) -> axum::Router {
    router_with_assets(pool, None)
}

fn router_with_assets(pool: &PgPool, asset_store: Option<PostgresAssetStore>) -> axum::Router {
    let token_service = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()));
    let queue = PostgresFeedRebuildQueue::new(
        PostgresJobRepository::new(pool.clone()),
        FeedRebuildJobConfig::default(),
    );
    let rebuild_service = FeedRebuildService::new(
        FeedRebuildDependencies::new(
            PostgresSourceRepository::new(pool.clone()),
            PostgresArticleRepository::new(pool.clone()),
            PostgresFeedBuildLeaseRepository::new(pool.clone()),
            UnitOfWorkFactory::new(pool.clone()),
        ),
        FeedRebuildConfig::new(
            Duration::minutes(5),
            Duration::minutes(30),
            "https://feeds.example.test/werrss.xml",
            "API integration feed",
        )
        .expect("feed rebuild configuration should be valid"),
        "api-feed-builder",
    )
    .expect("feed rebuild service should be constructible");
    let feed_service = FeedService::new(
        PostgresFeedCacheRepository::new(pool.clone()),
        queue,
        rebuild_service,
        FeedServiceConfig::default(),
    );
    if asset_store.is_none() {
        return feed_router(token_service, feed_service, pool.clone(), UTC);
    }
    feed_router_with_browser_health_and_assets(
        token_service,
        feed_service,
        pool.clone(),
        UTC,
        BrowserHealth::new(UTC),
        asset_store,
    )
}

fn get_request(path: &str, etag: Option<&str>) -> Request<Body> {
    let mut request = Request::builder().uri(path);
    if let Some(etag) = etag {
        request = request.header(header::IF_NONE_MATCH, etag);
    }
    request
        .body(Body::empty())
        .expect("request should be valid")
}

fn json_request(path: &str, value: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(value.to_string()))
        .expect("JSON request should be valid")
}

fn admin_request(
    path: &str,
    cookie: &str,
    csrf: Option<&str>,
    body: Option<serde_json::Value>,
) -> Request<Body> {
    let method = if body.is_some() { "POST" } else { "GET" };
    admin_request_with_method(path, cookie, csrf, body, method)
}

fn admin_request_with_method(
    path: &str,
    cookie: &str,
    csrf: Option<&str>,
    body: Option<serde_json::Value>,
    method: &str,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::COOKIE, cookie);
    if let Some(csrf) = csrf {
        builder = builder.header("x-csrf-token", csrf);
    }
    if let Some(body) = body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        builder
            .body(Body::from(body.to_string()))
            .expect("admin JSON request should be valid")
    } else {
        builder
            .body(Body::empty())
            .expect("admin request should be valid")
    }
}

fn admin_app(pool: &PgPool) -> axum::Router {
    admin_app_with_server_root_url(pool, None)
}

fn admin_app_with_server_root_url(pool: &PgPool, root: Option<&str>) -> axum::Router {
    admin_app_with_qr_login(pool, root, None)
}

fn admin_app_with_qr_login(
    pool: &PgPool,
    root: Option<&str>,
    qr_login: Option<Arc<dyn QrLoginService>>,
) -> axum::Router {
    let auth = AdminAuthenticator::new(
        "admin".to_owned(),
        SecretString::new("correct horse".to_owned().into_boxed_str()),
        SecretString::new("independent signing key".to_owned().into_boxed_str()),
    )
    .expect("test admin auth should be valid");
    let sources = SourceService::new(
        PostgresSourceRepository::new(pool.clone()),
        UnitOfWorkFactory::new(pool.clone()),
    );
    let feed_tokens = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()));
    let sync_runs = PostgresSyncRunRepository::new(pool.clone());
    let weread_auth = AuthService::new(
        AuthServiceDependencies {
            accounts: PostgresCredentialRepository::new(pool.clone()),
            leases: PostgresAccountLeaseRepository::new(pool.clone()),
            refresher: ManualCredentialRefresher,
            cipher: RingCredentialCipher::new(&SecretString::new(
                "integration credential key".to_owned().into_boxed_str(),
            ))
            .expect("test credential cipher should be valid"),
        },
        AuthServiceConfig::new(
            Duration::minutes(5),
            Duration::minutes(10),
            Duration::minutes(1),
        )
        .expect("test auth configuration should be valid"),
    );
    let root = root.map(|value| value.parse().expect("test server root URL should parse"));
    let router = match qr_login {
        Some(qr_login) => admin_router_with_server_root_url_and_qr_login(
            auth,
            sources,
            feed_tokens,
            sync_runs,
            weread_auth,
            root,
            qr_login,
        ),
        None => admin_router_with_server_root_url(
            auth,
            sources,
            feed_tokens,
            sync_runs,
            weread_auth,
            root,
        ),
    };
    router.layer(Extension(ConnectInfo(std::net::SocketAddr::from((
        [127, 0, 0, 1],
        43_210,
    )))))
}

async fn admin_session(app: &axum::Router) -> (String, String) {
    let login = app
        .clone()
        .oneshot(json_request(
            "/api/admin/login",
            serde_json::json!({"username": "admin", "password": "correct horse"}),
        ))
        .await
        .expect("valid login request should complete");
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()[header::SET_COOKIE]
        .to_str()
        .expect("session cookie should be valid")
        .split(';')
        .next()
        .expect("cookie should contain a value")
        .to_owned();
    let login_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(login.into_body(), usize::MAX)
            .await
            .expect("login body should be readable"),
    )
    .expect("login body should be JSON");
    let csrf = login_body["csrf_token"]
        .as_str()
        .expect("CSRF should be returned")
        .to_owned();
    (cookie, csrf)
}

async fn create_admin_source(
    app: &axum::Router,
    cookie: &str,
    csrf: &str,
    payload: serde_json::Value,
) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            cookie,
            Some(csrf),
            Some(payload),
        ))
        .await
        .expect("source creation request should complete");
    assert_eq!(response.status(), StatusCode::CREATED);
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("source response should be readable"),
    )
    .expect("source response should be JSON")
}

async fn status(app: &axum::Router, path: &str) -> StatusCode {
    app.clone()
        .oneshot(get_request(path, None))
        .await
        .expect("request should complete")
        .status()
}

async fn insert_source(pool: &PgPool) -> SourceId {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    sqlx::query(
        "INSERT INTO sources (id, book_id, display_name, article_url, feed_revision) VALUES ($1, $2, $3, $4, 1)",
    )
    .bind(source_id.as_uuid())
    .bind(format!("api-book-{source_id}"))
    .bind("API integration source")
    .bind("https://mp.weixin.qq.com/s/api-test")
    .execute(pool)
    .await
    .expect("source should be insertable");
    source_id
}

async fn insert_asset_fixture(pool: &PgPool) -> Uuid {
    let source_id = insert_source(pool).await;
    let review_id = "api-asset-article";
    sqlx::query(
        "INSERT INTO articles (source_id, review_id, title, original_url, published_at, content_html, content_hash, observation_version, fetched_at)
         VALUES ($1, $2, 'Asset article', 'https://mp.weixin.qq.com/s/api-asset-article', CURRENT_TIMESTAMP, '<p>asset</p>', 'asset-hash', 1, CURRENT_TIMESTAMP)",
    )
    .bind(source_id.as_uuid())
    .bind(review_id)
    .execute(pool)
    .await
    .expect("asset article should be insertable");

    let asset_id = Uuid::new_v4();
    let blob_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO asset_blobs (id, checksum_algorithm, checksum, byte_size, media_type, data)
         VALUES ($1, 'sha256', repeat('a', 64), 15, 'image/png', $2)",
    )
    .bind(blob_id)
    .bind(b"\x89PNG\r\n\x1a\nfixture".as_slice())
    .execute(pool)
    .await
    .expect("asset blob should be insertable");
    sqlx::query(
        "INSERT INTO asset_records (id, source_url, version, final_url, blob_id)
         VALUES ($1, 'https://cdn.example/api-asset.png', 1, 'https://cdn.example/api-asset.png', $2)",
    )
    .bind(asset_id)
    .bind(blob_id)
    .execute(pool)
    .await
    .expect("asset record should be insertable");
    sqlx::query(
        "INSERT INTO article_assets (source_id, review_id, asset_record_id, occurrence, referer_url)
         VALUES ($1, $2, $3, 0, 'https://mp.weixin.qq.com/s/api-asset-article')",
    )
    .bind(source_id.as_uuid())
    .bind(review_id)
    .bind(asset_id)
    .execute(pool)
    .await
    .expect("article asset relationship should be insertable");
    asset_id
}

async fn wait_for_advisory_waiter(pool: &PgPool, lock_name: &str) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks AS locks
                 JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
                 WHERE locks.locktype = 'advisory'
                   AND NOT locks.granted
                   AND locks.objsubid = 1
                   AND activity.datname = current_database()
                   AND locks.classid = (
                       (hashtextextended($1, 0) >> 32)
                       & 4294967295
                   )::oid
                   AND locks.objid = (
                       hashtextextended($1, 0) & 4294967295
                   )::oid
             )",
        )
        .bind(lock_name)
        .fetch_one(pool)
        .await
        .expect("advisory lock waiter should be queryable");
        if waiting {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "request did not wait for the {lock_name} advisory lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn publish_cache(pool: &PgPool, source_id: SourceId) {
    let generated_at = Utc::now() - Duration::seconds(1);
    let candidate = RssRenderer
        .render(RenderFeedInput {
            source_id,
            title: "API integration feed".to_owned(),
            feed_url: "https://rss.example.test/api-feed".to_owned(),
            description: "API integration feed".to_owned(),
            source_revision: FeedRevision::from_u64(1),
            generated_at,
            expires_at: generated_at + Duration::minutes(30),
            articles: vec![RenderArticle {
                review_id: "api-review-1".to_owned(),
                title: "API integration article".to_owned(),
                author: Some("Test author".to_owned()),
                summary: Some("Test summary".to_owned()),
                original_url: Some("https://mp.weixin.qq.com/s/api-article".to_owned()),
                published_at: generated_at,
                content_html: "<p>Test article body</p>".to_owned(),
            }],
        })
        .expect("candidate should render")
        .into_candidate();
    let leases = PostgresFeedBuildLeaseRepository::new(pool.clone());
    let lease = leases
        .acquire_build(source_id, "api-builder", Duration::minutes(5))
        .await
        .expect("build lease should be acquired")
        .expect("source should not have another builder");
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .feed_cache()
        .publish_if_current(candidate, lease.owner(), lease.token())
        .await
        .expect("cache should publish");
    assert!(matches!(result, FeedCachePublishResult::Published(_)));
    unit_of_work
        .commit()
        .await
        .expect("cache publication should commit");
}
