use chrono::{TimeZone, Utc};
use uuid::Uuid;
use werrss::{
    acquisition::{
        article_page::{ArticlePageError, ExtractedArticlePage},
        weread::{WeReadAdapterError, WeReadArticleReference},
    },
    application::sync_service::{
        classify_acquisition_error, SyncAcquisitionError, SyncService, SyncServiceError,
    },
    domain::{
        article::ArticleObservationVersion,
        source::{SourceId, VerifiedWechatArticleUrl},
        sync::{SyncFailureClass, SyncOutcome},
    },
};

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
        title: "Page title".to_owned(),
        author: Some("Page author".to_owned()),
        summary: None,
        published_at: Some(Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()),
        content_html: "<p>body</p>".to_owned(),
        cover_url: Some("https://cdn.example/page.jpg".to_owned()),
    }
}

#[test]
fn integration_preparation_prefers_page_metadata_and_returns_persistence_input() {
    let mut list_reference = reference();
    list_reference.published_at = Some(Utc.timestamp_opt(1_600_000_000, 0).single().unwrap());
    let prepared = SyncService::new()
        .prepare_article(
            source_id(),
            &list_reference,
            page(),
            ArticleObservationVersion::from_u64(1),
            Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
        )
        .expect("acquired article should be prepared");

    assert_eq!(prepared.article().title, "Page title");
    assert_eq!(prepared.article().author.as_deref(), Some("Page author"));
    assert_eq!(prepared.article().summary.as_deref(), Some("List summary"));
    assert_eq!(
        prepared.article().cover_url.as_deref(),
        Some("https://cdn.example/page.jpg")
    );
    assert_eq!(
        prepared.article().original_url.as_ref().unwrap().as_str(),
        "https://mp.weixin.qq.com/s/page-url"
    );
    assert_eq!(prepared.article().content_html, "<p>body</p>");
    assert_eq!(
        prepared.article().published_at,
        Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()
    );
    assert_eq!(prepared.external_assets(), &[]);
}

#[test]
fn integration_preparation_rejects_missing_required_identity_or_version() {
    let mut missing_identity = reference();
    missing_identity.review_id = " ".to_owned();
    assert_eq!(
        SyncService::default().prepare_article(
            source_id(),
            &missing_identity,
            page(),
            ArticleObservationVersion::from_u64(1),
            Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
        ),
        Err(SyncServiceError::Article(
            werrss::domain::article::ArticleError::EmptyReviewId
        ))
    );

    assert_eq!(
        SyncService::default().prepare_article(
            source_id(),
            &reference(),
            page(),
            ArticleObservationVersion::from_u64(0),
            Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
        ),
        Err(SyncServiceError::Article(
            werrss::domain::article::ArticleError::InvalidObservationVersion
        ))
    );
}

#[test]
fn integration_preparation_uses_list_publication_time_when_page_omits_it() {
    let mut list_reference = reference();
    list_reference.published_at = Some(Utc.timestamp_opt(1_600_000_000, 0).single().unwrap());
    let mut article_page = page();
    article_page.published_at = None;

    let prepared = SyncService::default()
        .prepare_article(
            source_id(),
            &list_reference,
            article_page,
            ArticleObservationVersion::from_u64(1),
            Utc.timestamp_opt(1_700_000_100, 0).single().unwrap(),
        )
        .expect("list timestamp should complete page metadata");

    assert_eq!(
        prepared.article().published_at,
        Utc.timestamp_opt(1_600_000_000, 0).single().unwrap()
    );
}

#[test]
fn integration_classification_is_stable_and_does_not_leak_error_text() {
    let error = SyncAcquisitionError::WeRead(WeReadAdapterError::RiskControlled { code: -2041 });
    let classified = classify_acquisition_error(&error);
    assert_eq!(classified.outcome(), SyncOutcome::RiskControlled);
    assert_eq!(
        classified.failure().class(),
        SyncFailureClass::RiskControlled
    );
    assert_eq!(
        classified.failure().message(),
        "WeRead request was risk-controlled"
    );

    let error =
        SyncAcquisitionError::ArticlePage(ArticlePageError::Browser("password=secret".to_owned()));
    let classified = classify_acquisition_error(&error);
    assert_eq!(classified.outcome(), SyncOutcome::RetryableFailure);
    assert_eq!(classified.failure().class(), SyncFailureClass::Retryable);
    assert!(!classified.failure().message().contains("secret"));

    let error = SyncAcquisitionError::WeRead(WeReadAdapterError::InvalidReviewId);
    let classified = classify_acquisition_error(&error);
    assert_eq!(classified.outcome(), SyncOutcome::Failed);
    assert_eq!(classified.failure().class(), SyncFailureClass::Permanent);
    assert_eq!(
        classified.failure().message(),
        "WeRead article identity was invalid"
    );
}

#[test]
fn integration_classification_labels_unexpected_weread_responses_as_retryable() {
    let error = SyncAcquisitionError::WeRead(WeReadAdapterError::UnexpectedResponse(
        "data must be an array".to_owned(),
    ));

    let classified = classify_acquisition_error(&error);

    assert_eq!(classified.outcome(), SyncOutcome::RetryableFailure);
    assert_eq!(classified.failure().class(), SyncFailureClass::Retryable);
    assert_eq!(
        classified.failure().message(),
        "WeRead returned an unexpected response"
    );
}
