use chrono::DateTime;
use serde_json::json;
use werrss::acquisition::weread::{parse_article_list_payload, WeReadAdapterError};

#[test]
fn public_parser_handles_current_and_legacy_payloads_consistently() {
    let current = json!({
        "data": [{
            "reviewId": "current-1",
            "title": "Current",
            "mpInfo": {"doc_url": "https://mp.weixin.qq.com/s/current"}
        }]
    });
    let legacy = json!({
        "reviews": [{
            "createTime": 1700000000,
            "subReviews": [{
                "review": {
                    "reviewId": "legacy-1",
                    "title": "Legacy",
                    "mpInfo": {"originalId": "legacy"}
                }
            }]
        }]
    });

    let current = parse_article_list_payload(&current).expect("current payload should parse");
    let legacy = parse_article_list_payload(&legacy).expect("legacy payload should parse");

    assert_eq!(current[0].review_id, "current-1");
    assert_eq!(
        current[0].article_url.as_ref().unwrap().as_str(),
        "https://mp.weixin.qq.com/s/current"
    );
    assert_eq!(legacy[0].review_id, "legacy-1");
    assert_eq!(
        legacy[0].published_at,
        DateTime::from_timestamp(1_700_000_000, 0)
    );
}

#[test]
fn public_parser_handles_the_cover_payload_shape() {
    let payload = json!({
        "reviewId": "MP_WXS_2103095721_1V0fvyRTje-N7TWQunyLJA",
        "title": "人物文章",
        "name": "人物",
        "pic": "https://mmbiz.qpic.cn/cover.jpg"
    });

    let articles = parse_article_list_payload(&payload).expect("cover payload should parse");

    assert_eq!(articles.len(), 1);
    assert_eq!(
        articles[0].review_id,
        "MP_WXS_2103095721_1V0fvyRTje-N7TWQunyLJA"
    );
    assert_eq!(articles[0].title.as_deref(), Some("人物文章"));
    assert_eq!(articles[0].author.as_deref(), Some("人物"));
    assert_eq!(
        articles[0].cover_url.as_deref(),
        Some("https://mmbiz.qpic.cn/cover.jpg")
    );
    assert_eq!(
        articles[0].article_url.as_ref().unwrap().as_str(),
        "https://mp.weixin.qq.com/s/1V0fvyRTje-N7TWQunyLJA"
    );
}

#[test]
fn public_parser_returns_typed_risk_control_and_rejects_malformed_json() {
    assert_eq!(
        parse_article_list_payload(&json!({"errcode": -2010})),
        Err(WeReadAdapterError::RiskControlled { code: -2010 })
    );
    assert_eq!(
        parse_article_list_payload(&json!({"data": "not-an-array"})),
        Err(WeReadAdapterError::UnexpectedResponse(
            "data must be an array".to_owned()
        ))
    );
}

#[test]
fn public_parser_rejects_same_host_non_article_paths() {
    for article_url in [
        "https://mp.weixin.qq.com/cgi-bin/appmsg",
        "https://mp.weixin.qq.com/script",
        "/script",
    ] {
        let payload = json!({
            "data": [{
                "reviewId": "review-1",
                "title": "Article",
                "mpInfo": {"docUrl": article_url}
            }]
        });

        assert_eq!(
            parse_article_list_payload(&payload),
            Err(WeReadAdapterError::InvalidArticleUrl),
            "non-article path should be rejected: {article_url}"
        );
    }
}
