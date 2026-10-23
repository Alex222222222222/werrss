//! URL checks shared by acquisition, article normalization, and persistence.

use url::Url;

/// Identifies query formats used by common cloud-provider signed URLs.
pub fn has_provider_signature(url: &Url) -> bool {
    let keys = url
        .query_pairs()
        .map(|(name, _)| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let has = |name: &str| keys.iter().any(|key| key == name);

    has("x-amz-signature")
        || has("x-goog-signature")
        || (has("googleaccessid") && has("signature") && has("expires"))
        || (has("awsaccesskeyid") && has("signature") && has("expires"))
        || (has("key-pair-id") && has("signature") && (has("expires") || has("policy")))
        || (has("sig") && has("se") && has("sv"))
        || (has("e")
            && url.query_pairs().any(|(name, value)| {
                name.eq_ignore_ascii_case("token")
                    && value.split_once(':').is_some_and(|(key, signature)| {
                        !key.is_empty() && !signature.is_empty() && !signature.contains(':')
                    })
            }))
        || (has("ossaccesskeyid") && has("signature") && has("expires"))
        || (has("q-signature") && has("q-ak") && has("q-sign-algorithm"))
        || (has("sign") && has("key-time") && has("x-cos-security-token"))
        || has("hdnts")
}

#[cfg(test)]
mod tests {
    use super::has_provider_signature;
    use url::Url;

    #[test]
    fn detects_qiniu_signed_download_tokens() {
        assert!(has_provider_signature(
            &Url::parse("https://cdn.example/image?e=123&token=access-key:encoded-signature")
                .unwrap()
        ));
    }

    #[test]
    fn detects_aws_v2_signatures() {
        assert!(has_provider_signature(
            &Url::parse(
                "https://cdn.example/image?AWSAccessKeyId=key&Expires=123&Signature=secret"
            )
            .unwrap()
        ));
    }

    #[test]
    fn detects_cloudfront_canned_and_custom_policies() {
        for query in ["Expires=123", "Policy=encoded"] {
            assert!(has_provider_signature(
                &Url::parse(&format!(
                    "https://cdn.example/image?Key-Pair-Id=key&Signature=secret&{query}"
                ))
                .unwrap()
            ));
        }
    }

    #[test]
    fn matches_provider_signatures_without_rejecting_ordinary_queries() {
        for signed in [
            "https://cdn.example/image?X-Amz-Signature=secret",
            "https://cdn.example/image?X-Goog-Signature=secret",
            "https://cdn.example/image?GoogleAccessId=key&Expires=123&Signature=secret",
            "https://cdn.example/image?sv=1&se=123&sr=b&sig=secret",
            "https://cdn.example/image?sv=1&se=123&sig=secret",
            "https://cdn.example/image?AWSAccessKeyId=key&Expires=123&Signature=secret",
            "https://cdn.example/image?Key-Pair-Id=key&Expires=123&Signature=secret",
            "https://cdn.example/image?Key-Pair-Id=key&Policy=encoded&Signature=secret",
            "https://cdn.example/image?e=123&token=access-key:encoded-signature",
            "https://cdn.example/image?OSSAccessKeyId=key&Expires=123&Signature=secret",
            "https://cdn.example/image?Q-Signature=secret&Q-AK=key&Q-Sign-Algorithm=sha256",
            "https://cdn.example/image?sign=secret&key-time=123&x-cos-security-token=token",
            "https://cdn.example/image?hdnts=secret",
        ] {
            assert!(
                has_provider_signature(&Url::parse(signed).unwrap()),
                "{signed}"
            );
        }
        for ordinary in [
            "https://cdn.example/image?width=640&fit=cover",
            "https://cdn.example/image?token=resource-token",
            "https://cdn.example/image?signature=resource-name",
            "https://cdn.example/image?e=123&token=resource-token",
            "https://cdn.example/image?token=access-key:resource-name",
            "https://cdn.example/image?e=123&token=:signature",
            "https://cdn.example/image?Expires=123&Signature=resource-name",
            "https://cdn.example/image?Key-Pair-Id=resource-name",
            "https://cdn.example/image?sig=resource-name&se=123",
        ] {
            assert!(
                !has_provider_signature(&Url::parse(ordinary).unwrap()),
                "{ordinary}"
            );
        }
    }
}
