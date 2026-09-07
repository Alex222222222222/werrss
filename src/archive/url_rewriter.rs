//! Rewrites approved sanitized image URLs to stable local asset routes.
//!
//! The input is the exact output of [`crate::archive::sanitizer::sanitize`].
//! Rewriting therefore operates only on serialized `src` attributes and never
//! reparses or broadens the HTML allowlist. Unresolved URLs are left intact so
//! a best-effort asset failure cannot make an otherwise valid article vanish.

use std::collections::HashMap;

use url::Url;
use uuid::Uuid;

/// Replaces selected sanitized image `src` values with stable local paths.
pub fn rewrite_sanitized_html(html: &str, replacements: &[(Url, String)]) -> String {
    if replacements.is_empty() || html.is_empty() {
        return html.to_owned();
    }

    let replacements = replacements
        .iter()
        .map(|(source, target)| (escape_attribute(source.as_str()), target.clone()))
        .collect::<HashMap<_, _>>();
    rewrite_src_attributes(html, |source| replacements.get(source).cloned())
}

/// Reports whether sanitized HTML contains a stable relative asset route.
pub fn contains_relative_asset_urls(html: &str) -> bool {
    has_src_matching(html, |source| stable_asset_id(source).is_some())
}

/// Reports whether sanitized HTML contains asset routes rooted at the given
/// public URL.
pub fn contains_absolute_asset_urls(html: &str, server_root_url: &Url) -> bool {
    let Some(asset_base) = asset_url_base(server_root_url) else {
        return false;
    };
    has_src_matching(html, |source| {
        let Some(url) = stable_absolute_asset_url(source) else {
            return false;
        };
        asset_url_matches_base(&url, &asset_base)
    })
}

/// Reports whether sanitized HTML contains any stable absolute asset route.
pub fn contains_any_absolute_asset_urls(html: &str) -> bool {
    has_src_matching(html, |source| stable_absolute_asset_url(source).is_some())
}

/// Reports whether sanitized HTML contains a stable absolute asset route that
/// is not rooted at the configured public URL.
pub fn contains_absolute_asset_urls_outside_root(html: &str, server_root_url: &Url) -> bool {
    let Some(asset_base) = asset_url_base(server_root_url) else {
        return false;
    };
    has_src_matching(html, |source| {
        let Some(url) = stable_absolute_asset_url(source) else {
            return false;
        };
        !asset_url_matches_base(&url, &asset_base)
    })
}

/// Converts stable relative asset routes to absolute public URLs.
///
/// Article rows keep `/assets/{id}` so they remain independent of the public
/// deployment hostname. Feed rendering can call this function when the
/// operator has configured a public server root; unrelated image URLs and
/// other attributes remain unchanged.
pub fn absolutize_asset_urls(html: &str, server_root_url: &Url) -> String {
    let Some(asset_base) = asset_url_base(server_root_url) else {
        return html.to_owned();
    };
    rewrite_src_attributes(html, |source| {
        let asset_id = stable_asset_id(source)?;
        asset_base.join(asset_id).ok().map(|url| url.to_string())
    })
}

fn stable_asset_id(source: &str) -> Option<&str> {
    let asset_id = source.strip_prefix("/assets/")?;
    stable_asset_id_segment(asset_id)
}

fn stable_asset_id_segment(asset_id: &str) -> Option<&str> {
    if asset_id.is_empty()
        || asset_id.contains('/')
        || asset_id.contains('?')
        || asset_id.contains('#')
    {
        return None;
    }
    asset_id.parse::<Uuid>().ok()?;
    Some(asset_id)
}

fn stable_absolute_asset_url(source: &str) -> Option<Url> {
    let url = Url::parse(source).ok()?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return None;
    }
    let asset_id = url.path().rsplit_once("/assets/")?.1;
    stable_asset_id_segment(asset_id)?;
    Some(url)
}

fn asset_url_matches_base(url: &Url, asset_base: &Url) -> bool {
    let Some(asset_id) = url.path().rsplit_once("/assets/").map(|(_, id)| id) else {
        return false;
    };
    if stable_asset_id_segment(asset_id).is_none() {
        return false;
    }
    asset_base
        .join(asset_id)
        .is_ok_and(|expected| expected.as_str() == url.as_str())
}

fn asset_url_base(server_root_url: &Url) -> Option<Url> {
    let mut base = server_root_url.clone();
    let root_path = base.path().trim_end_matches('/');
    base.set_path(&format!("{root_path}/"));
    base.join("assets/").ok()
}

fn has_src_matching<F>(html: &str, mut predicate: F) -> bool
where
    F: FnMut(&str) -> bool,
{
    let mut cursor = 0;
    while let Some(relative_start) = html[cursor..].find(" src=\"") {
        let attribute_start = cursor + relative_start;
        let value_start = attribute_start + " src=\"".len();
        let Some(relative_end) = html[value_start..].find('"') else {
            return false;
        };
        let value_end = value_start + relative_end;
        if predicate(&html[value_start..value_end]) {
            return true;
        }
        cursor = value_end + 1;
    }
    false
}

fn rewrite_src_attributes<F>(html: &str, mut replacement: F) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    if html.is_empty() {
        return html.to_owned();
    }

    let mut output = String::with_capacity(html.len());
    let mut cursor = 0;
    while let Some(relative_start) = html[cursor..].find(" src=\"") {
        let attribute_start = cursor + relative_start;
        let value_start = attribute_start + " src=\"".len();
        let Some(relative_end) = html[value_start..].find('"') else {
            break;
        };
        let value_end = value_start + relative_end;
        output.push_str(&html[cursor..value_start]);
        if let Some(target) = replacement(&html[value_start..value_end]) {
            output.push_str(&escape_attribute(&target));
        } else {
            output.push_str(&html[value_start..value_end]);
        }
        output.push('"');
        cursor = value_end + 1;
    }
    output.push_str(&html[cursor..]);
    output
}

fn escape_attribute(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_only_matching_image_sources() {
        let source = "https://cdn.example/image.jpg?size=1&mode=fit"
            .parse::<Url>()
            .expect("source URL should parse");
        let html = "<a href=\"https://cdn.example/image.jpg?size=1&mode=fit\"><img src=\"https://cdn.example/image.jpg?size=1&amp;mode=fit\" alt=\"cover\" /></a>";

        let rewritten = rewrite_sanitized_html(html, &[(source, "/assets/asset-1".to_owned())]);

        assert_eq!(
            rewritten,
            "<a href=\"https://cdn.example/image.jpg?size=1&mode=fit\"><img src=\"/assets/asset-1\" alt=\"cover\" /></a>"
        );
    }

    #[test]
    fn leaves_unresolved_and_malformed_attributes_unchanged() {
        let source = "https://cdn.example/known.png".parse::<Url>().unwrap();
        let html = "<img src=\"https://cdn.example/other.png\" /><div data-src=\"https://cdn.example/known.png\">text</div>";

        assert_eq!(
            rewrite_sanitized_html(html, &[(source, "/assets/known".to_owned())]),
            html
        );
    }

    #[test]
    fn absolutizes_only_stable_asset_sources_against_the_public_root() {
        let root = Url::parse("https://rss.example.test/werrss").unwrap();
        let html = "<img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" /><img src=\"https://cdn.example/image.png\" />";

        assert_eq!(
            absolutize_asset_urls(html, &root),
            "<img src=\"https://rss.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" /><img src=\"https://cdn.example/image.png\" />"
        );
    }

    #[test]
    fn detects_relative_asset_sources() {
        let html = "<img src=\"/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />";

        assert!(contains_relative_asset_urls(html));
    }

    #[test]
    fn detects_asset_sources_under_the_configured_public_root() {
        let root = Url::parse("https://rss.example.test/werrss").unwrap();
        let html = "<img src=\"https://rss.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />";

        assert!(contains_absolute_asset_urls(html, &root));
    }

    #[test]
    fn detects_asset_sources_outside_the_configured_public_root() {
        let root = Url::parse("https://rss.example.test/werrss").unwrap();
        let html = "<img src=\"https://old.example.test/werrss/assets/bd264535-3e4f-4da5-be01-73f3dcc98625\" />";

        assert!(contains_any_absolute_asset_urls(html));
        assert!(contains_absolute_asset_urls_outside_root(html, &root));
        assert!(!contains_absolute_asset_urls(html, &root));
    }

    #[test]
    fn ignores_non_uuid_asset_sources() {
        let root = Url::parse("https://rss.example.test/werrss").unwrap();
        let html = "<img src=\"/assets/logo.png\" /><img src=\"https://rss.example.test/werrss/assets/logo.png\" />";

        assert!(!contains_relative_asset_urls(html));
        assert!(!contains_absolute_asset_urls(html, &root));
    }
}
