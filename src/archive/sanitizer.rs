//! Conservative HTML sanitization for archived article content.
//!
//! The sanitizer parses browser-extracted fragments with the HTML5 parser and
//! serializes only an explicit element and attribute allowlist. Scripts,
//! styles, event handlers, frames, forms, foreign active content, unsafe URLs,
//! comments, and all unrecognized attributes are omitted. Unrecognized
//! formatting elements are transparent, so their text and safe descendants are
//! retained without retaining the element itself.
//!
//! The result contains normalized HTML and deduplicated approved external image
//! URLs. WeChat lazy-image attributes such as `data-src` are promoted to `src`
//! when the value is an absolute HTTP(S) URL. Version one may leave those URLs
//! external; the sanitizer never downloads them, writes PostgreSQL rows, or
//! renders RSS.
//!
//! This module has no application state and is safe to call repeatedly. The
//! application/archive boundary should treat the returned HTML as the only
//! content representation eligible for article persistence and RSS rendering.

use std::collections::HashSet;

use ego_tree::iter::Edge;
use scraper::{Html, Node};
use url::Url;

use crate::domain::url_policy::has_provider_signature;

const SAFE_ELEMENTS: &[&str] = &[
    "a",
    "abbr",
    "article",
    "b",
    "blockquote",
    "br",
    "caption",
    "code",
    "col",
    "colgroup",
    "dd",
    "del",
    "div",
    "dl",
    "dt",
    "em",
    "figcaption",
    "figure",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "i",
    "img",
    "ins",
    "kbd",
    "li",
    "mark",
    "ol",
    "p",
    "pre",
    "q",
    "s",
    "samp",
    "section",
    "small",
    "span",
    "strong",
    "sub",
    "sup",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "u",
    "ul",
    "wbr",
];

const DROP_CONTENT_ELEMENTS: &[&str] = &[
    "applet", "base", "embed", "frame", "frameset", "head", "iframe", "input", "link", "meta",
    "noscript", "object", "script", "select", "style", "svg", "template", "textarea", "video",
];

const VOID_ELEMENTS: &[&str] = &["br", "col", "hr", "img", "wbr"];

/// Sanitized article HTML and transient external media required for completeness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SanitizedHtml {
    html: String,
    external_assets: Vec<Url>,
}

impl SanitizedHtml {
    /// Returns the normalized, safe HTML fragment.
    pub fn html(&self) -> &str {
        &self.html
    }

    /// Returns required HTTP(S) image URLs in first-seen order.
    /// Signed URLs remain transient here even though they are omitted from HTML.
    pub fn external_assets(&self) -> &[Url] {
        &self.external_assets
    }
}

/// Stateless sanitizer for browser-extracted article fragments.
#[derive(Debug, Clone, Copy, Default)]
pub struct HtmlSanitizer;

impl HtmlSanitizer {
    /// Parses and sanitizes one HTML fragment.
    pub fn sanitize(self, input: &str) -> SanitizedHtml {
        sanitize(input)
    }
}

/// Parses and sanitizes one HTML fragment with the default policy.
pub fn sanitize(input: &str) -> SanitizedHtml {
    let document = Html::parse_fragment(input);
    let mut renderer = Renderer {
        html: String::new(),
        external_assets: Vec::new(),
        seen_assets: HashSet::new(),
        omitted_depth: 0,
    };

    for edge in document.tree.root().traverse() {
        renderer.render_edge(edge);
    }

    SanitizedHtml {
        html: renderer.html,
        external_assets: renderer.external_assets,
    }
}

struct Renderer {
    html: String,
    external_assets: Vec<Url>,
    seen_assets: HashSet<Url>,
    omitted_depth: usize,
}

impl Renderer {
    fn render_edge(&mut self, edge: Edge<'_, Node>) {
        match edge {
            Edge::Open(node) => self.open(node.value()),
            Edge::Close(node) => self.close(node.value()),
        }
    }

    fn open(&mut self, node: &Node) {
        if self.omitted_depth > 0 {
            self.omitted_depth += 1;
            return;
        }

        match node {
            Node::Element(element) => {
                let name = element.name();
                if DROP_CONTENT_ELEMENTS.contains(&name) {
                    self.omitted_depth = 1;
                } else if SAFE_ELEMENTS.contains(&name) {
                    self.render_open_element(element);
                }
            }
            Node::Text(text) => append_text(&mut self.html, text),
            Node::Document | Node::Fragment | Node::Doctype(_) | Node::Comment(_) => {}
            Node::ProcessingInstruction(_) => {}
        }
    }

    fn close(&mut self, node: &Node) {
        if self.omitted_depth > 0 {
            self.omitted_depth -= 1;
            return;
        }

        if let Node::Element(element) = node {
            let name = element.name();
            if SAFE_ELEMENTS.contains(&name) && !VOID_ELEMENTS.contains(&name) {
                self.html.push_str("</");
                self.html.push_str(name);
                self.html.push('>');
            }
        }
    }

    fn render_open_element(&mut self, element: &scraper::node::Element) {
        let name = element.name();
        if name == "img" {
            let Some(source) = image_source(element) else {
                self.omitted_depth = 1;
                return;
            };
            if self.seen_assets.insert(source.clone()) {
                self.external_assets.push(source.clone());
            }
            // Keep required signed images transient so acquisition cannot report completeness.
            if has_provider_signature(&source) {
                self.omitted_depth = 1;
                return;
            }
            self.html.push('<');
            self.html.push_str(name);
            self.append_attributes(element, Some(&source));
            self.html.push_str(" />");
            return;
        }

        self.html.push('<');
        self.html.push_str(name);
        self.append_attributes(element, None);
        if VOID_ELEMENTS.contains(&name) {
            self.html.push_str(" />");
        } else {
            self.html.push('>');
        }
    }

    fn append_attributes(&mut self, element: &scraper::node::Element, image_source: Option<&Url>) {
        let name = element.name();
        for (attribute, value) in element.attrs() {
            if attribute == "src" || attribute.starts_with("data-") {
                continue;
            }
            if !allowed_attribute(name, attribute) {
                continue;
            }
            if attribute == "href" {
                let Some(url) = approved_http_url(value, false) else {
                    continue;
                };
                if has_provider_signature(&url) {
                    continue;
                }
                append_attribute(&mut self.html, attribute, url.as_str());
            } else {
                append_attribute(&mut self.html, attribute, value);
            }
        }

        if let Some(source) = image_source {
            append_attribute(&mut self.html, "src", source.as_str());
        }
    }
}

fn allowed_attribute(element: &str, attribute: &str) -> bool {
    match attribute {
        "class" | "id" | "title" => true,
        "href" => element == "a",
        "alt" => element == "img",
        "width" | "height" => matches!(element, "img" | "td" | "th"),
        "colspan" | "rowspan" => matches!(element, "td" | "th"),
        "align" => matches!(element, "caption" | "div" | "p" | "table" | "td" | "th"),
        _ => false,
    }
}

fn image_source(element: &scraper::node::Element) -> Option<Url> {
    // WeChat commonly leaves a valid loading image in `src` while storing the
    // article image in a lazy-loading attribute. Prefer the article image and
    // fall back to `src` only when no lazy source is approved.
    for attribute in ["data-src", "data-original", "data-lazy-src", "src"] {
        if let Some(value) = element.attr(attribute) {
            if let Some(url) = approved_http_url(value, true) {
                return Some(url);
            }
        }
    }
    None
}

fn approved_http_url(value: &str, strip_fragment: bool) -> Option<Url> {
    let value = value.trim();
    if authority_contains_userinfo(value) {
        return None;
    }
    let mut url = Url::parse(value).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }

    if (url.scheme() == "http" && url.port() == Some(80))
        || (url.scheme() == "https" && url.port() == Some(443))
    {
        let _ = url.set_port(None);
    }
    if strip_fragment {
        url.set_fragment(None);
    }
    Some(url)
}

fn authority_contains_userinfo(value: &str) -> bool {
    value
        .split_once("://")
        .and_then(|(_, remainder)| remainder.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'))
}

fn append_attribute(output: &mut String, name: &str, value: &str) {
    output.push(' ');
    output.push_str(name);
    output.push_str("=\"");
    append_escaped(output, value, true);
    output.push('"');
}

fn append_text(output: &mut String, text: &str) {
    append_escaped(output, text, false);
}

fn append_escaped(output: &mut String, value: &str, attribute: bool) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' if attribute => output.push_str("&quot;"),
            '\'' if attribute => output.push_str("&#39;"),
            character if is_allowed_html_character(character) => output.push(character),
            _ => {}
        }
    }
}

fn is_allowed_html_character(character: char) -> bool {
    matches!(
        character as u32,
        0x9 | 0xA | 0xD | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omits_provider_signed_urls_but_preserves_ordinary_query_urls() {
        let sanitized = sanitize(
            r#"<p><img src="https://cdn.example/image.png?X-Amz-Signature=secret&amp;X-Amz-Credential=key"><a href="https://cdn.example/file?GoogleAccessId=key&amp;Expires=123&amp;Signature=secret">signed</a><img src="https://cdn.example/resize.png?width=640&amp;fit=cover"></p>"#,
        );

        assert_eq!(
            sanitized.html(),
            "<p><a>signed</a><img src=\"https://cdn.example/resize.png?width=640&amp;fit=cover\" /></p>"
        );
        assert_eq!(
            sanitized.external_assets(),
            &[
                Url::parse(
                    "https://cdn.example/image.png?X-Amz-Signature=secret&X-Amz-Credential=key"
                )
                .unwrap(),
                Url::parse("https://cdn.example/resize.png?width=640&fit=cover").unwrap(),
            ]
        );
    }

    #[test]
    fn removes_active_content_handlers_and_unsafe_urls() {
        let sanitized = sanitize(
            r#"<div onclick="alert(1)"><script>alert(2)</script><p>safe</p><a href="javascript:alert(3)">link</a><iframe src="https://evil.example"></iframe></div>"#,
        );

        assert_eq!(sanitized.html(), "<div><p>safe</p><a>link</a></div>");
        assert!(sanitized.external_assets().is_empty());
    }

    #[test]
    fn promotes_lazy_images_rejects_data_urls_and_deduplicates_assets() {
        let sanitized = sanitize(
            r#"<p><img data-src="https://cdn.example/a.jpg#fragment" onerror="bad()"><img src="data:image/svg+xml,<svg/onload=bad()>" data-original="https://cdn.example/a.jpg"><img src="https://cdn.example/b.jpg" /></p>"#,
        );

        assert_eq!(
            sanitized.html(),
            "<p><img src=\"https://cdn.example/a.jpg\" /><img src=\"https://cdn.example/a.jpg\" /><img src=\"https://cdn.example/b.jpg\" /></p>"
        );
        assert_eq!(
            sanitized.external_assets(),
            &[
                Url::parse("https://cdn.example/a.jpg").unwrap(),
                Url::parse("https://cdn.example/b.jpg").unwrap()
            ]
        );
    }

    #[test]
    fn prefers_a_valid_lazy_source_over_a_valid_placeholder_source() {
        let sanitized = sanitize(
            r#"<img src="https://cdn.example/loading.gif" data-src="https://cdn.example/article.jpg">"#,
        );

        assert_eq!(
            sanitized.html(),
            "<img src=\"https://cdn.example/article.jpg\" />"
        );
        assert_eq!(
            sanitized.external_assets(),
            &[Url::parse("https://cdn.example/article.jpg").unwrap()]
        );
    }

    #[test]
    fn strips_unknown_elements_but_retains_safe_text_and_descendants() {
        let sanitized = sanitize(
            "<font color='red'>plain <strong title='ok'>text</strong></font><!-- hidden --><svg><text>bad</text></svg>",
        );

        assert_eq!(sanitized.html(), "plain <strong title=\"ok\">text</strong>");
    }

    #[test]
    fn rejects_image_credentials_invalid_hosts_and_empty_sources() {
        let sanitized = sanitize(
            r#"<img src="https://user:pass@cdn.example/a.jpg"><img src="https://@cdn.example/b.jpg"><img src="//cdn.example/c.jpg"><img alt="no source">"#,
        );

        assert_eq!(sanitized.html(), "");
        assert!(sanitized.external_assets().is_empty());
    }

    #[test]
    fn escapes_text_attributes_and_xml_incompatible_controls() {
        let sanitized = sanitize("<p title='a &quot; b'>&lt;safe&gt;\u{0000}</p>");

        assert_eq!(sanitized.html(), "<p title=\"a &quot; b\">&lt;safe&gt;</p>");
    }
}
