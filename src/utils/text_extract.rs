use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq)]
pub struct ExtractionResult {
    pub text: String,
    pub canonical_url: Option<String>,
    pub title_clean: Option<String>,
}

// --- Metadata regexes (compiled once) ---

static RE_CANONICAL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)<link\s+[^>]*rel\s*=\s*["']canonical["'][^>]*href\s*=\s*["']([^"']+)["']"#)
        .unwrap()
});

static RE_CANONICAL_HREF_FIRST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)<link\s+[^>]*href\s*=\s*["']([^"']+)["'][^>]*rel\s*=\s*["']canonical["']"#)
        .unwrap()
});

static RE_OG_TITLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)<meta\s+[^>]*property\s*=\s*["']og:title["'][^>]*content\s*=\s*["']([^"']+)["']"#,
    )
    .unwrap()
});

static RE_OG_TITLE_CONTENT_FIRST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)<meta\s+[^>]*content\s*=\s*["']([^"']+)["'][^>]*property\s*=\s*["']og:title["']"#,
    )
    .unwrap()
});

static RE_TITLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)<title[^>]*>(.*?)</title>").unwrap());

// --- Content extraction regexes ---

static RE_ARTICLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)<article[^>]*>(.*)</article>").unwrap());

static RE_MAIN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)<main[^>]*>(.*)</main>").unwrap());

static RE_BODY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)<body[^>]*>(.*)</body>").unwrap());

// --- Strip tags (tag + content removed completely) ---

static STRIP_TAGS: &[&str] = &[
    "script",
    "style",
    "nav",
    "footer",
    "header",
    "aside",
    "noscript",
    "iframe",
    "form",
    "svg",
    "button",
    "input",
    "textarea",
    "select",
    "label",
    "figure",
    "figcaption",
    "img",
    "video",
    "audio",
    "canvas",
    "map",
    "object",
    "embed",
];

// --- Block tags (replaced by \n) ---

static BLOCK_TAGS: &[&str] = &[
    "p",
    "div",
    "br",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
    "tr",
    "blockquote",
];

// --- Inline tags (replaced by space) ---

static INLINE_TAGS: &[&str] = &["span", "a", "strong", "em", "b", "i", "code", "pre"];

/// Build a regex that matches an opening or self-closing tag (case-insensitive),
/// with optional attributes, possibly self-closing.
fn build_tag_regex(tag: &str) -> Regex {
    // Matches <tag ...> or <tag ... /> or <tag>
    let pattern = format!(r"(?is)<\s*{}\b[^>]*/?>", regex::escape(tag));
    Regex::new(&pattern).unwrap()
}

/// Build a regex that matches a closing tag.
fn build_closing_tag_regex(tag: &str) -> Regex {
    let pattern = format!(r"(?i)</\s*{}\s*>", regex::escape(tag));
    Regex::new(&pattern).unwrap()
}

/// Build a regex that matches an entire tag block (opening + content + closing),
/// or a self-closing tag. Used for stripping entire elements.
fn build_tag_block_regex(tag: &str) -> Regex {
    // For block-level strip: match opening tag + everything up to closing tag
    // This is greedy and works well for simple nested structures.
    // Also match self-closing tags like <img ... />
    let pattern = format!(
        r"(?is)<\s*{}\b[^>]*/?>.*?</\s*{}\s*>|<\s*{}\b[^>]*/?>",
        regex::escape(tag),
        regex::escape(tag),
        regex::escape(tag),
    );
    Regex::new(&pattern).unwrap()
}

fn extract_metadata(html: &str) -> (Option<String>, Option<String>) {
    // Canonical URL
    let canonical = RE_CANONICAL
        .captures(html)
        .or_else(|| RE_CANONICAL_HREF_FIRST.captures(html))
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim().to_string());

    // Title: og:title → <title>
    let title = RE_OG_TITLE
        .captures(html)
        .or_else(|| RE_OG_TITLE_CONTENT_FIRST.captures(html))
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim().to_string())
        .or_else(|| {
            RE_TITLE.captures(html).and_then(|c| c.get(1)).map(|m| {
                // strip inner HTML tags from title if any
                let raw = m.as_str();
                let cleaned = Regex::new(r"(?s)<[^>]*>")
                    .unwrap()
                    .replace_all(raw, "")
                    .trim()
                    .to_string();
                cleaned
            })
        });

    let title_clean = title.as_deref().map(clean_title);

    (canonical, title_clean)
}

/// Extract the main content region: <article> → <main> → <body> → full HTML.
fn extract_content_region(html: &str) -> &str {
    if let Some(m) = RE_ARTICLE.find(html) {
        return m.as_str();
    }
    if let Some(m) = RE_MAIN.find(html) {
        return m.as_str();
    }
    if let Some(m) = RE_BODY.find(html) {
        return m.as_str();
    }
    html
}

/// Strip non-content tags entirely (tag + inner content).
fn strip_non_content_tags(html: &str) -> String {
    let mut result = html.to_string();
    for &tag in STRIP_TAGS {
        let re = build_tag_block_regex(tag);
        result = re.replace_all(&result, "").to_string();
    }
    result
}

/// Replace block tags with newlines.
fn replace_block_tags(html: &str) -> String {
    let mut result = html.to_string();
    for &tag in BLOCK_TAGS {
        let re_open = build_tag_regex(tag);
        let re_close = build_closing_tag_regex(tag);
        result = re_open.replace_all(&result, "\n").to_string();
        result = re_close.replace_all(&result, "\n").to_string();
    }
    result
}

/// Replace inline tags with spaces.
fn replace_inline_tags(html: &str) -> String {
    let mut result = html.to_string();
    for &tag in INLINE_TAGS {
        let re_open = build_tag_regex(tag);
        let re_close = build_closing_tag_regex(tag);
        result = re_open.replace_all(&result, " ").to_string();
        result = re_close.replace_all(&result, " ").to_string();
    }
    result
}

/// Strip all remaining HTML tags (just the tags, keep content).
fn strip_remaining_tags(html: &str) -> String {
    let re = Regex::new(r"(?s)</?[a-zA-Z][^>]*>").unwrap();
    re.replace_all(html, "").to_string()
}

/// Decode common HTML entities.
fn decode_entities(text: &str) -> String {
    let mut result = text.to_string();
    let entities: &[(&str, &str)] = &[
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", "\""),
        ("&#39;", "'"),
        ("&apos;", "'"),
        ("&nbsp;", " "),
        ("&rsquo;", "'"),
        ("&lsquo;", "'"),
        ("&rdquo;", "\""),
        ("&ldquo;", "\""),
        ("&mdash;", "—"),
        ("&ndash;", "–"),
        ("&hellip;", "…"),
        ("&eacute;", "é"),
        ("&egrave;", "è"),
        ("&agrave;", "à"),
        ("&ccedil;", "ç"),
        ("&ocir;", "ô"),
        ("&ucir;", "û"),
        ("&icir;", "î"),
        ("&acir;", "â"),
    ];
    for &(entity, replacement) in entities {
        result = result.replace(entity, replacement);
    }
    // Numeric entities: &#123; and &#x7B;
    let re_dec = Regex::new(r"&#(\d+);").unwrap();
    result = re_dec
        .replace_all(&result, |caps: &regex::Captures| {
            caps.get(1)
                .and_then(|m| m.as_str().parse::<u32>().ok())
                .and_then(char::from_u32)
                .map(|c| c.to_string())
                .unwrap_or_default()
        })
        .to_string();
    let re_hex = Regex::new(r"&#x([0-9a-fA-F]+);").unwrap();
    result = re_hex
        .replace_all(&result, |caps: &regex::Captures| {
            caps.get(1)
                .and_then(|m| u32::from_str_radix(m.as_str(), 16).ok())
                .and_then(char::from_u32)
                .map(|c| c.to_string())
                .unwrap_or_default()
        })
        .to_string();
    result
}

/// Collapse multiple whitespace (spaces, tabs, newlines) into single spaces,
/// then trim.
fn collapse_whitespace(text: &str) -> String {
    let re = Regex::new(r"[ \t]+").unwrap();
    let collapsed = re.replace_all(text, " ");
    // Collapse runs of newlines into double newline (paragraph break)
    let re_newlines = Regex::new(r"\n{3,}").unwrap();
    let collapsed = re_newlines.replace_all(&collapsed, "\n\n");
    collapsed.trim().to_string()
}

/// Clean a title by splitting on common separators and keeping the longest part.
pub fn clean_title(title: &str) -> String {
    let separators = [" - ", " | ", " \u{2014} ", " \u{2013} ", " :: "];
    for sep in separators {
        if title.contains(sep) {
            let parts: Vec<&str> = title.split(sep).collect();
            return parts
                .iter()
                .max_by_key(|s| s.len())
                .unwrap_or(&title)
                .trim()
                .to_string();
        }
    }
    title.trim().to_string()
}

/// Extract clean text from HTML.
pub fn extract_text(html: &str) -> ExtractionResult {
    let (canonical_url, title_clean) = extract_metadata(html);
    let content_region = extract_content_region(html);

    let text = strip_non_content_tags(content_region);
    let text = replace_block_tags(&text);
    let text = replace_inline_tags(&text);
    let text = strip_remaining_tags(&text);
    let text = decode_entities(&text);
    let text = collapse_whitespace(&text);

    ExtractionResult {
        text,
        canonical_url,
        title_clean,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_title_separator_dash() {
        assert_eq!(clean_title("Article | The Guardian"), "The Guardian");
    }

    #[test]
    fn test_clean_title_separator_pipe() {
        assert_eq!(clean_title("Article | The Guardian"), "The Guardian");
    }

    #[test]
    fn test_clean_title_no_separator() {
        assert_eq!(clean_title("Just a title"), "Just a title");
    }

    #[test]
    fn test_strip_script_tags() {
        let html = r#"<p>Hello</p><script>alert('hi')</script><p>World</p>"#;
        let result = extract_text(html);
        assert!(!result.text.contains("alert"));
        assert!(result.text.contains("Hello"));
        assert!(result.text.contains("World"));
    }

    #[test]
    fn test_extract_canonical() {
        let html = r#"<html><head><link rel="canonical" href="https://example.com/page" /></head><body><p>Hi</p></body></html>"#;
        let result = extract_text(html);
        assert_eq!(
            result.canonical_url,
            Some("https://example.com/page".to_string())
        );
    }

    #[test]
    fn test_extract_og_title() {
        let html = r#"<html><head><meta property="og:title" content="OG Title" /><title>Old Title</title></head><body><p>Hi</p></body></html>"#;
        let result = extract_text(html);
        assert_eq!(result.title_clean, Some("OG Title".to_string()));
    }

    #[test]
    fn test_extract_title_fallback() {
        let html =
            r#"<html><head><title>Fallback Title</title></head><body><p>Hi</p></body></html>"#;
        let result = extract_text(html);
        assert_eq!(result.title_clean, Some("Fallback Title".to_string()));
    }
}
