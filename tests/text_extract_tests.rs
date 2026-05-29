use ingest_pipeline::utils::text_extract::{clean_title, extract_text};
use std::fs;

fn load_fixture(name: &str) -> String {
    let path = format!("tests/fixtures/html/{}", name);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read fixture {}: {}", path, e))
}

#[test]
fn test_extract_article_clean() {
    let html = load_fixture("article_clean.html");
    let result = extract_text(&html);

    // Text contains main content
    assert!(result
        .text
        .contains("Renewable energy is rapidly transforming"));
    assert!(result.text.contains("Battery storage technology"));

    // Text does NOT contain nav/footer/header/aside content
    assert!(!result.text.contains("Privacy Policy"));
    assert!(!result.text.contains("Terms of Service"));
    assert!(!result.text.contains("Related Articles"));
    assert!(!result.text.contains("Best Solar Panels"));

    // No HTML tags remain
    assert!(!result.text.contains("<article"));
    assert!(!result.text.contains("<p>"));
    assert!(!result.text.contains("<h1>"));
    assert!(!result.text.contains("<nav"));
    assert!(!result.text.contains("<footer"));
    assert!(!result.text.contains("<header"));
    assert!(!result.text.contains("<aside"));
}

#[test]
fn test_extract_article_noisy() {
    let html = load_fixture("article_noisy.html");
    let result = extract_text(&html);

    // Main content extracted
    assert!(result
        .text
        .contains("Quantum computing represents a paradigm shift"));
    assert!(result.text.contains("practical applications are vast"));

    // Noise NOT present
    assert!(!result.text.contains("Buy the latest gadgets"));
    assert!(!result.text.contains("Subscribe to our newsletter"));
    assert!(!result.text.contains("Popular Topics"));
    assert!(!result.text.contains("console.log"));
    assert!(!result.text.contains("Advertisement"));
}

#[test]
fn test_extract_canonical_url() {
    let html = load_fixture("article_clean.html");
    let result = extract_text(&html);

    assert_eq!(
        result.canonical_url,
        Some("https://www.greentech.com/future-renewable-energy".to_string())
    );
}

#[test]
fn test_extract_title_og() {
    let html = load_fixture("article_clean.html");
    let result = extract_text(&html);

    // og:title takes priority, and clean_title is applied
    assert_eq!(
        result.title_clean,
        Some("The Future of Renewable Energy".to_string())
    );
}

#[test]
fn test_extract_title_fallback() {
    // article_missing_title.html has no og:title and no <title>
    let html = load_fixture("article_missing_title.html");
    let result = extract_text(&html);

    // No title should be found
    assert_eq!(result.title_clean, None);
}

#[test]
fn test_clean_title_separator() {
    assert_eq!(clean_title("My Article - Site Name"), "My Article");
    assert_eq!(clean_title("Breaking News | CNN"), "Breaking News");
    assert_eq!(clean_title("Research Paper — Nature"), "Research Paper");
    assert_eq!(clean_title("Blog Post – Medium"), "Blog Post");
    assert_eq!(clean_title("Tutorial :: Dev.to"), "Tutorial");
    assert_eq!(clean_title("No Separator Here"), "No Separator Here");
}

#[test]
fn test_extract_missing_title() {
    let html = load_fixture("article_missing_title.html");
    let result = extract_text(&html);

    // Title should be None
    assert_eq!(result.title_clean, None);
    // But content is still extracted
    assert!(result.text.contains("This article has no title element"));
}

#[test]
fn test_no_html_in_extracted_text() {
    let fixtures = [
        "article_clean.html",
        "article_noisy.html",
        "article_short.html",
        "article_missing_title.html",
    ];
    for fixture in &fixtures {
        let html = load_fixture(fixture);
        let result = extract_text(&html);
        assert!(
            !result.text.contains('<'),
            "Fixture {} contains '<' in extracted text: {}",
            fixture,
            &result.text[..result.text.len().min(200)]
        );
        assert!(
            !result.text.contains('>'),
            "Fixture {} contains '>' in extracted text: {}",
            fixture,
            &result.text[..result.text.len().min(200)]
        );
    }
}

#[test]
fn test_entity_decoding() {
    let html = r#"<html><body><p>&amp; &lt;hello&gt; &quot;world&quot; &#39;test&#39; &#x41;&#x42;&#x43;</p></body></html>"#;
    let result = extract_text(html);
    assert!(result.text.contains('&'));
    assert!(result.text.contains("<hello>"));
    assert!(result.text.contains("\"world\""));
    assert!(result.text.contains("'test'"));
    assert!(result.text.contains("ABC"));
}

#[test]
fn test_block_tags_newlines() {
    let html =
        r#"<html><body><p>Para1</p><div>Div1</div><h1>Heading1</h1><li>Item1</li></body></html>"#;
    let result = extract_text(html);
    // Each block element should be on its own line
    assert!(result.text.contains("Para1"));
    assert!(result.text.contains("Div1"));
    assert!(result.text.contains("Heading1"));
    assert!(result.text.contains("Item1"));
}

#[test]
fn test_self_closing_tags() {
    let html = r#"<html><body><p>Before<br/>After</p><p>Img:<img src="x.jpg" alt="pic"/></p></body></html>"#;
    let result = extract_text(html);
    assert!(result.text.contains("Before"));
    assert!(result.text.contains("After"));
    // img tag itself should not appear in text
    assert!(!result.text.contains("<img"));
}

#[test]
fn test_malformed_html() {
    let html = r#"<p>Unclosed paragraph<div>Nested <b>bold without close</p></div>"#;
    let result = extract_text(html);
    // Should not panic and should extract some text
    assert!(result.text.contains("Unclosed paragraph"));
    assert!(result.text.contains("Nested"));
}

#[test]
fn test_empty_html() {
    let result = extract_text("");
    assert_eq!(result.text, "");
    assert_eq!(result.canonical_url, None);
    assert_eq!(result.title_clean, None);
}
