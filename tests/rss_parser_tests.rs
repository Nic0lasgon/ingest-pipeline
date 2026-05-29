use ingest_pipeline::utils::rss_parser::{
    normalize_url, parse_date, parse_feed, strip_html, unwrap_cdata,
};
use ingest_pipeline::utils::shared::decode_html_entities;

fn load_fixture(name: &str) -> String {
    let path = format!("tests/fixtures/rss/{}", name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to load fixture {}: {}", path, e))
}

#[test]
fn test_parse_rss2_simple() {
    let xml = load_fixture("rss2_simple.xml");
    let feed = parse_feed(&xml, "https://example.com").expect("Failed to parse RSS 2.0");

    assert_eq!(feed.items.len(), 3);

    let first = &feed.items[0];
    assert_eq!(first.title.as_deref(), Some("First Article"));
    assert_eq!(first.link.as_deref(), Some("https://example.com/article-1"));
    assert_eq!(
        first.description.as_deref(),
        Some("This is the first article description.")
    );
    assert!(first.pub_date.is_some());
    assert_eq!(first.author.as_deref(), Some("john@example.com (John Doe)"));

    let second = &feed.items[1];
    assert_eq!(second.title.as_deref(), Some("Second Article"));
    assert_eq!(
        second.description.as_deref(),
        Some("Another article with bold text.")
    );

    let third = &feed.items[2];
    assert_eq!(third.title.as_deref(), Some("Third Article"));
    assert_eq!(
        third.image_url.as_deref(),
        Some("https://example.com/image.jpg")
    );
}

#[test]
fn test_parse_rss2_cdata() {
    let xml = load_fixture("rss2_cdata.xml");
    let feed = parse_feed(&xml, "https://example.com").expect("Failed to parse CDATA feed");

    assert_eq!(feed.items.len(), 3);

    let first = &feed.items[0];
    assert_eq!(first.title.as_deref(), Some("Article with HTML & entities"));
    assert_eq!(first.author.as_deref(), Some("Jane & Co"));

    let second = &feed.items[1];
    assert_eq!(second.title.as_deref(), Some("Entity & Test <tag>"));
    assert_eq!(
        second.description.as_deref(),
        Some("Content with \"quotes\" and 'apostrophe' and ! entity.")
    );
    assert_eq!(
        second.image_url.as_deref(),
        Some("https://cdn.example.com/media.jpg")
    );
}

#[test]
fn test_parse_atom() {
    let xml = load_fixture("atom_feed.xml");
    let feed = parse_feed(&xml, "https://example.com").expect("Failed to parse Atom feed");

    assert_eq!(feed.items.len(), 3);

    let first = &feed.items[0];
    assert_eq!(first.title.as_deref(), Some("Atom Entry One"));
    assert_eq!(
        first.link.as_deref(),
        Some("https://example.com/atom/entry-1")
    );
    assert_eq!(
        first.description.as_deref(),
        Some("Summary of the first Atom entry.")
    );
    assert_eq!(first.author.as_deref(), Some("Alice Martin"));
    assert!(first.pub_date.is_some());

    let second = &feed.items[1];
    assert_eq!(second.title.as_deref(), Some("Atom Entry Two"));
    assert_eq!(
        second.description.as_deref(),
        Some("Full HTML content of entry two.")
    );
    assert_eq!(second.author.as_deref(), Some("Bob Dupont"));

    let third = &feed.items[2];
    assert_eq!(
        third.image_url.as_deref(),
        Some("https://example.com/atom-img.jpg")
    );
}

#[test]
fn test_parse_json_feed() {
    let json = load_fixture("json_feed.json");
    let feed = parse_feed(&json, "").expect("Failed to parse JSON Feed");

    assert_eq!(feed.items.len(), 3);

    let first = &feed.items[0];
    assert_eq!(first.title.as_deref(), Some("JSON Feed Item One"));
    assert_eq!(
        first.link.as_deref(),
        Some("https://example.com/json/item-1")
    );
    assert_eq!(
        first.description.as_deref(),
        Some("Plain text content of the first item.")
    );
    assert_eq!(first.author.as_deref(), Some("Claire Moreau"));
    assert_eq!(
        first.image_url.as_deref(),
        Some("https://example.com/json-img-1.jpg")
    );
    assert!(first.pub_date.is_some());

    let second = &feed.items[1];
    assert_eq!(second.title.as_deref(), Some("JSON Feed Item Two"));
    assert_eq!(
        second.description.as_deref(),
        Some("A short summary of the second item.")
    );
    assert_eq!(second.author.as_deref(), Some("David Leroy"));
    assert_eq!(
        second.image_url.as_deref(),
        Some("https://example.com/banner-2.jpg")
    );

    let third = &feed.items[2];
    assert!(third.title.is_none());
    assert_eq!(
        third.link.as_deref(),
        Some("https://example.com/json/item-3")
    );
}

#[test]
fn test_parse_malformed() {
    let xml = load_fixture("malformed.xml");
    // Malformed XML with missing '>' on <rss> tag - our parser will try but may get 0 items
    // The behavior depends on how broken the XML is
    let result = parse_feed(&xml, "https://example.com");
    // If it parses, it should handle gracefully (possibly 0 items)
    // If it errors, that's also acceptable for truly broken XML
    match result {
        Ok(feed) => {
            // Items with empty title AND empty link should be filtered
            for item in &feed.items {
                assert!(
                    item.title.is_some() || item.link.is_some(),
                    "Items with neither title nor link should be filtered"
                );
            }
        }
        Err(_) => {
            // Acceptable: truly malformed XML
        }
    }
}

#[test]
fn test_normalize_relative_url() {
    assert_eq!(
        normalize_url("https://absolute.com/img.jpg", "https://base.com"),
        "https://absolute.com/img.jpg"
    );
    assert_eq!(
        normalize_url("//cdn.com/img.jpg", "https://base.com"),
        "https://cdn.com/img.jpg"
    );
    assert_eq!(
        normalize_url("/path/img.jpg", "https://base.com/page/sub"),
        "https://base.com/path/img.jpg"
    );
    assert_eq!(
        normalize_url("relative/img.jpg", "https://base.com/feed.xml"),
        "https://base.com/relative/img.jpg"
    );
    assert_eq!(
        normalize_url("relative/img.jpg", "https://base.com/dir/"),
        "https://base.com/dir/relative/img.jpg"
    );
    assert_eq!(normalize_url("/img.jpg", ""), "/img.jpg");
}

#[test]
fn test_strip_html_integration() {
    assert_eq!(strip_html("<p>Hello <b>world</b></p>"), "\nHello world\n");
    assert_eq!(strip_html("<br/>line two"), "\nline two");
    assert_eq!(strip_html("plain text"), "plain text");
    assert_eq!(
        strip_html("<a href=\"https://link.com\">click</a>"),
        "click"
    );
    assert_eq!(
        strip_html("<div><span>nested</span> <em>text</em></div>"),
        "\nnested text\n"
    );
}

#[test]
fn test_parse_date_rfc822() {
    let dt = parse_date("Mon, 15 Jan 2024 10:30:00 GMT").unwrap();
    assert_eq!(
        dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        "2024-01-15 10:30:00"
    );

    let dt2 = parse_date("Tue, 16 Jan 2024 14:00:00 +0100").unwrap();
    assert_eq!(
        dt2.format("%Y-%m-%d %H:%M:%S").to_string(),
        "2024-01-16 13:00:00"
    );

    // Without day name
    let dt3 = parse_date("15 Jan 2024 10:30:00 +0000").unwrap();
    assert_eq!(
        dt3.format("%Y-%m-%d %H:%M:%S").to_string(),
        "2024-01-15 10:30:00"
    );

    // Invalid
    assert!(parse_date("not a date").is_none());
    assert!(parse_date("").is_none());
}

#[test]
fn test_parse_date_iso8601() {
    let dt = parse_date("2024-01-15T10:30:00Z").unwrap();
    assert_eq!(
        dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        "2024-01-15 10:30:00"
    );

    let dt2 = parse_date("2024-01-15T10:30:00+02:00").unwrap();
    assert_eq!(
        dt2.format("%Y-%m-%d %H:%M:%S").to_string(),
        "2024-01-15 08:30:00"
    );

    let dt3 = parse_date("2024-01-15T10:30:00").unwrap();
    assert_eq!(
        dt3.format("%Y-%m-%d %H:%M:%S").to_string(),
        "2024-01-15 10:30:00"
    );

    let dt4 = parse_date("2024-01-15").unwrap();
    assert_eq!(dt4.format("%Y-%m-%d").to_string(), "2024-01-15");
}

#[test]
fn test_lemonde_fixture() {
    let xml = load_fixture("lemonde.xml");
    let feed = parse_feed(&xml, "https://www.lemonde.fr").expect("Failed to parse Le Monde feed");

    assert_eq!(feed.items.len(), 3);

    let first = &feed.items[0];
    assert!(first
        .title
        .as_deref()
        .unwrap()
        .contains("conférence climat"));
    assert_eq!(first.link.as_deref(), Some("https://www.lemonde.fr/planete/article/2024/01/15/les-enjeux-de-la-prochaine-conference-climat_6109001_3244.html"));
    assert_eq!(first.author.as_deref(), Some("Marie Dupont"));
    assert_eq!(
        first.image_url.as_deref(),
        Some("https://img.lemde.fr/2024/01/15/climate.jpg")
    );

    let second = &feed.items[1];
    assert!(second.title.as_deref().unwrap().contains("régulations"));
    assert_eq!(
        second.image_url.as_deref(),
        Some("https://img.lemde.fr/2024/01/14/tech-thumb.jpg")
    );

    let third = &feed.items[2];
    assert!(third.title.as_deref().unwrap().contains("BCE"));
    assert_eq!(
        third.image_url.as_deref(),
        Some("https://img.lemde.fr/2024/01/13/bce.jpg")
    );
}

#[test]
fn test_decode_html_entities() {
    assert_eq!(decode_html_entities("&amp;test"), "&test");
    assert_eq!(decode_html_entities("&lt;div&gt;"), "<div>");
    assert_eq!(decode_html_entities("it&#39;s"), "it's");
    assert_eq!(decode_html_entities("&#x21;"), "!");
    assert_eq!(decode_html_entities("&#169;"), "©");
    assert_eq!(
        decode_html_entities("hello &amp; &quot;world&quot;"),
        "hello & \"world\""
    );
}

#[test]
fn test_unwrap_cdata() {
    assert_eq!(unwrap_cdata("<![CDATA[hello world]]>"), "hello world");
    assert_eq!(unwrap_cdata("no cdata"), "no cdata");
    assert_eq!(unwrap_cdata("<![CDATA[<b>bold</b>]]>"), "<b>bold</b>");
    assert_eq!(
        unwrap_cdata("before <![CDATA[middle]]> after"),
        "before middle after"
    );
}

#[test]
fn test_empty_feed() {
    let xml =
        r#"<?xml version="1.0"?><rss version="2.0"><channel><title>Empty</title></channel></rss>"#;
    let feed = parse_feed(xml, "https://example.com").expect("Failed to parse empty feed");
    assert_eq!(feed.items.len(), 0);
}

#[test]
fn test_empty_json_feed() {
    let json = r#"{"version": "https://jsonfeed.org/version/1.1", "items": []}"#;
    let feed = parse_feed(json, "").expect("Failed to parse empty JSON feed");
    assert_eq!(feed.items.len(), 0);
}
