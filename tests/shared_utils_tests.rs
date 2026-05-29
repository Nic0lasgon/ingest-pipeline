use ingest_pipeline::utils::shared::{decode_html_entities, safe_parse_json};

#[test]
fn test_decode_html_entities_basic() {
    assert_eq!(decode_html_entities("&amp;"), "&");
    assert_eq!(decode_html_entities("&lt;"), "<");
    assert_eq!(decode_html_entities("&gt;"), ">");
    assert_eq!(decode_html_entities("&quot;"), "\"");
    assert_eq!(decode_html_entities("&apos;"), "'");
}

#[test]
fn test_decode_html_entities_numeric() {
    assert_eq!(decode_html_entities("&#39;"), "'");
    assert_eq!(decode_html_entities("&#x27;"), "'");
    assert_eq!(decode_html_entities("&#65;"), "A");
    assert_eq!(decode_html_entities("&#x41;"), "A");
    assert_eq!(decode_html_entities("&#123;"), "{");
    assert_eq!(decode_html_entities("&#x7B;"), "{");
}

#[test]
fn test_decode_html_entities_french() {
    assert_eq!(decode_html_entities("&eacute;"), "é");
    assert_eq!(decode_html_entities("&egrave;"), "è");
    assert_eq!(decode_html_entities("&ecirc;"), "ê");
    assert_eq!(decode_html_entities("&agrave;"), "à");
    assert_eq!(decode_html_entities("&acirc;"), "â");
    assert_eq!(decode_html_entities("&ccedil;"), "ç");
    assert_eq!(decode_html_entities("&ugrave;"), "ù");
    assert_eq!(decode_html_entities("&ucirc;"), "û");
    assert_eq!(decode_html_entities("&icirc;"), "î");
    assert_eq!(decode_html_entities("&ocirc;"), "ô");
}

#[test]
fn test_decode_html_entities_mixed() {
    let input = "&lt;b&gt;c&rsquo;est &eacute;vident &amp; &ccedil;a marche&lt;/b&gt;";
    let expected = "<b>c'est évident & ça marche</b>";
    assert_eq!(decode_html_entities(input), expected);
}

#[test]
fn test_decode_html_entities_typographic() {
    assert_eq!(decode_html_entities("&laquo;"), "«");
    assert_eq!(decode_html_entities("&raquo;"), "»");
    assert_eq!(decode_html_entities("&ldquo;"), "\u{201c}");
    assert_eq!(decode_html_entities("&rdquo;"), "\u{201d}");
    assert_eq!(decode_html_entities("&hellip;"), "…");
    assert_eq!(decode_html_entities("&mdash;"), "—");
    assert_eq!(decode_html_entities("&ndash;"), "–");
    assert_eq!(decode_html_entities("&rsquo;"), "'");
    assert_eq!(decode_html_entities("&lsquo;"), "'");
    assert_eq!(decode_html_entities("&euro;"), "€");
    assert_eq!(decode_html_entities("&oslash;"), "ø");
    assert_eq!(decode_html_entities("&aelig;"), "æ");
    assert_eq!(decode_html_entities("&nbsp;"), "\u{00a0}");
}

#[test]
fn test_decode_html_entities_no_match() {
    assert_eq!(decode_html_entities("no entities here"), "no entities here");
    assert_eq!(decode_html_entities(""), "");
}

#[test]
fn test_safe_parse_json_valid() {
    let result: Option<serde_json::Value> = safe_parse_json(r#"{"key": "value"}"#);
    assert!(result.is_some());
    assert_eq!(result.unwrap()["key"], "value");
}

#[test]
fn test_safe_parse_json_invalid() {
    let result: Option<serde_json::Value> = safe_parse_json("not valid json");
    assert!(result.is_none());
}

#[test]
fn test_safe_parse_json_wrong_type() {
    let result: Option<Vec<String>> = safe_parse_json(r#"{"key": "value"}"#);
    assert!(result.is_none());
}

#[test]
fn test_safe_parse_json_empty() {
    let result: Option<serde_json::Value> = safe_parse_json("");
    assert!(result.is_none());
}
