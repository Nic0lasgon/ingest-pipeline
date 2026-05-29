use ingest_pipeline::utils::dedup::{check_duplicate, jaccard_similarity, normalize_url};

#[test]
fn test_normalize_url_basic() {
    assert_eq!(
        normalize_url("https://Example.com/Path"),
        "https://example.com/path"
    );
}

#[test]
fn test_normalize_url_trailing_slash() {
    assert_eq!(
        normalize_url("https://example.com/path/"),
        "https://example.com/path"
    );
    assert_eq!(normalize_url("https://example.com/"), "https://example.com");
}

#[test]
fn test_normalize_url_query_fragment() {
    assert_eq!(
        normalize_url("https://example.com/path?foo=bar#anchor"),
        "https://example.com/path"
    );
    assert_eq!(
        normalize_url("https://example.com/path#section"),
        "https://example.com/path"
    );
}

#[test]
fn test_normalize_url_protocol_relative() {
    assert_eq!(
        normalize_url("//example.com/path"),
        "https://example.com/path"
    );
}

#[test]
fn test_jaccard_identical() {
    assert!((jaccard_similarity("hello world", "hello world") - 1.0).abs() < 1e-10);
}

#[test]
fn test_jaccard_completely_different() {
    assert!((jaccard_similarity("hello world", "foo bar baz") - 0.0).abs() < 1e-10);
}

#[test]
fn test_jaccard_partial() {
    let sim = jaccard_similarity("hello world foo", "hello world bar");
    assert!((sim - 0.5).abs() < 1e-10);
}

#[test]
fn test_jaccard_stop_words() {
    let sim = jaccard_similarity("the cat in the hat", "the cat on the mat");
    assert!((sim - 1.0 / 3.0).abs() < 1e-10);
}

#[test]
fn test_check_duplicate_url_exact() {
    let existing = vec![(
        "id-1".to_string(),
        "https://example.com/article".to_string(),
        None,
        "Some title".to_string(),
    )];
    let result = check_duplicate(
        "https://example.com/article",
        None,
        "Another title",
        &existing,
    );
    assert!(result.is_duplicate);
    assert_eq!(result.reason, "url_exact_match");
    assert_eq!(result.duplicate_of, Some("id-1".to_string()));
}

#[test]
fn test_check_duplicate_canonical() {
    let existing = vec![(
        "id-1".to_string(),
        "https://example.com/redirect".to_string(),
        Some("https://example.com/canonical".to_string()),
        "Some title".to_string(),
    )];
    let result = check_duplicate(
        "https://example.com/canonical",
        None,
        "Another title",
        &existing,
    );
    assert!(result.is_duplicate);
    assert_eq!(result.reason, "url_exact_match");
    assert_eq!(result.duplicate_of, Some("id-1".to_string()));
}

#[test]
fn test_check_duplicate_title_similar() {
    let existing = vec![(
        "id-1".to_string(),
        "https://example.com/a".to_string(),
        None,
        "Breaking News Major Event Happens Today".to_string(),
    )];
    let result = check_duplicate(
        "https://example.com/b",
        None,
        "Breaking News: Major Event Happens Today!",
        &existing,
    );
    assert!(result.is_duplicate);
    assert_eq!(result.reason, "title_similarity");
    assert_eq!(result.duplicate_of, Some("id-1".to_string()));
}

#[test]
fn test_check_duplicate_distinct() {
    let existing = vec![(
        "id-1".to_string(),
        "https://example.com/a".to_string(),
        None,
        "Weather Forecast Sunny Skies Ahead".to_string(),
    )];
    let result = check_duplicate(
        "https://example.com/b",
        None,
        "Sports Team Wins Championship Final",
        &existing,
    );
    assert!(!result.is_duplicate);
    assert_eq!(result.reason, "distinct");
    assert_eq!(result.duplicate_of, None);
}

#[test]
fn test_check_duplicate_near_duplicate() {
    let existing = vec![(
        "id-1".to_string(),
        "https://example.com/a".to_string(),
        None,
        "Breaking News Major Event Happens Today Around World Crisis Summit".to_string(),
    )];
    let result = check_duplicate(
        "https://example.com/b",
        None,
        "Breaking News Major Event Happens Today Around World Crisis Meeting",
        &existing,
    );
    assert!(result.is_duplicate);
    assert_eq!(result.reason, "title_similarity");
}
