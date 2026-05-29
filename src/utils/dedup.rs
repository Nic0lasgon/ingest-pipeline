use std::collections::HashSet;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq)]
pub struct DedupResult {
    pub is_duplicate: bool,
    pub duplicate_of: Option<String>,
    pub reason: String,
}

const STOP_WORDS: &[&str] = &[
    "le", "la", "les", "un", "une", "des", "et", "ou", "mais", "donc", "or", "ni", "car", "de",
    "du", "à", "au", "aux", "en", "par", "pour", "avec", "sans", "sur", "sous", "dans", "chez",
    "selon", "vers", "pendant", "parmi", "the", "a", "an", "and", "or", "but", "of", "to", "in",
    "for", "on", "at", "by", "with", "from", "as", "is", "was", "are", "were", "be", "been",
    "have", "has", "had", "do", "does", "did", "will", "would", "could", "should",
];

fn get_url_regex() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^(https?://)([^/?#]+)([^?#]*)").unwrap())
}

pub fn normalize_url(url: &str) -> String {
    let url = url.trim();

    let url = if url.starts_with("//") {
        format!("https:{}", url)
    } else {
        url.to_string()
    };

    let url_lower = url.to_lowercase();

    if let Some(caps) = get_url_regex().captures(&url_lower) {
        let scheme = &caps[1];
        let host = &caps[2];
        let path = caps[3].trim_end_matches('/');
        format!("{}{}{}", scheme, host, path)
    } else {
        url_lower
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| c.is_whitespace() || "!?.:,;\"'()[]{}".contains(c))
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .filter(|s| !STOP_WORDS.contains(&s.as_str()))
        .collect()
}

pub fn jaccard_similarity(a: &str, b: &str) -> f64 {
    let tokens_a: HashSet<String> = tokenize(a).into_iter().collect();
    let tokens_b: HashSet<String> = tokenize(b).into_iter().collect();

    if tokens_a.is_empty() && tokens_b.is_empty() {
        return 1.0;
    }

    let intersection = tokens_a.intersection(&tokens_b).count();
    let union = tokens_a.union(&tokens_b).count();

    intersection as f64 / union as f64
}

pub fn check_duplicate(
    article_url: &str,
    article_canonical_url: Option<&str>,
    article_title: &str,
    existing_articles: &[(String, String, Option<String>, String)],
) -> DedupResult {
    let article_url_norm = normalize_url(article_url);
    let article_canonical_norm = article_canonical_url.map(normalize_url);

    for (id, existing_url, existing_canonical, existing_title) in existing_articles {
        let existing_url_norm = normalize_url(existing_url);

        if existing_url_norm == article_url_norm {
            return DedupResult {
                is_duplicate: true,
                duplicate_of: Some(id.clone()),
                reason: "url_exact_match".to_string(),
            };
        }

        if let Some(canonical) = existing_canonical {
            if normalize_url(canonical) == article_url_norm {
                return DedupResult {
                    is_duplicate: true,
                    duplicate_of: Some(id.clone()),
                    reason: "url_exact_match".to_string(),
                };
            }
        }

        if let Some(ref article_canonical) = article_canonical_norm {
            if existing_url_norm == *article_canonical {
                return DedupResult {
                    is_duplicate: true,
                    duplicate_of: Some(id.clone()),
                    reason: "url_exact_match".to_string(),
                };
            }

            if let Some(existing_canonical) = existing_canonical {
                if normalize_url(existing_canonical) == *article_canonical {
                    return DedupResult {
                        is_duplicate: true,
                        duplicate_of: Some(id.clone()),
                        reason: "url_exact_match".to_string(),
                    };
                }
            }
        }

        let similarity = jaccard_similarity(article_title, existing_title);
        if similarity >= 0.8 {
            return DedupResult {
                is_duplicate: true,
                duplicate_of: Some(id.clone()),
                reason: "title_similarity".to_string(),
            };
        }
    }

    DedupResult {
        is_duplicate: false,
        duplicate_of: None,
        reason: "distinct".to_string(),
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_normalize_url_basic() {
        assert_eq!(
            normalize_url("https://Example.com/Path"),
            "https://example.com/path"
        );
        assert_eq!(
            normalize_url("HTTPS://EXAMPLE.COM/Path"),
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
        let tokens: HashSet<String> = tokenize("the cat in the hat").into_iter().collect();
        assert!(!tokens.contains("the"));
        assert!(tokens.contains("cat"));
        assert!(tokens.contains("hat"));
        assert!((sim - 1.0 / 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_jaccard_all_stop_words() {
        let sim = jaccard_similarity("the a an of to", "le la de du");
        assert!((sim - 1.0).abs() < 1e-10);
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
    fn test_check_duplicate_url_case_insensitive() {
        let existing = vec![(
            "id-1".to_string(),
            "HTTPS://EXAMPLE.COM/Article".to_string(),
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
    fn test_check_duplicate_article_canonical_matches_existing_url() {
        let existing = vec![(
            "id-1".to_string(),
            "https://example.com/original".to_string(),
            None,
            "Some title".to_string(),
        )];
        let result = check_duplicate(
            "https://example.com/some-other",
            Some("https://example.com/original"),
            "Another title",
            &existing,
        );
        assert!(result.is_duplicate);
        assert_eq!(result.reason, "url_exact_match");
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
        let similarity = jaccard_similarity(
            "Breaking News Major Event Happens Today Around World Crisis Summit",
            "Breaking News Major Event Happens Today Around World Crisis Meeting",
        );
        assert!(similarity >= 0.8);
        assert!(result.is_duplicate);
        assert_eq!(result.reason, "title_similarity");
    }
}
