use ingest_pipeline::utils::word_count::{count_words, MIN_ARTICLE_WORD_COUNT};

#[test]
fn test_count_words_empty() {
    assert_eq!(count_words(""), 0);
    assert_eq!(count_words("   "), 0);
}

#[test]
fn test_count_words_single() {
    assert_eq!(count_words("Hello"), 1);
    assert_eq!(count_words("  Bonjour  "), 1);
}

#[test]
fn test_count_words_multiple() {
    assert_eq!(count_words("One two three four five"), 5);
}

#[test]
fn test_count_words_whitespace() {
    assert_eq!(count_words("  Hello   world  "), 2);
}

#[test]
fn test_count_words_newlines() {
    assert_eq!(count_words("line one\nline two\nline three"), 6);
}

#[test]
fn test_min_article_word_count() {
    assert_eq!(MIN_ARTICLE_WORD_COUNT, 350);
}
