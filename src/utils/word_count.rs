pub const MIN_ARTICLE_WORD_COUNT: usize = 350;

pub fn count_words(text: &str) -> usize {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }
    trimmed.split_whitespace().count()
}
