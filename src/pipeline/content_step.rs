use anyhow::Result;
use reqwest::Client;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::db::article_queries::{
    enrich_from_duplicate, find_similar_articles, get_by_id, get_comparable_articles,
    update_duplicate_and_processing, update_extraction, update_processing_status,
};
use crate::db::rejected_queries::insert_from_article;
use crate::db::schema::{DuplicateStatus, ProcessingStatus};
use crate::utils::dedup::check_duplicate;
use crate::utils::text_extract::{clean_with_trafilatura, extract_text, ExtractionResult};
use crate::utils::word_count::{count_words, MIN_ARTICLE_WORD_COUNT};

pub const CONTENT_FETCH_TIMEOUT_MS: u64 = 15_000;
const MIN_TEXT_LENGTH: usize = 300;
const STRATEGIES: &[&str] = &[
    "default",
    "google-referrer",
    "twitter-referrer",
    "amp",
    "facebook-referrer",
];

#[derive(Debug, Clone, PartialEq)]
pub enum ContentStepStatus {
    Extracted,
    ExtractionFailed,
    RejectedDuplicate,
    PendingQualification,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct ContentStepResult {
    pub article_id: Uuid,
    pub status: ContentStepStatus,
    pub content_length: Option<usize>,
    pub duplicate_reason: Option<String>,
    pub error: Option<String>,
    pub retryable: bool,
}

pub async fn process_content_step(
    pool: &PgPool,
    article_id: Uuid,
    config: Option<&Config>,
) -> ContentStepResult {
    // 1. Guard: load article
    let article = match get_by_id(pool, article_id).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return ContentStepResult {
                article_id,
                status: ContentStepStatus::ExtractionFailed,
                content_length: None,
                duplicate_reason: None,
                error: Some("not_found".to_string()),
                retryable: false,
            };
        }
        Err(e) => {
            error!(%article_id, error = %e, "Failed to load article");
            return ContentStepResult {
                article_id,
                status: ContentStepStatus::ExtractionFailed,
                content_length: None,
                duplicate_reason: None,
                error: Some(format!("DB error: {e}")),
                retryable: true,
            };
        }
    };

    // Already processed? Skip.
    if article.processing_status != ProcessingStatus::Ingested
        && article.processing_status != ProcessingStatus::Extracted
    {
        info!(
            %article_id,
            status = ?article.processing_status,
            "Article already processed, skipping"
        );
        return ContentStepResult {
            article_id,
            status: ContentStepStatus::Extracted,
            content_length: article.content_length.map(|l| l as usize),
            duplicate_reason: None,
            error: None,
            retryable: false,
        };
    }

    // 2. Pre-dedup by URL
    let comparable = match get_comparable_articles(pool, article_id, 500).await {
        Ok(c) => c,
        Err(e) => {
            error!(%article_id, error = %e, "Failed to get comparable articles");
            return ContentStepResult {
                article_id,
                status: ContentStepStatus::ExtractionFailed,
                content_length: None,
                duplicate_reason: None,
                error: Some(format!("DB error: {e}")),
                retryable: true,
            };
        }
    };

    let dedup_input: Vec<(String, String, Option<String>, String)> = comparable
        .iter()
        .map(|c| {
            (
                c.id.to_string(),
                c.url.clone(),
                c.canonical_url.clone(),
                c.title.clone(),
            )
        })
        .collect();

    // URL-only dedup (title_clean not available yet)
    let url_dup = check_duplicate(&article.url, None, &article.title, &dedup_input);
    if url_dup.is_duplicate {
        let dup_id = url_dup.duplicate_of.and_then(|s| Uuid::parse_str(&s).ok());
        if let Some(dup_id) = dup_id {
            let _ = enrich_from_duplicate(pool, article_id, dup_id).await;
            let _ = insert_from_article(
                pool,
                article_id,
                &article.source_id,
                &article.title,
                &article.url,
                "duplicate",
                Some(&format!("url_dup: {}", url_dup.reason)),
            )
            .await;
        }
        info!(%article_id, reason = %url_dup.reason, "Rejected as URL duplicate");
        return ContentStepResult {
            article_id,
            status: ContentStepStatus::RejectedDuplicate,
            content_length: None,
            duplicate_reason: Some(url_dup.reason),
            error: None,
            retryable: false,
        };
    }

    // 3. Extraction: try strategies
    let mut extraction: Option<ExtractionResult> = None;

    let default_client = Client::builder()
        .timeout(std::time::Duration::from_millis(CONTENT_FETCH_TIMEOUT_MS))
        .build()
        .unwrap_or_else(|_| Client::new());

    let client = config
        .map(|c| c.http_client.client.as_ref())
        .unwrap_or(&default_client);

    for strategy in STRATEGIES {
        match fetch_with_strategy(client, &article.url, strategy).await {
            Ok(html) => {
                let regex_result = extract_text(&html);
                let result =
                    clean_with_trafilatura(&html, Some(&article.url)).unwrap_or(regex_result);
                if result.text.len() >= MIN_TEXT_LENGTH
                    && count_words(&result.text) >= MIN_ARTICLE_WORD_COUNT
                {
                    info!(
                        %article_id,
                        strategy,
                        text_len = result.text.len(),
                        word_count = count_words(&result.text),
                        "Extraction succeeded"
                    );
                    extraction = Some(result);
                    break;
                }
            }
            Err(e) => {
                warn!(%article_id, strategy, error = %e, "Strategy failed");
            }
        }
    }

    // 4. Fallback Hetzner
    if extraction.is_none() {
        if let Some(cfg) = config {
            if let (Some(ref url), Some(ref secret)) =
                (&cfg.hetzner_extract_url, &cfg.hetzner_extract_secret)
            {
                match fetch_hetzner(&cfg.http_client.client, url, secret, &article.url).await {
                    Ok(html) => {
                        let regex_result = extract_text(&html);
                        let result = clean_with_trafilatura(&html, Some(&article.url))
                            .unwrap_or(regex_result);
                        if result.text.len() >= MIN_TEXT_LENGTH
                            && count_words(&result.text) >= MIN_ARTICLE_WORD_COUNT
                        {
                            info!(
                                %article_id,
                                text_len = result.text.len(),
                                word_count = count_words(&result.text),
                                "Hetzner extraction succeeded"
                            );
                            extraction = Some(result);
                        }
                    }
                    Err(e) => {
                        warn!(%article_id, error = %e, "Hetzner extraction failed");
                    }
                }
            }
        }
    }

    // 5. Validation
    let Some(extracted) = extraction else {
        error!(%article_id, "All extraction strategies failed");
        let _ =
            update_processing_status(pool, article_id, ProcessingStatus::ExtractionFailed).await;
        return ContentStepResult {
            article_id,
            status: ContentStepStatus::ExtractionFailed,
            content_length: None,
            duplicate_reason: None,
            error: Some("all_strategies_failed".to_string()),
            retryable: true,
        };
    };

    let text = extracted.text;
    let title_clean = extracted
        .title_clean
        .unwrap_or_else(|| article.title.clone());
    let canonical_url = extracted.canonical_url;

    if text.len() < MIN_TEXT_LENGTH || count_words(&text) < MIN_ARTICLE_WORD_COUNT {
        info!(
            %article_id,
            text_len = text.len(),
            word_count = count_words(&text),
            "Content too short, rejecting"
        );
        let _ = update_processing_status(pool, article_id, ProcessingStatus::Rejected).await;
        let _ = insert_from_article(
            pool,
            article_id,
            &article.source_id,
            &article.title,
            &article.url,
            "content_too_short",
            Some(&format!(
                "text_len={}, word_count={}",
                text.len(),
                count_words(&text)
            )),
        )
        .await;
        return ContentStepResult {
            article_id,
            status: ContentStepStatus::Rejected,
            content_length: Some(text.len()),
            duplicate_reason: None,
            error: None,
            retryable: false,
        };
    }

    // 6. Final dedup with title_clean (using pg_trgm index when available)
    let final_comparable = match find_similar_articles(pool, &title_clean, article_id, 20).await {
        Ok(c) => c,
        Err(e) => {
            warn!(%article_id, error = %e, "pg_trgm query failed, falling back to full scan");
            comparable.clone()
        }
    };

    let final_dedup_input: Vec<(String, String, Option<String>, String)> = final_comparable
        .iter()
        .map(|c| {
            (
                c.id.to_string(),
                c.url.clone(),
                c.canonical_url.clone(),
                c.title.clone(),
            )
        })
        .collect();

    let title_dup = check_duplicate(
        &article.url,
        canonical_url.as_deref(),
        &title_clean,
        &final_dedup_input,
    );
    if title_dup.is_duplicate {
        let dup_id = title_dup
            .duplicate_of
            .and_then(|s| Uuid::parse_str(&s).ok());
        if let Some(dup_id) = dup_id {
            let _ = enrich_from_duplicate(pool, article_id, dup_id).await;
            let _ = insert_from_article(
                pool,
                article_id,
                &article.source_id,
                &article.title,
                &article.url,
                "duplicate",
                Some(&format!("title_dup: {}", title_dup.reason)),
            )
            .await;
        }
        info!(%article_id, reason = %title_dup.reason, "Rejected as title duplicate");
        return ContentStepResult {
            article_id,
            status: ContentStepStatus::RejectedDuplicate,
            content_length: Some(text.len()),
            duplicate_reason: Some(title_dup.reason),
            error: None,
            retryable: false,
        };
    }

    // 7. Update DB
    if let Err(e) = update_extraction(
        pool,
        article_id,
        Some(&text),
        Some(text.len() as i32),
        Some(&title_clean),
        canonical_url.as_deref(),
    )
    .await
    {
        error!(%article_id, error = %e, "Failed to update extraction");
        return ContentStepResult {
            article_id,
            status: ContentStepStatus::ExtractionFailed,
            content_length: None,
            duplicate_reason: None,
            error: Some(format!("DB error: {e}")),
            retryable: true,
        };
    }

    if let Err(e) = update_duplicate_and_processing(
        pool,
        article_id,
        DuplicateStatus::Distinct,
        ProcessingStatus::PendingQualification,
    )
    .await
    {
        error!(%article_id, error = %e, "Failed to update processing status");
        return ContentStepResult {
            article_id,
            status: ContentStepStatus::ExtractionFailed,
            content_length: None,
            duplicate_reason: None,
            error: Some(format!("DB error: {e}")),
            retryable: true,
        };
    }

    info!(%article_id, text_len = text.len(), "Content step completed");
    ContentStepResult {
        article_id,
        status: ContentStepStatus::PendingQualification,
        content_length: Some(text.len()),
        duplicate_reason: None,
        error: None,
        retryable: false,
    }
}

async fn fetch_with_strategy(client: &Client, url: &str, strategy: &str) -> Result<String> {
    let mut request = match strategy {
        "amp" => client.get(format!("{url}?amp=1")),
        _ => client.get(url),
    };

    match strategy {
        "default" => {
            request = request.header("User-Agent", "Mozilla/5.0 (compatible; Googlebot/2.1)");
        }
        "google-referrer" => {
            request = request.header("User-Agent", "Mozilla/5.0");
            request = request.header("Referer", "https://www.google.com/");
        }
        "twitter-referrer" => {
            request = request.header("User-Agent", "Mozilla/5.0");
            request = request.header("Referer", "https://t.co/");
        }
        "facebook-referrer" => {
            request = request.header("User-Agent", "Mozilla/5.0");
            request = request.header("Referer", "https://www.facebook.com/");
        }
        _ => {}
    }

    let response = request.send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("HTTP {}", response.status()));
    }
    Ok(response.text().await?)
}

async fn fetch_hetzner(
    client: &Client,
    hetzner_url: &str,
    secret: &str,
    article_url: &str,
) -> Result<String> {
    let body = serde_json::json!({ "url": article_url });

    let response = client
        .post(hetzner_url)
        .header("Authorization", format!("Bearer {secret}"))
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Hetzner HTTP {}", response.status()));
    }

    let json: serde_json::Value = response.json().await?;
    let html = json["content"]
        .as_str()
        .or_else(|| json["html"].as_str())
        .ok_or_else(|| anyhow::anyhow!("No content in Hetzner response"))?;

    Ok(html.to_string())
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_content_step_status_variants() {
        assert_eq!(
            ContentStepStatus::PendingQualification,
            ContentStepStatus::PendingQualification
        );
        assert_ne!(ContentStepStatus::Extracted, ContentStepStatus::Rejected);
    }

    #[test]
    fn test_strategies_count() {
        assert_eq!(STRATEGIES.len(), 5);
    }

    #[test]
    fn test_constants() {
        assert_eq!(CONTENT_FETCH_TIMEOUT_MS, 15_000);
        assert_eq!(MIN_TEXT_LENGTH, 300);
    }
}
