use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::db::article_queries::{get_by_url, insert_batch};
use crate::db::feed_queries::{get_by_id, get_last_ingested_pub_date, update_fetch_status};
use crate::db::schema::{
    DuplicateStatus, FeedFetchStatus, ProcessingStatus, QualityStatus, RawArticle,
};
use crate::utils::rss_parser::parse_feed;
use crate::utils::url_resolver::resolve_source_url;

pub const MAX_RECENT_ITEMS: usize = 30;

#[derive(Debug, Clone)]
pub struct IngestStepResult {
    pub feed_id: String,
    pub feed_name: String,
    pub new_article_ids: Vec<Uuid>,
    pub duplicate_count: usize,
    pub inserted_count: usize,
    pub total_items: usize,
    pub max_pub_date: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub retryable: bool,
}

pub async fn process_ingest_step(
    pool: &PgPool,
    feed_id: &str,
    _run_id: Option<Uuid>,
    config: &Config,
) -> IngestStepResult {
    info!(%feed_id, "Starting ingest step");

    let feed = match get_by_id(pool, feed_id).await {
        Ok(Some(f)) if f.enabled => {
            info!(%feed_id, feed_name = %f.name, "Feed found and enabled");
            f
        }
        Ok(Some(_)) => {
            warn!(%feed_id, "Feed is disabled");
            return IngestStepResult {
                feed_id: feed_id.to_string(),
                feed_name: String::new(),
                new_article_ids: vec![],
                duplicate_count: 0,
                inserted_count: 0,
                total_items: 0,
                max_pub_date: None,
                error: Some("Feed is disabled".to_string()),
                retryable: false,
            };
        }
        Ok(None) => {
            warn!(%feed_id, "Feed not found");
            return IngestStepResult {
                feed_id: feed_id.to_string(),
                feed_name: String::new(),
                new_article_ids: vec![],
                duplicate_count: 0,
                inserted_count: 0,
                total_items: 0,
                max_pub_date: None,
                error: Some("Feed not found".to_string()),
                retryable: false,
            };
        }
        Err(e) => {
            error!(%feed_id, error = %e, "Failed to query feed");
            return IngestStepResult {
                feed_id: feed_id.to_string(),
                feed_name: String::new(),
                new_article_ids: vec![],
                duplicate_count: 0,
                inserted_count: 0,
                total_items: 0,
                max_pub_date: None,
                error: Some(format!("DB error: {e}")),
                retryable: true,
            };
        }
    };

    let feed_name = feed.name.clone();

    // 2. Fetch RSS
    let client = config.http_client.client.clone();

    info!(%feed_id, feed_url = %feed.feed_url, "Fetching RSS feed");

    let response = match client
        .get(&feed.feed_url)
        .header(
            "Accept",
            "application/rss+xml, application/atom+xml, application/xml, text/xml",
        )
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let err_msg = format!("Network error fetching feed: {e}");
            error!(%feed_id, "{err_msg}");
            let _ =
                update_fetch_status(pool, feed_id, FeedFetchStatus::Failed, Some(&err_msg)).await;
            return IngestStepResult {
                feed_id: feed_id.to_string(),
                feed_name,
                new_article_ids: vec![],
                duplicate_count: 0,
                inserted_count: 0,
                total_items: 0,
                max_pub_date: None,
                error: Some(err_msg),
                retryable: true,
            };
        }
    };

    let status = response.status();
    if !status.is_success() {
        let err_msg = format!("HTTP {} fetching feed", status);
        error!(%feed_id, "{err_msg}");
        let retryable = status.as_u16() != 404;
        let _ = update_fetch_status(pool, feed_id, FeedFetchStatus::Failed, Some(&err_msg)).await;
        return IngestStepResult {
            feed_id: feed_id.to_string(),
            feed_name,
            new_article_ids: vec![],
            duplicate_count: 0,
            inserted_count: 0,
            total_items: 0,
            max_pub_date: None,
            error: Some(err_msg),
            retryable,
        };
    }

    let body = match response.text().await {
        Ok(b) => b,
        Err(e) => {
            let err_msg = format!("Failed to read response body: {e}");
            error!(%feed_id, "{err_msg}");
            let _ =
                update_fetch_status(pool, feed_id, FeedFetchStatus::Failed, Some(&err_msg)).await;
            return IngestStepResult {
                feed_id: feed_id.to_string(),
                feed_name,
                new_article_ids: vec![],
                duplicate_count: 0,
                inserted_count: 0,
                total_items: 0,
                max_pub_date: None,
                error: Some(err_msg),
                retryable: true,
            };
        }
    };

    // 3. Parse
    let rss_feed = match parse_feed(&body, &feed.feed_url) {
        Ok(f) => f,
        Err(e) => {
            let err_msg = format!("Parse error: {e}");
            error!(%feed_id, "{err_msg}");
            let _ =
                update_fetch_status(pool, feed_id, FeedFetchStatus::Failed, Some(&err_msg)).await;
            return IngestStepResult {
                feed_id: feed_id.to_string(),
                feed_name,
                new_article_ids: vec![],
                duplicate_count: 0,
                inserted_count: 0,
                total_items: 0,
                max_pub_date: None,
                error: Some(err_msg),
                retryable: false,
            };
        }
    };

    // 4. Sort and limit
    let mut items = rss_feed.items;
    items.sort_by(|a, b| b.pub_date.cmp(&a.pub_date));
    let items: Vec<_> = items.into_iter().take(MAX_RECENT_ITEMS).collect();

    let total_items = items.len();
    let max_pub_date = items.iter().filter_map(|i| i.pub_date).max();

    info!(
        %feed_id,
        total_items,
        max_pub_date = ?max_pub_date.map(|d| d.to_rfc3339()),
        "Items to process after sorting and limiting"
    );

    let cutoff_date = match get_last_ingested_pub_date(pool, feed_id).await {
        Ok(Some(ref date_str)) => parse_cutoff_date(date_str),
        Ok(None) => {
            info!(%feed_id, "No cutoff date (first run)");
            None
        }
        Err(e) => {
            error!(%feed_id, error = %e, "Failed to get cutoff date, skipping cutoff check");
            None
        }
    };

    // 5. Process items
    let mut duplicate_count = 0;
    let mut articles_to_insert: Vec<RawArticle> = Vec::new();
    let now = Utc::now();

    for item in &items {
        let Some(ref link) = item.link else {
            continue;
        };

        // a. Resolve URL
        let resolved_url = resolve_source_url(&client, link)
            .await
            .unwrap_or_else(|| link.clone());

        // b. Check duplicate by URL
        match get_by_url(pool, &resolved_url, feed_id).await {
            Ok(Some(_)) => {
                duplicate_count += 1;
                continue;
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    %feed_id,
                    url = %resolved_url,
                    error = %e,
                    "Failed to check duplicate, proceeding"
                );
            }
        }

        // c. Cutoff check
        if let Some(cutoff) = cutoff_date {
            if let Some(item_date) = item.pub_date {
                if item_date < cutoff {
                    continue;
                }
            }
        }

        // d. Create article
        let article = RawArticle {
            id: Uuid::new_v4(),
            source_id: feed_id.to_string(),
            title: item.title.clone().unwrap_or_default(),
            url: resolved_url.clone(),
            description: item.description.clone(),
            image_url: item.image_url.clone(),
            author: item.author.clone(),
            pub_date: item.pub_date,
            content: None,
            content_length: None,
            content_hash: None,
            title_clean: None,
            canonical_url: None,
            processing_status: ProcessingStatus::Ingested,
            quality_status: QualityStatus::Pending,
            duplicate_status: DuplicateStatus::Pending,
            duplicate_of: None,
            preferred_extraction_method: None,
            extraction_attempts: 0,
            last_extraction_error: None,
            last_extraction_at: None,
            created_at: now,
            updated_at: now,
        };

        articles_to_insert.push(article);
    }

    // e. Batch insert
    let inserted_count = match insert_batch(pool, &articles_to_insert).await {
        Ok(count) => count,
        Err(e) => {
            error!(%feed_id, error = %e, "Failed to batch insert articles");
            0
        }
    };
    let new_article_ids: Vec<Uuid> = articles_to_insert.iter().map(|a| a.id).collect();

    // 6. Update fetch status
    let _ = update_fetch_status(pool, feed_id, FeedFetchStatus::Success, None).await;

    info!(
        %feed_id,
        inserted_count,
        duplicate_count,
        new_ids = new_article_ids.len(),
        "Ingest step completed"
    );

    IngestStepResult {
        feed_id: feed_id.to_string(),
        feed_name,
        new_article_ids,
        duplicate_count,
        inserted_count,
        total_items,
        max_pub_date,
        error: None,
        retryable: false,
    }
}

fn parse_cutoff_date(date_str: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
        return Some(dt.with_timezone(&Utc));
    }
    // PG timestamptz::text: "2024-02-01 00:00:00+00"
    // Strip timezone suffix for NaiveDateTime parsing
    let bytes = date_str.as_bytes();
    let date_part: &str = if bytes.len() > 19 && (bytes[19] == b'+' || bytes[19] == b'-') {
        &date_str[..19]
    } else {
        date_str
    };
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(date_part, "%Y-%m-%d %H:%M:%S") {
        return Some(naive.and_utc());
    }
    None
}
