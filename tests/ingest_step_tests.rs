#![allow(clippy::await_holding_lock)]

use chrono::Datelike;
use httpmock::prelude::*;
use ingest_pipeline::pipeline::ingest_step::{process_ingest_step, IngestStepResult};
use sqlx::PgPool;
use std::env;
use std::sync::{LazyLock, Mutex};

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const TEST_FEED_XML: &str = include_str!("fixtures/rss/test_feed.xml");

async fn setup_test_db(pool: &PgPool) {
    let _ = sqlx::query("DROP TABLE IF EXISTS raw_articles")
        .execute(pool)
        .await;
    let _ = sqlx::query("DROP TABLE IF EXISTS feed_sources")
        .execute(pool)
        .await;

    let _ = sqlx::query(
        r#"CREATE TABLE feed_sources (
            id                      TEXT PRIMARY KEY,
            feed_url                TEXT NOT NULL UNIQUE,
            name                    TEXT NOT NULL,
            category                TEXT,
            description             TEXT,
            logo                    TEXT,
            priority                INTEGER NOT NULL DEFAULT 0,
            tier                    TEXT NOT NULL DEFAULT 'free',
            fetch_status            TEXT NOT NULL DEFAULT 'Pending',
            last_fetch_error        TEXT,
            last_fetch_at           TIMESTAMPTZ,
            last_ingested_pub_date  TIMESTAMPTZ,
            enabled                 BOOLEAN NOT NULL DEFAULT true,
            created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await;

    let _ = sqlx::query(
        r#"CREATE TABLE raw_articles (
            id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_id                   TEXT NOT NULL REFERENCES feed_sources(id),
            title                       TEXT NOT NULL,
            url                         TEXT NOT NULL,
            description                 TEXT,
            image_url                   TEXT,
            author                      TEXT,
            pub_date                    TIMESTAMPTZ,
            content                     TEXT,
            content_length              INTEGER,
            content_hash                TEXT,
            title_clean                 TEXT,
            canonical_url               TEXT,
            processing_status           TEXT NOT NULL DEFAULT 'Ingested',
            quality_status              TEXT NOT NULL DEFAULT 'Pending',
            duplicate_status            TEXT NOT NULL DEFAULT 'Pending',
            duplicate_of                UUID,
            preferred_extraction_method TEXT,
            extraction_attempts         INTEGER NOT NULL DEFAULT 0,
            last_extraction_error       TEXT,
            last_extraction_at          TIMESTAMPTZ,
            created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await;

    sqlx::query("DELETE FROM raw_articles")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM feed_sources")
        .execute(pool)
        .await
        .unwrap();
}

async fn connect_test_pool() -> Option<PgPool> {
    let url = env::var("DATABASE_URL").ok()?;
    PgPool::connect(&url).await.ok()
}

async fn insert_test_feed(pool: &PgPool, id: &str, feed_url: &str, enabled: bool) {
    sqlx::query(
        r#"INSERT INTO feed_sources (id, feed_url, name, enabled)
           VALUES ($1, $2, $3, $4)"#,
    )
    .bind(id)
    .bind(feed_url)
    .bind(format!("Test Feed {}", id))
    .bind(enabled)
    .execute(pool)
    .await
    .unwrap();
}

async fn set_cutoff_date(pool: &PgPool, feed_id: &str, date: &str) {
    sqlx::query(
        r#"UPDATE feed_sources
           SET last_ingested_pub_date = $2::timestamptz, updated_at = now()
           WHERE id = $1"#,
    )
    .bind(feed_id)
    .bind(date)
    .execute(pool)
    .await
    .unwrap();
}

fn result_has_error_containing(result: &IngestStepResult, substring: &str) -> bool {
    result
        .error
        .as_ref()
        .map(|e| e.contains(substring))
        .unwrap_or(false)
}

#[tokio::test]
async fn test_ingest_step_success() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let server = MockServer::start();
    let feed_url = server.url("/rss");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss");
        then.status(200)
            .header("content-type", "application/rss+xml")
            .body(TEST_FEED_XML);
    });

    insert_test_feed(&pool, "test-feed", &feed_url, true).await;

    let result = process_ingest_step(&pool, "test-feed", None).await;

    assert!(
        result.error.is_none(),
        "expected no error, got: {:?}",
        result.error
    );
    assert_eq!(result.total_items, 3);
    assert_eq!(result.inserted_count, 3);
    assert_eq!(result.duplicate_count, 0);
    assert_eq!(result.new_article_ids.len(), 3);
    assert!(result.max_pub_date.is_some());

    let max_date = result.max_pub_date.unwrap();
    assert_eq!(
        max_date.year(),
        2024,
        "expected year 2024, got {}",
        max_date.year()
    );
    assert_eq!(result.feed_id, "test-feed");
}

#[tokio::test]
async fn test_ingest_step_duplicate_skip() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let server = MockServer::start();
    let feed_url = server.url("/rss");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss");
        then.status(200)
            .header("content-type", "application/rss+xml")
            .body(TEST_FEED_XML);
    });

    insert_test_feed(&pool, "test-feed-dup", &feed_url, true).await;

    let result1 = process_ingest_step(&pool, "test-feed-dup", None).await;
    assert!(result1.error.is_none());
    assert_eq!(result1.inserted_count, 3);

    let result2 = process_ingest_step(&pool, "test-feed-dup", None).await;
    assert!(result2.error.is_none());
    assert_eq!(result2.inserted_count, 0);
    assert_eq!(result2.duplicate_count, 3);
    assert!(result2.new_article_ids.is_empty());
}

#[tokio::test]
async fn test_ingest_step_cutoff_date() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let server = MockServer::start();
    let feed_url = server.url("/rss");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss");
        then.status(200)
            .header("content-type", "application/rss+xml")
            .body(TEST_FEED_XML);
    });

    insert_test_feed(&pool, "test-feed-cutoff", &feed_url, true).await;

    set_cutoff_date(&pool, "test-feed-cutoff", "2024-02-01T00:00:00Z").await;

    let result = process_ingest_step(&pool, "test-feed-cutoff", None).await;

    assert!(
        result.error.is_none(),
        "expected no error, got: {:?}",
        result.error
    );
    assert_eq!(result.total_items, 3);
    assert_eq!(result.inserted_count, 2);
    assert_eq!(result.duplicate_count, 0);

    let row_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM raw_articles WHERE url = $1 AND source_id = $2")
            .bind("https://example.com/article-old")
            .bind("test-feed-cutoff")
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(
        row_count, 0,
        "older article should have been skipped by cutoff"
    );
}

#[tokio::test]
async fn test_ingest_step_network_error() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let server = MockServer::start();
    let feed_url = server.url("/rss");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss");
        then.status(200)
            .delay(std::time::Duration::from_secs(20))
            .body(TEST_FEED_XML);
    });

    insert_test_feed(&pool, "test-feed-timeout", &feed_url, true).await;

    let result = process_ingest_step(&pool, "test-feed-timeout", None).await;

    assert!(result.error.is_some(), "expected an error for timeout");
    assert!(
        result.retryable,
        "expected retryable = true for network error"
    );

    let status: String =
        sqlx::query_scalar("SELECT fetch_status::text FROM feed_sources WHERE id = $1")
            .bind("test-feed-timeout")
            .fetch_one(&pool)
            .await
            .unwrap();

    assert!(
        status.contains("failed") || status.contains("Failed"),
        "expected fetch_status to be failed, got: {}",
        status
    );
}

#[tokio::test]
async fn test_ingest_step_malformed_xml() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let server = MockServer::start();
    let feed_url = server.url("/rss");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss");
        then.status(200)
            .header("content-type", "application/xml")
            .body("not a valid rss or atom or json feed at all");
    });

    insert_test_feed(&pool, "test-feed-malformed", &feed_url, true).await;

    let result = process_ingest_step(&pool, "test-feed-malformed", None).await;

    assert!(result.error.is_some(), "expected parse error");
    assert!(
        !result.retryable,
        "expected retryable = false for parse error"
    );
    assert!(result_has_error_containing(&result, "Parse error"));
}

#[tokio::test]
async fn test_ingest_step_disabled_feed() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    insert_test_feed(
        &pool,
        "test-feed-disabled",
        "https://example.com/disabled",
        false,
    )
    .await;

    let result = process_ingest_step(&pool, "test-feed-disabled", None).await;

    assert!(result.error.is_some());
    assert!(!result.retryable);
    assert!(result_has_error_containing(&result, "disabled"));
    assert_eq!(result.inserted_count, 0);
}

#[tokio::test]
async fn test_ingest_step_nonexistent_feed() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let result = process_ingest_step(&pool, "nonexistent-feed", None).await;

    assert!(result.error.is_some());
    assert!(!result.retryable);
    assert!(result_has_error_containing(&result, "not found"));
    assert_eq!(result.inserted_count, 0);
}

#[tokio::test]
async fn test_ingest_step_http_404() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let server = MockServer::start();
    let feed_url = server.url("/rss");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss");
        then.status(404).body("not found");
    });

    insert_test_feed(&pool, "test-feed-404", &feed_url, true).await;

    let result = process_ingest_step(&pool, "test-feed-404", None).await;

    assert!(result.error.is_some());
    assert!(!result.retryable, "404 should not be retryable");
    assert!(result_has_error_containing(&result, "404"));
}

#[tokio::test]
async fn test_ingest_step_max_pub_date() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let server = MockServer::start();
    let feed_url = server.url("/rss");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss");
        then.status(200)
            .header("content-type", "application/rss+xml")
            .body(TEST_FEED_XML);
    });

    insert_test_feed(&pool, "test-feed-maxdate", &feed_url, true).await;

    let result = process_ingest_step(&pool, "test-feed-maxdate", None).await;

    assert!(result.error.is_none());
    assert!(result.max_pub_date.is_some());
    let max_date = result.max_pub_date.unwrap();
    assert_eq!(max_date.month(), 5);
    assert_eq!(max_date.day(), 23);
}
