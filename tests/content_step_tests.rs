#![allow(clippy::await_holding_lock)]

use httpmock::prelude::*;
use ingest_pipeline::pipeline::content_step::{process_content_step, ContentStepStatus};
use sqlx::PgPool;
use std::env;
use std::sync::{LazyLock, Mutex};

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn long_article_html() -> String {
    // Generate enough words to pass MIN_ARTICLE_WORD_COUNT (350)
    let words: Vec<String> = (0..400).map(|i| format!("word{i}")).collect();
    let text = words.join(" ");
    format!(
        r#"<html>
        <head>
            <link rel="canonical" href="https://example.com/canonical-article" />
            <meta property="og:title" content="Great Article Title" />
        </head>
        <body>
            <article>
                <p>{text}</p>
            </article>
        </body>
        </html>"#
    )
}

fn short_article_html() -> String {
    r#"<html>
        <head><title>Short</title></head>
        <body><article><p>Too short content.</p></article></body>
    </html>"#
        .to_string()
}

async fn setup_test_db(pool: &PgPool) {
    let _ = sqlx::query("DROP TABLE IF EXISTS rejected_articles")
        .execute(pool)
        .await;
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

    let _ = sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS rejected_articles (
            id          UUID PRIMARY KEY,
            article_id  UUID NOT NULL,
            source_id   TEXT NOT NULL,
            title       TEXT NOT NULL,
            url         TEXT NOT NULL,
            reason      TEXT NOT NULL,
            details     TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await;

    sqlx::query("DELETE FROM rejected_articles")
        .execute(pool)
        .await
        .unwrap();
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

async fn insert_test_feed(pool: &PgPool, id: &str) {
    sqlx::query(
        r#"INSERT INTO feed_sources (id, feed_url, name, enabled)
           VALUES ($1, $2, $3, true)"#,
    )
    .bind(id)
    .bind(format!("https://example.com/{id}/rss"))
    .bind(format!("Test Feed {id}"))
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_test_article(
    pool: &PgPool,
    id: uuid::Uuid,
    source_id: &str,
    title: &str,
    url: &str,
    status: &str,
) {
    sqlx::query(
        r#"INSERT INTO raw_articles (id, source_id, title, url, processing_status)
           VALUES ($1, $2, $3, $4, $5)"#,
    )
    .bind(id)
    .bind(source_id)
    .bind(title)
    .bind(url)
    .bind(status)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn test_content_step_success() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;
    insert_test_feed(&pool, "cs-success").await;

    let server = MockServer::start();
    let article_url = server.url("/article");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/article");
        then.status(200)
            .header("content-type", "text/html")
            .body(long_article_html());
    });

    let article_id = uuid::Uuid::new_v4();
    insert_test_article(
        &pool,
        article_id,
        "cs-success",
        "Test Article",
        &article_url,
        "Ingested",
    )
    .await;

    let result = process_content_step(&pool, article_id, None).await;

    assert_eq!(
        result.status,
        ContentStepStatus::PendingQualification,
        "expected PendingQualification, got {:?} error={:?}",
        result.status,
        result.error
    );
    assert!(result.content_length.is_some());
    assert!(result.content_length.unwrap() >= 300);
    assert!(result.error.is_none());
    assert!(!result.retryable);

    // Verify DB state
    let row: (String, Option<i32>) =
        sqlx::query_as("SELECT processing_status, content_length FROM raw_articles WHERE id = $1")
            .bind(article_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, "PendingQualification");
    assert!(row.1.is_some());
    assert!(row.1.unwrap() >= 300);
}

#[tokio::test]
async fn test_content_step_too_short() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;
    insert_test_feed(&pool, "cs-short").await;

    let server = MockServer::start();
    let article_url = server.url("/short");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/short");
        then.status(200)
            .header("content-type", "text/html")
            .body(short_article_html());
    });

    let article_id = uuid::Uuid::new_v4();
    insert_test_article(
        &pool,
        article_id,
        "cs-short",
        "Short Article",
        &article_url,
        "Ingested",
    )
    .await;

    let result = process_content_step(&pool, article_id, None).await;

    assert_eq!(
        result.status,
        ContentStepStatus::Rejected,
        "expected Rejected, got {:?}",
        result.status
    );
    assert!(!result.retryable);

    // Verify article was rejected in DB
    let row: String =
        sqlx::query_scalar("SELECT processing_status FROM raw_articles WHERE id = $1")
            .bind(article_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row, "Rejected");
}

#[tokio::test]
async fn test_content_step_duplicate_url() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;
    insert_test_feed(&pool, "cs-dup").await;

    let server = MockServer::start();
    let article_url = server.url("/dup-article");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/dup-article");
        then.status(200)
            .header("content-type", "text/html")
            .body(long_article_html());
    });

    // Insert an existing article with the same URL
    let existing_id = uuid::Uuid::new_v4();
    insert_test_article(
        &pool,
        existing_id,
        "cs-dup",
        "Existing Article",
        &article_url,
        "Qualified",
    )
    .await;

    // Insert the new article we want to process
    let new_id = uuid::Uuid::new_v4();
    insert_test_article(
        &pool,
        new_id,
        "cs-dup",
        "New Article Same URL",
        &article_url,
        "Ingested",
    )
    .await;

    let result = process_content_step(&pool, new_id, None).await;

    assert_eq!(
        result.status,
        ContentStepStatus::RejectedDuplicate,
        "expected RejectedDuplicate, got {:?}",
        result.status
    );
    assert!(result.duplicate_reason.is_some());
}

#[tokio::test]
async fn test_content_step_strategies() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;
    insert_test_feed(&pool, "cs-strat").await;

    let server = MockServer::start();
    let article_url = server.url("/strat-article");

    let _mock = server.mock(|when, then| {
        when.method(GET).path("/strat-article");
        then.status(200)
            .header("content-type", "text/html")
            .body(long_article_html());
    });

    let article_id = uuid::Uuid::new_v4();
    insert_test_article(
        &pool,
        article_id,
        "cs-strat",
        "Strategy Test",
        &article_url,
        "Ingested",
    )
    .await;

    let result = process_content_step(&pool, article_id, None).await;

    assert_eq!(
        result.status,
        ContentStepStatus::PendingQualification,
        "expected PendingQualification, got {:?} error={:?}",
        result.status,
        result.error
    );
}

#[tokio::test]
async fn test_content_step_network_error() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;
    insert_test_feed(&pool, "cs-err").await;

    // Use a URL that will time out (no server running)
    let article_url = "http://192.0.2.1:1/no-server-here";

    let article_id = uuid::Uuid::new_v4();
    insert_test_article(
        &pool,
        article_id,
        "cs-err",
        "Network Error Article",
        article_url,
        "Ingested",
    )
    .await;

    let result = process_content_step(&pool, article_id, None).await;

    assert_eq!(
        result.status,
        ContentStepStatus::ExtractionFailed,
        "expected ExtractionFailed, got {:?}",
        result.status
    );
    assert!(
        result.retryable,
        "expected retryable=true for network error"
    );
}

#[tokio::test]
async fn test_content_step_already_processed() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;
    insert_test_feed(&pool, "cs-done").await;

    let article_id = uuid::Uuid::new_v4();
    insert_test_article(
        &pool,
        article_id,
        "cs-done",
        "Already Qualified Article",
        "https://example.com/already-qualified",
        "Qualified",
    )
    .await;

    let result = process_content_step(&pool, article_id, None).await;

    // Should be skipped (returns Extracted status as a "no-op")
    assert_eq!(
        result.status,
        ContentStepStatus::Extracted,
        "expected Extracted (skipped), got {:?}",
        result.status
    );
    assert!(result.error.is_none());
    assert!(!result.retryable);
}

#[tokio::test]
async fn test_content_step_not_found() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => return,
    };
    setup_test_db(&pool).await;

    let article_id = uuid::Uuid::new_v4();
    let result = process_content_step(&pool, article_id, None).await;

    assert_eq!(result.status, ContentStepStatus::ExtractionFailed);
    assert_eq!(result.error.as_deref(), Some("not_found"));
    assert!(!result.retryable);
}
