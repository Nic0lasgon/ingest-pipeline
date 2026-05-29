#![allow(clippy::await_holding_lock)]

use ingest_pipeline::db::article_queries;
use ingest_pipeline::pipeline::content_step::process_content_step;
use ingest_pipeline::pipeline::ingest_step::{process_ingest_step, IngestStepResult};
use sqlx::PgPool;
use std::sync::{LazyLock, Mutex};

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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

    sqlx::query(
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
    .await
    .unwrap();

    sqlx::query(
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
            duplicate_of                UUID REFERENCES raw_articles(id),
            preferred_extraction_method TEXT,
            extraction_attempts         INTEGER NOT NULL DEFAULT 0,
            last_extraction_error       TEXT,
            last_extraction_at          TIMESTAMPTZ,
            created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"CREATE TABLE rejected_articles (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            article_id  UUID NOT NULL REFERENCES raw_articles(id),
            source_id   TEXT NOT NULL,
            title       TEXT NOT NULL,
            url         TEXT NOT NULL,
            reason      TEXT NOT NULL,
            details     TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn connect_test_pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPool::connect(&url).await.ok()
}

async fn insert_test_feed(pool: &PgPool, id: &str, feed_url: &str, name: &str) {
    sqlx::query(
        r#"INSERT INTO feed_sources (id, feed_url, name, enabled)
           VALUES ($1, $2, $3, true)
           ON CONFLICT (id) DO UPDATE SET feed_url = EXCLUDED.feed_url"#,
    )
    .bind(id)
    .bind(feed_url)
    .bind(name)
    .execute(pool)
    .await
    .unwrap();
}

/// Helper: run ingest, assert success, return result
async fn run_ingest_assert_ok(pool: &PgPool, feed_id: &str) -> IngestStepResult {
    let result = process_ingest_step(pool, feed_id, None).await;
    assert!(
        result.error.is_none(),
        "Ingest failed for {}: {:?}",
        feed_id,
        result.error
    );
    result
}

// ---------------------------------------------------------------------------
// Test: Le Monde RSS
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "requires internet + PostgreSQL"]
async fn test_lemonde_rss_integration() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set");
            return;
        }
    };
    setup_test_db(&pool).await;

    insert_test_feed(
        &pool,
        "lemonde",
        "https://www.lemonde.fr/rss/une.xml",
        "Le Monde",
    )
    .await;

    // First run
    let result = run_ingest_assert_ok(&pool, "lemonde").await;

    assert!(
        result.inserted_count > 0,
        "Le Monde should insert articles, got {}",
        result.inserted_count
    );
    assert!(result.total_items > 0, "Le Monde feed should have items");
    assert!(
        !result.new_article_ids.is_empty(),
        "Should have new article IDs"
    );

    // Verify articles have titles and valid URLs
    for &article_id in &result.new_article_ids {
        let article = article_queries::get_by_id(&pool, article_id)
            .await
            .expect("DB query should succeed")
            .expect("article should exist");

        assert!(
            !article.title.is_empty(),
            "Article {} should have a non-empty title",
            article_id
        );
        assert!(
            article.url.starts_with("http"),
            "Article {} URL should start with http, got: {}",
            article_id,
            article.url
        );
    }

    // Idempotence: re-run immediately should insert 0 new articles
    let result2 = run_ingest_assert_ok(&pool, "lemonde").await;
    assert_eq!(
        result2.inserted_count, 0,
        "Second run should insert 0 articles (idempotence)"
    );
    assert!(
        result2.duplicate_count > 0 || result2.total_items == 0,
        "Second run should detect duplicates"
    );
}

// ---------------------------------------------------------------------------
// Test: BBC News RSS
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "requires internet + PostgreSQL"]
async fn test_bbc_rss_integration() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set");
            return;
        }
    };
    setup_test_db(&pool).await;

    insert_test_feed(
        &pool,
        "bbc",
        "http://feeds.bbci.co.uk/news/rss.xml",
        "BBC News",
    )
    .await;

    let result = run_ingest_assert_ok(&pool, "bbc").await;

    assert!(
        result.inserted_count > 0,
        "BBC should insert articles, got {}",
        result.inserted_count
    );
    assert!(result.total_items > 0, "BBC feed should have items");

    // Verify articles
    for &article_id in &result.new_article_ids {
        let article = article_queries::get_by_id(&pool, article_id)
            .await
            .expect("DB query should succeed")
            .expect("article should exist");

        assert!(
            !article.title.is_empty(),
            "Article {} should have a non-empty title",
            article_id
        );
        assert!(
            article.url.starts_with("http"),
            "Article {} URL should start with http, got: {}",
            article_id,
            article.url
        );
    }

    // Idempotence
    let result2 = run_ingest_assert_ok(&pool, "bbc").await;
    assert_eq!(result2.inserted_count, 0);
}

// ---------------------------------------------------------------------------
// Test: Atom feed (GitHub Blog)
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "requires internet + PostgreSQL"]
async fn test_atom_feed_integration() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set");
            return;
        }
    };
    setup_test_db(&pool).await;

    insert_test_feed(
        &pool,
        "github_blog",
        "https://github.blog/feed/",
        "GitHub Blog",
    )
    .await;

    let result = run_ingest_assert_ok(&pool, "github_blog").await;

    assert!(
        result.inserted_count > 0,
        "GitHub Blog should insert articles, got {}",
        result.inserted_count
    );
    assert!(result.total_items > 0, "GitHub Blog feed should have items");

    for &article_id in &result.new_article_ids {
        let article = article_queries::get_by_id(&pool, article_id)
            .await
            .expect("DB query should succeed")
            .expect("article should exist");

        assert!(
            !article.title.is_empty(),
            "Article {} should have a non-empty title",
            article_id
        );
        assert!(
            article.url.starts_with("http"),
            "Article {} URL should start with http, got: {}",
            article_id,
            article.url
        );
    }

    // Idempotence
    let result2 = run_ingest_assert_ok(&pool, "github_blog").await;
    assert_eq!(result2.inserted_count, 0);
}

// ---------------------------------------------------------------------------
// Test end-to-end: ingest → content step
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "requires internet + PostgreSQL"]
async fn test_end_to_end_pipeline() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set");
            return;
        }
    };
    setup_test_db(&pool).await;

    insert_test_feed(
        &pool,
        "e2e_test",
        "https://www.lemonde.fr/rss/une.xml",
        "E2E Test Feed",
    )
    .await;

    // Step 1: ingest
    let ingest_result = run_ingest_assert_ok(&pool, "e2e_test").await;
    assert!(
        ingest_result.inserted_count > 0,
        "E2E ingest should insert articles"
    );

    // Step 2: take the first article and run content step
    let first_article_id = ingest_result.new_article_ids[0];
    let content_result = process_content_step(&pool, first_article_id, None).await;

    // The content step should either:
    // - Succeed with PendingQualification (extraction worked)
    // - Fail with ExtractionFailed (network/JS issues, expected for real sites)
    // - Return RejectedDuplicate (unlikely on fresh DB)
    match &content_result.status {
        ingest_pipeline::pipeline::content_step::ContentStepStatus::PendingQualification => {
            // Best case: extraction succeeded
            assert!(
                content_result.content_length.is_some(),
                "PendingQualification should have content_length"
            );
            assert!(
                content_result.error.is_none(),
                "PendingQualification should have no error"
            );

            // Verify article was updated in DB
            let article = article_queries::get_by_id(&pool, first_article_id)
                .await
                .expect("DB query should succeed")
                .expect("article should exist");

            assert!(
                article.content.is_some(),
                "Article should have content after extraction"
            );
            assert!(
                article.title_clean.is_some(),
                "Article should have title_clean after extraction"
            );
            assert!(
                article.content_length.is_some(),
                "Article should have content_length after extraction"
            );
        }
        ingest_pipeline::pipeline::content_step::ContentStepStatus::Rejected => {
            // Content too short or other rejection — acceptable for some articles
            eprintln!(
                "Article {} was rejected (content too short?), this is acceptable",
                first_article_id
            );
        }
        ingest_pipeline::pipeline::content_step::ContentStepStatus::RejectedDuplicate => {
            // Duplicate — unexpected on fresh DB, log it
            eprintln!(
                "Article {} was rejected as duplicate: {:?}",
                first_article_id, content_result.duplicate_reason
            );
        }
        ingest_pipeline::pipeline::content_step::ContentStepStatus::ExtractionFailed => {
            // All strategies failed — acceptable for real sites with anti-bot
            eprintln!(
                "Article {} extraction failed (likely anti-bot protection): {:?}",
                first_article_id, content_result.error
            );
            assert!(
                content_result.retryable,
                "ExtractionFailed should be retryable"
            );
        }
        ingest_pipeline::pipeline::content_step::ContentStepStatus::Extracted => {
            // Shouldn't happen for a fresh article, but not a failure
            eprintln!(
                "Article {} returned Extracted (unexpected but OK)",
                first_article_id
            );
        }
    }

    // Step 3: verify idempotence — running content step again should skip
    let content_result2 = process_content_step(&pool, first_article_id, None).await;
    // Already processed, should return quickly without error
    assert!(
        content_result2.error.is_none(),
        "Second content step should not error: {:?}",
        content_result2.error
    );
}

// ---------------------------------------------------------------------------
// Test: multiple feeds in same run
// ---------------------------------------------------------------------------
#[tokio::test]
#[ignore = "requires internet + PostgreSQL"]
async fn test_multiple_feeds_integration() {
    let _guard = TEST_LOCK.lock().unwrap();
    let pool = match connect_test_pool().await {
        Some(p) => p,
        None => {
            eprintln!("Skipping: DATABASE_URL not set");
            return;
        }
    };
    setup_test_db(&pool).await;

    insert_test_feed(
        &pool,
        "multi_lemonde",
        "https://www.lemonde.fr/rss/une.xml",
        "Le Monde",
    )
    .await;

    insert_test_feed(
        &pool,
        "multi_bbc",
        "http://feeds.bbci.co.uk/news/rss.xml",
        "BBC News",
    )
    .await;

    let r1 = run_ingest_assert_ok(&pool, "multi_lemonde").await;
    let r2 = run_ingest_assert_ok(&pool, "multi_bbc").await;

    assert!(r1.inserted_count > 0, "Le Monde should insert articles");
    assert!(r2.inserted_count > 0, "BBC should insert articles");

    // Total articles in DB
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM raw_articles")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(
        total,
        (r1.inserted_count + r2.inserted_count) as i64,
        "Total DB articles should match sum of inserted"
    );
}
