use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::schema::{DuplicateStatus, ProcessingStatus, QualityStatus, RawArticle};

pub async fn get_by_id(pool: &PgPool, id: Uuid) -> Result<Option<RawArticle>> {
    sqlx::query_as::<_, RawArticle>("SELECT * FROM raw_articles WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .context("get_by_id: failed to query raw_articles")
}

pub async fn get_by_url(pool: &PgPool, url: &str, source_id: &str) -> Result<Option<RawArticle>> {
    sqlx::query_as::<_, RawArticle>("SELECT * FROM raw_articles WHERE url = $1 AND source_id = $2")
        .bind(url)
        .bind(source_id)
        .fetch_optional(pool)
        .await
        .context("get_by_url: failed to query raw_articles")
}

pub async fn insert(pool: &PgPool, article: &RawArticle) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO raw_articles (
            id, source_id, title, url, description, image_url, author,
            pub_date, content, content_length, content_hash, title_clean,
            canonical_url, processing_status, quality_status, duplicate_status,
            duplicate_of, preferred_extraction_method, extraction_attempts,
            last_extraction_error, last_extraction_at, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12,
            $13, $14, $15, $16,
            $17, $18, $19,
            $20, $21, $22, $23
        )"#,
    )
    .bind(article.id)
    .bind(&article.source_id)
    .bind(&article.title)
    .bind(&article.url)
    .bind(&article.description)
    .bind(&article.image_url)
    .bind(&article.author)
    .bind(article.pub_date)
    .bind(&article.content)
    .bind(article.content_length)
    .bind(&article.content_hash)
    .bind(&article.title_clean)
    .bind(&article.canonical_url)
    .bind(&article.processing_status)
    .bind(&article.quality_status)
    .bind(&article.duplicate_status)
    .bind(article.duplicate_of)
    .bind(&article.preferred_extraction_method)
    .bind(article.extraction_attempts)
    .bind(&article.last_extraction_error)
    .bind(article.last_extraction_at)
    .bind(article.created_at)
    .bind(article.updated_at)
    .execute(pool)
    .await
    .context("insert: failed to insert into raw_articles")?;

    Ok(())
}

pub async fn update_processing_status(
    pool: &PgPool,
    article_id: Uuid,
    status: ProcessingStatus,
) -> Result<()> {
    sqlx::query("UPDATE raw_articles SET processing_status = $2, updated_at = now() WHERE id = $1")
        .bind(article_id)
        .bind(status)
        .execute(pool)
        .await
        .context("update_processing_status: failed to update raw_articles")?;

    Ok(())
}

pub async fn update_quality_status(
    pool: &PgPool,
    article_id: Uuid,
    status: QualityStatus,
) -> Result<()> {
    sqlx::query("UPDATE raw_articles SET quality_status = $2, updated_at = now() WHERE id = $1")
        .bind(article_id)
        .bind(status)
        .execute(pool)
        .await
        .context("update_quality_status: failed to update raw_articles")?;

    Ok(())
}

pub async fn update_duplicate_and_processing(
    pool: &PgPool,
    article_id: Uuid,
    dup_status: DuplicateStatus,
    proc_status: ProcessingStatus,
) -> Result<()> {
    sqlx::query(
        "UPDATE raw_articles SET duplicate_status = $2, processing_status = $3, updated_at = now() WHERE id = $1",
    )
    .bind(article_id)
    .bind(dup_status)
    .bind(proc_status)
    .execute(pool)
    .await
    .context("update_duplicate_and_processing: failed to update raw_articles")?;

    Ok(())
}

pub async fn update_extraction(
    pool: &PgPool,
    article_id: Uuid,
    content: Option<&str>,
    content_length: Option<i32>,
    title_clean: Option<&str>,
    canonical_url: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE raw_articles SET
            content = $2,
            content_length = $3,
            title_clean = $4,
            canonical_url = $5,
            extraction_attempts = extraction_attempts + 1,
            last_extraction_at = now(),
            updated_at = now()
        WHERE id = $1"#,
    )
    .bind(article_id)
    .bind(content)
    .bind(content_length)
    .bind(title_clean)
    .bind(canonical_url)
    .execute(pool)
    .await
    .context("update_extraction: failed to update raw_articles")?;

    Ok(())
}

pub async fn get_recent_ingested_by_feed(
    pool: &PgPool,
    feed_id: &str,
    limit: i64,
) -> Result<Vec<RawArticle>> {
    sqlx::query_as::<_, RawArticle>(
        r#"SELECT * FROM raw_articles
        WHERE source_id = $1 AND processing_status = 'ingested'
        ORDER BY pub_date DESC NULLS LAST, created_at DESC
        LIMIT $2"#,
    )
    .bind(feed_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("get_recent_ingested_by_feed: failed to query raw_articles")
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ComparableArticle {
    pub id: Uuid,
    pub url: String,
    pub canonical_url: Option<String>,
    pub title: String,
    pub title_clean: Option<String>,
}

pub async fn get_comparable_articles(
    pool: &PgPool,
    exclude_id: Uuid,
    limit: i64,
) -> Result<Vec<ComparableArticle>> {
    sqlx::query_as::<_, ComparableArticle>(
        r#"SELECT id, url, canonical_url, title, title_clean
        FROM raw_articles
        WHERE id != $1
            AND processing_status IN ('ingested', 'extracted', 'pending_qualification', 'qualified')
        ORDER BY created_at DESC
        LIMIT $2"#,
    )
    .bind(exclude_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("get_comparable_articles: failed to query raw_articles")
}

pub async fn cleanup_old_duplicates(pool: &PgPool, days: i32) -> Result<i64> {
    let result = sqlx::query_scalar::<_, i64>(
        r#"WITH deleted AS (
            DELETE FROM raw_articles
            WHERE duplicate_status = 'duplicate' AND created_at < now() - make_interval(days => $1)
            RETURNING 1
        )
        SELECT count(*) FROM deleted"#,
    )
    .bind(days)
    .fetch_one(pool)
    .await
    .context("cleanup_old_duplicates: failed to clean raw_articles")?;

    Ok(result)
}

pub async fn enrich_from_duplicate(
    pool: &PgPool,
    article_id: Uuid,
    duplicate_of_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE raw_articles SET
            duplicate_of = $2,
            duplicate_status = 'duplicate',
            processing_status = 'qualified',
            updated_at = now()
        WHERE id = $1"#,
    )
    .bind(article_id)
    .bind(duplicate_of_id)
    .execute(pool)
    .await
    .context("enrich_from_duplicate: failed to update raw_articles")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::env;
    use std::sync::LazyLock;
    use tokio::sync::Mutex;

    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    async fn setup_test_pool() -> Option<PgPool> {
        let url = env::var("DATABASE_URL").ok()?;
        let pool = PgPool::connect(&url).await.ok()?;

        let _ = sqlx::query(
            "CREATE TABLE IF NOT EXISTS feed_sources (
                id                      TEXT PRIMARY KEY,
                feed_url                TEXT NOT NULL UNIQUE,
                name                    TEXT NOT NULL,
                category                TEXT,
                description             TEXT,
                logo                    TEXT,
                priority                INTEGER NOT NULL DEFAULT 0,
                tier                    TEXT NOT NULL DEFAULT 'free',
                fetch_status            TEXT NOT NULL DEFAULT 'pending',
                last_fetch_error        TEXT,
                last_fetch_at           TIMESTAMPTZ,
                last_ingested_pub_date  TIMESTAMPTZ,
                enabled                 BOOLEAN NOT NULL DEFAULT true,
                created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            "CREATE TABLE IF NOT EXISTS raw_articles (
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
                processing_status           TEXT NOT NULL DEFAULT 'ingested',
                quality_status              TEXT NOT NULL DEFAULT 'pending',
                duplicate_status            TEXT NOT NULL DEFAULT 'pending',
                duplicate_of                UUID,
                preferred_extraction_method TEXT,
                extraction_attempts         INTEGER NOT NULL DEFAULT 0,
                last_extraction_error       TEXT,
                last_extraction_at          TIMESTAMPTZ,
                created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await;

        // Clean raw_articles (feed_sources kept for FK)
        sqlx::query("DELETE FROM raw_articles")
            .execute(&pool)
            .await
            .unwrap();

        Some(pool)
    }

    async fn ensure_feed_source(pool: &PgPool, id: &str) {
        sqlx::query(
            r#"INSERT INTO feed_sources (id, feed_url, name, priority)
               VALUES ($1, 'https://example.com/' || $1, $1, 0)
               ON CONFLICT (id) DO NOTHING"#,
        )
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    }

    fn test_article(id: Uuid, source_id: &str, title: &str, url: &str) -> RawArticle {
        let now = Utc::now();
        RawArticle {
            id,
            source_id: source_id.to_string(),
            title: title.to_string(),
            url: url.to_string(),
            description: None,
            image_url: None,
            author: None,
            pub_date: Some(now),
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
        }
    }

    #[tokio::test]
    async fn test_insert_and_get_by_id() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };
        ensure_feed_source(&pool, "src-insert").await;

        let id = Uuid::new_v4();
        let article = test_article(id, "src-insert", "Test Article", "https://example.com/1");

        insert(&pool, &article)
            .await
            .expect("insert should succeed");

        let fetched = get_by_id(&pool, id)
            .await
            .expect("get_by_id should succeed");
        assert!(fetched.is_some());
        let a = fetched.unwrap();
        assert_eq!(a.id, id);
        assert_eq!(a.title, "Test Article");
        assert_eq!(a.url, "https://example.com/1");
        assert_eq!(a.source_id, "src-insert");
        assert_eq!(a.processing_status, ProcessingStatus::Ingested);
    }

    #[tokio::test]
    async fn test_get_by_url() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };
        ensure_feed_source(&pool, "src-url").await;

        let id = Uuid::new_v4();
        let article = test_article(id, "src-url", "URL Test", "https://example.com/unique-url");

        insert(&pool, &article)
            .await
            .expect("insert should succeed");

        let found = get_by_url(&pool, "https://example.com/unique-url", "src-url")
            .await
            .expect("get_by_url should succeed");
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, id);

        // Different source, same url
        let missing = get_by_url(&pool, "https://example.com/unique-url", "other-source")
            .await
            .expect("get_by_url should succeed");
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_update_processing_status() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };
        ensure_feed_source(&pool, "src-proc").await;

        let id = Uuid::new_v4();
        let article = test_article(
            id,
            "src-proc",
            "Processing Test",
            "https://example.com/processing",
        );

        insert(&pool, &article).await.unwrap();

        update_processing_status(&pool, id, ProcessingStatus::Extracted)
            .await
            .expect("update_processing_status should succeed");

        let updated = get_by_id(&pool, id).await.unwrap().unwrap();
        assert_eq!(updated.processing_status, ProcessingStatus::Extracted);
    }

    #[tokio::test]
    async fn test_update_extraction() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };
        ensure_feed_source(&pool, "src-extract").await;

        let id = Uuid::new_v4();
        let article = test_article(
            id,
            "src-extract",
            "Extraction Test",
            "https://example.com/extract",
        );

        insert(&pool, &article).await.unwrap();

        update_extraction(
            &pool,
            id,
            Some("Article content here"),
            Some(20),
            Some("Clean Title"),
            Some("https://example.com/canonical"),
        )
        .await
        .expect("update_extraction should succeed");

        let updated = get_by_id(&pool, id).await.unwrap().unwrap();
        assert_eq!(updated.content.as_deref(), Some("Article content here"));
        assert_eq!(updated.content_length, Some(20));
        assert_eq!(updated.title_clean.as_deref(), Some("Clean Title"));
        assert_eq!(
            updated.canonical_url.as_deref(),
            Some("https://example.com/canonical")
        );
        assert_eq!(updated.extraction_attempts, 1);
        assert!(updated.last_extraction_at.is_some());
    }

    #[tokio::test]
    async fn test_get_comparable_articles() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };
        ensure_feed_source(&pool, "src-comp").await;

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        let a1 = test_article(id1, "src-comp", "Article One", "https://example.com/one");
        let mut a2 = test_article(id2, "src-comp", "Article Two", "https://example.com/two");
        a2.processing_status = ProcessingStatus::Extracted;
        let mut a3 = test_article(
            id3,
            "src-comp",
            "Article Three",
            "https://example.com/three",
        );
        a3.processing_status = ProcessingStatus::Qualified;

        insert(&pool, &a1).await.unwrap();
        insert(&pool, &a2).await.unwrap();
        insert(&pool, &a3).await.unwrap();

        // Exclude a1, should get a2 and a3
        let comparable = get_comparable_articles(&pool, id1, 10)
            .await
            .expect("get_comparable_articles should succeed");

        // a2 and a3 have compatible status; a1 is excluded
        assert_eq!(comparable.len(), 2);
        let ids: Vec<Uuid> = comparable.iter().map(|c| c.id).collect();
        assert!(ids.contains(&id2));
        assert!(ids.contains(&id3));
        assert!(!ids.contains(&id1));
    }

    #[tokio::test]
    async fn test_cleanup_old_duplicates() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };
        ensure_feed_source(&pool, "src-cleanup").await;

        let id = Uuid::new_v4();
        let mut article = test_article(
            id,
            "src-cleanup",
            "Duplicate Article",
            "https://example.com/dup",
        );
        article.duplicate_status = DuplicateStatus::Duplicate;
        // Force created_at far in the past
        let past = Utc::now() - chrono::Duration::days(90);
        article.created_at = past;
        article.updated_at = past;

        insert(&pool, &article).await.unwrap();

        let deleted = cleanup_old_duplicates(&pool, 30)
            .await
            .expect("cleanup_old_duplicates should succeed");

        assert_eq!(deleted, 1);

        let after = get_by_id(&pool, id).await.unwrap();
        assert!(after.is_none());
    }
}
