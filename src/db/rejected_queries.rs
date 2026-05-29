use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::schema::RejectedArticle;

pub async fn insert_from_article(
    pool: &PgPool,
    article_id: Uuid,
    source_id: &str,
    title: &str,
    url: &str,
    reason: &str,
    details: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO rejected_articles (article_id, source_id, title, url, reason, details) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(article_id)
    .bind(source_id)
    .bind(title)
    .bind(url)
    .bind(reason)
    .bind(details)
    .execute(pool)
    .await
    .context("insert_from_article: failed to insert rejected_article")?;
    Ok(())
}

pub async fn get_by_article_id(pool: &PgPool, article_id: Uuid) -> Result<Option<RejectedArticle>> {
    sqlx::query_as::<_, RejectedArticle>("SELECT * FROM rejected_articles WHERE article_id = $1")
        .bind(article_id)
        .fetch_optional(pool)
        .await
        .context("get_by_article_id: failed to query rejected_articles")
}

pub async fn cleanup_old_rejected(pool: &PgPool, days: i32) -> Result<i64> {
    let result =
        sqlx::query("DELETE FROM rejected_articles WHERE created_at < now() - $1::interval")
            .bind(format!("{} days", days))
            .execute(pool)
            .await
            .context("cleanup_old_rejected: failed to delete old rejected_articles")?;
    Ok(result.rows_affected() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use std::env;
    use std::sync::LazyLock;
    use tokio::sync::Mutex;

    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    async fn setup_test_pool() -> Option<PgPool> {
        let url = env::var("DATABASE_URL").ok()?;
        let pool = PgPool::connect(&url).await.ok()?;

        let _ = sqlx::query(
            "CREATE TABLE IF NOT EXISTS rejected_articles (
                id          UUID PRIMARY KEY,
                article_id  UUID NOT NULL,
                source_id   TEXT NOT NULL,
                title       TEXT NOT NULL,
                url         TEXT NOT NULL,
                reason      TEXT NOT NULL,
                details     TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await;

        sqlx::query("DELETE FROM rejected_articles")
            .execute(&pool)
            .await
            .unwrap();

        Some(pool)
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let article_id = Uuid::new_v4();

        insert_from_article(
            &pool,
            article_id,
            "src-1",
            "Test Article",
            "https://example.com/test",
            "quality check failed",
            Some("audio quality too low"),
        )
        .await
        .expect("insert should succeed");

        let rejected = get_by_article_id(&pool, article_id)
            .await
            .expect("get should succeed")
            .expect("should find rejected article");

        assert_eq!(rejected.article_id, article_id);
        assert_eq!(rejected.source_id, "src-1");
        assert_eq!(rejected.title, "Test Article");
        assert_eq!(rejected.url, "https://example.com/test");
        assert_eq!(rejected.reason, "quality check failed");
        assert_eq!(rejected.details.as_deref(), Some("audio quality too low"));

        let missing = get_by_article_id(&pool, Uuid::new_v4())
            .await
            .expect("get should succeed");
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_cleanup_old() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let old_id = Uuid::new_v4();
        let recent_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO rejected_articles (id, article_id, source_id, title, url, reason, details, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(Uuid::new_v4())
        .bind(old_id)
        .bind("src-1")
        .bind("Old Article")
        .bind("https://example.com/old")
        .bind("too old")
        .bind(None::<&str>)
        .bind(Utc::now() - Duration::days(10))
        .execute(&pool)
        .await
        .unwrap();

        insert_from_article(
            &pool,
            recent_id,
            "src-2",
            "Recent Article",
            "https://example.com/recent",
            "other reason",
            None,
        )
        .await
        .expect("insert should succeed");

        let deleted = cleanup_old_rejected(&pool, 5)
            .await
            .expect("cleanup should succeed");

        assert_eq!(deleted, 1);

        assert!(get_by_article_id(&pool, old_id).await.unwrap().is_none());

        assert!(get_by_article_id(&pool, recent_id).await.unwrap().is_some());
    }
}
