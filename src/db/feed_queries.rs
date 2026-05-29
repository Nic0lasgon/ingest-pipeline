use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::db::schema::{FeedFetchStatus, FeedSource};

pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<FeedSource>> {
    sqlx::query_as::<_, FeedSource>("SELECT * FROM feed_sources WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .context("get_by_id: failed to query feed_sources")
}

pub async fn get_active_feeds(pool: &PgPool) -> Result<Vec<FeedSource>> {
    sqlx::query_as::<_, FeedSource>(
        "SELECT * FROM feed_sources WHERE enabled = true ORDER BY priority DESC, name ASC",
    )
    .fetch_all(pool)
    .await
    .context("get_active_feeds: failed to query feed_sources")
}

pub async fn get_active_tier1_feeds(pool: &PgPool) -> Result<Vec<FeedSource>> {
    sqlx::query_as::<_, FeedSource>(
        "SELECT * FROM feed_sources WHERE enabled = true AND tier = 'tier1_keep' ORDER BY priority DESC",
    )
    .fetch_all(pool)
    .await
    .context("get_active_tier1_feeds: failed to query feed_sources")
}

pub async fn update_fetch_status(
    pool: &PgPool,
    feed_id: &str,
    status: FeedFetchStatus,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE feed_sources SET fetch_status = $2, last_fetch_error = $3, last_fetch_at = now(), updated_at = now() WHERE id = $1"#,
    )
    .bind(feed_id)
    .bind(status)
    .bind(error)
    .execute(pool)
    .await
    .context("update_fetch_status: failed to update feed_sources")?;
    Ok(())
}

pub async fn update_last_ingested_pub_date(pool: &PgPool, feed_id: &str, date: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE feed_sources SET last_ingested_pub_date = $2::timestamptz, updated_at = now() WHERE id = $1"#,
    )
    .bind(feed_id)
    .bind(date)
    .execute(pool)
    .await
    .context("update_last_ingested_pub_date: failed to update feed_sources")?;
    Ok(())
}

pub async fn get_last_ingested_pub_date(pool: &PgPool, feed_id: &str) -> Result<Option<String>> {
    sqlx::query_scalar::<_, String>(
        "SELECT last_ingested_pub_date::text FROM feed_sources WHERE id = $1",
    )
    .bind(feed_id)
    .fetch_optional(pool)
    .await
    .context("get_last_ingested_pub_date: failed to query feed_sources")
}

pub async fn get_feed_preferred_method(_pool: &PgPool, _feed_id: &str) -> Result<Option<String>> {
    // TODO: preferred_extraction_method n'existe que sur raw_articles, pas sur feed_sources.
    // Si cette info est nécessaire au niveau feed, il faudra soit ajouter une colonne,
    // soit retourner la méthode la plus fréquente depuis raw_articles.
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::LazyLock;
    use tokio::sync::Mutex;

    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    async fn setup_test_pool() -> Option<PgPool> {
        let url = env::var("DATABASE_URL").ok()?;
        let pool = PgPool::connect(&url).await.ok()?;

        // Ensure feed_sources table exists
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

        // Clean slate
        sqlx::query("DELETE FROM feed_sources")
            .execute(&pool)
            .await
            .unwrap();

        Some(pool)
    }

    async fn insert_test_feed(
        pool: &PgPool,
        id: &str,
        name: &str,
        enabled: bool,
        tier: &str,
        priority: i32,
    ) {
        sqlx::query(
            r#"INSERT INTO feed_sources (id, feed_url, name, enabled, tier, priority)
               VALUES ($1, $2 || $1, $3, $4, $5, $6)
               ON CONFLICT (id) DO UPDATE SET feed_url = EXCLUDED.feed_url"#,
        )
        .bind(id)
        .bind("https://example.com/feed/")
        .bind(name)
        .bind(enabled)
        .bind(tier)
        .bind(priority)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_get_by_id() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        insert_test_feed(&pool, "feed-1", "Test Feed", true, "free", 5).await;

        let feed = get_by_id(&pool, "feed-1")
            .await
            .expect("get_by_id should succeed");

        assert!(feed.is_some());
        let feed = feed.unwrap();
        assert_eq!(feed.id, "feed-1");
        assert_eq!(feed.name, "Test Feed");
        assert!(feed.enabled);
        assert_eq!(feed.priority, 5);

        // Non-existent id
        let missing = get_by_id(&pool, "nonexistent")
            .await
            .expect("get_by_id should succeed");
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_get_active_feeds() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        insert_test_feed(&pool, "a1", "Alpha", true, "free", 10).await;
        insert_test_feed(&pool, "a2", "Beta", true, "free", 5).await;
        insert_test_feed(&pool, "a3", "Gamma", false, "free", 1).await;

        let feeds = get_active_feeds(&pool)
            .await
            .expect("get_active_feeds should succeed");

        assert_eq!(feeds.len(), 2);
        // Ordered by priority DESC, name ASC
        assert_eq!(feeds[0].id, "a1");
        assert_eq!(feeds[1].id, "a2");
    }

    #[tokio::test]
    async fn test_get_active_tier1_feeds() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        insert_test_feed(&pool, "t1", "Tier1 Feed", true, "tier1_keep", 10).await;
        insert_test_feed(&pool, "t2", "Free Feed", true, "free", 5).await;
        insert_test_feed(&pool, "t3", "Tier1 Disabled", false, "tier1_keep", 1).await;

        let feeds = get_active_tier1_feeds(&pool)
            .await
            .expect("get_active_tier1_feeds should succeed");

        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0].id, "t1");
    }

    #[tokio::test]
    async fn test_update_fetch_status() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        insert_test_feed(&pool, "fs-1", "Status Feed", true, "free", 0).await;

        update_fetch_status(&pool, "fs-1", FeedFetchStatus::Fetching, None)
            .await
            .expect("update_fetch_status should succeed");

        let feed = get_by_id(&pool, "fs-1").await.unwrap().unwrap();
        assert_eq!(feed.fetch_status, FeedFetchStatus::Fetching);
        assert!(feed.last_fetch_at.is_some());
        assert!(feed.last_fetch_error.is_none());

        // Update with error
        update_fetch_status(
            &pool,
            "fs-1",
            FeedFetchStatus::Failed,
            Some("connection timeout"),
        )
        .await
        .expect("update_fetch_status should succeed");

        let feed = get_by_id(&pool, "fs-1").await.unwrap().unwrap();
        assert_eq!(feed.fetch_status, FeedFetchStatus::Failed);
        assert_eq!(feed.last_fetch_error.as_deref(), Some("connection timeout"));
    }

    #[tokio::test]
    async fn test_update_last_ingested_pub_date() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        insert_test_feed(&pool, "ip-1", "Ingest Feed", true, "free", 0).await;

        let feed = get_by_id(&pool, "ip-1").await.unwrap().unwrap();
        assert!(feed.last_ingested_pub_date.is_none());

        update_last_ingested_pub_date(&pool, "ip-1", "2025-01-15T10:30:00Z")
            .await
            .expect("update_last_ingested_pub_date should succeed");

        let feed = get_by_id(&pool, "ip-1").await.unwrap().unwrap();
        assert!(feed.last_ingested_pub_date.is_some());

        let date_str = get_last_ingested_pub_date(&pool, "ip-1")
            .await
            .expect("get_last_ingested_pub_date should succeed");
        assert!(date_str.is_some());
        assert!(date_str.unwrap().contains("2025-01-15"));
    }

    #[tokio::test]
    async fn test_get_feed_preferred_method() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let result = get_feed_preferred_method(&pool, "any-id")
            .await
            .expect("get_feed_preferred_method should succeed");
        assert!(result.is_none());
    }
}
