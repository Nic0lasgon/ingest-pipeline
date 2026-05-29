use crate::config::Config;
use crate::db::feed_queries::update_last_ingested_pub_date;
use crate::pipeline::ingest_step::process_ingest_step;
use crate::queue::jobs::create_job;
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use tracing::{error, info};
use uuid::Uuid;

pub async fn handle_ingest_job(pool: &PgPool, payload: Value, config: &Config) -> Result<()> {
    let feed_id = payload
        .get("feed_id")
        .and_then(|v| v.as_str())
        .context("Missing feed_id in payload")?;
    let run_id = payload
        .get("run_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());

    info!(feed_id = %feed_id, "Starting ingest job");

    let result = process_ingest_step(pool, feed_id, run_id, config).await;

    if result.error.is_none() {
        info!(
            feed_id = %feed_id,
            new_articles = result.new_article_ids.len(),
            duplicates = result.duplicate_count,
            "Ingest step completed"
        );

        for chunk in result.new_article_ids.chunks(50) {
            for article_id in chunk {
                let job_payload = serde_json::json!({
                    "article_id": article_id.to_string(),
                    "run_id": run_id.map(|id| id.to_string()),
                });
                create_job(pool, "process_article", job_payload, 0)
                    .await
                    .context("Failed to create content job")?;
            }
        }

        if let Some(max_pub_date) = result.max_pub_date {
            update_last_ingested_pub_date(pool, feed_id, &max_pub_date.to_rfc3339())
                .await
                .context("Failed to update cursor")?;
        }

        info!(feed_id = %feed_id, "Cursor advanced to {:?}", result.max_pub_date);
    } else {
        error!(
            feed_id = %feed_id,
            error = %result.error.as_ref().unwrap(),
            retryable = result.retryable,
            "Ingest step failed"
        );

        if result.retryable {
            return Err(anyhow::anyhow!(
                "Ingest step failed (retryable): {}",
                result.error.unwrap()
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use sqlx::PgPool;
    use std::env;
    use std::sync::LazyLock;
    use tokio::sync::Mutex;

    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    async fn setup_test_pool() -> Option<PgPool> {
        let url = env::var("DATABASE_URL").ok()?;
        let pool = PgPool::connect(&url).await.ok()?;

        let _ = sqlx::query(
            "DO $$ BEGIN
                CREATE TYPE job_status AS ENUM ('pending', 'running', 'completed', 'failed', 'dead');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$",
        )
        .execute(&pool)
        .await;

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
                id                          UUID PRIMARY KEY,
                source_id                   TEXT NOT NULL,
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
                updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT unique_raw_articles_url_source UNIQUE (url, source_id)
            )",
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            "CREATE TABLE IF NOT EXISTS jobs (
                id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                job_type     TEXT NOT NULL,
                payload      JSONB NOT NULL,
                status       job_status NOT NULL DEFAULT 'pending',
                priority     INTEGER NOT NULL DEFAULT 0,
                attempts     INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                locked_at    TIMESTAMPTZ,
                locked_by    TEXT,
                last_error   TEXT,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await;

        sqlx::query("DELETE FROM jobs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM raw_articles")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM feed_sources")
            .execute(&pool)
            .await
            .unwrap();

        Some(pool)
    }

    #[tokio::test]
    async fn test_handle_ingest_job_invalid_payload() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let result = handle_ingest_job(&pool, serde_json::json!({}), &Config::for_tests()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Missing feed_id"));
    }

    #[tokio::test]
    async fn test_handle_ingest_job_missing_feed_id_field() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let result = handle_ingest_job(
            &pool,
            serde_json::json!({"other_field": "value"}),
            &Config::for_tests(),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_ingest_job_feed_not_found() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let result = handle_ingest_job(
            &pool,
            serde_json::json!({"feed_id": "nonexistent-feed"}),
            &Config::for_tests(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_ingest_job_disabled_feed() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        sqlx::query(
            "INSERT INTO feed_sources (id, feed_url, name, enabled, tier)
             VALUES ($1, $2, $3, false, 'free')",
        )
        .bind("disabled-feed")
        .bind("https://example.com/feed/disabled-feed")
        .bind("Disabled Feed")
        .execute(&pool)
        .await
        .unwrap();

        let result = handle_ingest_job(
            &pool,
            serde_json::json!({"feed_id": "disabled-feed"}),
            &Config::for_tests(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_ingest_job_with_run_id() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let run_id = Uuid::new_v4();

        let result = handle_ingest_job(
            &pool,
            serde_json::json!({
                "feed_id": "some-feed",
                "run_id": run_id.to_string()
            }),
            &Config::for_tests(),
        )
        .await;

        assert!(result.is_ok());
    }
}
