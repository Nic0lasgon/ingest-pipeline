use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::pipeline::content_step::{process_content_step, ContentStepStatus};

pub async fn handle_content_job(
    pool: &PgPool,
    payload: Value,
    config: Option<&Config>,
) -> Result<()> {
    // 1. Parse payload
    let article_id_str = payload
        .get("article_id")
        .and_then(|v| v.as_str())
        .context("Missing article_id in payload")?;
    let article_id = Uuid::parse_str(article_id_str).context("Invalid article_id UUID format")?;
    let run_id = payload
        .get("run_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok());

    info!(article_id = %article_id, run_id = ?run_id, "Starting content job");

    // 2. Delegate to content step
    let result = process_content_step(pool, article_id, config).await;

    // 3. Structured metrics log
    info!(
        article_id = %article_id,
        status = ?result.status,
        content_length = result.content_length,
        duplicate_reason = %result.duplicate_reason.as_deref().unwrap_or(""),
        error = %result.error.as_deref().unwrap_or(""),
        "Content step completed"
    );

    // 4. Branch on status
    match &result.status {
        ContentStepStatus::PendingQualification => {
            info!(
                article_id = %article_id,
                content_length = result.content_length,
                "Article qualified, creating editorial job"
            );
            // Placeholder: create editorial job when the pipeline stage is ready.
            // let job_payload = serde_json::json!({
            //     "article_id": article_id.to_string(),
            //     "run_id": run_id.map(|id| id.to_string()),
            // });
            // create_job(pool, "editorial_process", job_payload, 0).await?;
        }
        ContentStepStatus::RejectedDuplicate => {
            info!(
                article_id = %article_id,
                reason = %result.duplicate_reason.as_deref().unwrap_or("unknown"),
                "Article rejected as duplicate"
            );
        }
        ContentStepStatus::Rejected => {
            info!(
                article_id = %article_id,
                "Article rejected (content_too_short)"
            );
        }
        ContentStepStatus::ExtractionFailed => {
            if result.retryable {
                let err_msg = result.error.as_deref().unwrap_or("unknown");
                error!(
                    article_id = %article_id,
                    error = %err_msg,
                    "Content extraction failed (retryable)"
                );
                return Err(anyhow::anyhow!("Extraction failed: {err_msg}"));
            } else {
                warn!(
                    article_id = %article_id,
                    error = %result.error.as_deref().unwrap_or("unknown"),
                    "Content extraction failed (non-retryable)"
                );
            }
        }
        ContentStepStatus::Extracted => {
            info!(article_id = %article_id, "Content extracted successfully (idempotent skip)");
        }
    }

    Ok(())
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
                updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            "CREATE TABLE IF NOT EXISTS rejected_articles (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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

        sqlx::query("DELETE FROM jobs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM raw_articles")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM rejected_articles")
            .execute(&pool)
            .await
            .unwrap();

        Some(pool)
    }

    #[tokio::test]
    async fn test_handle_content_job_invalid_payload() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        // Empty payload → missing article_id
        let result = handle_content_job(&pool, serde_json::json!({}), None).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing article_id"));

        // Non-string article_id
        let result = handle_content_job(&pool, serde_json::json!({"article_id": 123}), None).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing article_id"));

        // Invalid UUID format
        let result =
            handle_content_job(&pool, serde_json::json!({"article_id": "not-a-uuid"}), None).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid article_id UUID"));
    }

    #[tokio::test]
    async fn test_handle_content_job_article_not_found() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let fake_id = Uuid::new_v4();
        let payload = serde_json::json!({
            "article_id": fake_id.to_string(),
            "run_id": Uuid::new_v4().to_string(),
        });

        // Article doesn't exist → process_content_step returns ExtractionFailed(non-retryable)
        // Handler returns Ok(()) (no retry for non-retryable)
        let result = handle_content_job(&pool, payload, None).await;
        assert!(
            result.is_ok(),
            "Non-retryable missing article should return Ok"
        );
    }

    #[tokio::test]
    async fn test_handle_content_job_idempotent_skip() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let article_id = Uuid::new_v4();

        // Insert a feed source (FK dependency)
        sqlx::query(
            "INSERT INTO feed_sources (id, feed_url, name, tier)
             VALUES ('test-src', 'https://example.com/feed.xml', 'Test', 'free')
             ON CONFLICT (id) DO NOTHING",
        )
        .execute(&pool)
        .await
        .unwrap();

        // Insert article already in PendingQualification state
        sqlx::query(
            "INSERT INTO raw_articles (id, source_id, title, url, processing_status, quality_status, duplicate_status, content_length)
             VALUES ($1, 'test-src', 'Test Article', 'https://example.com/article', 'pending_qualification', 'pending', 'distinct', 500)",
        )
        .bind(article_id)
        .execute(&pool)
        .await
        .unwrap();

        let payload = serde_json::json!({
            "article_id": article_id.to_string(),
        });

        // Already processed → process_content_step returns Extracted (idempotent skip)
        let result = handle_content_job(&pool, payload, None).await;
        assert!(result.is_ok());
    }
}
