use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::schema::{
    PipelineRun, PipelineStepRun, RunStatus, RunTriggerType, StepName, StepStatus,
};

// ── Pipeline Runs ─────────────────────────────────────────────────────────────

pub async fn start_run(
    pool: &PgPool,
    trigger_type: RunTriggerType,
    feeds_count: Option<i32>,
) -> Result<PipelineRun> {
    sqlx::query_as::<_, PipelineRun>(
        r#"INSERT INTO pipeline_runs (status, trigger_type, feeds_count)
           VALUES ($1, $2, $3)
           RETURNING *"#,
    )
    .bind(RunStatus::Running)
    .bind(trigger_type)
    .bind(feeds_count)
    .fetch_one(pool)
    .await
    .context("start_run: failed to insert pipeline_run")
}

pub async fn fail_run(pool: &PgPool, run_id: Uuid, error: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE pipeline_runs
           SET status = $2, error_message = $3, completed_at = now(), updated_at = now()
           WHERE id = $1"#,
    )
    .bind(run_id)
    .bind(RunStatus::Failed)
    .bind(error)
    .execute(pool)
    .await
    .context("fail_run: failed to update pipeline_run")?;
    Ok(())
}

pub async fn complete_run(pool: &PgPool, run_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"UPDATE pipeline_runs
           SET status = $2, completed_at = now(), updated_at = now()
           WHERE id = $1"#,
    )
    .bind(run_id)
    .bind(RunStatus::Completed)
    .execute(pool)
    .await
    .context("complete_run: failed to update pipeline_run")?;
    Ok(())
}

pub async fn mark_zombie_runs_failed(pool: &PgPool, hours: i32) -> Result<i64> {
    let msg = format!("Zombie run: no activity for {hours} hours");
    let result = sqlx::query(
        r#"UPDATE pipeline_runs
           SET status = $1, error_message = $2, completed_at = now(), updated_at = now()
           WHERE status = $3
             AND updated_at < now() - make_interval(secs => $4::int * 3600)"#,
    )
    .bind(RunStatus::Failed)
    .bind(&msg)
    .bind(RunStatus::Running)
    .bind(hours)
    .execute(pool)
    .await
    .context("mark_zombie_runs_failed: failed to update pipeline_runs")?;
    Ok(result.rows_affected() as i64)
}

// ── Pipeline Step Runs ────────────────────────────────────────────────────────

pub async fn start_run_step(
    pool: &PgPool,
    run_id: Uuid,
    step_name: StepName,
    items_count: Option<i32>,
) -> Result<PipelineStepRun> {
    sqlx::query_as::<_, PipelineStepRun>(
        r#"INSERT INTO pipeline_step_runs (run_id, step_name, status, items_count)
           VALUES ($1, $2, $3, $4)
           RETURNING *"#,
    )
    .bind(run_id)
    .bind(step_name)
    .bind(StepStatus::Running)
    .bind(items_count.unwrap_or(0))
    .fetch_one(pool)
    .await
    .context("start_run_step: failed to insert pipeline_step_run")
}

pub async fn record_run_step_progress(
    pool: &PgPool,
    step_run_id: Uuid,
    items_processed: i32,
    items_failed: i32,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE pipeline_step_runs
           SET items_processed = $2, items_failed = $3
           WHERE id = $1"#,
    )
    .bind(step_run_id)
    .bind(items_processed)
    .bind(items_failed)
    .execute(pool)
    .await
    .context("record_run_step_progress: failed to update pipeline_step_run")?;
    Ok(())
}

pub async fn complete_run_step(pool: &PgPool, step_run_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"UPDATE pipeline_step_runs
           SET status = $2, completed_at = now()
           WHERE id = $1"#,
    )
    .bind(step_run_id)
    .bind(StepStatus::Completed)
    .execute(pool)
    .await
    .context("complete_run_step: failed to update pipeline_step_run")?;
    Ok(())
}

pub async fn fail_run_step(pool: &PgPool, step_run_id: Uuid, error: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE pipeline_step_runs
           SET status = $2, error_message = $3, completed_at = now()
           WHERE id = $1"#,
    )
    .bind(step_run_id)
    .bind(StepStatus::Failed)
    .bind(error)
    .execute(pool)
    .await
    .context("fail_run_step: failed to update pipeline_step_run")?;
    Ok(())
}

pub async fn mark_stale_step_runs_completed(pool: &PgPool, minutes: i32) -> Result<i64> {
    let result = sqlx::query(
        r#"UPDATE pipeline_step_runs
           SET status = $1, completed_at = now()
           WHERE status = $2
             AND started_at < now() - make_interval(secs => $3::int * 60)"#,
    )
    .bind(StepStatus::Completed)
    .bind(StepStatus::Running)
    .bind(minutes)
    .execute(pool)
    .await
    .context("mark_stale_step_runs_completed: failed to update pipeline_step_runs")?;
    Ok(result.rows_affected() as i64)
}

pub async fn mark_zombie_step_runs_failed(pool: &PgPool, hours: i32) -> Result<i64> {
    let msg = format!("Zombie step: no activity for {hours} hours");
    let result = sqlx::query(
        r#"UPDATE pipeline_step_runs
           SET status = $1, error_message = $2, completed_at = now()
           WHERE status = $3
             AND started_at < now() - make_interval(secs => $4::int * 3600)"#,
    )
    .bind(StepStatus::Failed)
    .bind(&msg)
    .bind(StepStatus::Running)
    .bind(hours)
    .execute(pool)
    .await
    .context("mark_zombie_step_runs_failed: failed to update pipeline_step_runs")?;
    Ok(result.rows_affected() as i64)
}

pub async fn check_run_completion(pool: &PgPool, run_id: Uuid) -> Result<bool> {
    let running_count: i64 = sqlx::query_scalar::<_, i64>(
        r#"SELECT COUNT(*) FROM pipeline_step_runs WHERE run_id = $1 AND status = $2"#,
    )
    .bind(run_id)
    .bind(StepStatus::Running)
    .fetch_one(pool)
    .await
    .context("check_run_completion: failed to count running steps")?;

    if running_count > 0 {
        return Ok(false);
    }

    let failed_count: i64 = sqlx::query_scalar::<_, i64>(
        r#"SELECT COUNT(*) FROM pipeline_step_runs WHERE run_id = $1 AND status = $2"#,
    )
    .bind(run_id)
    .bind(StepStatus::Failed)
    .fetch_one(pool)
    .await
    .context("check_run_completion: failed to count failed steps")?;

    if failed_count > 0 {
        fail_run(pool, run_id, "One or more steps failed").await?;
    } else {
        complete_run(pool, run_id).await?;
    }

    Ok(true)
}

// ── Orphaned Articles ─────────────────────────────────────────────────────────

pub async fn reclaim_orphaned_articles(pool: &PgPool, _run_id: Uuid) -> Result<Vec<String>> {
    let ids = sqlx::query_scalar::<_, String>(
        r#"SELECT id::text FROM raw_articles
           WHERE processing_status = 'ingested'
             AND created_at > now() - interval '24 hours'"#,
    )
    .fetch_all(pool)
    .await
    .context("reclaim_orphaned_articles: failed to query raw_articles")?;
    Ok(ids)
}

pub async fn clean_orphaned_articles_from_failed_runs(pool: &PgPool) -> Result<i64> {
    let result = sqlx::query(
        r#"DELETE FROM raw_articles
           WHERE processing_status = 'ingested'
             AND created_at < now() - interval '7 days'
             AND source_id NOT IN (
               SELECT DISTINCT source_id FROM raw_articles a
               INNER JOIN pipeline_runs r ON r.started_at > a.created_at
               WHERE r.status = 'running'
             )"#,
    )
    .execute(pool)
    .await
    .context("clean_orphaned_articles_from_failed_runs: failed to delete raw_articles")?;
    Ok(result.rows_affected() as i64)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

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

        // Ensure tables exist
        let _ = sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS feed_sources (
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
            )"#,
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS pipeline_runs (
                id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                status              TEXT NOT NULL DEFAULT 'running',
                trigger_type        TEXT NOT NULL DEFAULT 'scheduled',
                started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                completed_at        TIMESTAMPTZ,
                error_message       TEXT,
                feeds_count         INTEGER,
                articles_ingested   INTEGER NOT NULL DEFAULT 0,
                articles_qualified  INTEGER NOT NULL DEFAULT 0,
                articles_rejected   INTEGER NOT NULL DEFAULT 0,
                articles_duplicate  INTEGER NOT NULL DEFAULT 0,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS pipeline_step_runs (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                run_id          UUID NOT NULL REFERENCES pipeline_runs(id),
                step_name       TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'running',
                started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                completed_at    TIMESTAMPTZ,
                error_message   TEXT,
                items_count     INTEGER NOT NULL DEFAULT 0,
                items_processed INTEGER NOT NULL DEFAULT 0,
                items_failed    INTEGER NOT NULL DEFAULT 0,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&pool)
        .await;

        let _ = sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS raw_articles (
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
                duplicate_of                UUID REFERENCES raw_articles(id),
                preferred_extraction_method TEXT,
                extraction_attempts         INTEGER NOT NULL DEFAULT 0,
                last_extraction_error       TEXT,
                last_extraction_at          TIMESTAMPTZ,
                created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT unique_raw_articles_url_source UNIQUE (url, source_id)
            )"#,
        )
        .execute(&pool)
        .await;

        // Clean slate
        sqlx::query("DELETE FROM pipeline_step_runs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM pipeline_runs")
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
    async fn test_start_run() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let run = start_run(&pool, RunTriggerType::Scheduled, Some(10))
            .await
            .expect("start_run should succeed");

        assert_eq!(run.status, RunStatus::Running);
        assert_eq!(run.trigger_type, RunTriggerType::Scheduled);
        assert_eq!(run.feeds_count, Some(10));
        assert!(run.completed_at.is_none());
        assert!(run.error_message.is_none());

        let run2 = start_run(&pool, RunTriggerType::Manual, None)
            .await
            .expect("start_run should succeed");

        assert_eq!(run2.trigger_type, RunTriggerType::Manual);
        assert!(run2.feeds_count.is_none());
    }

    #[tokio::test]
    async fn test_fail_run() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let run = start_run(&pool, RunTriggerType::Scheduled, None)
            .await
            .unwrap();

        fail_run(&pool, run.id, "connection timeout")
            .await
            .expect("fail_run should succeed");

        let updated: PipelineRun = sqlx::query_as("SELECT * FROM pipeline_runs WHERE id = $1")
            .bind(run.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(updated.status, RunStatus::Failed);
        assert_eq!(updated.error_message.as_deref(), Some("connection timeout"));
        assert!(updated.completed_at.is_some());
    }

    #[tokio::test]
    async fn test_mark_zombie_runs() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        // Create a run and backdate its updated_at
        let run = start_run(&pool, RunTriggerType::Scheduled, None)
            .await
            .unwrap();

        sqlx::query(
            "UPDATE pipeline_runs SET updated_at = now() - interval '48 hours' WHERE id = $1",
        )
        .bind(run.id)
        .execute(&pool)
        .await
        .unwrap();

        let affected = mark_zombie_runs_failed(&pool, 24)
            .await
            .expect("mark_zombie_runs_failed should succeed");

        assert_eq!(affected, 1);

        let updated: PipelineRun = sqlx::query_as("SELECT * FROM pipeline_runs WHERE id = $1")
            .bind(run.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(updated.status, RunStatus::Failed);
        assert!(updated
            .error_message
            .as_deref()
            .unwrap()
            .contains("Zombie run"));
    }

    #[tokio::test]
    async fn test_start_run_step() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let run = start_run(&pool, RunTriggerType::Manual, None)
            .await
            .unwrap();

        let step = start_run_step(&pool, run.id, StepName::Ingest, Some(42))
            .await
            .expect("start_run_step should succeed");

        assert_eq!(step.run_id, run.id);
        assert_eq!(step.step_name, StepName::Ingest);
        assert_eq!(step.status, StepStatus::Running);
        assert_eq!(step.items_count, 42);
        assert_eq!(step.items_processed, 0);
        assert!(step.completed_at.is_none());
    }

    #[tokio::test]
    async fn test_complete_run_step() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let run = start_run(&pool, RunTriggerType::Manual, None)
            .await
            .unwrap();

        let step = start_run_step(&pool, run.id, StepName::Content, Some(10))
            .await
            .unwrap();

        // Record progress
        record_run_step_progress(&pool, step.id, 8, 2)
            .await
            .expect("record_run_step_progress should succeed");

        complete_run_step(&pool, step.id)
            .await
            .expect("complete_run_step should succeed");

        let updated: PipelineStepRun =
            sqlx::query_as("SELECT * FROM pipeline_step_runs WHERE id = $1")
                .bind(step.id)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(updated.status, StepStatus::Completed);
        assert!(updated.completed_at.is_some());
        assert_eq!(updated.items_processed, 8);
        assert_eq!(updated.items_failed, 2);
    }

    #[tokio::test]
    async fn test_check_run_completion() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let run = start_run(&pool, RunTriggerType::Manual, None)
            .await
            .unwrap();

        let step1 = start_run_step(&pool, run.id, StepName::Ingest, None)
            .await
            .unwrap();
        let step2 = start_run_step(&pool, run.id, StepName::Content, None)
            .await
            .unwrap();

        // Not complete yet — both steps still running
        let done = check_run_completion(&pool, run.id)
            .await
            .expect("check_run_completion should succeed");
        assert!(!done);

        // Complete step1
        complete_run_step(&pool, step1.id).await.unwrap();

        let done = check_run_completion(&pool, run.id).await.unwrap();
        assert!(!done);

        // Fail step2
        fail_run_step(&pool, step2.id, "boom").await.unwrap();

        let done = check_run_completion(&pool, run.id).await.unwrap();
        assert!(done);

        // Run should be marked failed (because step2 failed)
        let updated: PipelineRun = sqlx::query_as("SELECT * FROM pipeline_runs WHERE id = $1")
            .bind(run.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(updated.status, RunStatus::Failed);
        assert!(updated.error_message.is_some());
    }
}
