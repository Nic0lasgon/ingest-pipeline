use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::schema::Job;

pub async fn create_job(
    pool: &PgPool,
    job_type: &str,
    payload: Value,
    priority: i32,
) -> Result<Job> {
    let job = sqlx::query_as::<_, Job>(
        "INSERT INTO jobs (job_type, payload, priority) VALUES ($1, $2, $3) RETURNING *",
    )
    .bind(job_type)
    .bind(&payload)
    .bind(priority)
    .fetch_one(pool)
    .await
    .context("create_job: failed to insert job")?;

    sqlx::query("NOTIFY jobs_channel")
        .execute(pool)
        .await
        .context("create_job: failed to send NOTIFY")?;

    Ok(job)
}

pub async fn pick_jobs(pool: &PgPool, worker_id: &str, batch_size: i32) -> Result<Vec<Job>> {
    sqlx::query_as::<_, Job>(
        r#"
        WITH selected AS (
            SELECT id FROM jobs
            WHERE status = 'pending' AND run_at <= now()
            ORDER BY priority DESC, created_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs SET
            status = 'running',
            locked_at = now(),
            locked_by = $1,
            updated_at = now()
        FROM selected
        WHERE jobs.id = selected.id
        RETURNING jobs.*
        "#,
    )
    .bind(worker_id)
    .bind(batch_size)
    .fetch_all(pool)
    .await
    .context("pick_jobs: failed to pick jobs")
}

pub async fn complete_job(pool: &PgPool, job_id: Uuid) -> Result<()> {
    sqlx::query("UPDATE jobs SET status = 'completed', updated_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await
        .context("complete_job: failed to update job status")?;
    Ok(())
}

pub async fn fail_job(pool: &PgPool, job_id: Uuid, error: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE jobs SET
            attempts = attempts + 1,
            last_error = $2,
            status = CASE
                WHEN attempts + 1 >= max_attempts THEN 'dead'::job_status
                ELSE 'pending'::job_status
            END,
            run_at = CASE
                WHEN attempts + 1 >= max_attempts THEN run_at
                ELSE now() + make_interval(secs => power(2, attempts)::integer)
            END,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(job_id)
    .bind(error)
    .execute(pool)
    .await
    .context("fail_job: failed to update job")?;
    Ok(())
}

pub async fn mark_dead(pool: &PgPool, job_id: Uuid) -> Result<()> {
    sqlx::query("UPDATE jobs SET status = 'dead', updated_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(pool)
        .await
        .context("mark_dead: failed to update job status")?;
    Ok(())
}

pub async fn retry_job(pool: &PgPool, job_id: Uuid, backoff_secs: i32) -> Result<()> {
    sqlx::query(
        "UPDATE jobs SET status = 'pending', run_at = now() + make_interval(secs => $2), updated_at = now() WHERE id = $1",
    )
    .bind(job_id)
    .bind(backoff_secs)
    .execute(pool)
    .await
    .context("retry_job: failed to requeue job")?;
    Ok(())
}

pub async fn cleanup_dead_jobs(pool: &PgPool, older_than_days: i32) -> Result<i64> {
    let result = sqlx::query(
        "DELETE FROM jobs WHERE status = 'dead' AND updated_at < now() - make_interval(days => $1::int)",
    )
    .bind(older_than_days)
    .execute(pool)
    .await
    .context("cleanup_dead_jobs: failed to delete dead jobs")?;
    Ok(result.rows_affected() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::JobStatus;
    use sqlx::PgPool;
    use std::env;
    use std::sync::LazyLock;
    use tokio::sync::Mutex;

    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    async fn setup_test_pool() -> Option<PgPool> {
        let url = env::var("DATABASE_URL").ok()?;
        let pool = PgPool::connect(&url).await.ok()?;

        // Ensure job_status enum exists
        let _ = sqlx::query(
            "DO $$ BEGIN
                CREATE TYPE job_status AS ENUM ('pending', 'running', 'completed', 'failed', 'dead');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$",
        )
        .execute(&pool)
        .await;

        // Ensure jobs table exists
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

        // Clean slate
        sqlx::query("DELETE FROM jobs")
            .execute(&pool)
            .await
            .unwrap();

        Some(pool)
    }

    #[tokio::test]
    async fn test_create_job() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let job = create_job(&pool, "test_type", serde_json::json!({"key": "value"}), 5)
            .await
            .expect("create_job should succeed");

        assert_eq!(job.job_type, "test_type");
        assert_eq!(job.payload, serde_json::json!({"key": "value"}));
        assert_eq!(job.priority, 5);
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.attempts, 0);
        assert_eq!(job.max_attempts, 3);
    }

    #[tokio::test]
    async fn test_pick_jobs() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        // Create jobs with different priorities
        create_job(&pool, "low", serde_json::json!({}), 1)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        create_job(&pool, "high", serde_json::json!({}), 10)
            .await
            .unwrap();

        let jobs = pick_jobs(&pool, "worker-1", 5)
            .await
            .expect("pick_jobs should succeed");

        // Should pick both, highest priority first
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_type, "high");
        assert_eq!(jobs[1].job_type, "low");
        assert_eq!(jobs[0].status, JobStatus::Running);
        assert_eq!(jobs[0].locked_by.as_deref(), Some("worker-1"));

        // No more pending jobs to pick
        let empty = pick_jobs(&pool, "worker-2", 5).await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_complete_job() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let job = create_job(&pool, "task", serde_json::json!({}), 0)
            .await
            .unwrap();

        complete_job(&pool, job.id)
            .await
            .expect("complete_job should succeed");

        let updated: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(updated.status, JobStatus::Completed);
    }

    #[tokio::test]
    async fn test_fail_job_with_retry() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let job = create_job(&pool, "retry-task", serde_json::json!({}), 0)
            .await
            .unwrap();

        fail_job(&pool, job.id, "temporary error")
            .await
            .expect("fail_job should succeed");

        let updated: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(updated.status, JobStatus::Pending);
        assert_eq!(updated.attempts, 1);
        assert_eq!(updated.last_error.as_deref(), Some("temporary error"));
        // run_at should be > now (backoff applied)
        assert!(
            updated.run_at > job.created_at,
            "run_at should be in the future due to backoff"
        );
    }

    #[tokio::test]
    async fn test_fail_job_to_dead() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let job = create_job(&pool, "doomed-task", serde_json::json!({}), 0)
            .await
            .unwrap();

        // Fail it 3 times (max_attempts is 3 by default)
        fail_job(&pool, job.id, "error 1").await.unwrap();
        fail_job(&pool, job.id, "error 2").await.unwrap();
        fail_job(&pool, job.id, "error 3").await.unwrap();

        let updated: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(updated.status, JobStatus::Dead);
        assert_eq!(updated.attempts, 3);
    }

    #[tokio::test]
    async fn test_cleanup_dead_jobs() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        // Create a job and mark it dead
        let job = create_job(&pool, "doomed", serde_json::json!({}), 0)
            .await
            .unwrap();

        mark_dead(&pool, job.id).await.unwrap();

        // Manually set updated_at far in the past (older than threshold)
        sqlx::query("UPDATE jobs SET updated_at = now() - interval '31 days' WHERE id = $1")
            .bind(job.id)
            .execute(&pool)
            .await
            .unwrap();

        let deleted = cleanup_dead_jobs(&pool, 30)
            .await
            .expect("cleanup_dead_jobs should succeed");

        assert_eq!(deleted, 1);

        // Verify job is gone
        let remaining: Vec<Job> = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_mark_dead() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let job = create_job(&pool, "kill-me", serde_json::json!({}), 0)
            .await
            .unwrap();

        mark_dead(&pool, job.id)
            .await
            .expect("mark_dead should succeed");

        let updated: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(updated.status, JobStatus::Dead);
    }

    #[tokio::test]
    async fn test_retry_job() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let job = create_job(&pool, "requeue", serde_json::json!({}), 0)
            .await
            .unwrap();

        retry_job(&pool, job.id, 60)
            .await
            .expect("retry_job should succeed");

        let updated: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(updated.status, JobStatus::Pending);
        // run_at should be ~60 seconds from now
    }

    #[tokio::test]
    async fn test_create_job_sends_notify() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
            .await
            .expect("Failed to connect PgListener");
        listener
            .listen("jobs_channel")
            .await
            .expect("Failed to listen on jobs_channel");

        // Create a job in a separate task so the listener can receive the NOTIFY
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            create_job(&pool_clone, "notify_test", serde_json::json!({}), 0)
                .await
                .expect("create_job should succeed");
        });

        // Wait for the notification with a timeout
        let recv_result =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.recv()).await;

        handle.await.unwrap();

        assert!(
            recv_result.is_ok(),
            "Should have received a notification within timeout"
        );
        let notification = recv_result.unwrap().expect("recv should succeed");
        assert_eq!(notification.channel(), "jobs_channel");
    }

    #[tokio::test]
    async fn test_pick_jobs_skip_locked() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        for _ in 0..3 {
            create_job(&pool, "batch", serde_json::json!({}), 0)
                .await
                .unwrap();
        }

        // Pick only 1 — the other 2 should remain pending
        let picked = pick_jobs(&pool, "worker-1", 1)
            .await
            .expect("pick_jobs should succeed");

        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].status, JobStatus::Running);
        assert_eq!(picked[0].locked_by.as_deref(), Some("worker-1"));

        // Remaining jobs should still be pending
        let remaining: Vec<Job> = sqlx::query_as("SELECT * FROM jobs WHERE status = 'pending'")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 2);
    }
}
