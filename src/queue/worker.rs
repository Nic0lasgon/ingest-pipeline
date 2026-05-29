use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

use crate::db::schema::Job;
use crate::queue::jobs::{complete_job, fail_job, pick_jobs};

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
pub type HandlerFn = Arc<dyn Fn(Job) -> HandlerFuture + Send + Sync>;

pub struct Worker {
    worker_id: String,
    pool: PgPool,
    handlers: HashMap<String, HandlerFn>,
    poll_interval: Duration,
    batch_size: i32,
}

impl Worker {
    pub fn new(worker_id: String, pool: PgPool, handlers: HashMap<String, HandlerFn>) -> Self {
        Self {
            worker_id,
            pool,
            handlers,
            poll_interval: Duration::from_secs(1),
            batch_size: 10,
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn with_batch_size(mut self, size: i32) -> Self {
        self.batch_size = size;
        self
    }

    pub async fn run(self) {
        info!("Worker {} started", self.worker_id);

        let mut listener = match PgListener::connect_with(&self.pool).await {
            Ok(mut l) => {
                if let Err(e) = l.listen("jobs_channel").await {
                    warn!(error = %e, "Failed to listen on jobs_channel, falling back to polling only");
                    None
                } else {
                    info!("Worker {} listening on jobs_channel", self.worker_id);
                    Some(l)
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to connect PgListener, falling back to polling only");
                None
            }
        };

        let mut poll_timer = interval(self.poll_interval);
        poll_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Worker {} shutting down gracefully", self.worker_id);
                    break;
                }
                notification = async {
                    match listener.as_mut() {
                        Some(l) => l.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match notification {
                        Ok(_notification) => {
                            info!(worker_id = %self.worker_id, "Wake up received via NOTIFY");
                        }
                        Err(e) => {
                            warn!(error = %e, worker_id = %self.worker_id, "LISTEN connection lost, falling back to polling");
                            listener = None;
                        }
                    }
                    // Poll immediately on notification
                    match pick_jobs(&self.pool, &self.worker_id, self.batch_size).await {
                        Ok(jobs) => {
                            for job in jobs {
                                self.process_job(job).await;
                            }
                        }
                        Err(e) => {
                            error!(error = %e, worker_id = %self.worker_id, "Failed to pick jobs after NOTIFY");
                        }
                    }
                }
                _ = poll_timer.tick() => {
                    // Regular polling fallback
                    match pick_jobs(&self.pool, &self.worker_id, self.batch_size).await {
                        Ok(jobs) => {
                            if !jobs.is_empty() {
                                for job in jobs {
                                    self.process_job(job).await;
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, worker_id = %self.worker_id, "Failed to pick jobs");
                        }
                    }
                }
            }
        }

        info!("Worker {} stopped", self.worker_id);
    }

    async fn process_job(&self, job: Job) {
        let job_id = job.id;
        let job_type = job.job_type.clone();
        let attempts = job.attempts;
        let max_attempts = job.max_attempts;

        info!(job_id = %job_id, job_type = %job_type, "Job started");
        let started = Instant::now();

        match self.handlers.get(&job_type) {
            Some(handler) => match handler(job).await {
                Ok(()) => {
                    let duration_ms = started.elapsed().as_millis();
                    if let Err(e) = complete_job(&self.pool, job_id).await {
                        error!(job_id = %job_id, error = %e, "Failed to mark job as complete");
                    } else {
                        info!(job_id = %job_id, duration_ms = %duration_ms, "Job completed");
                    }
                }
                Err(e) => {
                    let duration_ms = started.elapsed().as_millis();
                    error!(job_id = %job_id, error = %e, duration_ms = %duration_ms, "Job failed");
                    if let Err(fail_err) = fail_job(&self.pool, job_id, &e.to_string()).await {
                        error!(job_id = %job_id, error = %fail_err, "Failed to mark job as failed");
                    } else if attempts + 1 < max_attempts {
                        warn!(job_id = %job_id, attempt = attempts + 1, "Job retry scheduled");
                    }
                }
            },
            None => {
                warn!("No handler for job type: {}", job_type);
                if let Err(e) = fail_job(&self.pool, job_id, "no_handler").await {
                    error!(job_id = %job_id, error = %e, "Failed to fail job with no_handler");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::JobStatus;
    use crate::queue::jobs::create_job;
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

        Some(pool)
    }

    fn make_worker(pool: PgPool, handlers: HashMap<String, HandlerFn>) -> Worker {
        Worker::new("test-worker".to_string(), pool, handlers)
    }

    #[tokio::test]
    async fn test_worker_dispatch_success() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let mut handlers: HashMap<String, HandlerFn> = HashMap::new();
        handlers.insert(
            "test_success".to_string(),
            Arc::new(|_job| Box::pin(async { Ok(()) })),
        );

        let worker = make_worker(pool.clone(), handlers);

        let job = create_job(&pool, "test_success", serde_json::json!({}), 0)
            .await
            .unwrap();
        let picked = pick_jobs(&pool, &worker.worker_id, 1).await.unwrap();
        assert_eq!(picked.len(), 1);

        worker.process_job(picked.into_iter().next().unwrap()).await;

        let updated: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(updated.status, JobStatus::Completed);
    }

    #[tokio::test]
    async fn test_worker_dispatch_failure() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let mut handlers: HashMap<String, HandlerFn> = HashMap::new();
        handlers.insert(
            "test_failure".to_string(),
            Arc::new(|_job| Box::pin(async { Err(anyhow::anyhow!("handler error")) })),
        );

        let worker = make_worker(pool.clone(), handlers);

        let job = create_job(&pool, "test_failure", serde_json::json!({}), 0)
            .await
            .unwrap();
        let picked = pick_jobs(&pool, &worker.worker_id, 1).await.unwrap();
        assert_eq!(picked.len(), 1);

        worker.process_job(picked.into_iter().next().unwrap()).await;

        let updated: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(updated.status, JobStatus::Pending);
        assert_eq!(updated.attempts, 1);
        assert_eq!(updated.last_error.as_deref(), Some("handler error"));
    }

    #[tokio::test]
    async fn test_worker_no_handler() {
        let _guard = TEST_LOCK.lock().await;
        let pool = match setup_test_pool().await {
            Some(p) => p,
            None => return,
        };

        let handlers: HashMap<String, HandlerFn> = HashMap::new();
        let worker = make_worker(pool.clone(), handlers);

        let job = create_job(&pool, "unknown_type", serde_json::json!({}), 0)
            .await
            .unwrap();
        let picked = pick_jobs(&pool, &worker.worker_id, 1).await.unwrap();
        assert_eq!(picked.len(), 1);

        worker.process_job(picked.into_iter().next().unwrap()).await;

        let updated: Job = sqlx::query_as("SELECT * FROM jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(updated.status, JobStatus::Pending);
        assert_eq!(updated.last_error.as_deref(), Some("no_handler"));
    }
}
