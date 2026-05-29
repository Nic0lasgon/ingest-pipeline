pub mod content_worker;
pub mod ingest_worker;
pub mod scheduler;

use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgPool;

use crate::queue::worker::HandlerFn;

pub async fn start_worker(pool: PgPool) -> anyhow::Result<()> {
    let worker_id = format!("worker-{}", uuid::Uuid::new_v4());
    tracing::info!(worker_id = %worker_id, "Starting worker");

    let pool_for_ingest = pool.clone();
    let pool_for_content = pool.clone();

    let mut handlers: HashMap<String, HandlerFn> = HashMap::new();

    handlers.insert(
        "fetch_feed".to_string(),
        Arc::new(move |job| {
            let pool = pool_for_ingest.clone();
            Box::pin(async move { ingest_worker::handle_ingest_job(&pool, job.payload).await })
        }),
    );

    handlers.insert(
        "process_article".to_string(),
        Arc::new(move |job| {
            let pool = pool_for_content.clone();
            Box::pin(
                async move { content_worker::handle_content_job(&pool, job.payload, None).await },
            )
        }),
    );

    let worker = crate::queue::worker::Worker::new(worker_id, pool, handlers);
    worker.run().await;

    Ok(())
}

pub async fn start_scheduler(pool: PgPool) -> anyhow::Result<()> {
    scheduler::run_scheduler(pool).await;
    Ok(())
}
