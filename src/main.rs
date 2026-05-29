use clap::{Parser, Subcommand};
use ingest_pipeline::api;
use ingest_pipeline::config;
use ingest_pipeline::db;
use ingest_pipeline::workers;
use sqlx::PgPool;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "ingest-pipeline", about = "MyPod RSS Ingestion Pipeline")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the HTTP API server
    Api,
    /// Run job workers
    Worker,
    /// Run the scheduler
    Scheduler,
    /// Run all components (API + worker + scheduler)
    All,
}

fn init_tracing(log_level: &str) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    let is_dev = std::env::var("APP_ENV")
        .map(|v| v == "development" || v == "dev")
        .unwrap_or(false);

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true);

    if is_dev {
        subscriber.init();
    } else {
        subscriber.json().init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = config::Config::load_config()?;
    init_tracing(&config.log_level);

    let pool = PgPool::connect(&config.database_url).await?;
    db::run_migrations(&pool).await?;

    match cli.command {
        Commands::Api => {
            tracing::info!("Starting API server...");
            api::start(pool).await
        }
        Commands::Worker => {
            tracing::info!("Starting worker...");
            workers::start_worker(pool).await
        }
        Commands::Scheduler => {
            tracing::info!("Starting scheduler...");
            workers::start_scheduler(pool).await
        }
        Commands::All => {
            tracing::info!("Starting all components...");
            let pool_for_api = pool.clone();
            let pool_for_worker = pool.clone();

            let api_handle = tokio::spawn(async move { api::start(pool_for_api).await });
            let worker_handle =
                tokio::spawn(async move { workers::start_worker(pool_for_worker).await });
            let scheduler_handle =
                tokio::spawn(async move { workers::start_scheduler(pool).await });

            tokio::select! {
                res = api_handle => {
                    tracing::info!("API server stopped");
                    res?
                }
                res = worker_handle => {
                    tracing::info!("Worker stopped");
                    res?
                }
                res = scheduler_handle => {
                    tracing::info!("Scheduler stopped");
                    res?
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Received shutdown signal");
                    Ok(())
                }
            }
        }
    }
}
