pub mod article_queries;
pub mod feed_queries;
pub mod rejected_queries;
pub mod run_queries;
#[allow(dead_code)]
pub mod schema;

pub async fn run_migrations(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::migrate!("./migrations").run(pool).await?;
    tracing::info!("Database migrations applied successfully");
    Ok(())
}
