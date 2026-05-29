use axum::{routing::get, Json, Router};
use serde_json::Value;
use sqlx::PgPool;

pub async fn start(pool: PgPool) -> anyhow::Result<()> {
    let config = crate::config::Config::load_config()?;

    let app = Router::new()
        .route("/health", get(health_check))
        .with_state(pool);

    let addr = format!("0.0.0.0:{}", config.port);
    tracing::info!("API server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> Json<Value> {
    Json(serde_json::json!({"status": "ok"}))
}
