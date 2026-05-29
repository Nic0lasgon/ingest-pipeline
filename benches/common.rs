use ingest_pipeline::db::schema::{
    DuplicateStatus, ProcessingStatus, QualityStatus, RawArticle,
};
use sqlx::PgPool;

pub async fn setup_test_pool() -> Option<PgPool> {
    dotenvy::dotenv().ok();
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPool::connect(&url).await.ok()?;

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

    Some(pool)
}

pub async fn ensure_feed_source(pool: &PgPool, id: &str) {
    sqlx::query(
        r#"INSERT INTO feed_sources (id, feed_url, name, priority)
           VALUES ($1, 'https://bench.internal/' || $1, $1, 0)
           ON CONFLICT (id) DO NOTHING"#,
    )
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
}

pub fn make_test_article(source_id: &str, batch: u32, idx: u32) -> RawArticle {
    let now = chrono::Utc::now();
    RawArticle {
        id: uuid::Uuid::new_v4(),
        source_id: source_id.to_string(),
        title: format!("Bench Article {batch}-{idx}"),
        url: format!("https://bench.internal/{source_id}/{batch}/{idx}"),
        description: Some(format!("Description for benchmark article {batch}-{idx}")),
        image_url: None,
        author: Some("Bench Author".to_string()),
        pub_date: Some(now),
        content: None,
        content_length: None,
        content_hash: None,
        title_clean: None,
        canonical_url: None,
        processing_status: ProcessingStatus::Ingested,
        quality_status: QualityStatus::Pending,
        duplicate_status: DuplicateStatus::Pending,
        duplicate_of: None,
        preferred_extraction_method: None,
        extraction_attempts: 0,
        last_extraction_error: None,
        last_extraction_at: None,
        created_at: now,
        updated_at: now,
    }
}

pub async fn cleanup_bench_data(pool: &PgPool) {
    sqlx::query("DELETE FROM raw_articles WHERE source_id LIKE 'bench-%'")
        .execute(pool)
        .await
        .ok();
}
