mod common;

use common::{cleanup_bench_data, ensure_feed_source, setup_test_pool};
use criterion::{criterion_group, criterion_main, Criterion};
use httpmock::prelude::*;
use ingest_pipeline::config::Config;
use ingest_pipeline::pipeline::ingest_step::process_ingest_step;
use std::sync::Arc;

const LEMONDE_RSS: &str = include_str!("../tests/fixtures/rss/lemonde.xml");
const RSS2_SIMPLE: &str = include_str!("../tests/fixtures/rss/rss2_simple.xml");

async fn setup_ingest_bench(pool: &sqlx::PgPool, feed_id: &str, mock_url: &str) {
    ensure_feed_source(pool, feed_id).await;

    sqlx::query(
        r#"UPDATE feed_sources
           SET feed_url = $2,
               enabled = true,
               fetch_status = 'pending',
               last_ingested_pub_date = NULL
           WHERE id = $1"#,
    )
    .bind(feed_id)
    .bind(mock_url)
    .execute(pool)
    .await
    .unwrap();
}

async fn reset_feed(pool: &sqlx::PgPool, feed_id: &str) {
    sqlx::query("DELETE FROM raw_articles WHERE source_id = $1")
        .bind(feed_id)
        .execute(pool)
        .await
        .unwrap();

    sqlx::query(
        r#"UPDATE feed_sources
           SET fetch_status = 'pending',
               last_ingested_pub_date = NULL
           WHERE id = $1"#,
    )
    .bind(feed_id)
    .execute(pool)
    .await
    .unwrap();
}

fn bench_ingest_lemonde(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = match rt.block_on(setup_test_pool()) {
        Some(p) => p,
        None => {
            eprintln!("SKIP bench_ingest: DATABASE_URL not set or DB unreachable");
            return;
        }
    };

    let server = MockServer::start();
    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss/lemonde");
        then.status(200)
            .header("content-type", "application/xml; charset=utf-8")
            .body(LEMONDE_RSS);
    });

    let feed_id = "bench_ingest_lemonde";
    let feed_url = server.url("/rss/lemonde");
    rt.block_on(setup_ingest_bench(&pool, feed_id, &feed_url));

    let config = Arc::new(Config::for_tests());

    let mut group = c.benchmark_group("ingest_full_pipeline");
    group.sample_size(15);

    let pool_c = pool.clone();
    let config_c = Arc::clone(&config);
    let fid = feed_id.to_string();
    group.bench_function("lemonde_3_items", |b| {
        b.iter(|| {
            let config = &*config_c;
            rt.block_on(async {
                reset_feed(&pool_c, &fid).await;
                let result = process_ingest_step(&pool_c, &fid, None, config).await;
                assert!(result.error.is_none(), "ingest failed: {:?}", result.error);
            })
        })
    });

    group.finish();

    rt.block_on(cleanup_bench_data(&pool));
}

fn bench_ingest_rss2_simple(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = match rt.block_on(setup_test_pool()) {
        Some(p) => p,
        None => return,
    };

    let server = MockServer::start();
    let _mock = server.mock(|when, then| {
        when.method(GET).path("/rss/simple");
        then.status(200)
            .header("content-type", "application/xml")
            .body(RSS2_SIMPLE);
    });

    let feed_id = "bench_ingest_simple";
    let feed_url = server.url("/rss/simple");
    rt.block_on(setup_ingest_bench(&pool, feed_id, &feed_url));

    let config = Arc::new(Config::for_tests());

    let mut group = c.benchmark_group("ingest_full_pipeline");
    group.sample_size(15);

    let pool_c = pool.clone();
    let config_c = Arc::clone(&config);
    let fid = feed_id.to_string();
    group.bench_function("rss2_simple_3_items", |b| {
        b.iter(|| {
            let config = &*config_c;
            rt.block_on(async {
                reset_feed(&pool_c, &fid).await;
                let result = process_ingest_step(&pool_c, &fid, None, config).await;
                assert!(result.error.is_none(), "ingest failed: {:?}", result.error);
            })
        })
    });

    group.finish();

    rt.block_on(cleanup_bench_data(&pool));
}

criterion_group!(benches, bench_ingest_lemonde, bench_ingest_rss2_simple);
criterion_main!(benches);
