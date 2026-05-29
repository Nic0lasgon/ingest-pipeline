mod common;

use common::{cleanup_bench_data, ensure_feed_source, make_test_article, setup_test_pool};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use ingest_pipeline::db::article_queries::{insert, insert_batch};
use std::sync::atomic::{AtomicU32, Ordering};

fn bench_insert_comparison(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = match rt.block_on(setup_test_pool()) {
        Some(p) => p,
        None => {
            eprintln!("SKIP bench_insert: DATABASE_URL not set or DB unreachable");
            eprintln!("  -> docker compose up -d postgres");
            return;
        }
    };

    rt.block_on(async {
        ensure_feed_source(&pool, "bench_ind").await;
        ensure_feed_source(&pool, "bench_batch").await;
    });

    let mut group = c.benchmark_group("insert_30_articles");
    group.sample_size(20);

    let counter_ind = AtomicU32::new(0);
    let pool_ind = pool.clone();
    group.bench_function("individual_x30", |b| {
        b.iter(|| {
            let batch = counter_ind.fetch_add(1, Ordering::Relaxed);
            let articles: Vec<_> = (0..30)
                .map(|i| make_test_article("bench_ind", batch, i))
                .collect();
            rt.block_on(async {
                for article in &articles {
                    insert(&pool_ind, article).await.unwrap();
                }
            })
        })
    });

    let counter_batch = AtomicU32::new(0);
    let pool_batch = pool.clone();
    group.bench_function("batch_x30", |b| {
        b.iter(|| {
            let batch = counter_batch.fetch_add(1, Ordering::Relaxed);
            let articles: Vec<_> = (0..30)
                .map(|i| make_test_article("bench_batch", batch, i))
                .collect();
            rt.block_on(async {
                insert_batch(&pool_batch, &articles).await.unwrap();
            })
        })
    });

    group.finish();

    rt.block_on(cleanup_bench_data(&pool));
}

fn bench_batch_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = match rt.block_on(setup_test_pool()) {
        Some(p) => p,
        None => return,
    };

    let mut group = c.benchmark_group("batch_insert_scaling");
    group.sample_size(15);

    for size in [10usize, 30, 50, 100] {
        let source_id = format!("bench_scale_{}", size);
        rt.block_on(ensure_feed_source(&pool, &source_id));

        let counter = AtomicU32::new(0);
        let pool_s = pool.clone();
        let sid = source_id.clone();
        group.bench_with_input(
            BenchmarkId::new("batch_insert", size),
            &size,
            |b, &size| {
                b.iter(|| {
                    let batch = counter.fetch_add(1, Ordering::Relaxed);
                    let articles: Vec<_> = (0..size)
                        .map(|i| make_test_article(&sid, batch, i as u32))
                        .collect();
                    rt.block_on(async {
                        insert_batch(&pool_s, &articles).await.unwrap();
                    })
                })
            },
        );
    }

    group.finish();

    rt.block_on(cleanup_bench_data(&pool));
}

criterion_group!(benches, bench_insert_comparison, bench_batch_scaling);
criterion_main!(benches);
