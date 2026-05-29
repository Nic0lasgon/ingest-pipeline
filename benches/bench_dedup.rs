use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use ingest_pipeline::utils::dedup::{check_duplicate, jaccard_similarity, normalize_url};

fn generate_corpus(n: usize) -> Vec<(String, String, Option<String>, String)> {
    (0..n)
        .map(|i| {
            let title = format!(
                "Article {} : {} en France selon les experts",
                i,
                match i % 5 {
                    0 => "Politique : nouveau projet de loi presente",
                    1 => "Economie : croissance soutenue au premier trimestre",
                    2 => "Technologie : innovation majeure dans intelligence artificielle",
                    3 => "Science : decouverte importante en physique quantique",
                    _ => "Sport : victoire historique en championnat",
                }
            );
            let canonical = if i % 3 == 0 {
                Some(format!("https://news.example.com/canonical/{}", i))
            } else {
                None
            };
            (
                format!("id-{}", i),
                format!("https://news.example.com/article/{}", i),
                canonical,
                title,
            )
        })
        .collect()
}

fn bench_check_duplicate(c: &mut Criterion) {
    let mut group = c.benchmark_group("dedup_check_duplicate");

    for size in [10usize, 50, 100, 500, 1000] {
        let corpus = generate_corpus(size);

        group.bench_with_input(
            BenchmarkId::new("no_match_full_scan", size),
            &size,
            |b, _| {
                let new_url = "https://other-site.com/article/new";
                let new_title = "Completely Different Topic Not In Corpus";
                b.iter(|| check_duplicate(new_url, None, new_title, &corpus))
            },
        );

        let corpus_for_match = corpus.clone();
        group.bench_with_input(
            BenchmarkId::new("url_match_first_item", size),
            &size,
            |b, _| {
                let target_url = &corpus_for_match[0].1;
                b.iter(|| check_duplicate(target_url, None, "Any title", &corpus_for_match))
            },
        );
    }

    group.finish();
}

fn bench_jaccard(c: &mut Criterion) {
    let mut group = c.benchmark_group("dedup_jaccard");

    let identical = (
        "Breaking News Major Event Happens Today",
        "Breaking News Major Event Happens Today",
    );
    let similar = (
        "Breaking News Major Event Happens Today Around World Crisis Summit",
        "Breaking News Major Event Happens Today Around World Crisis Meeting",
    );
    let different = (
        "Weather Forecast Sunny Skies Ahead Tomorrow Morning",
        "Sports Team Wins Championship Final Match Result",
    );

    group.bench_function("identical_titles", |b| {
        b.iter(|| jaccard_similarity(identical.0, identical.1))
    });

    group.bench_function("similar_titles", |b| {
        b.iter(|| jaccard_similarity(similar.0, similar.1))
    });

    group.bench_function("different_titles", |b| {
        b.iter(|| jaccard_similarity(different.0, different.1))
    });

    group.finish();
}

fn bench_normalize_url(c: &mut Criterion) {
    let mut group = c.benchmark_group("dedup_normalize_url");

    let urls = [
        "https://www.example.com/path/to/article?utm_source=twitter&ref=feed",
        "HTTPS://EXAMPLE.COM/Path/",
        "//cdn.example.com/resource?v=1#section",
        "https://sub.domain.example.com/deeply/nested/path/to/content/page",
    ];

    for url in urls {
        group.bench_with_input(BenchmarkId::new("normalize", url), &url, |b, url| {
            b.iter(|| normalize_url(url))
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_check_duplicate,
    bench_jaccard,
    bench_normalize_url
);
criterion_main!(benches);
