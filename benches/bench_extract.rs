use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use ingest_pipeline::utils::text_extract::extract_text;

const ARTICLE_CLEAN: &str = include_str!("../tests/fixtures/html/article_clean.html");
const ARTICLE_NOISY: &str = include_str!("../tests/fixtures/html/article_noisy.html");
const ARTICLE_SHORT: &str = include_str!("../tests/fixtures/html/article_short.html");

fn generate_large_html(paragraphs: usize) -> String {
    let mut html = String::from(
        "<!DOCTYPE html><html><head><title>Large Article</title></head><body><article>",
    );
    for i in 0..paragraphs {
        html.push_str(&format!(
            "<p>Paragraph number {} of the large article. \
             This contains enough text to be meaningful for extraction benchmarking. \
             The quick brown fox jumps over the lazy dog. \
             Quantum computing represents a paradigm shift in computational power. \
             Researchers at leading institutions have recently achieved a breakthrough.</p>",
            i
        ));
    }
    html.push_str("</article></body></html>");
    html
}

fn bench_extract_real_fixtures(c: &mut Criterion) {
    let mut group = c.benchmark_group("extract_text");

    group.bench_function("article_clean", |b| b.iter(|| extract_text(ARTICLE_CLEAN)));

    group.bench_function("article_noisy", |b| b.iter(|| extract_text(ARTICLE_NOISY)));

    group.bench_function("article_short", |b| b.iter(|| extract_text(ARTICLE_SHORT)));

    group.finish();
}

fn bench_extract_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("extract_text_scaling");
    group.sample_size(20);

    for paragraphs in [10usize, 50, 100, 500] {
        let html = generate_large_html(paragraphs);

        group.bench_with_input(
            BenchmarkId::new("paragraphs", paragraphs),
            &paragraphs,
            |b, _| {
                b.iter(|| extract_text(&html));
            },
        );
    }

    group.finish();
}

fn bench_extract_with_scripts(c: &mut Criterion) {
    let html_with_scripts = r#"<!DOCTYPE html>
<html><head><title>Test</title>
<script>var x=1;function foo(){return x+2;}</script>
<style>body{margin:0;padding:0;}.nav{display:flex;}</style>
</head><body>
<nav><a href="/">Home</a><a href="/about">About</a></nav>
<main><article>
<h1>Real Article Title Here</h1>
<p>First paragraph with actual content about technology and innovation in the field of computing.</p>
<p>Second paragraph discussing the implications of recent developments in artificial intelligence research.</p>
<p>Third paragraph covering economic impact and market analysis of emerging tech companies worldwide.</p>
</article></main>
<aside><div class="ad"><img src="/ad.jpg">Buy now!</div></aside>
<footer><p>Copyright 2024</p></footer>
<script>console.log("tracking");fetch("/api/track",{method:"POST"});</script>
</body></html>"#;

    let mut group = c.benchmark_group("extract_text_realistic");
    group.bench_function("page_with_scripts_styles_nav", |b| {
        b.iter(|| extract_text(html_with_scripts))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_extract_real_fixtures,
    bench_extract_scaling,
    bench_extract_with_scripts
);
criterion_main!(benches);
