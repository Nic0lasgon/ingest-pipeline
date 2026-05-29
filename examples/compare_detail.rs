use ingest_pipeline::utils::text_extract::{clean_with_trafilatura, extract_text};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let samples = [
        ("TestingCatalog", "https://www.testingcatalog.com/anthropic-launches-claude-opus-4-8-and-new-effort-selector/"),
        ("TestingCatalog", "https://www.testingcatalog.com/anthropic-to-introduce-personal-ai-fluency-scorecard-in-claude/"),
        ("The Decoder", "https://the-decoder.com/anthropic-ships-claude-opus-4-8-as-a-modest-but-tangible-improvement-that-tops-gpt-5-5-in-most-benchmarks/"),
        ("The Decoder", "https://the-decoder.com/new-review-paper-argues-code-is-how-ai-agents-think-and-act-not-just-what-they-produce/"),
    ];

    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    for (source, url) in &samples {
        println!("\n{}", "=".repeat(100));
        println!("SOURCE: {source}");
        println!("URL:    {url}");
        println!("{}", "=".repeat(100));

        let html = client.get(*url).send().await?.text().await?;
        let regex_r = extract_text(&html);
        let traf_r = clean_with_trafilatura(&html, None);

        println!("\n--- REGEX ({}) ---\n", regex_r.text.len());
        for (i, line) in regex_r.text.lines().take(30).enumerate() {
            println!("{i:03} | {}", line);
        }
        println!("... ({} lignes total)", regex_r.text.lines().count());

        if let Some(ref t) = traf_r {
            println!("\n--- TRAFILATURA ({}) ---\n", t.text.len());
            for (i, line) in t.text.lines().take(30).enumerate() {
                println!("{i:03} | {}", line);
            }
            println!("... ({} lignes total)", t.text.lines().count());
        } else {
            println!("\n--- TRAFILATURA: ECHEC ---");
        }

        println!("\n{} done. Appuie sur Entree pour le suivant...", source);
    }

    Ok(())
}
