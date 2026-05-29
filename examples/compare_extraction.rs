use std::time::Instant;

use ingest_pipeline::utils::text_extract::{clean_with_trafilatura, extract_text};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let feeds = [
        ("TestingCatalog", "https://www.testingcatalog.com/rss/"),
        ("The Decoder", "https://the-decoder.com/feed/"),
    ];

    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    let mut total_regex_chars: usize = 0;
    let mut total_traf_chars: usize = 0;
    let mut total_regex_words: usize = 0;
    let mut total_traf_words: usize = 0;
    let mut success_both: usize = 0;
    let mut success_regex_only: usize = 0;
    let mut success_traf_only: usize = 0;
    let mut neither: usize = 0;
    let mut processed: usize = 0;
    let mut noisier_regex: usize = 0;

    let re_link = regex::Regex::new(r"<link>\s*(?:<!\[CDATA\[)?\s*(https?://[^<\s\]]+)").unwrap();

    for (feed_name, feed_url) in &feeds {
        println!("\n{}", "=".repeat(80));
        println!("Feed: {feed_name} ({feed_url})");
        println!("{}", "=".repeat(80));

        let resp = client.get(*feed_url).send().await?;
        let body = resp.text().await?;

        let urls: Vec<String> = re_link
            .captures_iter(&body)
            .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
            .filter(|u| u != *feed_url && u.len() > 30)
            .take(30)
            .collect();

        println!("Found {} URLs\n", urls.len());

        for (i, url) in urls.iter().enumerate() {
            print!("[{i:02}/{}] {url} ... ", urls.len());

            let html = match client.get(url).send().await {
                Ok(r) => match r.text().await {
                    Ok(t) => t,
                    Err(e) => {
                        println!("ERR body: {e}");
                        continue;
                    }
                },
                Err(e) => {
                    println!("ERR fetch: {e}");
                    continue;
                }
            };

            let start = Instant::now();
            let regex_result = extract_text(&html);
            let regex_ms = start.elapsed().as_millis();

            let start = Instant::now();
            let traf_result = clean_with_trafilatura(&html, None);
            let traf_ms = start.elapsed().as_millis();

            let regex_words = count_words(&regex_result.text);
            let traf_words = traf_result
                .as_ref()
                .map(|r| count_words(&r.text))
                .unwrap_or(0);

            let has_regex = regex_words > 50;
            let has_traf = traf_words > 50;

            match (has_regex, has_traf) {
                (true, true) => {
                    success_both += 1;
                    total_regex_chars += regex_result.text.len();
                    total_traf_chars += traf_result.as_ref().unwrap().text.len();
                    total_regex_words += regex_words;
                    total_traf_words += traf_words;

                    let noise_markers = [
                        "Save",
                        "Share",
                        "Cookie",
                        "Subscribe",
                        "Sign up",
                        "Newsletter",
                        "Read more",
                        "Click here",
                        "Follow us",
                        "Comments",
                        "Related",
                        "Advertisement",
                        "Sponsored",
                    ];
                    let regex_noise = noise_markers
                        .iter()
                        .filter(|m| regex_result.text.contains(*m))
                        .count();
                    let traf_noise = traf_result
                        .as_ref()
                        .map(|r| noise_markers.iter().filter(|m| r.text.contains(*m)).count())
                        .unwrap_or(0);
                    if regex_noise > traf_noise {
                        noisier_regex += 1;
                    }

                    println!(
                        "OK regex={}w/{}ms traf={}w/{}ms noise={}/{}",
                        regex_words, regex_ms, traf_words, traf_ms, regex_noise, traf_noise
                    );
                }
                (true, false) => {
                    success_regex_only += 1;
                    println!("OK regex={}w traf=FAILED", regex_words);
                }
                (false, true) => {
                    success_traf_only += 1;
                    println!("OK regex=EMPTY traf={}w", traf_words);
                }
                (false, false) => {
                    neither += 1;
                    println!("BOTH FAILED");
                }
            }

            processed += 1;

            if processed <= 5 || (has_regex && has_traf && processed <= 8) {
                println!("\n  --- REGEX (first 300 chars) ---");
                let preview = &regex_result.text.chars().take(300).collect::<String>();
                for line in preview.lines().take(8) {
                    println!("  | {}", line.trim());
                }
                println!("  --- TRAFILATURA (first 300 chars) ---");
                if let Some(ref t) = traf_result {
                    let preview = &t.text.chars().take(300).collect::<String>();
                    for line in preview.lines().take(8) {
                        println!("  | {}", line.trim());
                    }
                }
                println!();
            }
        }
    }

    println!("\n{}", "=".repeat(80));
    println!("COMPARISON SUMMARY");
    println!("{}", "=".repeat(80));
    println!("Processed:           {processed}");
    println!("Both succeeded:      {success_both}");
    println!("Regex only:          {success_regex_only}");
    println!("Trafilatura only:    {success_traf_only}");
    println!("Neither:             {neither}");
    println!();
    if success_both > 0 {
        println!(
            "Avg words (regex):       {}",
            total_regex_words / success_both
        );
        println!(
            "Avg words (trafilatura): {}",
            total_traf_words / success_both
        );
        println!(
            "Avg chars (regex):       {}",
            total_regex_chars / success_both
        );
        println!(
            "Avg chars (trafilatura): {}",
            total_traf_chars / success_both
        );
        println!(
            "Regex noisier:           {}/{}",
            noisier_regex, success_both
        );
    }

    Ok(())
}

fn count_words(text: &str) -> usize {
    text.split_whitespace().count()
}
