use regex::Regex;
use reqwest::redirect::Policy;
use reqwest::Client;
use std::sync::LazyLock;
use std::time::Duration;
use tracing::debug;
use url::Url;

const MAX_DEPTH: u32 = 3;
const TIMEOUT: Duration = Duration::from_secs(5);

const SHORTENER_DOMAINS: &[&str] = &[
    "news.google.com",
    "t.co",
    "bit.ly",
    "tinyurl.com",
    "ow.ly",
    "buff.ly",
    "dlvr.it",
    "rebrand.ly",
    "short.link",
];

static RE_META_REFRESH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)<meta\s+[^>]*http-equiv\s*=\s*["']refresh["'][^>]*content\s*=\s*["'][^"']*url\s*=\s*([^"'\s;>]+)[^"']*["']"#).unwrap()
});

static RE_META_REFRESH_CONTENT_FIRST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)<meta\s+[^>]*content\s*=\s*["'][^"']*url\s*=\s*([^"'\s;>]+)[^"']*["'][^>]*http-equiv\s*=\s*["']refresh["']"#).unwrap()
});

static RE_CANONICAL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)<link\s+[^>]*rel\s*=\s*["']canonical["'][^>]*href\s*=\s*["']([^"']+)["']"#)
        .unwrap()
});

static RE_CANONICAL_HREF_FIRST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)<link\s+[^>]*href\s*=\s*["']([^"']+)["'][^>]*rel\s*=\s*["']canonical["']"#)
        .unwrap()
});

fn is_shortener_domain(url: &str) -> bool {
    if let Ok(parsed) = Url::parse(url) {
        if let Some(host) = parsed.host_str() {
            let host_lower = host.to_lowercase();
            return SHORTENER_DOMAINS.iter().any(|&d| host_lower == d);
        }
    }
    false
}

#[allow(dead_code)]
fn build_resolver_client() -> Client {
    Client::builder()
        .timeout(TIMEOUT)
        .redirect(Policy::limited(10))
        .build()
        .expect("failed to build reqwest client")
}

fn extract_meta_refresh(html: &str) -> Option<String> {
    RE_META_REFRESH
        .captures(html)
        .or_else(|| RE_META_REFRESH_CONTENT_FIRST.captures(html))
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

fn extract_canonical(html: &str) -> Option<String> {
    RE_CANONICAL
        .captures(html)
        .or_else(|| RE_CANONICAL_HREF_FIRST.captures(html))
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

fn resolve_recursive<'a>(
    client: &'a Client,
    url: &'a str,
    depth: u32,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + 'a>> {
    Box::pin(async move { resolve_recursive_inner(client, url, depth).await })
}

async fn resolve_recursive_inner(client: &Client, url: &str, depth: u32) -> Option<String> {
    if depth >= MAX_DEPTH {
        debug!(url, depth, "max recursion depth reached");
        return None;
    }

    // Strategy 1: HEAD request — reqwest follows redirects automatically
    if let Ok(resp) = client.head(url).send().await {
        let final_url = resp.url().to_string();
        if final_url != url {
            debug!(from = url, to = %final_url, "resolved via HEAD redirect");
            return Some(final_url);
        }
    }

    // Strategy 2: GET request
    let resp = match client.get(url).send().await {
        Ok(r) => r,
        Err(e) => {
            debug!(url, error = %e, "GET request failed");
            return None;
        }
    };

    // If reqwest followed redirects, the final URL will differ from the original
    let final_url = resp.url().to_string();
    if final_url != url {
        debug!(from = url, to = %final_url, "resolved via GET redirect");
        return Some(final_url);
    }

    // Non-success status on the original URL (no redirect happened)
    if !resp.status().is_success() {
        debug!(url, status = %resp.status(), "non-success status, no redirect");
        return None;
    }

    // Parse the HTML body for meta refresh / canonical
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            debug!(url, error = %e, "failed to read response body");
            return None;
        }
    };

    // Strategy 3: Meta refresh
    if let Some(refresh_url) = extract_meta_refresh(&body) {
        debug!(from = url, to = %refresh_url, "found meta refresh");
        return Some(
            resolve_recursive(client, &refresh_url, depth + 1)
                .await
                .unwrap_or(refresh_url),
        );
    }

    // Strategy 4: Link canonical
    if let Some(canonical_url) = extract_canonical(&body) {
        debug!(from = url, to = %canonical_url, "found canonical link");
        return Some(canonical_url);
    }

    None
}

pub async fn resolve_source_url(client: &Client, url: &str) -> Option<String> {
    if !is_shortener_domain(url) {
        return None;
    }
    resolve_recursive(client, url, 0).await
}

pub async fn resolve_url_unchecked(client: &Client, url: &str) -> Option<String> {
    resolve_recursive(client, url, 0).await
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_is_shortener_domain_tco() {
        assert!(is_shortener_domain("https://t.co/abc123"));
    }

    #[test]
    fn test_is_shortener_domain_bitly() {
        assert!(is_shortener_domain("https://bit.ly/xyz"));
    }

    #[test]
    fn test_is_shortener_domain_google_news() {
        assert!(is_shortener_domain(
            "https://news.google.com/rss/articles/abc"
        ));
    }

    #[test]
    fn test_is_shortener_domain_all() {
        for domain in SHORTENER_DOMAINS {
            assert!(
                is_shortener_domain(&format!("https://{}/path", domain)),
                "expected {} to be recognized as shortener",
                domain
            );
        }
    }

    #[test]
    fn test_is_not_shortener() {
        assert!(!is_shortener_domain("https://example.com/article"));
        assert!(!is_shortener_domain("https://www.lemonde.fr/politique/"));
        assert!(!is_shortener_domain("https://github.com/rust-lang/rust"));
    }

    #[test]
    fn test_is_not_shortener_invalid_url() {
        assert!(!is_shortener_domain("not a url"));
        assert!(!is_shortener_domain(""));
    }

    #[test]
    fn test_extract_meta_refresh_basic() {
        let html = r#"<html><head><meta http-equiv="refresh" content="0; url=https://example.com/article"></head></html>"#;
        assert_eq!(
            extract_meta_refresh(html),
            Some("https://example.com/article".to_string())
        );
    }

    #[test]
    fn test_extract_meta_refresh_no_space_after_semicolon() {
        let html = r#"<meta http-equiv="refresh" content="0;url=https://example.com/page">"#;
        assert_eq!(
            extract_meta_refresh(html),
            Some("https://example.com/page".to_string())
        );
    }

    #[test]
    fn test_extract_meta_refresh_content_first() {
        let html = r#"<meta content="0; url=https://example.com/redirect" http-equiv="refresh">"#;
        assert_eq!(
            extract_meta_refresh(html),
            Some("https://example.com/redirect".to_string())
        );
    }

    #[test]
    fn test_extract_meta_refresh_none() {
        let html = r#"<html><head><meta charset="utf-8"></head></html>"#;
        assert_eq!(extract_meta_refresh(html), None);
    }

    #[test]
    fn test_extract_meta_refresh_different_delays() {
        let html = r#"<meta http-equiv="refresh" content="5; url=https://example.com/slow">"#;
        assert_eq!(
            extract_meta_refresh(html),
            Some("https://example.com/slow".to_string())
        );
    }

    #[test]
    fn test_extract_canonical_basic() {
        let html = r#"<link rel="canonical" href="https://example.com/canonical">"#;
        assert_eq!(
            extract_canonical(html),
            Some("https://example.com/canonical".to_string())
        );
    }

    #[test]
    fn test_extract_canonical_href_first() {
        let html = r#"<link href="https://example.com/canonical" rel="canonical">"#;
        assert_eq!(
            extract_canonical(html),
            Some("https://example.com/canonical".to_string())
        );
    }

    #[test]
    fn test_extract_canonical_none() {
        let html = r#"<html><head><meta charset="utf-8"></head></html>"#;
        assert_eq!(extract_canonical(html), None);
    }
}
