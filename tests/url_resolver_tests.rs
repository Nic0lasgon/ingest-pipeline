use httpmock::Method;
use httpmock::MockServer;
use ingest_pipeline::utils::url_resolver::{resolve_source_url, resolve_url_unchecked};

#[tokio::test]
async fn test_resolve_tco_redirect() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(Method::HEAD).path("/abc123");
        then.status(301)
            .header("Location", "https://example.com/article");
    });

    let result = resolve_url_unchecked(&format!("{}/abc123", server.base_url())).await;
    assert_eq!(result, Some("https://example.com/article".to_string()));
    mock.assert();
}

#[tokio::test]
async fn test_resolve_bitly_redirect() {
    let server = MockServer::start();

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/xyz789");
        then.status(302)
            .header("Location", format!("{}/dest", server.base_url()));
    });

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/dest");
        then.status(200);
    });
    server.mock(|when, then| {
        when.method(Method::GET).path("/dest");
        then.status(200).body("<html><body>Final</body></html>");
    });

    let result = resolve_url_unchecked(&format!("{}/xyz789", server.base_url())).await;
    assert_eq!(result, Some(format!("{}/dest", server.base_url())));
}

#[tokio::test]
async fn test_resolve_google_news_meta_refresh() {
    let server = MockServer::start();
    let html = r#"<!DOCTYPE html>
<html>
<head>
<meta http-equiv="refresh" content="0; url=https://www.lemonde.fr/politique/article/2024/01/01/title_123.html">
</head>
<body>Redirecting...</body>
</html>"#;

    let head_mock = server.mock(|when, then| {
        when.method(Method::HEAD).path("/rss/articles/CBminQ");
        then.status(200);
    });

    let get_mock = server.mock(|when, then| {
        when.method(Method::GET).path("/rss/articles/CBminQ");
        then.status(200).body(html);
    });

    let result = resolve_url_unchecked(&format!("{}/rss/articles/CBminQ", server.base_url())).await;
    assert_eq!(
        result,
        Some("https://www.lemonde.fr/politique/article/2024/01/01/title_123.html".to_string())
    );
    head_mock.assert();
    get_mock.assert();
}

#[tokio::test]
async fn test_resolve_meta_refresh_content_first() {
    let server = MockServer::start();
    let html =
        r#"<meta content="0;url=https://example.com/redirect" http-equiv="refresh"><p>Go</p>"#;

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/r");
        then.status(200);
    });
    server.mock(|when, then| {
        when.method(Method::GET).path("/r");
        then.status(200).body(html);
    });

    let result = resolve_url_unchecked(&format!("{}/r", server.base_url())).await;
    assert_eq!(result, Some("https://example.com/redirect".to_string()));
}

#[tokio::test]
async fn test_resolve_canonical() {
    let server = MockServer::start();
    let html = r#"<!DOCTYPE html>
<html>
<head>
<link rel="canonical" href="https://www.example.com/full-article-title">
<title>Article</title>
</head>
<body>Content</body>
</html>"#;

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/short");
        then.status(200);
    });
    server.mock(|when, then| {
        when.method(Method::GET).path("/short");
        then.status(200).body(html);
    });

    let result = resolve_url_unchecked(&format!("{}/short", server.base_url())).await;
    assert_eq!(
        result,
        Some("https://www.example.com/full-article-title".to_string())
    );
}

#[tokio::test]
async fn test_resolve_canonical_href_first() {
    let server = MockServer::start();
    let html = r#"<link href="https://example.com/canonical-page" rel="canonical"><p>Body</p>"#;

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/c");
        then.status(200);
    });
    server.mock(|when, then| {
        when.method(Method::GET).path("/c");
        then.status(200).body(html);
    });

    let result = resolve_url_unchecked(&format!("{}/c", server.base_url())).await;
    assert_eq!(
        result,
        Some("https://example.com/canonical-page".to_string())
    );
}

#[tokio::test]
async fn test_resolve_no_redirect_for_normal_domain() {
    let result = resolve_source_url("https://example.com/article").await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_resolve_no_redirect_for_lemonde() {
    let result = resolve_source_url("https://www.lemonde.fr/politique/article.html").await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_resolve_timeout_returns_none() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(Method::HEAD).path("/timeout");
        then.status(200).delay(std::time::Duration::from_secs(10));
    });

    let result = resolve_url_unchecked(&format!("{}/timeout", server.base_url())).await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_resolve_404_returns_none() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(Method::HEAD).path("/notfound");
        then.status(404);
    });
    server.mock(|when, then| {
        when.method(Method::GET).path("/notfound");
        then.status(404);
    });

    let result = resolve_url_unchecked(&format!("{}/notfound", server.base_url())).await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_resolve_500_returns_none() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(Method::HEAD).path("/error");
        then.status(500);
    });
    server.mock(|when, then| {
        when.method(Method::GET).path("/error");
        then.status(500);
    });

    let result = resolve_url_unchecked(&format!("{}/error", server.base_url())).await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_resolve_head_fails_get_redirect_works() {
    let server = MockServer::start();

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/article");
        then.status(405);
    });

    server.mock(|when, then| {
        when.method(Method::GET).path("/article");
        then.status(301)
            .header("Location", "https://example.com/final");
    });

    let result = resolve_url_unchecked(&format!("{}/article", server.base_url())).await;
    assert_eq!(result, Some("https://example.com/final".to_string()));
}

#[tokio::test]
async fn test_resolve_chained_redirect() {
    let server = MockServer::start();

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/step1");
        then.status(301)
            .header("Location", format!("{}/step2", server.base_url()));
    });

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/step2");
        then.status(302)
            .header("Location", "https://example.com/final");
    });

    let result = resolve_url_unchecked(&format!("{}/step1", server.base_url())).await;
    assert_eq!(result, Some("https://example.com/final".to_string()));
}

#[tokio::test]
async fn test_resolve_head_no_redirect_get_no_redirect_no_content() {
    let server = MockServer::start();

    server.mock(|when, then| {
        when.method(Method::HEAD).path("/plain");
        then.status(200);
    });
    server.mock(|when, then| {
        when.method(Method::GET).path("/plain");
        then.status(200)
            .body("<html><body>No redirect info</body></html>");
    });

    let result = resolve_url_unchecked(&format!("{}/plain", server.base_url())).await;
    assert_eq!(result, None);
}
