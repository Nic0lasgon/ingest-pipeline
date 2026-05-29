use crate::utils::shared::decode_html_entities;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use regex::Regex;

#[derive(Debug, Clone, PartialEq)]
pub struct RssFeed {
    pub items: Vec<RssItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RssItem {
    pub title: Option<String>,
    pub link: Option<String>,
    pub description: Option<String>,
    pub image_url: Option<String>,
    pub pub_date: Option<DateTime<Utc>>,
    pub author: Option<String>,
}

pub fn parse_feed(xml_or_json: &str, base_url: &str) -> Result<RssFeed> {
    let trimmed = xml_or_json.trim();
    if trimmed.starts_with('{') {
        parse_json_feed(trimmed)
    } else if trimmed.contains("<feed") {
        parse_atom_feed(trimmed, base_url)
    } else if trimmed.contains("<rss") || trimmed.contains("<channel") {
        parse_rss2_feed(trimmed, base_url)
    } else {
        anyhow::bail!("Unknown feed format: cannot detect RSS, Atom, or JSON Feed")
    }
}

fn parse_rss2_feed(xml: &str, base_url: &str) -> Result<RssFeed> {
    let items_regex = Regex::new(r"(?is)<item(?:\s[^>]*)?>(.*?)</item>")
        .context("Failed to compile item regex")?;

    let mut items = Vec::new();
    for cap in items_regex.captures_iter(xml) {
        let item_xml = &cap[1];
        let item = parse_rss2_item(item_xml, base_url);
        if item.title.is_some() || item.link.is_some() {
            items.push(item);
        }
    }

    Ok(RssFeed { items })
}

fn parse_rss2_item(xml: &str, base_url: &str) -> RssItem {
    let title = extract_tag(xml, "title")
        .map(|s| {
            decode_html_entities(&strip_html(&unwrap_cdata(&s)))
                .trim()
                .to_string()
        })
        .filter(|s| !s.is_empty());
    let link = extract_tag(xml, "link")
        .map(|s| unwrap_cdata(&s).trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|u| normalize_url(&u, base_url));
    let description = extract_tag(xml, "description")
        .map(|s| {
            decode_html_entities(&strip_html(&unwrap_cdata(&s)))
                .trim()
                .to_string()
        })
        .filter(|s| !s.is_empty());
    let pub_date = extract_tag(xml, "pubDate").and_then(|s| parse_date(s.trim()));
    let author = extract_tag(xml, "author")
        .or_else(|| extract_tag(xml, "dc:creator"))
        .map(|s| decode_html_entities(&unwrap_cdata(&s)).trim().to_string())
        .filter(|s| !s.is_empty());

    let image_url = extract_rss_image(xml, base_url);

    RssItem {
        title,
        link,
        description,
        image_url,
        pub_date,
        author,
    }
}

fn extract_rss_image(xml: &str, base_url: &str) -> Option<String> {
    // <media:content url="...">
    let media_content = Regex::new(r#"(?i)<media:content[^>]*\surl\s*=\s*"([^"]+)""#).ok()?;
    if let Some(cap) = media_content.captures(xml) {
        return Some(normalize_url(&cap[1], base_url));
    }

    // <media:thumbnail url="...">
    let media_thumb = Regex::new(r#"(?i)<media:thumbnail[^>]*\surl\s*=\s*"([^"]+)""#).ok()?;
    if let Some(cap) = media_thumb.captures(xml) {
        return Some(normalize_url(&cap[1], base_url));
    }

    // <enclosure type="image/..." url="...">
    let enclosure =
        Regex::new(r#"(?i)<enclosure[^>]*\stype\s*=\s*"image/[^"]*"[^>]*\surl\s*=\s*"([^"]+)""#)
            .ok()?;
    if let Some(cap) = enclosure.captures(xml) {
        return Some(normalize_url(&cap[1], base_url));
    }
    // <enclosure url="..." type="image/...">
    let enclosure2 =
        Regex::new(r#"(?i)<enclosure[^>]*\surl\s*=\s*"([^"]+)"[^>]*\stype\s*=\s*"image/[^"]*""#)
            .ok()?;
    if let Some(cap) = enclosure2.captures(xml) {
        return Some(normalize_url(&cap[1], base_url));
    }

    // Fallback: <img> in description
    if let Some(desc) = extract_tag(xml, "description") {
        let img_regex = Regex::new(r#"(?i)<img[^>]*\ssrc\s*=\s*"([^"]+)""#).ok()?;
        if let Some(cap) = img_regex.captures(&desc) {
            return Some(normalize_url(&cap[1], base_url));
        }
    }

    None
}

fn parse_atom_feed(xml: &str, base_url: &str) -> Result<RssFeed> {
    let entry_regex = Regex::new(r"(?is)<entry(?:\s[^>]*)?>(.*?)</entry>")
        .context("Failed to compile entry regex")?;

    let mut items = Vec::new();
    for cap in entry_regex.captures_iter(xml) {
        let entry_xml = &cap[1];
        let item = parse_atom_entry(entry_xml, base_url);
        if item.title.is_some() || item.link.is_some() {
            items.push(item);
        }
    }

    Ok(RssFeed { items })
}

fn parse_atom_entry(xml: &str, base_url: &str) -> RssItem {
    let title = extract_tag(xml, "title")
        .map(|s| {
            strip_html(&decode_html_entities(&strip_html(&unwrap_cdata(&s))))
                .trim()
                .to_string()
        })
        .filter(|s| !s.is_empty());

    let link = extract_atom_link(xml, base_url);

    let description = extract_tag(xml, "summary")
        .or_else(|| extract_tag(xml, "content"))
        .map(|s| {
            strip_html(&decode_html_entities(&strip_html(&unwrap_cdata(&s))))
                .trim()
                .to_string()
        })
        .filter(|s| !s.is_empty());

    let pub_date = extract_tag(xml, "published")
        .or_else(|| extract_tag(xml, "updated"))
        .and_then(|s| parse_date(s.trim()));

    let author = extract_nested_tag(xml, "author", "name")
        .or_else(|| extract_tag(xml, "dc:creator"))
        .map(|s| decode_html_entities(&unwrap_cdata(&s)).trim().to_string())
        .filter(|s| !s.is_empty());

    let image_url = extract_atom_image(xml, base_url);

    RssItem {
        title,
        link,
        description,
        image_url,
        pub_date,
        author,
    }
}

fn extract_atom_link(xml: &str, base_url: &str) -> Option<String> {
    let link_regex =
        Regex::new(r#"(?i)<link[^>]*\srel\s*=\s*"alternate"[^>]*\shref\s*=\s*"([^"]+)""#).ok()?;
    if let Some(cap) = link_regex.captures(xml) {
        return Some(normalize_url(&cap[1], base_url));
    }
    // Fallback: <link href="..."> without rel
    let link_any = Regex::new(r#"(?i)<link(?:\s[^>]*)?\shref\s*=\s*"([^"]+)""#).ok()?;
    if let Some(cap) = link_any.captures(xml) {
        return Some(normalize_url(&cap[1], base_url));
    }
    None
}

fn extract_atom_image(xml: &str, base_url: &str) -> Option<String> {
    let media_content = Regex::new(r#"(?i)<media:content[^>]*\surl\s*=\s*"([^"]+)""#).ok()?;
    if let Some(cap) = media_content.captures(xml) {
        return Some(normalize_url(&cap[1], base_url));
    }
    let media_thumb = Regex::new(r#"(?i)<media:thumbnail[^>]*\surl\s*=\s*"([^"]+)""#).ok()?;
    if let Some(cap) = media_thumb.captures(xml) {
        return Some(normalize_url(&cap[1], base_url));
    }
    // <content> or <summary> with <img> (possibly entity-encoded)
    let img_regex = Regex::new(r#"(?i)<img[^>]*\ssrc\s*=\s*"([^"]+)""#).ok()?;
    for tag in &["content", "summary"] {
        if let Some(text) = extract_tag(xml, tag) {
            // Try raw content first
            if let Some(cap) = img_regex.captures(&text) {
                return Some(normalize_url(&cap[1], base_url));
            }
            // Try decoded content (entities like &lt;img ...&gt;)
            let decoded = decode_html_entities(&text);
            if let Some(cap) = img_regex.captures(&decoded) {
                return Some(normalize_url(&cap[1], base_url));
            }
        }
    }
    None
}

fn parse_json_feed(json_str: &str) -> Result<RssFeed> {
    let v: serde_json::Value =
        serde_json::from_str(json_str).context("Failed to parse JSON Feed")?;

    let items_arr = v
        .get("items")
        .and_then(|i| i.as_array())
        .context("JSON Feed missing 'items' array")?;

    let mut items = Vec::new();
    for item_val in items_arr {
        let title = item_val
            .get("title")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());

        let link = item_val
            .get("url")
            .or_else(|| item_val.get("external_url"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());

        let description = item_val
            .get("summary")
            .or_else(|| item_val.get("content_text"))
            .and_then(|v| v.as_str())
            .map(|s| strip_html(s).trim().to_string())
            .filter(|s| !s.is_empty());

        let pub_date = item_val
            .get("date_published")
            .and_then(|v| v.as_str())
            .and_then(parse_date);

        let author = item_val
            .get("author")
            .and_then(|a| {
                a.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| a.as_str().map(|s| s.to_string()))
            })
            .filter(|s| !s.is_empty());

        let image_url = item_val
            .get("image")
            .or_else(|| item_val.get("banner_image"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());

        if title.is_some() || link.is_some() {
            items.push(RssItem {
                title,
                link,
                description,
                image_url,
                pub_date,
                author,
            });
        }
    }

    Ok(RssFeed { items })
}

pub fn unwrap_cdata(text: &str) -> String {
    let re = Regex::new(r"<!\[CDATA\[(.*?)\]\]>").unwrap();
    re.replace_all(text, "$1").to_string()
}

pub fn strip_html(text: &str) -> String {
    let block_re = Regex::new(r"(?i)</?(?:p|div|h[1-6]|li)>|<br\s*/?>").unwrap();
    let result = block_re.replace_all(text, "\n").to_string();
    let tag_re = Regex::new(r"<[^>]+>").unwrap();
    tag_re.replace_all(&result, "").to_string()
}

pub fn normalize_url(url: &str, base_url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        return url.to_string();
    }
    if url.starts_with("//") {
        return format!("https:{}", url);
    }
    if base_url.is_empty() {
        return url.to_string();
    }
    if url.starts_with('/') {
        if let Some(pos) = base_url.find("://") {
            let after_scheme = &base_url[pos + 3..];
            if let Some(slash_pos) = after_scheme.find('/') {
                return format!("{}{}", &base_url[..pos + 3 + slash_pos], url);
            } else {
                return format!("{}{}", base_url, url);
            }
        }
    }
    // Relative path
    let base = if base_url.ends_with('/') {
        base_url.to_string()
    } else {
        match base_url.rfind('/') {
            Some(pos) if pos > base_url.find("://").map(|p| p + 2).unwrap_or(0) => {
                base_url[..=pos].to_string()
            }
            _ => format!("{}/", base_url),
        }
    };
    format!("{}{}", base, url)
}

pub fn parse_date(date_str: &str) -> Option<DateTime<Utc>> {
    let s = date_str.trim();
    if s.is_empty() {
        return None;
    }

    // ISO 8601: 2024-01-15T10:30:00Z or with offset
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }

    // RFC 822: "Mon, 15 Jan 2024 10:30:00 +0000" or "Mon, 15 Jan 2024 10:30:00 GMT"
    let rfc822_fixes = s
        .replace("GMT", "+0000")
        .replace("UTC", "+0000")
        .replace("EST", "-0500")
        .replace("EDT", "-0400")
        .replace("CST", "-0600")
        .replace("CDT", "-0500")
        .replace("MST", "-0700")
        .replace("MDT", "-0600")
        .replace("PST", "-0800")
        .replace("PDT", "-0700");

    if let Ok(dt) = DateTime::parse_from_str(&rfc822_fixes, "%a, %d %b %Y %H:%M:%S %z") {
        return Some(dt.with_timezone(&Utc));
    }
    // Without day name: "15 Jan 2024 10:30:00 +0000"
    if let Ok(dt) = DateTime::parse_from_str(&rfc822_fixes, "%d %b %Y %H:%M:%S %z") {
        return Some(dt.with_timezone(&Utc));
    }

    // ISO 8601 without timezone: "2024-01-15T10:30:00"
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc());
    }
    // ISO date only
    if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(dt.and_hms_opt(0, 0, 0).unwrap().and_utc());
    }

    None
}

use chrono::NaiveDate;
use chrono::NaiveDateTime;

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let pattern = format!(
        r"(?is)<{}(?:\s[^>]*)?>(.*?)</{}>",
        regex::escape(tag),
        regex::escape(tag)
    );
    let re = Regex::new(&pattern).ok()?;
    re.captures(xml).map(|cap| cap[1].to_string())
}

fn extract_nested_tag(xml: &str, parent: &str, child: &str) -> Option<String> {
    let parent_content = extract_tag(xml, parent)?;
    extract_tag(&parent_content, child)
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_unwrap_cdata() {
        assert_eq!(unwrap_cdata("<![CDATA[hello]]>"), "hello");
        assert_eq!(unwrap_cdata("no cdata here"), "no cdata here");
        assert_eq!(unwrap_cdata("<![CDATA[<p>html</p>]]>"), "<p>html</p>");
    }

    #[test]
    fn test_decode_entities() {
        assert_eq!(decode_html_entities("&amp;"), "&");
        assert_eq!(decode_html_entities("&lt;b&gt;"), "<b>");
        assert_eq!(decode_html_entities("&#39;"), "'");
        assert_eq!(decode_html_entities("&#x27;"), "'");
        assert_eq!(decode_html_entities("&#65;"), "A");
    }

    #[test]
    fn test_strip_html() {
        assert_eq!(strip_html("<p>Hello <b>world</b></p>"), "\nHello world\n");
        assert_eq!(strip_html("no tags"), "no tags");
        assert_eq!(strip_html("<br>line"), "\nline");
    }

    #[test]
    fn test_normalize_url() {
        assert_eq!(
            normalize_url("https://example.com/img.jpg", ""),
            "https://example.com/img.jpg"
        );
        assert_eq!(
            normalize_url("//cdn.example.com/img.jpg", ""),
            "https://cdn.example.com/img.jpg"
        );
        assert_eq!(
            normalize_url("/img.jpg", "https://example.com/page"),
            "https://example.com/img.jpg"
        );
        assert_eq!(
            normalize_url("img.jpg", "https://example.com/feed"),
            "https://example.com/img.jpg"
        );
        assert_eq!(
            normalize_url("img.jpg", "https://example.com/feed/"),
            "https://example.com/feed/img.jpg"
        );
    }

    #[test]
    fn test_parse_date_iso8601() {
        let dt = parse_date("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(dt.to_rfc3339(), "2024-01-15T10:30:00+00:00");

        let dt2 = parse_date("2024-01-15T10:30:00+02:00").unwrap();
        assert_eq!(dt2.to_rfc3339(), "2024-01-15T08:30:00+00:00");
    }

    #[test]
    fn test_parse_date_rfc822() {
        let dt = parse_date("Mon, 15 Jan 2024 10:30:00 GMT").unwrap();
        assert_eq!(dt.to_rfc3339(), "2024-01-15T10:30:00+00:00");

        let dt2 = parse_date("Mon, 15 Jan 2024 10:30:00 +0100").unwrap();
        assert_eq!(dt2.to_rfc3339(), "2024-01-15T09:30:00+00:00");
    }

    #[test]
    fn test_parse_unknown_format() {
        let result = parse_feed("not a feed", "https://example.com");
        assert!(result.is_err());
    }
}
