use regex::Regex;
use serde::de::DeserializeOwned;
use tracing::info;

#[allow(dead_code)]
pub fn log_metric(name: &str, value: f64, tags: &[(&str, &str)]) {
    let tags_str = tags
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",");
    info!(
        metric_name = name,
        metric_value = value,
        metric_tags = tags_str,
        "metric"
    );
}

pub fn decode_html_entities(text: &str) -> String {
    let entity_re = Regex::new(
        r"&(amp|lt|gt|quot|apos|nbsp|eacute|egrave|ecirc|agrave|acirc|ccedil|ugrave|ucirc|icirc|ocirc|oslash|aelig|euro|laquo|raquo|ldquo|rdquo|hellip|mdash|ndash|rsquo|lsquo);|&#(\d+);|&#x([0-9a-fA-F]+);"
    ).unwrap();

    entity_re
        .replace_all(text, |caps: &regex::Captures| {
            if let Some(name) = caps.get(1) {
                match name.as_str() {
                    "amp" => "&",
                    "lt" => "<",
                    "gt" => ">",
                    "quot" => "\"",
                    "apos" => "'",
                    "nbsp" => "\u{00a0}",
                    "eacute" => "é",
                    "egrave" => "è",
                    "ecirc" => "ê",
                    "agrave" => "à",
                    "acirc" => "â",
                    "ccedil" => "ç",
                    "ugrave" => "ù",
                    "ucirc" => "û",
                    "icirc" => "î",
                    "ocirc" => "ô",
                    "oslash" => "ø",
                    "aelig" => "æ",
                    "euro" => "€",
                    "laquo" => "«",
                    "raquo" => "»",
                    "ldquo" => "\u{201c}",
                    "rdquo" => "\u{201d}",
                    "hellip" => "…",
                    "mdash" => "—",
                    "ndash" => "–",
                    "rsquo" => "'",
                    "lsquo" => "'",
                    _ => return caps[0].to_string(),
                }
                .to_string()
            } else if let Some(dec) = caps.get(2) {
                dec.as_str()
                    .parse::<u32>()
                    .ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| caps[0].to_string())
            } else if let Some(hex) = caps.get(3) {
                u32::from_str_radix(hex.as_str(), 16)
                    .ok()
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| caps[0].to_string())
            } else {
                caps[0].to_string()
            }
        })
        .to_string()
}

pub fn safe_parse_json<T: DeserializeOwned>(json: &str) -> Option<T> {
    serde_json::from_str(json).ok()
}
