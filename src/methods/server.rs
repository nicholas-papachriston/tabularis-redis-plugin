use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

/// Parse Redis INFO output into a flat JSON object. Section headers (# ...) are skipped.
/// Numeric values are stored as numbers when possible.
fn parse_info_to_json(info: &str) -> serde_json::Map<String, JsonValue> {
    let mut out = serde_json::Map::new();
    for line in info.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_string();
            let value = value.trim();
            #[allow(clippy::option_if_let_else)]
            let json_val = if let Ok(n) = value.parse::<i64>() {
                JsonValue::Number(serde_json::Number::from(n))
            } else if let Ok(n) = value.parse::<u64>() {
                JsonValue::Number(serde_json::Number::from(n))
            } else if let Ok(f) = value.parse::<f64>() {
                JsonValue::Number(
                    serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)),
                )
            } else {
                JsonValue::String(value.to_string())
            };
            out.insert(key, json_val);
        }
    }
    out
}

/// Returns Redis server info from INFO ALL as structured JSON.
pub fn get_server_info(client: &mut RedisClient) -> Result<JsonValue, String> {
    let info = client.info(Some("all"))?;
    let obj = parse_info_to_json(&info);
    Ok(JsonValue::Object(obj))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_info_to_json_skips_comments_and_empty() {
        let info = "\n# Server\nredis_version:7.0.0\n\n";
        let out = parse_info_to_json(info);
        assert_eq!(
            out.get("redis_version").and_then(|v| v.as_str()),
            Some("7.0.0")
        );
        assert!(!out.contains_key("#"));
    }

    #[test]
    fn parse_info_to_json_parses_numbers() {
        let info = "connected_clients:2\nused_memory:1000\n";
        let out = parse_info_to_json(info);
        assert_eq!(
            out.get("connected_clients").and_then(|v| v.as_i64()),
            Some(2)
        );
        assert_eq!(out.get("used_memory").and_then(|v| v.as_i64()), Some(1000));
    }
}
