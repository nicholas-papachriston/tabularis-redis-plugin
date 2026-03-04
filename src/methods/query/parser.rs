use beef::Cow;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

pub fn json_value_to_cmp_str(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => String::new(),
        _ => v.to_string(),
    }
}

/// Type-aware comparison for ORDER BY: numbers by value, bools (false < true), nulls first, then strings lexically.
pub fn json_value_cmp(a: &JsonValue, b: &JsonValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Null, _) => Ordering::Less,
        (_, JsonValue::Null) => Ordering::Greater,
        (JsonValue::Number(na), JsonValue::Number(nb)) => {
            let fa = na.as_f64().unwrap_or(f64::NAN);
            let fb = nb.as_f64().unwrap_or(f64::NAN);
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }
        (JsonValue::Number(_), _) => Ordering::Less,
        (_, JsonValue::Number(_)) => Ordering::Greater,
        (JsonValue::Bool(x), JsonValue::Bool(y)) => x.cmp(y),
        (JsonValue::Bool(_), _) => Ordering::Less,
        (_, JsonValue::Bool(_)) => Ordering::Greater,
        (JsonValue::String(sa), JsonValue::String(sb)) => sa.cmp(sb),
        (JsonValue::String(_), _) => Ordering::Less,
        (_, JsonValue::String(_)) => Ordering::Greater,
        _ => json_value_to_cmp_str(a).cmp(&json_value_to_cmp_str(b)),
    }
}

/// Build row values for columns; when column is "_key", use pk.
pub fn row_values_for_columns(
    columns: &[String],
    pk: &str,
    hash_map: Option<&HashMap<String, String>>,
    json_obj: Option<&serde_json::Map<String, JsonValue>>,
) -> Vec<JsonValue> {
    columns
        .iter()
        .map(|c| {
            if c == "_key" {
                return JsonValue::String(pk.to_string());
            }
            if let Some(map) = hash_map {
                return map
                    .get(c)
                    .cloned()
                    .map_or(JsonValue::Null, JsonValue::String);
            }
            if let Some(obj) = json_obj {
                return obj.get(c).cloned().unwrap_or(JsonValue::Null);
            }
            JsonValue::Null
        })
        .collect()
}

/// Extract the first quoted value from a segment (stops at closing quote).
fn first_quoted_value(rest: &str) -> &str {
    let rest = rest.trim();
    let Some(quote) = rest.chars().next() else {
        return rest;
    };
    if quote != '\'' && quote != '"' {
        return rest.split_whitespace().next().unwrap_or(rest);
    }
    let rest = rest[1..].trim_start();
    rest.find(quote).map_or(rest, |end| &rest[..end])
}

/// Parse a single segment (e.g. "key = 'x'" or "key LIKE 'u%'") for key pattern. Returns Redis glob.
fn parse_redis_keys_key_segment(segment: &str) -> Option<String> {
    let segment = segment.trim();
    let seg_upper = segment.to_uppercase();
    if seg_upper.contains(" LIKE ") {
        let like_idx = seg_upper.find(" LIKE ")?;
        let left = segment[..like_idx].trim().trim_matches('"').to_lowercase();
        if left != "key" {
            return None;
        }
        let after_like = segment[segment.len().min(like_idx + 6)..].trim();
        let right = first_quoted_value(after_like);
        if right.is_empty() {
            return None;
        }
        if right.starts_with('%') && right.ends_with('%') && right.len() > 2 {
            return Some(format!("*{}*", &right[1..right.len() - 1]));
        }
        if right.ends_with('%') && right.len() > 1 {
            return Some(format!("{}*", right.trim_end_matches('%')));
        }
        return Some(format!("*{right}*"));
    }
    if segment.contains(" = ") {
        let eq_idx = segment.find(" = ")?;
        let left = segment[..eq_idx].trim().trim_matches('"').to_lowercase();
        if left != "key" {
            return None;
        }
        let after_eq = segment[eq_idx + 3..].trim();
        let right = first_quoted_value(after_eq);
        if right.is_empty() {
            return None;
        }
        return Some(format!("{right}*"));
    }
    None
}

/// Parse WHERE key = 'x' or key LIKE 'x%' / '%x%' from query for __`redis_keys`__. Supports AND: each segment is tried. Returns Redis glob pattern.
pub fn parse_redis_keys_where(upper: &str, q: &str) -> Option<String> {
    let after_where = upper.find(" WHERE ")?;
    let clause = q[after_where + 7..].trim();
    let clause_upper = clause.to_uppercase();
    let end = clause_upper.find(" ORDER BY ").unwrap_or(clause.len());
    let clause = clause[..end.min(clause.len())].trim();
    if clause.is_empty() {
        return None;
    }
    let segments = split_where_by_and(clause);
    for segment in segments {
        if let Some(pattern) = parse_redis_keys_key_segment(segment) {
            return Some(pattern);
        }
    }
    None
}

/// Split WHERE clause by " AND " (case-insensitive), returning trimmed segments.
fn split_where_by_and(clause: &str) -> Vec<&str> {
    let clause_upper = clause.to_uppercase();
    let and = " AND ";
    let mut out = Vec::new();
    let mut start = 0;
    while let Some(rel_pos) = clause_upper[start..].find(and) {
        let pos = start + rel_pos;
        let seg = clause[start..pos].trim();
        if !seg.is_empty() {
            out.push(seg);
        }
        start = pos + and.len();
    }
    let seg = clause[start..].trim();
    if !seg.is_empty() {
        out.push(seg);
    }
    out
}

/// Parse a single segment for type filter: "type = 'hash'" or "type != 'string'".
fn parse_redis_keys_type_segment(segment: &str) -> Option<(String, bool)> {
    let segment = segment.trim();
    let seg_upper = segment.to_uppercase();
    let (op_len, negate) = if seg_upper.starts_with("TYPE != ") {
        ("TYPE != ".len(), true)
    } else if seg_upper.starts_with("TYPE = ") {
        ("TYPE = ".len(), false)
    } else {
        return None;
    };
    let rest = segment[op_len..].trim();
    let value = first_quoted_value(rest).to_lowercase();
    if value.is_empty() {
        return None;
    }
    Some((value, negate))
}

/// Parse WHERE type = 'hash' or type != 'string' for __`redis_keys`__. Supports AND. Returns (`type_value`, negate).
pub fn parse_redis_keys_type_filter(upper: &str, q: &str) -> Option<(String, bool)> {
    let after_where = upper.find(" WHERE ")?;
    let clause = q[after_where + 7..].trim();
    let clause_upper = clause.to_uppercase();
    let end = clause_upper.find(" ORDER BY ").unwrap_or(clause.len());
    let clause = clause[..end.min(clause.len())].trim();
    for segment in split_where_by_and(clause) {
        if let Some(t) = parse_redis_keys_type_segment(segment) {
            return Some(t);
        }
    }
    None
}

/// How to match the value preview for __`redis_keys`__ WHERE value ... filters.
#[derive(Debug, Clone)]
pub enum RedisValueFilterKind<'a> {
    Exact(Cow<'a, str>),
    StartsWith(Cow<'a, str>),
    Contains(Cow<'a, str>),
}

/// Parse a single segment for value filter: "value = 'x'" or "value LIKE 'x%'".
fn parse_redis_keys_value_segment(segment: &str) -> Option<RedisValueFilterKind<'_>> {
    let segment = segment.trim();
    let seg_upper = segment.to_uppercase();
    let (op_len, is_like) = if seg_upper.starts_with("VALUE LIKE ") {
        ("VALUE LIKE ".len(), true)
    } else if seg_upper.starts_with("VALUE = ") {
        ("VALUE = ".len(), false)
    } else {
        return None;
    };
    let rest = segment[op_len..].trim();
    let right = first_quoted_value(rest);
    if right.is_empty() {
        return None;
    }
    let kind = if is_like {
        if right.starts_with('%') && right.ends_with('%') && right.len() > 2 {
            RedisValueFilterKind::Contains(Cow::borrowed(&right[1..right.len() - 1]))
        } else if right.ends_with('%') && right.len() > 1 {
            RedisValueFilterKind::StartsWith(Cow::borrowed(right.trim_end_matches('%')))
        } else if right.starts_with('%') && right.len() > 1 {
            RedisValueFilterKind::Contains(Cow::borrowed(&right[1..]))
        } else {
            RedisValueFilterKind::StartsWith(Cow::borrowed(right))
        }
    } else {
        RedisValueFilterKind::Exact(Cow::borrowed(right))
    };
    Some(kind)
}

/// Parse WHERE value = 'x' or value LIKE 'x%' / '%x%' for __`redis_keys`__. Supports AND. Matches against the value preview.
pub fn parse_redis_keys_value_filter<'a>(
    upper: &str,
    q: &'a str,
) -> Option<RedisValueFilterKind<'a>> {
    let after_where = upper.find(" WHERE ")?;
    let clause = q[after_where + 7..].trim();
    let clause_upper = clause.to_uppercase();
    let end = clause_upper
        .find(" ORDER BY ")
        .unwrap_or(clause_upper.len())
        .min(clause_upper.find(" LIMIT ").unwrap_or(clause_upper.len()));
    let clause = clause[..end.min(clause.len())].trim();
    for segment in split_where_by_and(clause) {
        if let Some(v) = parse_redis_keys_value_segment(segment) {
            return Some(v);
        }
    }
    None
}

/// Parse ORDER BY key/type/value [ASC|DESC] for __`redis_keys`__. Returns (column "key"|"type"|"value", descending).
pub fn parse_redis_keys_order_by(upper: &str, q: &str) -> Option<(&'static str, bool)> {
    let order_pos = upper.find(" ORDER BY ")?;
    let clause = q[order_pos + 10..].trim();
    let end = clause
        .to_uppercase()
        .find(" LIMIT ")
        .unwrap_or(clause.len());
    let clause = clause[..end.min(clause.len())].trim();
    let mut tokens = clause.split_whitespace();
    let col = tokens.next()?.trim_matches('"').to_lowercase();
    let desc = tokens.next().is_some_and(|s| s.to_uppercase() == "DESC");
    if col == "key" {
        Some(("key", desc))
    } else if col == "type" {
        Some(("type", desc))
    } else if col == "value" {
        Some(("value", desc))
    } else {
        None
    }
}

/// Parse LIMIT n from query.
pub fn parse_limit(upper: &str, q: &str) -> Option<u64> {
    let limit_pos = upper.find(" LIMIT ")?;
    let rest = q[limit_pos + 7..].trim();
    let num: u64 = rest.split_whitespace().next()?.parse().ok()?;
    Some(num)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_limit_num() {
        let q = "SELECT * FROM t LIMIT 100";
        let u = q.to_uppercase();
        assert_eq!(super::parse_limit(&u, q), Some(100));
    }

    #[test]
    fn test_parse_redis_keys_where_key_eq() {
        let q = "SELECT * FROM __redis_keys__ WHERE key = 'batch'";
        let u = q.to_uppercase();
        assert_eq!(
            super::parse_redis_keys_where(&u, q),
            Some("batch*".to_string())
        );
    }

    #[test]
    fn test_parse_redis_keys_where_key_like() {
        let q = "SELECT * FROM __redis_keys__ WHERE key LIKE 'stress%'";
        let u = q.to_uppercase();
        assert_eq!(
            super::parse_redis_keys_where(&u, q),
            Some("stress*".to_string())
        );
        let q2 = "SELECT * FROM __redis_keys__ WHERE key LIKE '%mid%'";
        let u2 = q2.to_uppercase();
        assert_eq!(
            super::parse_redis_keys_where(&u2, q2),
            Some("*mid*".to_string())
        );
    }

    #[test]
    fn test_parse_redis_keys_type_filter() {
        let q = "SELECT * FROM __redis_keys__ WHERE type = 'hash'";
        let u = q.to_uppercase();
        assert_eq!(
            super::parse_redis_keys_type_filter(&u, q),
            Some(("hash".to_string(), false))
        );
        let q2 = "SELECT * FROM __redis_keys__ WHERE type != 'string'";
        let u2 = q2.to_uppercase();
        assert_eq!(
            super::parse_redis_keys_type_filter(&u2, q2),
            Some(("string".to_string(), true))
        );
    }

    #[test]
    fn test_parse_redis_keys_order_by() {
        let q = "SELECT * FROM __redis_keys__ ORDER BY value DESC";
        let u = q.to_uppercase();
        assert_eq!(
            super::parse_redis_keys_order_by(&u, q),
            Some(("value", true))
        );
    }

    #[test]
    fn test_parse_redis_keys_value_filter() {
        let q = "SELECT * FROM __redis_keys__ WHERE value = 'foo'";
        let u = q.to_uppercase();
        match super::parse_redis_keys_value_filter(&u, q) {
            Some(super::RedisValueFilterKind::Exact(s)) => assert_eq!(s.as_ref(), "foo"),
            _ => panic!("expected Exact"),
        }
        let q2 = "SELECT * FROM __redis_keys__ WHERE value LIKE 'pre%'";
        let u2 = q2.to_uppercase();
        match super::parse_redis_keys_value_filter(&u2, q2) {
            Some(super::RedisValueFilterKind::StartsWith(s)) => assert_eq!(s.as_ref(), "pre"),
            _ => panic!("expected StartsWith"),
        }
    }

    #[test]
    fn test_json_value_to_cmp_str() {
        assert_eq!(
            super::json_value_to_cmp_str(&JsonValue::String("x".into())),
            "x"
        );
        assert_eq!(
            super::json_value_to_cmp_str(&JsonValue::Number(42i64.into())),
            "42"
        );
        assert_eq!(super::json_value_to_cmp_str(&JsonValue::Null), "");
    }

    #[test]
    fn test_row_values_for_columns_uses_pk_for_key() {
        let cols = vec!["_key".to_string(), "name".to_string()];
        let pk = "my-pk";
        let mut map = HashMap::new();
        map.insert("name".to_string(), "alice".to_string());
        let out = super::row_values_for_columns(&cols, pk, Some(&map), None);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], JsonValue::String("my-pk".to_string()));
        assert_eq!(out[1], JsonValue::String("alice".to_string()));
    }
}
