use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Single WHERE condition: column name, operator ("=" or "!="), literal value (unquoted).
pub fn parse_where(upper: &str, q: &str) -> Option<(String, bool, String)> {
    let after_where = upper.find(" WHERE ")?;
    let clause = q[after_where + 7..].trim();
    let clause_upper = clause.to_uppercase();
    let end = clause_upper.find(" ORDER BY ").unwrap_or(clause.len());
    let clause = clause[..end.min(clause.len())].trim();
    if clause.is_empty() {
        return None;
    }
    let (op, op_len) = if clause.contains(" != ") {
        ("!=", 4)
    } else if clause.contains(" = ") {
        ("=", 3)
    } else {
        return None;
    };
    let idx = clause.to_uppercase().find(&format!(" {op} "))?;
    let left = clause[..idx].trim().trim_matches('"').to_string();
    let right = clause[idx + op_len..]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();
    Some((left, op == "!=", right))
}

/// ORDER BY: column name and ascending flag. Parses SQL-style "ORDER BY <column> [ASC|DESC]".
pub fn parse_order_by(upper: &str, q: &str) -> Option<(String, bool)> {
    let order_pos = upper.find(" ORDER BY ")?;
    let clause = q[order_pos + 10..].trim();
    let end = clause
        .to_uppercase()
        .find(" LIMIT ")
        .unwrap_or(clause.len());
    let clause = clause[..end.min(clause.len())].trim();
    let mut tokens = clause.split_whitespace();
    let col = tokens.next()?.trim_matches('"').to_string();
    if col.is_empty() {
        return None;
    }
    let asc = !matches!(tokens.next().map(str::to_uppercase), Some(s) if s == "DESC");
    Some((col, asc))
}

pub fn json_value_to_cmp_str(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => String::new(),
        _ => v.to_string(),
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

pub enum PageResult {
    Keys(Vec<String>),
    Rows(Vec<Vec<JsonValue>>),
}

/// Parse WHERE key = 'x' or key LIKE 'x%' / '%x%' from query for __`redis_keys`__. Returns Redis glob pattern.
pub fn parse_redis_keys_where(upper: &str, q: &str) -> Option<String> {
    let after_where = upper.find(" WHERE ")?;
    let clause = q[after_where + 7..].trim();
    let clause_upper = clause.to_uppercase();
    let end = clause_upper.find(" ORDER BY ").unwrap_or(clause.len());
    let clause = clause[..end.min(clause.len())].trim();
    if clause.is_empty() {
        return None;
    }
    if clause_upper.contains(" LIKE ") {
        let like_idx = clause_upper.find(" LIKE ")?;
        let left = clause[..like_idx].trim().trim_matches('"').to_lowercase();
        if left != "key" {
            return None;
        }
        let right = clause[like_idx + 6..]
            .trim()
            .trim_matches('\'')
            .trim_matches('"');
        if right.starts_with('%') && right.ends_with('%') && right.len() > 2 {
            return Some(format!("*{}*", &right[1..right.len() - 1]));
        }
        if right.ends_with('%') && right.len() > 1 {
            return Some(format!("{}*", right.trim_end_matches('%')));
        }
        return Some(format!("*{right}*"));
    }
    if clause.contains(" = ") {
        let eq_idx = clause.find(" = ")?;
        let left = clause[..eq_idx].trim().trim_matches('"').to_lowercase();
        if left != "key" {
            return None;
        }
        let right = clause[eq_idx + 3..]
            .trim()
            .trim_matches('\'')
            .trim_matches('"');
        if right.is_empty() {
            return None;
        }
        return Some(format!("{right}*"));
    }
    None
}

/// Parse WHERE type = 'hash' or type != 'string' for __`redis_keys`__. Returns (`type_value`, negate).
pub fn parse_redis_keys_type_filter(upper: &str, q: &str) -> Option<(String, bool)> {
    let after_where = upper.find(" WHERE ")?;
    let clause = q[after_where + 7..].trim();
    let clause_upper = clause.to_uppercase();
    let end = clause_upper.find(" ORDER BY ").unwrap_or(clause.len());
    let clause = clause[..end.min(clause.len())].trim();
    if clause.is_empty() {
        return None;
    }
    let (idx, op_len, negate) = if clause_upper.starts_with("TYPE != ") {
        (0, "TYPE != ".len(), true)
    } else if clause_upper.starts_with("TYPE = ") {
        (0, "TYPE = ".len(), false)
    } else if let Some(pos) = clause_upper.find(" TYPE != ") {
        (pos, " TYPE != ".len(), true)
    } else if let Some(pos) = clause_upper.find(" TYPE = ") {
        (pos, " TYPE = ".len(), false)
    } else {
        return None;
    };
    let rest = clause[idx + op_len..].trim();
    let value = rest
        .strip_prefix('\'')
        .map(|stripped| {
            let end = stripped.find('\'').unwrap_or(stripped.len());
            stripped[..end].to_lowercase()
        })
        .or_else(|| {
            rest.strip_prefix('"').map(|stripped| {
                let end = stripped.find('"').unwrap_or(stripped.len());
                stripped[..end].to_lowercase()
            })
        })
        .unwrap_or_else(|| {
            rest.split_whitespace()
                .next()
                .unwrap_or(rest)
                .to_lowercase()
        });
    if value.is_empty() {
        return None;
    }
    Some((value, negate))
}

/// How to match the value preview for __`redis_keys`__ WHERE value ... filters.
#[derive(Debug, Clone)]
pub enum RedisValueFilterKind {
    Exact(String),
    StartsWith(String),
    Contains(String),
}

/// Parse WHERE value = 'x' or value LIKE 'x%' / '%x%' for __`redis_keys`__. Matches against the value preview.
pub fn parse_redis_keys_value_filter(upper: &str, q: &str) -> Option<RedisValueFilterKind> {
    let after_where = upper.find(" WHERE ")?;
    let clause = q[after_where + 7..].trim();
    let clause_upper = clause.to_uppercase();
    let end = clause_upper
        .find(" ORDER BY ")
        .unwrap_or(clause_upper.len())
        .min(clause_upper.find(" LIMIT ").unwrap_or(clause_upper.len()));
    let clause = clause[..end.min(clause.len())].trim();
    if clause.is_empty() {
        return None;
    }
    let (idx, op_len, is_like) = if clause_upper.starts_with("VALUE LIKE ") {
        (0, "VALUE LIKE ".len(), true)
    } else if clause_upper.starts_with("VALUE = ") {
        (0, "VALUE = ".len(), false)
    } else if let Some(pos) = clause_upper.find(" VALUE LIKE ") {
        (pos, " VALUE LIKE ".len(), true)
    } else if let Some(pos) = clause_upper.find(" VALUE = ") {
        (pos, " VALUE = ".len(), false)
    } else {
        return None;
    };
    let rest = clause[idx + op_len..].trim();
    let kind = if is_like {
        let right = rest
            .strip_prefix('\'')
            .map(|stripped| {
                let end = stripped.find('\'').unwrap_or(stripped.len());
                stripped[..end].to_string()
            })
            .or_else(|| {
                rest.strip_prefix('"').map(|stripped| {
                    let end = stripped.find('"').unwrap_or(stripped.len());
                    stripped[..end].to_string()
                })
            })
            .or_else(|| rest.split_whitespace().next().map(String::from))?;
        if right.starts_with('%') && right.ends_with('%') && right.len() > 2 {
            RedisValueFilterKind::Contains(right[1..right.len() - 1].to_string())
        } else if right.ends_with('%') && right.len() > 1 {
            RedisValueFilterKind::StartsWith(right.trim_end_matches('%').to_string())
        } else if right.starts_with('%') && right.len() > 1 {
            RedisValueFilterKind::Contains(right[1..].to_string())
        } else {
            RedisValueFilterKind::StartsWith(right)
        }
    } else {
        let value = rest
            .strip_prefix('\'')
            .map(|stripped| {
                let end = stripped.find('\'').unwrap_or(stripped.len());
                stripped[..end].to_string()
            })
            .or_else(|| {
                rest.strip_prefix('"').map(|stripped| {
                    let end = stripped.find('"').unwrap_or(stripped.len());
                    stripped[..end].to_string()
                })
            })
            .unwrap_or_else(|| rest.split_whitespace().next().unwrap_or(rest).to_string());
        RedisValueFilterKind::Exact(value)
    };
    let is_empty = match &kind {
        RedisValueFilterKind::Exact(s)
        | RedisValueFilterKind::StartsWith(s)
        | RedisValueFilterKind::Contains(s) => s.is_empty(),
    };
    if is_empty {
        return None;
    }
    Some(kind)
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
