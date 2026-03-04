use crate::methods::common::{
    column_def_to_api_json, redis_virtual_columns_json, type_virtual_columns_json, HASHES_TABLE,
    LISTS_TABLE, SETS_TABLE, STREAMS_TABLE, ZSETS_TABLE,
};
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

/// Virtual table that lists all Redis keys (via SCAN). Shows real keys in the DB, not just tabularis metadata.
pub const REDIS_KEYS_TABLE: &str = "__redis_keys__";

/// Key-pattern virtual tables (preferred exposed form): `__keys_stress__` shows keys matching `stress:*`.
pub const KEY_PATTERN_PREFIX: &str = "__keys:";
pub const KEY_PATTERN_SAFE_PREFIX: &str = "__keys_";
pub const KEY_PATTERN_SUFFIX: &str = "__";

pub fn is_key_pattern_table(name: &str) -> bool {
    (name.starts_with(KEY_PATTERN_PREFIX)
        && name.ends_with(KEY_PATTERN_SUFFIX)
        && name.len() > KEY_PATTERN_PREFIX.len() + KEY_PATTERN_SUFFIX.len())
        || (name.starts_with(KEY_PATTERN_SAFE_PREFIX)
            && name.ends_with(KEY_PATTERN_SUFFIX)
            && name.len() > KEY_PATTERN_SAFE_PREFIX.len() + KEY_PATTERN_SUFFIX.len())
}

/// If `name` is `__keys_stress__` or `__keys:stress__`, returns Some("stress").
/// Used for SCAN MATCH "stress:*".
pub fn key_pattern_from_table(name: &str) -> Option<String> {
    if !is_key_pattern_table(name) {
        return None;
    }
    let inner = if let Some(s) = name.strip_prefix(KEY_PATTERN_SAFE_PREFIX) {
        s.strip_suffix(KEY_PATTERN_SUFFIX)?
    } else {
        name.strip_prefix(KEY_PATTERN_PREFIX)?
            .strip_suffix(KEY_PATTERN_SUFFIX)?
    };
    Some(inner.to_string())
}

/// How to scan when the query targets a virtual key table (main keys list or key-pattern).
#[derive(Debug, Clone)]
pub enum VirtualScanTarget {
    /// All keys (__`redis_keys`__), no pattern.
    AllKeys,
    /// Key-pattern table: SCAN with prefix (e.g. "batch" -> "batch:*").
    Pattern(String),
}

/// Recognizes virtual key table names as sent by the app (e.g. _`redis_keys`, _keys:batch_).
/// Returns the scan target so `execute_query` can run the keys scan with or without a pattern.
pub fn virtual_table_scan_target(table: &str) -> Option<VirtualScanTarget> {
    let t = table.trim().trim_matches('"');
    let target =
        if t == REDIS_KEYS_TABLE || (t.trim_matches('_') == "redis_keys" && t.contains("redis")) {
            Some(VirtualScanTarget::AllKeys)
        } else if let Some(prefix) = key_pattern_from_table(t) {
            Some(VirtualScanTarget::Pattern(prefix))
        } else if t == "__keys*" || t == "_keys*" || t == "__keys__" || t == "_keys_" {
            Some(VirtualScanTarget::AllKeys)
        } else if t.starts_with("_keys:") && t.ends_with('_') {
            let inner = t.strip_prefix("_keys:")?.strip_suffix('_')?;
            if inner.is_empty() {
                None
            } else {
                Some(VirtualScanTarget::Pattern(inner.to_string()))
            }
        } else if t.starts_with("__keys:") && t.ends_with("__") {
            let inner = t.strip_prefix("__keys:")?.strip_suffix("__")?;
            if inner.is_empty() {
                None
            } else {
                Some(VirtualScanTarget::Pattern(inner.to_string()))
            }
        } else {
            None
        };

    if let Some(ref resolved) = target {
        log::debug!(
            "virtual_table_scan_target: input={table:?} normalized={t:?} resolved={resolved:?}"
        );
    }
    target
}

/// Type-specific virtual tables: hashes, lists, sets, zsets, streams.
pub fn is_type_virtual_table(name: &str) -> bool {
    type_virtual_columns_json(name).is_some()
}

/// Redis TYPE string for a type-specific virtual table, or None if not a type table.
pub fn type_virtual_table_redis_type(name: &str) -> Option<&'static str> {
    let t = name.trim().trim_matches('"');
    match t {
        HASHES_TABLE => Some("hash"),
        LISTS_TABLE => Some("list"),
        SETS_TABLE => Some("set"),
        ZSETS_TABLE => Some("zset"),
        STREAMS_TABLE => Some("stream"),
        _ => None,
    }
}

const TYPE_VIRTUAL_TABLES: &[(&str, &str)] = &[
    (HASHES_TABLE, "All fields in hash keys"),
    (LISTS_TABLE, "All elements in list keys"),
    (SETS_TABLE, "All members in set keys"),
    (ZSETS_TABLE, "All members in sorted set keys"),
    (STREAMS_TABLE, "All entries in stream keys"),
];

pub fn test_connection(client: &mut RedisClient) -> Result<JsonValue, String> {
    log::debug!("test_connection: pinging Redis");
    client.ping()?;
    log::info!("test_connection: success");
    Ok(serde_json::json!({ "success": true }))
}

/// Redis logical databases 0-15 (default Redis config). Returned so Tabularis can show a database dropdown.
pub fn get_databases(client: &mut RedisClient) -> Result<JsonValue, String> {
    let dbs = client.list_databases()?;
    log::info!("get_databases: returning {} databases (0-15)", dbs.len());
    Ok(serde_json::json!(dbs))
}

pub const fn get_schemas(_client: &mut RedisClient) -> JsonValue {
    serde_json::json!([])
}

/// Full list of table names: metadata tables, __`redis_keys`__, and key-pattern virtual tables (__keys:prefix__).
pub fn list_table_names(
    client: &mut RedisClient,
    _schema: Option<&str>,
) -> Result<Vec<String>, String> {
    let mut names = client.metadata().list_tables()?;
    if !names.contains(&REDIS_KEYS_TABLE.to_string()) {
        names.push(REDIS_KEYS_TABLE.to_string());
    }
    for (table_name, _) in TYPE_VIRTUAL_TABLES {
        if !names.contains(&(*table_name).to_string()) {
            names.push((*table_name).to_string());
        }
    }
    let prefixes = client.scan_key_prefixes(500)?;
    for prefix in prefixes {
        if prefix == "tabularis" {
            // Internal metadata keys are protected from row-level edits/deletes.
            // Do not expose them as virtual user tables in explorer.
            continue;
        }
        let virtual_name = format!("{KEY_PATTERN_SAFE_PREFIX}{prefix}{KEY_PATTERN_SUFFIX}");
        if !names.contains(&virtual_name) {
            names.push(virtual_name);
        }
    }
    Ok(names)
}

pub fn get_tables(client: &mut RedisClient, schema: Option<&str>) -> Result<JsonValue, String> {
    log::debug!("get_tables: listing tables");
    let names = list_table_names(client, schema)?;
    log::info!("get_tables: found {} table(s)", names.len());
    let comment_for: std::collections::HashMap<String, String> = TYPE_VIRTUAL_TABLES
        .iter()
        .map(|(n, c)| ((*n).to_string(), (*c).to_string()))
        .collect();
    let tables: Vec<JsonValue> = names
        .into_iter()
        .map(|name| {
            let comment = comment_for
                .get(&name)
                .cloned()
                .map_or(JsonValue::Null, JsonValue::String);
            serde_json::json!({
                "name": name,
                "schema": serde_json::Value::Null,
                "comment": comment
            })
        })
        .collect();
    Ok(serde_json::json!(tables))
}

pub fn get_columns(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
) -> Result<JsonValue, String> {
    log::debug!("get_columns: table={table}");
    if table == REDIS_KEYS_TABLE
        || is_key_pattern_table(table)
        || virtual_table_scan_target(table).is_some()
    {
        return Ok(serde_json::json!(redis_virtual_columns_json()));
    }
    if let Some(cols) = type_virtual_columns_json(table) {
        return Ok(serde_json::json!(cols));
    }
    let list = match client.metadata().get_table_columns(table)? {
        Some(cols) if !cols.is_empty() => cols,
        _ => client.infer_columns_from_data(table)?.unwrap_or_default(),
    };
    log::debug!("get_columns: table={table} columns={}", list.len());
    let out: Vec<JsonValue> = list.iter().map(column_def_to_api_json).collect();
    Ok(serde_json::json!(out))
}

pub const fn get_foreign_keys(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    _table: &str,
) -> JsonValue {
    serde_json::json!([])
}

pub fn get_indexes(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
) -> Result<JsonValue, String> {
    log::debug!("get_indexes: table={table}");
    let indexes = client.metadata().get_table_indexes(table)?;
    log::info!("get_indexes: table={table} count={}", indexes.len());
    let out: Vec<JsonValue> = indexes
        .into_iter()
        .map(|idx| {
            serde_json::json!({
                "index_name": idx.index_name,
                "name": idx.index_name,
                "columns": idx.columns,
                "is_unique": idx.is_unique
            })
        })
        .collect();
    Ok(serde_json::json!(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_key_pattern_table_accepts_valid() {
        assert!(is_key_pattern_table("__keys:stress__"));
        assert!(is_key_pattern_table("__keys_stress__"));
        assert!(is_key_pattern_table("__keys:batch__"));
        assert!(is_key_pattern_table("__keys_batch__"));
    }

    #[test]
    fn is_key_pattern_table_rejects_invalid() {
        assert!(!is_key_pattern_table("__redis_keys__"));
        assert!(!is_key_pattern_table("__keys:__"));
        assert!(!is_key_pattern_table("__keys:"));
        assert!(!is_key_pattern_table("keys:stress__"));
    }

    #[test]
    fn test_key_pattern_from_table() {
        assert_eq!(
            super::key_pattern_from_table("__keys:stress__"),
            Some("stress".to_string())
        );
        assert_eq!(
            super::key_pattern_from_table("__keys_stress__"),
            Some("stress".to_string())
        );
        assert_eq!(
            super::key_pattern_from_table("__keys:batch__"),
            Some("batch".to_string())
        );
        assert_eq!(
            super::key_pattern_from_table("__keys_batch__"),
            Some("batch".to_string())
        );
        assert_eq!(super::key_pattern_from_table("__redis_keys__"), None);
    }

    #[test]
    fn test_virtual_table_scan_target() {
        use super::VirtualScanTarget;
        assert!(matches!(
            super::virtual_table_scan_target("__redis_keys__"),
            Some(VirtualScanTarget::AllKeys)
        ));
        assert!(matches!(
            super::virtual_table_scan_target("_redis_keys"),
            Some(VirtualScanTarget::AllKeys)
        ));
        assert!(matches!(
            super::virtual_table_scan_target("__keys:batch__"),
            Some(VirtualScanTarget::Pattern(p)) if p == "batch"
        ));
        assert!(matches!(
            super::virtual_table_scan_target("__keys_batch__"),
            Some(VirtualScanTarget::Pattern(p)) if p == "batch"
        ));
        assert!(matches!(
            super::virtual_table_scan_target("_keys:batch_"),
            Some(VirtualScanTarget::Pattern(p)) if p == "batch"
        ));
        assert!(matches!(
            super::virtual_table_scan_target("__keys*"),
            Some(VirtualScanTarget::AllKeys)
        ));
        assert!(super::virtual_table_scan_target("test_data").is_none());
    }
}
