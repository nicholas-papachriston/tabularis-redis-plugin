use crate::methods::common::{column_def_to_api_json, redis_virtual_columns_json};
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

/// Virtual table that lists all Redis keys (via SCAN). Shows real keys in the DB, not just tabularis metadata.
pub const REDIS_KEYS_TABLE: &str = "__redis_keys__";

/// Key-pattern virtual tables: "__keys:stress__" shows keys matching stress:*. Prefix/suffix for discovery.
pub const KEY_PATTERN_PREFIX: &str = "__keys:";
pub const KEY_PATTERN_SUFFIX: &str = "__";

pub fn is_key_pattern_table(name: &str) -> bool {
    name.starts_with(KEY_PATTERN_PREFIX)
        && name.ends_with(KEY_PATTERN_SUFFIX)
        && name.len() > KEY_PATTERN_PREFIX.len() + KEY_PATTERN_SUFFIX.len()
}

/// If `name` is "__keys:stress__", returns Some("stress"). Used for SCAN MATCH "stress:*".
pub fn key_pattern_from_table(name: &str) -> Option<String> {
    if !is_key_pattern_table(name) {
        return None;
    }
    let inner = name
        .strip_prefix(KEY_PATTERN_PREFIX)
        .and_then(|s| s.strip_suffix(KEY_PATTERN_SUFFIX))?;
    Some(inner.to_string())
}

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
    let mut names = client.list_tables()?;
    if !names.contains(&REDIS_KEYS_TABLE.to_string()) {
        names.push(REDIS_KEYS_TABLE.to_string());
    }
    let prefixes = client.scan_key_prefixes(500)?;
    for prefix in prefixes {
        let virtual_name = format!("{KEY_PATTERN_PREFIX}{prefix}{KEY_PATTERN_SUFFIX}");
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
    let tables: Vec<JsonValue> = names
        .into_iter()
        .map(|name| {
            serde_json::json!({
                "name": name,
                "schema": serde_json::Value::Null,
                "comment": serde_json::Value::Null
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
    if table == REDIS_KEYS_TABLE || is_key_pattern_table(table) {
        return Ok(serde_json::json!(redis_virtual_columns_json()));
    }
    let list = match client.get_table_columns(table)? {
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
    let indexes = client.get_table_indexes(table)?;
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
