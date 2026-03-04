use crate::metadata::RowStorageMode;
use crate::methods::common::AppError;
use crate::methods::discovery;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

const TABULARIS_KEY_PREFIX: &str = "tabularis:";

fn is_virtual_key_table(table: &str) -> bool {
    table == discovery::REDIS_KEYS_TABLE || discovery::is_key_pattern_table(table)
}

fn validate_virtual_key(key: &str) -> Result<(), AppError> {
    if key.is_empty() {
        return Err(AppError::InvalidParams("Key cannot be empty".into()));
    }
    if key.starts_with(TABULARIS_KEY_PREFIX) {
        return Err(AppError::Unsupported(
            "Keys under tabularis: are reserved for plugin metadata".into(),
        ));
    }
    Ok(())
}

fn json_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => String::new(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

fn insert_record_virtual(
    client: &mut RedisClient,
    data: &serde_json::Map<String, JsonValue>,
) -> Result<u64, AppError> {
    let key = data
        .get("key")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_default();
    validate_virtual_key(key.trim())?;
    let key = key.trim().to_string();
    let value = data.get("value").map(json_to_string).unwrap_or_default();
    client
        .set_key_string(&key, &value)
        .map_err(AppError::Backend)?;
    if let Some(ttl) = data.get("ttl_seconds") {
        let s = json_to_string(ttl);
        if let Ok(n) = s.parse::<i64>() {
            let _ = client.set_key_ttl(&key, n);
        }
    }
    log::info!("insert_record_virtual: key={key}");
    Ok(1u64)
}

fn update_record_virtual(
    client: &mut RedisClient,
    key: &str,
    column: &str,
    value: &JsonValue,
) -> Result<u64, AppError> {
    validate_virtual_key(key)?;
    if column == "value" {
        let key_bytes = key.as_bytes();
        let key_type = client.key_type(key_bytes).map_err(AppError::Backend)?;
        if key_type == "none" {
            return Ok(0u64);
        }
        if key_type == "string" {
            let s = json_to_string(value);
            client.set_key_string(key, &s).map_err(AppError::Backend)?;
            return Ok(1u64);
        }
        return Err(AppError::Unsupported(format!(
            "Updating value for type '{key_type}' is not supported; only string keys are editable"
        )));
    }
    if column == "ttl_seconds" {
        let s = json_to_string(value);
        let n = s
            .parse::<i64>()
            .map_err(|_| AppError::InvalidParams("ttl_seconds must be an integer".into()))?;
        if n == -1 {
            client.persist_key(key).map_err(AppError::Backend)?;
        } else {
            client.set_key_ttl(key, n).map_err(AppError::Backend)?;
        }
        return Ok(1u64);
    }
    Err(AppError::Unsupported(format!(
        "Column '{column}' is not editable on virtual key tables"
    )))
}

fn delete_record_virtual(client: &mut RedisClient, key: &str) -> Result<u64, AppError> {
    validate_virtual_key(key)?;
    let n = client.del_key(key).map_err(AppError::Backend)?;
    log::info!("delete_record_virtual: key={key} deleted={n}");
    Ok(n)
}

pub fn insert_record(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    data: &serde_json::Map<String, JsonValue>,
) -> Result<u64, String> {
    log::debug!("insert_record: table={table}");
    if is_virtual_key_table(table) {
        return insert_record_virtual(client, data).map_err(|e| e.message().to_string());
    }
    let pk_col = if let Some(pk) = client.get_table_pk(table)? {
        pk
    } else {
        if data.is_empty() {
            return Err("No columns in data".to_string());
        }
        let col_names: Vec<String> = data.keys().cloned().collect();
        let pk_col = col_names.first().cloned().unwrap_or_default();
        let columns: Vec<crate::models::ColumnDef> = col_names
            .iter()
            .map(|name| crate::models::ColumnDef {
                name: name.clone(),
                data_type: "TEXT".to_string(),
                is_nullable: true,
                column_default: None,
                is_primary_key: name == &pk_col,
                is_auto_increment: false,
                comment: None,
            })
            .collect();
        client.register_table(table, &pk_col, &columns, "hash")?;
        pk_col
    };
    let pk_val = data
        .get(&pk_col)
        .map(json_to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("Primary key '{pk_col}' value missing in data"))?;
    let mode = client.get_table_mode(table)?;

    match mode {
        RowStorageMode::Json => {
            let obj: serde_json::Map<String, JsonValue> =
                data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let value = JsonValue::Object(obj);
            client.set_row_json(table, &pk_val, &value)?;
        }
        RowStorageMode::Hash => {
            let mut map = HashMap::new();
            for (k, v) in data {
                map.insert(k.clone(), json_to_string(v));
            }
            client.set_row_hash(table, &pk_val, &map)?;
        }
    }
    log::info!("insert_record: table={table} pk={pk_val}");
    Ok(1u64)
}

pub fn update_record(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    _primary_key_column: &str,
    primary_key_value: &JsonValue,
    column: &str,
    value: &JsonValue,
) -> Result<u64, AppError> {
    log::debug!("update_record: table={table} column={column}");
    let pk_str = json_to_string(primary_key_value).trim().to_string();
    if is_virtual_key_table(table) {
        return update_record_virtual(client, &pk_str, column, value);
    }
    let mode = client.get_table_mode(table)?;

    match mode {
        RowStorageMode::Json => {
            let opt = client.get_row_json(table, &pk_str)?;
            let Some(mut obj) = opt.and_then(|v| v.as_object().cloned()) else {
                return Err(AppError::NotFound("Row not found".into()));
            };
            obj.insert(column.to_string(), value.clone());
            client.set_row_json(table, &pk_str, &JsonValue::Object(obj))?;
        }
        RowStorageMode::Hash => {
            let opt = client.get_row_hash(table, &pk_str)?;
            let Some(mut map) = opt else {
                return Err(AppError::NotFound("Row not found".into()));
            };
            map.insert(column.to_string(), json_to_string(value));
            client.set_row_hash(table, &pk_str, &map)?;
        }
    }
    log::info!("update_record: table={table} pk={pk_str} column={column}");
    Ok(1u64)
}

pub fn delete_record(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    primary_key_column: &str,
    primary_key_value: &JsonValue,
) -> Result<u64, String> {
    log::debug!("delete_record: table={table} pk_column={primary_key_column}");
    let pk_str = json_to_string(primary_key_value).trim().to_string();
    if is_virtual_key_table(table) {
        return delete_record_virtual(client, &pk_str).map_err(|e| e.message().to_string());
    }
    if primary_key_column.is_empty() {
        return Err("Primary key column required".to_string());
    }
    client.delete_row(table, &pk_str)?;
    log::info!("delete_record: table={table} pk={pk_str}");
    Ok(1u64)
}
