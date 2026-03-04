use crate::metadata::RowStorageMode;
use crate::methods::common::AppError;
use crate::methods::discovery;
use crate::redis_client::RedisClient;
use base64::Engine;
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

fn validate_virtual_key_bytes(key: &[u8]) -> Result<(), AppError> {
    if key.is_empty() {
        return Err(AppError::InvalidParams("Key cannot be empty".into()));
    }
    if key.starts_with(TABULARIS_KEY_PREFIX.as_bytes()) {
        return Err(AppError::Unsupported(
            "Keys under tabularis: are reserved for plugin metadata".into(),
        ));
    }
    Ok(())
}

/// Decode `key_raw` (Base64) for round-trip of binary keys. Returns None if not valid base64.
fn try_decode_key_raw(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    base64::engine::general_purpose::STANDARD
        .decode(s.as_bytes())
        .ok()
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
    let value = data.get("value").map(json_to_string).unwrap_or_default();
    let value_bytes = value.as_bytes();

    if let Some(key_raw) = data
        .get("key_raw")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
    {
        let key_bytes = try_decode_key_raw(key_raw)
            .ok_or_else(|| AppError::InvalidParams("data.key_raw must be valid Base64".into()))?;
        validate_virtual_key_bytes(&key_bytes)?;
        client
            .set_key_bytes(&key_bytes, value_bytes)
            .map_err(AppError::Backend)?;
        if let Some(ttl) = data.get("ttl_seconds") {
            let s = json_to_string(ttl);
            if let Ok(n) = s.parse::<i64>() {
                let _ = client.set_key_ttl_bytes(&key_bytes, n);
            }
        }
        log::info!("insert_record_virtual: key_raw (binary)");
        return Ok(1u64);
    }

    let key = data
        .get("key")
        .and_then(|v| v.as_str())
        .map(String::from)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            AppError::InvalidParams(
                "data.key or data.key_raw is required and must be non-empty".into(),
            )
        })?;
    validate_virtual_key(key.trim())?;
    let key = key.trim().to_string();
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

fn update_virtual_value_by_type(
    client: &mut RedisClient,
    key_bytes: &[u8],
    key_type: &str,
    value: &JsonValue,
) -> Result<u64, AppError> {
    match key_type {
        "string" => {
            let s = json_to_string(value);
            client
                .set_key_bytes(key_bytes, s.as_bytes())
                .map_err(AppError::Backend)?;
            Ok(1u64)
        }
        "hash" => {
            let obj = value.as_object().ok_or_else(|| {
                AppError::InvalidParams("value must be a JSON object for hash type".into())
            })?;
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_string(v));
            }
            client
                .hset_multiple(key_bytes, &map)
                .map_err(AppError::Backend)?;
            Ok(1u64)
        }
        "list" => {
            let (index, val) = match value {
                JsonValue::Object(o) => {
                    let i = o
                        .get("index")
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| AppError::InvalidParams("value must have index for list type".into()))?;
                    let v = o.get("value").map(json_to_string).unwrap_or_default();
                    (i, v)
                }
                JsonValue::Array(a) if a.len() >= 2 => {
                    let i = a[0].as_i64().unwrap_or(0);
                    let v = json_to_string(&a[1]);
                    (i, v)
                }
                _ => return Err(AppError::InvalidParams(
                    "value must be { index: number, value: string } or [index, value] for list type".into(),
                )),
            };
            client
                .lset(key_bytes, index, &val)
                .map_err(AppError::Backend)?;
            Ok(1u64)
        }
        "set" => {
            let members: Vec<String> = value
                .as_array()
                .ok_or_else(|| {
                    AppError::InvalidParams(
                        "value must be a JSON array of strings for set type".into(),
                    )
                })?
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            client
                .sadd_members(key_bytes, &members)
                .map_err(AppError::Backend)?;
            Ok(1u64)
        }
        "zset" => {
            let entries: Vec<(f64, String)> = value
                .as_array()
                .ok_or_else(|| {
                    AppError::InvalidParams(
                        "value must be a JSON array of [score, member] for zset type".into(),
                    )
                })?
                .iter()
                .filter_map(|v| {
                    let arr = v.as_array()?;
                    #[allow(clippy::cast_precision_loss)]
                    let score = arr
                        .first()?
                        .as_f64()
                        .or_else(|| arr.first()?.as_i64().map(|n| n as f64))?;
                    let member = arr.get(1).map(json_to_string).unwrap_or_default();
                    Some((score, member))
                })
                .collect();
            if entries.is_empty() {
                return Err(AppError::InvalidParams(
                    "zset value must have at least one [score, member] pair".into(),
                ));
            }
            client
                .zadd_entries(key_bytes, &entries)
                .map_err(AppError::Backend)?;
            Ok(1u64)
        }
        _ => Err(AppError::Unsupported(format!(
            "Updating value for type '{key_type}' is not supported",
        ))),
    }
}

#[allow(clippy::too_many_lines)]
fn update_record_virtual(
    client: &mut RedisClient,
    key: &str,
    column: &str,
    value: &JsonValue,
) -> Result<u64, AppError> {
    let key_bytes_opt = try_decode_key_raw(key);
    let use_bytes = key_bytes_opt
        .as_ref()
        .is_some_and(|b| validate_virtual_key_bytes(b).is_ok());

    if use_bytes {
        let key_bytes = key_bytes_opt.as_ref().unwrap();
        if column == "value" {
            let key_type = client.key_type(key_bytes).map_err(AppError::Backend)?;
            if key_type == "none" {
                return Ok(0u64);
            }
            if key_type == "string" {
                let s = json_to_string(value);
                client
                    .set_key_bytes(key_bytes, s.as_bytes())
                    .map_err(AppError::Backend)?;
                return Ok(1u64);
            }
            return update_virtual_value_by_type(client, key_bytes, &key_type, value);
        }
        if column.starts_with("field:") {
            let key_type = client.key_type(key_bytes).map_err(AppError::Backend)?;
            if key_type != "hash" {
                return Err(AppError::Unsupported(
                    "field:<name> is only for hash keys".into(),
                ));
            }
            let field = column.strip_prefix("field:").unwrap_or_default();
            if field.is_empty() {
                return Err(AppError::InvalidParams(
                    "field: must be followed by field name".into(),
                ));
            }
            let val = json_to_string(value);
            client
                .hset_field(key_bytes, field, &val)
                .map_err(AppError::Backend)?;
            return Ok(1u64);
        }
        if column.starts_with("index:") {
            let key_type = client.key_type(key_bytes).map_err(AppError::Backend)?;
            if key_type != "list" {
                return Err(AppError::Unsupported(
                    "index:<n> is only for list keys".into(),
                ));
            }
            let index_str = column.strip_prefix("index:").unwrap_or_default();
            let index: i64 = index_str
                .parse()
                .map_err(|_| AppError::InvalidParams("index must be a number".into()))?;
            let val = json_to_string(value);
            client
                .lset(key_bytes, index, &val)
                .map_err(AppError::Backend)?;
            return Ok(1u64);
        }
        if column == "ttl_seconds" {
            let s = json_to_string(value);
            let n = s
                .parse::<i64>()
                .map_err(|_| AppError::InvalidParams("ttl_seconds must be an integer".into()))?;
            if n == -1 {
                client
                    .persist_key_bytes(key_bytes)
                    .map_err(AppError::Backend)?;
            } else {
                client
                    .set_key_ttl_bytes(key_bytes, n)
                    .map_err(AppError::Backend)?;
            }
            return Ok(1u64);
        }
    } else {
        validate_virtual_key(key)?;
        let key_bytes = key.as_bytes();
        if column == "value" {
            let key_type = client.key_type(key_bytes).map_err(AppError::Backend)?;
            if key_type == "none" {
                return Ok(0u64);
            }
            if key_type == "string" {
                let s = json_to_string(value);
                client.set_key_string(key, &s).map_err(AppError::Backend)?;
                return Ok(1u64);
            }
            return update_virtual_value_by_type(client, key_bytes, &key_type, value);
        }
        if column.starts_with("field:") {
            let key_type = client.key_type(key_bytes).map_err(AppError::Backend)?;
            if key_type != "hash" {
                return Err(AppError::Unsupported(
                    "field:<name> is only for hash keys".into(),
                ));
            }
            let field = column.strip_prefix("field:").unwrap_or_default();
            if field.is_empty() {
                return Err(AppError::InvalidParams(
                    "field: must be followed by field name".into(),
                ));
            }
            let val = json_to_string(value);
            client
                .hset_field(key_bytes, field, &val)
                .map_err(AppError::Backend)?;
            return Ok(1u64);
        }
        if column.starts_with("index:") {
            let key_type = client.key_type(key_bytes).map_err(AppError::Backend)?;
            if key_type != "list" {
                return Err(AppError::Unsupported(
                    "index:<n> is only for list keys".into(),
                ));
            }
            let index_str = column.strip_prefix("index:").unwrap_or_default();
            let index: i64 = index_str
                .parse()
                .map_err(|_| AppError::InvalidParams("index must be a number".into()))?;
            let val = json_to_string(value);
            client
                .lset(key_bytes, index, &val)
                .map_err(AppError::Backend)?;
            return Ok(1u64);
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
    }
    Err(AppError::Unsupported(format!(
        "Column '{column}' is not editable on virtual key tables"
    )))
}

fn delete_record_virtual(client: &mut RedisClient, key: &str) -> Result<u64, AppError> {
    if let Some(key_bytes) = try_decode_key_raw(key) {
        if validate_virtual_key_bytes(&key_bytes).is_ok() {
            let n = client
                .del_key_bytes(&key_bytes)
                .map_err(AppError::Backend)?;
            log::info!("delete_record_virtual: key_raw (binary) deleted={n}");
            return Ok(n);
        }
    }
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

pub fn insert_records_batch(
    client: &mut RedisClient,
    schema: Option<&str>,
    table: &str,
    rows: &[serde_json::Map<String, JsonValue>],
) -> Result<u64, String> {
    let mut total = 0u64;
    for data in rows {
        let n = insert_record(client, schema, table, data)?;
        total += n;
    }
    log::info!(
        "insert_records_batch: table={table} rows={} total_affected={total}",
        rows.len()
    );
    Ok(total)
}

pub fn delete_records_batch(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    primary_keys: &[String],
) -> Result<u64, String> {
    if primary_keys.is_empty() {
        return Ok(0);
    }
    if is_virtual_key_table(table) {
        for key in primary_keys {
            validate_virtual_key(key).map_err(|e| e.message().to_string())?;
        }
        let count = client.del_keys_batch(primary_keys)?;
        log::info!(
            "delete_records_batch: virtual table={table} keys={} deleted={count}",
            primary_keys.len()
        );
        return Ok(count);
    }
    let count = client.delete_rows_batch(table, primary_keys)?;
    log::info!(
        "delete_records_batch: table={table} pks={} deleted={count}",
        primary_keys.len()
    );
    Ok(count)
}

#[cfg(test)]
mod tests {
    use crate::methods::common::AppError;
    use base64::Engine;

    #[test]
    fn validate_virtual_key_empty() {
        let r = super::validate_virtual_key("");
        assert!(matches!(r, Err(AppError::InvalidParams(_))));
    }

    #[test]
    fn validate_virtual_key_tabularis_reserved() {
        let r = super::validate_virtual_key("tabularis:foo");
        assert!(matches!(r, Err(AppError::Unsupported(_))));
    }

    #[test]
    fn validate_virtual_key_ok() {
        assert!(super::validate_virtual_key("user:1").is_ok());
        assert!(super::validate_virtual_key("batch:abc").is_ok());
    }

    #[test]
    fn try_decode_key_raw() {
        let raw = b"hello";
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
        let decoded = super::try_decode_key_raw(&encoded).unwrap();
        assert_eq!(decoded.as_slice(), raw);
        assert!(super::try_decode_key_raw("not-base64!!").is_none());
        assert!(super::try_decode_key_raw("").is_none());
    }
}
