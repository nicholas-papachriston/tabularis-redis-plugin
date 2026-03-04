use crate::methods::common::{column_def_to_api_json, redis_virtual_columns_json};
use crate::methods::discovery;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

pub fn get_schema_snapshot(
    client: &mut RedisClient,
    schema: Option<&str>,
) -> Result<JsonValue, String> {
    log::debug!("get_schema_snapshot");
    let table_names = discovery::list_table_names(client, schema)?;
    let tables: Vec<JsonValue> = table_names
        .iter()
        .map(|name| {
            serde_json::json!({
                "name": name,
                "schema": JsonValue::Null,
                "comment": JsonValue::Null
            })
        })
        .collect();

    let mut columns: serde_json::Map<String, JsonValue> = serde_json::Map::new();
    let mut foreign_keys: serde_json::Map<String, JsonValue> = serde_json::Map::new();

    for name in &table_names {
        if name == discovery::REDIS_KEYS_TABLE || discovery::is_key_pattern_table(name) {
            columns.insert(name.clone(), JsonValue::Array(redis_virtual_columns_json()));
            foreign_keys.insert(name.clone(), JsonValue::Array(vec![]));
            continue;
        }
        let cols = client
            .metadata()
            .get_table_columns(name)?
            .unwrap_or_default();
        let col_list: Vec<JsonValue> = cols.iter().map(column_def_to_api_json).collect();
        columns.insert(name.clone(), JsonValue::Array(col_list));
        foreign_keys.insert(name.clone(), JsonValue::Array(vec![]));
    }

    log::info!("get_schema_snapshot: {} table(s)", table_names.len());
    Ok(serde_json::json!({
        "tables": tables,
        "columns": columns,
        "foreign_keys": foreign_keys
    }))
}

pub fn get_all_columns_batch(
    client: &mut RedisClient,
    schema: Option<&str>,
    tables: &[String],
) -> Result<JsonValue, String> {
    let tables = if tables.is_empty() {
        discovery::list_table_names(client, schema)?
    } else {
        tables.to_vec()
    };
    log::debug!("get_all_columns_batch: {} table(s)", tables.len());
    let mut result: serde_json::Map<String, JsonValue> = serde_json::Map::new();
    for name in &tables {
        if name == discovery::REDIS_KEYS_TABLE || discovery::is_key_pattern_table(name) {
            result.insert(name.clone(), JsonValue::Array(redis_virtual_columns_json()));
            continue;
        }
        let cols = client
            .metadata()
            .get_table_columns(name)?
            .unwrap_or_default();
        let col_list: Vec<JsonValue> = cols.iter().map(column_def_to_api_json).collect();
        result.insert(name.clone(), JsonValue::Array(col_list));
    }
    Ok(JsonValue::Object(result))
}

pub fn get_all_foreign_keys_batch(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    tables: &[String],
) -> JsonValue {
    let mut result: serde_json::Map<String, JsonValue> = serde_json::Map::new();
    for name in tables {
        result.insert(name.clone(), JsonValue::Array(vec![]));
    }
    JsonValue::Object(result)
}
