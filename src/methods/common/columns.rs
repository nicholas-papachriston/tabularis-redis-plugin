use crate::models::ColumnDef;
use serde_json::Value as JsonValue;

/// API-shaped column object used by discovery and batch responses.
pub fn column_def_to_api_json(c: &ColumnDef) -> JsonValue {
    serde_json::json!({
        "name": c.name,
        "data_type": c.data_type,
        "is_pk": c.is_primary_key,
        "is_nullable": c.is_nullable,
        "is_auto_increment": c.is_auto_increment,
        "default_value": c.column_default
    })
}

/// Column definitions for the __`redis_keys`__ and __keys:*__ virtual tables (key, type, value, `ttl_seconds`).
pub fn redis_virtual_columns_json() -> Vec<JsonValue> {
    vec![
        column_def_to_api_json(&ColumnDef {
            name: "key".to_string(),
            data_type: "TEXT".to_string(),
            is_nullable: false,
            column_default: None,
            is_primary_key: true,
            is_auto_increment: false,
            comment: None,
        }),
        column_def_to_api_json(&ColumnDef {
            name: "type".to_string(),
            data_type: "TEXT".to_string(),
            is_nullable: false,
            column_default: None,
            is_primary_key: false,
            is_auto_increment: false,
            comment: None,
        }),
        column_def_to_api_json(&ColumnDef {
            name: "value".to_string(),
            data_type: "TEXT".to_string(),
            is_nullable: true,
            column_default: None,
            is_primary_key: false,
            is_auto_increment: false,
            comment: None,
        }),
        column_def_to_api_json(&ColumnDef {
            name: "ttl_seconds".to_string(),
            data_type: "TEXT".to_string(),
            is_nullable: true,
            column_default: None,
            is_primary_key: false,
            is_auto_increment: false,
            comment: None,
        }),
    ]
}
