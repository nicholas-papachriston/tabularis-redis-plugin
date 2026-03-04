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
        column_def_to_api_json(&ColumnDef {
            name: "key_raw".to_string(),
            data_type: "TEXT".to_string(),
            is_nullable: false,
            column_default: None,
            is_primary_key: false,
            is_auto_increment: false,
            comment: Some("Base64-encoded key for lossless round-trip of binary keys".to_string()),
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_def_to_api_json_shape() {
        let c = ColumnDef {
            name: "id".to_string(),
            data_type: "TEXT".to_string(),
            is_primary_key: true,
            is_nullable: false,
            is_auto_increment: false,
            column_default: None,
            comment: None,
        };
        let j = column_def_to_api_json(&c);
        assert_eq!(j.get("name").and_then(|v| v.as_str()), Some("id"));
        assert_eq!(j.get("is_pk").and_then(|v| v.as_bool()), Some(true));
    }

    #[test]
    fn redis_virtual_columns_includes_key_raw() {
        let cols = redis_virtual_columns_json();
        let names: Vec<&str> = cols
            .iter()
            .filter_map(|c| c.get("name").and_then(|v| v.as_str()))
            .collect();
        assert!(names.contains(&"key"));
        assert!(names.contains(&"key_raw"));
        assert!(names.contains(&"type"));
        assert!(names.contains(&"value"));
        assert!(names.contains(&"ttl_seconds"));
    }
}
