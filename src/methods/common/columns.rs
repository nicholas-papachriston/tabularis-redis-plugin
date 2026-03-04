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

/// Table names for type-specific virtual tables (hashes, lists, sets, zsets, streams).
pub const HASHES_TABLE: &str = "hashes";
pub const LISTS_TABLE: &str = "lists";
pub const SETS_TABLE: &str = "sets";
pub const ZSETS_TABLE: &str = "zsets";
pub const STREAMS_TABLE: &str = "streams";

/// Column definitions for type-specific virtual tables. Returns None if table is not one of hashes, lists, sets, zsets, streams.
#[allow(clippy::too_many_lines)]
pub fn type_virtual_columns_json(table: &str) -> Option<Vec<JsonValue>> {
    let table = table.trim().trim_matches('"');
    Some(match table {
        HASHES_TABLE => vec![
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
                name: "field".to_string(),
                data_type: "TEXT".to_string(),
                is_nullable: false,
                column_default: None,
                is_primary_key: true,
                is_auto_increment: false,
                comment: None,
            }),
            column_def_to_api_json(&ColumnDef {
                name: "value".to_string(),
                data_type: "TEXT".to_string(),
                is_nullable: false,
                column_default: None,
                is_primary_key: false,
                is_auto_increment: false,
                comment: None,
            }),
        ],
        LISTS_TABLE => vec![
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
                name: "index".to_string(),
                data_type: "INTEGER".to_string(),
                is_nullable: false,
                column_default: None,
                is_primary_key: true,
                is_auto_increment: false,
                comment: None,
            }),
            column_def_to_api_json(&ColumnDef {
                name: "value".to_string(),
                data_type: "TEXT".to_string(),
                is_nullable: false,
                column_default: None,
                is_primary_key: false,
                is_auto_increment: false,
                comment: None,
            }),
        ],
        SETS_TABLE => vec![
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
                name: "value".to_string(),
                data_type: "TEXT".to_string(),
                is_nullable: false,
                column_default: None,
                is_primary_key: true,
                is_auto_increment: false,
                comment: None,
            }),
        ],
        ZSETS_TABLE => vec![
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
                name: "value".to_string(),
                data_type: "TEXT".to_string(),
                is_nullable: false,
                column_default: None,
                is_primary_key: true,
                is_auto_increment: false,
                comment: None,
            }),
            column_def_to_api_json(&ColumnDef {
                name: "score".to_string(),
                data_type: "DOUBLE".to_string(),
                is_nullable: false,
                column_default: None,
                is_primary_key: false,
                is_auto_increment: false,
                comment: None,
            }),
        ],
        STREAMS_TABLE => vec![
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
                name: "id".to_string(),
                data_type: "TEXT".to_string(),
                is_nullable: false,
                column_default: None,
                is_primary_key: true,
                is_auto_increment: false,
                comment: None,
            }),
            column_def_to_api_json(&ColumnDef {
                name: "fields".to_string(),
                data_type: "TEXT".to_string(),
                is_nullable: true,
                column_default: None,
                is_primary_key: false,
                is_auto_increment: false,
                comment: Some("JSON-encoded field map".to_string()),
            }),
        ],
        _ => return None,
    })
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
