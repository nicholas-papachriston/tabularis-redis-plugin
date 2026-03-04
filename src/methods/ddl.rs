use crate::methods::common::AppError;
use crate::methods::discovery;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;
use std::fmt::Write;

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub fn get_create_table_sql(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
) -> Result<JsonValue, AppError> {
    let columns = client
        .get_table_columns(table)?
        .ok_or_else(|| AppError::NotFound(format!("Table '{table}' not found")))?;
    let pk = client.get_table_pk(table)?.unwrap_or_default();
    let mut parts: Vec<String> = Vec::new();
    for c in &columns {
        let mut def = format!("{} {}", quote_ident(&c.name), c.data_type);
        if !c.is_nullable {
            def.push_str(" NOT NULL");
        }
        if let Some(ref d) = c.column_default {
            let _ = write!(def, " DEFAULT {d}");
        }
        parts.push(def);
    }
    if !pk.is_empty() {
        parts.push(format!("PRIMARY KEY ({})", quote_ident(&pk)));
    }
    let sql = format!(
        "CREATE TABLE {} (\n  {}\n)",
        quote_ident(table),
        parts.join(",\n  ")
    );
    Ok(JsonValue::String(sql))
}

pub fn get_add_column_sql(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    column: &JsonValue,
) -> Result<JsonValue, AppError> {
    if table.is_empty() {
        return Err(AppError::InvalidParams("table is required".into()));
    }
    let name = column
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::InvalidParams("column.name is required".into()))?;
    let data_type = column
        .get("data_type")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("TEXT");
    let nullable = column
        .get("is_nullable")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let default = column.get("column_default").and_then(|v| v.as_str());
    let mut sql = format!(
        "ALTER TABLE {} ADD COLUMN {} {}",
        quote_ident(table),
        quote_ident(name),
        data_type
    );
    if !nullable {
        sql.push_str(" NOT NULL");
    }
    if let Some(d) = default {
        let _ = write!(sql, " DEFAULT {d}");
    }
    Ok(JsonValue::String(sql))
}

pub fn get_alter_column_sql(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    column: &JsonValue,
) -> Result<JsonValue, AppError> {
    if table.is_empty() {
        return Err(AppError::InvalidParams("table is required".into()));
    }
    let name = column
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::InvalidParams("column.name is required".into()))?;
    let data_type = column
        .get("data_type")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("TEXT");
    let nullable = column
        .get("is_nullable")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let mut sql = format!(
        "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
        quote_ident(table),
        quote_ident(name),
        data_type
    );
    if !nullable {
        sql.push_str(" NOT NULL");
    }
    Ok(JsonValue::String(sql))
}

pub fn get_create_index_sql(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    index: &JsonValue,
) -> Result<JsonValue, AppError> {
    if table.is_empty() {
        return Err(AppError::InvalidParams("table is required".into()));
    }
    let name = index
        .get("index_name")
        .or_else(|| index.get("name"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::InvalidParams("index.index_name or index.name is required".into())
        })?;
    let cols = index
        .get("columns")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
        .ok_or_else(|| {
            AppError::InvalidParams("index.columns (non-empty array) is required".into())
        })?;
    let cols_str: String = cols
        .iter()
        .filter_map(|v| v.as_str().map(quote_ident))
        .collect::<Vec<_>>()
        .join(", ");
    if cols_str.is_empty() {
        return Err(AppError::InvalidParams(
            "index.columns must contain at least one column name".into(),
        ));
    }
    let unique = index
        .get("is_unique")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let u = if unique { "UNIQUE " } else { "" };
    let sql = format!(
        "CREATE {}INDEX {} ON {} ({})",
        u,
        quote_ident(name),
        quote_ident(table),
        cols_str
    );
    Ok(JsonValue::String(sql))
}

pub fn get_create_foreign_key_sql(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    fk: &JsonValue,
) -> Result<JsonValue, AppError> {
    if table.is_empty() {
        return Err(AppError::InvalidParams("table is required".into()));
    }
    let name = fk
        .get("constraint_name")
        .or_else(|| fk.get("name"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("fk");
    let column = fk
        .get("column_name")
        .or_else(|| fk.get("column"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::InvalidParams("fk.column_name or fk.column is required".into()))?;
    let ref_table = fk
        .get("referenced_table")
        .or_else(|| fk.get("ref_table"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::InvalidParams("fk.referenced_table or fk.ref_table is required".into())
        })?;
    let ref_column = fk
        .get("referenced_column")
        .or_else(|| fk.get("ref_column"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::InvalidParams("fk.referenced_column or fk.ref_column is required".into())
        })?;
    let on_delete = fk.get("on_delete").and_then(|v| v.as_str());
    let on_update = fk.get("on_update").and_then(|v| v.as_str());
    let mut sql = format!(
        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
        quote_ident(table),
        quote_ident(name),
        quote_ident(column),
        quote_ident(ref_table),
        quote_ident(ref_column)
    );
    if let Some(a) = on_delete {
        let _ = write!(sql, " ON DELETE {a}");
    }
    if let Some(a) = on_update {
        let _ = write!(sql, " ON UPDATE {a}");
    }
    Ok(JsonValue::String(sql))
}

pub fn drop_index(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    index_name: &str,
) -> Result<JsonValue, AppError> {
    log::debug!("drop_index: table={table} index_name={index_name}");
    if index_name.is_empty() {
        return Err(AppError::InvalidParams("index_name is required".into()));
    }
    let mut indexes = client.get_table_indexes(table)?;
    let pos = indexes
        .iter()
        .position(|i| i.index_name == index_name)
        .ok_or_else(|| AppError::NotFound(format!("Index '{index_name}' does not exist")))?;
    indexes.remove(pos);
    client.set_table_indexes(table, &indexes)?;
    log::info!("drop_index: removed index {index_name} from table {table}");
    Ok(JsonValue::Null)
}

pub fn drop_foreign_key(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
    constraint_name: &str,
) -> Result<JsonValue, String> {
    log::warn!("drop_foreign_key: not supported (table={table} constraint={constraint_name})");
    Err("Foreign keys are not supported by the Redis plugin".to_string())
}

pub fn drop_table(
    client: &mut RedisClient,
    _schema: Option<&str>,
    table: &str,
) -> Result<JsonValue, AppError> {
    if table.is_empty() {
        return Err(AppError::InvalidParams("table is required".into()));
    }
    if table == discovery::REDIS_KEYS_TABLE || discovery::is_key_pattern_table(table) {
        return Err(AppError::InvalidParams(format!(
            "Cannot drop virtual table '{table}'"
        )));
    }
    client.drop_table(table).map_err(AppError::Backend)?;
    log::info!("drop_table: dropped table {table}");
    Ok(JsonValue::Null)
}
