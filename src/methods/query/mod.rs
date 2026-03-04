mod aggregates;
mod parser;
mod redis_keys;
mod sql_parser;
mod type_tables;

use crate::metadata::RowStorageMode;
use crate::methods::common::AppError;
use crate::methods::crud;
use crate::methods::discovery;
use crate::models::IndexDef;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;
use std::time::Instant;

use parser::{json_value_cmp, json_value_to_cmp_str, row_values_for_columns};
use redis_keys::execute_redis_keys_scan;
use sql_parser::{evaluate_where, parse_sql, ParsedStatement};
use type_tables::execute_type_table_scan;

/// Fetch one row as column values; returns None if the row is missing or not an object/hash.
fn fetch_row_values(
    client: &mut RedisClient,
    table: &str,
    pk: &str,
    columns: &[String],
    mode: RowStorageMode,
) -> Result<Option<Vec<JsonValue>>, AppError> {
    match mode {
        RowStorageMode::Json => {
            let opt = client
                .metadata()
                .get_row_json(table, pk)
                .map_err(AppError::Backend)?;
            let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                return Ok(None);
            };
            Ok(Some(row_values_for_columns(columns, pk, None, Some(&obj))))
        }
        RowStorageMode::Hash => {
            let opt = client
                .metadata()
                .get_row_hash(table, pk)
                .map_err(AppError::Backend)?;
            let Some(map) = opt.as_ref() else {
                return Ok(None);
            };
            Ok(Some(row_values_for_columns(columns, pk, Some(map), None)))
        }
    }
}

/// Fetch multiple rows in one pipelined batch. Returns row values in same order as pks; skips missing/empty.
fn fetch_rows_batch(
    client: &mut RedisClient,
    table: &str,
    columns: &[String],
    mode: RowStorageMode,
    pks: &[String],
) -> Result<Vec<Vec<JsonValue>>, AppError> {
    if pks.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(pks.len());
    match mode {
        RowStorageMode::Hash => {
            let maps = client
                .metadata()
                .get_rows_hash_batch(table, pks)
                .map_err(AppError::Backend)?;
            for (pk, map) in pks.iter().zip(maps) {
                if map.is_empty() {
                    continue;
                }
                out.push(row_values_for_columns(columns, pk, Some(&map), None));
            }
        }
        RowStorageMode::Json => {
            let opts = client
                .metadata()
                .get_rows_json_batch(table, pks)
                .map_err(AppError::Backend)?;
            for (pk, opt) in pks.iter().zip(opts) {
                let Some(JsonValue::Object(obj)) = opt.as_ref() else {
                    continue;
                };
                out.push(row_values_for_columns(columns, pk, None, Some(obj)));
            }
        }
    }
    Ok(out)
}

/// Parse CREATE [UNIQUE] INDEX name ON table (col1, col2) and persist index metadata.
fn execute_create_index(client: &mut RedisClient, q: &str) -> Result<JsonValue, AppError> {
    let upper = q.to_uppercase();
    if !upper.starts_with("CREATE ") || !upper.contains(" INDEX ") {
        return Err(AppError::Backend("Not a CREATE INDEX statement".into()));
    }
    let after_create = q[7..].trim_start();
    let upper_after = after_create.to_uppercase();
    let (after_create, is_unique) = if upper_after.starts_with("UNIQUE ") {
        (after_create[6..].trim_start(), true)
    } else {
        (after_create, false)
    };
    let on_pos = after_create
        .to_uppercase()
        .find(" ON ")
        .ok_or_else(|| AppError::Backend("CREATE INDEX: missing ON".into()))?;
    let name_part = after_create[..on_pos].trim();
    let idx_name = name_part
        .strip_prefix("INDEX")
        .or_else(|| name_part.strip_prefix("index"))
        .ok_or_else(|| AppError::Backend("CREATE INDEX: missing index name".into()))?
        .trim()
        .trim_matches('"')
        .to_string();
    if idx_name.is_empty() {
        return Err(AppError::Backend("CREATE INDEX: empty index name".into()));
    }
    let after_on = after_create[on_pos + 4..].trim();
    let paren = after_on
        .find('(')
        .ok_or_else(|| AppError::Backend("CREATE INDEX: missing (".into()))?;
    let table = after_on[..paren].trim().trim_matches('"').to_string();
    let cols_part = after_on[paren + 1..].trim();
    let close = cols_part
        .rfind(')')
        .ok_or_else(|| AppError::Backend("CREATE INDEX: missing )".into()))?;
    let cols_str = cols_part[..close].trim();
    let columns: Vec<String> = cols_str
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if columns.is_empty() {
        return Err(AppError::Backend(
            "CREATE INDEX: at least one column required".into(),
        ));
    }
    log::info!(
        "execute_create_index: table={table} index={idx_name} columns={}",
        columns.len()
    );
    let mut indexes = client.metadata().get_table_indexes(&table)?;
    if indexes.iter().any(|i| i.index_name == idx_name) {
        return Err(AppError::Conflict(format!(
            "Index '{idx_name}' already exists"
        )));
    }
    indexes.push(IndexDef {
        index_name: idx_name,
        columns,
        is_unique,
    });
    client.metadata().set_table_indexes(&table, &indexes)?;
    Ok(serde_json::json!({
        "columns": [],
        "rows": [],
        "affected_rows": 0u64,
        "truncated": false
    }))
}

/// Execute a SELECT that was parsed by sqlparser. Used for metadata tables (not virtual key tables).
#[allow(clippy::too_many_lines)]
fn execute_select_parsed(
    client: &mut RedisClient,
    sel: &sql_parser::SelectQuery,
    page: u64,
    page_size: u64,
    start: std::time::Instant,
) -> Result<JsonValue, AppError> {
    let use_aggregates = !sel.aggregates.is_empty() || !sel.group_by.is_empty();
    if use_aggregates {
        return execute_select_aggregated(client, sel, page, page_size, start);
    }

    let table = sel.table.as_str();
    let column_names: Vec<String> = if sel.columns.len() == 1 && sel.columns[0].name == "*" {
        match client.metadata().get_table_columns(table)? {
            Some(cols) if !cols.is_empty() => cols.into_iter().map(|x| x.name).collect(),
            _ => client
                .infer_columns_from_data(table)?
                .map(|c| c.into_iter().map(|x| x.name).collect())
                .unwrap_or_default(),
        }
    } else {
        sel.columns.iter().map(|c| c.name.clone()).collect()
    };
    let output_columns: Vec<String> = if sel.columns.len() == 1 && sel.columns[0].name == "*" {
        column_names.clone()
    } else {
        sel.columns
            .iter()
            .map(|c| c.alias.as_ref().unwrap_or(&c.name).clone())
            .collect()
    };
    let _pk_col = client.get_table_pk_or_inferred(table)?.ok_or_else(|| {
        AppError::Backend(format!(
            "Table '{table}' has no rows and no primary key metadata"
        ))
    })?;
    let mode = client.metadata().get_table_mode(table)?;
    let need_full_scan = sel.where_clause.is_some() || !sel.order_by.is_empty();
    let skip = usize::try_from((page.saturating_sub(1)) * page_size).unwrap_or(0);
    let take_n = usize::try_from(page_size).unwrap_or(usize::MAX);
    let sql_limit = sel.limit.map(|n| usize::try_from(n).unwrap_or(usize::MAX));

    let (total_count, page_rows) = if need_full_scan {
        let all_keys = client.metadata().list_row_keys(table)?;
        let mut rows_with_pk: Vec<(String, Vec<JsonValue>)> = Vec::new();
        for pk in &all_keys {
            let Some(row_values) = fetch_row_values(client, table, pk, &column_names, mode)? else {
                continue;
            };
            let matches = sel.where_clause.as_ref().is_none_or(|expr| {
                let row_strs: Vec<String> = row_values.iter().map(json_value_to_cmp_str).collect();
                evaluate_where(&column_names, &row_strs, expr)
            });
            if matches {
                rows_with_pk.push((pk.clone(), row_values));
            }
        }
        if let Some((ref col, asc)) = sel.order_by.first() {
            if let Some(idx) = column_names.iter().position(|c| c == col) {
                rows_with_pk.sort_by(|a, b| {
                    let ord = json_value_cmp(&a.1[idx], &b.1[idx]);
                    if *asc {
                        ord
                    } else {
                        ord.reverse()
                    }
                });
            }
        }
        if let Some(limit) = sql_limit {
            rows_with_pk.truncate(limit);
        }
        let total = rows_with_pk.len();
        let page_rows: Vec<Vec<JsonValue>> = rows_with_pk
            .into_iter()
            .skip(skip)
            .take(take_n)
            .map(|(_, r)| r)
            .collect();
        (total as u64, page_rows)
    } else if sel.where_clause.is_none() {
        let (page_keys, paged_has_more) =
            client.metadata().list_row_keys_paged(table, skip, take_n)?;
        let total = if paged_has_more {
            skip as u64 + (page_keys.len() as u64) + 1
        } else {
            skip as u64 + (page_keys.len() as u64)
        };
        let out = fetch_rows_batch(client, table, &column_names, mode, &page_keys)?;
        (total, out)
    } else {
        let all_keys = client.metadata().list_row_keys(table)?;
        let mut filtered: Vec<String> = all_keys;
        if let Some(ref expr) = sel.where_clause {
            filtered.retain(|pk| {
                let Some(row_values) = fetch_row_values(client, table, pk, &column_names, mode)
                    .ok()
                    .flatten()
                else {
                    return false;
                };
                let row_strs: Vec<String> = row_values.iter().map(json_value_to_cmp_str).collect();
                evaluate_where(&column_names, &row_strs, expr)
            });
        }
        if let Some(limit) = sql_limit {
            filtered.truncate(limit);
        }
        let total = filtered.len();
        let page_keys: Vec<String> = filtered.into_iter().skip(skip).take(take_n).collect();
        let out = fetch_rows_batch(client, table, &column_names, mode, &page_keys)?;
        (total as u64, out)
    };

    let rows: Vec<JsonValue> = page_rows.into_iter().map(JsonValue::Array).collect();
    let has_more = (page.saturating_sub(1)) * page_size + (rows.len() as u64) < total_count;
    let mut result = serde_json::json!({
        "columns": output_columns,
        "rows": rows,
        "affected_rows": 0u64,
        "truncated": false,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_rows": total_count,
            "has_more": has_more
        }
    });
    if let Some(obj) = result.as_object_mut() {
        obj.insert(
            "execution_time_ms".to_string(),
            JsonValue::Number(serde_json::Number::from(
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            )),
        );
    }
    Ok(result)
}

#[allow(dead_code)]
fn execute_select_aggregated(
    client: &mut RedisClient,
    sel: &sql_parser::SelectQuery,
    page: u64,
    page_size: u64,
    start: std::time::Instant,
) -> Result<JsonValue, AppError> {
    let table = sel.table.as_str();
    let column_names: Vec<String> = match client.metadata().get_table_columns(table)? {
        Some(cols) if !cols.is_empty() => cols.into_iter().map(|x| x.name).collect(),
        _ => client
            .infer_columns_from_data(table)?
            .map(|c| c.into_iter().map(|x| x.name).collect())
            .unwrap_or_default(),
    };
    let mode = client.metadata().get_table_mode(table)?;
    let all_keys = client.metadata().list_row_keys(table)?;
    let mut rows: Vec<Vec<JsonValue>> = Vec::new();
    for pk in &all_keys {
        let Some(row_values) = fetch_row_values(client, table, pk, &column_names, mode)? else {
            continue;
        };
        let matches = sel.where_clause.as_ref().is_none_or(|expr| {
            let row_strs: Vec<String> = row_values.iter().map(json_value_to_cmp_str).collect();
            evaluate_where(&column_names, &row_strs, expr)
        });
        if matches {
            rows.push(row_values);
        }
    }
    let (output_columns, agg_rows) =
        aggregates::compute_aggregates(&column_names, &rows, &sel.group_by, &sel.aggregates);
    let skip = usize::try_from((page.saturating_sub(1)) * page_size).unwrap_or(0);
    let take_n = usize::try_from(page_size).unwrap_or(usize::MAX);
    let total_count = agg_rows.len() as u64;
    let rows_json: Vec<JsonValue> = agg_rows
        .into_iter()
        .skip(skip)
        .take(take_n)
        .map(JsonValue::Array)
        .collect();
    let has_more = (page.saturating_sub(1)) * page_size + (rows_json.len() as u64) < total_count;
    let mut result = serde_json::json!({
        "columns": output_columns,
        "rows": rows_json,
        "affected_rows": 0u64,
        "truncated": false,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_rows": total_count,
            "has_more": has_more
        }
    });
    if let Some(obj) = result.as_object_mut() {
        obj.insert(
            "execution_time_ms".to_string(),
            JsonValue::Number(serde_json::Number::from(
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            )),
        );
    }
    Ok(result)
}

fn execute_insert_parsed(
    client: &mut RedisClient,
    ins: &sql_parser::InsertQuery,
    start: std::time::Instant,
) -> Result<JsonValue, AppError> {
    let table = ins.table.as_str();
    let mut affected = 0u64;
    for row in &ins.rows {
        let mut data = serde_json::Map::new();
        for (col, val) in ins.columns.iter().zip(row.iter()) {
            data.insert(col.clone(), JsonValue::String(val.clone()));
        }
        let n = crud::insert_record(client, None, table, &data).map_err(AppError::Backend)?;
        affected += n;
    }
    let mut result = serde_json::json!({
        "columns": serde_json::Value::Array(vec![]),
        "rows": serde_json::Value::Array(vec![]),
        "affected_rows": affected,
        "truncated": false
    });
    if let Some(obj) = result.as_object_mut() {
        obj.insert(
            "execution_time_ms".to_string(),
            JsonValue::Number(serde_json::Number::from(
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            )),
        );
    }
    Ok(result)
}

fn execute_update_parsed(
    client: &mut RedisClient,
    upd: &sql_parser::UpdateQuery,
    start: std::time::Instant,
) -> Result<JsonValue, AppError> {
    let table = upd.table.as_str();
    let pk_col = client
        .get_table_pk_or_inferred(table)?
        .ok_or_else(|| AppError::Backend(format!("Table '{table}' has no primary key")))?;
    let mode = client.metadata().get_table_mode(table)?;
    let column_names = client.metadata().get_table_columns(table)?.map_or_else(
        || upd.assignments.iter().map(|(k, _)| k.clone()).collect(),
        |c| c.into_iter().map(|x| x.name).collect::<Vec<_>>(),
    );
    let all_keys = client.metadata().list_row_keys(table)?;
    let mut affected = 0u64;
    for pk in &all_keys {
        let Some(row_values) = fetch_row_values(client, table, pk, &column_names, mode)? else {
            continue;
        };
        let row_strs: Vec<String> = row_values.iter().map(json_value_to_cmp_str).collect();
        let matches = upd
            .where_clause
            .as_ref()
            .is_none_or(|expr| sql_parser::evaluate_where(&column_names, &row_strs, expr));
        if !matches {
            continue;
        }
        let pk_value = JsonValue::String(pk.clone());
        for (col, val) in &upd.assignments {
            let n = crud::update_record(
                client,
                None,
                table,
                &pk_col,
                &pk_value,
                col,
                &JsonValue::String(val.clone()),
            )?;
            affected += n;
        }
    }
    let mut result = serde_json::json!({
        "columns": serde_json::Value::Array(vec![]),
        "rows": serde_json::Value::Array(vec![]),
        "affected_rows": affected,
        "truncated": false
    });
    if let Some(obj) = result.as_object_mut() {
        obj.insert(
            "execution_time_ms".to_string(),
            JsonValue::Number(serde_json::Number::from(
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            )),
        );
    }
    Ok(result)
}

fn execute_delete_parsed(
    client: &mut RedisClient,
    del: &sql_parser::DeleteQuery,
    start: std::time::Instant,
) -> Result<JsonValue, AppError> {
    let table = del.table.as_str();
    let pk_col = client
        .get_table_pk_or_inferred(table)?
        .ok_or_else(|| AppError::Backend(format!("Table '{table}' has no primary key")))?;
    let mode = client.metadata().get_table_mode(table)?;
    let column_names = client
        .metadata()
        .get_table_columns(table)?
        .map(|c| c.into_iter().map(|x| x.name).collect::<Vec<_>>())
        .unwrap_or_default();
    let all_keys = client.metadata().list_row_keys(table)?;
    let mut affected = 0u64;
    for pk in &all_keys {
        let Some(row_values) = fetch_row_values(client, table, pk, &column_names, mode)? else {
            continue;
        };
        let row_strs: Vec<String> = row_values.iter().map(json_value_to_cmp_str).collect();
        let matches = del
            .where_clause
            .as_ref()
            .is_none_or(|expr| sql_parser::evaluate_where(&column_names, &row_strs, expr));
        if matches {
            let n =
                crud::delete_record(client, None, table, &pk_col, &JsonValue::String(pk.clone()))
                    .map_err(AppError::Backend)?;
            affected += n;
        }
    }
    let mut result = serde_json::json!({
        "columns": serde_json::Value::Array(vec![]),
        "rows": serde_json::Value::Array(vec![]),
        "affected_rows": affected,
        "truncated": false
    });
    if let Some(obj) = result.as_object_mut() {
        obj.insert(
            "execution_time_ms".to_string(),
            JsonValue::Number(serde_json::Number::from(
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            )),
        );
    }
    Ok(result)
}

/// Parse "DROP TABLE [IF EXISTS] [schema.]`table_name`" and return the table name (no schema). Handles "`0"."test_data`" from UI.
fn parse_drop_table(q: &str) -> Option<String> {
    let upper = q.trim().to_uppercase();
    if !upper.starts_with("DROP TABLE") {
        return None;
    }
    let rest = q.trim()[10..].trim_start(); // after "DROP TABLE"
    let rest_upper = rest.to_uppercase();
    let after_keywords = if rest_upper.starts_with("IF EXISTS") {
        rest[9..].trim_start()
    } else {
        rest
    };
    let raw = after_keywords.trim_end_matches(';').trim();
    let table = raw
        .rfind("\".\"")
        .map_or_else(
            || {
                raw.find('.').map_or_else(
                    || raw.trim_matches('"'),
                    |dot| raw[dot + 1..].trim().trim_matches('"'),
                )
            },
            |dot| raw[dot + 3..].trim().trim_matches('"'),
        )
        .to_string();
    if table.is_empty() {
        return None;
    }
    Some(table)
}

/// If `after_from` is "( inner ) AS alias" (app Total Limit wrapper), return the inner query.
/// Finds the matching ')' while skipping single- and double-quoted strings.
fn extract_wrapped_subquery(after_from: &str) -> Option<String> {
    let s = after_from.trim();
    if !s.starts_with('(') {
        return None;
    }
    let mut depth: i32 = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        let b = bytes[i];
        if in_single {
            if b == b'\'' && (i + 1 >= bytes.len() || bytes[i + 1] != b'\'') {
                in_single = false;
            } else if b == b'\'' && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 1;
            }
            i += 1;
            continue;
        }
        if in_double {
            if b == b'"' && (i + 1 >= bytes.len() || bytes[i + 1] != b'"') {
                in_double = false;
            } else if b == b'"' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 1;
            }
            i += 1;
            continue;
        }
        match b {
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                if depth == 0 {
                    let inner = s[1..i].trim();
                    return if inner.to_uppercase().starts_with("SELECT ") {
                        Some(inner.to_string())
                    } else {
                        None
                    };
                }
                i += 1;
            }
            b'\'' => {
                in_single = true;
                i += 1;
            }
            b'"' => {
                in_double = true;
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

#[allow(clippy::too_many_lines)]
pub fn execute_query(
    client: &mut RedisClient,
    query: &str,
    page: u64,
    page_size: u64,
    app_limit: Option<u64>,
    json_path: Option<&str>,
) -> Result<JsonValue, AppError> {
    let start = Instant::now();
    let q = query.trim();
    log::debug!(
        "execute_query: page={page} page_size={page_size} app_limit={:?} query_len={}",
        app_limit,
        q.len()
    );
    if q.is_empty() {
        return Err(AppError::Backend("Empty query".into()));
    }
    let upper = q.to_uppercase();
    if upper.starts_with("CREATE ") && upper.contains(" INDEX ") {
        log::debug!("execute_query: delegating to execute_create_index");
        return execute_create_index(client, q);
    }
    if let Some(table) = parse_drop_table(q) {
        log::debug!("execute_query: delegating to drop_table for {table}");
        client
            .metadata()
            .drop_table(&table)
            .map_err(AppError::Backend)?;
        return Ok(serde_json::json!({
            "columns": [],
            "rows": [],
            "affected_rows": 0u64,
            "truncated": false,
            "execution_time_ms": u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
        }));
    }

    if let Ok(parsed) = parse_sql(q) {
        match parsed {
            ParsedStatement::Insert(ins) => {
                return execute_insert_parsed(client, &ins, start);
            }
            ParsedStatement::Update(upd) => {
                return execute_update_parsed(client, &upd, start);
            }
            ParsedStatement::Delete(del) => {
                return execute_delete_parsed(client, &del, start);
            }
            _ => {}
        }
    }

    if !upper.starts_with("SELECT") {
        return Err(AppError::Backend("Only SELECT is supported".into()));
    }

    let from_idx = upper
        .find(" FROM ")
        .ok_or_else(|| AppError::Backend("Missing FROM clause".into()))?;
    let select_part = q[..from_idx].trim();
    let after_from = q[from_idx + 6..].trim();
    if after_from.trim_start().starts_with('(') {
        if let Some(inner) = extract_wrapped_subquery(after_from) {
            let upper_select = select_part.to_uppercase();
            if upper_select.contains("COUNT(*)") && upper_select.trim().starts_with("SELECT") {
                log::debug!(
                    "execute_query: COUNT(*) FROM (subquery), running inner for total count"
                );
                let inner_result = execute_query(client, &inner, 1, 1, None, json_path)?;
                let total: u64 = inner_result
                    .get("pagination")
                    .and_then(|p| p.get("total_rows"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let elapsed = start.elapsed().as_millis();
                return Ok(serde_json::json!({
                    "columns": ["COUNT(*)"],
                    "rows": [[total]],
                    "affected_rows": 0u64,
                    "truncated": false,
                    "pagination": { "page": 1, "page_size": 1, "total_rows": total, "has_more": false },
                    "execution_time_ms": u64::try_from(elapsed).unwrap_or(u64::MAX)
                }));
            }
            log::debug!(
                "execute_query: unwrapping app subquery (Total Limit), inner query len={}",
                inner.len()
            );
            return execute_query(client, &inner, page, page_size, app_limit, json_path);
        }
        return Err(AppError::Backend(
            "Subqueries are not supported. Use a table name directly (e.g. SELECT * FROM __redis_keys__).".into(),
        ));
    }

    let Ok(ParsedStatement::Select(sel)) = parse_sql(q) else {
        return Err(AppError::Backend("Invalid or unsupported SELECT".into()));
    };

    let table = sel.table.as_str();
    if discovery::is_type_virtual_table(table) {
        let mut result = execute_type_table_scan(client, table, q, page, page_size)?;
        if let Some(obj) = result.as_object_mut() {
            obj.insert(
                "execution_time_ms".to_string(),
                JsonValue::Number(serde_json::Number::from(
                    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
                )),
            );
        }
        return Ok(result);
    }
    let (is_virtual_table, pattern_override) = match discovery::virtual_table_scan_target(table) {
        Some(discovery::VirtualScanTarget::AllKeys) => (true, None),
        Some(discovery::VirtualScanTarget::Pattern(prefix)) => (true, Some(format!("{prefix}:*"))),
        None => (false, None),
    };
    log::info!(
        "execute_query: table={table} is_virtual={is_virtual_table} pattern_override={:?}",
        pattern_override.as_deref()
    );
    log::debug!("execute_query: raw query={q}");
    let mut result = if is_virtual_table {
        execute_redis_keys_scan(
            client,
            q,
            page,
            page_size,
            app_limit,
            pattern_override.as_deref(),
            json_path,
        )
        .map_err(AppError::Backend)?
    } else {
        execute_select_parsed(client, &sel, page, page_size, start)?
    };

    if let Some(obj) = result.as_object_mut() {
        obj.insert(
            "execution_time_ms".to_string(),
            JsonValue::Number(serde_json::Number::from(
                u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
            )),
        );
    }
    Ok(result)
}

#[cfg(test)]
mod query_tests {
    use super::extract_wrapped_subquery;

    #[test]
    fn test_extract_wrapped_subquery() {
        let wrapped = r#"(SELECT * FROM "__redis_keys__" WHERE id > 5 ORDER BY type DESC LIMIT 1000) AS limited_subset"#;
        let inner = extract_wrapped_subquery(wrapped).expect("should extract");
        assert!(inner.starts_with("SELECT * FROM"));
        assert!(inner.contains("__redis_keys__"));
        assert!(inner.contains("LIMIT 1000"));
        assert!(extract_wrapped_subquery("SELECT * FROM t").is_none());
        assert!(extract_wrapped_subquery("(invalid) AS x").is_none());
    }
}
