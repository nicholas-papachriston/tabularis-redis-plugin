mod aggregates;
mod parser;
mod redis_keys;
mod sql_parser;

use crate::metadata::RowStorageMode;
use crate::methods::common::AppError;
use crate::methods::crud;
use crate::methods::ddl;
use crate::methods::discovery;
use crate::models::IndexDef;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;
use std::time::Instant;

use parser::{
    json_value_to_cmp_str, parse_order_by, parse_where, row_values_for_columns, PageResult,
};
use redis_keys::execute_redis_keys_scan;
use sql_parser::{evaluate_where, parse_sql, ParsedStatement};

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
    let mut indexes = client.get_table_indexes(&table)?;
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
    client.set_table_indexes(&table, &indexes)?;
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
        match client.get_table_columns(table)? {
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
    let mode = client.get_table_mode(table)?;
    let need_full_scan = sel.where_clause.is_some() || !sel.order_by.is_empty();
    let skip = usize::try_from((page.saturating_sub(1)) * page_size).unwrap_or(0);
    let take_n = usize::try_from(page_size).unwrap_or(usize::MAX);
    let sql_limit = sel.limit.map(|n| usize::try_from(n).unwrap_or(usize::MAX));

    let (total_count, page_rows) = if need_full_scan {
        let all_keys = client.list_row_keys(table)?;
        let mut rows_with_pk: Vec<(String, Vec<JsonValue>)> = Vec::new();
        for pk in &all_keys {
            let row_values: Vec<JsonValue> = match mode {
                RowStorageMode::Json => {
                    let opt = client.get_row_json(table, pk)?;
                    let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                        continue;
                    };
                    row_values_for_columns(&column_names, pk, None, Some(&obj))
                }
                RowStorageMode::Hash => {
                    let opt = client.get_row_hash(table, pk)?;
                    let Some(map) = opt.as_ref() else {
                        continue;
                    };
                    row_values_for_columns(&column_names, pk, Some(map), None)
                }
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
                    let a_str = json_value_to_cmp_str(&a.1[idx]);
                    let b_str = json_value_to_cmp_str(&b.1[idx]);
                    let ord = a_str.cmp(&b_str);
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
        let (page_keys, paged_has_more) = client.list_row_keys_paged(table, skip, take_n)?;
        let total = if paged_has_more {
            skip as u64 + (page_keys.len() as u64) + 1
        } else {
            skip as u64 + (page_keys.len() as u64)
        };
        let mut out = Vec::new();
        for pk in &page_keys {
            let row_values: Vec<JsonValue> = match mode {
                RowStorageMode::Json => {
                    let opt = client.get_row_json(table, pk)?;
                    let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                        continue;
                    };
                    row_values_for_columns(&column_names, pk, None, Some(&obj))
                }
                RowStorageMode::Hash => {
                    let opt = client.get_row_hash(table, pk)?;
                    let Some(map) = opt.as_ref() else {
                        continue;
                    };
                    row_values_for_columns(&column_names, pk, Some(map), None)
                }
            };
            out.push(row_values);
        }
        (total, out)
    } else {
        let all_keys = client.list_row_keys(table)?;
        let mut filtered: Vec<String> = all_keys;
        if let Some(ref expr) = sel.where_clause {
            filtered.retain(|pk| {
                let row_values: Vec<JsonValue> = match mode {
                    RowStorageMode::Json => {
                        let opt = client.get_row_json(table, pk).ok().flatten();
                        let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                            return false;
                        };
                        row_values_for_columns(&column_names, pk, None, Some(&obj))
                    }
                    RowStorageMode::Hash => {
                        let opt = client.get_row_hash(table, pk).ok().flatten();
                        let Some(map) = opt else {
                            return false;
                        };
                        row_values_for_columns(&column_names, pk, Some(&map), None)
                    }
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
        let mut out = Vec::new();
        for pk in &page_keys {
            let row_values: Vec<JsonValue> = match mode {
                RowStorageMode::Json => {
                    let opt = client.get_row_json(table, pk)?;
                    let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                        continue;
                    };
                    row_values_for_columns(&column_names, pk, None, Some(&obj))
                }
                RowStorageMode::Hash => {
                    let opt = client.get_row_hash(table, pk)?;
                    let Some(map) = opt.as_ref() else {
                        continue;
                    };
                    row_values_for_columns(&column_names, pk, Some(map), None)
                }
            };
            out.push(row_values);
        }
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
    let column_names: Vec<String> = match client.get_table_columns(table)? {
        Some(cols) if !cols.is_empty() => cols.into_iter().map(|x| x.name).collect(),
        _ => client
            .infer_columns_from_data(table)?
            .map(|c| c.into_iter().map(|x| x.name).collect())
            .unwrap_or_default(),
    };
    let mode = client.get_table_mode(table)?;
    let all_keys = client.list_row_keys(table)?;
    let mut rows: Vec<Vec<JsonValue>> = Vec::new();
    for pk in &all_keys {
        let row_values: Vec<JsonValue> = match mode {
            RowStorageMode::Json => {
                let opt = client.get_row_json(table, pk)?;
                let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                    continue;
                };
                row_values_for_columns(&column_names, pk, None, Some(&obj))
            }
            RowStorageMode::Hash => {
                let opt = client.get_row_hash(table, pk)?;
                let Some(map) = opt.as_ref() else {
                    continue;
                };
                row_values_for_columns(&column_names, pk, Some(map), None)
            }
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
    let mode = client.get_table_mode(table)?;
    let column_names = client.get_table_columns(table)?.map_or_else(
        || upd.assignments.iter().map(|(k, _)| k.clone()).collect(),
        |c| c.into_iter().map(|x| x.name).collect::<Vec<_>>(),
    );
    let all_keys = client.list_row_keys(table)?;
    let mut affected = 0u64;
    for pk in &all_keys {
        let row_values: Vec<JsonValue> = match mode {
            RowStorageMode::Json => {
                let opt = client.get_row_json(table, pk)?;
                let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                    continue;
                };
                row_values_for_columns(&column_names, pk, None, Some(&obj))
            }
            RowStorageMode::Hash => {
                let opt = client.get_row_hash(table, pk)?;
                let Some(map) = opt.as_ref() else {
                    continue;
                };
                row_values_for_columns(&column_names, pk, Some(map), None)
            }
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
    let mode = client.get_table_mode(table)?;
    let column_names = client
        .get_table_columns(table)?
        .map(|c| c.into_iter().map(|x| x.name).collect::<Vec<_>>())
        .unwrap_or_default();
    let all_keys = client.list_row_keys(table)?;
    let mut affected = 0u64;
    for pk in &all_keys {
        let row_values: Vec<JsonValue> = match mode {
            RowStorageMode::Json => {
                let opt = client.get_row_json(table, pk)?;
                let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                    continue;
                };
                row_values_for_columns(&column_names, pk, None, Some(&obj))
            }
            RowStorageMode::Hash => {
                let opt = client.get_row_hash(table, pk)?;
                let Some(map) = opt.as_ref() else {
                    continue;
                };
                row_values_for_columns(&column_names, pk, Some(map), None)
            }
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

/// Parse "DROP TABLE [IF EXISTS] `table_name`" and return the table name if matched.
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
    let table = after_keywords
        .trim_end_matches(';')
        .trim()
        .trim_matches('"')
        .to_string();
    if table.is_empty() {
        return None;
    }
    Some(table)
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
        ddl::drop_table(client, None, &table)?;
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

    if let Ok(ParsedStatement::Select(ref sel)) = parse_sql(q) {
        let table = sel.table.as_str();
        if table != discovery::REDIS_KEYS_TABLE && !discovery::is_key_pattern_table(table) {
            return execute_select_parsed(client, sel, page, page_size, start);
        }
    }

    let from_idx = upper
        .find(" FROM ")
        .ok_or_else(|| AppError::Backend("Missing FROM clause".into()))?;
    let select_part = q[..from_idx].trim();
    let after_from = q[from_idx + 6..].trim();
    let table_raw = after_from
        .split_whitespace()
        .next()
        .ok_or_else(|| AppError::Backend("Missing table name after FROM".into()))?
        .trim();
    if table_raw.starts_with('(') {
        return Err(AppError::Backend(
            "Subqueries are not supported. Use a table name directly (e.g. SELECT * FROM __redis_keys__).".into(),
        ));
    }
    let table = table_raw
        .trim_matches('"')
        .trim_matches('(')
        .trim_matches(')');
    if table.is_empty() {
        return Err(AppError::Backend("Missing table name after FROM".into()));
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
        let columns = if select_part["SELECT".len()..].trim() == "*" {
            match client.get_table_columns(table)? {
                Some(cols) if !cols.is_empty() => {
                    cols.into_iter().map(|x| x.name).collect::<Vec<_>>()
                }
                _ => client
                    .infer_columns_from_data(table)?
                    .map(|c| c.into_iter().map(|x| x.name).collect::<Vec<_>>())
                    .unwrap_or_default(),
            }
        } else {
            let col_list = select_part["SELECT".len()..].trim();
            col_list
                .split(',')
                .map(|s| s.trim().trim_matches('"').to_string())
                .collect::<Vec<_>>()
        };

        let pk_col = client.get_table_pk_or_inferred(table)?.ok_or_else(|| {
            AppError::Backend(format!(
                "Table '{table}' has no rows and no primary key metadata"
            ))
        })?;
        let mode = client.get_table_mode(table)?;
        let where_cond = parse_where(&upper, q);
        let order_by = parse_order_by(&upper, q);
        let need_full_scan = where_cond
            .as_ref()
            .is_some_and(|(col, _, _)| col != &pk_col)
            || order_by.is_some();

        let skip = usize::try_from((page.saturating_sub(1)) * page_size)
            .ok()
            .map_or(0, |n| n);
        let take_n = usize::try_from(page_size).ok().map_or(usize::MAX, |n| n);

        let (total_count, has_more_override, page_result) = if need_full_scan {
            let all_keys = client.list_row_keys(table)?;
            let mut rows_with_pk: Vec<(String, Vec<JsonValue>)> = Vec::new();
            for pk in &all_keys {
                let row_values: Vec<JsonValue> = match mode {
                    RowStorageMode::Json => {
                        let opt = client.get_row_json(table, pk)?;
                        let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                            continue;
                        };
                        row_values_for_columns(&columns, pk, None, Some(&obj))
                    }
                    RowStorageMode::Hash => {
                        let opt = client.get_row_hash(table, pk)?;
                        let Some(map) = opt.as_ref() else {
                            continue;
                        };
                        row_values_for_columns(&columns, pk, Some(map), None)
                    }
                };
                let col_index = where_cond
                    .as_ref()
                    .and_then(|(col, _, _)| columns.iter().position(|c| c == col));
                let matches = match (&where_cond, col_index) {
                    (Some((_col, negate, val)), Some(idx)) => {
                        let cell_str = match mode {
                            RowStorageMode::Json => json_value_to_cmp_str(&row_values[idx]),
                            RowStorageMode::Hash => {
                                row_values[idx].as_str().unwrap_or("").to_string()
                            }
                        };
                        let eq = cell_str == *val;
                        if *negate {
                            !eq
                        } else {
                            eq
                        }
                    }
                    (Some((col, negate, val)), None) if col == &pk_col => {
                        let eq = pk == val;
                        if *negate {
                            !eq
                        } else {
                            eq
                        }
                    }
                    (None, _) => true,
                    _ => false,
                };
                if matches {
                    rows_with_pk.push((pk.clone(), row_values));
                }
            }
            if let Some((col, asc)) = &order_by {
                if let Some(idx) = columns.iter().position(|c| c == col) {
                    rows_with_pk.sort_by(|a, b| {
                        let a_str = json_value_to_cmp_str(&a.1[idx]);
                        let b_str = json_value_to_cmp_str(&b.1[idx]);
                        let ord = a_str.cmp(&b_str);
                        if *asc {
                            ord
                        } else {
                            ord.reverse()
                        }
                    });
                }
            }
            let total = rows_with_pk.len();
            let offset = usize::try_from((page.saturating_sub(1)) * page_size)
                .ok()
                .map_or(0, |n| n);
            let take_n = usize::try_from(page_size).ok().map_or(usize::MAX, |n| n);
            let page_rows: Vec<Vec<JsonValue>> = rows_with_pk
                .into_iter()
                .skip(offset)
                .take(take_n)
                .map(|(_, row_values)| row_values)
                .collect();
            (total as u64, None, PageResult::Rows(page_rows))
        } else if where_cond.is_none() {
            let (page_keys, paged_has_more) = client.list_row_keys_paged(table, skip, take_n)?;
            let total = if paged_has_more {
                skip as u64 + (page_keys.len() as u64) + 1
            } else {
                skip as u64 + (page_keys.len() as u64)
            };
            (total, Some(paged_has_more), PageResult::Keys(page_keys))
        } else {
            let all_keys = client.list_row_keys(table)?;
            let mut filtered_keys: Vec<String> = all_keys;
            if let Some((col, negate, val)) = &where_cond {
                if col == &pk_col {
                    filtered_keys.retain(|k| {
                        let eq = k == val;
                        if *negate {
                            !eq
                        } else {
                            eq
                        }
                    });
                }
            }
            let total = filtered_keys.len();
            let page_keys: Vec<String> =
                filtered_keys.into_iter().skip(skip).take(take_n).collect();
            (total as u64, None, PageResult::Keys(page_keys))
        };

        let rows: Vec<JsonValue> = match page_result {
            PageResult::Rows(page_rows) => page_rows.into_iter().map(JsonValue::Array).collect(),
            PageResult::Keys(keys_page) => {
                let mut out = Vec::new();
                for pk in keys_page {
                    let row_values: Vec<JsonValue> = match mode {
                        RowStorageMode::Json => {
                            let opt = client.get_row_json(table, &pk)?;
                            let Some(obj) = opt.and_then(|v| v.as_object().cloned()) else {
                                continue;
                            };
                            row_values_for_columns(&columns, &pk, None, Some(&obj))
                        }
                        RowStorageMode::Hash => {
                            let opt = client.get_row_hash(table, &pk)?;
                            let Some(map) = opt.as_ref() else {
                                continue;
                            };
                            row_values_for_columns(&columns, &pk, Some(map), None)
                        }
                    };
                    out.push(JsonValue::Array(row_values));
                }
                out
            }
        };

        let start = (page.saturating_sub(1)) * page_size;
        let has_more =
            has_more_override.unwrap_or_else(|| start + (rows.len() as u64) < total_count);
        log::info!(
            "execute_query: table={table} total_count={total_count} rows_returned={}",
            rows.len()
        );
        serde_json::json!({
            "columns": columns,
            "rows": rows,
            "affected_rows": 0u64,
            "truncated": false,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_rows": total_count,
                "has_more": has_more
            }
        })
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
