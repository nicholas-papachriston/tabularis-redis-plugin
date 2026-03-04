mod parser;
mod redis_keys;

use crate::metadata::RowStorageMode;
use crate::methods::common::AppError;
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
    if !upper.starts_with("SELECT") {
        return Err(AppError::Backend("Only SELECT is supported".into()));
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

    let pattern_override = discovery::key_pattern_from_table(table).map(|p| format!("{p}:*"));
    let pattern_override = pattern_override.or_else(|| {
        if table.starts_with("__keys") && table != discovery::REDIS_KEYS_TABLE {
            let rest = table
                .strip_prefix("__keys")
                .and_then(|s| s.strip_prefix(':'))
                .unwrap_or_else(|| table.strip_prefix("__keys").unwrap_or(""));
            let rest = rest.strip_suffix("__").unwrap_or(rest);
            if rest.is_empty() {
                None
            } else {
                Some(format!("{rest}:*"))
            }
        } else {
            None
        }
    });
    let mut result = if table == discovery::REDIS_KEYS_TABLE || pattern_override.is_some() {
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
