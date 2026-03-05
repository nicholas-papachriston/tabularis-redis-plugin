//! Type-specific virtual tables: hashes, lists, sets, zsets, streams.
//! Supports SELECT * FROM hashes [WHERE key = 'x'] with single-key or full-scan paths.

use crate::methods::common::AppError;
use crate::methods::discovery;
use crate::redis_client::{bytes_to_display, RedisClient};
use serde_json::Value as JsonValue;

const FULL_SCAN_SAFETY_CAP: u64 = 10_000_000;
const TYPE_BATCH_CHUNK: usize = 200;

/// Extract WHERE key = 'value' to get the exact key. Returns None if no such clause or not exact eq.
fn extract_where_key_eq(query: &str) -> Option<String> {
    let upper = query.to_uppercase();
    let after_where = upper.find(" WHERE ")?;
    let clause = query[after_where + 7..].trim();
    let end = clause
        .to_uppercase()
        .find(" ORDER BY ")
        .unwrap_or(clause.len());
    let clause = clause[..end.min(clause.len())].trim();
    for segment in clause.split(" AND ").map(str::trim) {
        let seg_upper = segment.to_uppercase();
        if seg_upper.starts_with("KEY = ") {
            let rest = segment[6..].trim();
            let value = rest.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn key_display(key: &[u8]) -> String {
    bytes_to_display(key)
}

fn paginate(
    rows: &[Vec<JsonValue>],
    page: u64,
    page_size: u64,
) -> (Vec<Vec<JsonValue>>, u64, bool) {
    let total = rows.len() as u64;
    let page_size = page_size.max(1);
    let skip = (page.saturating_sub(1)) * page_size;
    let skip_usize = usize::try_from(skip).unwrap_or(usize::MAX).min(rows.len());
    let take_usize = usize::try_from(page_size)
        .unwrap_or(100)
        .min(rows.len().saturating_sub(skip_usize));
    let page_rows = rows[skip_usize..skip_usize + take_usize].to_vec();
    let has_more = skip + (page_rows.len() as u64) < total;
    (page_rows, total, has_more)
}

fn build_result(
    columns: &[&str],
    rows: &[Vec<JsonValue>],
    page: u64,
    page_size: u64,
    total: u64,
    has_more: bool,
) -> JsonValue {
    serde_json::json!({
        "columns": columns,
        "rows": rows,
        "affected_rows": 0u64,
        "truncated": has_more,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_rows": total,
            "has_more": has_more
        }
    })
}

/// Execute SELECT on a type-specific virtual table (hashes, lists, sets, zsets, streams).
pub fn execute_type_table_scan(
    client: &mut RedisClient,
    table: &str,
    query: &str,
    page: u64,
    page_size: u64,
) -> Result<JsonValue, AppError> {
    let table = table.trim().trim_matches('"');
    let redis_type = discovery::type_virtual_table_redis_type(table)
        .ok_or_else(|| AppError::Backend(format!("Unknown type table: {table}")))?;

    let opt_key = extract_where_key_eq(query);
    let page_size = page_size.max(1);

    if let Some(key_str) = opt_key {
        return execute_single_key(client, table, redis_type, &key_str, page, page_size);
    }

    execute_full_scan(client, table, redis_type, page, page_size)
}

fn execute_single_key(
    client: &mut RedisClient,
    table: &str,
    redis_type: &str,
    key_str: &str,
    page: u64,
    page_size: u64,
) -> Result<JsonValue, AppError> {
    let key_bytes = key_str.as_bytes();
    let key_display = key_display(key_bytes);

    let rows: Vec<Vec<JsonValue>> = match (table, redis_type) {
        ("hashes", "hash") => {
            let pairs = client.hgetall(key_bytes).map_err(AppError::Backend)?;
            pairs
                .into_iter()
                .map(|(f, v)| {
                    vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::String(f),
                        JsonValue::String(v),
                    ]
                })
                .collect()
        }
        ("lists", "list") => {
            let values = client.lrange_all(key_bytes).map_err(AppError::Backend)?;
            values
                .into_iter()
                .enumerate()
                .map(|(i, v)| {
                    vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::Number(serde_json::Number::from(
                            i64::try_from(i).unwrap_or(i64::MAX),
                        )),
                        JsonValue::String(v),
                    ]
                })
                .collect()
        }
        ("sets", "set") => {
            let members = client.smembers(key_bytes).map_err(AppError::Backend)?;
            members
                .into_iter()
                .map(|v| vec![JsonValue::String(key_display.clone()), JsonValue::String(v)])
                .collect()
        }
        ("zsets", "zset") => {
            let pairs = client
                .zrange_with_scores(key_bytes)
                .map_err(AppError::Backend)?;
            pairs
                .into_iter()
                .map(|(v, s)| {
                    vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::String(v),
                        JsonValue::Number(
                            serde_json::Number::from_f64(s)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
                    ]
                })
                .collect()
        }
        ("streams", "stream") => {
            let entries = client.xrange_all(key_bytes).map_err(AppError::Backend)?;
            entries
                .into_iter()
                .map(|(id, fields)| {
                    let fields_json =
                        serde_json::to_string(&fields).unwrap_or_else(|_| "[]".to_string());
                    vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::String(id),
                        JsonValue::String(fields_json),
                    ]
                })
                .collect()
        }
        _ => {
            return Err(AppError::Backend(format!(
                "Table {table} does not match type {redis_type}"
            )))
        }
    };

    let (page_rows, total, has_more) = paginate(&rows, page, page_size);
    let columns: &[&str] = match table {
        "hashes" => &["key", "field", "value"],
        "lists" => &["key", "index", "value"],
        "zsets" => &["key", "value", "score"],
        "streams" => &["key", "id", "fields"],
        _ => &["key", "value"],
    };
    Ok(build_result(
        columns, &page_rows, page, page_size, total, has_more,
    ))
}

#[allow(clippy::too_many_lines)]
fn execute_full_scan(
    client: &mut RedisClient,
    table: &str,
    redis_type: &str,
    page: u64,
    page_size: u64,
) -> Result<JsonValue, AppError> {
    let mut all_keys: Vec<Vec<u8>> = Vec::new();
    let mut cursor = 0u64;
    loop {
        let (next, keys) = client
            .scan_keys(cursor, TYPE_BATCH_CHUNK, None)
            .map_err(AppError::Backend)?;
        for k in keys {
            if (all_keys.len() as u64) >= FULL_SCAN_SAFETY_CAP {
                break;
            }
            all_keys.push(k);
        }
        if next == 0 || (all_keys.len() as u64) >= FULL_SCAN_SAFETY_CAP {
            break;
        }
        cursor = next;
    }

    let key_refs: Vec<&[u8]> = all_keys.iter().map(Vec::as_slice).collect();
    let types = client
        .key_types_batch(&key_refs)
        .map_err(AppError::Backend)?;
    let matching: Vec<Vec<u8>> = all_keys
        .into_iter()
        .zip(types)
        .filter(|(_, t)| t.as_str() == redis_type)
        .map(|(k, _)| k)
        .collect();

    if matching.is_empty() {
        let columns: &[&str] = match table {
            "hashes" => &["key", "field", "value"],
            "lists" => &["key", "index", "value"],
            "zsets" => &["key", "value", "score"],
            "streams" => &["key", "id", "fields"],
            _ => &["key", "value"],
        };
        return Ok(build_result(
            columns,
            &[],
            page,
            page_size,
            0,
            false,
        ));
    }

    let lengths = client
        .key_cardinality_batch(
            &matching.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            redis_type,
        )
        .map_err(AppError::Backend)?;
    let mut cumulative = Vec::with_capacity(matching.len() + 1);
    cumulative.push(0u64);
    for len in &lengths {
        cumulative.push(cumulative.last().copied().unwrap_or(0) + *len);
    }
    let total = cumulative.last().copied().unwrap_or(0);
    let page_size = page_size.max(1);
    let skip = (page.saturating_sub(1)) * page_size;
    let take = page_size;

    let (start_key, offset_in_first) = if skip >= total {
        (matching.len(), 0u64)
    } else {
        let idx = cumulative
            .iter()
            .position(|&c| c > skip)
            .map_or(0, |i| i.saturating_sub(1));
        let offset = skip - cumulative.get(idx).copied().unwrap_or(0);
        (idx, offset)
    };
    let end_key = if skip + take == 0 {
        start_key
    } else {
        let end_row = skip + take;
        cumulative
            .iter()
            .position(|&c| c >= end_row)
            .map_or_else(|| matching.len().saturating_sub(1), |i| i.saturating_sub(1))
    };

    let keys_to_fetch: Vec<&[u8]> = if start_key <= end_key && end_key < matching.len() {
        matching[start_key..=end_key].iter().map(Vec::as_slice).collect()
    } else {
        Vec::new()
    };

    let rows = if keys_to_fetch.is_empty() {
        Vec::new()
    } else {
        let full = fetch_all_rows_for_type(client, table, redis_type, &keys_to_fetch)?;
        let offset_usize = usize::try_from(offset_in_first).unwrap_or(usize::MAX).min(full.len());
        let take_usize = usize::try_from(take).unwrap_or(usize::MAX).min(full.len().saturating_sub(offset_usize));
        full.into_iter()
            .skip(offset_usize)
            .take(take_usize)
            .collect::<Vec<_>>()
    };

    let has_more = skip + (rows.len() as u64) < total;
    let columns: &[&str] = match table {
        "hashes" => &["key", "field", "value"],
        "lists" => &["key", "index", "value"],
        "zsets" => &["key", "value", "score"],
        "streams" => &["key", "id", "fields"],
        _ => &["key", "value"],
    };
    Ok(build_result(
        columns,
        &rows,
        page,
        page_size,
        total,
        has_more,
    ))
}

fn fetch_all_rows_for_type(
    client: &mut RedisClient,
    table: &str,
    _redis_type: &str,
    keys: &[&[u8]],
) -> Result<Vec<Vec<JsonValue>>, AppError> {
    let mut out = Vec::new();
    match table {
        "hashes" => {
            let batch = client.hgetall_batch(keys).map_err(AppError::Backend)?;
            for (key_bytes, pairs) in keys.iter().zip(batch) {
                let key_display = key_display(key_bytes);
                for (f, v) in pairs {
                    out.push(vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::String(f),
                        JsonValue::String(v),
                    ]);
                }
            }
        }
        "lists" => {
            let batch = client.lrange_all_batch(keys).map_err(AppError::Backend)?;
            for (key_bytes, values) in keys.iter().zip(batch) {
                let key_display = key_display(key_bytes);
                for (i, v) in values.into_iter().enumerate() {
                    out.push(vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::Number(serde_json::Number::from(
                            i64::try_from(i).unwrap_or(i64::MAX),
                        )),
                        JsonValue::String(v),
                    ]);
                }
            }
        }
        "sets" => {
            let batch = client.smembers_batch(keys).map_err(AppError::Backend)?;
            for (key_bytes, members) in keys.iter().zip(batch) {
                let key_display = key_display(key_bytes);
                for v in members {
                    out.push(vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::String(v),
                    ]);
                }
            }
        }
        "zsets" => {
            let batch = client
                .zrange_with_scores_batch(keys)
                .map_err(AppError::Backend)?;
            for (key_bytes, pairs) in keys.iter().zip(batch) {
                let key_display = key_display(key_bytes);
                for (v, s) in pairs {
                    out.push(vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::String(v),
                        JsonValue::Number(
                            serde_json::Number::from_f64(s)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
                    ]);
                }
            }
        }
        "streams" => {
            let batch = client.xrange_all_batch(keys).map_err(AppError::Backend)?;
            for (key_bytes, entries) in keys.iter().zip(batch) {
                let key_display = key_display(key_bytes);
                for (id, fields) in entries {
                    let fields_json =
                        serde_json::to_string(&fields).unwrap_or_else(|_| "[]".to_string());
                    out.push(vec![
                        JsonValue::String(key_display.clone()),
                        JsonValue::String(id),
                        JsonValue::String(fields_json),
                    ]);
                }
            }
        }
        _ => return Err(AppError::Backend(format!("Unknown type table: {table}"))),
    }
    Ok(out)
}
