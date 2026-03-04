use std::sync::Arc;

use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

use super::parser::{
    parse_limit, parse_redis_keys_order_by, parse_redis_keys_type_filter,
    parse_redis_keys_value_filter, parse_redis_keys_where, RedisValueFilterKind,
};

/// Max keys to scan when we need an accurate total (filters/order/pattern). Avoids OOM on huge DBs.
const FULL_SCAN_SAFETY_CAP: u64 = 10_000_000;

#[allow(clippy::too_many_lines)]
pub fn execute_redis_keys_scan(
    client: &mut RedisClient,
    query: &str,
    page: u64,
    page_size: u64,
    _app_limit: Option<u64>,
    pattern_override: Option<&str>,
    json_path: Option<&str>,
) -> Result<JsonValue, String> {
    let upper = query.to_uppercase();
    let pattern = pattern_override
        .map(String::from)
        .or_else(|| parse_redis_keys_where(&upper, query));
    let query_limit = parse_limit(&upper, query);
    let type_filter = parse_redis_keys_type_filter(&upper, query);
    let value_filter = parse_redis_keys_value_filter(&upper, query);
    let order_by = parse_redis_keys_order_by(&upper, query);
    let order_by_type = order_by.is_some_and(|(col, _)| col == "type");
    let order_by_value = order_by.is_some_and(|(col, _)| col == "value");
    let order_desc = order_by.is_some_and(|(_, d)| d);

    log::info!(
        "execute_redis_keys_scan: pattern_override={:?} parsed_pattern={:?} type_filter={:?} value_filter={:?} order_by={:?} page={page} page_size={page_size}",
        pattern_override,
        pattern.as_deref(),
        type_filter,
        value_filter.as_ref().map(|vf| match vf {
            RedisValueFilterKind::Exact(s) => format!("Exact({s})"),
            RedisValueFilterKind::StartsWith(s) => format!("StartsWith({s})"),
            RedisValueFilterKind::Contains(s) => format!("Contains({s})"),
        }),
        order_by
    );
    log::debug!("execute_redis_keys_scan: query={query}");

    let dbsize = client.dbsize()?;
    let skip = usize::try_from((page.saturating_sub(1)) * page_size).unwrap_or(0);
    let take = usize::try_from(page_size).unwrap_or(100);
    let need_full_scan =
        pattern.is_some() || type_filter.is_some() || order_by.is_some() || value_filter.is_some();
    let max_to_scan: u64 = if need_full_scan {
        FULL_SCAN_SAFETY_CAP
    } else {
        let keys_needed = (skip + take) as u64;
        query_limit.map_or(keys_needed, |l| l.max(keys_needed))
    };
    let mut all_keys: Vec<Arc<Vec<u8>>> = Vec::new();
    let mut cursor = 0u64;
    loop {
        let (next, keys) = client.scan_keys(cursor, 200, pattern.as_deref())?;
        for k in keys {
            if (all_keys.len() as u64) >= max_to_scan {
                break;
            }
            all_keys.push(Arc::new(k));
        }
        if next == 0 {
            break;
        }
        cursor = next;
        if (all_keys.len() as u64) >= max_to_scan {
            break;
        }
        if !need_full_scan
            && type_filter.is_none()
            && order_by.is_none()
            && value_filter.is_none()
            && pattern.is_none()
            && all_keys.len() >= skip + take
        {
            break;
        }
    }

    let (page_keys_owned, types_for_page, total_count) =
        if type_filter.is_some() || order_by_type || order_by_value || value_filter.is_some() {
            let key_refs: Vec<&[u8]> = all_keys
                .iter()
                .map(Arc::as_ref)
                .map(Vec::as_slice)
                .collect();
            let types = client.key_types_batch(&key_refs)?;
            let keys_with_types: Vec<(Arc<Vec<u8>>, String)> = all_keys
                .into_iter()
                .zip(types)
                .filter(|(_, t)| {
                    type_filter.as_ref().is_none_or(|(want, negate)| {
                        let match_type = t.to_lowercase() == *want;
                        if *negate {
                            !match_type
                        } else {
                            match_type
                        }
                    })
                })
                .collect();
            let (mut keys_with_types, mut value_previews) = if let Some(ref vf) = value_filter {
                let key_refs2: Vec<&Vec<u8>> =
                    keys_with_types.iter().map(|(k, _)| k.as_ref()).collect();
                let types2: Vec<String> = keys_with_types.iter().map(|(_, t)| t.clone()).collect();
                let previews = client.key_value_preview_batch(&key_refs2, &types2, json_path)?;
                let mut filtered: Vec<(Arc<Vec<u8>>, String)> =
                    Vec::with_capacity(keys_with_types.len());
                let mut filtered_previews: Vec<String> = Vec::with_capacity(keys_with_types.len());
                for ((k, t), preview) in keys_with_types.into_iter().zip(previews) {
                    let matches = match vf {
                        RedisValueFilterKind::Exact(s) => preview.eq_ignore_ascii_case(s),
                        RedisValueFilterKind::StartsWith(s) => {
                            preview.len() >= s.len() && preview[..s.len()].eq_ignore_ascii_case(s)
                        }
                        RedisValueFilterKind::Contains(s) => {
                            preview.to_lowercase().contains(&s.to_lowercase())
                        }
                    };
                    if matches {
                        filtered.push((k, t));
                        filtered_previews.push(preview);
                    }
                }
                (filtered, Some(filtered_previews))
            } else {
                (keys_with_types, None)
            };
            if order_by_value && value_previews.is_none() {
                let key_refs2: Vec<&Vec<u8>> =
                    keys_with_types.iter().map(|(k, _)| k.as_ref()).collect();
                let types2: Vec<String> = keys_with_types.iter().map(|(_, t)| t.clone()).collect();
                value_previews =
                    Some(client.key_value_preview_batch(&key_refs2, &types2, json_path)?);
            }
            let total_filtered = keys_with_types.len() as u64;
            let total_count = total_filtered;
            if let Some(previews) = value_previews {
                let mut with_previews: Vec<(Arc<Vec<u8>>, String, String)> = keys_with_types
                    .into_iter()
                    .zip(previews)
                    .map(|((k, t), p)| (k, t, p))
                    .collect();
                with_previews.sort_by(|a, b| {
                    let ord = if order_by_type {
                        a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0))
                    } else if order_by_value {
                        a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0))
                    } else {
                        a.0.cmp(&b.0)
                    };
                    if order_desc {
                        ord.reverse()
                    } else {
                        ord
                    }
                });
                keys_with_types = with_previews.into_iter().map(|(k, t, _)| (k, t)).collect();
            } else {
                keys_with_types.sort_by(|a, b| {
                    let ord = if order_by_type {
                        a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0))
                    } else {
                        a.0.cmp(&b.0)
                    };
                    if order_desc {
                        ord.reverse()
                    } else {
                        ord
                    }
                });
            }
            let page_tuples: Vec<_> = keys_with_types.into_iter().skip(skip).take(take).collect();
            let page_keys_owned: Vec<Arc<Vec<u8>>> =
                page_tuples.iter().map(|(k, _)| Arc::clone(k)).collect();
            let types_for_page: Vec<String> = page_tuples.into_iter().map(|(_, t)| t).collect();
            (page_keys_owned, types_for_page, total_count)
        } else {
            let total_count = match (query_limit, pattern.is_some()) {
                (Some(_), _) | (None, true) => all_keys.len() as u64,
                (None, false) => dbsize,
            };
            all_keys.sort_by(|a, b| {
                let ord = a.cmp(b);
                if order_desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
            let page_keys_owned: Vec<Arc<Vec<u8>>> =
                all_keys.iter().skip(skip).take(take).cloned().collect();
            (page_keys_owned, vec![], total_count)
        };

    let page_keys: Vec<&Vec<u8>> = page_keys_owned.iter().map(Arc::as_ref).collect();
    let (type_previews, ttls) = if types_for_page.is_empty() {
        let tp = client.key_type_and_preview_batch(&page_keys, json_path)?;
        let ttl_keys: Vec<&[u8]> = page_keys.iter().map(|k| k.as_slice()).collect();
        let ttls = client.key_ttl_batch(&ttl_keys)?;
        (tp, ttls)
    } else {
        let previews = client.key_value_preview_batch(&page_keys, &types_for_page, json_path)?;
        let ttl_keys: Vec<&[u8]> = page_keys.iter().map(|k| k.as_slice()).collect();
        let ttls = client.key_ttl_batch(&ttl_keys)?;
        let tp: Vec<(String, String)> = types_for_page.into_iter().zip(previews).collect();
        (tp, ttls)
    };

    let mut rows: Vec<JsonValue> = Vec::with_capacity(page_keys.len());
    for (i, key_bytes) in page_keys.iter().enumerate() {
        let key_display = crate::redis_client::bytes_to_display(key_bytes);
        let key_raw = crate::redis_client::bytes_to_key_id(key_bytes);
        let (type_str, preview) = type_previews
            .get(i)
            .map_or(("", ""), |t| (t.0.as_str(), t.1.as_str()));
        let ttl_str = ttls
            .get(i)
            .and_then(|o| o.as_ref())
            .map_or_else(String::new, std::string::ToString::to_string);
        rows.push(serde_json::json!([
            key_display,
            type_str,
            preview,
            ttl_str,
            key_raw
        ]));
    }
    let start = (page.saturating_sub(1)) * page_size;
    let has_more = start + (rows.len() as u64) < total_count;
    log::info!(
        "execute_redis_keys_scan: pattern={:?} type_filter={:?} total_count={total_count} rows_returned={}",
        pattern,
        type_filter,
        rows.len()
    );
    Ok(serde_json::json!({
        "columns": ["key", "type", "value", "ttl_seconds", "key_raw"],
        "rows": rows,
        "affected_rows": 0u64,
        "truncated": false,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_rows": total_count,
            "has_more": has_more
        }
    }))
}
