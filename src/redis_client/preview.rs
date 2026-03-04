use redis::Connection;
use redis::Value as RedisValue;

use super::bytes_to_display;

pub fn json_path_first_field(path: Option<&str>) -> Option<String> {
    let path = path?.trim();
    let rest = path.strip_prefix("$.")?;
    let first = rest.split('.').next()?.trim();
    if first.is_empty() {
        return None;
    }
    Some(first.to_string())
}

fn value_to_bytes(v: &RedisValue) -> String {
    match v {
        RedisValue::BulkString(b) => bytes_to_display(b),
        RedisValue::SimpleString(s) => s.clone(),
        RedisValue::Int(n) => n.to_string(),
        _ => String::new(),
    }
}

/// Parse Redis value reply into a short preview string for __`redis_keys`__ display.
/// Handles HSCAN/SSCAN reply shape (cursor, array) by using the array part.
pub fn value_to_preview(type_str: &str, v: &RedisValue) -> String {
    const MAX_PREVIEW: usize = 200;
    let s = match (type_str, v) {
        ("string" | "ReJSON-RL" | "hash", RedisValue::BulkString(b)) => bytes_to_display(b),
        ("hash" | _, RedisValue::Nil) => String::new(),
        ("hash", RedisValue::Array(arr)) => {
            let pairs = if arr.len() == 2 {
                if let RedisValue::Array(inner) = &arr[1] {
                    inner.as_slice()
                } else {
                    arr.as_slice()
                }
            } else {
                arr.as_slice()
            };
            let len = pairs.len() / 2;
            if len == 0 {
                "[hash 0 fields]".to_string()
            } else if len <= 10 {
                let pair_strs: Vec<String> = pairs
                    .chunks(2)
                    .take(2)
                    .map(|c| {
                        format!(
                            "{}={}",
                            c.first().map(value_to_bytes).unwrap_or_default(),
                            c.get(1).map(value_to_bytes).unwrap_or_default()
                        )
                    })
                    .collect();
                let sample = pair_strs.join(", ");
                if len > 2 {
                    format!("{sample} (+ {} more)", len - 2)
                } else {
                    sample
                }
            } else {
                format!("[hash {len} fields]")
            }
        }
        ("list", RedisValue::Int(n)) => format!("[list {n} items]"),
        ("set", RedisValue::Array(arr)) => {
            let members = if arr.len() == 2 {
                if let RedisValue::Array(inner) = &arr[1] {
                    inner.as_slice()
                } else {
                    arr.as_slice()
                }
            } else {
                arr.as_slice()
            };
            let len = members.len();
            if len == 0 {
                "[set 0 members]".to_string()
            } else if len <= 5 {
                let sample = members
                    .iter()
                    .take(2)
                    .map(value_to_bytes)
                    .collect::<Vec<_>>()
                    .join(", ");
                if len > 2 {
                    format!("{sample} (+ {} more)", len - 2)
                } else {
                    sample
                }
            } else {
                format!("[set {len} members]")
            }
        }
        ("zset", RedisValue::Int(n)) => format!("[zset {n} members]"),
        _ => format!("{v:?}"),
    };
    if s.len() > MAX_PREVIEW {
        format!("{}...", &s[..MAX_PREVIEW])
    } else {
        s
    }
}

pub fn key_value_preview_batch(
    conn: &mut Connection,
    has_redis_json: bool,
    keys: &[&Vec<u8>],
    types: &[String],
    json_path: Option<&str>,
) -> Result<Vec<String>, String> {
    if keys.is_empty() {
        return Ok(vec![]);
    }
    let hash_field = json_path_first_field(json_path);
    let mut value_pipe = redis::pipe();
    for (key, t) in keys.iter().zip(types.iter()) {
        match t.as_str() {
            "string" => value_pipe.cmd("GET").arg(key.as_slice()),
            "ReJSON-RL" if has_redis_json => value_pipe
                .cmd("JSON.GET")
                .arg(key.as_slice())
                .arg(json_path.unwrap_or("$")),
            "hash" if hash_field.is_some() => value_pipe
                .cmd("HGET")
                .arg(key.as_slice())
                .arg(hash_field.as_ref().unwrap().as_str()),
            "hash" => value_pipe
                .cmd("HSCAN")
                .arg(key.as_slice())
                .arg(0u8)
                .arg("COUNT")
                .arg(5),
            "list" => value_pipe.cmd("LLEN").arg(key.as_slice()),
            "set" => value_pipe
                .cmd("SSCAN")
                .arg(key.as_slice())
                .arg(0u8)
                .arg("COUNT")
                .arg(5),
            "zset" => value_pipe.cmd("ZCARD").arg(key.as_slice()),
            _ => value_pipe.cmd("TYPE").arg(key.as_slice()),
        };
    }
    let values: Vec<RedisValue> = value_pipe.query(conn).map_err(|e| e.to_string())?;
    let out: Vec<String> = types
        .iter()
        .zip(values.iter())
        .map(|(t, v)| {
            let s = value_to_preview(t, v);
            if s.len() > 200 {
                format!("{}...", &s[..200])
            } else {
                s
            }
        })
        .collect();
    Ok(out)
}

pub fn key_type_and_preview_batch(
    conn: &mut Connection,
    has_redis_json: bool,
    keys: &[&Vec<u8>],
    json_path: Option<&str>,
) -> Result<Vec<(String, String)>, String> {
    if keys.is_empty() {
        return Ok(vec![]);
    }
    let mut type_pipe = redis::pipe();
    for k in keys {
        type_pipe.cmd("TYPE").arg(k.as_slice());
    }
    let types: Vec<String> = type_pipe.query(conn).map_err(|e| e.to_string())?;
    let hash_field = json_path_first_field(json_path);
    let mut value_pipe = redis::pipe();
    for (key, t) in keys.iter().zip(types.iter()) {
        match t.as_str() {
            "string" => value_pipe.cmd("GET").arg(key.as_slice()),
            "ReJSON-RL" if has_redis_json => value_pipe
                .cmd("JSON.GET")
                .arg(key.as_slice())
                .arg(json_path.unwrap_or("$")),
            "hash" if hash_field.is_some() => value_pipe
                .cmd("HGET")
                .arg(key.as_slice())
                .arg(hash_field.as_ref().unwrap().as_str()),
            "hash" => value_pipe
                .cmd("HSCAN")
                .arg(key.as_slice())
                .arg(0u8)
                .arg("COUNT")
                .arg(5),
            "list" => value_pipe.cmd("LLEN").arg(key.as_slice()),
            "set" => value_pipe
                .cmd("SSCAN")
                .arg(key.as_slice())
                .arg(0u8)
                .arg("COUNT")
                .arg(5),
            "zset" => value_pipe.cmd("ZCARD").arg(key.as_slice()),
            _ => value_pipe.cmd("TYPE").arg(key.as_slice()),
        };
    }
    let values: Vec<RedisValue> = value_pipe.query(conn).map_err(|e| e.to_string())?;
    let out: Vec<(String, String)> = types
        .into_iter()
        .zip(values.iter())
        .map(|(t, v)| {
            let preview = value_to_preview(&t, v);
            (t, preview)
        })
        .collect();
    Ok(out)
}

pub fn key_type_and_preview(conn: &mut Connection, key: &[u8]) -> Result<(String, String), String> {
    const MAX_PREVIEW: usize = 200;
    let type_str: String = redis::cmd("TYPE")
        .arg(key)
        .query(conn)
        .map_err(|e| e.to_string())?;
    let preview = match type_str.as_str() {
        "string" => {
            let v: Option<Vec<u8>> = redis::cmd("GET")
                .arg(key)
                .query(conn)
                .map_err(|e| e.to_string())?;
            v.map(|b| bytes_to_display(&b)).unwrap_or_default()
        }
        "hash" => {
            let len: usize = redis::cmd("HLEN")
                .arg(key)
                .query(conn)
                .map_err(|e| e.to_string())?;
            if len == 0 {
                "[hash 0 fields]".to_string()
            } else if len <= 10 {
                let pairs: Vec<Vec<u8>> = redis::cmd("HGETALL")
                    .arg(key)
                    .query(conn)
                    .map_err(|e| e.to_string())?;
                let preview_pairs: Vec<String> = pairs
                    .chunks(2)
                    .take(2)
                    .map(|c| {
                        format!(
                            "{}={}",
                            c.first().map(|b| bytes_to_display(b)).unwrap_or_default(),
                            c.get(1).map(|b| bytes_to_display(b)).unwrap_or_default()
                        )
                    })
                    .collect();
                let sample = preview_pairs.join(", ");
                if len > 2 {
                    format!("{sample} (+ {} more)", len - 2)
                } else {
                    sample
                }
            } else {
                format!("[hash {len} fields]")
            }
        }
        "list" => {
            let len: usize = redis::cmd("LLEN")
                .arg(key)
                .query(conn)
                .map_err(|e| e.to_string())?;
            format!("[list {len} items]")
        }
        "set" => {
            let len: usize = redis::cmd("SCARD")
                .arg(key)
                .query(conn)
                .map_err(|e| e.to_string())?;
            if len == 0 {
                "[set 0 members]".to_string()
            } else if len <= 5 {
                let members: Vec<Vec<u8>> = redis::cmd("SMEMBERS")
                    .arg(key)
                    .query(conn)
                    .map_err(|e| e.to_string())?;
                let sample = members
                    .iter()
                    .take(2)
                    .map(|b| bytes_to_display(b))
                    .collect::<Vec<_>>()
                    .join(", ");
                if len > 2 {
                    format!("{sample} (+ {} more)", len - 2)
                } else {
                    sample
                }
            } else {
                format!("[set {len} members]")
            }
        }
        "zset" => {
            let len: usize = redis::cmd("ZCARD")
                .arg(key)
                .query(conn)
                .map_err(|e| e.to_string())?;
            format!("[zset {len} members]")
        }
        _ => type_str.clone(),
    };
    let preview = if preview.len() > MAX_PREVIEW {
        format!("{}...", &preview[..MAX_PREVIEW])
    } else {
        preview
    };
    Ok((type_str, preview))
}
