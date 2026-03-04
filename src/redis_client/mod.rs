mod preview;

use crate::metadata::{MetadataStore, RowStorageMode};
use crate::models::{ColumnDef, ConnectionParams, IndexDef};
use redis::Connection;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Convert bytes to a String, replacing invalid UTF-8 so JSON and the UI never see invalid sequences.
pub fn bytes_to_display(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn connection_string(params: &ConnectionParams) -> String {
    let host = params
        .host
        .as_deref()
        .filter(|h| !h.trim().is_empty())
        .unwrap_or("127.0.0.1");
    let port = params.port.unwrap_or(6379);
    let db = params
        .database
        .as_ref()
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(0);
    let auth = match (
        params.username.as_deref().filter(|u| !u.trim().is_empty()),
        params.password.as_deref().filter(|p| !p.trim().is_empty()),
    ) {
        (Some(user), Some(pass)) => format!("{user}:{pass}@"),
        (None, Some(pass)) => format!(":{pass}@"),
        (Some(user), None) => format!("{user}:@"),
        (None, None) => String::new(),
    };
    format!("redis://{auth}{host}:{port}/{db}")
}

pub struct RedisClient {
    conn: Connection,
    has_redis_json: bool,
}

impl RedisClient {
    pub fn connect(params: &ConnectionParams) -> Result<Self, String> {
        let url = connection_string(params);
        log::debug!("Connecting to Redis at {url}");
        let client = redis::Client::open(url.as_str()).map_err(|e| e.to_string())?;
        let mut conn = client.get_connection().map_err(|e| e.to_string())?;
        let has_redis_json = Self::detect_redis_json(&mut conn);
        log::info!("Redis connection established (redis_json_available={has_redis_json})");
        Ok(Self {
            conn,
            has_redis_json,
        })
    }

    fn detect_redis_json(conn: &mut Connection) -> bool {
        let _: Result<(), _> = redis::cmd("MODULE").arg("LIST").query(conn);
        let r: Result<redis::Value, _> = redis::cmd("JSON.GET")
            .arg("__tabularis_no_key__")
            .query(conn);
        r.is_ok()
    }

    pub fn ping(&mut self) -> Result<(), String> {
        log::debug!("Sending Redis PING");
        redis::cmd("PING")
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    pub fn list_databases(&mut self) -> Result<Vec<String>, String> {
        let raw: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("databases")
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        let db_count: u8 = raw.get(1).and_then(|s| s.parse().ok()).unwrap_or(16);
        let dbs: Vec<String> = (0..db_count).map(|i| i.to_string()).collect();
        Ok(dbs)
    }

    #[allow(dead_code)]
    pub const fn has_redis_json(&self) -> bool {
        self.has_redis_json
    }

    pub fn list_tables(&mut self) -> Result<Vec<String>, String> {
        MetadataStore::new(&mut self.conn).list_tables()
    }

    pub fn get_table_columns(&mut self, table: &str) -> Result<Option<Vec<ColumnDef>>, String> {
        MetadataStore::new(&mut self.conn).get_table_columns(table)
    }

    pub fn get_table_pk(&mut self, table: &str) -> Result<Option<String>, String> {
        MetadataStore::new(&mut self.conn).get_table_pk(table)
    }

    /// Primary key for a table: from metadata, or "key" for virtual keys tables, or "_key" when table has row data but no metadata.
    pub fn get_table_pk_or_inferred(&mut self, table: &str) -> Result<Option<String>, String> {
        if table == "__redis_keys__" {
            return Ok(Some("key".to_string()));
        }
        if table.starts_with("__keys:") && table.ends_with("__") && table.len() > 8 {
            return Ok(Some("key".to_string()));
        }
        if table.starts_with("__keys") && table != "__redis_keys__" {
            return Ok(Some("key".to_string()));
        }
        if let Some(pk) = self.get_table_pk(table)? {
            return Ok(Some(pk));
        }
        let keys = self.list_row_keys(table)?;
        if keys.is_empty() {
            return Ok(None);
        }
        Ok(Some("_key".to_string()))
    }

    /// Infer column definitions from the first row when metadata has no columns. Returns a leading "_key" (PK) plus field names from data.
    pub fn infer_columns_from_data(
        &mut self,
        table: &str,
    ) -> Result<Option<Vec<ColumnDef>>, String> {
        let keys = self.list_row_keys(table)?;
        let first_pk = match keys.first() {
            Some(pk) => pk.as_str(),
            None => return Ok(None),
        };
        let key_col = ColumnDef {
            name: "_key".to_string(),
            data_type: "TEXT".to_string(),
            is_nullable: false,
            column_default: None,
            is_primary_key: true,
            is_auto_increment: false,
            comment: None,
        };
        if let Ok(Some(map)) = self.get_row_hash(table, first_pk) {
            let mut names: Vec<String> = map.keys().cloned().collect();
            names.sort();
            let mut cols = vec![key_col];
            for name in names {
                cols.push(ColumnDef {
                    name,
                    data_type: "TEXT".to_string(),
                    is_nullable: true,
                    column_default: None,
                    is_primary_key: false,
                    is_auto_increment: false,
                    comment: None,
                });
            }
            log::debug!(
                "infer_columns_from_data: table={table} inferred {} columns from hash",
                cols.len()
            );
            return Ok(Some(cols));
        }
        if let Ok(Some(JsonValue::Object(obj))) = self.get_row_json(table, first_pk) {
            let mut names: Vec<String> = obj.keys().cloned().collect();
            names.sort();
            let mut cols = vec![key_col];
            for name in names {
                cols.push(ColumnDef {
                    name,
                    data_type: "TEXT".to_string(),
                    is_nullable: true,
                    column_default: None,
                    is_primary_key: false,
                    is_auto_increment: false,
                    comment: None,
                });
            }
            log::debug!(
                "infer_columns_from_data: table={table} inferred {} columns from json",
                cols.len()
            );
            return Ok(Some(cols));
        }
        Ok(Some(vec![key_col]))
    }

    pub fn get_table_mode(&mut self, table: &str) -> Result<RowStorageMode, String> {
        MetadataStore::new(&mut self.conn).get_table_mode(table)
    }

    pub fn register_table(
        &mut self,
        table: &str,
        pk_column: &str,
        columns: &[ColumnDef],
        mode: &str,
    ) -> Result<(), String> {
        let normalized_mode = RowStorageMode::from_str(mode);
        MetadataStore::new(&mut self.conn).register_table(
            table,
            pk_column,
            columns,
            normalized_mode,
        )
    }

    pub fn list_row_keys(&mut self, table: &str) -> Result<Vec<String>, String> {
        MetadataStore::new(&mut self.conn).list_row_keys(table)
    }

    /// Page of row keys without loading the full set. Use when only a slice is needed (e.g. simple pagination).
    pub fn list_row_keys_paged(
        &mut self,
        table: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<String>, bool), String> {
        MetadataStore::new(&mut self.conn).list_row_keys_paged(table, offset, limit)
    }

    pub fn get_row_hash(
        &mut self,
        table: &str,
        pk: &str,
    ) -> Result<Option<HashMap<String, String>>, String> {
        MetadataStore::new(&mut self.conn).get_row_hash(table, pk)
    }

    pub fn set_row_hash(
        &mut self,
        table: &str,
        pk: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), String> {
        MetadataStore::new(&mut self.conn).set_row_hash(table, pk, fields)
    }

    pub fn get_row_json(&mut self, table: &str, pk: &str) -> Result<Option<JsonValue>, String> {
        MetadataStore::new(&mut self.conn).get_row_json(table, pk)
    }

    pub fn set_row_json(&mut self, table: &str, pk: &str, value: &JsonValue) -> Result<(), String> {
        MetadataStore::new(&mut self.conn).set_row_json(table, pk, value)
    }

    pub fn delete_row(&mut self, table: &str, pk: &str) -> Result<(), String> {
        MetadataStore::new(&mut self.conn).delete_row(table, pk)
    }

    /// Remove a registered table and all its data and metadata. Fails if the table is not in the registry.
    pub fn drop_table(&mut self, table: &str) -> Result<(), String> {
        MetadataStore::new(&mut self.conn).drop_table(table)
    }

    pub fn get_table_indexes(&mut self, table: &str) -> Result<Vec<IndexDef>, String> {
        MetadataStore::new(&mut self.conn).get_table_indexes(table)
    }

    pub fn set_table_indexes(&mut self, table: &str, indexes: &[IndexDef]) -> Result<(), String> {
        MetadataStore::new(&mut self.conn).set_table_indexes(table, indexes)
    }

    /// Delete a key by name (raw Redis DEL). Use for virtual key table CRUD.
    pub fn del_key(&mut self, key: &str) -> Result<u64, String> {
        redis::cmd("DEL")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Set a string key (raw Redis SET). Use for virtual key table CRUD.
    pub fn set_key_string(&mut self, key: &str, value: &str) -> Result<(), String> {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Set TTL in seconds (EXPIRE). Use -1 to clear expiry via PERSIST (call `persist_key` for that).
    pub fn set_key_ttl(&mut self, key: &str, seconds: i64) -> Result<(), String> {
        if seconds < 0 {
            return self.persist_key(key);
        }
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(seconds)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Remove TTL from a key (PERSIST). Returns 1 if timeout was removed, 0 if key has no expiry.
    pub fn persist_key(&mut self, key: &str) -> Result<(), String> {
        let _: i32 = redis::cmd("PERSIST")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Return type of a single key (e.g. "string", "hash"). Key as bytes for binary keys.
    pub fn key_type(&mut self, key: &[u8]) -> Result<String, String> {
        redis::cmd("TYPE")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Number of keys in the current database (DBSIZE).
    pub fn dbsize(&mut self) -> Result<u64, String> {
        redis::cmd("DBSIZE")
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Discover top-level key prefixes by scanning (keys with "prefix:..." yield "prefix"). Used for key-pattern virtual tables.
    pub fn scan_key_prefixes(&mut self, max_keys: usize) -> Result<Vec<String>, String> {
        let mut prefixes: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cursor = 0u64;
        let mut total = 0usize;
        loop {
            let (next, keys) = self.scan_keys(cursor, 100, None)?;
            for key_bytes in &keys {
                if let Some(pos) = key_bytes.iter().position(|&b| b == b':') {
                    let prefix = String::from_utf8_lossy(&key_bytes[..pos]).into_owned();
                    if !prefix.is_empty() && !prefix.starts_with("__") {
                        prefixes.insert(prefix);
                    }
                }
                total += 1;
                if total >= max_keys {
                    break;
                }
            }
            if next == 0 || total >= max_keys {
                break;
            }
            cursor = next;
        }
        let mut out: Vec<String> = prefixes.into_iter().collect();
        out.sort();
        Ok(out)
    }

    /// SCAN cursor MATCH pattern COUNT count. Returns (`next_cursor`, keys as raw bytes). Use `bytes_to_display` for UI.
    pub fn scan_keys(
        &mut self,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
    ) -> Result<(u64, Vec<Vec<u8>>), String> {
        let match_pattern = pattern.unwrap_or("*");
        let (next, key_bytes): (String, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor.to_string())
            .arg("MATCH")
            .arg(match_pattern)
            .arg("COUNT")
            .arg(count)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        let next_cursor = next.parse::<u64>().unwrap_or(0);
        Ok((next_cursor, key_bytes))
    }

    /// Return TTL in seconds: None if key missing (-2), Some(-1) if no expiry, Some(n) if n seconds left.
    #[allow(dead_code)]
    pub fn key_ttl(&mut self, key: &[u8]) -> Result<Option<i64>, String> {
        let ttl: i64 = redis::cmd("TTL")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        Ok(match ttl {
            -2 => None,
            -1 => Some(-1),
            n => Some(n),
        })
    }

    /// Pipeline TYPE for many keys. Returns one type string per key (e.g. "string", "hash").
    pub fn key_types_batch(&mut self, keys: &[&[u8]]) -> Result<Vec<String>, String> {
        const CHUNK: usize = 500;
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for k in chunk {
                pipe.cmd("TYPE").arg(*k);
            }
            let types: Vec<String> = pipe.query(&mut self.conn).map_err(|e| e.to_string())?;
            out.extend(types);
        }
        Ok(out)
    }

    /// Value preview only (types already known). Use after `key_types_batch` when filtering/sorting by type.
    pub fn key_value_preview_batch(
        &mut self,
        keys: &[&Vec<u8>],
        types: &[String],
        json_path: Option<&str>,
    ) -> Result<Vec<String>, String> {
        preview::key_value_preview_batch(
            &mut self.conn,
            self.has_redis_json,
            keys,
            types,
            json_path,
        )
    }

    /// Pipeline TTL for many keys. Returns one Option<i64> per key (None = missing, Some(-1) = no expiry, Some(n) = seconds).
    pub fn key_ttl_batch(&mut self, keys: &[&[u8]]) -> Result<Vec<Option<i64>>, String> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.cmd("TTL").arg(*key);
        }
        let raw: Vec<i64> = pipe.query(&mut self.conn).map_err(|e| e.to_string())?;
        Ok(raw
            .into_iter()
            .map(|n| match n {
                -2 => None,
                -1 => Some(-1),
                s => Some(s),
            })
            .collect())
    }

    /// Batch (type, `value_preview`) for many keys using pipelining. Same semantics as `key_type_and_preview` per key.
    pub fn key_type_and_preview_batch(
        &mut self,
        keys: &[&Vec<u8>],
        json_path: Option<&str>,
    ) -> Result<Vec<(String, String)>, String> {
        preview::key_type_and_preview_batch(&mut self.conn, self.has_redis_json, keys, json_path)
    }

    /// Return (type, `value_preview`) for a key. Key is raw bytes so binary keys work. Preview uses lossy UTF-8 for display.
    #[allow(dead_code)]
    pub fn key_type_and_preview(&mut self, key: &[u8]) -> Result<(String, String), String> {
        preview::key_type_and_preview(&mut self.conn, key)
    }

    #[allow(dead_code)]
    pub const fn conn_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }
}
