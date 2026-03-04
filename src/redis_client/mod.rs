mod preview;

use crate::metadata::{MetadataStore, RowStorageMode};
use crate::models::{ColumnDef, ConnectionParams, IndexDef};
use crate::schema_cache::SchemaCache;
use base64::Engine;
use redis::cluster::ClusterClient;
use redis::ConnectionLike;
use redis::{transaction, ToRedisArgs};
use redis::{Connection, Pipeline, Value as RedisValue};

use redis::sentinel::{Sentinel, SentinelNodeConnectionInfo};
use redis::RedisConnectionInfo;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::time::Duration;

/// One stream entry: (`entry_id`, field-value pairs).
pub type StreamEntry = (String, Vec<(String, String)>);
/// List of stream entries from XRANGE.
pub type StreamEntries = Vec<StreamEntry>;

/// Abstraction over single-node and cluster connections so all commands go through `ConnectionLike`.
#[allow(clippy::large_enum_variant)]
pub enum RedisConn {
    Single(Connection),
    Cluster(Box<redis::cluster::ClusterConnection>),
}

impl redis::ConnectionLike for RedisConn {
    fn req_packed_command(&mut self, cmd: &[u8]) -> redis::RedisResult<RedisValue> {
        match self {
            Self::Single(c) => c.req_packed_command(cmd),
            Self::Cluster(c) => c.req_packed_command(cmd),
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> redis::RedisResult<Vec<redis::Value>> {
        match self {
            Self::Single(c) => c.req_packed_commands(cmd, offset, count),
            Self::Cluster(c) => c.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Single(c) => c.get_db(),
            Self::Cluster(c) => c.get_db(),
        }
    }

    fn check_connection(&mut self) -> bool {
        match self {
            Self::Single(c) => c.check_connection(),
            Self::Cluster(c) => c.check_connection(),
        }
    }

    fn is_open(&self) -> bool {
        match self {
            Self::Single(c) => c.is_open(),
            Self::Cluster(c) => c.is_open(),
        }
    }
}

/// Convert bytes to a String, replacing invalid UTF-8 so JSON and the UI never see invalid sequences.
pub fn bytes_to_display(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Lossless encoding of key bytes as Base64 for round-trip (e.g. binary keys in __`redis_keys`__).
pub fn bytes_to_key_id(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn bytes_to_display_utf8() {
        assert_eq!(bytes_to_display(b"hello"), "hello");
    }

    #[test]
    fn bytes_to_display_invalid_utf8() {
        let b = b"hi\xff\xfe";
        let s = bytes_to_display(b);
        assert!(s.contains("hi"));
        assert!(s.len() > 2);
    }

    #[test]
    fn bytes_to_key_id_roundtrip() {
        let raw = b"binary\x00key";
        let encoded = bytes_to_key_id(raw);
        assert!(!encoded.is_empty());
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .unwrap();
        assert_eq!(decoded.as_slice(), raw);
    }
}

fn connection_string(params: &ConnectionParams) -> String {
    let scheme = if params.use_tls() { "rediss" } else { "redis" };
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
    format!("{scheme}://{auth}{host}:{port}/{db}")
}

/// Returns a URL safe for logging: auth (user:password) is replaced with ***.
fn redact_url_for_log(url: &str) -> String {
    let rest = url
        .strip_prefix("redis://")
        .or_else(|| url.strip_prefix("rediss://"));
    let Some(rest) = rest else {
        return "redis(s)://***".to_string();
    };
    let (scheme, suffix) = if url.starts_with("rediss://") {
        ("rediss", rest)
    } else {
        ("redis", rest)
    };
    suffix.find('@').map_or_else(
        || format!("{scheme}://{suffix}"),
        |after_at| format!("{scheme}://***@{}", &suffix[after_at + 1..]),
    )
}

pub struct RedisClient {
    conn: RedisConn,
    has_redis_json: bool,
    schema_cache: SchemaCache,
}

/// Metadata store that caches column definitions and PK with TTL; invalidates on DDL.
pub struct MetadataStoreWithCache<'a> {
    conn: &'a mut RedisConn,
    cache: &'a mut SchemaCache,
}

impl MetadataStoreWithCache<'_> {
    const fn store(&mut self) -> MetadataStore<'_, RedisConn> {
        MetadataStore::new(self.conn)
    }

    pub fn list_tables(&mut self) -> Result<Vec<String>, String> {
        self.store().list_tables()
    }

    pub fn get_table_columns(&mut self, table: &str) -> Result<Option<Vec<ColumnDef>>, String> {
        if let Some(cols) = self.cache.get_columns(table) {
            return Ok(Some(cols));
        }
        let result = self.store().get_table_columns(table)?;
        if let Some(ref cols) = result {
            self.cache.set_columns(table, cols.clone());
        }
        Ok(result)
    }

    pub fn get_table_pk(&mut self, table: &str) -> Result<Option<String>, String> {
        if let Some(pk) = self.cache.get_pk(table) {
            return Ok(Some(pk));
        }
        let result = self.store().get_table_pk(table)?;
        if let Some(ref pk) = result {
            self.cache.set_pk(table, pk.clone());
        }
        Ok(result)
    }

    pub fn get_table_mode(&mut self, table: &str) -> Result<RowStorageMode, String> {
        self.store().get_table_mode(table)
    }

    pub fn get_table_indexes(&mut self, table: &str) -> Result<Vec<IndexDef>, String> {
        self.store().get_table_indexes(table)
    }

    pub fn set_table_indexes(&mut self, table: &str, indexes: &[IndexDef]) -> Result<(), String> {
        self.store().set_table_indexes(table, indexes)
    }

    pub fn register_table(
        &mut self,
        table: &str,
        pk_column: &str,
        columns: &[ColumnDef],
        mode: RowStorageMode,
    ) -> Result<(), String> {
        self.cache.invalidate_table(table);
        self.store().register_table(table, pk_column, columns, mode)
    }

    #[allow(dead_code)]
    pub fn scan_row_keys(
        &mut self,
        table: &str,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>), String> {
        self.store().scan_row_keys(table, cursor, count)
    }

    pub fn list_row_keys(&mut self, table: &str) -> Result<Vec<String>, String> {
        self.store().list_row_keys(table)
    }

    pub fn list_row_keys_paged(
        &mut self,
        table: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<String>, bool), String> {
        self.store().list_row_keys_paged(table, offset, limit)
    }

    pub fn get_row_hash(
        &mut self,
        table: &str,
        pk: &str,
    ) -> Result<Option<HashMap<String, String>>, String> {
        self.store().get_row_hash(table, pk)
    }

    pub fn set_row_hash(
        &mut self,
        table: &str,
        pk: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), String> {
        self.store().set_row_hash(table, pk, fields)
    }

    pub fn set_rows_hash_batch(
        &mut self,
        table: &str,
        rows: &[(String, HashMap<String, String>)],
    ) -> Result<(), String> {
        self.store().set_rows_hash_batch(table, rows)
    }

    pub fn get_rows_hash_batch(
        &mut self,
        table: &str,
        pks: &[String],
    ) -> Result<Vec<HashMap<String, String>>, String> {
        self.store().get_rows_hash_batch(table, pks)
    }

    pub fn get_row_json(&mut self, table: &str, pk: &str) -> Result<Option<JsonValue>, String> {
        self.store().get_row_json(table, pk)
    }

    pub fn set_row_json(&mut self, table: &str, pk: &str, value: &JsonValue) -> Result<(), String> {
        self.store().set_row_json(table, pk, value)
    }

    pub fn set_rows_json_batch(
        &mut self,
        table: &str,
        rows: &[(String, String)],
    ) -> Result<(), String> {
        self.store().set_rows_json_batch(table, rows)
    }

    pub fn get_rows_json_batch(
        &mut self,
        table: &str,
        pks: &[String],
    ) -> Result<Vec<Option<JsonValue>>, String> {
        self.store().get_rows_json_batch(table, pks)
    }

    pub fn delete_row(&mut self, table: &str, pk: &str) -> Result<(), String> {
        self.store().delete_row(table, pk)
    }

    pub fn delete_rows_batch(&mut self, table: &str, pks: &[String]) -> Result<u64, String> {
        self.store().delete_rows_batch(table, pks)
    }

    pub fn drop_table(&mut self, table: &str) -> Result<(), String> {
        self.cache.invalidate_table(table);
        self.store().drop_table(table)
    }
}

impl RedisClient {
    pub fn connect(params: &ConnectionParams) -> Result<Self, String> {
        let mut conn = if let Some(nodes) = params.cluster_nodes.as_ref().filter(|v| !v.is_empty())
        {
            log::debug!("Connecting to Redis Cluster with {} nodes", nodes.len());
            let client = ClusterClient::new(nodes.iter().map(std::string::String::as_str))
                .map_err(|e| e.to_string())?;
            let cluster_conn = client.get_connection().map_err(|e| e.to_string())?;
            log::info!("Redis Cluster connection established");
            RedisConn::Cluster(Box::new(cluster_conn))
        } else if let (Some(master), Some(sentinel_nodes)) = (
            params.sentinel_master.as_deref(),
            params.sentinel_nodes.as_ref(),
        ) {
            if sentinel_nodes.is_empty() {
                return Err(
                    "sentinel_nodes must be non-empty when sentinel_master is set".to_string(),
                );
            }
            log::debug!("Connecting via Sentinel master={master}");
            let node_urls: Vec<String> = sentinel_nodes
                .iter()
                .map(|s| {
                    let t = s.trim();
                    if t.starts_with("redis://") || t.starts_with("rediss://") {
                        t.to_string()
                    } else {
                        format!("redis://{t}")
                    }
                })
                .collect();
            let mut sentinel = Sentinel::build(node_urls).map_err(|e| e.to_string())?;
            let db = params
                .database
                .as_ref()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(0);
            let mut redis_info = RedisConnectionInfo::default().set_db(db);
            if let Some(u) = params.username.as_deref().filter(|x| !x.is_empty()) {
                redis_info = redis_info.set_username(u);
            }
            if let Some(p) = params.password.as_deref().filter(|x| !x.is_empty()) {
                redis_info = redis_info.set_password(p);
            }
            let mut node_info =
                SentinelNodeConnectionInfo::default().set_redis_connection_info(redis_info);
            if params.use_tls() {
                node_info = node_info.set_tls_mode(redis::TlsMode::Secure);
            }
            let master_client = sentinel
                .master_for(master, Some(&node_info))
                .map_err(|e| e.to_string())?;
            let connect_timeout = Duration::from_millis(params.connect_timeout_ms.unwrap_or(5000));
            let single_conn = master_client
                .get_connection_with_timeout(connect_timeout)
                .map_err(|e| e.to_string())?;
            let read_ms = params.read_timeout_ms.unwrap_or(10000);
            if let Err(e) = single_conn.set_read_timeout(Some(Duration::from_millis(read_ms))) {
                log::warn!("Failed to set read timeout ({read_ms} ms): {e}");
            }
            let write_ms = params.write_timeout_ms.unwrap_or(10000);
            if let Err(e) = single_conn.set_write_timeout(Some(Duration::from_millis(write_ms))) {
                log::warn!("Failed to set write timeout ({write_ms} ms): {e}");
            }
            log::info!("Redis connection established via Sentinel (master={master})");
            RedisConn::Single(single_conn)
        } else {
            let url = connection_string(params);
            log::debug!("Connecting to Redis at {}", redact_url_for_log(&url));
            let client = redis::Client::open(url.as_str()).map_err(|e| e.to_string())?;
            let connect_timeout = Duration::from_millis(params.connect_timeout_ms.unwrap_or(5000));
            let single_conn = client
                .get_connection_with_timeout(connect_timeout)
                .map_err(|e| e.to_string())?;
            let read_ms = params.read_timeout_ms.unwrap_or(10000);
            if let Err(e) = single_conn.set_read_timeout(Some(Duration::from_millis(read_ms))) {
                log::warn!("Failed to set read timeout ({read_ms} ms): {e}");
            }
            let write_ms = params.write_timeout_ms.unwrap_or(10000);
            if let Err(e) = single_conn.set_write_timeout(Some(Duration::from_millis(write_ms))) {
                log::warn!("Failed to set write timeout ({write_ms} ms): {e}");
            }
            log::info!("Redis connection established");
            RedisConn::Single(single_conn)
        };

        let has_redis_json = Self::detect_redis_json_connlike(&mut conn);
        log::info!("redis_json_available={has_redis_json}");
        Ok(Self {
            conn,
            has_redis_json,
            schema_cache: SchemaCache::with_default_ttl(),
        })
    }

    fn detect_redis_json_connlike(conn: &mut impl ConnectionLike) -> bool {
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

    /// Returns true if the connection is still open (no round-trip).
    pub fn is_open(&self) -> bool {
        self.conn.is_open()
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

    /// Access the metadata store for table/row operations. Columns and PK are cached with TTL; invalidated on DDL.
    pub const fn metadata(&mut self) -> MetadataStoreWithCache<'_> {
        MetadataStoreWithCache {
            conn: &mut self.conn,
            cache: &mut self.schema_cache,
        }
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
        if let Some(pk) = self.metadata().get_table_pk(table)? {
            return Ok(Some(pk));
        }
        let keys = self.metadata().list_row_keys(table)?;
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
        let keys = self.metadata().list_row_keys(table)?;
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
        if let Ok(Some(map)) = self.metadata().get_row_hash(table, first_pk) {
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
        if let Ok(Some(JsonValue::Object(obj))) = self.metadata().get_row_json(table, first_pk) {
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

    /// Run a transaction with optional WATCH on keys. The closure receives the connection and an
    /// atomic pipeline (MULTI/EXEC). Return Ok(Some(t)) to commit and return t, Ok(None) to retry.
    /// Cluster connections may have limited support for WATCH.
    #[allow(dead_code)]
    pub fn run_transaction<K, F, T>(&mut self, watch_keys: &[K], mut f: F) -> Result<T, String>
    where
        K: ToRedisArgs,
        F: FnMut(&mut RedisConn, &mut Pipeline) -> redis::RedisResult<Option<T>>,
    {
        transaction(&mut self.conn, watch_keys, &mut f).map_err(|e| e.to_string())
    }

    /// Delete a key by name (raw Redis DEL). Use for virtual key table CRUD.
    pub fn del_key(&mut self, key: &str) -> Result<u64, String> {
        redis::cmd("DEL")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Delete a key by raw bytes (for binary keys from `key_raw` round-trip).
    pub fn del_key_bytes(&mut self, key: &[u8]) -> Result<u64, String> {
        redis::cmd("DEL")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Delete multiple keys in chunked pipelines. Returns total number of keys removed.
    pub fn del_keys_batch(&mut self, keys: &[String]) -> Result<u64, String> {
        const CHUNK: usize = 500;
        if keys.is_empty() {
            return Ok(0);
        }
        let mut total = 0u64;
        for chunk in keys.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for k in chunk {
                pipe.cmd("DEL").arg(k.as_str());
            }
            let results: Vec<u64> = pipe.query(&mut self.conn).map_err(|e| e.to_string())?;
            total += results.iter().sum::<u64>();
        }
        Ok(total)
    }

    /// Set a string key (raw Redis SET). Use for virtual key table CRUD.
    pub fn set_key_string(&mut self, key: &str, value: &str) -> Result<(), String> {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Set a key by raw bytes (for binary keys from `key_raw` round-trip).
    pub fn set_key_bytes(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
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

    /// Set TTL for a key by raw bytes (binary keys from `key_raw`).
    pub fn set_key_ttl_bytes(&mut self, key: &[u8], seconds: i64) -> Result<(), String> {
        if seconds < 0 {
            return self.persist_key_bytes(key);
        }
        redis::cmd("EXPIRE")
            .arg(key)
            .arg(seconds)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// Remove TTL from a key by raw bytes (binary keys from `key_raw`).
    pub fn persist_key_bytes(&mut self, key: &[u8]) -> Result<(), String> {
        let _: i32 = redis::cmd("PERSIST")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// HSET key field value. For virtual key table hash updates.
    pub fn hset_field(&mut self, key: &[u8], field: &str, value: &str) -> Result<(), String> {
        redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg(value)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// HSET key with multiple field-value pairs.
    pub fn hset_multiple(
        &mut self,
        key: &[u8],
        fields: &HashMap<String, String>,
    ) -> Result<(), String> {
        if fields.is_empty() {
            return Ok(());
        }
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key);
        for (k, v) in fields {
            cmd.arg(k).arg(v);
        }
        cmd.query(&mut self.conn).map_err(|e| e.to_string())
    }

    /// LSET key index value. For virtual key table list updates.
    pub fn lset(&mut self, key: &[u8], index: i64, value: &str) -> Result<(), String> {
        redis::cmd("LSET")
            .arg(key)
            .arg(index)
            .arg(value)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// SADD key member [member ...]. For virtual key table set updates.
    pub fn sadd_members(&mut self, key: &[u8], members: &[String]) -> Result<(), String> {
        if members.is_empty() {
            return Ok(());
        }
        let mut cmd = redis::cmd("SADD");
        cmd.arg(key);
        for m in members {
            cmd.arg(m.as_str());
        }
        cmd.query(&mut self.conn).map_err(|e| e.to_string())
    }

    /// SREM key member [member ...]. Removes members from set.
    #[allow(dead_code)]
    pub fn srem_members(&mut self, key: &[u8], members: &[String]) -> Result<(), String> {
        if members.is_empty() {
            return Ok(());
        }
        let mut cmd = redis::cmd("SREM");
        cmd.arg(key);
        for m in members {
            cmd.arg(m.as_str());
        }
        cmd.query(&mut self.conn).map_err(|e| e.to_string())
    }

    /// ZADD key score member [score member ...]. For virtual key table zset updates.
    pub fn zadd_entries(&mut self, key: &[u8], entries: &[(f64, String)]) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut cmd = redis::cmd("ZADD");
        cmd.arg(key);
        for (score, member) in entries {
            cmd.arg(score).arg(member.as_str());
        }
        cmd.query(&mut self.conn).map_err(|e| e.to_string())
    }

    /// Return type of a single key (e.g. "string", "hash"). Key as bytes for binary keys.
    pub fn key_type(&mut self, key: &[u8]) -> Result<String, String> {
        redis::cmd("TYPE")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// HGETALL for a single key. Returns field-value pairs in iteration order. Uses lossy UTF-8 so binary data is displayable.
    pub fn hgetall(&mut self, key: &[u8]) -> Result<Vec<(String, String)>, String> {
        let raw: Vec<Vec<u8>> = redis::cmd("HGETALL")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(raw.len() / 2);
        for chunk in raw.chunks(2) {
            if chunk.len() >= 2 {
                out.push((
                    String::from_utf8_lossy(&chunk[0]).into_owned(),
                    String::from_utf8_lossy(&chunk[1]).into_owned(),
                ));
            }
        }
        Ok(out)
    }

    /// LRANGE key 0 -1. Returns all list elements.
    pub fn lrange_all(&mut self, key: &[u8]) -> Result<Vec<String>, String> {
        let raw: Vec<Vec<u8>> = redis::cmd("LRANGE")
            .arg(key)
            .arg(0i32)
            .arg(-1i32)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        Ok(raw
            .into_iter()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .collect())
    }

    /// SMEMBERS for a single key. Returns all set members.
    pub fn smembers(&mut self, key: &[u8]) -> Result<Vec<String>, String> {
        let raw: Vec<Vec<u8>> = redis::cmd("SMEMBERS")
            .arg(key)
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        Ok(raw
            .into_iter()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .collect())
    }

    /// ZRANGE key 0 -1 WITHSCORES. Returns (member, score) pairs.
    pub fn zrange_with_scores(&mut self, key: &[u8]) -> Result<Vec<(String, f64)>, String> {
        let raw: Vec<RedisValue> = redis::cmd("ZRANGE")
            .arg(key)
            .arg(0i32)
            .arg(-1i32)
            .arg("WITHSCORES")
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(raw.len() / 2);
        let mut i = 0;
        while i + 1 < raw.len() {
            let member = match &raw[i] {
                RedisValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                RedisValue::SimpleString(s) => s.clone(),
                _ => {
                    i += 1;
                    continue;
                }
            };
            let score = match &raw[i + 1] {
                RedisValue::BulkString(b) => {
                    String::from_utf8_lossy(b).parse::<f64>().unwrap_or(0.0)
                }
                RedisValue::SimpleString(s) => s.parse::<f64>().unwrap_or(0.0),
                RedisValue::Int(n) => {
                    #[allow(clippy::cast_precision_loss)]
                    let s = *n as f64;
                    s
                }
                RedisValue::Double(d) => *d,
                _ => 0.0,
            };
            out.push((member, score));
            i += 2;
        }
        Ok(out)
    }

    /// XRANGE key - +. Returns (`entry_id`, field-value pairs) for each stream entry.
    pub fn xrange_all(&mut self, key: &[u8]) -> Result<StreamEntries, String> {
        let raw: Vec<RedisValue> = redis::cmd("XRANGE")
            .arg(key)
            .arg("-")
            .arg("+")
            .query(&mut self.conn)
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for entry in raw {
            let RedisValue::Array(ref items) = entry else {
                continue;
            };
            if items.len() < 2 {
                continue;
            }
            let id = match &items[0] {
                RedisValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                RedisValue::SimpleString(s) => s.clone(),
                _ => continue,
            };
            let RedisValue::Array(ref field_list) = items[1] else {
                continue;
            };
            let mut fields = Vec::new();
            let mut j = 0;
            while j + 1 < field_list.len() {
                let f = match &field_list[j] {
                    RedisValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    RedisValue::SimpleString(s) => s.clone(),
                    _ => {
                        j += 1;
                        continue;
                    }
                };
                let v = match &field_list[j + 1] {
                    RedisValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    RedisValue::SimpleString(s) => s.clone(),
                    _ => String::new(),
                };
                fields.push((f, v));
                j += 2;
            }
            out.push((id, fields));
        }
        Ok(out)
    }

    /// Pipeline HGETALL for multiple keys. Returns one Vec<(String,String)> per key (field-value pairs). Uses lossy UTF-8 for binary data.
    pub fn hgetall_batch(&mut self, keys: &[&[u8]]) -> Result<Vec<Vec<(String, String)>>, String> {
        const CHUNK: usize = 100;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for k in chunk {
                pipe.cmd("HGETALL").arg(*k);
            }
            let raw_list: Vec<Vec<Vec<u8>>> =
                pipe.query(&mut self.conn).map_err(|e| e.to_string())?;
            for raw in raw_list {
                let mut pairs = Vec::with_capacity(raw.len() / 2);
                for c in raw.chunks(2) {
                    if c.len() >= 2 {
                        pairs.push((
                            String::from_utf8_lossy(&c[0]).into_owned(),
                            String::from_utf8_lossy(&c[1]).into_owned(),
                        ));
                    }
                }
                out.push(pairs);
            }
        }
        Ok(out)
    }

    /// Pipeline LRANGE key 0 -1 for multiple keys.
    pub fn lrange_all_batch(&mut self, keys: &[&[u8]]) -> Result<Vec<Vec<String>>, String> {
        const CHUNK: usize = 100;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for k in chunk {
                pipe.cmd("LRANGE").arg(*k).arg(0i32).arg(-1i32);
            }
            let raw_list: Vec<Vec<Vec<u8>>> =
                pipe.query(&mut self.conn).map_err(|e| e.to_string())?;
            for raw in raw_list {
                out.push(
                    raw.into_iter()
                        .map(|b| String::from_utf8_lossy(&b).into_owned())
                        .collect(),
                );
            }
        }
        Ok(out)
    }

    /// Pipeline SMEMBERS for multiple keys.
    pub fn smembers_batch(&mut self, keys: &[&[u8]]) -> Result<Vec<Vec<String>>, String> {
        const CHUNK: usize = 100;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for k in chunk {
                pipe.cmd("SMEMBERS").arg(*k);
            }
            let raw_list: Vec<Vec<Vec<u8>>> =
                pipe.query(&mut self.conn).map_err(|e| e.to_string())?;
            for raw in raw_list {
                out.push(
                    raw.into_iter()
                        .map(|b| String::from_utf8_lossy(&b).into_owned())
                        .collect(),
                );
            }
        }
        Ok(out)
    }

    /// Pipeline ZRANGE key 0 -1 WITHSCORES for multiple keys.
    pub fn zrange_with_scores_batch(
        &mut self,
        keys: &[&[u8]],
    ) -> Result<Vec<Vec<(String, f64)>>, String> {
        const CHUNK: usize = 50;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for k in chunk {
                pipe.cmd("ZRANGE")
                    .arg(*k)
                    .arg(0i32)
                    .arg(-1i32)
                    .arg("WITHSCORES");
            }
            let raw_list: Vec<Vec<RedisValue>> =
                pipe.query(&mut self.conn).map_err(|e| e.to_string())?;
            for raw in raw_list {
                let mut pairs = Vec::new();
                let mut i = 0;
                while i + 1 < raw.len() {
                    let member = match &raw[i] {
                        RedisValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                        RedisValue::SimpleString(s) => s.clone(),
                        _ => {
                            i += 1;
                            continue;
                        }
                    };
                    let score = match &raw[i + 1] {
                        RedisValue::BulkString(b) => {
                            String::from_utf8_lossy(b).parse::<f64>().unwrap_or(0.0)
                        }
                        RedisValue::SimpleString(s) => s.parse::<f64>().unwrap_or(0.0),
                        RedisValue::Int(n) => {
                            #[allow(clippy::cast_precision_loss)]
                            let s = *n as f64;
                            s
                        }
                        RedisValue::Double(d) => *d,
                        _ => 0.0,
                    };
                    pairs.push((member, score));
                    i += 2;
                }
                out.push(pairs);
            }
        }
        Ok(out)
    }

    /// Pipeline XRANGE key - + for multiple keys.
    pub fn xrange_all_batch(&mut self, keys: &[&[u8]]) -> Result<Vec<StreamEntries>, String> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let entries = self.xrange_all(key)?;
            out.push(entries);
        }
        Ok(out)
    }

    /// Number of keys in the current database (DBSIZE).
    pub fn dbsize(&mut self) -> Result<u64, String> {
        redis::cmd("DBSIZE")
            .query(&mut self.conn)
            .map_err(|e| e.to_string())
    }

    /// INFO [section]. Returns Redis INFO output as a string. Section defaults to "all".
    pub fn info(&mut self, section: Option<&str>) -> Result<String, String> {
        let mut cmd = redis::cmd("INFO");
        cmd.arg(section.unwrap_or("all"));
        let raw: Vec<u8> = cmd.query(&mut self.conn).map_err(|e| e.to_string())?;
        Ok(String::from_utf8_lossy(&raw).into_owned())
    }

    /// PUBSUB CHANNELS [pattern]. Returns list of channel names. Pattern defaults to *.
    pub fn pubsub_channels(&mut self, pattern: Option<&str>) -> Result<Vec<String>, String> {
        let mut cmd = redis::cmd("PUBSUB");
        cmd.arg("CHANNELS");
        if let Some(p) = pattern {
            cmd.arg(p);
        } else {
            cmd.arg("*");
        }
        cmd.query(&mut self.conn).map_err(|e| e.to_string())
    }

    /// PUBSUB NUMSUB channel [channel ...]. Returns (`channel_name`, `subscriber_count`) per channel.
    pub fn pubsub_numsub(&mut self, channels: &[String]) -> Result<Vec<(String, u64)>, String> {
        if channels.is_empty() {
            return Ok(vec![]);
        }
        let mut cmd = redis::cmd("PUBSUB");
        cmd.arg("NUMSUB");
        for c in channels {
            cmd.arg(c.as_str());
        }
        let raw: Vec<RedisValue> = cmd.query(&mut self.conn).map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(channels.len());
        for chunk in raw.chunks(2) {
            if chunk.len() >= 2 {
                let name = match &chunk[0] {
                    RedisValue::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    RedisValue::Int(n) => n.to_string(),
                    _ => continue,
                };
                let count = match &chunk[1] {
                    RedisValue::Int(n) => (*n).unsigned_abs(),
                    _ => 0,
                };
                out.push((name, count));
            }
        }
        Ok(out)
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
        keys: &[&[u8]],
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
        keys: &[&[u8]],
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
    pub const fn conn_mut(&mut self) -> &mut RedisConn {
        &mut self.conn
    }
}
