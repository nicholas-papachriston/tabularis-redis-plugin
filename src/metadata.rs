use crate::models::{ColumnDef, IndexDef};
use redis::ConnectionLike;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

const META_PREFIX: &str = "tabularis:meta";
const DATA_PREFIX: &str = "tabularis:data";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowStorageMode {
    Hash,
    Json,
}

impl RowStorageMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Json => "json",
        }
    }

    pub const fn from_str(mode: &str) -> Self {
        if mode.eq_ignore_ascii_case("json") {
            Self::Json
        } else {
            Self::Hash
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_storage_mode_from_str() {
        assert_eq!(RowStorageMode::from_str("json"), RowStorageMode::Json);
        assert_eq!(RowStorageMode::from_str("JSON"), RowStorageMode::Json);
        assert_eq!(RowStorageMode::from_str("hash"), RowStorageMode::Hash);
        assert_eq!(RowStorageMode::from_str("Hash"), RowStorageMode::Hash);
        assert_eq!(RowStorageMode::from_str("other"), RowStorageMode::Hash);
    }
}

pub struct MetadataStore<'a, C: ConnectionLike> {
    conn: &'a mut C,
}

impl<'a, C: ConnectionLike> MetadataStore<'a, C> {
    pub const fn new(conn: &'a mut C) -> Self {
        Self { conn }
    }

    fn meta_tables_key() -> String {
        format!("{META_PREFIX}:tables")
    }

    fn meta_table_columns_key(table: &str) -> String {
        format!("{META_PREFIX}:table:{table}:columns")
    }

    fn meta_table_pk_key(table: &str) -> String {
        format!("{META_PREFIX}:table:{table}:pk")
    }

    fn meta_table_mode_key(table: &str) -> String {
        format!("{META_PREFIX}:table:{table}:mode")
    }

    fn meta_table_indexes_key(table: &str) -> String {
        format!("{META_PREFIX}:table:{table}:indexes")
    }

    fn data_key(table: &str, pk: &str) -> String {
        format!("{DATA_PREFIX}:{table}:{pk}")
    }

    fn data_keys_set(table: &str) -> String {
        format!("{DATA_PREFIX}:{table}:keys")
    }

    pub fn list_tables(&mut self) -> Result<Vec<String>, String> {
        let key = Self::meta_tables_key();
        let tables: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&key)
            .query(self.conn)
            .map_err(|e| e.to_string())?;
        log::debug!("list_tables: key={key} count={}", tables.len());
        Ok(tables)
    }

    pub fn get_table_columns(&mut self, table: &str) -> Result<Option<Vec<ColumnDef>>, String> {
        let key = Self::meta_table_columns_key(table);
        let raw: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query(self.conn)
            .map_err(|e| e.to_string())?;
        let Some(serialized) = raw else {
            return Ok(None);
        };
        let cols: Vec<ColumnDef> = serde_json::from_str(&serialized).map_err(|e| e.to_string())?;
        Ok(Some(cols))
    }

    pub fn get_table_pk(&mut self, table: &str) -> Result<Option<String>, String> {
        let key = Self::meta_table_pk_key(table);
        redis::cmd("GET")
            .arg(&key)
            .query(self.conn)
            .map_err(|e| e.to_string())
    }

    pub fn get_table_mode(&mut self, table: &str) -> Result<RowStorageMode, String> {
        let key = Self::meta_table_mode_key(table);
        let mode: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query(self.conn)
            .map_err(|e| e.to_string())?;
        Ok(mode
            .as_deref()
            .map_or(RowStorageMode::Hash, RowStorageMode::from_str))
    }

    pub fn get_table_indexes(&mut self, table: &str) -> Result<Vec<IndexDef>, String> {
        let key = Self::meta_table_indexes_key(table);
        let raw: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query(self.conn)
            .map_err(|e| e.to_string())?;
        let Some(serialized) = raw else {
            return Ok(Vec::new());
        };
        let list: Vec<IndexDef> = serde_json::from_str(&serialized).map_err(|e| e.to_string())?;
        Ok(list)
    }

    pub fn set_table_indexes(&mut self, table: &str, indexes: &[IndexDef]) -> Result<(), String> {
        let key = Self::meta_table_indexes_key(table);
        let serialized = serde_json::to_string(indexes).map_err(|e| e.to_string())?;
        redis::cmd("SET")
            .arg(&key)
            .arg(&serialized)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())
    }

    pub fn register_table(
        &mut self,
        table: &str,
        pk_column: &str,
        columns: &[ColumnDef],
        mode: RowStorageMode,
    ) -> Result<(), String> {
        log::info!(
            "register_table: table={table} pk={pk_column} mode={} columns={}",
            mode.as_str(),
            columns.len()
        );
        let tables_key = Self::meta_tables_key();
        redis::cmd("SADD")
            .arg(&tables_key)
            .arg(table)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        redis::cmd("SET")
            .arg(Self::meta_table_pk_key(table))
            .arg(pk_column)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        let cols_json = serde_json::to_string(columns).map_err(|e| e.to_string())?;
        redis::cmd("SET")
            .arg(Self::meta_table_columns_key(table))
            .arg(&cols_json)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        redis::cmd("SET")
            .arg(Self::meta_table_mode_key(table))
            .arg(mode.as_str())
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// SSCAN one chunk of row keys. Returns (`next_cursor`, batch). Use cursor=0 to start; when `next_cursor` is 0, done.
    pub fn scan_row_keys(
        &mut self,
        table: &str,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>), String> {
        let key = Self::data_keys_set(table);
        let (next, batch): (u64, Vec<String>) = redis::cmd("SSCAN")
            .arg(&key)
            .arg(cursor)
            .arg("COUNT")
            .arg(count)
            .query(self.conn)
            .map_err(|e| e.to_string())?;
        Ok((next, batch))
    }

    /// Maximum row keys to load into memory to avoid OOM on very large tables.
    const MAX_ROW_KEYS: usize = 500_000;

    pub fn list_row_keys(&mut self, table: &str) -> Result<Vec<String>, String> {
        const CHUNK: usize = 500;
        let mut cursor = 0u64;
        let mut out = Vec::new();
        loop {
            let (next, batch) = self.scan_row_keys(table, cursor, CHUNK)?;
            for k in batch {
                if out.len() >= Self::MAX_ROW_KEYS {
                    log::warn!(
                        "list_row_keys: table {table} hit safety cap {} keys, stopping scan",
                        Self::MAX_ROW_KEYS
                    );
                    return Ok(out);
                }
                out.push(k);
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(out)
    }

    /// Fetch a page of row keys without loading the full set. Returns (`page_keys`, `has_more`).
    pub fn list_row_keys_paged(
        &mut self,
        table: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<String>, bool), String> {
        const CHUNK: usize = 200;
        let mut cursor = 0u64;
        let mut collected = Vec::with_capacity((offset + limit).min(1000));
        #[allow(unused_assignments)]
        let mut last_next = 0u64;
        loop {
            let (next, batch) = self.scan_row_keys(table, cursor, CHUNK)?;
            last_next = next;
            for k in batch {
                if collected.len() >= offset + limit {
                    break;
                }
                collected.push(k);
            }
            if next == 0 || collected.len() >= offset + limit {
                break;
            }
            cursor = next;
        }
        let start = offset.min(collected.len());
        let end = (offset + limit).min(collected.len());
        let page: Vec<String> = collected[start..end].to_vec();
        Ok((page, last_next != 0))
    }

    pub fn get_row_hash(
        &mut self,
        table: &str,
        pk: &str,
    ) -> Result<Option<HashMap<String, String>>, String> {
        let key = Self::data_key(table, pk);
        redis::cmd("HGETALL")
            .arg(&key)
            .query(self.conn)
            .map_err(|e| e.to_string())
    }

    pub fn set_row_hash(
        &mut self,
        table: &str,
        pk: &str,
        fields: &HashMap<String, String>,
    ) -> Result<(), String> {
        let key = Self::data_key(table, pk);
        let keys_key = Self::data_keys_set(table);

        let mut cmd = redis::cmd("HSET");
        cmd.arg(&key);
        for (k, v) in fields {
            cmd.arg(k).arg(v);
        }
        cmd.query::<()>(self.conn).map_err(|e| e.to_string())?;

        redis::cmd("SADD")
            .arg(&keys_key)
            .arg(pk)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Insert multiple hash rows in a single pipeline. Keys set is updated for each pk.
    pub fn set_rows_hash_batch(
        &mut self,
        table: &str,
        rows: &[(String, HashMap<String, String>)],
    ) -> Result<(), String> {
        const CHUNK: usize = 200;
        if rows.is_empty() {
            return Ok(());
        }
        let keys_key = Self::data_keys_set(table);
        for chunk in rows.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for (pk, fields) in chunk {
                let key = Self::data_key(table, pk);
                pipe.cmd("HSET").arg(&key);
                for (k, v) in fields {
                    pipe.arg(k).arg(v);
                }
                pipe.ignore();
                pipe.cmd("SADD").arg(&keys_key).arg(pk).ignore();
            }
            pipe.query::<()>(self.conn).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Fetch multiple hash rows in pipelined chunks. Returns one `HashMap` per pk; empty map if key missing or empty.
    pub fn get_rows_hash_batch(
        &mut self,
        table: &str,
        pks: &[String],
    ) -> Result<Vec<HashMap<String, String>>, String> {
        const CHUNK: usize = 200;
        if pks.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(pks.len());
        for chunk in pks.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for pk in chunk {
                let key = Self::data_key(table, pk);
                pipe.cmd("HGETALL").arg(&key);
            }
            let chunk_results: Vec<HashMap<String, String>> =
                pipe.query(self.conn).map_err(|e| e.to_string())?;
            out.extend(chunk_results);
        }
        Ok(out)
    }

    pub fn get_row_json(&mut self, table: &str, pk: &str) -> Result<Option<JsonValue>, String> {
        let key = Self::data_key(table, pk);
        let raw: Option<String> = redis::cmd("JSON.GET")
            .arg(&key)
            .arg("$")
            .query(self.conn)
            .map_err(|e| e.to_string())?;
        let Some(serialized) = raw else {
            return Ok(None);
        };
        let arr: Vec<JsonValue> = serde_json::from_str(&serialized).map_err(|e| e.to_string())?;
        Ok(arr.into_iter().next())
    }

    pub fn set_row_json(&mut self, table: &str, pk: &str, value: &JsonValue) -> Result<(), String> {
        let key = Self::data_key(table, pk);
        let keys_key = Self::data_keys_set(table);
        let payload = value.to_string();

        redis::cmd("JSON.SET")
            .arg(&key)
            .arg("$")
            .arg(&payload)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        redis::cmd("SADD")
            .arg(&keys_key)
            .arg(pk)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Insert multiple JSON rows in a single pipeline. Keys set is updated for each pk.
    pub fn set_rows_json_batch(
        &mut self,
        table: &str,
        rows: &[(String, String)],
    ) -> Result<(), String> {
        const CHUNK: usize = 200;
        if rows.is_empty() {
            return Ok(());
        }
        let keys_key = Self::data_keys_set(table);
        for chunk in rows.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for (pk, payload) in chunk {
                let key = Self::data_key(table, pk);
                pipe.cmd("JSON.SET")
                    .arg(&key)
                    .arg("$")
                    .arg(payload)
                    .ignore();
                pipe.cmd("SADD").arg(&keys_key).arg(pk).ignore();
            }
            pipe.query::<()>(self.conn).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Fetch multiple JSON rows in pipelined chunks. Returns one Option<JsonValue> per pk; None if key missing or not JSON.
    pub fn get_rows_json_batch(
        &mut self,
        table: &str,
        pks: &[String],
    ) -> Result<Vec<Option<JsonValue>>, String> {
        const CHUNK: usize = 200;
        if pks.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(pks.len());
        for chunk in pks.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for pk in chunk {
                let key = Self::data_key(table, pk);
                pipe.cmd("JSON.GET").arg(&key).arg("$");
            }
            let raw_list: Vec<Option<String>> = pipe.query(self.conn).map_err(|e| e.to_string())?;
            for raw in raw_list {
                let opt = match raw {
                    Some(serialized) => {
                        let arr: Vec<JsonValue> =
                            serde_json::from_str(&serialized).map_err(|e| e.to_string())?;
                        arr.into_iter().next()
                    }
                    None => None,
                };
                out.push(opt);
            }
        }
        Ok(out)
    }

    pub fn delete_row(&mut self, table: &str, pk: &str) -> Result<(), String> {
        let key = Self::data_key(table, pk);
        let keys_key = Self::data_keys_set(table);

        redis::cmd("DEL")
            .arg(&key)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;
        redis::cmd("SREM")
            .arg(&keys_key)
            .arg(pk)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Delete multiple rows in chunked pipelines. Returns the number of rows deleted.
    pub fn delete_rows_batch(&mut self, table: &str, pks: &[String]) -> Result<u64, String> {
        const CHUNK: usize = 500;
        if pks.is_empty() {
            return Ok(0);
        }
        let keys_set = Self::data_keys_set(table);
        for chunk in pks.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for pk in chunk {
                let key = Self::data_key(table, pk);
                pipe.cmd("DEL").arg(&key).ignore();
                pipe.cmd("SREM").arg(&keys_set).arg(pk).ignore();
            }
            pipe.query::<()>(self.conn).map_err(|e| e.to_string())?;
        }
        Ok(pks.len() as u64)
    }

    /// Remove a table from the registry and delete all its data and metadata. Fails if the table is not in the registry.
    pub fn drop_table(&mut self, table: &str) -> Result<(), String> {
        const CHUNK: usize = 500;
        let tables_key = Self::meta_tables_key();
        let is_member: u8 = redis::cmd("SISMEMBER")
            .arg(&tables_key)
            .arg(table)
            .query(self.conn)
            .map_err(|e| e.to_string())?;
        if is_member == 0 {
            return Err(format!(
                "Table '{table}' not found or not a droppable table"
            ));
        }

        let pks = self.list_row_keys(table)?;
        let keys_set = Self::data_keys_set(table);
        for chunk in pks.chunks(CHUNK) {
            let mut pipe = redis::pipe();
            for pk in chunk {
                let key = Self::data_key(table, pk);
                pipe.cmd("DEL").arg(&key).ignore();
                pipe.cmd("SREM").arg(&keys_set).arg(pk).ignore();
            }
            pipe.query::<()>(self.conn).map_err(|e| e.to_string())?;
        }

        redis::cmd("DEL")
            .arg(&keys_set)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        for key in &[
            Self::meta_table_columns_key(table),
            Self::meta_table_pk_key(table),
            Self::meta_table_mode_key(table),
            Self::meta_table_indexes_key(table),
        ] {
            redis::cmd("DEL")
                .arg(key)
                .query::<()>(self.conn)
                .map_err(|e| e.to_string())?;
        }

        redis::cmd("SREM")
            .arg(&tables_key)
            .arg(table)
            .query::<()>(self.conn)
            .map_err(|e| e.to_string())?;

        log::info!(
            "drop_table: dropped table {table} ({} rows removed)",
            pks.len()
        );
        Ok(())
    }
}
