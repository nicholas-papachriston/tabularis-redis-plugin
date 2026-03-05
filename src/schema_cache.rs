//! In-process cache for table column definitions and primary key with TTL.
//! Reduces Redis round-trips for metadata that is read on every query but changes rarely.

use crate::models::ColumnDef;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_TTL_SECS: u64 = 60;

/// Caches table columns and PK in memory. Entries expire after `ttl`.
/// Columns are stored as Arc to avoid cloning when populating from store; one clone on read.
pub struct SchemaCache {
    columns: HashMap<String, (Arc<Vec<ColumnDef>>, Instant)>,
    pk: HashMap<String, (Arc<str>, Instant)>,
    ttl: Duration,
}

impl SchemaCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            columns: HashMap::new(),
            pk: HashMap::new(),
            ttl,
        }
    }

    pub fn with_default_ttl() -> Self {
        Self::new(Duration::from_secs(DEFAULT_TTL_SECS))
    }

    fn is_expired(created: Instant, ttl: Duration) -> bool {
        created.elapsed() >= ttl
    }

    pub fn get_columns(&self, table: &str) -> Option<Vec<ColumnDef>> {
        let (cols, created) = self.columns.get(table)?;
        if Self::is_expired(*created, self.ttl) {
            return None;
        }
        Some((**cols).clone())
    }

    pub fn get_pk(&self, table: &str) -> Option<String> {
        let (pk, created) = self.pk.get(table)?;
        if Self::is_expired(*created, self.ttl) {
            return None;
        }
        Some((*pk).to_string())
    }

    /// Takes ownership of `columns` to avoid clone at call site.
    pub fn set_columns(&mut self, table: &str, columns: Vec<ColumnDef>) {
        self.columns
            .insert(table.to_string(), (Arc::new(columns), Instant::now()));
    }

    pub fn set_pk(&mut self, table: &str, pk: &str) {
        self.pk
            .insert(table.to_string(), (Arc::from(pk), Instant::now()));
    }

    /// Remove cached columns and PK for a table. Call after DDL (CREATE TABLE, DROP TABLE, ALTER).
    pub fn invalidate_table(&mut self, table: &str) {
        self.columns.remove(table);
        self.pk.remove(table);
    }
}
