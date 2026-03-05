use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Connection params from Tabularis (params.params in JSON-RPC).
#[derive(Debug, Clone, Default)]
pub struct ConnectionParams {
    pub driver: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ssl_mode: Option<String>,
    pub tls: Option<bool>,
    pub connect_timeout_ms: Option<u64>,
    pub read_timeout_ms: Option<u64>,
    pub write_timeout_ms: Option<u64>,
    pub cluster_nodes: Option<Vec<String>>,
    pub sentinel_master: Option<String>,
    pub sentinel_nodes: Option<Vec<String>>,
}

impl ConnectionParams {
    pub fn from_json(params: &JsonValue) -> Self {
        let empty = serde_json::Map::new();
        let obj = params.as_object().unwrap_or(&empty);
        Self {
            driver: obj
                .get("driver")
                .and_then(|v| v.as_str())
                .unwrap_or("redis")
                .to_string(),
            host: obj.get("host").and_then(|v| v.as_str()).map(String::from),
            port: obj
                .get("port")
                .and_then(serde_json::Value::as_u64)
                .and_then(|p| u16::try_from(p).ok()),
            database: obj.get("database").and_then(|v| {
                v.as_str().map(String::from).or_else(|| {
                    v.as_array()
                        .and_then(|a| a.first())
                        .and_then(|x| x.as_str().map(String::from))
                })
            }),
            username: obj
                .get("username")
                .and_then(|v| v.as_str())
                .map(String::from),
            password: obj
                .get("password")
                .and_then(|v| v.as_str())
                .map(String::from),
            ssl_mode: obj
                .get("ssl_mode")
                .and_then(|v| v.as_str())
                .map(String::from),
            tls: obj.get("tls").and_then(serde_json::Value::as_bool),
            connect_timeout_ms: obj
                .get("connect_timeout_ms")
                .and_then(serde_json::Value::as_u64),
            read_timeout_ms: obj
                .get("read_timeout_ms")
                .and_then(serde_json::Value::as_u64),
            write_timeout_ms: obj
                .get("write_timeout_ms")
                .and_then(serde_json::Value::as_u64),
            cluster_nodes: obj
                .get("cluster_nodes")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                }),
            sentinel_master: obj
                .get("sentinel_master")
                .and_then(|v| v.as_str())
                .map(String::from),
            sentinel_nodes: obj
                .get("sentinel_nodes")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                }),
        }
    }

    /// True if TLS should be used (explicit tls: true or `ssl_mode` require/verify-full).
    pub fn use_tls(&self) -> bool {
        if self.tls == Some(true) {
            return true;
        }
        let mode = match &self.ssl_mode {
            Some(m) => m.as_str(),
            None => return false,
        };
        let mode_lower: String = mode.chars().flat_map(char::to_lowercase).collect();
        matches!(
            mode_lower.as_str(),
            "require" | "verify-full" | "verify-ca" | "prefer"
        )
    }
}

/// Index definition for metadata storage (tabularis:meta:table:{table}:indexes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub index_name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub is_unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    #[serde(default)]
    pub column_default: Option<String>,
    pub is_primary_key: bool,
    #[serde(default)]
    pub is_auto_increment: bool,
    #[serde(default)]
    pub comment: Option<String>,
}

/// Extract `ConnectionParams` from JSON-RPC request params.
/// Request params shape: { "params": `ConnectionParams`, "schema": ?, "table": ?, ... }.
pub fn conn_params_from_request(params: &JsonValue) -> ConnectionParams {
    let inner = params.get("params").unwrap_or(&JsonValue::Null);
    ConnectionParams::from_json(inner)
}
