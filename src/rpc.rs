use crate::models::conn_params_from_request;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Write};

fn connection_key(params: &crate::models::ConnectionParams) -> String {
    let host = params.host.as_deref().unwrap_or("127.0.0.1");
    let port = params.port.unwrap_or(6379);
    let db = params
        .database
        .as_ref()
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(0);
    let username = params.username.as_deref().unwrap_or("");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    params.password.as_deref().unwrap_or("").hash(&mut hasher);
    let pw_tag = hasher.finish();
    let tls = params.use_tls();
    let ct = params.connect_timeout_ms.unwrap_or(5000);
    let rt = params.read_timeout_ms.unwrap_or(10000);
    let wt = params.write_timeout_ms.unwrap_or(10000);
    let cluster = params
        .cluster_nodes
        .as_ref()
        .map(|v| {
            let mut v = v.clone();
            v.sort();
            v.join(",")
        })
        .unwrap_or_default();
    let sentinel_master = params.sentinel_master.as_deref().unwrap_or("");
    let sentinel_nodes = params
        .sentinel_nodes
        .as_ref()
        .map(|v| {
            let mut v = v.clone();
            v.sort();
            v.join(",")
        })
        .unwrap_or_default();
    format!("{host}:{port}/{db}|u={username}|pw={pw_tag}|tls={tls}|ct={ct}|rt={rt}|wt={wt}|cluster={cluster}|sm={sentinel_master}|sn={sentinel_nodes}")
}

/// Get a cached connection (if open) or create a new one. Evicts stale entries without PING round-trip.
fn get_or_create_connection<'a>(
    connections: &'a mut HashMap<String, RedisClient>,
    key: &str,
    conn_params: &crate::models::ConnectionParams,
) -> Result<&'a mut RedisClient, String> {
    if connections
        .get(key)
        .is_some_and(super::redis_client::RedisClient::is_open)
    {
        return Ok(connections.get_mut(key).expect("just checked"));
    }
    connections.remove(key);
    let client = RedisClient::connect(conn_params)?;
    log::info!("Created new Redis connection for key {key}");
    connections.insert(key.to_string(), client);
    Ok(connections.get_mut(key).expect("just inserted"))
}

/// Handle a single JSON-RPC request: resolve connection, dispatch method, return response.
/// Caller is responsible for transport (read line, write response).
pub fn handle_request(
    req: &JsonValue,
    connections: &mut HashMap<String, RedisClient>,
) -> JsonValue {
    let id = &req["id"];
    let method = match req["method"].as_str() {
        Some(m) => m.to_string(),
        None => {
            return crate::methods::AppError::InvalidParams("Method not specified".into())
                .to_rpc_error(id);
        }
    };
    let params = req["params"].clone();

    if method == "get_databases" {
        return crate::methods::dispatch_get_databases_no_connection(id);
    }

    let conn_params = conn_params_from_request(&params);
    let key = connection_key(&conn_params);
    log::debug!("Handling method '{method}' for connection key {key}");

    let client = match get_or_create_connection(connections, &key, &conn_params) {
        Ok(c) => c,
        Err(e) => {
            log::warn!("Failed to connect for method '{method}', key {key}: {e}");
            return crate::methods::AppError::Backend(e).to_rpc_error(id);
        }
    };

    crate::methods::dispatch(&method, &params, id, client)
}

pub fn run_loop(connections: &mut HashMap<String, RedisClient>) {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    log::info!("RPC loop started");

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                log::error!("Error reading stdin: {e}");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let req: JsonValue = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("Parse error on incoming request: {e}");
                send_error(&mut stdout, &JsonValue::Null, -32700, &e.to_string());
                continue;
            }
        };
        let response = handle_request(&req, connections);
        if response.get("error").is_some() {
            let msg = response["error"]["message"]
                .as_str()
                .unwrap_or("(no message)");
            log::debug!("RPC response: error={msg}");
        } else {
            log::debug!("RPC response: success");
        }
        let res_str = match serde_json::to_string(&response) {
            Ok(s) => s,
            Err(e) => {
                log::error!("Failed to serialize response: {e}");
                send_error(
                    &mut stdout,
                    &req["id"],
                    -32603,
                    "Internal error: serialize response",
                );
                continue;
            }
        };
        let out = res_str + "\n";
        if let Err(e) = stdout.write_all(out.as_bytes()) {
            log::error!("Failed to write response: {e}");
            break;
        }
        if let Err(e) = stdout.flush() {
            log::error!("Failed to flush stdout: {e}");
            break;
        }
    }
    log::info!("RPC loop ended");
}

fn send_error(stdout: &mut io::Stdout, id: &JsonValue, code: i32, message: &str) {
    log::warn!("RPC error: code={code} message={message}");
    let response = serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": message },
        "id": id
    });
    match serde_json::to_string(&response) {
        Ok(s) => {
            let out = s + "\n";
            let _ = stdout.write_all(out.as_bytes());
            let _ = stdout.flush();
        }
        Err(e) => log::error!("Failed to serialize error response: {e}"),
    }
}
