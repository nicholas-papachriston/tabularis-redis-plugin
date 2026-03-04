use crate::models::conn_params_from_request;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};

fn connection_key(params: &crate::models::ConnectionParams) -> String {
    let host = params.host.as_deref().unwrap_or("127.0.0.1");
    let port = params.port.unwrap_or(6379);
    let db = params
        .database
        .as_ref()
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(0);
    format!("{host}:{port}/{db}")
}

/// Get a cached connection (validated with PING) or create a new one. Evicts stale entries.
fn get_or_create_connection<'a>(
    connections: &'a mut HashMap<String, RedisClient>,
    key: &str,
    conn_params: &crate::models::ConnectionParams,
) -> Result<&'a mut RedisClient, String> {
    loop {
        let cached_ok = connections.get_mut(key).is_some_and(|c| c.ping().is_ok());
        if cached_ok {
            return Ok(connections.get_mut(key).expect("cached connection present"));
        }
        let had_cached = connections.contains_key(key);
        connections.remove(key);
        if had_cached {
            log::warn!("Stale connection for key {key}, reconnecting");
        }
        log::debug!("No cached connection for key {key}, creating one");
        let new_client = RedisClient::connect(conn_params)?;
        log::info!("Created new Redis connection for key {key}");
        connections.insert(key.to_string(), new_client);
    }
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
        let mut res_str = serde_json::to_string(&response).expect("serialize response");
        res_str.push('\n');
        let _ = stdout.write_all(res_str.as_bytes());
        let _ = stdout.flush();
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
    let mut s = serde_json::to_string(&response).expect("serialize error");
    s.push('\n');
    let _ = stdout.write_all(s.as_bytes());
    let _ = stdout.flush();
}
