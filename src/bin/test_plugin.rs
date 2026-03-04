// Integration test: spawns the plugin and sends JSON-RPC requests. Requires a running Redis.
// Run with: cargo run --bin test_plugin

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value as JsonValue};

fn redis_params() -> JsonValue {
    json!({
        "driver": "redis",
        "host": "127.0.0.1",
        "port": 6379
    })
}

fn read_response(reader: &mut BufReader<std::process::ChildStdout>) -> Option<JsonValue> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let line = line.trim();
    if line.is_empty() {
        return read_response(reader);
    }
    serde_json::from_str(line).ok()
}

fn assert_has_result_or_error(resp: &JsonValue, method: &str) {
    let has_result = resp.get("result").is_some();
    let has_error = resp.get("error").is_some();
    assert!(
        has_result || has_error,
        "{method}: response must have 'result' or 'error': {resp}"
    );
}

fn json_number_to_u64(value: &JsonValue) -> Option<u64> {
    value.as_u64().or_else(|| {
        let float_value = value.as_f64()?;
        if !float_value.is_finite() || float_value.is_sign_negative() || float_value.fract() != 0.0
        {
            return None;
        }
        float_value.to_string().parse::<u64>().ok()
    })
}

fn json_number_to_f64(value: &JsonValue) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_u64()?.to_string().parse::<f64>().ok())
}

fn start_pubsub_subscriber(channel: &str) -> (mpsc::Sender<()>, thread::JoinHandle<u64>) {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let channel_name = channel.to_string();
    let handle = thread::spawn(move || {
        let client = redis::Client::open("redis://127.0.0.1:6379/").expect("redis client");
        let mut conn = client.get_connection().expect("redis connection");
        let _ = conn.set_read_timeout(Some(Duration::from_millis(200)));
        let mut pubsub = conn.as_pubsub();
        pubsub
            .subscribe(&channel_name)
            .expect("subscribe pubsub channel");
        let _ = ready_tx.send(());

        let mut received = 0u64;
        loop {
            if stop_rx.try_recv().is_ok() {
                break;
            }
            if pubsub.get_message().is_ok() {
                received += 1;
            }
        }
        let _ = pubsub.unsubscribe(&channel_name);
        received
    });

    ready_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("pubsub subscriber ready");
    (stop_tx, handle)
}

fn publish_test_message(channel: &str, payload: &str) {
    let client = redis::Client::open("redis://127.0.0.1:6379/").expect("redis client");
    let mut conn = client.get_connection().expect("redis connection");
    let _: i64 = redis::cmd("PUBLISH")
        .arg(channel)
        .arg(payload)
        .query(&mut conn)
        .expect("publish test message");
}

#[allow(clippy::too_many_lines)]
fn main() {
    let mut child = Command::new("cargo")
        .args(["run", "--bin", "tabularis-redis-plugin", "--quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Failed to spawn plugin");

    let mut stdin = child.stdin.take().expect("Failed to open stdin");
    let stdout = child.stdout.take().expect("Failed to open stdout");
    let mut reader = BufReader::new(stdout);

    let params = redis_params();
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let pubsub_channel = format!("tabularis:test:pubsub:{run_id}");
    let bulk_rows_count: u64 = 1_200;
    let bulk_name_prefix = format!("Bulk{run_id}_");
    let alice_name = format!("Alice_{run_id}");
    let bob_name = format!("Bob_{run_id}");
    let carol_name = format!("Carol_{run_id}");
    let stress_key = format!("stress:test:{run_id}");
    let bulk_rows: Vec<JsonValue> = (0..bulk_rows_count)
        .map(|i| {
            json!({
                "id": format!("bulk_{run_id}_{i}"),
                "name": format!("{bulk_name_prefix}{i}")
            })
        })
        .collect();

    let (pubsub_stop_tx, pubsub_handle) = start_pubsub_subscriber(&pubsub_channel);
    publish_test_message(&pubsub_channel, "warmup");

    let mut requests: Vec<(JsonValue, &str)> = vec![
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_databases",
                "params": {},
                "id": 1
            }),
            "get_databases",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "test_connection",
                "params": { "params": params },
                "id": 2
            }),
            "test_connection",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_tables",
                "params": { "params": params },
                "id": 3
            }),
            "get_tables",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_columns",
                "params": { "params": params, "table": "__redis_keys__" },
                "id": 4
            }),
            "get_columns __redis_keys__",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_pubsub_channels",
                "params": { "params": params },
                "id": 5
            }),
            "get_pubsub_channels",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_server_info",
                "params": { "params": params },
                "id": 6
            }),
            "get_server_info",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "insert_record",
                "params": {
                    "params": params,
                    "table": "test_data",
                    "data": { "id": "1", "name": alice_name }
                },
                "id": 7
            }),
            "insert_record",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "insert_record",
                "params": {
                    "params": params,
                    "table": "test_data",
                    "data": { "id": "2", "name": bob_name }
                },
                "id": 8
            }),
            "insert_record",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "insert_records_batch",
                "params": {
                    "params": params,
                    "table": "test_data",
                    "rows": [{ "id": "3", "name": carol_name }]
                },
                "id": 9
            }),
            "insert_records_batch",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_tables",
                "params": { "params": params },
                "id": 10
            }),
            "get_tables",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "SELECT * FROM test_data",
                    "page": 1,
                    "page_size": 10
                },
                "id": 11
            }),
            "execute_query SELECT *",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": format!("SELECT * FROM test_data WHERE name = '{alice_name}'"),
                    "page": 1,
                    "page_size": 10
                },
                "id": 111
            }),
            "execute_query WHERE",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "INSERT INTO test_data (id, name) VALUES ('dml1', 'DML')",
                    "page": 1,
                    "page_size": 10
                },
                "id": 112
            }),
            "execute_query INSERT",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "SELECT COUNT(*) FROM test_data",
                    "page": 1,
                    "page_size": 10
                },
                "id": 113
            }),
            "execute_query COUNT(*)",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "UPDATE test_data SET name = 'Updated' WHERE id = 'dml1'",
                    "page": 1,
                    "page_size": 10
                },
                "id": 114
            }),
            "execute_query UPDATE",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "DELETE FROM test_data WHERE id = 'dml1'",
                    "page": 1,
                    "page_size": 10
                },
                "id": 115
            }),
            "execute_query DELETE",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_columns",
                "params": { "params": params, "table": "test_data" },
                "id": 12
            }),
            "get_columns test_data",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "SELECT id AS pk, name AS n FROM test_data LIMIT 10",
                    "page": 1,
                    "page_size": 10
                },
                "id": 20
            }),
            "execute_query column alias",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": format!("SELECT * FROM test_data WHERE name LIKE '{bob_name}%'"),
                    "page": 1,
                    "page_size": 10
                },
                "id": 21
            }),
            "execute_query LIKE",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "SELECT * FROM test_data ORDER BY name ASC LIMIT 10",
                    "page": 1,
                    "page_size": 10
                },
                "id": 22
            }),
            "execute_query ORDER BY",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "SELECT * FROM test_data LIMIT 1",
                    "page": 1,
                    "page_size": 10
                },
                "id": 23
            }),
            "execute_query LIMIT",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "SELECT * FROM __redis_keys__ LIMIT 2",
                    "page": 1,
                    "page_size": 10
                },
                "id": 24
            }),
            "execute_query virtual __redis_keys__",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "CREATE INDEX idx_test_name ON test_data (name)",
                    "page": 1,
                    "page_size": 10
                },
                "id": 25
            }),
            "execute_query CREATE INDEX",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_indexes",
                "params": { "params": params, "table": "test_data" },
                "id": 26
            }),
            "get_indexes test_data",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_schema_snapshot",
                "params": { "params": params },
                "id": 27
            }),
            "get_schema_snapshot",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "get_all_columns_batch",
                "params": { "params": params, "tables": ["test_data", "__redis_keys__"] },
                "id": 271
            }),
            "get_all_columns_batch",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "SELECT * FROM nonexistent_table_xyz_123",
                    "page": 1,
                    "page_size": 10
                },
                "id": 28
            }),
            "execute_query nonexistent table",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "INSERT INTO test_agg (id, cat, amt) VALUES ('g1', 'X', 10), ('g2', 'X', 20), ('g3', 'Y', 5)",
                    "page": 1,
                    "page_size": 10
                },
                "id": 29
            }),
            "execute_query INSERT multi row",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "execute_query",
                "params": {
                    "params": params,
                    "query": "SELECT cat, COUNT(*), SUM(amt) FROM test_agg GROUP BY cat",
                    "page": 1,
                    "page_size": 10
                },
                "id": 30
            }),
            "execute_query GROUP BY",
        ),
        (
            json!({
                "jsonrpc": "2.0",
                "method": "delete_records_batch",
                "params": {
                    "params": params,
                    "table": "test_data",
                    "primary_keys": ["3"]
                },
                "id": 13
            }),
            "delete_records_batch",
        ),
    ];
    requests.push((
        json!({
            "jsonrpc": "2.0",
            "method": "insert_records_batch",
            "params": {
                "params": params,
                "table": "test_data",
                "rows": bulk_rows
            },
            "id": 431
        }),
        "insert_records_batch large volume",
    ));
    requests.push((
        json!({
            "jsonrpc": "2.0",
            "method": "execute_query",
            "params": {
                "params": params,
                "query": format!("SELECT COUNT(*) FROM test_data WHERE name LIKE '{bulk_name_prefix}%'"),
                "page": 1,
                "page_size": 25
            },
            "id": 432
        }),
        "execute_query COUNT(*) large volume",
    ));
    requests.push((
        json!({
            "jsonrpc": "2.0",
            "method": "execute_query",
            "params": {
                "params": params,
                "query": format!("SELECT * FROM test_data WHERE name LIKE '{bulk_name_prefix}%' ORDER BY name ASC"),
                "page": 2,
                "page_size": 400
            },
            "id": 433
        }),
        "execute_query pagination large volume",
    ));
    requests.push((
        json!({
            "jsonrpc": "2.0",
            "method": "insert_record",
            "params": {
                "params": params,
                "table": "__keys_stress__",
                "data": { "key": stress_key, "value": "before_update" }
            },
            "id": 434
        }),
        "insert_record virtual __keys_stress__",
    ));
    requests.push((
        json!({
            "jsonrpc": "2.0",
            "method": "update_record",
            "params": {
                "params": params,
                "table": "__keys_stress__",
                "pk_col": "key",
                "pk_val": stress_key,
                "col_name": "value",
                "new_val": "after_update"
            },
            "id": 435
        }),
        "update_record virtual __keys_stress__",
    ));
    requests.push((
        json!({
            "jsonrpc": "2.0",
            "method": "execute_query",
            "params": {
                "params": params,
                "query": format!("SELECT * FROM __redis_keys__ WHERE key = '{stress_key}'"),
                "page": 1,
                "page_size": 10
            },
            "id": 436
        }),
        "execute_query virtual __keys_stress__ WHERE",
    ));
    requests.push((
        json!({
            "jsonrpc": "2.0",
            "method": "delete_record",
            "params": {
                "params": params,
                "table": "__keys_stress__",
                "pk_col": "key",
                "pk_val": stress_key
            },
            "id": 437
        }),
        "delete_record virtual __keys_stress__",
    ));
    requests.push((
        json!({
            "jsonrpc": "2.0",
            "method": "execute_query",
            "params": {
                "params": params,
                "query": format!("SELECT * FROM __redis_keys__ WHERE key = '{stress_key}'"),
                "page": 1,
                "page_size": 10
            },
            "id": 438
        }),
        "execute_query virtual __keys_stress__ WHERE after delete",
    ));

    for (req, method) in requests {
        let req_str = serde_json::to_string(&req).unwrap() + "\n";
        stdin.write_all(req_str.as_bytes()).unwrap();
        stdin.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));

        let resp = read_response(&mut reader).unwrap_or_else(|| panic!("{method}: no response"));
        assert_has_result_or_error(&resp, method);

        if method == "execute_query nonexistent table" {
            assert!(
                resp.get("error").is_some(),
                "expected error for nonexistent table"
            );
            continue;
        }

        if let Some(result) = resp.get("result") {
            match method {
                "get_databases" => assert!(result.is_array(), "get_databases should return array"),
                "get_tables" => {
                    let arr = result.as_array().expect("get_tables should return array");
                    assert!(
                        !arr.iter()
                            .filter_map(|t| t.get("name").and_then(|v| v.as_str()))
                            .any(|x| x == "__keys_tabularis__"),
                        "internal metadata prefix should not be exposed as virtual table"
                    );
                }
                "test_connection" => {
                    let ok = result
                        .get("success")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    assert!(ok, "test_connection should return success: true");
                }
                "get_columns __redis_keys__" => {
                    let arr = result.as_array().expect("get_columns returns array");
                    let names: Vec<&str> = arr
                        .iter()
                        .filter_map(|c| c.get("name").and_then(|v| v.as_str()))
                        .collect();
                    assert!(names.contains(&"key"));
                    assert!(names.contains(&"key_raw"));
                }
                "get_pubsub_channels" => {
                    let channels = result
                        .get("channels")
                        .and_then(|v| v.as_array())
                        .expect("get_pubsub_channels should return channels array");
                    let matched = channels.iter().find(|entry| {
                        entry
                            .get("name")
                            .and_then(|v| v.as_str())
                            .is_some_and(|name| name == pubsub_channel)
                    });
                    let channel = matched.expect("pubsub channel should be listed");
                    let subscribers = channel
                        .get("subscribers")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    assert!(
                        subscribers >= 1,
                        "pubsub channel should have at least one subscriber"
                    );
                }
                "get_server_info" => {
                    let obj = result.as_object().expect("get_server_info returns object");
                    assert!(!obj.is_empty(), "get_server_info should have keys");
                }
                "insert_records_batch" => {
                    assert!(
                        result.is_number(),
                        "insert_records_batch should return affected count"
                    );
                }
                "delete_records_batch" => {
                    assert!(
                        result.is_number(),
                        "delete_records_batch should return deleted count"
                    );
                }
                "execute_query WHERE" => {
                    let rows_arr = result
                        .get("rows")
                        .and_then(|v| v.as_array())
                        .expect("rows array");
                    assert_eq!(
                        rows_arr.len(),
                        1,
                        "WHERE name should return one unique seeded row"
                    );
                    let first_row = rows_arr[0].as_array().expect("row is array");
                    let cols = result
                        .get("columns")
                        .and_then(|c| c.as_array())
                        .expect("columns");
                    let name_idx = cols
                        .iter()
                        .position(|x| x.as_str() == Some("name"))
                        .unwrap_or(1);
                    assert_eq!(
                        first_row.get(name_idx).and_then(|v| v.as_str()),
                        Some(alice_name.as_str())
                    );
                }
                "execute_query INSERT" => {
                    let affected = result
                        .get("affected_rows")
                        .and_then(serde_json::Value::as_u64)
                        .expect("affected_rows");
                    assert_eq!(affected, 1, "INSERT should affect 1 row");
                }
                "execute_query COUNT(*)" => {
                    let rows_arr = result
                        .get("rows")
                        .and_then(|v| v.as_array())
                        .expect("rows array");
                    assert_eq!(rows_arr.len(), 1, "COUNT(*) should return one row");
                    let first_row = rows_arr[0].as_array().expect("row is array");
                    assert_eq!(first_row.len(), 1, "COUNT(*) row has one column");
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let n = first_row[0]
                        .as_u64()
                        .or_else(|| first_row[0].as_f64().map(|f| f as u64))
                        .expect("count is number");
                    assert!(n >= 3, "COUNT(*) should be at least 3");
                }
                "execute_query UPDATE" => {
                    let affected = result
                        .get("affected_rows")
                        .and_then(serde_json::Value::as_u64)
                        .expect("affected_rows");
                    assert!(affected >= 1, "UPDATE should affect at least 1 row");
                }
                "execute_query DELETE" => {
                    let affected = result
                        .get("affected_rows")
                        .and_then(serde_json::Value::as_u64)
                        .expect("affected_rows");
                    assert_eq!(affected, 1, "DELETE should affect 1 row");
                }
                "execute_query column alias" => {
                    let cols = result
                        .get("columns")
                        .and_then(|c| c.as_array())
                        .expect("columns");
                    let names: Vec<&str> = cols.iter().filter_map(|c| c.as_str()).collect();
                    assert!(names.contains(&"pk"), "expected column alias pk");
                    assert!(names.contains(&"n"), "expected column alias n");
                }
                "execute_query LIKE" => {
                    let rows_arr = result.get("rows").and_then(|v| v.as_array()).expect("rows");
                    assert_eq!(
                        rows_arr.len(),
                        1,
                        "LIKE should return the one uniquely-seeded Bob row"
                    );
                    let cols = result
                        .get("columns")
                        .and_then(|c| c.as_array())
                        .expect("columns");
                    let name_idx = cols
                        .iter()
                        .position(|x| x.as_str() == Some("name"))
                        .unwrap_or(1);
                    let first_row = rows_arr[0].as_array().expect("row is array");
                    assert_eq!(
                        first_row.get(name_idx).and_then(|v| v.as_str()),
                        Some(bob_name.as_str())
                    );
                }
                "execute_query ORDER BY" => {
                    let rows_arr = result.get("rows").and_then(|v| v.as_array()).expect("rows");
                    let cols = result
                        .get("columns")
                        .and_then(|c| c.as_array())
                        .expect("columns");
                    let name_idx = cols
                        .iter()
                        .position(|x| x.as_str() == Some("name"))
                        .unwrap_or(1);
                    let names: Vec<&str> = rows_arr
                        .iter()
                        .filter_map(|r| {
                            r.as_array()
                                .and_then(|arr| arr.get(name_idx).and_then(|v| v.as_str()))
                        })
                        .collect();
                    let mut sorted = names.clone();
                    sorted.sort_unstable();
                    assert_eq!(
                        names, sorted,
                        "ORDER BY name ASC should return rows in ascending order"
                    );
                }
                "execute_query LIMIT" => {
                    let rows_arr = result.get("rows").and_then(|v| v.as_array()).expect("rows");
                    assert!(
                        !rows_arr.is_empty(),
                        "SELECT LIMIT 1 should return at least one row"
                    );
                }
                "execute_query virtual __redis_keys__" => {
                    let cols = result
                        .get("columns")
                        .and_then(|c| c.as_array())
                        .expect("columns");
                    assert!(
                        cols.iter().filter_map(|c| c.as_str()).any(|x| x == "key"),
                        "virtual table should have key column"
                    );
                    let rows_arr = result.get("rows").and_then(|v| v.as_array()).expect("rows");
                    assert!(
                        rows_arr.len() <= 10,
                        "__redis_keys__ query should respect page_size upper bound"
                    );
                }
                "execute_query CREATE INDEX" => {
                    assert!(
                        result.is_object(),
                        "CREATE INDEX should return result object"
                    );
                }
                "get_indexes test_data" => {
                    let arr = result.as_array().expect("get_indexes returns array");
                    let index_names: Vec<&str> = arr
                        .iter()
                        .filter_map(|o| o.get("index_name").and_then(|v| v.as_str()))
                        .collect();
                    assert!(
                        index_names.contains(&"idx_test_name"),
                        "get_indexes should include idx_test_name, got {index_names:?}"
                    );
                }
                "get_schema_snapshot" => {
                    let tables = result
                        .get("tables")
                        .and_then(|t| t.as_array())
                        .expect("tables");
                    let names: Vec<&str> = tables
                        .iter()
                        .filter_map(|t| t.get("name").and_then(|v| v.as_str()))
                        .collect();
                    assert!(
                        names.contains(&"test_data"),
                        "schema_snapshot should include test_data, got {names:?}"
                    );
                }
                "get_all_columns_batch" => {
                    let obj = result
                        .as_object()
                        .expect("get_all_columns_batch returns object");
                    assert!(
                        obj.contains_key("test_data"),
                        "get_all_columns_batch should have test_data"
                    );
                    assert!(
                        obj.contains_key("__redis_keys__"),
                        "get_all_columns_batch should have __redis_keys__"
                    );
                    let test_data_cols = obj
                        .get("test_data")
                        .and_then(|v| v.as_array())
                        .expect("columns array");
                    assert!(!test_data_cols.is_empty());
                }
                "execute_query INSERT multi row" => {
                    let affected = result
                        .get("affected_rows")
                        .and_then(serde_json::Value::as_u64)
                        .expect("affected_rows");
                    assert!(
                        affected >= 1,
                        "multi-row INSERT should affect at least 1 row"
                    );
                }
                "execute_query GROUP BY" => {
                    let rows_arr = result.get("rows").and_then(|v| v.as_array()).expect("rows");
                    let cols = result
                        .get("columns")
                        .and_then(|c| c.as_array())
                        .expect("columns");
                    assert_eq!(
                        rows_arr.len(),
                        2,
                        "GROUP BY cat should return 2 groups (X, Y)"
                    );
                    let cat_idx = cols
                        .iter()
                        .position(|x| x.as_str() == Some("cat"))
                        .unwrap_or(0);
                    let sum_idx = cols
                        .iter()
                        .position(|x| {
                            let s = x.as_str().unwrap_or("");
                            s.starts_with("SUM") || s == "sum_amt"
                        })
                        .unwrap_or(2);
                    let count_idx = cols
                        .iter()
                        .position(|x| {
                            let s = x.as_str().unwrap_or("");
                            s.starts_with("COUNT") || s == "cnt"
                        })
                        .unwrap_or(1);
                    let mut by_cat: Vec<(String, u64, f64)> = rows_arr
                        .iter()
                        .filter_map(|r| {
                            let arr = r.as_array()?;
                            let cat = arr.get(cat_idx)?.as_str()?.to_string();
                            let cnt = json_number_to_u64(arr.get(count_idx)?)?;
                            let sum = json_number_to_f64(arr.get(sum_idx)?)?;
                            Some((cat, cnt, sum))
                        })
                        .collect();
                    by_cat.sort_by(|a, b| a.0.cmp(&b.0));
                    assert_eq!(by_cat.len(), 2);
                    let (x_row, y_row) = if by_cat[0].0 == "X" {
                        (&by_cat[0], &by_cat[1])
                    } else {
                        (&by_cat[1], &by_cat[0])
                    };
                    assert_eq!(x_row.0, "X");
                    assert_eq!(x_row.1, 2);
                    assert!((x_row.2 - 30.0).abs() < 1e-9);
                    assert_eq!(y_row.0, "Y");
                    assert_eq!(y_row.1, 1);
                    assert!((y_row.2 - 5.0).abs() < 1e-9);
                }
                "insert_records_batch large volume" => {
                    let affected = result.as_u64().expect("insert_records_batch returns u64");
                    assert_eq!(
                        affected, bulk_rows_count,
                        "bulk insert should affect all requested rows"
                    );
                }
                "execute_query COUNT(*) large volume" => {
                    let rows_arr = result
                        .get("rows")
                        .and_then(|v| v.as_array())
                        .expect("rows array");
                    assert_eq!(rows_arr.len(), 1, "COUNT(*) should return one row");
                    let first_row = rows_arr[0].as_array().expect("row is array");
                    let n = json_number_to_u64(&first_row[0]).expect("count is number");
                    assert_eq!(n, bulk_rows_count, "COUNT(*) should match bulk insert size");
                }
                "execute_query pagination large volume" => {
                    let rows_arr = result.get("rows").and_then(|v| v.as_array()).expect("rows");
                    assert_eq!(rows_arr.len(), 400, "page 2 should return full 400 rows");
                    let pagination = result
                        .get("pagination")
                        .and_then(|v| v.as_object())
                        .expect("pagination object");
                    let total_rows = pagination
                        .get("total_rows")
                        .and_then(json_number_to_u64)
                        .expect("total_rows");
                    assert_eq!(
                        total_rows, bulk_rows_count,
                        "pagination total_rows should match filtered dataset"
                    );
                    let has_more = pagination
                        .get("has_more")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    assert!(has_more, "page 2 of 1200 rows should have more pages");
                }
                "insert_record virtual __keys_stress__" => {
                    let affected = result.as_u64().expect("insert_record returns u64");
                    assert_eq!(affected, 1, "virtual key insert should affect one row");
                }
                "update_record virtual __keys_stress__" => {
                    let affected = result.as_u64().expect("update_record returns u64");
                    assert_eq!(affected, 1, "virtual key update should affect one row");
                }
                "execute_query virtual __keys_stress__ WHERE" => {
                    let rows_arr = result.get("rows").and_then(|v| v.as_array()).expect("rows");
                    assert_eq!(rows_arr.len(), 1, "should find updated virtual key row");
                    let cols = result
                        .get("columns")
                        .and_then(|c| c.as_array())
                        .expect("columns");
                    let key_idx = cols
                        .iter()
                        .position(|x| x.as_str() == Some("key"))
                        .unwrap_or(0);
                    let value_idx = cols
                        .iter()
                        .position(|x| x.as_str() == Some("value"))
                        .unwrap_or(2);
                    let first_row = rows_arr[0].as_array().expect("row is array");
                    assert_eq!(
                        first_row.get(key_idx).and_then(|v| v.as_str()),
                        Some(stress_key.as_str())
                    );
                    assert_eq!(
                        first_row.get(value_idx).and_then(|v| v.as_str()),
                        Some("after_update")
                    );
                }
                "delete_record virtual __keys_stress__" => {
                    let affected = result.as_u64().expect("delete_record returns u64");
                    assert_eq!(affected, 1, "virtual key delete should affect one row");
                }
                "execute_query virtual __keys_stress__ WHERE after delete" => {
                    let rows_arr = result.get("rows").and_then(|v| v.as_array()).expect("rows");
                    assert!(
                        rows_arr.is_empty(),
                        "deleted virtual key should no longer exist"
                    );
                }
                _ => {}
            }
        }
    }

    publish_test_message(&pubsub_channel, "shutdown");
    let _ = pubsub_stop_tx.send(());
    let received_messages = pubsub_handle.join().expect("pubsub thread join");
    assert!(
        received_messages >= 1,
        "pubsub subscriber should receive at least one message"
    );

    drop(stdin);
    let status = child.wait().unwrap();
    assert!(status.success(), "plugin process should exit successfully");
    println!("All integration checks passed.");
}
