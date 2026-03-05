mod batch;
mod common;
mod crud;

pub use common::AppError;
mod ddl;
mod discovery;
mod pubsub;
mod query;
mod routines;
mod server;
mod views;

use crate::methods::common::{
    optional_array_of_str, optional_json_path, optional_object, optional_str,
    page_request_from_params, required_str,
};
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

fn success_response(id: &JsonValue, result: &JsonValue) -> JsonValue {
    serde_json::json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id
    })
}

/// Return `get_databases` result when no connection is available (e.g. before user has connected).
/// Tabularis needs a database list to show in the connection form; we return Redis DB indices 0-15.
/// Echoes the request `id` as in the normal dispatch path.
pub fn dispatch_get_databases_no_connection(id: &JsonValue) -> JsonValue {
    let dbs: Vec<String> = (0..16).map(|i| i.to_string()).collect();
    let result = serde_json::json!(dbs);
    success_response(id, &result)
}

fn dispatch_discovery(
    method: &str,
    params: &JsonValue,
    id: &JsonValue,
    client: &mut RedisClient,
    schema_ref: Option<&str>,
) -> Result<JsonValue, AppError> {
    let r = match method {
        "test_connection" => {
            discovery::test_connection(client).map(|r| success_response(id, &r))?
        }
        "get_databases" => discovery::get_databases(client).map(|r| success_response(id, &r))?,
        "get_schemas" => success_response(id, &discovery::get_schemas(client)),
        "get_tables" => {
            discovery::get_tables(client, schema_ref).map(|r| success_response(id, &r))?
        }
        "get_columns" => {
            let table = required_str(params, "table")?;
            discovery::get_columns(client, schema_ref, &table).map(|r| success_response(id, &r))?
        }
        "get_foreign_keys" => {
            let table = optional_str(params, "table").unwrap_or_default();
            success_response(id, &discovery::get_foreign_keys(client, schema_ref, &table))
        }
        "get_indexes" => {
            let table = optional_str(params, "table").unwrap_or_default();
            discovery::get_indexes(client, schema_ref, &table).map(|r| success_response(id, &r))?
        }
        "get_pubsub_channels" => {
            pubsub::get_pubsub_channels(client).map(|r| success_response(id, &r))?
        }
        "get_server_info" => server::get_server_info(client).map(|r| success_response(id, &r))?,
        _ => {
            return Err(AppError::MethodNotFound(format!(
                "Method '{method}' not implemented"
            )))
        }
    };
    Ok(r)
}

fn dispatch_query(
    params: &JsonValue,
    id: &JsonValue,
    client: &mut RedisClient,
) -> Result<JsonValue, AppError> {
    let query = required_str(params, "query")?;
    let page_req = page_request_from_params(params);
    let json_path = optional_json_path(params);
    query::execute_query(
        client,
        &query,
        page_req.page,
        page_req.page_size,
        page_req.limit,
        json_path,
    )
    .map(|r| success_response(id, &r))
}

fn dispatch_crud(
    method: &str,
    params: &JsonValue,
    id: &JsonValue,
    client: &mut RedisClient,
    schema_ref: Option<&str>,
) -> Result<JsonValue, AppError> {
    let r = match method {
        "insert_record" => {
            let table = required_str(params, "table")?;
            let empty = serde_json::Map::new();
            let data = optional_object(params, "data").unwrap_or(&empty);
            crud::insert_record(client, schema_ref, &table, data)
                .map(|n| success_response(id, &serde_json::json!(n)))?
        }
        "insert_records_batch" => {
            let table = required_str(params, "table")?;
            let arr = params
                .get("rows")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| {
                    AppError::InvalidParams("rows is required and must be an array".into())
                })?;
            let rows: Vec<serde_json::Map<String, JsonValue>> =
                arr.iter().filter_map(|v| v.as_object().cloned()).collect();
            if rows.len() != arr.len() {
                return Err(AppError::InvalidParams(
                    "each element of rows must be an object".into(),
                ));
            }
            crud::insert_records_batch(client, schema_ref, &table, &rows)
                .map(|n| success_response(id, &serde_json::json!(n)))?
        }
        "delete_records_batch" => {
            let table = required_str(params, "table")?;
            let primary_keys = optional_array_of_str(params, "primary_keys");
            crud::delete_records_batch(client, schema_ref, &table, &primary_keys)
                .map(|n| success_response(id, &serde_json::json!(n)))?
        }
        "update_record" => {
            let table = required_str(params, "table")?;
            let pk_col = optional_str(params, "pk_col")
                .or_else(|| optional_str(params, "primary_key_column"))
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AppError::InvalidParams("pk_col or primary_key_column is required".into())
                })?;
            let pk_val = params
                .get("pk_val")
                .or_else(|| params.get("primary_key_value"))
                .cloned()
                .unwrap_or(JsonValue::Null);
            let column = optional_str(params, "col_name")
                .or_else(|| optional_str(params, "column"))
                .filter(|s| !s.is_empty())
                .ok_or_else(|| AppError::InvalidParams("col_name or column is required".into()))?;
            let value = params
                .get("new_val")
                .or_else(|| params.get("value"))
                .cloned()
                .unwrap_or(JsonValue::Null);
            crud::update_record(
                client, schema_ref, &table, &pk_col, &pk_val, &column, &value,
            )
            .map(|n| success_response(id, &serde_json::json!(n)))?
        }
        "delete_record" => {
            let table = required_str(params, "table")?;
            let pk_col = optional_str(params, "pk_col")
                .or_else(|| optional_str(params, "primary_key_column"))
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    AppError::InvalidParams("pk_col or primary_key_column is required".into())
                })?;
            let pk_val = params
                .get("pk_val")
                .or_else(|| params.get("primary_key_value"))
                .cloned()
                .unwrap_or(JsonValue::Null);
            crud::delete_record(client, schema_ref, &table, &pk_col, &pk_val)
                .map(|n| success_response(id, &serde_json::json!(n)))?
        }
        _ => {
            return Err(AppError::MethodNotFound(format!(
                "Method '{method}' not implemented"
            )))
        }
    };
    Ok(r)
}

fn dispatch_batch(
    method: &str,
    params: &JsonValue,
    id: &JsonValue,
    client: &mut RedisClient,
    schema_ref: Option<&str>,
) -> Result<JsonValue, AppError> {
    let r = match method {
        "get_schema_snapshot" => {
            batch::get_schema_snapshot(client, schema_ref).map(|r| success_response(id, &r))?
        }
        "get_all_columns_batch" => {
            let tables = optional_array_of_str(params, "tables");
            batch::get_all_columns_batch(client, schema_ref, &tables)
                .map(|r| success_response(id, &r))?
        }
        "get_all_foreign_keys_batch" => {
            let tables = optional_array_of_str(params, "tables");
            let r = batch::get_all_foreign_keys_batch(client, schema_ref, &tables);
            success_response(id, &r)
        }
        _ => {
            return Err(AppError::MethodNotFound(format!(
                "Method '{method}' not implemented"
            )))
        }
    };
    Ok(r)
}

fn dispatch_views(
    method: &str,
    params: &JsonValue,
    id: &JsonValue,
    client: &mut RedisClient,
    schema_ref: Option<&str>,
) -> Result<JsonValue, AppError> {
    let r = match method {
        "get_views" => success_response(id, &views::get_views(client, schema_ref)),
        "get_view_definition" => {
            let view = optional_str(params, "view").unwrap_or_default();
            views::get_view_definition(client, schema_ref, &view)
                .map(|r| success_response(id, &r))?
        }
        "get_view_columns" => {
            let view = optional_str(params, "view").unwrap_or_default();
            success_response(id, &views::get_view_columns(client, schema_ref, &view))
        }
        "create_view" => {
            let name = optional_str(params, "name").unwrap_or_default();
            let definition = optional_str(params, "definition").unwrap_or_default();
            views::create_view(client, schema_ref, &name, &definition)
                .map(|r| success_response(id, &r))?
        }
        "alter_view" => {
            let name = optional_str(params, "name").unwrap_or_default();
            let definition = optional_str(params, "definition").unwrap_or_default();
            views::alter_view(client, schema_ref, &name, &definition)
                .map(|r| success_response(id, &r))?
        }
        "drop_view" => {
            let name = optional_str(params, "name").unwrap_or_default();
            views::drop_view(client, schema_ref, &name).map(|r| success_response(id, &r))?
        }
        _ => {
            return Err(AppError::MethodNotFound(format!(
                "Method '{method}' not implemented"
            )))
        }
    };
    Ok(r)
}

fn dispatch_routines(
    method: &str,
    params: &JsonValue,
    id: &JsonValue,
    client: &mut RedisClient,
    schema_ref: Option<&str>,
) -> Result<JsonValue, AppError> {
    let r = match method {
        "get_routines" => success_response(id, &routines::get_routines(client, schema_ref)),
        "get_routine_parameters" => {
            let routine = optional_str(params, "routine").unwrap_or_default();
            success_response(
                id,
                &routines::get_routine_parameters(client, schema_ref, &routine),
            )
        }
        "get_routine_definition" => {
            let routine = optional_str(params, "routine").unwrap_or_default();
            routines::get_routine_definition(client, schema_ref, &routine)
                .map(|r| success_response(id, &r))?
        }
        _ => {
            return Err(AppError::MethodNotFound(format!(
                "Method '{method}' not implemented"
            )))
        }
    };
    Ok(r)
}

fn dispatch_ddl(
    method: &str,
    params: &JsonValue,
    id: &JsonValue,
    client: &mut RedisClient,
    schema_ref: Option<&str>,
) -> Result<JsonValue, AppError> {
    let r = match method {
        "get_create_table_sql" => {
            let table = required_str(params, "table")?;
            ddl::get_create_table_sql(client, schema_ref, &table)
                .map(|r| success_response(id, &r))?
        }
        "get_add_column_sql" => {
            let table = required_str(params, "table")?;
            let column = params.get("column").cloned().unwrap_or(JsonValue::Null);
            ddl::get_add_column_sql(client, schema_ref, &table, &column)
                .map(|r| success_response(id, &r))?
        }
        "get_alter_column_sql" => {
            let table = required_str(params, "table")?;
            let column = params.get("column").cloned().unwrap_or(JsonValue::Null);
            ddl::get_alter_column_sql(client, schema_ref, &table, &column)
                .map(|r| success_response(id, &r))?
        }
        "get_create_index_sql" => {
            let table = required_str(params, "table")?;
            let index = params.get("index").cloned().unwrap_or(JsonValue::Null);
            ddl::get_create_index_sql(client, schema_ref, &table, &index)
                .map(|r| success_response(id, &r))?
        }
        "get_create_foreign_key_sql" => {
            let table = required_str(params, "table")?;
            let fk = params.get("fk").cloned().unwrap_or(JsonValue::Null);
            ddl::get_create_foreign_key_sql(client, schema_ref, &table, &fk)
                .map(|r: JsonValue| success_response(id, &r))?
        }
        "drop_index" => {
            let table = required_str(params, "table")?;
            let index_name = required_str(params, "index_name")?;
            ddl::drop_index(client, schema_ref, &table, &index_name)
                .map(|r| success_response(id, &r))?
        }
        "drop_foreign_key" => {
            let table = required_str(params, "table")?;
            let constraint_name = required_str(params, "constraint_name")?;
            ddl::drop_foreign_key(client, schema_ref, &table, &constraint_name)
                .map(|r| success_response(id, &r))?
        }
        "drop_table" => {
            let table = required_str(params, "table")?;
            ddl::drop_table(client, schema_ref, &table).map(|r| success_response(id, &r))?
        }
        _ => {
            return Err(AppError::MethodNotFound(format!(
                "Method '{method}' not implemented"
            )))
        }
    };
    Ok(r)
}

pub fn dispatch(
    method: &str,
    params: &JsonValue,
    id: &JsonValue,
    client: &mut RedisClient,
) -> JsonValue {
    log::debug!("Dispatching method '{method}'");
    let schema = optional_str(params, "schema");
    let schema_ref = schema.as_deref();

    let result = match method {
        "test_connection"
        | "get_databases"
        | "get_schemas"
        | "get_tables"
        | "get_columns"
        | "get_foreign_keys"
        | "get_indexes"
        | "get_pubsub_channels"
        | "get_server_info" => dispatch_discovery(method, params, id, client, schema_ref),
        "execute_query" => dispatch_query(params, id, client),
        "insert_record"
        | "insert_records_batch"
        | "delete_records_batch"
        | "update_record"
        | "delete_record" => dispatch_crud(method, params, id, client, schema_ref),
        "get_schema_snapshot" | "get_all_columns_batch" | "get_all_foreign_keys_batch" => {
            dispatch_batch(method, params, id, client, schema_ref)
        }
        "get_views"
        | "get_view_definition"
        | "get_view_columns"
        | "create_view"
        | "alter_view"
        | "drop_view" => dispatch_views(method, params, id, client, schema_ref),
        "get_routines" | "get_routine_parameters" | "get_routine_definition" => {
            dispatch_routines(method, params, id, client, schema_ref)
        }
        "get_create_table_sql"
        | "get_add_column_sql"
        | "get_alter_column_sql"
        | "get_create_index_sql"
        | "get_create_foreign_key_sql"
        | "drop_index"
        | "drop_foreign_key"
        | "drop_table" => dispatch_ddl(method, params, id, client, schema_ref),
        _ => {
            log::warn!("Method '{method}' not implemented");
            Err(AppError::MethodNotFound(format!(
                "Method '{method}' not implemented"
            )))
        }
    };

    match result {
        Ok(resp) => resp,
        Err(err) => {
            log::warn!("Method '{method}' failed: {}", err.message());
            err.to_rpc_error(id)
        }
    }
}
