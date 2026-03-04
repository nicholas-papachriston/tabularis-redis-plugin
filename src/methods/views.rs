use crate::methods::common::AppError;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

pub const fn get_views(_client: &mut RedisClient, _schema: Option<&str>) -> JsonValue {
    serde_json::json!([])
}

pub fn get_view_definition(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    _view: &str,
) -> Result<JsonValue, AppError> {
    Err(AppError::Unsupported("Views are not supported".into()))
}

pub const fn get_view_columns(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    _view: &str,
) -> JsonValue {
    serde_json::json!([])
}

pub fn create_view(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    _name: &str,
    _definition: &str,
) -> Result<JsonValue, AppError> {
    Err(AppError::Unsupported("Views are not supported".into()))
}

pub fn alter_view(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    _name: &str,
    _definition: &str,
) -> Result<JsonValue, AppError> {
    Err(AppError::Unsupported("Views are not supported".into()))
}

pub fn drop_view(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    _name: &str,
) -> Result<JsonValue, AppError> {
    Err(AppError::Unsupported("Views are not supported".into()))
}
