use crate::methods::common::AppError;
use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

pub const fn get_routines(_client: &mut RedisClient, _schema: Option<&str>) -> JsonValue {
    serde_json::json!([])
}

pub const fn get_routine_parameters(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    _routine: &str,
) -> JsonValue {
    serde_json::json!([])
}

pub fn get_routine_definition(
    _client: &mut RedisClient,
    _schema: Option<&str>,
    _routine: &str,
) -> Result<JsonValue, AppError> {
    Err(AppError::Unsupported("Routines are not supported".into()))
}
