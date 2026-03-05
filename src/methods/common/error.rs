use serde_json::Value as JsonValue;

#[derive(Debug)]
pub enum AppError {
    InvalidParams(String),
    MethodNotFound(String),
    NotFound(String),
    Unsupported(String),
    Conflict(String),
    Backend(String),
    #[allow(dead_code)]
    Internal(String),
}

impl AppError {
    pub fn message(&self) -> &str {
        match self {
            Self::InvalidParams(m)
            | Self::MethodNotFound(m)
            | Self::NotFound(m)
            | Self::Unsupported(m)
            | Self::Conflict(m)
            | Self::Backend(m)
            | Self::Internal(m) => m,
        }
    }

    pub const fn json_rpc_code(&self) -> i32 {
        match self {
            Self::InvalidParams(_) => -32602,
            Self::MethodNotFound(_) => -32601,
            Self::NotFound(_) => -32004,
            Self::Unsupported(_) => -32001,
            Self::Conflict(_) => -32009,
            Self::Backend(_) => -32050,
            Self::Internal(_) => -32603,
        }
    }

    pub const fn error_kind(&self) -> &'static str {
        match self {
            Self::InvalidParams(_) => "INVALID_PARAMS",
            Self::MethodNotFound(_) => "METHOD_NOT_FOUND",
            Self::NotFound(_) => "NOT_FOUND",
            Self::Unsupported(_) => "UNSUPPORTED",
            Self::Conflict(_) => "CONFLICT",
            Self::Backend(_) => "BACKEND_ERROR",
            Self::Internal(_) => "INTERNAL_ERROR",
        }
    }

    pub fn to_rpc_error(&self, id: &JsonValue) -> JsonValue {
        serde_json::json!({
            "jsonrpc": "2.0",
            "error": {
                "code": self.json_rpc_code(),
                "message": self.message(),
                "data": { "kind": self.error_kind() }
            },
            "id": id
        })
    }
}

impl From<redis::RedisError> for AppError {
    fn from(e: redis::RedisError) -> Self {
        Self::Backend(e.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        Self::InvalidParams(e.to_string())
    }
}

impl From<String> for AppError {
    fn from(s: String) -> Self {
        Self::Backend(s)
    }
}

impl From<&str> for AppError {
    fn from(s: &str) -> Self {
        Self::Backend(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_kind_codes() {
        assert_eq!(
            AppError::InvalidParams("x".into()).error_kind(),
            "INVALID_PARAMS"
        );
        assert_eq!(AppError::NotFound("x".into()).error_kind(), "NOT_FOUND");
        assert_eq!(AppError::Backend("x".into()).error_kind(), "BACKEND_ERROR");
    }

    #[test]
    fn json_rpc_code() {
        assert_eq!(AppError::InvalidParams("x".into()).json_rpc_code(), -32602);
        assert_eq!(AppError::MethodNotFound("x".into()).json_rpc_code(), -32601);
    }

    #[test]
    fn to_rpc_error_includes_data_kind() {
        let e = AppError::InvalidParams("bad".into());
        let out = e.to_rpc_error(&JsonValue::Number(1i64.into()));
        let err = out.get("error").and_then(|v| v.get("data"));
        assert_eq!(
            err.and_then(|d| d.get("kind")).and_then(|k| k.as_str()),
            Some("INVALID_PARAMS")
        );
    }
}
