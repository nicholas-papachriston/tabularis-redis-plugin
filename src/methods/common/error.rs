use serde_json::Value as JsonValue;

#[derive(Debug)]
pub enum AppError {
    InvalidParams(String),
    MethodNotFound(String),
    NotFound(String),
    Unsupported(String),
    Conflict(String),
    Backend(String),
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

    pub fn to_rpc_error(&self, id: &JsonValue) -> JsonValue {
        serde_json::json!({
            "jsonrpc": "2.0",
            "error": { "code": self.json_rpc_code(), "message": self.message() },
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
