use crate::methods::common::error::AppError;
use serde_json::Value as JsonValue;

pub(super) const DEFAULT_PAGE: u64 = 1;
pub(super) const DEFAULT_PAGE_SIZE: u64 = 100;

#[derive(Debug, Clone)]
pub struct PageRequest {
    pub page: u64,
    pub page_size: u64,
    pub limit: Option<u64>,
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            page: DEFAULT_PAGE,
            page_size: DEFAULT_PAGE_SIZE,
            limit: None,
        }
    }
}

pub fn required_str(params: &JsonValue, key: &str) -> Result<String, AppError> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| AppError::InvalidParams(format!("Missing or invalid parameter: {key}")))
}

pub fn optional_str(params: &JsonValue, key: &str) -> Option<String> {
    params.get(key).and_then(|v| v.as_str()).map(String::from)
}

pub fn optional_array_of_str(params: &JsonValue, key: &str) -> Vec<String> {
    params
        .get(key)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

pub fn optional_object<'a>(
    params: &'a JsonValue,
    key: &str,
) -> Option<&'a serde_json::Map<String, JsonValue>> {
    params.get(key).and_then(|v| v.as_object())
}

pub fn page_request_from_params(params: &JsonValue) -> PageRequest {
    let page = params
        .get("page")
        .and_then(JsonValue::as_u64)
        .unwrap_or(DEFAULT_PAGE);
    let page_size = params.get("page_size").and_then(JsonValue::as_u64);
    let limit = params.get("limit").and_then(JsonValue::as_u64);
    let page_size = page_size.or(limit).unwrap_or(DEFAULT_PAGE_SIZE);
    PageRequest {
        page,
        page_size,
        limit,
    }
}

pub fn optional_json_path(params: &JsonValue) -> Option<&str> {
    params.get("json_path").and_then(|v| v.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::methods::common::AppError;

    #[test]
    fn test_required_str_present() {
        let params = serde_json::json!({ "table": "t1" });
        assert_eq!(super::required_str(&params, "table").unwrap(), "t1");
    }

    #[test]
    fn test_required_str_missing() {
        let params = serde_json::json!({});
        let r = super::required_str(&params, "table");
        assert!(matches!(r, Err(AppError::InvalidParams(_))));
    }

    #[test]
    fn test_optional_str() {
        let params = serde_json::json!({ "a": "x", "b": 1 });
        assert_eq!(super::optional_str(&params, "a"), Some("x".to_string()));
        assert_eq!(super::optional_str(&params, "b"), None);
        assert_eq!(super::optional_str(&params, "c"), None);
    }

    #[test]
    fn test_page_request_from_params_defaults() {
        let params = serde_json::json!({});
        let p = super::page_request_from_params(&params);
        assert_eq!(p.page, DEFAULT_PAGE);
        assert_eq!(p.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(p.limit, None);
    }

    #[test]
    fn test_page_request_from_params_uses_limit_as_page_size() {
        let params = serde_json::json!({ "page": 2, "limit": 50 });
        let p = super::page_request_from_params(&params);
        assert_eq!(p.page, 2);
        assert_eq!(p.page_size, 50);
        assert_eq!(p.limit, Some(50));
    }
}
