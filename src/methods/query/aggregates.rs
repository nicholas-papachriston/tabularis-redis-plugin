//! Aggregate computation (COUNT, SUM, AVG, MIN, MAX) and GROUP BY.

#![allow(dead_code)]

use crate::methods::query::sql_parser::{AggFunc, AggregateExpr};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

fn json_to_f64(v: &JsonValue) -> Option<f64> {
    match v {
        JsonValue::Number(n) => n.as_f64(),
        JsonValue::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn json_to_cmp_str(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => String::new(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

/// Compute aggregates over rows, optionally grouped by columns.
/// Returns (`column_headers`, `result_rows`). Headers = `group_by` columns + aggregate aliases.
pub fn compute_aggregates(
    column_names: &[String],
    rows: &[Vec<JsonValue>],
    group_by: &[String],
    aggregates: &[AggregateExpr],
) -> (Vec<String>, Vec<Vec<JsonValue>>) {
    let group_indices: Vec<usize> = group_by
        .iter()
        .filter_map(|g| column_names.iter().position(|c| c == g))
        .collect();
    let agg_col_indices: Vec<(usize, &AggregateExpr)> = aggregates
        .iter()
        .map(|a| {
            let idx = if a.column == "*" {
                None
            } else {
                column_names.iter().position(|c| c == &a.column)
            };
            (idx.unwrap_or(0), a)
        })
        .collect();

    let mut groups: HashMap<Vec<String>, Vec<&[JsonValue]>> = HashMap::new();
    for row in rows {
        let key: Vec<String> = group_indices
            .iter()
            .map(|&i| json_to_cmp_str(&row[i]))
            .collect();
        groups.entry(key).or_default().push(row.as_slice());
    }

    let mut headers: Vec<String> = group_by.to_vec();
    for a in aggregates {
        headers.push(a.alias.clone());
    }

    let mut result_rows = Vec::new();
    let mut group_keys: Vec<_> = groups.keys().collect();
    group_keys.sort();
    for key in group_keys {
        let Some(group_rows) = groups.get(key) else {
            continue;
        };
        let mut row: Vec<JsonValue> = key.iter().map(|s| JsonValue::String(s.clone())).collect();
        for (col_idx, agg) in &agg_col_indices {
            let val = compute_one_aggregate(agg.function, *col_idx, &agg.column, group_rows);
            row.push(val);
        }
        result_rows.push(row);
    }
    (headers, result_rows)
}

fn compute_one_aggregate(
    func: AggFunc,
    col_idx: usize,
    col_name: &str,
    group_rows: &[&[JsonValue]],
) -> JsonValue {
    match func {
        AggFunc::Count => {
            let n = if col_name == "*" {
                group_rows.len()
            } else {
                group_rows
                    .iter()
                    .filter(|r| r.get(col_idx).is_some_and(|v| !v.is_null()))
                    .count()
            };
            #[allow(clippy::cast_possible_wrap)]
            JsonValue::Number(serde_json::Number::from(n as i64))
        }
        AggFunc::Sum => {
            let values: Vec<f64> = group_rows
                .iter()
                .filter_map(|r| r.get(col_idx).and_then(json_to_f64))
                .collect();
            let sum: f64 = values.iter().sum();
            serde_json::Number::from_f64(sum).map_or(JsonValue::Null, JsonValue::Number)
        }
        AggFunc::Avg => {
            let values: Vec<f64> = group_rows
                .iter()
                .filter_map(|r| r.get(col_idx).and_then(json_to_f64))
                .collect();
            if values.is_empty() {
                JsonValue::Null
            } else {
                let sum: f64 = values.iter().sum();
                #[allow(clippy::cast_precision_loss)]
                let avg = sum / (values.len() as f64);
                serde_json::Number::from_f64(avg).map_or(JsonValue::Null, JsonValue::Number)
            }
        }
        AggFunc::Min => {
            let min = group_rows
                .iter()
                .filter_map(|r| r.get(col_idx))
                .map(json_to_cmp_str)
                .min_by(std::cmp::Ord::cmp);
            min.map_or(JsonValue::Null, JsonValue::String)
        }
        AggFunc::Max => {
            let max = group_rows
                .iter()
                .filter_map(|r| r.get(col_idx))
                .map(json_to_cmp_str)
                .max_by(std::cmp::Ord::cmp);
            max.map_or(JsonValue::Null, JsonValue::String)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::methods::query::sql_parser::{AggFunc, AggregateExpr};

    fn agg_count_star() -> AggregateExpr {
        AggregateExpr {
            function: AggFunc::Count,
            column: "*".to_string(),
            alias: "cnt".to_string(),
        }
    }

    fn agg_sum(col: &str) -> AggregateExpr {
        AggregateExpr {
            function: AggFunc::Sum,
            column: col.to_string(),
            alias: format!("sum_{}", col),
        }
    }

    fn agg_avg(col: &str) -> AggregateExpr {
        AggregateExpr {
            function: AggFunc::Avg,
            column: col.to_string(),
            alias: format!("avg_{}", col),
        }
    }

    fn agg_min(col: &str) -> AggregateExpr {
        AggregateExpr {
            function: AggFunc::Min,
            column: col.to_string(),
            alias: format!("min_{}", col),
        }
    }

    fn agg_max(col: &str) -> AggregateExpr {
        AggregateExpr {
            function: AggFunc::Max,
            column: col.to_string(),
            alias: format!("max_{}", col),
        }
    }

    #[test]
    fn compute_aggregates_count_star() {
        let cols = ["id".to_string(), "name".to_string()];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![JsonValue::String("1".into()), JsonValue::String("a".into())],
            vec![JsonValue::String("2".into()), JsonValue::String("b".into())],
        ];
        let (headers, result) = compute_aggregates(&cols, &rows, &[], &[agg_count_star()]);
        assert_eq!(headers, ["cnt"]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], JsonValue::Number(serde_json::Number::from(2)));
    }

    #[test]
    fn compute_aggregates_sum_avg() {
        let cols = ["id".to_string(), "price".to_string()];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![JsonValue::String("1".into()), JsonValue::Number(10.into())],
            vec![JsonValue::String("2".into()), JsonValue::Number(20.into())],
            vec![JsonValue::String("3".into()), JsonValue::Number(30.into())],
        ];
        let (headers, result) =
            compute_aggregates(&cols, &rows, &[], &[agg_sum("price"), agg_avg("price")]);
        assert_eq!(headers, ["sum_price", "avg_price"]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0].as_f64(), Some(60.0));
        let avg = result[0][1].as_f64().expect("avg is number");
        assert!((avg - 20.0).abs() < 1e-9);
    }

    #[test]
    fn compute_aggregates_min_max() {
        let cols = ["label".to_string()];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![JsonValue::String("z".into())],
            vec![JsonValue::String("a".into())],
            vec![JsonValue::String("m".into())],
        ];
        let (headers, result) =
            compute_aggregates(&cols, &rows, &[], &[agg_min("label"), agg_max("label")]);
        assert_eq!(headers, ["min_label", "max_label"]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], JsonValue::String("a".into()));
        assert_eq!(result[0][1], JsonValue::String("z".into()));
    }

    #[test]
    fn compute_aggregates_group_by() {
        let cols = ["category".to_string(), "amount".to_string()];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![JsonValue::String("A".into()), JsonValue::Number(10.into())],
            vec![JsonValue::String("A".into()), JsonValue::Number(20.into())],
            vec![JsonValue::String("B".into()), JsonValue::Number(5.into())],
        ];
        let (headers, result) = compute_aggregates(
            &cols,
            &rows,
            &["category".to_string()],
            &[agg_sum("amount")],
        );
        assert_eq!(headers, ["category", "sum_amount"]);
        assert_eq!(result.len(), 2);
        let row_a = result.iter().find(|r| r[0].as_str() == Some("A")).unwrap();
        let row_b = result.iter().find(|r| r[0].as_str() == Some("B")).unwrap();
        assert_eq!(row_a[1].as_f64(), Some(30.0));
        assert_eq!(row_b[1].as_f64(), Some(5.0));
    }

    #[test]
    fn compute_aggregates_empty_rows() {
        let cols = ["id".to_string()];
        let rows: Vec<Vec<JsonValue>> = vec![];
        let (headers, result) = compute_aggregates(&cols, &rows, &[], &[agg_count_star()]);
        assert_eq!(headers, ["cnt"]);
        assert_eq!(result.len(), 0);
    }
}
