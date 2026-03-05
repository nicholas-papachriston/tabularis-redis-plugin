//! Wrapper around sqlparser-rs that produces plugin-specific AST types for SELECT, INSERT, UPDATE, DELETE.

#![allow(
    dead_code,
    clippy::match_wildcard_for_single_variants,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::or_fun_call
)]

use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, Ident, LimitClause, ObjectNamePart, ObjectType, OrderByExpr, OrderByKind,
    SelectItem, SetExpr, Statement, TableFactor, TableObject, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::str::FromStr;

/// Top-level parsed statement.
#[derive(Debug, Clone)]
pub enum ParsedStatement {
    Select(SelectQuery),
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
    CreateIndex(CreateIndexQuery),
    DropTable(DropTableQuery),
    Unsupported(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: AggFunc,
    pub column: String,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub columns: Vec<SelectColumn>,
    pub table: String,
    pub where_clause: Option<WhereExpr>,
    pub order_by: Vec<(String, bool)>,
    pub limit: Option<u64>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<AggregateExpr>,
}

#[derive(Debug, Clone)]
pub struct SelectColumn {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum WhereExpr {
    Condition(Condition),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub column: String,
    pub op: ComparisonOp,
    pub value: ConditionValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
    In,
    Between,
}

#[derive(Debug, Clone)]
pub enum ConditionValue {
    Literal(String),
    List(Vec<String>),
    Range(String, String),
}

#[derive(Debug, Clone)]
pub struct InsertQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct UpdateQuery {
    pub table: String,
    pub assignments: Vec<(String, String)>,
    pub where_clause: Option<WhereExpr>,
}

#[derive(Debug, Clone)]
pub struct DeleteQuery {
    pub table: String,
    pub where_clause: Option<WhereExpr>,
}

#[derive(Debug, Clone)]
pub struct CreateIndexQuery {
    pub index_name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

#[derive(Debug, Clone)]
pub struct DropTableQuery {
    pub table: String,
    pub if_exists: bool,
}

/// SQL LIKE pattern matching: % = any sequence, _ = any single character.
pub fn sql_like_match(haystack: &str, pattern: &str) -> bool {
    like_match_iterative(haystack, pattern)
}

/// Returns true if segment matches hay at position start; segment may contain '_' (any one char).
fn segment_matches(hay_chars: &[char], start: usize, segment: &[char]) -> bool {
    if start + segment.len() > hay_chars.len() {
        return false;
    }
    for (i, &sc) in segment.iter().enumerate() {
        let hc = hay_chars[start + i];
        if sc != '_' && sc != hc {
            return false;
        }
    }
    true
}

/// Single-pass iterative LIKE: split pattern by %, match each segment in order; _ = one char.
fn like_match_iterative(hay: &str, pat: &str) -> bool {
    let pat_chars: Vec<char> = pat.chars().collect();
    if pat_chars.is_empty() {
        return hay.is_empty();
    }
    let mut segs: Vec<Vec<char>> = Vec::new();
    let mut cur = Vec::new();
    for &c in &pat_chars {
        if c == '%' {
            if !cur.is_empty() {
                segs.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(c);
        }
    }
    if !cur.is_empty() {
        segs.push(cur);
    }
    let leading_wild = pat.starts_with('%');
    let trailing_wild = pat.ends_with('%');
    let hay_chars: Vec<char> = hay.chars().collect();
    let n = hay_chars.len();
    let mut hay_start = 0;
    for (seg_idx, seg) in segs.iter().enumerate() {
        if seg.is_empty() {
            continue;
        }
        let is_first = seg_idx == 0 && !leading_wild;
        let is_last = seg_idx == segs.len() - 1 && !trailing_wild;
        let need = seg.len();
        if need > n.saturating_sub(hay_start) {
            return false;
        }
        let found = if is_first {
            if segment_matches(&hay_chars, hay_start, seg) {
                Some(hay_start + need)
            } else {
                None
            }
        } else if is_last {
            (hay_start..=n.saturating_sub(need))
                .find(|&i| segment_matches(&hay_chars, i, seg))
                .map(|i| i + need)
        } else {
            (hay_start..=n.saturating_sub(need))
                .find(|&i| segment_matches(&hay_chars, i, seg))
                .map(|i| i + need)
        };
        let Some(next_start) = found else {
            return false;
        };
        hay_start = next_start;
    }
    trailing_wild || hay_start >= n
}


/// Evaluate WHERE expression against a row. `column_names` and `row_values` must be parallel (same length).
pub fn evaluate_where(column_names: &[String], row_values: &[String], expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::And(l, r) => {
            evaluate_where(column_names, row_values, l)
                && evaluate_where(column_names, row_values, r)
        }
        WhereExpr::Or(l, r) => {
            evaluate_where(column_names, row_values, l)
                || evaluate_where(column_names, row_values, r)
        }
        WhereExpr::Condition(c) => evaluate_condition(column_names, row_values, c),
    }
}

fn try_cmp_numeric(cell: &str, lit: &str, op: std::cmp::Ordering) -> Option<bool> {
    let a: f64 = cell.trim().parse().ok()?;
    let b: f64 = lit.trim().parse().ok()?;
    Some(a.partial_cmp(&b).is_some_and(|ord| ord == op))
}

fn try_cmp_numeric_ge(cell: &str, lit: &str) -> Option<bool> {
    let a: f64 = cell.trim().parse().ok()?;
    let b: f64 = lit.trim().parse().ok()?;
    Some(a.partial_cmp(&b).is_some_and(|ord| ord != std::cmp::Ordering::Less))
}

fn try_cmp_numeric_le(cell: &str, lit: &str) -> Option<bool> {
    let a: f64 = cell.trim().parse().ok()?;
    let b: f64 = lit.trim().parse().ok()?;
    Some(a.partial_cmp(&b).is_some_and(|ord| ord != std::cmp::Ordering::Greater))
}

fn evaluate_condition(column_names: &[String], row_values: &[String], c: &Condition) -> bool {
    let Some(col_idx) = column_names.iter().position(|n| n == &c.column) else {
        return false;
    };
    let cell = row_values.get(col_idx).map_or("", String::as_str);
    let lit = c.value.as_literal();
    let result = match &c.op {
        ComparisonOp::Eq => cell == lit,
        ComparisonOp::Ne => cell != lit,
        ComparisonOp::Gt => try_cmp_numeric(cell, lit, std::cmp::Ordering::Greater)
            .unwrap_or_else(|| cell > lit),
        ComparisonOp::Lt => try_cmp_numeric(cell, lit, std::cmp::Ordering::Less)
            .unwrap_or_else(|| cell < lit),
        ComparisonOp::Gte => try_cmp_numeric_ge(cell, lit).unwrap_or_else(|| cell >= lit),
        ComparisonOp::Lte => try_cmp_numeric_le(cell, lit).unwrap_or_else(|| cell <= lit),
        ComparisonOp::Like => sql_like_match(cell, lit),
        ComparisonOp::In => c.value.as_list().iter().any(|v| v == cell),
        ComparisonOp::Between => {
            let (lo, hi) = c.value.as_range();
            if let (Some(a), Some(b), Some(c_hi)) = (
                cell.trim().parse::<f64>().ok(),
                lo.trim().parse::<f64>().ok(),
                hi.trim().parse::<f64>().ok(),
            ) {
                a >= b && a <= c_hi
            } else {
                cell >= lo && cell <= hi
            }
        }
    };
    result
}

impl ConditionValue {
    fn as_literal(&self) -> &str {
        match self {
            Self::Literal(s) => s.as_str(),
            Self::List(v) => v.first().map_or("", String::as_str),
            Self::Range(lo, _) => lo.as_str(),
        }
    }
    const fn as_list(&self) -> &[String] {
        match self {
            Self::List(v) => v.as_slice(),
            _ => &[],
        }
    }
    const fn as_range(&self) -> (&str, &str) {
        match self {
            Self::Range(lo, hi) => (lo.as_str(), hi.as_str()),
            _ => ("", ""),
        }
    }
}

/// Parse SQL string into our `ParsedStatement`. Uses `GenericDialect`.
pub fn parse_sql(query: &str) -> Result<ParsedStatement, String> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, query).map_err(|e| e.to_string())?;
    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| "Empty query".to_string())?;
    map_statement(stmt)
}

fn map_statement(stmt: Statement) -> Result<ParsedStatement, String> {
    match stmt {
        Statement::Query(q) => map_query(*q),
        Statement::Insert(i) => map_insert(i),
        Statement::Update(u) => map_update(u.table.clone(), u.assignments.clone(), u.selection),
        Statement::Delete(d) => map_delete(d),
        Statement::CreateIndex(c) => map_create_index(c),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            if object_type == ObjectType::Table && names.len() == 1 {
                let table = object_name_to_string(&names[0]);
                Ok(ParsedStatement::DropTable(DropTableQuery {
                    table,
                    if_exists,
                }))
            } else {
                Ok(ParsedStatement::Unsupported(
                    "Only DROP TABLE supported".to_string(),
                ))
            }
        }
        _ => Ok(ParsedStatement::Unsupported(format!(
            "Statement not supported: {stmt:?}"
        ))),
    }
}

fn map_query(q: sqlparser::ast::Query) -> Result<ParsedStatement, String> {
    let select = match *q.body {
        SetExpr::Select(s) => *s,
        _ => {
            return Ok(ParsedStatement::Unsupported(
                "Only SELECT body supported".to_string(),
            ))
        }
    };
    let table = first_table_name(&select.from)?;
    let (columns, aggregates) = map_projection(&select.projection)?;
    let where_clause = select.selection.map(map_where_expr).transpose()?;
    let order_by = q
        .order_by
        .as_ref()
        .and_then(|o| match &o.kind {
            OrderByKind::Expressions(exprs) => Some(exprs.iter().map(map_order_by).collect()),
            _ => None,
        })
        .unwrap_or_default();
    let limit = q.limit_clause.as_ref().and_then(|lc| match lc {
        LimitClause::LimitOffset { limit: Some(e), .. }
        | LimitClause::OffsetCommaLimit { limit: e, .. } => expr_to_u64(e),
        _ => None,
    });
    let group_by = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .filter_map(expr_to_ident)
            .map(|s| s.value.clone())
            .collect(),
        _ => Vec::new(),
    };
    Ok(ParsedStatement::Select(SelectQuery {
        columns,
        table,
        where_clause,
        order_by,
        limit,
        group_by,
        aggregates,
    }))
}

fn object_name_to_string(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(id) => Some(id.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn first_table_name(from: &[TableWithJoins]) -> Result<String, String> {
    let twj = from
        .first()
        .ok_or_else(|| "Missing FROM clause".to_string())?;
    match &twj.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        _ => Err("Only simple table names supported in FROM".to_string()),
    }
}

fn map_projection(
    projection: &[SelectItem],
) -> Result<(Vec<SelectColumn>, Vec<AggregateExpr>), String> {
    let mut columns = Vec::new();
    let mut aggregates = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some((agg, display_name)) = try_parse_aggregate(expr) {
                    columns.push(SelectColumn {
                        name: display_name.clone(),
                        alias: None,
                    });
                    aggregates.push(AggregateExpr {
                        alias: display_name,
                        ..agg
                    });
                } else {
                    let (name, alias) = expr_to_name_alias(expr)?;
                    columns.push(SelectColumn { name, alias });
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let alias_str = alias.value.clone();
                if let Some((agg, display_name)) = try_parse_aggregate(expr) {
                    columns.push(SelectColumn {
                        name: display_name.clone(),
                        alias: Some(alias_str.clone()),
                    });
                    aggregates.push(AggregateExpr {
                        alias: alias_str,
                        ..agg
                    });
                } else {
                    let (name, _) = expr_to_name_alias(expr)?;
                    columns.push(SelectColumn {
                        name,
                        alias: Some(alias_str),
                    });
                }
            }
            SelectItem::Wildcard(_) => {
                columns.push(SelectColumn {
                    name: "*".to_string(),
                    alias: None,
                });
            }
            _ => return Err("Unsupported SELECT item".to_string()),
        }
    }
    Ok((columns, aggregates))
}

fn try_parse_aggregate(expr: &Expr) -> Option<(AggregateExpr, String)> {
    let Expr::Function(func) = expr else {
        return None;
    };
    let name = object_name_to_string(&func.name).to_uppercase();
    let agg_func = match name.as_str() {
        "COUNT" => AggFunc::Count,
        "SUM" => AggFunc::Sum,
        "AVG" => AggFunc::Avg,
        "MIN" => AggFunc::Min,
        "MAX" => AggFunc::Max,
        _ => return None,
    };
    let column = match &func.args {
        FunctionArguments::List(list) => list
            .args
            .first()
            .and_then(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Some("*".to_string()),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                    expr_to_ident(e).map(|id| id.value.clone())
                }
                _ => None,
            })
            .unwrap_or_else(|| "*".to_string()),
        _ => "*".to_string(),
    };
    let display = format!("{name}({column})");
    Some((
        AggregateExpr {
            function: agg_func,
            column,
            alias: display.clone(),
        },
        display,
    ))
}

fn expr_to_name_alias(expr: &Expr) -> Result<(String, Option<String>), String> {
    match expr {
        Expr::Identifier(id) => Ok((id.value.clone(), None)),
        Expr::CompoundIdentifier(ids) => {
            let name = ids
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            Ok((name, None))
        }
        Expr::Function(func) => Ok((object_name_to_string(&func.name), None)),
        _ => Err(format!("Unsupported select expression: {expr:?}")),
    }
}

fn map_where_expr(expr: Expr) -> Result<WhereExpr, String> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(WhereExpr::And(
                Box::new(map_where_expr(*left)?),
                Box::new(map_where_expr(*right)?),
            )),
            BinaryOperator::Or => Ok(WhereExpr::Or(
                Box::new(map_where_expr(*left)?),
                Box::new(map_where_expr(*right)?),
            )),
            BinaryOperator::Eq => Ok(WhereExpr::Condition(Condition {
                column: expr_to_ident(&left)
                    .ok_or("left must be column")?
                    .to_string(),
                op: ComparisonOp::Eq,
                value: expr_to_literal(&right)?,
            })),
            BinaryOperator::NotEq | BinaryOperator::Spaceship => {
                Ok(WhereExpr::Condition(Condition {
                    column: expr_to_ident(&left)
                        .ok_or("left must be column")?
                        .to_string(),
                    op: ComparisonOp::Ne,
                    value: expr_to_literal(&right)?,
                }))
            }
            BinaryOperator::Gt => Ok(WhereExpr::Condition(Condition {
                column: expr_to_ident(&left)
                    .ok_or("left must be column")?
                    .to_string(),
                op: ComparisonOp::Gt,
                value: expr_to_literal(&right)?,
            })),
            BinaryOperator::Lt => Ok(WhereExpr::Condition(Condition {
                column: expr_to_ident(&left)
                    .ok_or("left must be column")?
                    .to_string(),
                op: ComparisonOp::Lt,
                value: expr_to_literal(&right)?,
            })),
            BinaryOperator::GtEq => Ok(WhereExpr::Condition(Condition {
                column: expr_to_ident(&left)
                    .ok_or("left must be column")?
                    .to_string(),
                op: ComparisonOp::Gte,
                value: expr_to_literal(&right)?,
            })),
            BinaryOperator::LtEq => Ok(WhereExpr::Condition(Condition {
                column: expr_to_ident(&left)
                    .ok_or("left must be column")?
                    .to_string(),
                op: ComparisonOp::Lte,
                value: expr_to_literal(&right)?,
            })),
            _ => Err(format!("Unsupported binary operator in WHERE: {op}")),
        },
        Expr::Like {
            negated: false,
            expr,
            pattern,
            ..
        } => Ok(WhereExpr::Condition(Condition {
            column: expr_to_ident(&expr)
                .ok_or("left of LIKE must be column")?
                .to_string(),
            op: ComparisonOp::Like,
            value: ConditionValue::Literal(expr_to_literal_str(&pattern)?),
        })),
        Expr::Like { negated: true, .. } => Err("NOT LIKE not yet supported".to_string()),
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = expr_to_ident(&expr)
                .ok_or("left of IN must be column")?
                .to_string();
            let values: Result<Vec<_>, _> = list.iter().map(expr_to_literal_str).collect();
            Ok(WhereExpr::Condition(Condition {
                column,
                op: ComparisonOp::In,
                value: ConditionValue::List(values?),
            }))
        }
        Expr::InList { negated: true, .. } => Err("NOT IN not yet supported".to_string()),
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => Ok(WhereExpr::Condition(Condition {
            column: expr_to_ident(&expr)
                .ok_or("BETWEEN left must be column")?
                .to_string(),
            op: ComparisonOp::Between,
            value: ConditionValue::Range(expr_to_literal_str(&low)?, expr_to_literal_str(&high)?),
        })),
        Expr::Between { negated: true, .. } => Err("NOT BETWEEN not yet supported".to_string()),
        _ => Err(format!("Unsupported WHERE expression: {expr:?}")),
    }
}

const fn expr_to_ident(expr: &Expr) -> Option<&Ident> {
    match expr {
        Expr::Identifier(id) => Some(id),
        _ => None,
    }
}

fn expr_to_literal(expr: &Expr) -> Result<ConditionValue, String> {
    let s = expr_to_literal_str(expr)?;
    Ok(ConditionValue::Literal(s))
}

fn expr_to_literal_str(expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Value(v) => value_to_string(&v.value),
        Expr::Identifier(id) => Ok(id.value.clone()),
        _ => Err(format!("Expected literal value: {expr:?}")),
    }
}

fn value_to_string(v: &Value) -> Result<String, String> {
    match v {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s.clone()),
        Value::Number(n, _) => Ok(n.clone()),
        Value::Boolean(b) => Ok(b.to_string()),
        Value::Null => Ok("NULL".to_string()),
        _ => Err(format!("Unsupported value: {v:?}")),
    }
}

fn expr_to_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => u64::from_str(n).ok(),
            _ => None,
        },
        _ => None,
    }
}

fn map_order_by(o: &OrderByExpr) -> (String, bool) {
    let col = match &o.expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(ids) => ids
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        _ => String::new(),
    };
    let asc = !matches!(o.options.asc, Some(false));
    (col, asc)
}

fn map_insert(i: sqlparser::ast::Insert) -> Result<ParsedStatement, String> {
    let table = match &i.table {
        TableObject::TableName(name) => object_name_to_string(name),
        _ => return Err("INSERT table must be a table name".to_string()),
    };
    let columns: Vec<String> = i.columns.iter().map(|c| c.value.clone()).collect();
    let rows = match i.source {
        Some(q) => {
            let body = match *q.body {
                SetExpr::Values(v) => v.rows,
                _ => return Err("INSERT ... SELECT not supported".to_string()),
            };
            let mut out = Vec::new();
            for row in body {
                let row_vals: Result<Vec<String>, _> =
                    row.iter().map(expr_to_literal_str).collect();
                out.push(row_vals?);
            }
            out
        }
        None => return Err("INSERT must have VALUES (...)".to_string()),
    };
    Ok(ParsedStatement::Insert(InsertQuery {
        table,
        columns,
        rows,
    }))
}

fn map_update(
    twj: TableWithJoins,
    assignments: Vec<sqlparser::ast::Assignment>,
    selection: Option<Expr>,
) -> Result<ParsedStatement, String> {
    let table = match &twj.relation {
        TableFactor::Table { name, .. } => object_name_to_string(name),
        _ => return Err("UPDATE: simple table required".to_string()),
    };
    let assigns: Result<Vec<(String, String)>, _> = assignments
        .iter()
        .map(|a| {
            let col = match &a.target {
                AssignmentTarget::ColumnName(name) => object_name_to_string(name),
                _ => return Err("UPDATE: only simple column assignments supported".to_string()),
            };
            let val = expr_to_literal_str(&a.value)?;
            Ok((col, val))
        })
        .collect();
    let assignments = assigns?;
    let where_clause = selection.map(map_where_expr).transpose()?;
    Ok(ParsedStatement::Update(UpdateQuery {
        table,
        assignments,
        where_clause,
    }))
}

fn map_delete(d: sqlparser::ast::Delete) -> Result<ParsedStatement, String> {
    let table = d.tables.first().map(object_name_to_string).or_else(|| {
        let from_tables = match &d.from {
            sqlparser::ast::FromTable::WithFromKeyword(twjs)
            | sqlparser::ast::FromTable::WithoutKeyword(twjs) => twjs,
        };
        from_tables.first().and_then(|twj| match &twj.relation {
            TableFactor::Table { name, .. } => Some(object_name_to_string(name)),
            _ => None,
        })
    });
    let table = table.ok_or_else(|| "DELETE: table required".to_string())?;
    let where_clause = d.selection.map(map_where_expr).transpose()?;
    Ok(ParsedStatement::Delete(DeleteQuery {
        table,
        where_clause,
    }))
}

fn map_create_index(c: sqlparser::ast::CreateIndex) -> Result<ParsedStatement, String> {
    let index_name = c
        .name
        .as_ref()
        .map_or_else(|| "idx".to_string(), object_name_to_string);
    let table = object_name_to_string(&c.table_name);
    let columns: Vec<String> = c
        .columns
        .iter()
        .filter_map(|e| {
            if let Expr::Identifier(id) = &e.column.expr {
                Some(id.value.clone())
            } else {
                None
            }
        })
        .collect();
    Ok(ParsedStatement::CreateIndex(CreateIndexQuery {
        index_name,
        table,
        columns,
        is_unique: c.unique,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_like_match_exact() {
        assert!(sql_like_match("hello", "hello"));
        assert!(!sql_like_match("hello", "hell"));
        assert!(!sql_like_match("hell", "hello"));
    }

    #[test]
    fn sql_like_match_wildcards() {
        assert!(sql_like_match("hello", "%"));
        assert!(sql_like_match("", "%"));
        assert!(sql_like_match("hello", "%o"));
        assert!(sql_like_match("hello", "h%"));
        assert!(sql_like_match("hello", "%l%"));
        assert!(sql_like_match("hello", "h%o"));
        assert!(!sql_like_match("hello", "%x%"));
        assert!(!sql_like_match("hi", "_"));
        assert!(sql_like_match("hi", "__"));
        assert!(sql_like_match("hi", "_%"));
    }

    #[test]
    fn evaluate_where_eq_and_like() {
        let cols = ["name".to_string(), "score".to_string()];
        let row_ok = ["alice".to_string(), "100".to_string()];
        let row_bad = ["bob".to_string(), "50".to_string()];

        let cond_eq = WhereExpr::Condition(Condition {
            column: "name".to_string(),
            op: ComparisonOp::Eq,
            value: ConditionValue::Literal("alice".to_string()),
        });
        assert!(evaluate_where(&cols, &row_ok, &cond_eq));
        assert!(!evaluate_where(&cols, &row_bad, &cond_eq));

        let cond_like = WhereExpr::Condition(Condition {
            column: "name".to_string(),
            op: ComparisonOp::Like,
            value: ConditionValue::Literal("ali%".to_string()),
        });
        assert!(evaluate_where(&cols, &row_ok, &cond_like));
        assert!(!evaluate_where(&cols, &row_bad, &cond_like));
    }

    #[test]
    fn parse_sql_select() {
        let r = parse_sql("SELECT a, b FROM t").unwrap();
        match &r {
            ParsedStatement::Select(s) => {
                assert_eq!(s.table, "t");
                assert_eq!(s.columns.len(), 2);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_sql_insert() {
        let r = parse_sql("INSERT INTO t (id, name) VALUES (1, 'x')").unwrap();
        match &r {
            ParsedStatement::Insert(s) => {
                assert_eq!(s.table, "t");
                assert_eq!(s.columns, ["id", "name"]);
                assert_eq!(s.rows.len(), 1);
                assert_eq!(s.rows[0], ["1", "x"]);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_sql_update() {
        let r = parse_sql("UPDATE t SET name = 'y' WHERE id = 1").unwrap();
        match &r {
            ParsedStatement::Update(s) => {
                assert_eq!(s.table, "t");
                assert_eq!(s.assignments.len(), 1);
                assert_eq!(s.assignments[0].0, "name");
                assert_eq!(s.assignments[0].1, "y");
                assert!(s.where_clause.is_some());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn parse_sql_delete() {
        let r = parse_sql("DELETE FROM t WHERE id = 1").unwrap();
        match &r {
            ParsedStatement::Delete(s) => {
                assert_eq!(s.table, "t");
                assert!(s.where_clause.is_some());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn parse_sql_select_aggregates() {
        let r = parse_sql("SELECT COUNT(*), SUM(price) AS total FROM orders").unwrap();
        match &r {
            ParsedStatement::Select(s) => {
                assert_eq!(s.table, "orders");
                assert_eq!(s.aggregates.len(), 2);
                assert_eq!(s.aggregates[0].function, AggFunc::Count);
                assert_eq!(s.aggregates[0].column, "*");
                assert_eq!(s.aggregates[1].function, AggFunc::Sum);
                assert_eq!(s.aggregates[1].column, "price");
                assert_eq!(s.aggregates[1].alias, "total");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_sql_select_group_by() {
        let r = parse_sql("SELECT category, COUNT(*) FROM products GROUP BY category").unwrap();
        match &r {
            ParsedStatement::Select(s) => {
                assert_eq!(s.table, "products");
                assert_eq!(s.group_by, ["category"]);
                assert_eq!(s.aggregates.len(), 1);
                assert_eq!(s.aggregates[0].function, AggFunc::Count);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn evaluate_where_and_or() {
        let cols = ["a".to_string(), "b".to_string()];
        let row1 = ["1".to_string(), "x".to_string()];
        let row2 = ["2".to_string(), "x".to_string()];
        let row3 = ["1".to_string(), "y".to_string()];

        let expr = WhereExpr::And(
            Box::new(WhereExpr::Condition(Condition {
                column: "a".to_string(),
                op: ComparisonOp::Eq,
                value: ConditionValue::Literal("1".to_string()),
            })),
            Box::new(WhereExpr::Condition(Condition {
                column: "b".to_string(),
                op: ComparisonOp::Eq,
                value: ConditionValue::Literal("x".to_string()),
            })),
        );
        assert!(evaluate_where(&cols, &row1, &expr));
        assert!(!evaluate_where(&cols, &row2, &expr));
        assert!(!evaluate_where(&cols, &row3, &expr));

        let expr_or = WhereExpr::Or(
            Box::new(WhereExpr::Condition(Condition {
                column: "a".to_string(),
                op: ComparisonOp::Eq,
                value: ConditionValue::Literal("2".to_string()),
            })),
            Box::new(WhereExpr::Condition(Condition {
                column: "b".to_string(),
                op: ComparisonOp::Eq,
                value: ConditionValue::Literal("y".to_string()),
            })),
        );
        assert!(!evaluate_where(&cols, &row1, &expr_or));
        assert!(evaluate_where(&cols, &row2, &expr_or));
        assert!(evaluate_where(&cols, &row3, &expr_or));
    }

    #[test]
    fn evaluate_where_in_between() {
        let cols = ["id".to_string(), "score".to_string()];
        let row_in = ["2".to_string(), "50".to_string()];
        let row_not_in = ["99".to_string(), "50".to_string()];
        let row_between = ["x".to_string(), "75".to_string()];
        let row_not_between = ["x".to_string(), "10".to_string()];

        let cond_in = WhereExpr::Condition(Condition {
            column: "id".to_string(),
            op: ComparisonOp::In,
            value: ConditionValue::List(vec!["1".into(), "2".into(), "3".into()]),
        });
        assert!(evaluate_where(&cols, &row_in, &cond_in));
        assert!(!evaluate_where(&cols, &row_not_in, &cond_in));

        // Between uses string comparison: "50" <= "75" <= "99" (lexicographic)
        let cond_between = WhereExpr::Condition(Condition {
            column: "score".to_string(),
            op: ComparisonOp::Between,
            value: ConditionValue::Range("50".to_string(), "99".to_string()),
        });
        assert!(evaluate_where(&cols, &row_between, &cond_between));
        assert!(!evaluate_where(&cols, &row_not_between, &cond_between));
    }
}
