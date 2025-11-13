use datafusion::{
    logical_expr::{BinaryExpr, Like, Operator},
    prelude::Expr,
    scalar::ScalarValue,
};

use crate::{LINE_FIELD_REF, TIMESTAMP_FIELD_REF};

// TODO whether this can be simplified
pub fn expr_to_label_filter(expr: &Expr, labels: &[String]) -> Option<String> {
    let cols = expr.column_refs();
    if cols.len() != 1 {
        return None;
    }
    let Some(col) = cols.iter().next() else {
        return None;
    };
    if !labels.contains(&col.name) {
        return None;
    }

    let empty_string = String::new();

    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(ScalarValue::Utf8(value), _)) => Some(format!(
                    "{}=\"{}\"",
                    col.name,
                    value.as_ref().unwrap_or(&empty_string)
                )),
                (Expr::Literal(ScalarValue::Utf8(value), _), Expr::Column(_)) => Some(format!(
                    "{}=\"{}\"",
                    col.name,
                    value.as_ref().unwrap_or(&empty_string)
                )),
                _ => None,
            },
            Operator::NotEq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(ScalarValue::Utf8(value), _)) => Some(format!(
                    "{}!=\"{}\"",
                    col.name,
                    value.as_ref().unwrap_or(&empty_string)
                )),
                (Expr::Literal(ScalarValue::Utf8(value), _), Expr::Column(_)) => Some(format!(
                    "{}!=\"{}\"",
                    col.name,
                    value.as_ref().unwrap_or(&empty_string)
                )),
                _ => None,
            },
            Operator::RegexMatch => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(ScalarValue::Utf8(value), _)) => Some(format!(
                    "{}=~\"{}\"",
                    col.name,
                    value.as_ref().unwrap_or(&empty_string)
                )),
                (Expr::Literal(ScalarValue::Utf8(value), _), Expr::Column(_)) => Some(format!(
                    "{}=~\"{}\"",
                    col.name,
                    value.as_ref().unwrap_or(&empty_string)
                )),
                _ => None,
            },
            Operator::RegexNotMatch => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(ScalarValue::Utf8(value), _)) => Some(format!(
                    "{}!~\"{}\"",
                    col.name,
                    value.as_ref().unwrap_or(&empty_string)
                )),
                (Expr::Literal(ScalarValue::Utf8(value), _), Expr::Column(_)) => Some(format!(
                    "{}!~\"{}\"",
                    col.name,
                    value.as_ref().unwrap_or(&empty_string)
                )),
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    }
}

pub fn expr_to_line_filter(expr: &Expr) -> Option<String> {
    let cols = expr.column_refs();
    if cols.len() != 1 {
        return None;
    }
    let Some(col) = cols.iter().next() else {
        return None;
    };
    if col.name() != LINE_FIELD_REF.name() {
        return None;
    }

    let empty_string = String::new();

    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        match op {
            Operator::RegexMatch => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(ScalarValue::Utf8(value), _)) => {
                    Some(format!("|~`{}`", value.as_ref().unwrap_or(&empty_string)))
                }
                (Expr::Literal(ScalarValue::Utf8(value), _), Expr::Column(_)) => {
                    Some(format!("|~`{}`", value.as_ref().unwrap_or(&empty_string)))
                }
                _ => None,
            },
            Operator::RegexNotMatch => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(ScalarValue::Utf8(value), _)) => {
                    Some(format!("!~`{}`", value.as_ref().unwrap_or(&empty_string)))
                }
                (Expr::Literal(ScalarValue::Utf8(value), _), Expr::Column(_)) => {
                    Some(format!("!~`{}`", value.as_ref().unwrap_or(&empty_string)))
                }
                _ => None,
            },
            _ => None,
        }
    } else if let Expr::Like(Like {
        negated,
        expr,
        pattern,
        case_insensitive,
        ..
    }) = expr
    {
        let Expr::Column(_) = expr.as_ref() else {
            return None;
        };
        let Expr::Literal(ScalarValue::Utf8(value), _) = pattern.as_ref() else {
            return None;
        };
        let value = value.as_ref().unwrap_or(&empty_string);
        if value.starts_with("%") && value.ends_with("%") && !value.contains("_") {
            match (negated, case_insensitive) {
                (true, true) => Some(format!("!~ `(?i){}`", value)),
                (true, false) => Some(format!("!= `{}`", value)),
                (false, true) => Some(format!("|~ `(?i){}`", value)),
                (false, false) => Some(format!("|= `{}`", value)),
            }
        } else {
            None
        }
    } else {
        None
    }
}

pub enum TimestampBound {
    Start(Option<i64>),
    End(Option<i64>),
}
pub fn parse_timestamp_bound(expr: &Expr) -> Option<TimestampBound> {
    let cols = expr.column_refs();
    if cols.len() != 1 {
        return None;
    }
    let Some(col) = cols.iter().next() else {
        return None;
    };
    if col.name() != TIMESTAMP_FIELD_REF.name() {
        return None;
    }
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        match op {
            Operator::Lt | Operator::LtEq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(ScalarValue::TimestampNanosecond(value, _), _)) => {
                    Some(TimestampBound::End(*value))
                }
                (Expr::Literal(ScalarValue::TimestampNanosecond(value, _), _), Expr::Column(_)) => {
                    Some(TimestampBound::Start(*value))
                }
                _ => None,
            },
            Operator::Gt | Operator::GtEq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(ScalarValue::TimestampNanosecond(value, _), _)) => {
                    Some(TimestampBound::Start(*value))
                }
                (Expr::Literal(ScalarValue::TimestampNanosecond(value, _), _), Expr::Column(_)) => {
                    Some(TimestampBound::End(*value))
                }
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    }
}
