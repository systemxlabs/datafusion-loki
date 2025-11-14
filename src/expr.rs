use std::sync::LazyLock;

use datafusion::{
    logical_expr::{BinaryExpr, Like, Operator, ScalarUDFImpl, expr::ScalarFunction},
    prelude::Expr,
    scalar::ScalarValue,
};

use crate::{LABELS_FIELD_REF, LINE_FIELD_REF, MapGet, TIMESTAMP_FIELD_REF};

static MAP_GET_FUNC: LazyLock<MapGet> = LazyLock::new(|| MapGet::new());

pub fn expr_to_label_filter(expr: &Expr) -> Option<String> {
    if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
        let Expr::ScalarFunction(ScalarFunction { func, args }) = left.as_ref() else {
            return None;
        };
        if func.name() != MAP_GET_FUNC.name() {
            return None;
        }
        if args.len() != 2 {
            return None;
        }
        let label = match (&args[0], &args[1]) {
            (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(value), _))
                if col.name() == LABELS_FIELD_REF.name() =>
            {
                value.as_ref()
            }
            _ => None,
        }?;

        let Expr::Literal(ScalarValue::Utf8(value), _) = right.as_ref() else {
            return None;
        };
        let empty_string = String::new();
        let value = value.as_ref().unwrap_or(&empty_string);

        match op {
            Operator::Eq => Some(format!("{label}=\"{value}\"")),
            Operator::NotEq => Some(format!("{label}!=\"{value}\"")),
            Operator::RegexMatch => Some(format!("{label}=~\"{value}\"")),
            Operator::RegexNotMatch => Some(format!("{label}!~\"{value}\"")),
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
