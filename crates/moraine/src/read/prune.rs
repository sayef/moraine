//! Whether rows matching a filter can exist in a file or a page, from statistics alone.
//!
//! The answer is conservative: `false` only when the bounds prove no row matches. Understood
//! forms are comparisons of a column with a literal (either side), `IN` lists, `BETWEEN`,
//! `IS [NOT] NULL`, `AND` and `OR`; anything else may match.

use std::cmp::Ordering;

use arrow::datatypes::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Operator, expr::InList};

use crate::stats::ColRange;

/// Bounds of a column in the unit being pruned (a file or a page).
pub(crate) trait Bounds {
    fn bounds(&self, column: &str) -> Option<ColRange>;
}

/// Whether a row satisfying `e` may be present.
pub(crate) fn may_match(e: &Expr, schema: &Schema, b: &dyn Bounds) -> bool {
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => may_match(left, schema, b) && may_match(right, schema, b),
            Operator::Or => may_match(left, schema, b) || may_match(right, schema, b),
            _ => compare(left, *op, right, schema, b),
        },
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) => match column_of(expr) {
            Some(c) => list.iter().any(|v| match literal_of(v) {
                Some(lit) => test(c, Operator::Eq, lit, schema, b),
                None => true,
            }),
            None => true,
        },
        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => match (column_of(expr), literal_of(low), literal_of(high)) {
            (Some(c), Some(lo), Some(hi)) => {
                test(c, Operator::GtEq, lo, schema, b) && test(c, Operator::LtEq, hi, schema, b)
            }
            _ => true,
        },
        Expr::IsNull(x) => match column_of(x).and_then(|c| b.bounds(c)) {
            Some(r) => r.nulls != Some(0),
            None => true,
        },
        Expr::IsNotNull(x) => match column_of(x).and_then(|c| b.bounds(c)) {
            Some(r) => !r.all_null(),
            None => true,
        },
        Expr::Literal(ScalarValue::Boolean(Some(false)), _) => false,
        _ => true,
    }
}

fn column_of(e: &Expr) -> Option<&str> {
    match e {
        Expr::Column(c) => Some(c.name.as_str()),
        _ => None,
    }
}

fn literal_of(e: &Expr) -> Option<&ScalarValue> {
    match e {
        Expr::Literal(v, _) => Some(v),
        _ => None,
    }
}

fn compare(l: &Expr, op: Operator, r: &Expr, schema: &Schema, b: &dyn Bounds) -> bool {
    match (column_of(l), literal_of(r), column_of(r), literal_of(l)) {
        (Some(c), Some(v), _, _) => test(c, op, v, schema, b),
        (_, _, Some(c), Some(v)) => match op.swap() {
            Some(op) => test(c, op, v, schema, b),
            None => true,
        },
        _ => true,
    }
}

/// `column op value` against the bounds.
fn test(column: &str, op: Operator, value: &ScalarValue, schema: &Schema, b: &dyn Bounds) -> bool {
    let Ok(field) = schema.field_with_name(column) else {
        return true;
    };
    let Some(r) = b.bounds(column) else {
        return true;
    };
    if value.is_null() {
        // A comparison with NULL is never true.
        return !matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        );
    }
    if r.all_null() {
        return false;
    }
    let Some(v) = cast_to(value, field.data_type()) else {
        return true;
    };
    let cmp = |bound: &Option<ScalarValue>| bound.as_ref().and_then(|x| x.partial_cmp(&v));
    let (lo, hi) = (cmp(&r.min), cmp(&r.max));
    match op {
        Operator::Eq => lo != Some(Ordering::Greater) && hi != Some(Ordering::Less),
        Operator::NotEq => !(lo == Some(Ordering::Equal) && hi == Some(Ordering::Equal)),
        Operator::Lt => lo != Some(Ordering::Greater) && lo != Some(Ordering::Equal),
        Operator::LtEq => lo != Some(Ordering::Greater),
        Operator::Gt => hi != Some(Ordering::Less) && hi != Some(Ordering::Equal),
        Operator::GtEq => hi != Some(Ordering::Less),
        _ => true,
    }
}

/// `value` as the column's type, when that is lossless enough to compare.
pub(crate) fn cast_to(value: &ScalarValue, t: &DataType) -> Option<ScalarValue> {
    if &value.data_type() == t {
        return Some(value.clone());
    }
    // Integer literals against float columns and the reverse would round; leave them alone.
    let int = |d: &DataType| d.is_integer();
    let float = |d: &DataType| d.is_floating();
    if (int(&value.data_type()) && float(t)) || (float(&value.data_type()) && int(t)) {
        return None;
    }
    let c = value.cast_to(t).ok()?;
    // A cast that changed the value (an integer out of range) is not usable.
    match c.cast_to(&value.data_type()) {
        Ok(back) if &back == value => Some(c),
        _ => None,
    }
}

/// Equality literals per column of a filter's top-level conjuncts (`c = v` and `c IN (...)`),
/// for bloom-filter checks.
pub(crate) fn equality_values<'a>(e: &'a Expr, out: &mut Vec<(&'a str, Vec<&'a ScalarValue>)>) {
    match e {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            equality_values(left, out);
            equality_values(right, out);
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => match (
            column_of(left),
            literal_of(right),
            column_of(right),
            literal_of(left),
        ) {
            (Some(c), Some(v), _, _) | (_, _, Some(c), Some(v)) => out.push((c, vec![v])),
            _ => {}
        },
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) => {
            if let Some(c) = column_of(expr) {
                let vals: Option<Vec<&ScalarValue>> = list.iter().map(literal_of).collect();
                if let Some(v) = vals {
                    out.push((c, v));
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::prelude::{col, lit};
    use std::collections::HashMap;

    struct M(HashMap<&'static str, ColRange>);
    impl Bounds for M {
        fn bounds(&self, c: &str) -> Option<ColRange> {
            self.0.get(c).cloned()
        }
    }

    fn r(lo: i64, hi: i64) -> ColRange {
        ColRange {
            min: Some(ScalarValue::Int64(Some(lo))),
            max: Some(ScalarValue::Int64(Some(hi))),
            nulls: Some(0),
            rows: Some(10),
        }
    }

    #[test]
    fn prunes_ranges() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]);
        let mut m = HashMap::new();
        m.insert("a", r(10, 20));
        m.insert(
            "s",
            ColRange {
                min: Some(ScalarValue::Utf8(Some("m".into()))),
                max: Some(ScalarValue::Utf8(Some("p".into()))),
                nulls: Some(0),
                rows: Some(10),
            },
        );
        let m = M(m);
        let t = |e: Expr| may_match(&e, &schema, &m);
        assert!(t(col("a").eq(lit(15i64))));
        assert!(t(col("a").eq(lit(15i32))));
        assert!(!t(col("a").eq(lit(21i64))));
        assert!(!t(lit(5i64).eq(col("a"))));
        assert!(!t(col("a").gt(lit(20i64))));
        assert!(t(col("a").gt_eq(lit(20i64))));
        assert!(!t(col("a").lt(lit(10i64))));
        assert!(t(col("a").in_list(vec![lit(1i64), lit(12i64)], false)));
        assert!(!t(col("a").in_list(vec![lit(1i64), lit(30i64)], false)));
        assert!(!t(col("a").between(lit(21i64), lit(30i64))));
        assert!(t(col("a").eq(lit(1i64)).or(col("s").eq(lit("n")))));
        assert!(!t(col("a").eq(lit(15i64)).and(col("s").eq(lit("z")))));
        assert!(!t(col("a").is_null()));
        // Unknown forms may match.
        assert!(t(!col("a").not_eq(lit(15i64))));
    }
}
