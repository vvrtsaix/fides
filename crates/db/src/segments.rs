//! Dynamic-segment rule DSL: validation, safe SQL compilation, and the worker
//! reconcile job.
//!
//! SECURITY: the rule comes from API input. Field names and operators are mapped
//! through fixed whitelists to constant SQL fragments; every *value* is bound as
//! a parameter via `QueryBuilder::push_bind` and never inlined into SQL. There is
//! no code path that puts user text into the query string.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use fides_shared::{AppError, AppResult};

fn internal(e: sqlx::Error) -> AppError {
    AppError::Internal(e.to_string())
}

/// Cap on rule nesting — bounds recursion against a hostile deeply-nested rule.
const MAX_DEPTH: usize = 20;

#[derive(Clone, Copy)]
enum FieldType {
    Int,
    Uuid,
    Text,
    Timestamp,
}

/// Whitelisted field → (constant SQL column expression, value type).
/// Numeric balance fields COALESCE to 0 so customers without a balance row still match.
fn field_def(name: &str) -> Option<(&'static str, FieldType)> {
    use FieldType::*;
    Some(match name {
        "lifetime_earned" => ("COALESCE(b.lifetime_earned, 0)", Int),
        "lifetime_redeemed" => ("COALESCE(b.lifetime_redeemed, 0)", Int),
        "spendable_balance" => ("COALESCE(b.spendable_balance, 0)", Int),
        "locked_balance" => ("COALESCE(b.locked_balance, 0)", Int),
        "current_tier_id" => ("c.current_tier_id", Uuid),
        "status" => ("c.status", Text),
        "created_at" => ("c.created_at", Timestamp),
        _ => return None,
    })
}

/// Whitelisted comparison operator → constant SQL operator. `in` is handled separately.
fn op_sql(op: &str) -> Option<&'static str> {
    Some(match op {
        "eq" => "=",
        "neq" => "<>",
        "gt" => ">",
        "gte" => ">=",
        "lt" => "<",
        "lte" => "<=",
        _ => return None,
    })
}

/// Coerce a JSON value to the field's type and bind it as a parameter.
fn push_value(qb: &mut QueryBuilder<Postgres>, ft: FieldType, v: &Value) -> Result<(), String> {
    match ft {
        FieldType::Int => {
            let n = v.as_i64().ok_or("expected an integer value")?;
            qb.push_bind(n);
        }
        FieldType::Uuid => {
            let s = v.as_str().ok_or("expected a uuid string value")?;
            let u = Uuid::parse_str(s).map_err(|_| "invalid uuid value")?;
            qb.push_bind(u);
        }
        FieldType::Text => {
            let s = v.as_str().ok_or("expected a string value")?;
            qb.push_bind(s.to_string());
        }
        FieldType::Timestamp => {
            let s = v.as_str().ok_or("expected an rfc3339 timestamp string")?;
            let t = DateTime::parse_from_rfc3339(s)
                .map_err(|_| "invalid rfc3339 timestamp")?
                .with_timezone(&Utc);
            qb.push_bind(t);
        }
    }
    Ok(())
}

/// Compile one rule node into the query builder. Also serves as the validator
/// (run against a throwaway builder) — one code path, so validation can never
/// drift from compilation.
fn append_node(qb: &mut QueryBuilder<Postgres>, node: &Value, depth: usize) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("rule nesting too deep".into());
    }
    let obj = node.as_object().ok_or("rule node must be an object")?;

    if let Some(items) = obj.get("all") {
        return append_combinator(qb, items, " AND ", "TRUE", depth);
    }
    if let Some(items) = obj.get("any") {
        return append_combinator(qb, items, " OR ", "FALSE", depth);
    }
    if let Some(child) = obj.get("not") {
        qb.push("(NOT ");
        append_node(qb, child, depth + 1)?;
        qb.push(")");
        return Ok(());
    }

    // Leaf predicate.
    let field = obj
        .get("field")
        .and_then(Value::as_str)
        .ok_or("predicate missing string 'field'")?;
    let (col, ft) = field_def(field).ok_or_else(|| format!("unknown field: {field}"))?;
    let op = obj
        .get("op")
        .and_then(Value::as_str)
        .ok_or("predicate missing string 'op'")?;
    let value = obj.get("value").ok_or("predicate missing 'value'")?;

    if op == "in" {
        let arr = value.as_array().ok_or("'in' value must be an array")?;
        if arr.is_empty() {
            return Err("'in' value must be a non-empty array".into());
        }
        qb.push("(").push(col).push(" IN (");
        for (i, item) in arr.iter().enumerate() {
            if i > 0 {
                qb.push(", ");
            }
            push_value(qb, ft, item)?;
        }
        qb.push("))");
        return Ok(());
    }

    let sql_op = op_sql(op).ok_or_else(|| format!("unknown op: {op}"))?;
    qb.push("(").push(col).push(" ").push(sql_op).push(" ");
    push_value(qb, ft, value)?;
    qb.push(")");
    Ok(())
}

fn append_combinator(
    qb: &mut QueryBuilder<Postgres>,
    items: &Value,
    joiner: &str,
    empty: &str,
    depth: usize,
) -> Result<(), String> {
    let items = items.as_array().ok_or("combinator value must be an array")?;
    if items.is_empty() {
        qb.push(empty);
        return Ok(());
    }
    qb.push("(");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            qb.push(joiner);
        }
        append_node(qb, item, depth + 1)?;
    }
    qb.push(")");
    Ok(())
}

/// Validate a rule definition. Returns the human-readable reason on failure.
pub fn validate(def: &Value) -> Result<(), String> {
    let mut qb = QueryBuilder::<Postgres>::new("");
    append_node(&mut qb, def, 0)
}

/// Worker job: for every active dynamic segment, recompute membership and
/// reconcile `customer_segments` (add matches, drop non-matches). Returns the
/// number of membership rows changed. Runs as the BYPASSRLS worker role, so it
/// filters `tenant_id` explicitly.
pub async fn reconcile_segments(pool: &PgPool) -> AppResult<u64> {
    let segs: Vec<(Uuid, Uuid, Value)> =
        sqlx::query_as("SELECT id, tenant_id, definition FROM segments WHERE definition IS NOT NULL AND active")
            .fetch_all(pool)
            .await
            .map_err(internal)?;

    let mut changed: u64 = 0;
    for (segment_id, tenant_id, def) in segs {
        if validate(&def).is_err() {
            tracing::warn!(segment = %segment_id, "skipping segment with invalid definition");
            continue;
        }

        // Add matching customers.
        let mut ins = QueryBuilder::<Postgres>::new(
            "INSERT INTO customer_segments (tenant_id, segment_id, customer_id) SELECT ",
        );
        ins.push_bind(tenant_id).push(", ").push_bind(segment_id).push(
            ", c.id FROM customers c LEFT JOIN customer_balances b ON b.customer_id = c.id \
             WHERE c.tenant_id = ",
        );
        ins.push_bind(tenant_id).push(" AND ");
        append_node(&mut ins, &def, 0).map_err(AppError::Validation)?;
        ins.push(" ON CONFLICT (segment_id, customer_id) DO NOTHING");
        changed += ins.build().execute(pool).await.map_err(internal)?.rows_affected();

        // Drop customers that no longer match.
        let mut del =
            QueryBuilder::<Postgres>::new("DELETE FROM customer_segments cs WHERE cs.segment_id = ");
        del.push_bind(segment_id).push(
            " AND cs.customer_id NOT IN (SELECT c.id FROM customers c \
             LEFT JOIN customer_balances b ON b.customer_id = c.id WHERE c.tenant_id = ",
        );
        del.push_bind(tenant_id).push(" AND ");
        append_node(&mut del, &def, 0).map_err(AppError::Validation)?;
        del.push(")");
        changed += del.build().execute(pool).await.map_err(internal)?.rows_affected();
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sql(def: &Value) -> Result<String, String> {
        let mut qb = QueryBuilder::<Postgres>::new("");
        append_node(&mut qb, def, 0)?;
        Ok(qb.sql().to_string())
    }

    #[test]
    fn compiles_and_parametrizes_values() {
        let def = json!({
            "all": [
                { "field": "lifetime_earned", "op": "gte", "value": 1000 },
                { "any": [
                    { "field": "status", "op": "eq", "value": "ACTIVE'; DROP TABLE customers;--" },
                    { "not": { "field": "current_tier_id", "op": "eq",
                               "value": "00000000-0000-0000-0000-000000000000" } }
                ]}
            ]
        });
        let s = sql(&def).unwrap();
        assert!(s.contains("COALESCE(b.lifetime_earned, 0) >="));
        assert!(s.contains("c.status ="));
        assert!(s.contains(" AND ") && s.contains(" OR ") && s.contains("(NOT "));
        // The hostile value is bound, never inlined.
        assert!(!s.contains("DROP"));
        assert!(s.contains("$1") && s.contains("$2") && s.contains("$3"));
    }

    #[test]
    fn rejects_bad_rules() {
        assert!(validate(&json!({ "field": "evil", "op": "eq", "value": 1 })).is_err());
        assert!(validate(&json!({ "field": "status", "op": "hack", "value": "x" })).is_err());
        assert!(validate(&json!({ "field": "status", "op": "in", "value": [] })).is_err());
        assert!(validate(&json!({ "field": "lifetime_earned", "op": "eq", "value": "nan" })).is_err());
        assert!(validate(&json!("not-an-object")).is_err());
        // Depth guard.
        let mut deep = json!({ "field": "status", "op": "eq", "value": "ACTIVE" });
        for _ in 0..MAX_DEPTH + 2 {
            deep = json!({ "not": deep });
        }
        assert!(validate(&deep).is_err());
    }

    #[test]
    fn accepts_in_and_combinators() {
        let def = json!({ "field": "status", "op": "in", "value": ["ACTIVE", "ANONYMIZED"] });
        let s = sql(&def).unwrap();
        assert!(s.contains("c.status IN ("));
        assert!(s.contains("$1") && s.contains("$2"));
        assert!(validate(&json!({ "all": [] })).is_ok()); // empty AND → TRUE
    }
}
