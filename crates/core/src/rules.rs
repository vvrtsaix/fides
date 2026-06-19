//! Schema-less rule condition evaluator (FR-2.3). A rule's JSONB `condition` is matched against
//! an event's JSONB `payload`. Kept pure so the matching logic is exhaustively unit-testable.
//!
//! Grammar (recursive):
//!   {}                                        → always matches
//!   { "all": [ <cond>, ... ] }                → every sub-condition matches
//!   { "any": [ <cond>, ... ] }                → at least one matches
//!   { "field": "a.b", "op": ">=", "value": V } → compare payload path `a.b` to V
//!
//! Operators: `==`, `!=`, `>`, `>=`, `<`, `<=`. Numbers compare numerically; strings/bools by
//! equality. A missing path or a type mismatch on an ordering operator is a non-match (never an
//! error) — events vary across integrations, so evaluation is total.

use serde_json::Value;

pub fn matches(condition: &Value, payload: &Value) -> bool {
    match condition {
        Value::Null => true,
        Value::Object(map) if map.is_empty() => true,
        Value::Object(map) => {
            if let Some(Value::Array(subs)) = map.get("all") {
                return subs.iter().all(|c| matches(c, payload));
            }
            if let Some(Value::Array(subs)) = map.get("any") {
                return subs.iter().any(|c| matches(c, payload));
            }
            match (map.get("field"), map.get("op"), map.get("value")) {
                (Some(Value::String(field)), Some(Value::String(op)), Some(expected)) => {
                    let actual = lookup(payload, field);
                    eval_op(op, actual, expected)
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// Resolve a dot-path (`"cart.total"`) into the payload.
fn lookup<'a>(payload: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = payload;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn eval_op(op: &str, actual: Option<&Value>, expected: &Value) -> bool {
    let actual = match actual {
        Some(v) => v,
        None => return false,
    };
    match op {
        "==" => actual == expected,
        "!=" => actual != expected,
        ">" | ">=" | "<" | "<=" => match (actual.as_f64(), expected.as_f64()) {
            (Some(a), Some(b)) => match op {
                ">" => a > b,
                ">=" => a >= b,
                "<" => a < b,
                _ => a <= b,
            },
            _ => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_condition_always_matches() {
        assert!(matches(&json!({}), &json!({"x": 1})));
        assert!(matches(&Value::Null, &json!({})));
    }

    #[test]
    fn comparisons() {
        let p = json!({"amount_cents": 250, "channel": "web"});
        assert!(matches(
            &json!({"field":"amount_cents","op":">=","value":100}),
            &p
        ));
        assert!(!matches(
            &json!({"field":"amount_cents","op":"<","value":100}),
            &p
        ));
        assert!(matches(
            &json!({"field":"channel","op":"==","value":"web"}),
            &p
        ));
        assert!(!matches(&json!({"field":"missing","op":">","value":1}), &p));
    }

    #[test]
    fn nested_and_dotpath() {
        let p = json!({"cart": {"total": 500}, "vip": true});
        let cond = json!({"all": [
            {"field":"cart.total","op":">=","value":300},
            {"field":"vip","op":"==","value":true}
        ]});
        assert!(matches(&cond, &p));
        let any = json!({"any": [
            {"field":"cart.total","op":">","value":9999},
            {"field":"vip","op":"==","value":true}
        ]});
        assert!(matches(&any, &p));
    }
}
