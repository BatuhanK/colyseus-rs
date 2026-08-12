//! JSON-Patch (RFC 6902) diff generation, tuned for game state.
//!
//! The stock `json_patch::diff` is positional: in a sliding-window list
//! (e.g. a capped chat/feed queue) every element shifts by one index, so a
//! single push produces O(n) replace ops. This module generates minimal ops
//! instead:
//!
//! - same-length arrays → per-index deep recursion (small, precise patches
//!   for arrays of entities like players)
//! - length-changing arrays → LCS-based diff, so queue shifts become
//!   `remove /list/0` + `add /list/-` (2 ops, not 100)
//! - huge arrays fall back to positional diffing to bound CPU

use serde_json::{Map, Value};

/// Max cells for the LCS dynamic table; bigger arrays use positional diff.
const LCS_MAX_CELLS: usize = 8192;

/// Compute the JSON-Patch ops transforming `old` into `new`.
pub fn diff(old: &Value, new: &Value) -> Vec<Value> {
    let mut ops = Vec::new();
    diff_value(old, new, String::new(), &mut ops);
    ops
}

fn escape(seg: &str) -> String {
    seg.replace('~', "~0").replace('/', "~1")
}

fn diff_value(old: &Value, new: &Value, path: String, ops: &mut Vec<Value>) {
    if old == new {
        return;
    }
    match (old, new) {
        (Value::Object(o), Value::Object(n)) => diff_object(o, n, path, ops),
        (Value::Array(o), Value::Array(n)) => diff_array(o, n, path, ops),
        _ => ops.push(op("replace", &path, Some(new.clone()))),
    }
}

fn diff_object(old: &Map<String, Value>, new: &Map<String, Value>, path: String, ops: &mut Vec<Value>) {
    for key in old.keys() {
        if !new.contains_key(key) {
            ops.push(op("remove", &format!("{path}/{}", escape(key)), None));
        }
    }
    for (key, nval) in new {
        let child = format!("{path}/{}", escape(key));
        match old.get(key) {
            None => ops.push(op("add", &child, Some(nval.clone()))),
            Some(oval) => diff_value(oval, nval, child, ops),
        }
    }
}

fn diff_array(old: &[Value], new: &[Value], path: String, ops: &mut Vec<Value>) {
    if old.len() == new.len() {
        // positional deep diff is optimal for in-place entity updates, but a
        // shift inside a fixed-length array (capped queue!) makes every
        // position differ — detect that and use the LCS path instead.
        let aligned = old.iter().zip(new.iter()).filter(|(o, n)| o == n).count();
        if aligned * 2 >= old.len() || old.len() <= 8 {
            for (i, (o, n)) in old.iter().zip(new.iter()).enumerate() {
                diff_value(o, n, format!("{path}/{i}"), ops);
            }
            return;
        }
        // fall through to LCS
    }

    if old.len() * new.len() > LCS_MAX_CELLS {
        return diff_array_positional(old, new, path, ops);
    }

    diff_array_lcs(old, new, path, ops);
}

/// Fallback for large arrays: recurse over the overlap, then truncate or append.
fn diff_array_positional(old: &[Value], new: &[Value], path: String, ops: &mut Vec<Value>) {
    let overlap = old.len().min(new.len());
    for i in 0..overlap {
        diff_value(&old[i], &new[i], format!("{path}/{i}"), ops);
    }
    // remove extras from the end (indices stay valid while shrinking)
    for _ in overlap..old.len() {
        ops.push(op("remove", &format!("{path}/{}", overlap), None));
    }
    for (j, item) in new.iter().enumerate().skip(overlap) {
        ops.push(op("add", &format!("{path}/{j}"), Some(item.clone())));
    }
}

/// LCS-based array diff: finds the longest common subsequence and emits
/// removes/adds around it. This is what turns queue shifts into 2 ops.
fn diff_array_lcs(old: &[Value], new: &[Value], path: String, ops: &mut Vec<Value>) {
    let (n, m) = (old.len(), new.len());

    // dp[i][j] = LCS length of old[i..] and new[j..]
    let mut dp = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i][j] = if old[i] == new[j] {
                dp[i + 1][j + 1] + 1
            } else {
                dp[i + 1][j].max(dp[i][j + 1])
            };
        }
    }

    // Walk the table. `cur` is the index in the *evolving* array (the one the
    // already-emitted ops have produced) that corresponds to old[i].
    let (mut i, mut j, mut cur) = (0usize, 0usize, 0usize);
    while i < n && j < m {
        if old[i] == new[j] {
            i += 1;
            j += 1;
            cur += 1;
        } else if dp[i + 1][j] >= dp[i][j + 1] {
            ops.push(op("remove", &format!("{path}/{cur}"), None));
            i += 1; // removal shifts the next element into `cur`
        } else {
            ops.push(op("add", &format!("{path}/{cur}"), Some(new[j].clone())));
            j += 1;
            cur += 1;
        }
    }
    while i < n {
        ops.push(op("remove", &format!("{path}/{cur}"), None));
        i += 1;
    }
    while j < m {
        ops.push(op("add", &format!("{path}/{cur}"), Some(new[j].clone())));
        j += 1;
        cur += 1;
    }
}

fn op(kind: &str, path: &str, value: Option<Value>) -> Value {
    let mut o = Map::new();
    o.insert("op".into(), Value::from(kind));
    o.insert("path".into(), Value::from(path));
    if let Some(v) = value {
        o.insert("value".into(), v);
    }
    Value::Object(o)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn apply(doc: &Value, ops: &[Value]) -> Value {
        let mut doc = doc.clone();
        let patch: json_patch::Patch =
            serde_json::from_value(Value::Array(ops.to_vec())).unwrap();
        json_patch::patch(&mut doc, &patch).expect("patch must apply");
        doc
    }

    fn roundtrip(old: Value, new: Value) -> Vec<Value> {
        let ops = diff(&old, &new);
        let result = apply(&old, &ops);
        assert_eq!(result, new, "patch did not produce target; ops: {ops:?}");
        ops
    }

    #[test]
    fn sliding_window_queue_is_two_ops() {
        let old: Value = (0..50).map(|i| json!({"id": i, "text": format!("m{i}")})).collect();
        let new: Value = (1..51).map(|i| json!({"id": i, "text": format!("m{i}")})).collect();
        let ops = roundtrip(json!({ "messages": old }), json!({ "messages": new }));
        assert_eq!(ops.len(), 2, "expected remove+add, got {ops:?}");
        assert_eq!(ops[0]["op"], "remove");
        assert_eq!(ops[0]["path"], "/messages/0");
        assert_eq!(ops[1]["op"], "add");
    }

    #[test]
    fn same_length_entity_array_deep_diffs() {
        let old = json!({"players": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]});
        let new = json!({"players": [{"x": 1, "y": 2}, {"x": 9, "y": 4}]});
        let ops = roundtrip(old, new);
        assert_eq!(ops, vec![json!({"op": "replace", "path": "/players/1/x", "value": 9})]);
    }

    #[test]
    fn object_key_add_remove() {
        roundtrip(json!({"a": 1, "b": 2}), json!({"b": 3, "c": 4}));
    }

    #[test]
    fn array_append_and_prepend() {
        roundtrip(json!([1, 2, 3]), json!([1, 2, 3, 4]));
        roundtrip(json!([1, 2, 3]), json!([0, 1, 2, 3]));
    }

    #[test]
    fn array_mixed_edits_and_shift() {
        let old: Value = (0..10).map(|i| json!({"id": i, "hp": 100})).collect();
        let mut new_items: Vec<Value> = (1..11).map(|i| json!({"id": i, "hp": 100})).collect();
        new_items[0]["hp"] = json!(50); // shifted + edited
        roundtrip(json!({"e": old}), json!({"e": Value::Array(new_items)}));
    }

    #[test]
    fn truncation_and_growth() {
        roundtrip(json!([1, 2, 3, 4, 5]), json!([1, 2]));
        roundtrip(json!([1]), json!([1, 2, 3, 4]));
    }

    #[test]
    fn nested_paths_escaped() {
        let ops = roundtrip(json!({"a/b": {"~x": 1}}), json!({"a/b": {"~x": 2}}));
        assert_eq!(ops[0]["path"], "/a~1b/~0x");
    }

    #[test]
    fn no_change_no_ops() {
        assert!(diff(&json!({"a": [1, 2, {"b": 3}]}), &json!({"a": [1, 2, {"b": 3}]})).is_empty());
    }
}
