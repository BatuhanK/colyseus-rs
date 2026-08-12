//! Room listings and the matchmaking driver.
//!
//! The driver stores [`RoomListing`]s — the public, queryable view of a room —
//! and answers matchmaking queries. The default implementation is in-memory;
//! the API is deliberately small so a Redis/SQL-backed driver can be added
//! without touching the rest of the framework.

use std::cmp::Ordering;

use dashmap::DashMap;
use serde::Serialize;
use serde_json::{Map, Value};

/// The public, matchmaking-visible state of a room.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomListing {
    pub room_id: String,
    pub name: String,
    pub process_id: String,
    pub clients: u32,
    /// `None` means unlimited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_clients: Option<u32>,
    pub locked: bool,
    #[serde(rename = "private")]
    pub is_private: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    pub created_at: u64,
    /// Extra filter fields (from `filter_by`), flattened into the listing JSON.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl RoomListing {
    /// Serialize to a flat JSON object used for condition matching and sorting.
    fn to_flat_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// Sort options: `(field, direction)` pairs. Direction `1` = ascending, `-1` = descending.
/// Fields support dot notation (e.g. `metadata.region`).
pub type SortOptions = Vec<(String, i32)>;

/// Filter conditions: field paths (dot notation supported) → expected value.
pub type Conditions = Map<String, Value>;

/// Resolve a dot-notation path against a JSON value.
fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    for part in path.split('.') {
        cur = cur.get(part)?;
    }
    Some(cur)
}

/// Numeric-aware equality (so `1` matches `1.0`).
fn value_eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => false,
    }
}

fn value_cmp(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    match (a, b) {
        (Some(Value::Number(x)), Some(Value::Number(y))) => x
            .as_f64()
            .partial_cmp(&y.as_f64())
            .unwrap_or(Ordering::Equal),
        (Some(Value::String(x)), Some(Value::String(y))) => x.cmp(y),
        (Some(Value::Bool(x)), Some(Value::Bool(y))) => x.cmp(y),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

/// In-memory matchmaking driver.
#[derive(Default)]
pub struct LocalDriver {
    listings: DashMap<String, RoomListing>,
}

impl LocalDriver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, room_id: &str) -> Option<RoomListing> {
        self.listings.get(room_id).map(|l| l.clone())
    }

    pub fn insert(&self, listing: RoomListing) {
        self.listings.insert(listing.room_id.clone(), listing);
    }

    pub fn update_by_id(&self, room_id: &str, f: impl FnOnce(&mut RoomListing)) -> bool {
        if let Some(mut l) = self.listings.get_mut(room_id) {
            f(&mut l);
            true
        } else {
            false
        }
    }

    pub fn remove(&self, room_id: &str) -> bool {
        self.listings.remove(room_id).is_some()
    }

    pub fn len(&self) -> usize {
        self.listings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.listings.is_empty()
    }

    pub fn all(&self) -> Vec<RoomListing> {
        self.listings.iter().map(|l| l.clone()).collect()
    }

    pub fn clear(&self) {
        self.listings.clear();
    }

    /// Query listings by conditions, with optional sorting.
    pub fn query(&self, conditions: &Conditions, sort: Option<&SortOptions>) -> Vec<RoomListing> {
        let mut out: Vec<RoomListing> = self
            .listings
            .iter()
            .filter(|l| {
                let flat = l.to_flat_value();
                conditions.iter().all(|(key, expected)| {
                    resolve_path(&flat, key).is_some_and(|actual| value_eq(actual, expected))
                })
            })
            .map(|l| l.clone())
            .collect();

        if let Some(sort) = sort {
            let flats: Vec<Value> = out.iter().map(|l| l.to_flat_value()).collect();
            let mut indexed: Vec<(usize, RoomListing)> = out.drain(..).enumerate().collect();
            indexed.sort_by(|(ia, _), (ib, _)| {
                for (field, dir) in sort {
                    let ord = value_cmp(
                        resolve_path(&flats[*ia], field),
                        resolve_path(&flats[*ib], field),
                    );
                    let ord = if *dir < 0 { ord.reverse() } else { ord };
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });
            out = indexed.into_iter().map(|(_, l)| l).collect();
        }

        out
    }

    pub fn find_one(&self, conditions: &Conditions, sort: Option<&SortOptions>) -> Option<RoomListing> {
        self.query(conditions, sort).into_iter().next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn listing(id: &str, clients: u32, locked: bool) -> RoomListing {
        RoomListing {
            room_id: id.into(),
            name: "game".into(),
            process_id: "p1".into(),
            clients,
            max_clients: Some(4),
            locked,
            is_private: false,
            metadata: Some(json!({"region": "eu"})),
            created_at: 0,
            extra: Map::new(),
        }
    }

    #[test]
    fn query_filters_and_sorts() {
        let d = LocalDriver::new();
        d.insert(listing("a", 3, false));
        d.insert(listing("b", 1, false));
        d.insert(listing("c", 2, true));

        let mut cond = Conditions::new();
        cond.insert("name".into(), json!("game"));
        cond.insert("locked".into(), json!(false));
        let res = d.query(&cond, Some(&vec![("clients".into(), 1)]));
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].room_id, "b");

        let mut cond = Conditions::new();
        cond.insert("metadata.region".into(), json!("eu"));
        assert_eq!(d.query(&cond, None).len(), 3);
    }
}
