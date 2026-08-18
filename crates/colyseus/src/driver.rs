//! Room listings and the matchmaking driver.
//!
//! The driver stores [`RoomListing`]s — the public, queryable view of a room —
//! and answers matchmaking queries. The default implementation is in-memory;
//! the API is deliberately small so a Redis/SQL-backed driver can be added
//! without touching the rest of the framework.
//!
//! # Query API
//!
//! Beyond exact-equality conditions (the classic Colyseus `filter_by` style),
//! [`LocalDriver::query_rooms`] accepts a [`RoomQuery`] with comparison
//! operators (`gt/gte/lt/lte/ne/in/exists`), sorting, and pagination — the
//! surface used by `GET /rooms/{name}`, the admin `/admin/api/rooms`
//! endpoint, and the `AdminClient` SDK.

use std::cmp::Ordering;
use std::collections::HashMap;

use dashmap::DashMap;
use serde::Serialize;
use serde_json::{json, Map, Value};

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
///
/// Legacy exact-equality form — see [`RoomQuery`] for the operator-aware API.
pub type Conditions = Map<String, Value>;

// ---------------------------------------------------------------------
// Query API
// ---------------------------------------------------------------------

/// Comparison operators for a single [`RoomQuery`] condition.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    /// Field equals the expected value (numeric-aware; string/number coercion).
    Eq,
    /// Field does not equal the expected value.
    Ne,
    /// Field > expected (numbers, strings, booleans).
    Gt,
    /// Field >= expected.
    Gte,
    /// Field < expected.
    Lt,
    /// Field <= expected.
    Lte,
    /// Field is one of the listed values.
    In(Vec<Value>),
    /// Field is present.
    Exists,
    /// Field is absent.
    NotExists,
}

/// A single filter: `field (path, dot notation) → operator + expected value`.
#[derive(Debug, Clone)]
pub struct Condition {
    pub op: Op,
    /// Expected value for `Eq/Ne/Gt/Gte/Lt/Lte` (ignored by `In/Exists/NotExists`).
    pub value: Option<Value>,
}

impl Condition {
    pub fn eq(value: Value) -> Self {
        Condition { op: Op::Eq, value: Some(value) }
    }
}

/// A parameterized room listing query.
///
/// Built either programmatically or parsed from HTTP query params via
/// [`RoomQuery::from_params`]:
///
/// ```text
/// name=tictactoe
/// slug=abc123                (any filter_by field)
/// clients=1 | clients.gte=1 | clients.lt=2
/// locked.exists=false
/// sort=createdAt:desc,clients:asc
/// limit=25&offset=0
/// count=true                 (only `total` is computed)
/// ```
#[derive(Debug, Clone, Default)]
pub struct RoomQuery {
    /// Restrict to a single room type name.
    pub name: Option<String>,
    /// Field filters (field → operator + expected value).
    pub conditions: Vec<(String, Condition)>,
    /// Sort keys: `(field, direction)` — `1` asc, `-1` desc.
    pub sort: SortOptions,
    pub limit: Option<usize>,
    pub offset: usize,
    /// When true, only `total` is meaningful (items are empty).
    pub count: bool,
}

/// A page of [`RoomQuery`] results.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomQueryResult {
    pub items: Vec<RoomListing>,
    /// Number of rooms matching the query (before pagination).
    pub total: usize,
    /// Echo of the requested limit (`None` = unlimited).
    pub limit: Option<usize>,
    pub offset: usize,
    /// Offset of the next page, `None` when there is none.
    pub next_offset: Option<usize>,
}

impl RoomQuery {
    /// Parse from HTTP query params (axum `Query<HashMap<String, String>>`).
    ///
    /// Field suffixes select the operator: `clients=2` (eq), `clients.gte=1`,
    /// `slug=abc`, `mode.in=a,b`, `locked.exists=false`. Values are parsed as
    /// numbers/booleans when they look like one, otherwise strings.
    pub fn from_params(params: &HashMap<String, String>) -> Result<RoomQuery, String> {
        let mut q = RoomQuery::default();
        for (key, raw) in params {
            match key.as_str() {
                "name" => q.name = Some(raw.clone()),
                "sort" => q.sort = parse_sort(raw)?,
                "limit" => {
                    q.limit = Some(
                        raw.parse::<usize>()
                            .map_err(|_| format!("invalid limit \"{raw}\""))?,
                    )
                }
                "offset" => {
                    q.offset = raw
                        .parse::<usize>()
                        .map_err(|_| format!("invalid offset \"{raw}\""))?
                }
                "count" => {
                    q.count = raw
                        .parse::<bool>()
                        .map_err(|_| format!("invalid count \"{raw}\""))?
                }
                _ => {
                    let (field, condition) = parse_condition(key, raw)?;
                    q.conditions.push((field, condition));
                }
            }
        }
        Ok(q)
    }

    /// Chainable builder for programmatic use.
    pub fn builder() -> RoomQueryBuilder {
        RoomQueryBuilder(RoomQuery::default())
    }
}

/// Convenience builder over [`RoomQuery`].
#[derive(Debug, Clone, Default)]
pub struct RoomQueryBuilder(pub RoomQuery);

impl RoomQueryBuilder {
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.0.name = Some(name.into());
        self
    }
    pub fn filter(mut self, field: impl Into<String>, value: Value) -> Self {
        self.0.conditions.push((field.into(), Condition::eq(value)));
        self
    }
    pub fn condition(mut self, field: impl Into<String>, condition: Condition) -> Self {
        self.0.conditions.push((field.into(), condition));
        self
    }
    pub fn sort(mut self, field: impl Into<String>, desc: bool) -> Self {
        self.0.sort.push((field.into(), if desc { -1 } else { 1 }));
        self
    }
    pub fn limit(mut self, limit: usize) -> Self {
        self.0.limit = Some(limit);
        self
    }
    pub fn offset(mut self, offset: usize) -> Self {
        self.0.offset = offset;
        self
    }
    pub fn count_only(mut self) -> Self {
        self.0.count = true;
        self
    }
    pub fn build(self) -> RoomQuery {
        self.0
    }
}

fn parse_sort(raw: &str) -> Result<SortOptions, String> {
    let mut sort = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (field, dir) = match part.rsplit_once(':') {
            Some((f, "asc")) => (f.to_string(), 1),
            Some((f, "desc")) => (f.to_string(), -1),
            Some((f, "1")) => (f.to_string(), 1),
            Some((f, "-1")) => (f.to_string(), -1),
            _ => (part.to_string(), 1),
        };
        if field.is_empty() {
            return Err(format!("invalid sort key \"{part}\""));
        }
        sort.push((field, dir));
    }
    Ok(sort)
}

/// `field` (eq) · `field.op` for gt/gte/lt/lte/ne/in/exists/notExists.
fn parse_condition(key: &str, raw: &str) -> Result<(String, Condition), String> {
    const OPS: [&str; 9] = ["eq", "ne", "gt", "gte", "lt", "lte", "in", "exists", "notExists"];

    let (field, suffix) = match key.rsplit_once('.') {
        Some((f, s)) if OPS.contains(&s) => (f.to_string(), Some(s)),
        _ => (key.to_string(), None),
    };
    if field.is_empty() {
        return Err(format!("invalid filter field \"{key}\""));
    }

    let condition = match suffix {
        None | Some("eq") => Condition { op: Op::Eq, value: Some(parse_value(raw)) },
        Some("ne") => Condition { op: Op::Ne, value: Some(parse_value(raw)) },
        Some("gt") => Condition { op: Op::Gt, value: Some(parse_value(raw)) },
        Some("gte") => Condition { op: Op::Gte, value: Some(parse_value(raw)) },
        Some("lt") => Condition { op: Op::Lt, value: Some(parse_value(raw)) },
        Some("lte") => Condition { op: Op::Lte, value: Some(parse_value(raw)) },
        Some("in") => Condition {
            op: Op::In(raw.split(',').map(parse_value).collect()),
            value: None,
        },
        Some("exists") | Some("notExists") => {
            let expect = raw
                .parse::<bool>()
                .map_err(|_| format!("invalid exists value \"{raw}\""))?;
            Condition {
                op: if expect { Op::Exists } else { Op::NotExists },
                value: None,
            }
        }
        _ => unreachable!("suffix validated above"),
    };
    Ok((field, condition))
}

/// Parse a raw query value as number / boolean / null / string.
fn parse_value(raw: &str) -> Value {
    if let Ok(n) = raw.parse::<i64>() {
        return json!(n);
    }
    if let Ok(f) = raw.parse::<f64>() {
        return json!(f);
    }
    match raw {
        "true" => json!(true),
        "false" => json!(false),
        "null" => Value::Null,
        _ => json!(raw),
    }
}

// ---------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------

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

    /// Query listings with the full [`RoomQuery`] (operators, sort, pagination).
    pub fn query_rooms(&self, query: &RoomQuery) -> RoomQueryResult {
        let mut matched: Vec<RoomListing> = self
            .listings
            .iter()
            .filter(|l| {
                let flat = l.to_flat_value();
                if let Some(name) = &query.name {
                    if flat.get("name").and_then(|v| v.as_str()) != Some(name.as_str()) {
                        return false;
                    }
                }
                for (field, condition) in &query.conditions {
                    if !matches_condition(resolve_path(&flat, field), condition) {
                        return false;
                    }
                }
                true
            })
            .map(|l| l.clone())
            .collect();

        if !query.sort.is_empty() {
            sort_listings(&mut matched, &query.sort);
        }

        let total = matched.len();
        if query.count {
            return RoomQueryResult {
                items: Vec::new(),
                total,
                limit: query.limit,
                offset: query.offset,
                next_offset: None,
            };
        }

        let offset = query.offset.min(total);
        let limit = query.limit.unwrap_or(usize::MAX);
        let items: Vec<RoomListing> = matched.into_iter().skip(offset).take(limit).collect();
        let next_offset = if offset + items.len() < total {
            Some(offset + items.len())
        } else {
            None
        };

        RoomQueryResult {
            items,
            total,
            limit: query.limit,
            offset,
            next_offset,
        }
    }

    /// Legacy exact-equality query (kept for compatibility — delegates to
    /// [`LocalDriver::query_rooms`]).
    pub fn query(&self, conditions: &Conditions, sort: Option<&SortOptions>) -> Vec<RoomListing> {
        let room_query = RoomQuery {
            name: None,
            conditions: conditions
                .iter()
                .map(|(field, value)| {
                    (field.clone(), Condition { op: Op::Eq, value: Some(value.clone()) })
                })
                .collect(),
            sort: sort.cloned().unwrap_or_default(),
            limit: None,
            offset: 0,
            count: false,
        };
        self.query_rooms(&room_query).items
    }

    pub fn find_one(
        &self,
        conditions: &Conditions,
        sort: Option<&SortOptions>,
    ) -> Option<RoomListing> {
        self.query(conditions, sort).into_iter().next()
    }
}

// ---------------------------------------------------------------------
// Matching helpers
// ---------------------------------------------------------------------

/// Resolve a dot-notation path against a JSON value.
fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    for part in path.split('.') {
        cur = cur.get(part)?;
    }
    Some(cur)
}

/// Numeric-aware equality with string/number coercion (so `slug=123` matches
/// the string field `"123"`, and `1` matches `1.0`).
fn value_eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::String(x), Value::Number(y)) => x == &y.to_string(),
        (Value::Number(x), Value::String(y)) => &x.to_string() == y,
        _ => false,
    }
}

/// Orderable comparison for numbers/strings/bools; `None` when incomparable.
fn value_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64().partial_cmp(&y.as_f64()),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Does `actual` satisfy `condition`?
fn matches_condition(actual: Option<&Value>, condition: &Condition) -> bool {
    match &condition.op {
        Op::Eq => {
            actual.is_some_and(|a| value_eq(a, condition.value.as_ref().unwrap_or(&Value::Null)))
        }
        Op::Ne => {
            !actual.is_some_and(|a| value_eq(a, condition.value.as_ref().unwrap_or(&Value::Null)))
        }
        Op::Gt | Op::Gte | Op::Lt | Op::Lte => {
            let Some(expected) = condition.value.as_ref() else {
                return false;
            };
            let Some(ord) = actual.and_then(|a| value_cmp(a, expected)) else {
                return false;
            };
            match condition.op {
                Op::Gt => ord == Ordering::Greater,
                Op::Gte => ord != Ordering::Less,
                Op::Lt => ord == Ordering::Less,
                Op::Lte => ord != Ordering::Greater,
                _ => unreachable!(),
            }
        }
        Op::In(values) => actual.is_some_and(|a| values.iter().any(|v| value_eq(a, v))),
        Op::Exists => actual.is_some(),
        Op::NotExists => actual.is_none(),
    }
}

fn sort_listings(listings: &mut Vec<RoomListing>, sort: &SortOptions) {
    let flats: Vec<Value> = listings.iter().map(|l| l.to_flat_value()).collect();
    let mut indexed: Vec<(usize, RoomListing)> =
        std::mem::take(listings).into_iter().enumerate().collect();
    indexed.sort_by(|(ia, _), (ib, _)| {
        for (field, dir) in sort {
            let ord = value_cmp(
                resolve_path(&flats[*ia], field).unwrap_or(&Value::Null),
                resolve_path(&flats[*ib], field).unwrap_or(&Value::Null),
            )
            .unwrap_or(Ordering::Equal);
            let ord = if *dir < 0 { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
    *listings = indexed.into_iter().map(|(_, l)| l).collect();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn listing(id: &str, name: &str, clients: u32, locked: bool) -> RoomListing {
        let mut extra = Map::new();
        extra.insert("slug".into(), json!(format!("slug-{id}")));
        RoomListing {
            room_id: id.into(),
            name: name.into(),
            process_id: "p1".into(),
            clients,
            max_clients: Some(4),
            locked,
            is_private: false,
            metadata: Some(json!({"region": "eu"})),
            created_at: id.len() as u64,
            extra,
        }
    }

    fn driver() -> LocalDriver {
        let d = LocalDriver::new();
        d.insert(listing("a", "game", 3, false));
        d.insert(listing("b", "game", 1, false));
        d.insert(listing("c", "game", 2, true));
        d.insert(listing("d", "chat", 1, false));
        d
    }

    #[test]
    fn legacy_query_filters_and_sorts() {
        let d = driver();
        let mut cond = Conditions::new();
        cond.insert("name".into(), json!("game"));
        cond.insert("locked".into(), json!(false));
        let res = d.query(&cond, Some(&vec![("clients".into(), 1)]));
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].room_id, "b");
    }

    #[test]
    fn query_rooms_equality_and_range() {
        let d = driver();
        let q = RoomQuery::builder()
            .name("game")
            .filter("clients", json!(1))
            .build();
        let r = d.query_rooms(&q);
        assert_eq!(r.total, 1);
        assert_eq!(r.items[0].room_id, "b");

        let q = RoomQuery::builder()
            .name("game")
            .condition("clients", Condition { op: Op::Gte, value: Some(json!(2)) })
            .build();
        let r = d.query_rooms(&q);
        assert_eq!(r.total, 2);
        let mut ids: Vec<&str> = r.items.iter().map(|l| l.room_id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec!["a", "c"]);
    }

    #[test]
    fn query_rooms_ops_in_exists_not_exists() {
        let d = driver();
        let q = RoomQuery::builder()
            .name("game")
            .condition("clients", Condition { op: Op::In(vec![json!(1), json!(3)]), value: None })
            .build();
        assert_eq!(d.query_rooms(&q).total, 2);

        let q = RoomQuery::builder()
            .name("game")
            .condition("metadata.region", Condition { op: Op::Exists, value: None })
            .build();
        assert_eq!(d.query_rooms(&q).total, 3);

        let q = RoomQuery::builder()
            .name("game")
            .condition("metadata.rank", Condition { op: Op::NotExists, value: None })
            .build();
        assert_eq!(d.query_rooms(&q).total, 3);

        let q = RoomQuery::builder()
            .name("game")
            .condition("locked", Condition { op: Op::Ne, value: Some(json!(true)) })
            .build();
        assert_eq!(d.query_rooms(&q).total, 2);
    }

    #[test]
    fn query_rooms_string_number_coercion() {
        let d = LocalDriver::new();
        d.insert(listing("a", "game", 1, false));
        // numeric-looking value for a string filter field (slug-1 → slug=slug-1 is a string;
        // here we test `slug=slug-a` as string and numeric coercion via clients)
        let q = RoomQuery::builder().name("game").filter("clients", json!(1.0)).build();
        assert_eq!(d.query_rooms(&q).total, 1);
    }

    #[test]
    fn query_rooms_pagination_and_sort() {
        let d = driver();
        let q = RoomQuery::builder()
            .name("game")
            .sort("createdAt", true)
            .limit(2)
            .build();
        let r = d.query_rooms(&q);
        assert_eq!(r.total, 3);
        assert_eq!(r.items.len(), 2);
        assert_eq!(r.next_offset, Some(2));

        let q2 = RoomQuery { offset: 2, ..q };
        let r2 = d.query_rooms(&q2);
        assert_eq!(r2.items.len(), 1);
        assert_eq!(r2.next_offset, None);
    }

    #[test]
    fn query_rooms_count_only() {
        let d = driver();
        let q = RoomQuery::builder().name("game").count_only().build();
        let r = d.query_rooms(&q);
        assert_eq!(r.total, 3);
        assert!(r.items.is_empty());
    }

    #[test]
    fn from_params_parses_operators() {
        let mut p = HashMap::new();
        p.insert("name".into(), "game".into());
        p.insert("clients.gte".into(), "2".into());
        p.insert("slug.in".into(), "slug-a,slug-b".into());
        p.insert("locked.exists".into(), "false".into());
        p.insert("sort".into(), "createdAt:desc,clients:asc".into());
        p.insert("limit".into(), "5".into());
        let q = RoomQuery::from_params(&p).unwrap();
        assert_eq!(q.name.as_deref(), Some("game"));
        assert_eq!(q.conditions.len(), 3);
        assert_eq!(q.sort, vec![("createdAt".into(), -1), ("clients".into(), 1)]);
        assert_eq!(q.limit, Some(5));
        let ops = [
            q.conditions[0].1.op.clone(),
            q.conditions[1].1.op.clone(),
            q.conditions[2].1.op.clone(),
        ];
        assert!(ops.iter().any(|op| matches!(op, Op::Gte)));
        assert!(ops.iter().any(|op| matches!(op, Op::In(_))));
        assert!(ops.iter().any(|op| matches!(op, Op::NotExists)));
    }

    #[test]
    fn from_params_rejects_bad_values() {
        let mut p = HashMap::new();
        p.insert("limit".into(), "abc".into());
        assert!(RoomQuery::from_params(&p).is_err());
        let mut p = HashMap::new();
        p.insert("locked.exists".into(), "nope".into());
        assert!(RoomQuery::from_params(&p).is_err());
    }
}
