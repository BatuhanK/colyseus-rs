//! The matchmaker: room type registry, room creation, seat reservations,
//! and lobby events.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::sync::{broadcast, oneshot, Mutex};

use crate::actor::{spawn_restored_room, spawn_room, RoomEvent, RoomHandle};
use crate::driver::{
    parse_filter, Condition, Conditions, Driver, LocalDriver, Op, RoomListing, RoomQuery,
    RoomQueryResult, SortOptions,
};
use crate::error::{codes, Result, ServerError};
use crate::presence::{LocalPresence, Presence};
use crate::room::{Room, RoomContext};
use crate::snapshot::{PersistenceConfig, PersistenceHandle, SnapshotStore, SnapshotWriter};
use crate::utils::generate_id;

const RESERVE_SEAT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const JOIN_OR_CREATE_RETRIES: usize = 3;
/// Extra attempts `join` / `join_by_id` make after a stale listing (room
/// disposing between `find_one` and `reserve_seat`).
const JOIN_RETRIES: usize = 2;
/// How long a seat reservation is replayed for a duplicate `Idempotency-Key`.
const IDEMPOTENCY_TTL: Duration = Duration::from_secs(30);
/// Upper bound of cached idempotent reservations (oldest are evicted).
const IDEMPOTENCY_CAP: usize = 1024;

/// Authentication context of an incoming matchmaking request.
#[derive(Debug, Clone, Default)]
pub struct AuthContext {
    /// Bearer token from the `Authorization` header, if present.
    pub token: Option<String>,
    pub headers: HashMap<String, String>,
    pub ip: Option<String>,
}

/// Events emitted for lobby-style listing subscriptions.
#[derive(Debug, Clone)]
pub enum MatchmakerEvent {
    RoomCreated(RoomListing),
    RoomUpdated(RoomListing),
    RoomRemoved(String),
}

/// A custom matchmaking predicate: `(listing, request options) -> keep?`.
/// Runs after the field conditions when finding a room to join.
pub type MatchFilterFn = dyn Fn(&RoomListing, &Value) -> bool + Send + Sync;

/// A custom matchmaking comparator; overrides `sort_by` at match time.
pub type MatchSortFn =
    dyn Fn(&RoomListing, &RoomListing) -> Ordering + Send + Sync;

/// A registered room type, returned by [`crate::Server::define`].
pub struct RegisteredHandler {
    name: String,
    factory: Arc<dyn Fn() -> Box<dyn Room> + Send + Sync>,
    filter_by: Vec<String>,
    sort_by: SortOptions,
    default_options: Option<Value>,
    internal: bool,
    /// Whether rooms of this type are persisted/restored. Default `true`.
    persistent: bool,
    /// Option fields forming a uniqueness key: `create_room` reuses a live
    /// room with the same key instead of creating a duplicate.
    unique_by: Vec<String>,
    /// Strict `filter_by` semantics (see [`RegisteredHandler::strict_filter_fields`]).
    strict_filter: bool,
    match_filter_fn: Option<Arc<MatchFilterFn>>,
    match_sort_fn: Option<Arc<MatchSortFn>>,
}

impl RegisteredHandler {
    pub(crate) fn new<R, F>(name: &str, factory: F) -> Self
    where
        R: Room,
        F: Fn() -> R + Send + Sync + 'static,
    {
        RegisteredHandler {
            name: name.to_string(),
            factory: Arc::new(move || Box::new(factory())),
            filter_by: Vec::new(),
            sort_by: Vec::new(),
            default_options: None,
            internal: false,
            persistent: true,
            unique_by: Vec::new(),
            strict_filter: false,
            match_filter_fn: None,
            match_sort_fn: None,
        }
    }

    /// Mark this room type as internal: the public matchmaking HTTP API
    /// rejects `create` / `joinOrCreate` for it — instances can only be
    /// created server-side via [`MatchMaker::create_room`]. Clients may
    /// still `join` / `joinById` (e.g. a global lobby/chat room).
    pub fn internal(&mut self) -> &mut Self {
        self.internal = true;
        self
    }

    pub fn is_internal(&self) -> bool {
        self.internal
    }

    /// Whether rooms of this type are persisted to snapshots. Mark bootstrap
    /// / global rooms (e.g. a chat lobby recreated at startup) as
    /// non-persistent so they are neither saved nor restored.
    pub fn persistent(&mut self, persistent: bool) -> &mut Self {
        self.persistent = persistent;
        self
    }

    pub fn is_persistent(&self) -> bool {
        self.persistent
    }

    /// Which client option fields are used to filter rooms during matchmaking.
    /// The fields are also exposed on the room listing.
    pub fn filter_by(&mut self, fields: &[&str]) -> &mut Self {
        self.filter_by = fields.iter().map(|s| s.to_string()).collect();
        self
    }

    /// How candidate rooms are sorted during matchmaking.
    /// `(field, direction)` — direction `1` ascending, `-1` descending.
    pub fn sort_by(&mut self, sort: &[(&str, i32)]) -> &mut Self {
        self.sort_by = sort
            .iter()
            .map(|(f, d)| (f.to_string(), *d))
            .collect();
        self
    }

    /// Default options merged into (and overridden by) client-provided options.
    pub fn default_options(&mut self, options: Value) -> &mut Self {
        self.default_options = Some(options);
        self
    }

    /// Option fields forming a uniqueness key (e.g. `["slug"]`).
    ///
    /// When set, [`MatchMaker::create_room`] checks — under the per-key
    /// creation lock — for a live room whose listing carries the same values
    /// and returns it (`CreateRoomOutcome { created: false, .. }`) instead of
    /// creating a duplicate. Retried or double-submitted creations thus stay
    /// idempotent. Unique fields are also exposed on the room listing (like
    /// `filter_by` fields) so the engine can look them up.
    pub fn unique_by(&mut self, fields: &[&str]) -> &mut Self {
        self.unique_by = fields.iter().map(|s| s.to_string()).collect();
        self
    }

    /// How requests that omit a `filter_by` field behave during matchmaking.
    ///
    /// - `false` (default, Colyseus-compatible): the missing field is a
    ///   wildcard — `joinOrCreate({})` can match a room created with any
    ///   value of that field.
    /// - `true` (strict): the missing field only matches rooms that are also
    ///   missing it, and the creation lock key covers *all* filter fields —
    ///   so `joinOrCreate({})` can never cross-join a room created with
    ///   `{ "mode": "a" }`.
    pub fn strict_filter_fields(&mut self, strict: bool) -> &mut Self {
        self.strict_filter = strict;
        self
    }

    /// A custom predicate run after the field conditions when finding a room
    /// to join: `(listing, request options) -> bool`. Listings for which it
    /// returns `false` are skipped.
    pub fn match_filter_fn(
        &mut self,
        f: impl Fn(&RoomListing, &Value) -> bool + Send + Sync + 'static,
    ) -> &mut Self {
        self.match_filter_fn = Some(Arc::new(f));
        self
    }

    /// A custom comparator used to pick among candidate rooms at match time.
    /// Overrides `sort_by`.
    pub fn match_sort_fn(
        &mut self,
        f: impl Fn(&RoomListing, &RoomListing) -> Ordering + Send + Sync + 'static,
    ) -> &mut Self {
        self.match_sort_fn = Some(Arc::new(f));
        self
    }

    /// Build matchmaking conditions from client options.
    fn match_conditions(&self, options: &Value) -> Vec<(String, Condition)> {
        let mut conditions = vec![
            ("name".into(), Condition::eq(json!(self.name))),
            ("locked".into(), Condition::eq(json!(false))),
            ("private".into(), Condition::eq(json!(false))),
        ];
        for field in &self.filter_by {
            match options.get(field) {
                Some(v) => conditions.push((field.clone(), Condition::eq(v.clone()))),
                None if self.strict_filter => conditions.push((
                    field.clone(),
                    Condition { op: Op::NotExists, value: None },
                )),
                None => {}
            }
        }
        conditions
    }

    /// The uniqueness lookup: room type name + every `unique_by` field
    /// (fields missing from the options match rooms also missing them).
    fn unique_conditions(&self, options: &Value) -> Vec<(String, Condition)> {
        let mut conditions = vec![("name".into(), Condition::eq(json!(self.name)))];
        for field in &self.unique_by {
            match options.get(field) {
                Some(v) => conditions.push((field.clone(), Condition::eq(v.clone()))),
                None => conditions.push((
                    field.clone(),
                    Condition { op: Op::NotExists, value: None },
                )),
            }
        }
        conditions
    }

    /// Extract `filter_by` (and `unique_by`) fields from options to embed in
    /// the listing.
    fn filter_extra(&self, options: &Value) -> Map<String, Value> {
        let mut extra = Map::new();
        for field in self.filter_by.iter().chain(self.unique_by.iter()) {
            if let Some(v) = options.get(field) {
                extra.insert(field.clone(), v.clone());
            }
        }
        extra
    }

    /// Machine-readable description for `GET /admin/api/schema`. Note that
    /// `maxClients` is a per-room runtime setting, not a registration knob,
    /// so it is not part of the schema.
    pub(crate) fn schema(&self) -> Value {
        let mut schema = json!({
            "name": self.name,
            "filterBy": self.filter_by,
            "uniqueBy": self.unique_by,
            "sortBy": self.sort_by,
            "strictFilterFields": self.strict_filter,
            "internal": self.internal,
            "persistent": self.persistent,
        });
        if let Some(options) = &self.default_options {
            schema["defaultOptions"] = options.clone();
        }
        schema
    }

    fn merged_options(&self, options: Value) -> Value {
        match (&self.default_options, options) {
            (Some(defaults), Value::Object(mut opts)) => {
                for (k, v) in defaults.as_object().cloned().unwrap_or_default() {
                    opts.entry(k).or_insert(v);
                }
                Value::Object(opts)
            }
            (Some(defaults), Value::Null) => defaults.clone(),
            (_, opts) => opts,
        }
    }
}

/// Cheap per-room-type status counts (see [`MatchMaker::room_stats`]).
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomStats {
    /// Rooms of the type (or all rooms when no name filter).
    pub total: usize,
    /// Not locked and not at capacity.
    pub open: usize,
    /// Open with at least one client — i.e. waiting for an opponent.
    pub waiting: usize,
    /// Locked or at capacity.
    pub full: usize,
    pub locked: usize,
    pub private: usize,
}

/// The result of a successful matchmaking call: a reserved seat.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeatReservation {
    pub room: RoomListing,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconnection_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_address: Option<String>,
    pub process_id: String,
}

/// The result of a server-side room creation.
#[derive(Debug, Clone)]
pub struct CreateRoomOutcome {
    pub listing: RoomListing,
    /// `false` when an existing live room with the same `unique_by` key was
    /// reused instead of creating a new one.
    pub created: bool,
}

struct MatchMakerInner {
    handlers: RwLock<HashMap<String, Arc<RegisteredHandler>>>,
    rooms: Arc<DashMap<String, RoomHandle>>,
    driver: Arc<dyn Driver>,
    presence: Arc<dyn Presence>,
    process_id: String,
    public_address: Option<String>,
    shutting_down: AtomicBool,
    lobby_tx: broadcast::Sender<MatchmakerEvent>,
    /// Prevents concurrent creation of rooms with identical filter criteria.
    create_locks: DashMap<String, Arc<Mutex<()>>>,
    /// Seat reservations cached by `Idempotency-Key` for replay.
    idempotency: DashMap<String, (Instant, SeatReservation)>,
    /// Snapshot persistence (when configured on the server).
    store: Option<Arc<dyn SnapshotStore>>,
    writer: Option<SnapshotWriter>,
    persistence: Option<PersistenceConfig>,
}

/// The matchmaker. Cloneable handle, safe to share across tasks.
#[derive(Clone)]
pub struct MatchMaker {
    inner: Arc<MatchMakerInner>,
}

impl MatchMaker {
    pub(crate) fn new(
        presence: Option<Arc<dyn Presence>>,
        driver: Option<Arc<dyn Driver>>,
        public_address: Option<String>,
        persistence: Option<PersistenceConfig>,
    ) -> Self {
        MatchMaker {
            inner: Arc::new(MatchMakerInner {
                handlers: RwLock::new(HashMap::new()),
                rooms: Arc::new(DashMap::new()),
                driver: driver.unwrap_or_else(|| Arc::new(LocalDriver::new())),
                presence: presence.unwrap_or_else(|| LocalPresence::new()),
                process_id: generate_id(),
                public_address,
                shutting_down: AtomicBool::new(false),
                lobby_tx: broadcast::channel(256).0,
                create_locks: DashMap::new(),
                idempotency: DashMap::new(),
                store: persistence.as_ref().map(|p| p.store.clone()),
                writer: persistence
                    .as_ref()
                    .map(|p| SnapshotWriter::spawn(p.store.clone())),
                persistence,
            }),
        }
    }

    pub(crate) fn register(&self, handler: RegisteredHandler) {
        self.inner
            .handlers
            .write()
            .insert(handler.name.clone(), Arc::new(handler));
    }

    pub fn remove_room_type(&self, name: &str) {
        self.inner.handlers.write().remove(name);
    }

    /// All registered room types (for the admin schema endpoint).
    pub(crate) fn handlers(&self) -> Vec<Arc<RegisteredHandler>> {
        self.inner.handlers.read().values().cloned().collect()
    }

    pub fn handler(&self, name: &str) -> Result<Arc<RegisteredHandler>> {
        self.inner
            .handlers
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| ServerError::no_handler(name))
    }

    pub fn process_id(&self) -> &str {
        &self.inner.process_id
    }

    /// The persistence handle handed to every room (when configured).
    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        self.inner.persistence.as_ref().map(|c| PersistenceHandle {
            config: c.clone(),
            writer: self.inner.writer.clone().expect("snapshot writer"),
        })
    }

    pub fn driver(&self) -> Arc<dyn Driver> {
        self.inner.driver.clone()
    }

    pub fn presence(&self) -> Arc<dyn Presence> {
        self.inner.presence.clone()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(AtomicOrdering::SeqCst)
    }

    /// Subscribe to lobby events (room created / updated / removed).
    pub fn subscribe(&self) -> broadcast::Receiver<MatchmakerEvent> {
        self.inner.lobby_tx.subscribe()
    }

    pub(crate) fn room_handle(&self, room_id: &str) -> Option<RoomHandle> {
        self.inner.rooms.get(room_id).map(|h| h.clone())
    }

    // ------------------------------------------------------------------
    // Matchmaking operations
    // ------------------------------------------------------------------

    /// Find an available room or create one, then reserve a seat.
    pub async fn join_or_create(
        &self,
        room_name: &str,
        options: Value,
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        self.join_or_create_inner(room_name, options, &[], auth).await
    }

    /// [`Self::join_or_create`] with an additional operator-style filter
    /// (see [`Self::parse_match_filter`]) applied on top of the room type's
    /// `filter_by` conditions — e.g. `clients < 2`.
    pub async fn join_or_create_with_filter(
        &self,
        room_name: &str,
        options: Value,
        filter: &[(String, Condition)],
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        self.check_fields(room_name, filter.iter().map(|(f, _)| f.as_str()), "filter")?;
        self.join_or_create_inner(room_name, options, filter, auth).await
    }

    async fn join_or_create_inner(
        &self,
        room_name: &str,
        options: Value,
        filter: &[(String, Condition)],
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        let handler = self.handler(room_name)?;
        let mut last_err: Option<ServerError> = None;

        for _ in 0..JOIN_OR_CREATE_RETRIES {
            let conditions = handler.match_conditions(&options);
            let listing = self.find_match(&handler, &conditions, filter, &options);

            let listing = match listing {
                Some(l) => l,
                None => {
                    // serialize concurrent creations for identical criteria
                    let lock_key = format!("{room_name}:{}", conditions_key(&conditions));
                    let lock = self.create_lock(&lock_key);
                    let listing = {
                        let _guard = lock.lock().await;

                        // re-check: another request may have created the room meanwhile
                        match self.find_match(&handler, &conditions, filter, &options) {
                            Some(l) => Ok(l),
                            None => self
                                .create_room_inner(room_name, handler.merged_options(options.clone()))
                                .await
                                .map(|outcome| outcome.listing),
                        }
                    };
                    self.release_create_lock(&lock_key, &lock);
                    listing?
                }
            };

            match self.reserve_seat(&listing, options.clone(), auth.clone()).await {
                Ok(reservation) => return Ok(reservation),
                Err(e) if e.is_seat_reservation_failure() => {
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ServerError::new(codes::MATCHMAKE_UNHANDLED, "join_or_create failed")
        }))
    }

    /// Always create a new room, then reserve a seat.
    pub async fn create(
        &self,
        room_name: &str,
        options: Value,
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        let handler = self.handler(room_name)?;
        let listing = self
            .create_room_inner(room_name, handler.merged_options(options.clone()))
            .await?
            .listing;
        self.reserve_seat(&listing, options, auth).await
    }

    /// Join an existing room matching the criteria; error when none found.
    pub async fn join(
        &self,
        room_name: &str,
        options: Value,
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        self.join_inner(room_name, options, &[], auth).await
    }

    /// [`Self::join`] with an additional operator-style filter (see
    /// [`Self::parse_match_filter`]).
    pub async fn join_with_filter(
        &self,
        room_name: &str,
        options: Value,
        filter: &[(String, Condition)],
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        self.check_fields(room_name, filter.iter().map(|(f, _)| f.as_str()), "filter")?;
        self.join_inner(room_name, options, filter, auth).await
    }

    async fn join_inner(
        &self,
        room_name: &str,
        options: Value,
        filter: &[(String, Condition)],
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        let handler = self.handler(room_name)?;
        let mut last_err: Option<ServerError> = None;

        for _ in 0..=JOIN_RETRIES {
            let conditions = handler.match_conditions(&options);
            let Some(listing) = self.find_match(&handler, &conditions, filter, &options) else {
                return Err(ServerError::new(
                    codes::MATCHMAKE_INVALID_CRITERIA,
                    "no rooms found with the provided criteria",
                ));
            };
            match self.reserve_seat(&listing, options.clone(), auth.clone()).await {
                Ok(reservation) => return Ok(reservation),
                Err(e) if e.is_seat_reservation_failure() => {
                    // stale listing (room disposing) — re-fetch and retry
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_err.unwrap_or_else(ServerError::seat_expired))
    }

    /// Join a specific room by id.
    pub async fn join_by_id(
        &self,
        room_id: &str,
        options: Value,
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        let mut last_err: Option<ServerError> = None;

        for _ in 0..=JOIN_RETRIES {
            let listing = self
                .inner
                .driver
                .get(room_id)
                .ok_or_else(|| ServerError::room_not_found(room_id))?;
            if listing.locked {
                return Err(ServerError::new(
                    codes::MATCHMAKE_INVALID_ROOM_ID,
                    format!("room \"{room_id}\" is locked"),
                ));
            }
            match self.reserve_seat(&listing, options.clone(), auth.clone()).await {
                Ok(reservation) => return Ok(reservation),
                Err(e) if e.is_seat_reservation_failure() => {
                    // stale listing (room disposing) — re-fetch and retry
                    last_err = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_err.unwrap_or_else(ServerError::seat_expired))
    }

    /// Rejoin a room with a reconnection token obtained earlier.
    pub async fn reconnect(&self, room_id: &str, reconnection_token: &str) -> Result<SeatReservation> {
        let Some(listing) = self.inner.driver.get(room_id) else {
            return Err(ServerError::new(
                codes::MATCHMAKE_INVALID_ROOM_ID,
                format!("room \"{room_id}\" has been disposed. Did you forget allow_reconnection()?"),
            ));
        };
        let Some(handle) = self.room_handle(room_id) else {
            return Err(ServerError::room_not_found(room_id));
        };

        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(RoomEvent::CheckReconnection {
                token: reconnection_token.to_string(),
                respond: tx,
            })
            .map_err(|_| ServerError::room_not_found(room_id))?;

        let session_id = tokio::time::timeout(RESERVE_SEAT_RPC_TIMEOUT, rx)
            .await
            .ok()
            .and_then(|r| r.ok())
            .flatten();

        match session_id {
            Some(session_id) => Ok(SeatReservation {
                room: listing,
                session_id,
                reconnection_token: Some(reconnection_token.to_string()),
                public_address: self.inner.public_address.clone(),
                process_id: self.inner.process_id.clone(),
            }),
            None => Err(ServerError::new(
                codes::MATCHMAKE_EXPIRED,
                "reconnection token invalid or expired",
            )),
        }
    }

    /// Query room listings.
    pub fn query(&self, room_name: Option<&str>, mut conditions: Conditions) -> Vec<RoomListing> {
        if let Some(name) = room_name {
            conditions.insert("name".into(), json!(name));
        }
        self.inner.driver.query(&conditions, None)
    }

    /// Fields every room type may be filtered/sorted on.
    pub const CORE_FILTER_FIELDS: &'static [&'static str] = &[
        "name",
        "clients",
        "maxClients",
        "locked",
        "private",
        "createdAt",
        "processId",
    ];

    /// Is `field` filterable/sortable for the given room type? Core listing
    /// fields, `metadata.*` paths, and the room type's own `filter_by` fields
    /// are allowed. `roomId` is always rejected — it would collide with the
    /// engine's generated room id in the flattened listing.
    fn allowed_field(&self, room_name: &str, field: &str) -> bool {
        if field == "roomId" {
            return false;
        }
        if Self::CORE_FILTER_FIELDS.contains(&field) || field.starts_with("metadata.") {
            return true;
        }
        self.handler(room_name)
            .map(|h| h.filter_by.iter().any(|f| f == field))
            .unwrap_or(false)
    }

    /// Validate that every field is filterable for this room type.
    fn check_fields<'a>(
        &self,
        room_name: &str,
        fields: impl Iterator<Item = &'a str>,
        what: &str,
    ) -> Result<()> {
        for field in fields {
            if !self.allowed_field(room_name, field) {
                return Err(ServerError::new(
                    codes::MATCHMAKE_INVALID_CRITERIA,
                    format!("unknown {what} field \"{field}\""),
                ));
            }
        }
        Ok(())
    }

    /// Parse and validate a matchmaking `filter` JSON object (operator-style,
    /// like [`RoomQuery`]: `{ "clients": { "lt": 2 }, "slug": "abc" }`) for a
    /// room type. Fields are restricted to the same whitelist as
    /// [`Self::query_rooms`]. The result can be passed to
    /// [`Self::join_or_create_with_filter`] / [`Self::join_with_filter`].
    pub fn parse_match_filter(
        &self,
        room_name: &str,
        filter: &Value,
    ) -> Result<Vec<(String, Condition)>> {
        // validate the room type exists first, so typos fail loudly
        self.handler(room_name)?;
        let conditions = parse_filter(filter)
            .map_err(|m| ServerError::new(codes::MATCHMAKE_INVALID_CRITERIA, m))?;
        self.check_fields(room_name, conditions.iter().map(|(f, _)| f.as_str()), "filter")?;
        Ok(conditions)
    }

    /// Run a parameterized [`RoomQuery`], validating that every filter/sort
    /// field is allowed (see [`Self::parse_match_filter`]).
    pub fn query_rooms(
        &self,
        room_name: Option<&str>,
        mut query: RoomQuery,
    ) -> Result<RoomQueryResult> {
        if let Some(name) = room_name {
            query.name = Some(name.to_string());
        }

        if let Some(name) = query.name.clone() {
            self.check_fields(&name, query.conditions.iter().map(|(f, _)| f.as_str()), "filter")?;
            self.check_fields(&name, query.sort.iter().map(|(f, _)| f.as_str()), "sort")?;
        } else {
            // no room type: only core fields and metadata.* are filterable
            let allowed = |field: &str| {
                field != "roomId"
                    && (Self::CORE_FILTER_FIELDS.contains(&field) || field.starts_with("metadata."))
            };
            for (field, _) in &query.conditions {
                if !allowed(field) {
                    return Err(ServerError::new(
                        codes::MATCHMAKE_INVALID_CRITERIA,
                        format!("unknown filter field \"{field}\""),
                    ));
                }
            }
            for (field, _) in &query.sort {
                if !allowed(field) {
                    return Err(ServerError::new(
                        codes::MATCHMAKE_INVALID_CRITERIA,
                        format!("unknown sort field \"{field}\""),
                    ));
                }
            }
        }

        Ok(self.inner.driver.query_rooms(&query))
    }

    /// Cheap per-room-type status counts (open / waiting / full / locked …).
    pub fn room_stats(&self, room_name: Option<&str>) -> RoomStats {
        let mut stats = RoomStats::default();
        for listing in self.inner.driver.all() {
            if let Some(name) = room_name {
                if listing.name != name {
                    continue;
                }
            }
            stats.total += 1;
            if listing.is_private {
                stats.private += 1;
            }
            if listing.locked {
                stats.locked += 1;
            }
            let at_capacity = listing
                .max_clients
                .is_some_and(|max| listing.clients >= max);
            if listing.locked || at_capacity {
                stats.full += 1;
            } else {
                stats.open += 1;
                if listing.clients >= 1 {
                    stats.waiting += 1;
                }
            }
        }
        stats
    }

    /// Gracefully shut down: dispose all rooms and stop matchmaking.
    pub async fn shutdown(&self) {
        if self.inner.shutting_down.swap(true, AtomicOrdering::SeqCst) {
            return;
        }
        for entry in self.inner.rooms.iter() {
            let _ = entry.tx.send(RoomEvent::Shutdown);
        }
        // wait for rooms to dispose (bounded)
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !self.inner.rooms.is_empty() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.inner.driver.clear();
        // make sure every queued snapshot write hits disk before we exit
        if let Some(writer) = &self.inner.writer {
            writer.flush();
        }
    }

    /// Server-side: create a room without reserving a seat — for bootstrap
    /// rooms, server-initiated matches, background jobs, etc.
    /// (Clients use the matchmaking HTTP API, which always reserves a seat.)
    ///
    /// When the room type declares [`RegisteredHandler::unique_by`] and a
    /// live room with the same key already exists, that room is returned with
    /// `created: false` instead of creating a duplicate.
    pub async fn create_room(&self, room_name: &str, options: Value) -> Result<CreateRoomOutcome> {
        self.create_room_inner(room_name, options).await
    }

    /// Restore a single persisted room by id.
    ///
    /// Returns `Ok(None)` when the snapshot was skipped (its room type is
    /// marked non-persistent, so the stale snapshot is dropped).
    pub async fn restore_room(&self, room_id: &str) -> Result<Option<RoomListing>> {
        let Some(store) = self.inner.store.clone() else {
            return Err(ServerError::new(
                codes::APPLICATION_ERROR,
                "no snapshot store configured",
            ));
        };
        let Some(bytes) = store.load(room_id) else {
            return Err(ServerError::room_not_found(room_id));
        };
        let snapshot = match crate::snapshot::decode_snapshot(&bytes) {
            Ok(s) => s,
            Err(e) => {
                store.quarantine(room_id, &e);
                return Err(ServerError::new(
                    codes::APPLICATION_ERROR,
                    format!("corrupt snapshot for {room_id}: {e}"),
                ));
            }
        };

        if self.inner.rooms.contains_key(&snapshot.room_id) {
            return Err(ServerError::new(
                codes::APPLICATION_ERROR,
                format!("room {} is already running", snapshot.room_id),
            ));
        }

        let handler = match self.handler(&snapshot.room_name) {
            Ok(h) => h,
            Err(_) => {
                tracing::warn!(
                    "snapshot for {room_id} references unregistered room type {}; skipping",
                    snapshot.room_name
                );
                return Err(ServerError::no_handler(&snapshot.room_name));
            }
        };

        if !handler.is_persistent() {
            let _ = store.delete(room_id);
            tracing::info!(
                "dropping snapshot for non-persistent room type {} ({room_id})",
                snapshot.room_name
            );
            return Ok(None);
        }

        let room = (handler.factory)();
        let ctx = RoomContext::new(
            snapshot.room_id.clone(),
            snapshot.room_name.clone(),
            self.inner.process_id.clone(),
            self.inner.driver.clone(),
            self.inner.presence.clone(),
            self.inner.lobby_tx.clone(),
            snapshot.filter_extra.clone(),
            self.persistence_handle(),
        );

        let rooms = self.inner.rooms.clone();
        let cleanup_room_id = snapshot.room_id.clone();
        let (handle, listing) = spawn_restored_room(
            room,
            ctx,
            snapshot,
            Box::new(move || {
                rooms.remove(&cleanup_room_id);
            }),
        )
        .await?;

        self.inner.rooms.insert(listing.room_id.clone(), handle);
        self.inner.driver.insert(listing.clone());
        let _ = self
            .inner
            .lobby_tx
            .send(MatchmakerEvent::RoomCreated(listing.clone()));
        tracing::info!(
            "restored room {} (roomId: {}) from snapshot",
            listing.name,
            listing.room_id
        );
        Ok(Some(listing))
    }

    /// Restore all persisted rooms. Call before accepting traffic (the server
    /// does this automatically in [`crate::Server::listen`]).
    ///
    /// Returns the room ids that were successfully restored.
    pub async fn restore_all(&self) -> Vec<String> {
        let Some(store) = self.inner.store.clone() else {
            return Vec::new();
        };
        let ids = store.list_room_ids();
        tracing::info!("restoring {} room(s) from snapshots", ids.len());
        let mut restored = Vec::new();
        for room_id in ids {
            match self.restore_room(&room_id).await {
                Ok(Some(_)) => restored.push(room_id),
                Ok(None) => {} // skipped (non-persistent room type)
                Err(e) => {
                    tracing::warn!("failed to restore room {room_id}: {e}");
                }
            }
        }
        restored
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    async fn create_room_inner(&self, room_name: &str, options: Value) -> Result<CreateRoomOutcome> {
        let handler = self.handler(room_name)?;

        // unique_by: dedupe duplicate/concurrent creations on the unique key
        if !handler.unique_by.is_empty() {
            let key_conditions = handler.unique_conditions(&options);
            let lock_key = format!("{room_name}:unique:{}", conditions_key(&key_conditions));
            let lock = self.create_lock(&lock_key);
            let outcome = {
                let _guard = lock.lock().await;
                match self.find_live(&key_conditions) {
                    Some(listing) => Ok(CreateRoomOutcome { listing, created: false }),
                    None => self
                        .spawn_new_room(room_name, &handler, options.clone())
                        .await
                        .map(|listing| CreateRoomOutcome { listing, created: true }),
                }
            };
            self.release_create_lock(&lock_key, &lock);
            return outcome;
        }

        Ok(CreateRoomOutcome {
            listing: self.spawn_new_room(room_name, &handler, options).await?,
            created: true,
        })
    }

    async fn spawn_new_room(
        &self,
        room_name: &str,
        handler: &RegisteredHandler,
        options: Value,
    ) -> Result<RoomListing> {
        let room = (handler.factory)();
        let room_id = generate_id();
        let filter_extra = handler.filter_extra(&options);

        let ctx = RoomContext::new(
            room_id.clone(),
            room_name.to_string(),
            self.inner.process_id.clone(),
            self.inner.driver.clone(),
            self.inner.presence.clone(),
            self.inner.lobby_tx.clone(),
            filter_extra,
            if handler.is_persistent() {
                self.persistence_handle()
            } else {
                None
            },
        );

        let rooms = self.inner.rooms.clone();
        let cleanup_room_id = room_id.clone();
        let (handle, listing) = spawn_room(room, ctx, options, Box::new(move || {
            rooms.remove(&cleanup_room_id);
        }))
        .await
        .map_err(|e| {
            ServerError::new(
                if e.code == 0 { codes::MATCHMAKE_UNHANDLED } else { e.code },
                e.message,
            )
        })?;

        self.inner.rooms.insert(room_id.clone(), handle);
        self.inner.driver.insert(listing.clone());
        let _ = self
            .inner
            .lobby_tx
            .send(MatchmakerEvent::RoomCreated(listing.clone()));

        tracing::info!("room created: \"{room_name}\" (roomId: {room_id})");
        Ok(listing)
    }

    /// Find a live room matching `conditions`; stale listings (room gone or
    /// mid-disposal) are removed and skipped.
    fn find_live(&self, conditions: &[(String, Condition)]) -> Option<RoomListing> {
        let query = RoomQuery {
            name: None,
            conditions: conditions.to_vec(),
            sort: Vec::new(),
            limit: None,
            offset: 0,
            count: false,
        };
        for listing in self.inner.driver.query_rooms(&query).items {
            if self.inner.rooms.contains_key(&listing.room_id) {
                return Some(listing);
            }
            self.inner.driver.remove(&listing.room_id);
        }
        None
    }

    /// Pick a room to join: field conditions + the request's operator filter,
    /// then the room type's custom predicate, ordered by its custom
    /// comparator (falling back to `sort_by`).
    fn find_match(
        &self,
        handler: &RegisteredHandler,
        conditions: &[(String, Condition)],
        filter: &[(String, Condition)],
        options: &Value,
    ) -> Option<RoomListing> {
        let mut query_conditions = conditions.to_vec();
        query_conditions.extend(filter.iter().cloned());
        let query = RoomQuery {
            name: None, // the name is already part of `conditions`
            conditions: query_conditions,
            sort: if handler.match_sort_fn.is_some() {
                Vec::new()
            } else {
                handler.sort_by.clone()
            },
            limit: None,
            offset: 0,
            count: false,
        };
        let mut candidates = self.inner.driver.query_rooms(&query).items;
        if let Some(predicate) = &handler.match_filter_fn {
            candidates.retain(|l| predicate(l, options));
        }
        if let Some(compare) = &handler.match_sort_fn {
            candidates.sort_by(|a, b| compare(a, b));
        }
        candidates.into_iter().next()
    }

    /// The per-criteria creation lock for `key`.
    fn create_lock(&self, key: &str) -> Arc<Mutex<()>> {
        self.inner
            .create_locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Drop the map entry once no other task references the lock, so
    /// `create_locks` doesn't grow without bound.
    fn release_create_lock(&self, key: &str, lock: &Arc<Mutex<()>>) {
        if Arc::strong_count(lock) <= 2 {
            self.inner
                .create_locks
                .remove_if(key, |_, v| Arc::strong_count(v) <= 2);
        }
    }

    /// Replay a cached reservation for a duplicate `Idempotency-Key`.
    pub(crate) fn idempotency_get(&self, key: &str) -> Option<SeatReservation> {
        let entry = self.inner.idempotency.get(key)?;
        if entry.0.elapsed() > IDEMPOTENCY_TTL {
            let key = entry.key().clone();
            drop(entry);
            self.inner.idempotency.remove(&key);
            return None;
        }
        Some(entry.1.clone())
    }

    /// Cache a reservation for `Idempotency-Key` replays. Expired entries
    /// are swept on insert; the map is bounded (oldest evicted).
    pub(crate) fn idempotency_put(&self, key: String, reservation: SeatReservation) {
        self.inner
            .idempotency
            .retain(|_, (at, _)| at.elapsed() <= IDEMPOTENCY_TTL);
        while self.inner.idempotency.len() >= IDEMPOTENCY_CAP {
            let oldest = self
                .inner
                .idempotency
                .iter()
                .max_by_key(|e| e.0.elapsed())
                .map(|e| e.key().clone());
            match oldest {
                Some(k) => {
                    self.inner.idempotency.remove(&k);
                }
                None => break,
            }
        }
        self.inner
            .idempotency
            .insert(key, (Instant::now(), reservation));
    }

    async fn reserve_seat(
        &self,
        listing: &RoomListing,
        options: Value,
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        let Some(handle) = self.room_handle(&listing.room_id) else {
            // the room actor is gone — drop the stale listing too
            self.inner.driver.remove(&listing.room_id);
            return Err(ServerError::seat_expired());
        };

        let session_id = generate_id();
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(RoomEvent::ReserveSeat {
                session_id: session_id.clone(),
                options,
                auth,
                respond: tx,
            })
            .map_err(|_| {
                // the room actor is gone — drop the stale listing too
                self.inner.driver.remove(&listing.room_id);
                ServerError::seat_expired()
            })?;

        tokio::time::timeout(RESERVE_SEAT_RPC_TIMEOUT, rx)
            .await
            .map_err(|_| ServerError::new(codes::MATCHMAKE_UNHANDLED, "seat reservation timed out"))?
            .map_err(|_| ServerError::seat_expired())??;

        let room = self
            .inner
            .driver
            .get(&listing.room_id)
            .ok_or_else(ServerError::seat_expired)?;
        Ok(SeatReservation {
            room,
            session_id,
            reconnection_token: None,
            public_address: self.inner.public_address.clone(),
            process_id: self.inner.process_id.clone(),
        })
    }
}

/// A stable key for a set of matchmaking conditions.
fn conditions_key(conditions: &[(String, Condition)]) -> String {
    let mut entries: Vec<String> = conditions
        .iter()
        .filter(|(k, _)| k != "locked" && k != "private" && k != "name")
        .map(|(k, c)| format!("{k}={}", condition_key(c)))
        .collect();
    entries.sort();
    entries.join("&")
}

fn condition_key(condition: &Condition) -> String {
    match &condition.op {
        Op::NotExists => "<absent>".to_string(),
        Op::Exists => "<present>".to_string(),
        _ => condition
            .value
            .as_ref()
            .map_or_else(|| "null".to_string(), |v| v.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::RoomEvent;
    use crate::error::close_codes;

    struct TestRoom;

    #[async_trait::async_trait]
    impl Room for TestRoom {}

    fn mm_with(configure: impl FnOnce(&mut RegisteredHandler)) -> MatchMaker {
        let mm = MatchMaker::new(None, None, None, None);
        let mut handler = RegisteredHandler::new::<TestRoom, _>("game", || TestRoom);
        configure(&mut handler);
        mm.register(handler);
        mm
    }

    fn mm_plain() -> MatchMaker {
        mm_with(|_| {})
    }

    #[tokio::test]
    async fn create_locks_are_released_after_use() {
        let mm = mm_plain();
        mm.join_or_create("game", json!({}), AuthContext::default())
            .await
            .unwrap();
        // the per-criteria lock entry is dropped once unreferenced
        assert!(mm.inner.create_locks.is_empty());
    }

    #[tokio::test]
    async fn concurrent_join_or_create_same_criteria_seats_everyone_in_one_room() {
        let mm = mm_plain();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let mm = mm.clone();
            tasks.push(tokio::spawn(async move {
                mm.join_or_create("game", json!({}), AuthContext::default()).await
            }));
        }
        let mut room_ids = std::collections::HashSet::new();
        let mut session_ids = std::collections::HashSet::new();
        for task in tasks {
            let reservation = task.await.unwrap().unwrap();
            room_ids.insert(reservation.room.room_id.clone());
            session_ids.insert(reservation.session_id.clone());
        }
        assert_eq!(room_ids.len(), 1, "all joiners must land in one room");
        assert_eq!(session_ids.len(), 8, "every joiner gets its own seat");
        let room_id = room_ids.into_iter().next().unwrap();
        assert_eq!(mm.driver().get(&room_id).unwrap().clients, 8);
        assert!(mm.inner.create_locks.is_empty());
    }

    #[tokio::test]
    async fn unique_by_dedupes_concurrent_creations() {
        let mm = mm_with(|h| {
            h.unique_by(&["slug"]);
        });
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let mm = mm.clone();
            tasks.push(tokio::spawn(async move {
                mm.create_room("game", json!({ "slug": "abc" })).await
            }));
        }
        let mut created = 0;
        let mut room_ids = std::collections::HashSet::new();
        for task in tasks {
            let outcome = task.await.unwrap().unwrap();
            if outcome.created {
                created += 1;
            }
            room_ids.insert(outcome.listing.room_id);
        }
        assert_eq!(created, 1, "exactly one task creates the room");
        assert_eq!(room_ids.len(), 1);

        // sequential re-creation reuses the live room as well
        let again = mm.create_room("game", json!({ "slug": "abc" })).await.unwrap();
        assert!(!again.created);
        // …but a different key creates a new room
        let other = mm.create_room("game", json!({ "slug": "xyz" })).await.unwrap();
        assert!(other.created);
        assert!(mm.inner.create_locks.is_empty());
    }

    #[tokio::test]
    async fn wildcard_filter_matches_missing_fields_strict_does_not() {
        // default (Colyseus-compatible) wildcard semantics: a request without
        // `mode` matches a room created with any `mode`
        let mm = mm_with(|h| {
            h.filter_by(&["mode"]);
        });
        let created = mm.create_room("game", json!({ "mode": "a" })).await.unwrap();
        let joined = mm
            .join_or_create("game", json!({}), AuthContext::default())
            .await
            .unwrap();
        assert_eq!(joined.room.room_id, created.listing.room_id);

        // strict semantics: a request without `mode` only matches rooms also
        // missing it — so it creates a new room, then finds it again
        let mm = mm_with(|h| {
            h.filter_by(&["mode"]).strict_filter_fields(true);
        });
        let created = mm.create_room("game", json!({ "mode": "a" })).await.unwrap();
        let joined = mm
            .join_or_create("game", json!({}), AuthContext::default())
            .await
            .unwrap();
        assert_ne!(joined.room.room_id, created.listing.room_id);
        let rejoined = mm
            .join_or_create("game", json!({}), AuthContext::default())
            .await
            .unwrap();
        assert_eq!(rejoined.room.room_id, joined.room.room_id);

        // and an exact `mode` request never cross-joins the modeless room
        let modal = mm
            .join_or_create("game", json!({ "mode": "a" }), AuthContext::default())
            .await
            .unwrap();
        assert_eq!(modal.room.room_id, created.listing.room_id);
    }

    #[tokio::test]
    async fn match_hooks_customize_filter_and_sort() {
        let mm = mm_with(|h| {
            h.match_filter_fn(|listing, _| listing.clients >= 1)
                .match_sort_fn(|a, b| b.clients.cmp(&a.clients));
        });
        let empty = mm.create_room("game", json!({})).await.unwrap();
        let busy = mm.create_room("game", json!({})).await.unwrap();
        // seat someone in `busy` so its listing shows clients = 1
        mm.join_by_id(&busy.listing.room_id, json!({}), AuthContext::default())
            .await
            .unwrap();

        // the filter predicate skips the empty room; the comparator would
        // pick the busiest anyway
        let joined = mm
            .join_or_create("game", json!({}), AuthContext::default())
            .await
            .unwrap();
        assert_eq!(joined.room.room_id, busy.listing.room_id);
        assert_ne!(joined.room.room_id, empty.listing.room_id);
    }

    #[tokio::test]
    async fn join_by_id_against_disposing_room_errors_cleanly() {
        let mm = mm_plain();
        let outcome = mm.create_room("game", json!({})).await.unwrap();
        let room_id = outcome.listing.room_id.clone();

        let handle = mm.room_handle(&room_id).unwrap();
        handle
            .tx
            .send(RoomEvent::Dispose {
                code: close_codes::CONSENTED,
            })
            .unwrap();

        // wait for the disposal to settle
        for _ in 0..100 {
            if mm.room_handle(&room_id).is_none() && mm.driver().get(&room_id).is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let err = mm
            .join_by_id(&room_id, json!({}), AuthContext::default())
            .await
            .unwrap_err();
        assert!(
            err.code == codes::MATCHMAKE_INVALID_ROOM_ID || err.code == codes::MATCHMAKE_EXPIRED,
            "expected a clean matchmaking error, got {err}"
        );
        // the stale listing is gone
        assert!(mm.driver().get(&room_id).is_none());
    }
}

