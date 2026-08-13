//! The matchmaker: room type registry, room creation, seat reservations,
//! and lobby events.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::sync::{broadcast, oneshot, Mutex};

use crate::actor::{spawn_restored_room, spawn_room, RoomEvent, RoomHandle};
use crate::driver::{Conditions, LocalDriver, RoomListing, SortOptions};
use crate::error::{codes, Result, ServerError};
use crate::presence::{LocalPresence, Presence};
use crate::room::{Room, RoomContext};
use crate::snapshot::{PersistenceConfig, PersistenceHandle, SnapshotStore, SnapshotWriter};
use crate::utils::generate_id;

const RESERVE_SEAT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const JOIN_OR_CREATE_RETRIES: usize = 3;

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

    /// Build matchmaking conditions from client options.
    fn match_conditions(&self, options: &Value) -> Conditions {
        let mut conditions = Conditions::new();
        conditions.insert("name".into(), json!(self.name));
        conditions.insert("locked".into(), json!(false));
        conditions.insert("private".into(), json!(false));
        for field in &self.filter_by {
            if let Some(v) = options.get(field) {
                conditions.insert(field.clone(), v.clone());
            }
        }
        conditions
    }

    /// Extract `filter_by` fields from options to embed in the listing.
    fn filter_extra(&self, options: &Value) -> Map<String, Value> {
        let mut extra = Map::new();
        for field in &self.filter_by {
            if let Some(v) = options.get(field) {
                extra.insert(field.clone(), v.clone());
            }
        }
        extra
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

struct MatchMakerInner {
    handlers: RwLock<HashMap<String, Arc<RegisteredHandler>>>,
    rooms: Arc<DashMap<String, RoomHandle>>,
    driver: Arc<LocalDriver>,
    presence: Arc<dyn Presence>,
    process_id: String,
    public_address: Option<String>,
    shutting_down: AtomicBool,
    lobby_tx: broadcast::Sender<MatchmakerEvent>,
    /// Prevents concurrent creation of rooms with identical filter criteria.
    create_locks: DashMap<String, Arc<Mutex<()>>>,
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
        public_address: Option<String>,
        persistence: Option<PersistenceConfig>,
    ) -> Self {
        MatchMaker {
            inner: Arc::new(MatchMakerInner {
                handlers: RwLock::new(HashMap::new()),
                rooms: Arc::new(DashMap::new()),
                driver: Arc::new(LocalDriver::new()),
                presence: presence.unwrap_or_else(|| LocalPresence::new()),
                process_id: generate_id(),
                public_address,
                shutting_down: AtomicBool::new(false),
                lobby_tx: broadcast::channel(256).0,
                create_locks: DashMap::new(),
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

    pub fn driver(&self) -> Arc<LocalDriver> {
        self.inner.driver.clone()
    }

    pub fn presence(&self) -> Arc<dyn Presence> {
        self.inner.presence.clone()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::SeqCst)
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
        let handler = self.handler(room_name)?;
        let mut last_err: Option<ServerError> = None;

        for _ in 0..JOIN_OR_CREATE_RETRIES {
            let conditions = handler.match_conditions(&options);
            let listing = self.inner.driver.find_one(&conditions, Some(&handler.sort_by));

            let listing = match listing {
                Some(l) => l,
                None => {
                    // serialize concurrent creations for identical criteria
                    let lock_key = format!("{room_name}:{}", conditions_key(&conditions));
                    let lock = self
                        .inner
                        .create_locks
                        .entry(lock_key)
                        .or_insert_with(|| Arc::new(Mutex::new(())))
                        .clone();
                    let _guard = lock.lock().await;

                    // re-check: another request may have created the room meanwhile
                    let listing = match self.inner.driver.find_one(&conditions, Some(&handler.sort_by)) {
                        Some(l) => l,
                        None => self.create_room_inner(room_name, handler.merged_options(options.clone())).await?,
                    };
                    drop(_guard);
                    listing
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
            .await?;
        self.reserve_seat(&listing, options, auth).await
    }

    /// Join an existing room matching the criteria; error when none found.
    pub async fn join(
        &self,
        room_name: &str,
        options: Value,
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        let handler = self.handler(room_name)?;
        let conditions = handler.match_conditions(&options);
        let listing = self
            .inner
            .driver
            .find_one(&conditions, Some(&handler.sort_by))
            .ok_or_else(|| {
                ServerError::new(
                    codes::MATCHMAKE_INVALID_CRITERIA,
                    "no rooms found with the provided criteria",
                )
            })?;
        self.reserve_seat(&listing, options, auth).await
    }

    /// Join a specific room by id.
    pub async fn join_by_id(
        &self,
        room_id: &str,
        options: Value,
        auth: AuthContext,
    ) -> Result<SeatReservation> {
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
        self.reserve_seat(&listing, options, auth).await
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

    /// Gracefully shut down: dispose all rooms and stop matchmaking.
    pub async fn shutdown(&self) {
        if self.inner.shutting_down.swap(true, Ordering::SeqCst) {
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
    pub async fn create_room(&self, room_name: &str, options: Value) -> Result<RoomListing> {
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

    async fn create_room_inner(&self, room_name: &str, options: Value) -> Result<RoomListing> {
        let handler = self.handler(room_name)?;
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

    async fn reserve_seat(
        &self,
        listing: &RoomListing,
        options: Value,
        auth: AuthContext,
    ) -> Result<SeatReservation> {
        let handle = self
            .room_handle(&listing.room_id)
            .ok_or_else(|| ServerError::room_not_found(&listing.room_id))?;

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
            .map_err(|_| ServerError::seat_expired())?;

        tokio::time::timeout(RESERVE_SEAT_RPC_TIMEOUT, rx)
            .await
            .map_err(|_| ServerError::new(codes::MATCHMAKE_UNHANDLED, "seat reservation timed out"))?
            .map_err(|_| ServerError::seat_expired())??;

        let room = self.inner.driver.get(&listing.room_id).unwrap_or_else(|| listing.clone());
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
fn conditions_key(conditions: &Conditions) -> String {
    let mut keys: Vec<&String> = conditions.keys().collect();
    keys.sort();
    keys.iter()
        .filter(|k| k.as_str() != "locked" && k.as_str() != "private" && k.as_str() != "name")
        .map(|k| format!("{k}={}", conditions[*k]))
        .collect::<Vec<_>>()
        .join("&")
}

