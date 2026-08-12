//! The [`Room`] trait and [`RoomContext`] — the heart of the framework.
//!
//! Each room runs as its own async task (actor). All handlers are called
//! sequentially on that task, so room code never needs locks: you get
//! `&mut self` plus a `&mut RoomContext` in every handler.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::sync::broadcast;

use crate::actor::RoomSender;
use crate::client::{
    invalid_payload, AfterPatchItem, AfterPatchQueue, AfterPatchTarget, Client, ClientState,
};
use crate::driver::{LocalDriver, RoomListing};
use crate::error::{codes, Result, ServerError};
use crate::matchmaker::{AuthContext, MatchmakerEvent};
use crate::presence::Presence;
use crate::protocol::{self, MessageType};
use crate::state::{StateSlot, JSON_PATCH_SERIALIZER_ID, NONE_SERIALIZER_ID};
use crate::utils::Clock;

/// Boxed, sendable future used by handlers, timers and commands.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(crate) type MessageHandlerFn = dyn for<'a> Fn(&'a mut dyn Room, &'a mut RoomContext, Client, Value) -> BoxFuture<'a, Result<()>>
    + Send
    + Sync;

pub(crate) type AnyMessageHandlerFn = dyn for<'a> Fn(&'a mut dyn Room, &'a mut RoomContext, Client, MessageType, Value) -> BoxFuture<'a, Result<()>>
    + Send
    + Sync;

pub(crate) type BytesHandlerFn = dyn for<'a> Fn(&'a mut dyn Room, &'a mut RoomContext, Client, Vec<u8>) -> BoxFuture<'a, Result<()>>
    + Send
    + Sync;

pub(crate) type TimerCallback =
    dyn for<'a> FnOnce(&'a mut dyn Room, &'a mut RoomContext) -> BoxFuture<'a, ()> + Send;

/// Per-client state projection: (type-erased state, client) → serialized view.
pub(crate) type ViewFilterFn = dyn Fn(&dyn Any, &Client) -> Value + Send + Sync;

/// Force a closure to be checked against a higher-ranked (for<'a>) signature.
/// (Plain closure inference cannot produce HRTB signatures on its own.)
fn hrtb_msg<F>(f: F) -> F
where
    F: for<'a> Fn(&'a mut dyn Room, &'a mut RoomContext, Client, Value) -> BoxFuture<'a, Result<()>>
        + Send
        + Sync,
{
    f
}

fn hrtb_any<F>(f: F) -> F
where
    F: for<'a> Fn(&'a mut dyn Room, &'a mut RoomContext, Client, MessageType, Value) -> BoxFuture<'a, Result<()>>
        + Send
        + Sync,
{
    f
}

fn hrtb_bytes<F>(f: F) -> F
where
    F: for<'a> Fn(&'a mut dyn Room, &'a mut RoomContext, Client, Vec<u8>) -> BoxFuture<'a, Result<()>>
        + Send
        + Sync,
{
    f
}

fn hrtb_timer<F>(f: F) -> F
where
    F: for<'a> FnOnce(&'a mut dyn Room, &'a mut RoomContext) -> BoxFuture<'a, ()> + Send,
{
    f
}

pub(crate) struct TimerEntry {
    pub id: u64,
    pub at: Instant,
    pub cb: Box<TimerCallback>,
}

/// A decoded room traffic event, streamed to admin panel subscribers.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomEventLog {
    /// ms epoch
    pub at: u64,
    /// "in" (client → room), "out" (room → client), or "sys" (lifecycle)
    pub direction: &'static str,
    /// "message" | "bytes" | "broadcast" | "send" | "state_patch" |
    /// "state_full" | "join" | "reconnect" | "leave" | "seat" | "dispose"
    pub kind: &'static str,
    pub client: Option<String>,
    pub msg_type: Option<String>,
    pub payload: Option<Value>,
    pub bytes: usize,
}

impl RoomEventLog {
    pub fn new(direction: &'static str, kind: &'static str) -> Self {
        RoomEventLog {
            at: now_millis(),
            direction,
            kind,
            client: None,
            msg_type: None,
            payload: None,
            bytes: 0,
        }
    }

    pub fn client(mut self, session_id: &str) -> Self {
        self.client = Some(session_id.to_string());
        self
    }

    pub fn msg_type(mut self, t: &MessageType) -> Self {
        self.msg_type = Some(match t {
            MessageType::Str(s) => s.clone(),
            MessageType::Num(n) => n.to_string(),
        });
        self
    }

    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn bytes(mut self, n: usize) -> Self {
        self.bytes = n;
        self
    }
}

/// Stream of room traffic events (admin panel taps into this).
pub(crate) type EventTap = broadcast::Sender<RoomEventLog>;

/// A seat reserved via matchmaking, waiting for the client to connect.
pub(crate) struct ReservedSeat {
    pub options: Value,
    pub auth: Option<Value>,
    pub consumed: bool,
    pub waiting_reconnection: bool,
    pub expires_at: Instant,
}

/// A client allowed to reconnect.
pub(crate) struct ReconnectionEntry {
    pub session_id: String,
    pub client: Client,
    pub expires_at: Option<Instant>,
}

/// Options for [`RoomContext::broadcast_with_options`].
#[derive(Default)]
pub struct BroadcastOptions {
    /// Skip this client when broadcasting.
    pub except: Option<Client>,
    /// Defer delivery until right after the next state patch broadcast.
    pub after_next_patch: bool,
}

/// Object-safe downcasting support for rooms (blanket-implemented).
/// You never implement this yourself.
pub trait RoomAny: Send + 'static {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Send + 'static> RoomAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Implement this trait to define a room type.
///
/// All methods have default no-op implementations; override what you need.
/// Use [`RoomContext::on_message`] inside `on_create` to register typed
/// message handlers.
#[async_trait]
pub trait Room: RoomAny {
    /// Called once when the room is created. Returning `Err` aborts creation
    /// and propagates the error back to the matchmaking request.
    async fn on_create(&mut self, _ctx: &mut RoomContext, _options: Value) -> Result<()> {
        Ok(())
    }

    /// Called when a client requests a seat (before the WebSocket connects).
    /// Return `Ok(Some(auth))` to accept, `Ok(None)` to accept without auth
    /// data, or `Err` to reject the client.
    async fn on_auth(
        &mut self,
        _ctx: &mut RoomContext,
        _options: &Value,
        _auth: &AuthContext,
    ) -> Result<Option<Value>> {
        Ok(None)
    }

    /// Called when a client joins the room (after its seat is consumed).
    async fn on_join(
        &mut self,
        _ctx: &mut RoomContext,
        _client: Client,
        _options: Value,
        _auth: Option<Value>,
    ) -> Result<()> {
        Ok(())
    }

    /// Called when a client's connection drops unexpectedly. Call
    /// [`RoomContext::allow_reconnection`] here to let the client back in.
    async fn on_drop(&mut self, _ctx: &mut RoomContext, _client: Client, _code: u16) {}

    /// Called when a client successfully reconnects after `allow_reconnection`.
    async fn on_reconnect(&mut self, _ctx: &mut RoomContext, _client: Client) {}

    /// Called when a client leaves for good (consented leave, or reconnection
    /// not allowed / expired).
    async fn on_leave(&mut self, _ctx: &mut RoomContext, _client: Client, _code: u16) {}

    /// Called right before the room is disposed.
    async fn on_dispose(&mut self, _ctx: &mut RoomContext) {}

    /// The game loop. Called at the rate set by
    /// [`RoomContext::set_simulation_interval`]. `delta` is in seconds.
    async fn on_tick(&mut self, _ctx: &mut RoomContext, _delta: f64) {}
}

/// Everything a room can interact with: clients, state, broadcasting,
/// timers, listing metadata, presence.
pub struct RoomContext {
    room_id: String,
    room_name: String,
    process_id: String,
    created_at: u64,

    pub(crate) clients: Vec<Client>,
    pub(crate) state_slot: Option<StateSlot>,

    /// Room clock; ticked by the framework.
    pub clock: Clock,
    /// Dispose the room when the last client leaves. Default: `true`.
    pub auto_dispose: bool,
    /// Per-client incoming message rate limit. Default: unlimited.
    pub max_messages_per_second: Option<u32>,
    /// How long a reserved seat waits for its client to connect. Default: 15s.
    pub seat_reservation_timeout: Duration,

    pub(crate) patch_rate: Option<Duration>,
    pub(crate) sim_interval: Option<Duration>,
    pub(crate) max_clients: Option<u32>,
    pub(crate) locked: bool,
    pub(crate) is_private: bool,
    pub(crate) metadata: Option<Value>,

    pub(crate) reserved_seats: HashMap<String, ReservedSeat>,
    pub(crate) reconnections: HashMap<String, ReconnectionEntry>,

    pub(crate) message_handlers: HashMap<MessageType, Vec<Arc<MessageHandlerFn>>>,
    pub(crate) any_handlers: Vec<Arc<AnyMessageHandlerFn>>,
    pub(crate) bytes_handlers: HashMap<MessageType, Vec<Arc<BytesHandlerFn>>>,

    pub(crate) view_filter: Option<Arc<ViewFilterFn>>,
    pub(crate) view_snapshots: HashMap<String, Value>,

    pub(crate) after_patch: AfterPatchQueue,
    pub(crate) timers: Vec<TimerEntry>,
    next_timer_id: u64,

    pub(crate) driver: Arc<LocalDriver>,
    presence: Arc<dyn Presence>,
    pub(crate) lobby_tx: broadcast::Sender<MatchmakerEvent>,

    pub(crate) request_dispose: Option<u16>,
    pub(crate) disposing: bool,
    pub(crate) had_clients: bool,
    pub(crate) filter_extra: Map<String, Value>,
    pub(crate) sender: Option<RoomSender>,
    pub(crate) tap: EventTap,
}

impl RoomContext {
    pub(crate) fn new(
        room_id: String,
        room_name: String,
        process_id: String,
        driver: Arc<LocalDriver>,
        presence: Arc<dyn Presence>,
        lobby_tx: broadcast::Sender<MatchmakerEvent>,
        filter_extra: Map<String, Value>,
    ) -> Self {
        RoomContext {
            room_id,
            room_name,
            process_id,
            created_at: now_millis(),
            clients: Vec::new(),
            state_slot: None,
            clock: Clock::new(),
            auto_dispose: true,
            max_messages_per_second: None,
            seat_reservation_timeout: Duration::from_secs(15),
            patch_rate: Some(Duration::from_millis(50)),
            sim_interval: None,
            max_clients: None,
            locked: false,
            is_private: false,
            metadata: None,
            reserved_seats: HashMap::new(),
            reconnections: HashMap::new(),
            message_handlers: HashMap::new(),
            any_handlers: Vec::new(),
            bytes_handlers: HashMap::new(),
            view_filter: None,
            view_snapshots: HashMap::new(),
            after_patch: Arc::new(Mutex::new(Vec::new())),
            timers: Vec::new(),
            next_timer_id: 0,
            driver,
            presence,
            lobby_tx,
            request_dispose: None,
            disposing: false,
            had_clients: false,
            filter_extra,
            sender: None,
            tap: broadcast::channel(512).0,
        }
    }

    /// Emit a traffic event to admin-panel subscribers (no-op when none).
    pub(crate) fn tap_log(&self, log: RoomEventLog) {
        if self.tap.receiver_count() > 0 {
            let _ = self.tap.send(log);
        }
    }

    pub(crate) fn set_sender(&mut self, sender: RoomSender) {
        self.sender = Some(sender);
    }

    /// A handle for injecting commands into this room from background tasks
    /// (LLM calls, external subscribers, …). See [`RoomSender`].
    pub fn sender(&self) -> RoomSender {
        self.sender
            .clone()
            .expect("RoomContext::sender is available once the room is spawned")
    }

    // ------------------------------------------------------------------
    // Identity / clients
    // ------------------------------------------------------------------

    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    pub fn room_name(&self) -> &str {
        &self.room_name
    }

    pub fn process_id(&self) -> &str {
        &self.process_id
    }

    pub fn clients(&self) -> &[Client] {
        &self.clients
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn get_client(&self, session_id: &str) -> Option<Client> {
        self.clients.iter().find(|c| c.session_id() == session_id).cloned()
    }

    pub fn presence(&self) -> Arc<dyn Presence> {
        self.presence.clone()
    }

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------

    /// Set the room's synchronized state. Any `Serialize` struct works.
    /// (`DeserializeOwned` is also required so the admin panel can edit
    /// state with type validation via a serialize→edit→deserialize round-trip.)
    pub fn set_state<S: Serialize + serde::de::DeserializeOwned + Send + 'static>(&mut self, state: S) {
        self.state_slot = Some(StateSlot::new(state));
    }

    pub fn state<T: 'static>(&self) -> Option<&T> {
        self.state_slot.as_ref()?.get::<T>()
    }

    pub fn state_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.state_slot.as_mut()?.get_mut::<T>()
    }

    pub fn serializer_id(&self) -> &'static str {
        if self.state_slot.is_some() {
            JSON_PATCH_SERIALIZER_ID
        } else {
            NONE_SERIALIZER_ID
        }
    }

    // ------------------------------------------------------------------
    // Per-client state views (the `@view()` counterpart)
    // ------------------------------------------------------------------

    /// Set a per-client projection of the state.
    ///
    /// When a view filter is set, every client receives patches computed
    /// against *their own* projected view of the state instead of the shared
    /// full state — e.g. hidden hands in a card game, or fog-of-war.
    ///
    /// ```ignore
    /// ctx.set_view_filter(|state: &CardGame, client: &Client| PlayerView {
    ///     table: state.table.clone(),
    ///     my_hand: state.hands[client.session_id()].clone(),
    ///     opponents: state.opponent_card_counts(),
    /// });
    /// ```
    ///
    /// Note: views cost one serialization + diff **per client per patch**;
    /// rooms without a filter share a single patch broadcast as before.
    pub fn set_view_filter<S, V, F>(&mut self, f: F)
    where
        S: Serialize + Send + 'static,
        V: Serialize + Send + 'static,
        F: Fn(&S, &Client) -> V + Send + Sync + 'static,
    {
        self.view_filter = Some(Arc::new(move |state: &dyn Any, client: &Client| {
            let Some(state) = state.downcast_ref::<S>() else {
                return Value::Null;
            };
            serde_json::to_value(f(state, client)).unwrap_or(Value::Null)
        }));
    }

    /// Remove the view filter; clients go back to the shared full state.
    pub fn clear_view_filter(&mut self) {
        self.view_filter = None;
        self.view_snapshots.clear();
    }

    pub fn has_view_filter(&self) -> bool {
        self.view_filter.is_some()
    }

    // ------------------------------------------------------------------
    // Messaging
    // ------------------------------------------------------------------

    /// Broadcast a typed message to all joined clients.
    pub fn broadcast<T: Serialize>(&self, msg_type: impl Into<MessageType>, message: &T) {
        self.broadcast_with_options(msg_type, message, BroadcastOptions::default());
    }

    pub fn broadcast_with_options<T: Serialize>(
        &self,
        msg_type: impl Into<MessageType>,
        message: &T,
        options: BroadcastOptions,
    ) {
        let msg_type = msg_type.into();
        if self.tap.receiver_count() > 0 {
            self.tap_log(
                RoomEventLog::new("out", "broadcast")
                    .msg_type(&msg_type)
                    .payload(serde_json::to_value(message).unwrap_or(Value::Null)),
            );
        }
        let bytes = protocol::room_data(&msg_type, message);
        let except = options.except.map(|c| c.session_id().to_string());
        if options.after_next_patch {
            self.after_patch.lock().push(AfterPatchItem {
                target: AfterPatchTarget::Broadcast { except },
                bytes,
            });
            return;
        }
        for client in &self.clients {
            if !client.is_joined() {
                continue;
            }
            if except.as_deref() == Some(client.session_id()) {
                continue;
            }
            client.raw_now(bytes.clone());
        }
    }

    /// Broadcast a typed binary message to all joined clients.
    pub fn broadcast_bytes(&self, msg_type: impl Into<MessageType>, payload: &[u8]) {
        let msg_type = msg_type.into();
        self.tap_log(
            RoomEventLog::new("out", "broadcast")
                .msg_type(&msg_type)
                .bytes(payload.len()),
        );
        let bytes = protocol::room_data_bytes(&msg_type, payload);
        for client in &self.clients {
            if client.is_joined() {
                client.raw_now(bytes.clone());
            }
        }
    }

    /// Register a typed message handler. `M` is deserialized from the msgpack
    /// payload (must be JSON-compatible). `R` must be this room's concrete type.
    ///
    /// Handlers return a boxed future (`Box::pin(async move { ... })`) — this
    /// is what allows borrowing `&mut room` / `&mut ctx` on stable Rust.
    ///
    /// ```ignore
    /// ctx.on_message("chat", |room: &mut ChatRoom, ctx, client, msg: ChatMsg| Box::pin(async move {
    ///     ctx.broadcast("chat", &msg);
    ///     Ok(())
    /// }));
    /// ```
    pub fn on_message<R, M, F>(&mut self, msg_type: impl Into<MessageType>, f: F)
    where
        R: Room,
        M: DeserializeOwned + Send,
        F: for<'a> Fn(&'a mut R, &'a mut RoomContext, Client, M) -> BoxFuture<'a, Result<()>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        let wrapper = hrtb_msg(move |room, ctx, client, payload| {
            let f = f.clone();
            let Some(room) = room.as_any_mut().downcast_mut::<R>() else {
                return Box::pin(async {
                    Err(ServerError::new(
                        codes::APPLICATION_ERROR,
                        "room type mismatch in message handler",
                    ))
                });
            };
            match serde_json::from_value::<M>(payload) {
                Ok(msg) => f(room, ctx, client, msg),
                Err(e) => {
                    invalid_payload(&client, e);
                    Box::pin(async { Ok(()) })
                }
            }
        });
        self.message_handlers
            .entry(msg_type.into())
            .or_default()
            .push(Arc::new(wrapper));
    }

    /// Register a catch-all handler receiving every message (including ones
    /// handled by typed handlers).
    pub fn on_any_message<R, F>(&mut self, f: F)
    where
        R: Room,
        F: for<'a> Fn(&'a mut R, &'a mut RoomContext, Client, MessageType, Value) -> BoxFuture<'a, Result<()>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        let wrapper = hrtb_any(move |room, ctx, client, msg_type, payload| {
            let f = f.clone();
            let Some(room) = room.as_any_mut().downcast_mut::<R>() else {
                return Box::pin(async { Ok(()) });
            };
            f(room, ctx, client, msg_type, payload)
        });
        self.any_handlers.push(Arc::new(wrapper));
    }

    /// Register a handler for binary messages (`[17, type, bytes]`).
    pub fn on_message_bytes<R, F>(&mut self, msg_type: impl Into<MessageType>, f: F)
    where
        R: Room,
        F: for<'a> Fn(&'a mut R, &'a mut RoomContext, Client, Vec<u8>) -> BoxFuture<'a, Result<()>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        let wrapper = hrtb_bytes(move |room, ctx, client, payload| {
            let f = f.clone();
            let Some(room) = room.as_any_mut().downcast_mut::<R>() else {
                return Box::pin(async { Ok(()) });
            };
            f(room, ctx, client, payload)
        });
        self.bytes_handlers
            .entry(msg_type.into())
            .or_default()
            .push(Arc::new(wrapper));
    }

    // ------------------------------------------------------------------
    // Timers / game loop
    // ------------------------------------------------------------------

    /// Run a closure once after `delay`. Returns a timer id.
    ///
    /// ```ignore
    /// ctx.set_timeout(Duration::from_secs(5), |room: &mut MyRoom, ctx| Box::pin(async move {
    ///     ctx.disconnect(close_codes::CONSENTED);
    /// }));
    /// ```
    pub fn set_timeout<R, F>(&mut self, delay: Duration, f: F) -> u64
    where
        R: Room,
        F: for<'a> FnOnce(&'a mut R, &'a mut RoomContext) -> BoxFuture<'a, ()> + Send + 'static,
    {
        self.next_timer_id += 1;
        let id = self.next_timer_id;
        let wrapper = hrtb_timer(move |room, ctx| {
            let Some(room) = room.as_any_mut().downcast_mut::<R>() else {
                return Box::pin(async {});
            };
            f(room, ctx)
        });
        self.timers.push(TimerEntry {
            id,
            at: Instant::now() + delay,
            cb: Box::new(wrapper),
        });
        id
    }

    pub fn clear_timeout(&mut self, id: u64) -> bool {
        let len = self.timers.len();
        self.timers.retain(|t| t.id != id);
        self.timers.len() != len
    }

    /// Set the game-loop interval. `Room::on_tick` is called at this rate.
    /// `None` disables the game loop.
    pub fn set_simulation_interval(&mut self, interval: Option<Duration>) {
        self.sim_interval = interval;
    }

    /// Set how often state patches are broadcast. Default: 50ms.
    /// `None` disables automatic patches — call [`Self::broadcast_patch`] manually.
    pub fn set_patch_rate(&mut self, rate: Option<Duration>) {
        self.patch_rate = rate;
    }

    pub fn patch_rate(&self) -> Option<Duration> {
        self.patch_rate
    }

    // ------------------------------------------------------------------
    // Matchmaking knobs
    // ------------------------------------------------------------------

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    /// Lock the room: no new seats can be reserved.
    pub fn lock(&mut self) {
        if !self.locked {
            self.locked = true;
            self.sync_listing();
        }
    }

    pub fn unlock(&mut self) {
        if self.locked {
            self.locked = false;
            self.sync_listing();
        }
    }

    pub fn max_clients(&self) -> Option<u32> {
        self.max_clients
    }

    pub fn set_max_clients(&mut self, max_clients: Option<u32>) {
        self.max_clients = max_clients;
        self.sync_listing();
    }

    pub fn is_private(&self) -> bool {
        self.is_private
    }

    /// Private rooms are hidden from `join_or_create` / `join` matchmaking.
    pub fn set_private(&mut self, is_private: bool) {
        self.is_private = is_private;
        self.sync_listing();
    }

    pub fn metadata(&self) -> Option<&Value> {
        self.metadata.as_ref()
    }

    pub fn set_metadata(&mut self, metadata: Value) {
        self.metadata = Some(metadata);
        self.sync_listing();
    }

    pub fn has_reached_max_clients(&self) -> bool {
        let Some(max) = self.max_clients else { return false };
        let pending_seats = self
            .reserved_seats
            .values()
            .filter(|s| !s.consumed && !s.waiting_reconnection)
            .count();
        (self.clients.len() + pending_seats) as u32 >= max
    }

    /// Allow `client` to reconnect. Must be called inside `Room::on_drop`.
    /// `timeout: None` means "manual" — the seat never expires on its own.
    pub fn allow_reconnection(&mut self, client: &Client, timeout: Option<Duration>) -> bool {
        if self.disposing {
            return false;
        }
        let token = client.reconnection_token();
        if token.is_empty() || self.reconnections.contains_key(&token) {
            return false;
        }
        client.set_state(ClientState::Reconnecting);
        let expires_at = timeout.map(|d| Instant::now() + d);
        self.reconnections.insert(
            token,
            ReconnectionEntry {
                session_id: client.session_id().to_string(),
                client: client.clone(),
                expires_at,
            },
        );
        self.reserved_seats.insert(
            client.session_id().to_string(),
            ReservedSeat {
                options: Value::Null,
                auth: client.auth(),
                consumed: false,
                waiting_reconnection: true,
                // governed by the reconnection entry's expiry
                expires_at: expires_at.unwrap_or_else(|| Instant::now() + Duration::from_secs(86400 * 365)),
            },
        );
        true
    }

    /// Forget a client completely: drop it from the client list and cancel
    /// any pending reconnection / seat reservation / view snapshot for it.
    ///
    /// Useful when a user's session is *taken over* by a newer connection
    /// (e.g. page reload after the reconnection window expired, and your
    /// `on_join` re-seats the same account under a new session id).
    pub fn remove_client(&mut self, session_id: &str) {
        self.clients.retain(|c| c.session_id() != session_id);
        self.reconnections.retain(|_, e| e.session_id != session_id);
        self.reserved_seats.remove(session_id);
        self.view_snapshots.remove(session_id);
        self.sync_listing();
    }

    /// Dispose the room: all clients are disconnected with `code`
    /// (default suggestion: [`close_codes::CONSENTED`]) and `on_dispose` runs.
    pub fn disconnect(&mut self, code: u16) {
        self.request_dispose = Some(code);
    }

    // ------------------------------------------------------------------
    // Internals used by the room actor
    // ------------------------------------------------------------------

    /// Diff the state and broadcast patches; then flush `after_next_patch`
    /// messages. Returns whether anything changed.
    ///
    /// With a view filter, each client gets a patch against their own
    /// projection; otherwise a single shared patch is broadcast.
    pub fn broadcast_patch(&mut self) -> bool {
        let mut has_changes = false;

        if self.state_slot.is_some() {
            if let Some(filter) = self.view_filter.clone() {
                let slot = self.state_slot.as_ref().unwrap();
                for client in self.clients.iter().filter(|c| c.is_joined()) {
                    let view = filter(slot.as_any(), client);
                    let snapshot = self
                        .view_snapshots
                        .entry(client.session_id().to_string())
                        .or_insert_with(|| view.clone());
                    if *snapshot != view {
                        let patch_ops = crate::diff::diff(snapshot, &view);
                        *snapshot = view;
                        if !patch_ops.is_empty() {
                            has_changes = true;
                            let patch = Value::Array(patch_ops);
                            self.tap_log(
                                RoomEventLog::new("out", "state_patch")
                                    .client(client.session_id())
                                    .payload(patch.clone()),
                            );
                            client.raw_now(protocol::room_state_patch(&patch));
                        }
                    }
                }
            } else {
                let slot = self.state_slot.as_mut().unwrap();
                if let Some(patch) = slot.diff() {
                    has_changes = true;
                    self.tap_log(RoomEventLog::new("out", "state_patch").payload(patch.clone()));
                    let bytes = protocol::room_state_patch(&patch);
                    for client in &self.clients {
                        if client.is_joined() {
                            client.raw_now(bytes.clone());
                        }
                    }
                }
            }
        }

        let items = std::mem::take(&mut *self.after_patch.lock());
        for item in items {
            match item.target {
                AfterPatchTarget::Client(session_id) => {
                    if let Some(client) = self.clients.iter().find(|c| c.session_id() == session_id) {
                        if client.is_joined() {
                            client.raw_now(item.bytes);
                        }
                    }
                }
                AfterPatchTarget::Broadcast { except } => {
                    for client in &self.clients {
                        if !client.is_joined() || except.as_deref() == Some(client.session_id()) {
                            continue;
                        }
                        client.raw_now(item.bytes.clone());
                    }
                }
            }
        }
        has_changes
    }

    /// Full serialized state for a specific client (view-filtered when set).
    pub(crate) fn full_state_for(&self, client: &Client) -> Option<Value> {
        let slot = self.state_slot.as_ref()?;
        Some(match &self.view_filter {
            Some(filter) => filter(slot.as_any(), client),
            None => slot.full(),
        })
    }

    /// Record the state a client currently holds (baseline for its next patch).
    pub(crate) fn set_view_snapshot(&mut self, session_id: &str, view: Value) {
        self.view_snapshots.insert(session_id.to_string(), view);
    }

    pub(crate) fn drop_view_snapshot(&mut self, session_id: &str) {
        self.view_snapshots.remove(session_id);
    }

    pub(crate) fn listing_client_count(&self) -> u32 {
        let pending = self
            .reserved_seats
            .values()
            .filter(|s| !s.consumed && !s.waiting_reconnection)
            .count();
        (self.clients.len() + pending) as u32
    }

    pub(crate) fn build_listing(&self) -> RoomListing {
        RoomListing {
            room_id: self.room_id.clone(),
            name: self.room_name.clone(),
            process_id: self.process_id.clone(),
            clients: self.listing_client_count(),
            max_clients: self.max_clients,
            locked: self.locked,
            is_private: self.is_private,
            metadata: self.metadata.clone(),
            created_at: self.created_at,
            extra: self.filter_extra.clone(),
        }
    }

    /// Persist listing-affecting fields to the driver and notify lobby subscribers.
    pub(crate) fn sync_listing(&self) {
        let mut updated = None;
        self.driver.update_by_id(&self.room_id, |l| {
            l.clients = self.listing_client_count();
            l.locked = self.locked;
            l.is_private = self.is_private;
            l.metadata = self.metadata.clone();
            l.max_clients = self.max_clients;
            updated = Some(l.clone());
        });
        if let Some(l) = updated {
            let _ = self.lobby_tx.send(MatchmakerEvent::RoomUpdated(l));
        }
    }
}

pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Default for RoomContext {
    fn default() -> Self {
        // for docs/tests only; rooms always get a fully-wired context.
        // the sender is a dead letter queue — sends quietly fail, which is
        // what tests that dispatch task-spawning commands want.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ctx = RoomContext::new(
            String::new(),
            String::new(),
            String::new(),
            Arc::new(LocalDriver::new()),
            crate::presence::LocalPresence::new(),
            broadcast::channel(16).0,
            Map::new(),
        );
        ctx.set_sender(RoomSender::for_tests(tx));
        ctx
    }
}

