//! The room actor: every room runs as an independent tokio task. All room
//! events (seat reservations, connections, messages, timers, ticks) are
//! processed sequentially, so user code never needs synchronization.


use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep_until, Instant as TokioInstant, Interval};

use crate::client::{Client, ClientState};
use crate::driver::RoomListing;
use crate::error::{close_codes, codes, Result, ServerError};
use crate::matchmaker::{AuthContext, MatchmakerEvent};
use crate::protocol::{self, ClientMessage};
use crate::room::{BoxFuture, ReservedSeat, Room, RoomContext};
use crate::snapshot::RoomSnapshot;
use crate::utils::generate_id;

/// Events the room actor processes.
pub(crate) enum RoomEvent {
    ReserveSeat {
        session_id: String,
        options: Value,
        auth: AuthContext,
        respond: oneshot::Sender<Result<()>>,
    },
    CheckReconnection {
        token: String,
        respond: oneshot::Sender<Option<String>>,
    },
    Connect {
        client: Client,
        reconnection_token: Option<String>,
        respond: oneshot::Sender<Result<()>>,
    },
    Message {
        client: Client,
        msg: ClientMessage,
    },
    Disconnected {
        client: Client,
        code: u16,
    },
    /// External command injected via [`RoomSender`] (e.g. result of a
    /// background task like an LLM call, or a Redis subscription).
    Command(Box<CommandFn>),
    /// Admin room RPC: typed request/response operation run on this room actor.
    CallRoomRpc {
        cmd: Box<RoomRpcFn>,
        respond: oneshot::Sender<Result<Value>>,
    },
    /// Admin panel: report room internals.
    Inspect {
        respond: oneshot::Sender<RoomInspection>,
    },
    /// Admin panel: force-disconnect a client.
    Kick {
        session_id: String,
    },
    /// Admin panel: lock/unlock the room.
    SetLocked(bool),
    /// Admin panel: send a message to one client (or broadcast to all).
    AdminMessage {
        session_id: Option<String>,
        msg_type: crate::protocol::MessageType,
        payload: Value,
    },
    /// Admin panel: dispose the room with a custom close code.
    Dispose {
        code: u16,
    },
    /// Admin panel: validated state edit (set/remove at path).
    EditState {
        path: Vec<String>,
        edit: crate::state::StateEdit,
        respond: oneshot::Sender<std::result::Result<(), String>>,
    },
    Shutdown,
}

/// Admin panel inspection payload for a room.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomInspection {
    pub room_id: String,
    pub state: Option<Value>,
    pub clients: Vec<ClientInspection>,
    pub reserved_seats: usize,
    pub pending_reconnections: usize,
    pub elapsed_millis: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientInspection {
    pub session_id: String,
    pub state: String,
    pub auth: Option<Value>,
    pub user_data: Option<Value>,
}

pub(crate) type CommandFn =
    dyn for<'a> FnOnce(&'a mut dyn Room, &'a mut RoomContext) -> BoxFuture<'a, ()> + Send;

/// A room-RPC command: runs on the room actor with `&mut Room` + `&mut RoomContext`
/// and produces a JSON value that is returned to the caller (request/response).
pub(crate) type RoomRpcFn = dyn for<'a> FnOnce(
    &'a mut dyn Room,
    &'a mut RoomContext,
) -> BoxFuture<'a, Result<Value>>
    + Send;

fn hrtb_command<F>(f: F) -> F
where
    F: for<'a> FnOnce(&'a mut dyn Room, &'a mut RoomContext) -> BoxFuture<'a, ()> + Send,
{
    f
}

/// A clonable handle for sending commands into a room from anywhere
/// (background tasks, external subscribers, other rooms).
///
/// Commands are queued on the room's mailbox and executed sequentially on the
/// room actor, exactly like message handlers — so they can safely mutate the
/// room and its state.
///
/// ```ignore
/// let sender = ctx.sender();
/// tokio::spawn(async move {
///     let question = llm.generate(...).await;
///     sender.send(move |room: &mut MyRoom, ctx| Box::pin(async move {
///         room.set_question(ctx, question);
///     }));
/// });
/// ```
#[derive(Clone)]
pub struct RoomSender {
    tx: mpsc::UnboundedSender<RoomEvent>,
}

impl RoomSender {
    pub(crate) fn for_tests(tx: mpsc::UnboundedSender<RoomEvent>) -> Self {
        RoomSender { tx }
    }
    pub fn send<R, F>(&self, f: F) -> bool
    where
        R: Room,
        F: for<'a> FnOnce(&'a mut R, &'a mut RoomContext) -> BoxFuture<'a, ()> + Send + 'static,
    {
        let wrapper = hrtb_command(move |room, ctx| {
            let Some(room) = room.as_any_mut().downcast_mut::<R>() else {
                return Box::pin(async {});
            };
            f(room, ctx)
        });
        self.tx.send(RoomEvent::Command(Box::new(wrapper))).is_ok()
    }
}

#[derive(Clone)]
pub(crate) struct RoomHandle {
    #[allow(dead_code)]
    pub room_id: String,
    pub tx: mpsc::UnboundedSender<RoomEvent>,
    pub tap: crate::room::EventTap,
}

impl RoomHandle {
    /// A [`RoomSender`] for injecting typed commands into this room (used by
    /// admin RPCs via [`crate::admin_rpc::AdminContext::command_room`]).
    pub fn sender(&self) -> RoomSender {
        RoomSender { tx: self.tx.clone() }
    }
}

/// Called when the room is fully disposed (used by the matchmaker for cleanup).
pub(crate) type OnDispose = Box<dyn FnOnce() + Send>;

/// Create a room, run `on_create`, and spawn its actor task.
///
/// Returns the room handle plus the initial listing (to be registered with the
/// driver by the caller).
pub(crate) async fn spawn_room(
    mut room: Box<dyn Room>,
    mut ctx: RoomContext,
    options: Value,
    on_dispose: OnDispose,
) -> Result<(RoomHandle, RoomListing)> {
    ctx.create_options = options.clone();

    // The sender must be available *during* `on_create` (rooms can use
    // `ctx.sender()` to spawn background work there).
    let (tx, rx) = mpsc::unbounded_channel();
    ctx.set_sender(RoomSender { tx: tx.clone() });

    room.on_create(&mut ctx, options).await?;
    ctx.schema_version = room.schema_version();
    finish_spawn(room, ctx, tx, rx, on_dispose).await
}

/// Restore a room from a snapshot, run `on_restore`, and spawn its actor task.
pub(crate) async fn spawn_restored_room(
    mut room: Box<dyn Room>,
    mut ctx: RoomContext,
    mut snapshot: RoomSnapshot,
    on_dispose: OnDispose,
) -> Result<(RoomHandle, RoomListing)> {
    let (tx, rx) = mpsc::unbounded_channel();
    ctx.set_sender(RoomSender { tx: tx.clone() });

    // `on_create` always runs first so message handlers and defaults are
    // re-registered; `on_restore` then overlays the persisted state.
    room.on_create(&mut ctx, snapshot.options.clone()).await?;

    let current = room.schema_version();
    if snapshot.schema_version < current {
        room.on_migrate(snapshot.schema_version, &mut snapshot)?;
        snapshot.schema_version = current;
    } else if snapshot.schema_version > current {
        return Err(ServerError::new(
            codes::APPLICATION_ERROR,
            format!(
                "snapshot schema version {} is newer than room version {}",
                snapshot.schema_version, current
            ),
        ));
    }

    room.on_restore(&mut ctx, &snapshot).await?;

    // Framework-managed fields (clock, metadata, seats, reconnections) are
    // applied last so persisted values win over `on_create` defaults.
    ctx.apply_snapshot(&snapshot);

    ctx.schema_version = current;
    finish_spawn(room, ctx, tx, rx, on_dispose).await
}

async fn finish_spawn(
    room: Box<dyn Room>,
    ctx: RoomContext,
    tx: mpsc::UnboundedSender<RoomEvent>,
    rx: mpsc::UnboundedReceiver<RoomEvent>,
    on_dispose: OnDispose,
) -> Result<(RoomHandle, RoomListing)> {
    let tap = ctx.tap.clone();

    let listing = ctx.build_listing();
    let handle = RoomHandle {
        room_id: ctx.room_id().to_string(),
        tx,
        tap,
    };

    let actor = RoomActor {
        room,
        ctx,
        rx,
        on_dispose: Some(on_dispose),
        sim: None,
        sim_cfg: None,
        patch: None,
        patch_cfg: None,
        last_tick: Instant::now(),
        last_auto_save: None,
        created_at: Instant::now(),
    };
    tokio::spawn(actor.run());

    Ok((handle, listing))
}

enum Flow {
    Continue,
    Break,
}

struct RoomActor {
    room: Box<dyn Room>,
    ctx: RoomContext,
    rx: mpsc::UnboundedReceiver<RoomEvent>,
    on_dispose: Option<OnDispose>,
    sim: Option<Interval>,
    sim_cfg: Option<Duration>,
    patch: Option<Interval>,
    patch_cfg: Option<Duration>,
    last_tick: Instant,
    last_auto_save: Option<Instant>,
    created_at: Instant,
}

impl RoomActor {
    async fn run(mut self) {
        loop {
            self.sync_intervals();

            let deadline = self.next_deadline();

            tokio::select! {
                biased;

                ev = self.rx.recv() => {
                    match ev {
                        Some(ev) => {
                            if let Flow::Break = self.handle_event(ev).await {
                                break;
                            }
                        }
                        None => break,
                    }
                }

                _ = async {
                    match self.sim.as_mut() {
                        Some(i) => i.tick().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let now = Instant::now();
                    let delta = now.duration_since(self.last_tick).as_secs_f64();
                    self.last_tick = now;
                    self.ctx.clock.tick();
                    self.room.on_tick(&mut self.ctx, delta).await;
                }

                _ = async {
                    match self.patch.as_mut() {
                        Some(i) => i.tick().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if self.ctx.sim_interval.is_none() {
                        self.ctx.clock.tick();
                    }
                    let changed = self.ctx.broadcast_patch();
                    if changed {
                        self.maybe_auto_save();
                    }
                }

                _ = async {
                    match deadline {
                        Some(d) => sleep_until(TokioInstant::from_std(d)).await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.handle_deadlines().await;
                }
            }

            if let Some(code) = self.ctx.request_dispose.take() {
                self.dispose(code).await;
                break;
            }
            if self.should_auto_dispose() {
                self.dispose(close_codes::CONSENTED).await;
                break;
            }
        }
    }

    fn sync_intervals(&mut self) {
        if self.sim_cfg != self.ctx.sim_interval {
            self.sim_cfg = self.ctx.sim_interval;
            self.sim = self.sim_cfg.map(|d| {
                let mut i = tokio::time::interval(d);
                i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                i
            });
            self.last_tick = Instant::now();
        }
        if self.patch_cfg != self.ctx.patch_rate {
            self.patch_cfg = self.ctx.patch_rate;
            self.patch = self.patch_cfg.map(|d| {
                let mut i = tokio::time::interval(d);
                i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                i
            });
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        let mut deadline: Option<Instant> = None;
        let mut consider = |t: Instant| {
            deadline = Some(deadline.map_or(t, |d: Instant| d.min(t)));
        };
        for t in &self.ctx.timers {
            consider(t.at);
        }
        for s in self.ctx.reserved_seats.values() {
            if !s.consumed && !s.waiting_reconnection {
                consider(s.expires_at);
            }
        }
        for e in self.ctx.reconnections.values() {
            if let Some(t) = e.expires_at {
                consider(t);
            }
        }
        if self.ctx.auto_dispose
            && !self.ctx.had_clients
            && self.ctx.clients.is_empty()
            && !self.ctx.disposing
        {
            consider(self.created_at + self.ctx.seat_reservation_timeout);
        }
        deadline
    }

    fn should_auto_dispose(&self) -> bool {
        if !self.ctx.auto_dispose || self.ctx.disposing {
            return false;
        }
        let has_active_client = self
            .ctx
            .clients
            .iter()
            .any(|c| !matches!(c.state(), ClientState::Closed));
        if has_active_client || !self.ctx.reconnections.is_empty() {
            return false;
        }
        if self
            .ctx
            .reserved_seats
            .values()
            .any(|s| !s.consumed && !s.waiting_reconnection)
        {
            return false;
        }
        if self.ctx.had_clients {
            // everyone left for good
            return true;
        }
        // never had a client: dispose if nobody showed up within the seat timeout
        self.created_at.elapsed() >= self.ctx.seat_reservation_timeout
    }

    /// Debounced automatic snapshot write after a state change.
    fn maybe_auto_save(&mut self) {
        if self.ctx.persistence.is_none() {
            return;
        }
        if self.ctx.state_slot.is_none() && self.ctx.internal_slot.is_none() {
            return;
        }
        let Some(p) = self.ctx.persistence.clone() else {
            return;
        };
        let interval = p.config.auto_save_interval;
        let now = Instant::now();
        if self
            .last_auto_save
            .map_or(true, |t| now.duration_since(t) >= interval)
        {
            self.last_auto_save = Some(now);
            self.ctx.persist_now();
        }
    }

    async fn handle_event(&mut self, ev: RoomEvent) -> Flow {
        match ev {
            RoomEvent::ReserveSeat {
                session_id,
                options,
                auth,
                respond,
            } => self.on_reserve_seat(session_id, options, auth, respond).await,
            RoomEvent::CheckReconnection { token, respond } => {
                let session_id = self.ctx.reconnections.get(&token).map(|e| e.session_id.clone());
                let _ = respond.send(session_id);
            }
            RoomEvent::Connect {
                client,
                reconnection_token,
                respond,
            } => self.on_connect(client, reconnection_token, respond).await,
            RoomEvent::Message { client, msg } => self.on_client_message(client, msg).await,
            RoomEvent::Disconnected { client, code } => self.on_disconnected(client, code).await,
            RoomEvent::Command(cmd) => {
                cmd(&mut *self.room, &mut self.ctx).await;
            }
            RoomEvent::CallRoomRpc { cmd, respond } => {
                let result = cmd(&mut *self.room, &mut self.ctx).await;
                let _ = respond.send(result);
            }
            RoomEvent::Inspect { respond } => {
                let _ = respond.send(RoomInspection {
                    room_id: self.ctx.room_id().to_string(),
                    state: self.ctx.state_slot.as_ref().map(|s| s.full()),
                    clients: self
                        .ctx
                        .clients
                        .iter()
                        .map(|c| ClientInspection {
                            session_id: c.session_id().to_string(),
                            state: format!("{:?}", c.state()),
                            auth: c.auth(),
                            user_data: c.user_data(),
                        })
                        .collect(),
                    reserved_seats: self.ctx.reserved_seats.len(),
                    pending_reconnections: self.ctx.reconnections.len(),
                    elapsed_millis: self.created_at.elapsed().as_millis() as u64,
                });
            }
            RoomEvent::Kick { session_id } => {
                if let Some(client) = self.ctx.get_client(&session_id) {
                    client.leave(Some(crate::error::close_codes::CONSENTED), Some("kicked by admin"));
                }
            }
            RoomEvent::SetLocked(locked) => {
                if locked {
                    self.ctx.lock();
                } else {
                    self.ctx.unlock();
                }
            }
            RoomEvent::AdminMessage {
                session_id,
                msg_type,
                payload,
            } => {
                match session_id {
                    Some(sid) => {
                        if let Some(client) = self.ctx.get_client(&sid) {
                            client.send(msg_type, &payload);
                        }
                    }
                    None => self.ctx.broadcast(msg_type, &payload),
                }
            }
            RoomEvent::Dispose { code } => {
                self.dispose(code).await;
                return Flow::Break;
            }
            RoomEvent::EditState { path, edit, respond } => {
                let result = match &mut self.ctx.state_slot {
                    Some(slot) => slot.apply_edit(&path, &edit),
                    None => Err("room has no state".to_string()),
                };
                let _ = respond.send(result);
            }
            RoomEvent::Shutdown => {
                self.dispose(close_codes::SERVER_SHUTDOWN).await;
                return Flow::Break;
            }
        }
        Flow::Continue
    }

    async fn on_reserve_seat(
        &mut self,
        session_id: String,
        options: Value,
        auth: AuthContext,
        respond: oneshot::Sender<Result<()>>,
    ) {
        if self.ctx.disposing || self.ctx.locked || self.ctx.has_reached_max_clients() {
            let _ = respond.send(Err(ServerError::new(
                codes::MATCHMAKE_EXPIRED,
                format!("room \"{}\" is not available", self.ctx.room_id()),
            )));
            return;
        }

        match self.room.on_auth(&mut self.ctx, &options, &auth).await {
            Ok(auth_data) => {
                self.ctx.tap_log(
                    crate::room::RoomEventLog::new("sys", "seat").client(&session_id),
                );
                self.ctx.reserved_seats.insert(
                    session_id,
                    ReservedSeat {
                        options,
                        auth: auth_data,
                        consumed: false,
                        waiting_reconnection: false,
                        expires_at: Instant::now() + self.ctx.seat_reservation_timeout,
                    },
                );
                self.ctx.sync_listing();
                let _ = respond.send(Ok(()));
            }
            Err(e) => {
                let _ = respond.send(Err(e));
            }
        }
    }

    async fn on_connect(
        &mut self,
        client: Client,
        reconnection_token: Option<String>,
        respond: oneshot::Sender<Result<()>>,
    ) {
        let session_id = client.session_id().to_string();
        client.attach_after_patch(self.ctx.after_patch.clone());
        client.attach_tap(self.ctx.tap.clone());

        // ---------------------------------------------------------------
        // Reconnection path
        // ---------------------------------------------------------------
        if let Some(token) = reconnection_token {
            let entry = self
                .ctx
                .reconnections
                .get(&token)
                .filter(|e| e.session_id == session_id)
                .map(|e| (e.session_id.clone(), e.client.clone()));

            let Some((_, previous)) = entry else {
                let _ = respond.send(Err(ServerError::new(
                    codes::MATCHMAKE_EXPIRED,
                    "reconnection token invalid or expired",
                )));
                return;
            };

            self.ctx.reconnections.remove(&token);
            self.ctx.reserved_seats.remove(&session_id);

            client.set_auth(previous.auth());
            if let Some(ud) = previous.user_data() {
                client.set_user_data(ud);
            }
            previous.set_state(ClientState::Reconnected);
            client.set_state(ClientState::Reconnecting);

            if let Some(pos) = self
                .ctx
                .clients
                .iter()
                .position(|c| c.session_id() == session_id)
            {
                self.ctx.clients[pos] = client.clone();
            } else {
                self.ctx.clients.push(client.clone());
            }

            self.room.on_reconnect(&mut self.ctx, client.clone()).await;

            client.set_state(ClientState::Reconnected);
            let new_token = generate_id();
            client.set_reconnection_token(new_token.clone());
            client.raw_now(protocol::join_room(&new_token, self.ctx.serializer_id(), None));
            self.ctx
                .tap_log(crate::room::RoomEventLog::new("sys", "reconnect").client(&session_id));
            if let Some(state) = self.ctx.full_state_for(&client) {
                self.ctx.set_view_snapshot(&session_id, state.clone());
                self.ctx.tap_log(
                    crate::room::RoomEventLog::new("out", "state_full")
                        .client(&session_id)
                        .payload(state.clone()),
                );
                client.raw_now(protocol::room_state(&state));
            }
            client.flush_pending();
            self.ctx.sync_listing();
            let _ = respond.send(Ok(()));
            return;
        }

        // ---------------------------------------------------------------
        // Normal join path
        // ---------------------------------------------------------------
        // Mark the seat consumed up front (keeping a tombstone in the map) so
        // a duplicate connection with the same session id — e.g. a second
        // WebSocket arriving while `on_join` is still running — is rejected
        // by the `consumed` branch instead of double-joining.
        let Some(seat) = self.ctx.reserved_seats.get_mut(&session_id) else {
            let _ = respond.send(Err(ServerError::seat_expired()));
            return;
        };
        if seat.consumed {
            let _ = respond.send(Err(ServerError::new(
                codes::MATCHMAKE_EXPIRED,
                "seat reservation already consumed",
            )));
            return;
        }
        seat.consumed = true;
        let (options, auth) = (seat.options.clone(), seat.auth.clone());

        client.set_auth(auth.clone());
        client.set_state(ClientState::Joining);
        self.ctx.clients.push(client.clone());

        let join_result = self
            .room
            .on_join(&mut self.ctx, client.clone(), options, auth)
            .await;

        // join decided — drop the consumed tombstone either way
        self.ctx.reserved_seats.remove(&session_id);

        // user may have closed the client inside on_join
        let early_leave = client.state() == ClientState::Leaving;

        match (join_result, early_leave) {
            (Ok(()), false) => {
                client.set_state(ClientState::Joined);
                self.ctx.had_clients = true;
                let token = generate_id();
                client.set_reconnection_token(token.clone());
                client.raw_now(protocol::join_room(&token, self.ctx.serializer_id(), None));
                self.ctx
                    .tap_log(crate::room::RoomEventLog::new("sys", "join").client(&session_id));
                if let Some(state) = self.ctx.full_state_for(&client) {
                    self.ctx.set_view_snapshot(&session_id, state.clone());
                    self.ctx.tap_log(
                        crate::room::RoomEventLog::new("out", "state_full")
                            .client(&session_id)
                            .payload(state.clone()),
                    );
                    client.raw_now(protocol::room_state(&state));
                }
                client.flush_pending();
                self.ctx.sync_listing();
                let _ = respond.send(Ok(()));
            }
            (join_result, _) => {
                let err = match join_result {
                    Err(e) => e,
                    Ok(()) => {
                        ServerError::new(codes::MATCHMAKE_UNHANDLED, "client left during on_join")
                    }
                };
                self.ctx
                    .clients
                    .retain(|c| c.session_id() != session_id);
                client.raw_now(protocol::error(err.code, &err.message));
                client.leave(Some(close_codes::WITH_ERROR), Some(&err.message));
                self.ctx.sync_listing();
                let _ = respond.send(Err(err));
            }
        }
    }

    async fn on_client_message(&mut self, client: Client, msg: ClientMessage) {
        // ignore events from stale (pre-reconnection) connections
        let Some(current) = self.ctx.get_client(client.session_id()) else {
            if matches!(msg, ClientMessage::Ping) {
                client.raw_now(protocol::ping());
            }
            return;
        };
        if current.inner.connection_id != client.inner.connection_id {
            return;
        }

        match msg {
            ClientMessage::Ping => client.raw_now(protocol::ping()),
            ClientMessage::Leave => {
                self.ctx
                    .tap_log(crate::room::RoomEventLog::new("in", "leave").client(client.session_id()));
                self.leave_client(client, close_codes::CONSENTED, true).await;
            }
            ClientMessage::Data(msg_type, payload) => {
                if !client.is_joined() {
                    return;
                }
                self.ctx.tap_log(
                    crate::room::RoomEventLog::new("in", "message")
                        .client(client.session_id())
                        .msg_type(&msg_type)
                        .payload(payload.clone()),
                );
                if !client.check_rate(self.ctx.max_messages_per_second) {
                    tracing::warn!(
                        "client {} exceeded message rate limit in room {}",
                        client.session_id(),
                        self.ctx.room_id()
                    );
                    return;
                }

                let handlers = self
                    .ctx
                    .message_handlers
                    .get(&msg_type)
                    .cloned()
                    .unwrap_or_default();
                let mut handled = !handlers.is_empty();
                for handler in handlers {
                    if let Err(e) =
                        handler(&mut *self.room, &mut self.ctx, client.clone(), payload.clone())
                            .await
                    {
                        tracing::error!("on_message({msg_type:?}) error: {e}");
                        client.error(e.code, &e.message);
                    }
                }

                let any_handlers = self.ctx.any_handlers.clone();
                if !any_handlers.is_empty() {
                    handled = true;
                    for handler in any_handlers {
                        if let Err(e) = handler(
                            &mut *self.room,
                            &mut self.ctx,
                            client.clone(),
                            msg_type.clone(),
                            payload.clone(),
                        )
                        .await
                        {
                            tracing::error!("on_any_message error: {e}");
                        }
                    }
                }

                if !handled {
                    tracing::debug!(
                        "room {}: unhandled message type {msg_type:?}",
                        self.ctx.room_id()
                    );
                }
            }
            ClientMessage::DataBytes(msg_type, payload) => {
                if !client.is_joined() || !client.check_rate(self.ctx.max_messages_per_second) {
                    return;
                }
                self.ctx.tap_log(
                    crate::room::RoomEventLog::new("in", "bytes")
                        .client(client.session_id())
                        .msg_type(&msg_type)
                        .bytes(payload.len()),
                );
                let handlers = self
                    .ctx
                    .bytes_handlers
                    .get(&msg_type)
                    .cloned()
                    .unwrap_or_default();
                for handler in handlers {
                    if let Err(e) =
                        handler(&mut *self.room, &mut self.ctx, client.clone(), payload.clone())
                            .await
                    {
                        tracing::error!("on_message_bytes({msg_type:?}) error: {e}");
                        client.error(e.code, &e.message);
                    }
                }
            }
        }
    }

    async fn on_disconnected(&mut self, client: Client, code: u16) {
        let Some(current) = self.ctx.get_client(client.session_id()) else {
            return;
        };
        // ignore disconnects from stale connections (client already reconnected)
        if current.inner.connection_id != client.inner.connection_id {
            return;
        }
        self.leave_client(client, code, false).await;
    }

    /// Shared leave logic. `consented` skips `on_drop` (explicit leave).
    async fn leave_client(&mut self, client: Client, code: u16, consented: bool) {
        if matches!(client.state(), ClientState::Leaving | ClientState::Closed) {
            return;
        }
        client.set_state(ClientState::Leaving);

        if !consented && !self.ctx.disposing {
            self.room.on_drop(&mut self.ctx, client.clone(), code).await;
        }

        // did on_drop allow reconnection?
        let token = client.reconnection_token();
        if self.ctx.reconnections.contains_key(&token) {
            // client stays in the list as Reconnecting; recycle its transport
            client.close_transport(code, "reconnecting");
            return;
        }

        self.room.on_leave(&mut self.ctx, client.clone(), code).await;
        client.set_state(ClientState::Closed);
        self.ctx
            .tap_log(crate::room::RoomEventLog::new("sys", "leave").client(client.session_id()).bytes(code as usize));
        if consented {
            client.leave(Some(code), None);
        }
        self.ctx.drop_view_snapshot(client.session_id());
        self.ctx.clients.retain(|c| c.session_id() != client.session_id());
        self.ctx.sync_listing();
    }

    async fn handle_deadlines(&mut self) {
        let now = Instant::now();

        // expired reconnections → finalize leave
        let expired_tokens: Vec<String> = self
            .ctx
            .reconnections
            .iter()
            .filter(|(_, e)| e.expires_at.is_some_and(|t| t <= now))
            .map(|(t, _)| t.clone())
            .collect();
        for token in expired_tokens {
            let Some(entry) = self.ctx.reconnections.remove(&token) else {
                continue;
            };
            self.ctx.reserved_seats.remove(&entry.session_id);
            let client = entry.client;
            self.room
                .on_leave(&mut self.ctx, client.clone(), close_codes::FAILED_TO_RECONNECT)
                .await;
            client.set_state(ClientState::Closed);
            self.ctx.drop_view_snapshot(client.session_id());
            self.ctx.clients.retain(|c| c.session_id() != client.session_id());
            self.ctx.sync_listing();
        }

        // expired unconsumed seat reservations
        let expired_seats: Vec<String> = self
            .ctx
            .reserved_seats
            .iter()
            .filter(|(_, s)| !s.consumed && !s.waiting_reconnection && s.expires_at <= now)
            .map(|(id, _)| id.clone())
            .collect();
        if !expired_seats.is_empty() {
            for id in expired_seats {
                self.ctx.reserved_seats.remove(&id);
            }
            self.ctx.sync_listing();
        }

        // due user timers
        let mut i = 0;
        let mut due = Vec::new();
        while i < self.ctx.timers.len() {
            if self.ctx.timers[i].at <= now {
                due.push(self.ctx.timers.remove(i));
            } else {
                i += 1;
            }
        }
        for timer in due {
            (timer.cb)(&mut *self.room, &mut self.ctx).await;
        }
    }

    async fn dispose(&mut self, code: u16) {
        if self.ctx.disposing {
            return;
        }
        self.ctx.disposing = true;
        self.ctx.request_dispose = None;

        // Final snapshot decision — before clearing clients/reconnections so
        // persisted reconnection seats survive the restart.
        if let Some(p) = self.ctx.persistence.clone() {
            if p.config.delete_on_dispose {
                self.ctx.delete_snapshot();
            } else if p.config.save_on_dispose {
                self.ctx.persist_now();
            }
        }

        let clients = std::mem::take(&mut self.ctx.clients);
        for client in clients {
            client.set_state(ClientState::Closed);
            client.leave(Some(code), Some("room disposed"));
        }
        self.ctx.reserved_seats.clear();
        self.ctx.reconnections.clear();

        self.room.on_dispose(&mut self.ctx).await;
        self.ctx
            .tap_log(crate::room::RoomEventLog::new("sys", "dispose"));

        self.ctx.driver.remove(self.ctx.room_id());
        let _ = self
            .ctx
            .lobby_tx
            .send(MatchmakerEvent::RoomRemoved(self.ctx.room_id().to_string()));

        if let Some(on_dispose) = self.on_dispose.take() {
            on_dispose();
        }
    }
}
