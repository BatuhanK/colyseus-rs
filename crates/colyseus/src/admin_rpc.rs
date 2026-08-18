//! Server-side admin RPCs.
//!
//! Register custom, token-guarded operations that a trusted backend can call
//! over HTTP (`POST /admin/api/rpc/{name}`). See [`crate::Server::admin_rpc`]
//! and [`AdminRpc`].

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::actor::{RoomEvent, RoomHandle, RoomInspection};
use crate::driver::{RoomListing, RoomQuery};
use crate::error::{close_codes, codes, Result, ServerError};
use crate::matchmaker::{CreateRoomOutcome, MatchMaker, MatchmakerEvent};
use crate::presence::Presence;
use crate::protocol::MessageType;
use crate::room::{BoxFuture, Room, RoomContext};
use crate::state::StateEdit;

/// How long an admin room RPC waits for the room actor to respond.
const ROOM_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// A registered RPC handler: `(ctx, params JSON) -> response JSON`.
pub(crate) type RpcFn =
    Arc<dyn Fn(AdminContext, Value) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

/// A custom admin RPC registration: the erased handler plus the Rust
/// param/response type names, surfaced by `GET /admin/api/schema`.
#[derive(Clone)]
pub(crate) struct AdminRpcRegistration {
    pub name: String,
    pub handler: RpcFn,
    pub params_type: &'static str,
    pub response_type: &'static str,
}

/// A clonable handle giving admin RPCs safe access to the matchmaker and its
/// rooms. Operations run outside any room actor (they talk to rooms through
/// their mailboxes); use [`AdminContext::command_room`] when you need typed
/// `&mut MyRoom` access.
#[derive(Clone)]
pub struct AdminContext {
    mm: MatchMaker,
}

impl AdminContext {
    pub(crate) fn new(mm: MatchMaker) -> Self {
        Self { mm }
    }

    /// This server's process id.
    pub fn process_id(&self) -> String {
        self.mm.process_id().to_string()
    }

    /// List rooms, optionally filtered by room type name.
    pub fn list_rooms(&self, name: Option<&str>) -> Vec<RoomListing> {
        self.mm.query(name, Default::default())
    }

    /// Run a parameterized [`RoomQuery`] (operators, sort, pagination) — the
    /// SDK counterpart of `GET /admin/api/rooms`.
    pub fn query_rooms(&self, query: RoomQuery) -> Result<crate::driver::RoomQueryResult> {
        self.mm.query_rooms(None, query)
    }

    /// Per-room-type status counts (open / waiting / full / locked …).
    pub fn room_stats(&self, name: Option<&str>) -> crate::matchmaker::RoomStats {
        self.mm.room_stats(name)
    }

    /// A single room's public listing, by id.
    pub fn room(&self, room_id: &str) -> Option<RoomListing> {
        self.mm.driver().get(room_id)
    }

    /// Create a room server-side (no seat is reserved). When the room type
    /// declares `unique_by` and a live room with the same key exists, it is
    /// returned with `created: false`.
    pub async fn create_room(&self, room_name: &str, options: Value) -> Result<CreateRoomOutcome> {
        self.mm.create_room(room_name, options).await
    }

    /// Inspect a room's internals (state, clients, seats, reconnections).
    pub async fn inspect_room(&self, room_id: &str) -> Result<RoomInspection> {
        let handle = self
            .mm
            .room_handle(room_id)
            .ok_or_else(|| ServerError::room_not_found(room_id))?;
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(RoomEvent::Inspect { respond: tx })
            .map_err(|_| ServerError::room_not_found(room_id))?;
        tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .map_err(|_| ServerError::new(codes::APPLICATION_ERROR, "room did not respond"))?
            .map_err(|_| ServerError::room_not_found(room_id))
    }

    /// Dispose a room (all clients disconnected, `on_dispose` runs).
    pub fn dispose_room(&self, room_id: &str) -> bool {
        let Some(handle) = self.mm.room_handle(room_id) else {
            return false;
        };
        handle
            .tx
            .send(RoomEvent::Dispose {
                code: close_codes::CONSENTED,
            })
            .is_ok()
    }

    /// Lock a room against new seat reservations.
    pub fn lock_room(&self, room_id: &str) -> bool {
        self.set_locked(room_id, true)
    }

    /// Unlock a room.
    pub fn unlock_room(&self, room_id: &str) -> bool {
        self.set_locked(room_id, false)
    }

    fn set_locked(&self, room_id: &str, locked: bool) -> bool {
        let Some(handle) = self.mm.room_handle(room_id) else {
            return false;
        };
        handle.tx.send(RoomEvent::SetLocked(locked)).is_ok()
    }

    /// Force-disconnect a client.
    pub fn kick(&self, room_id: &str, session_id: &str) -> bool {
        let Some(handle) = self.mm.room_handle(room_id) else {
            return false;
        };
        handle
            .tx
            .send(RoomEvent::Kick {
                session_id: session_id.to_string(),
            })
            .is_ok()
    }

    /// Send a message to one client (`session_id: Some`) or broadcast to all
    /// (`session_id: None`).
    pub fn send_message(
        &self,
        room_id: &str,
        session_id: Option<&str>,
        msg_type: impl Into<MessageType>,
        payload: Value,
    ) -> bool {
        let Some(handle) = self.mm.room_handle(room_id) else {
            return false;
        };
        handle
            .tx
            .send(RoomEvent::AdminMessage {
                session_id: session_id.map(|s| s.to_string()),
                msg_type: msg_type.into(),
                payload,
            })
            .is_ok()
    }

    /// Apply a validated state edit (set/remove at a JSON-pointer-ish path).
    /// The change is delivered to clients on the next patch broadcast.
    pub async fn edit_state(
        &self,
        room_id: &str,
        path: &[&str],
        edit: StateEdit,
    ) -> std::result::Result<(), String> {
        let Some(handle) = self.mm.room_handle(room_id) else {
            return Err("room not found".to_string());
        };
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(RoomEvent::EditState {
                path: path.iter().map(|s| s.to_string()).collect(),
                edit,
                respond: tx,
            })
            .is_err()
        {
            return Err("room not found".to_string());
        }
        tokio::time::timeout(Duration::from_secs(3), rx)
            .await
            .map_err(|_| "room did not respond".to_string())?
            .map_err(|_| "room not found".to_string())?
    }

    /// Set a value at a JSON-pointer-ish path (e.g. `["players", "abc", "score"]`).
    pub async fn set_state_path(
        &self,
        room_id: &str,
        path: &[&str],
        value: Value,
    ) -> std::result::Result<(), String> {
        self.edit_state(room_id, path, StateEdit::Set(value)).await
    }

    /// Remove a value at a JSON-pointer-ish path.
    pub async fn remove_state_path(&self, room_id: &str, path: &[&str]) -> std::result::Result<(), String> {
        self.edit_state(room_id, path, StateEdit::Remove).await
    }

    /// Inject a typed command into a room's actor. It runs sequentially with
    /// the room's own handlers, so it can safely mutate `&mut MyRoom` and its
    /// state. Returns `false` when the room is unknown or already gone.
    ///
    /// ```ignore
    /// ctx.command_room::<GameRoom, _>(&params.room_id, |room, ctx| Box::pin(async move {
    ///     room.grant_coins(ctx, params.amount);
    /// }));
    /// ```
    pub fn command_room<R, F>(&self, room_id: &str, f: F) -> bool
    where
        R: Room,
        F: for<'a> FnOnce(&'a mut R, &'a mut RoomContext) -> BoxFuture<'a, ()> + Send + 'static,
    {
        let Some(handle) = self.mm.room_handle(room_id) else {
            return false;
        };
        handle.sender().send::<R, F>(f)
    }

    /// Call a room-based RPC: run `T` on the room actor (`&mut R` +
    /// `&mut RoomContext`, sequential with its handlers) and return its typed
    /// response. Unlike [`Self::command_room`], this is request/response.
    ///
    /// ```ignore
    /// let score: Score = ctx.call_room::<GameRoom, GetScore>(&room_id, GetScoreParams {
    ///     player: "p1".into(),
    /// }).await?;
    /// ```
    pub async fn call_room<R, T>(&self, room_id: &str, params: T::Params) -> Result<T::Response>
    where
        R: Room,
        T: RoomRpc<R>,
    {
        let handle = self
            .mm
            .room_handle(room_id)
            .ok_or_else(|| ServerError::room_not_found(room_id))?;
        let value = dispatch_room_rpc::<R, T>(handle, params).await?;
        serde_json::from_value(value)
            .map_err(|e| ServerError::new(codes::APPLICATION_ERROR, e.to_string()))
    }

    /// Presence (pub/sub + KV) for cross-room / cross-process coordination.
    pub fn presence(&self) -> Arc<dyn Presence> {
        self.mm.presence()
    }

    /// Subscribe to lobby events (room created / updated / removed).
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<MatchmakerEvent> {
        self.mm.subscribe()
    }
}

/// A server-side admin RPC, callable from a trusted backend over HTTP.
///
/// Implement this trait and register it with [`crate::Server::admin_rpc`]:
///
/// ```ignore
/// use colyseus::{AdminContext, AdminRpc, Result};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct ResetRoom { room_id: String }
///
/// #[derive(Serialize)]
/// struct ResetRoomResult { ok: bool }
///
/// #[async_trait]
/// impl AdminRpc for ResetRoom {
///     type Params = ResetRoom;
///     type Response = ResetRoomResult;
///
///     async fn call(params: Self::Params, ctx: AdminContext) -> Result<Self::Response> {
///         ctx.dispose_room(&params.room_id);
///         Ok(ResetRoomResult { ok: true })
///     }
/// }
///
/// server.admin_rpc::<ResetRoom>("resetRoom");
/// ```
///
/// For a params-less RPC use `type Params = ()` (the HTTP body may be empty).
///
/// Callers may send an `Idempotency-Key` header: a successful response is
/// then cached for ~30s and replayed for duplicate keys (errors are never
/// cached). Handlers must therefore be side-effect-safe under replay within
/// that window — a retried call returns the original response without
/// re-running the handler.
#[async_trait]
pub trait AdminRpc: Send + 'static {
    type Params: DeserializeOwned + Send;
    type Response: Serialize + Send;

    async fn call(params: Self::Params, ctx: AdminContext) -> Result<Self::Response>;
}

/// Build the erased [`RpcFn`] for a concrete [`AdminRpc`] implementation.
pub(crate) fn rpc_fn<T: AdminRpc>() -> RpcFn {
    Arc::new(|ctx: AdminContext, params: Value| {
        Box::pin(async move {
            let params: T::Params = serde_json::from_value(params)
                .map_err(|e| ServerError::new(codes::INVALID_PAYLOAD, e.to_string()))?;
            let resp = T::call(params, ctx).await?;
            serde_json::to_value(resp)
                .map_err(|e| ServerError::new(codes::APPLICATION_ERROR, e.to_string()))
        })
    })
}

// ---------------------------------------------------------------------
// Room-based RPCs
// ---------------------------------------------------------------------

/// A room-based admin RPC: runs on the room actor with `&mut R` +
/// `&mut RoomContext` (sequentially with the room's own handlers), and returns
/// a typed response to the caller.
///
/// Register it with [`crate::Server::room_rpc`] and call it over HTTP:
/// `POST /admin/api/rooms/{roomId}/rpc/{name}` — or from another admin RPC via
/// [`AdminContext::call_room`].
///
/// ```ignore
/// use colyseus::{Room, RoomContext, RoomRpc, Result};
/// use serde::{Deserialize, Serialize};
///
/// struct GameRoom;
/// impl Room for GameRoom {}
///
/// #[derive(Deserialize)]
/// #[serde(rename_all = "camelCase")]
/// struct GetScore { player: String }
///
/// #[derive(Serialize, Deserialize)]
/// struct Score { points: i64 }
///
/// #[async_trait]
/// impl RoomRpc<GameRoom> for GetScore {
///     type Params = GetScore;
///     type Response = Score;
///     async fn call(_room: &mut GameRoom, ctx: &mut RoomContext, _p: Self::Params) -> Result<Score> {
///         Ok(Score { points: ctx.state::<GameState>().map(|s| s.score).unwrap_or(0) })
///     }
/// }
///
/// server.room_rpc::<GameRoom, GetScore>("getScore");
/// ```
#[async_trait]
pub trait RoomRpc<R: Room>: Send + 'static {
    type Params: DeserializeOwned + Send + 'static;
    type Response: Serialize + DeserializeOwned + Send + 'static;

    async fn call(room: &mut R, ctx: &mut RoomContext, params: Self::Params) -> Result<Self::Response>;
}

/// An erased room-RPC handler: `(room handle, params JSON) -> response JSON`.
pub(crate) type RoomRpcHandler =
    Arc<dyn Fn(RoomHandle, Value) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

/// Force a closure to be checked against a higher-ranked room-RPC signature.
fn hrtb_room_rpc<F>(f: F) -> F
where
    F: for<'a> FnOnce(&'a mut dyn Room, &'a mut RoomContext) -> BoxFuture<'a, Result<Value>>
        + Send,
{
    f
}

/// Run a typed room RPC on the given room actor and return its serialized response.
async fn dispatch_room_rpc<R, T>(handle: RoomHandle, params: T::Params) -> Result<Value>
where
    R: Room,
    T: RoomRpc<R>,
{
    let (tx, rx) = oneshot::channel();
    let cmd = hrtb_room_rpc(move |room: &mut dyn Room, ctx: &mut RoomContext| {
        let Some(room) = room.as_any_mut().downcast_mut::<R>() else {
            return Box::pin(async {
                Err(ServerError::new(codes::APPLICATION_ERROR, "room type mismatch"))
            });
        };
        Box::pin(async move {
            let resp = T::call(room, ctx, params).await?;
            serde_json::to_value(resp)
                .map_err(|e| ServerError::new(codes::APPLICATION_ERROR, e.to_string()))
        })
    });

    handle
        .tx
        .send(RoomEvent::CallRoomRpc {
            cmd: Box::new(cmd),
            respond: tx,
        })
        .map_err(|_| ServerError::room_not_found(&handle.room_id))?;

    tokio::time::timeout(ROOM_RPC_TIMEOUT, rx)
        .await
        .map_err(|_| ServerError::new(codes::APPLICATION_ERROR, "room did not respond"))?
        .map_err(|_| ServerError::room_not_found(&handle.room_id))?
}

/// Build the erased [`RoomRpcHandler`] for a concrete [`RoomRpc`] implementation.
pub(crate) fn room_rpc_fn<R, T>() -> RoomRpcHandler
where
    R: Room,
    T: RoomRpc<R>,
{
    Arc::new(|handle: RoomHandle, params: Value| {
        Box::pin(async move {
            let params: T::Params = serde_json::from_value(params)
                .map_err(|e| ServerError::new(codes::INVALID_PAYLOAD, e.to_string()))?;
            dispatch_room_rpc::<R, T>(handle, params).await
        })
    })
}
