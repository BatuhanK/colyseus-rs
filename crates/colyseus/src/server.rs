//! The server: HTTP matchmaking API + WebSocket transport (axum).
//!
//! HTTP API:
//! - `POST /matchmake/{method}/{roomName}` — `method` is one of
//!   `joinOrCreate`, `create`, `join`, `joinById`, `reconnect`
//!   (`joinById`/`reconnect` take a room id in place of the room name).
//!   Body: JSON client options. Response: a seat reservation.
//! - `GET /rooms` / `GET /rooms/{roomName}` — room listing queries, with
//!   optional filters (`clients=1`, `clients.gte=1`, any `filter_by` field),
//!   sorting (`sort=createdAt:desc`) and pagination (`limit`/`offset`).
//!
//! WebSocket:
//! - `GET /ws/{roomId}?sessionId=...&reconnectionToken=...` — binary msgpack
//!   frames as described in [`crate::protocol`].

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use axum::extract::ws::{CloseFrame, Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tower_http::cors::CorsLayer;

use crate::actor::{RoomEvent, RoomHandle};
use crate::client::{Client, Outbound};
use crate::driver::{Driver, RoomQuery};
use crate::error::{close_codes, codes, Result, ServerError};
use crate::matchmaker::{AuthContext, MatchMaker, RegisteredHandler};
use crate::presence::Presence;
use crate::protocol::{self, ClientMessage};
use crate::room::{BoxFuture, Room};
use crate::snapshot::PersistenceConfig;

/// A bootstrap closure run by [`Server::listen`] before accepting traffic.
type OnStartFn = Box<dyn FnOnce(MatchMaker) -> BoxFuture<'static, Result<()>> + Send>;

/// The game server. Register room types, then [`Server::listen`].
pub struct Server {
    handlers: HashMap<String, RegisteredHandler>,
    presence: Option<Arc<dyn Presence>>,
    driver: Option<Arc<dyn Driver>>,
    public_address: Option<String>,
    extra_router: Option<Router>,
    cors: bool,
    greet: bool,
    ws_read_buffer_size: Option<usize>,
    ws_write_buffer_size: Option<usize>,
    /// `true` = serve the admin panel UI at `/admin`.
    admin_panel_enabled: bool,
    /// Bearer token guarding the `/admin/api/*` endpoints (panel + SDK RPCs).
    admin_token: Option<String>,
    /// Custom admin RPCs registered via [`Server::admin_rpc`].
    admin_rpcs: Vec<crate::admin_rpc::AdminRpcRegistration>,
    /// Custom room RPCs registered via [`Server::room_rpc`].
    room_rpcs: Vec<(String, crate::admin_rpc::RoomRpcHandler)>,
    /// Snapshot persistence configuration.
    persistence: Option<PersistenceConfig>,
    /// Bootstrap closure run by [`Server::listen`] before accepting traffic.
    on_start: Option<OnStartFn>,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Server {
            handlers: HashMap::new(),
            presence: None,
            driver: None,
            public_address: None,
            extra_router: None,
            cors: true,
            greet: true,
            ws_read_buffer_size: None,
            ws_write_buffer_size: None,
            admin_panel_enabled: false,
            admin_token: None,
            admin_rpcs: Vec::new(),
            room_rpcs: Vec::new(),
            persistence: None,
            on_start: None,
        }
    }

    /// Override the default in-process presence.
    pub fn presence(mut self, presence: Arc<dyn Presence>) -> Self {
        self.presence = Some(presence);
        self
    }

    /// Override the default in-memory room-listing driver (the scale-out
    /// seam — see [`Driver`]).
    pub fn driver(mut self, driver: Arc<dyn Driver>) -> Self {
        self.driver = Some(driver);
        self
    }

    /// Advertised address included in seat reservations (for clients that
    /// connect through a load balancer / proxy).
    pub fn public_address(mut self, address: impl Into<String>) -> Self {
        self.public_address = Some(address.into());
        self
    }

    /// Disable the permissive CORS layer.
    pub fn disable_cors(mut self) -> Self {
        self.cors = false;
        self
    }

    /// Disable the startup banner.
    pub fn disable_greet(mut self) -> Self {
        self.greet = false;
        self
    }

    /// Enable the built-in admin panel at `/admin` (the `@colyseus/monitor`
    /// counterpart). Optionally protect it with a bearer token.
    ///
    /// ```ignore
    /// Server::new().admin_panel(Some("secret".to_string()))
    /// ```
    ///
    /// The same token also guards the admin RPC API (`/admin/api/rpc/*`).
    /// If you only need the RPC API (no panel UI), use [`Server::admin_token`].
    pub fn admin_panel(mut self, token: Option<String>) -> Self {
        self.admin_panel_enabled = true;
        self.admin_token = token;
        self
    }

    /// Enable the admin API (custom RPCs + the `/admin/api/*` endpoints)
    /// protected by a bearer token, without serving the `/admin` panel UI.
    ///
    /// ```ignore
    /// Server::new().admin_token(Some("backend-secret".to_string()))
    /// ```
    pub fn admin_token(mut self, token: Option<String>) -> Self {
        self.admin_token = token;
        self
    }

    /// Register a custom admin RPC, callable from a trusted backend via
    /// `POST /admin/api/rpc/{name}` with a bearer token.
    ///
    /// `T` implements [`AdminRpc`](crate::admin_rpc::AdminRpc): its `Params`
    /// is deserialized from the JSON request body and its `Response` is
    /// serialized as the JSON reply.
    ///
    /// ```ignore
    /// use colyseus::{AdminRpc, AdminContext, Result};
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
    ///     async fn call(p: Self::Params, ctx: AdminContext) -> Result<Self::Response> {
    ///         ctx.dispose_room(&p.room_id);
    ///         Ok(ResetRoomResult { ok: true })
    ///     }
    /// }
    ///
    /// server.admin_token(Some("backend-secret".into()))
    ///       .admin_rpc::<ResetRoom>("resetRoom");
    /// ```
    pub fn admin_rpc<T: crate::admin_rpc::AdminRpc>(mut self, name: &str) -> Self {
        self.admin_rpcs
            .push(crate::admin_rpc::AdminRpcRegistration {
                name: name.to_string(),
                handler: crate::admin_rpc::rpc_fn::<T>(),
                params_type: std::any::type_name::<T::Params>(),
                response_type: std::any::type_name::<T::Response>(),
            });
        self
    }

    /// Register a room-based admin RPC, callable via
    /// `POST /admin/api/rooms/{roomId}/rpc/{name}` with a bearer token.
    ///
    /// Unlike [`Server::admin_rpc`], the handler runs **on the room actor** with
    /// typed `&mut R` + `&mut RoomContext` access (sequentially with the room's
    /// own handlers) and returns its response to the caller.
    ///
    /// ```ignore
    /// use colyseus::{Room, RoomRpc, RoomContext, Result};
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// #[serde(rename_all = "camelCase")]
    /// struct GetScore { player: String }
    ///
    /// #[derive(Serialize)]
    /// struct Score { points: i64 }
    ///
    /// #[async_trait]
    /// impl RoomRpc<GameRoom> for GetScore {
    ///     type Params = GetScore;
    ///     type Response = Score;
    ///     async fn call(room: &mut GameRoom, ctx: &mut RoomContext, p: GetScore) -> Result<Score> {
    ///         Ok(Score { points: ctx.state::<GameState>().map(|s| s.score).unwrap_or(0) })
    ///     }
    /// }
    ///
    /// server.room_rpc::<GameRoom, GetScore>("getScore");
    /// ```
    pub fn room_rpc<R, T>(mut self, name: &str) -> Self
    where
        R: crate::room::Room,
        T: crate::admin_rpc::RoomRpc<R>,
    {
        self.room_rpcs
            .push((name.to_string(), crate::admin_rpc::room_rpc_fn::<R, T>()));
        self
    }

    /// Enable room persistence. Rooms snapshot their state (public + internal)
    /// to a [`SnapshotStore`](crate::snapshot::SnapshotStore) and are restored
    /// automatically on startup — before any traffic is accepted.
    ///
    /// ```ignore
    /// use colyseus::snapshot::{FileSnapshotStore, PersistenceConfig};
    ///
    /// let server = Server::new().persistence(PersistenceConfig::new(
    ///     FileSnapshotStore::new("./snapshots"),
    /// ));
    /// ```
    pub fn persistence(mut self, config: PersistenceConfig) -> Self {
        self.persistence = Some(config);
        self
    }

    /// Tune per-connection WebSocket buffers (bytes).
    ///
    /// The underlying defaults are large (~128 KiB read + ~128 KiB write per
    /// connection), which dominates memory usage at high connection counts.
    /// For many small messages, e.g. `(16 * 1024, 32 * 1024)` dramatically
    /// lowers per-connection memory.
    pub fn ws_buffer_sizes(mut self, read: usize, write: usize) -> Self {
        self.ws_read_buffer_size = Some(read);
        self.ws_write_buffer_size = Some(write);
        self
    }

    /// Merge additional axum routes into the server's router.
    pub fn routes(mut self, router: Router) -> Self {
        self.extra_router = Some(match self.extra_router {
            Some(existing) => existing.merge(router),
            None => router,
        });
        self
    }

    /// Register a bootstrap closure run by [`Server::listen`] — after the
    /// matchmaker is built and persisted rooms are restored, but before any
    /// connection is accepted. The [`MatchMaker`] handle exposes
    /// `create_room` / `restore_all` / `presence` / `subscribe`.
    ///
    /// ```ignore
    /// let server = Server::new().on_start(|mm| async move {
    ///     mm.create_room("chat", json!({})).await?;
    ///     Ok(())
    /// });
    /// ```
    ///
    /// Only `listen` runs it — when embedding via [`Server::build`], run your
    /// bootstrap against the returned matchmaker yourself.
    pub fn on_start<F, Fut>(mut self, f: F) -> Self
    where
        F: FnOnce(MatchMaker) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_start = Some(Box::new(move |mm| Box::pin(f(mm))));
        self
    }

    /// Define a room type. Returns the handler for further configuration
    /// (`filter_by`, `sort_by`, `default_options`).
    ///
    /// ```ignore
    /// server.define("game", GameRoom::new).filter_by(&["mode"]).sort_by(&[("clients", 1)]);
    /// ```
    pub fn define<R, F>(&mut self, name: &str, factory: F) -> &mut RegisteredHandler
    where
        R: Room,
        F: Fn() -> R + Send + Sync + 'static,
    {
        self.handlers
            .insert(name.to_string(), RegisteredHandler::new::<R, F>(name, factory));
        self.handlers.get_mut(name).unwrap()
    }

    pub fn remove_room_type(&mut self, name: &str) {
        self.handlers.remove(name);
    }

    /// Build the axum router and the matchmaker handle. Useful for embedding
    /// into an existing axum app or for tests.
    pub fn build(self) -> (Router, MatchMaker) {
        let mm = MatchMaker::new(self.presence, self.driver, self.public_address, self.persistence);
        for (_, handler) in self.handlers {
            mm.register(handler);
        }

        let state = AppState {
            mm: mm.clone(),
            ws_read_buffer_size: self.ws_read_buffer_size,
            ws_write_buffer_size: self.ws_write_buffer_size,
        };

        let mut app = Router::new()
            .route("/matchmake/{method}/{room_name}", post(matchmake_handler))
            .route("/rooms", get(list_rooms))
            .route("/rooms/{room_name}", get(list_rooms_by_name))
            .route("/ws/{room_id}", get(ws_handler))
            .with_state(state);

        if self.cors {
            app = app.layer(CorsLayer::permissive());
        }
        if let Some(extra) = self.extra_router {
            app = app.merge(extra);
        }
        if self.admin_panel_enabled || self.admin_token.is_some() || !self.admin_rpcs.is_empty() || !self.room_rpcs.is_empty() {
            if self.admin_panel_enabled && self.admin_token.is_none() {
                tracing::warn!("admin panel enabled WITHOUT a token — anyone can access /admin");
            }
            app = app.merge(crate::admin::router(
                mm.clone(),
                self.admin_token,
                self.admin_panel_enabled,
                self.admin_rpcs,
                self.room_rpcs,
            ));
        }

        (app, mm)
    }

    /// Bind, serve, and handle graceful shutdown (Ctrl-C / SIGTERM).
    pub async fn listen(mut self, addr: &str) -> Result<()> {
        self.greet_banner(addr);
        let on_start = self.on_start.take();
        let (app, mm) = self.build();

        // Restore persisted rooms before accepting any traffic.
        let restored = mm.restore_all().await;
        tracing::info!("restored {} room(s) from snapshots", restored.len());

        // Bootstrap hook (create bootstrap rooms, subscribe to lobby events…).
        if let Some(on_start) = on_start {
            on_start(mm.clone()).await?;
        }

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| ServerError::new(codes::APPLICATION_ERROR, e.to_string()))?;

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                shutdown_signal().await;
                tracing::info!("shutting down: disposing all rooms...");
                mm.shutdown().await;
            })
            .await
            .map_err(|e| ServerError::new(codes::APPLICATION_ERROR, e.to_string()))?;
        Ok(())
    }

    fn greet_banner(&self, addr: &str) {
        if self.greet {
            println!("⚔️  colyseus-rs — listening on ws://{addr}");
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

#[derive(Clone)]
struct AppState {
    mm: MatchMaker,
    ws_read_buffer_size: Option<usize>,
    ws_write_buffer_size: Option<usize>,
}

// ----------------------------------------------------------------------
// HTTP matchmaking
// ----------------------------------------------------------------------

async fn matchmake_handler(
    State(state): State<AppState>,
    Path((method, room_name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> Response {
    let mm = state.mm;
    if mm.is_shutting_down() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    let (options, filter) = split_matchmake_body(body.map(|Json(v)| v).unwrap_or(Value::Null));
    let auth = AuthContext {
        token: bearer_token(&headers),
        headers: headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.to_string(), v.to_string())))
            .collect(),
        ip: header(&headers, "x-forwarded-for")
            .or_else(|| header(&headers, "x-real-ip")),
    };
    let idempotency_key = header(&headers, "idempotency-key")
        .filter(|k| !k.is_empty())
        .map(|k| format!("{method}:{room_name}:{k}"));

    // internal room types can't be created through the public API
    if matches!(method.as_str(), "joinOrCreate" | "create") {
        if let Ok(handler) = mm.handler(&room_name) {
            if handler.is_internal() {
                return error_response(ServerError::new(
                    codes::MATCHMAKE_NO_HANDLER,
                    format!("room type \"{room_name}\" is internal"),
                ));
            }
        }
    }

    // an optional operator-style filter applies to joinOrCreate / join
    let filter = match filter {
        Some(f) if matches!(method.as_str(), "joinOrCreate" | "join") => {
            match mm.parse_match_filter(&room_name, &f) {
                Ok(conditions) => Some(conditions),
                Err(e) => return error_response(e),
            }
        }
        // `create` always makes a new room; `joinById` targets a specific one
        _ => None,
    };

    // replay the cached reservation for a duplicate Idempotency-Key
    if let Some(key) = &idempotency_key {
        if let Some(reservation) = mm.idempotency_get(key) {
            return Json(reservation).into_response();
        }
    }

    let result = match method.as_str() {
        "joinOrCreate" => {
            mm.join_or_create_with_filter(&room_name, options, filter.as_deref().unwrap_or(&[]), auth).await
        }
        "create" => mm.create(&room_name, options, auth).await,
        "join" => {
            mm.join_with_filter(&room_name, options, filter.as_deref().unwrap_or(&[]), auth).await
        }
        "joinById" => mm.join_by_id(&room_name, options, auth).await,
        "reconnect" => {
            let token = options
                .get("reconnectionToken")
                .and_then(|t| t.as_str())
                .unwrap_or_default()
                .to_string();
            if token.is_empty() {
                Err(ServerError::new(
                    codes::MATCHMAKE_UNHANDLED,
                    "'reconnectionToken' must be provided for reconnection",
                ))
            } else {
                mm.reconnect(&room_name, &token).await
            }
        }
        other => Err(ServerError::new(
            codes::MATCHMAKE_NO_HANDLER,
            format!("invalid matchmaking method \"{other}\""),
        )),
    };

    match result {
        Ok(reservation) => {
            if let Some(key) = idempotency_key {
                mm.idempotency_put(key, reservation.clone());
            }
            Json(reservation).into_response()
        }
        Err(e) => error_response(e),
    }
}

/// Split a matchmake body into `(options, filter)`. The extended form
/// `{ "options": {…}, "filter": {…} }` is detected by the reserved keys
/// `options` / `filter`; any other body is treated as bare client options
/// (backwards compatible).
fn split_matchmake_body(body: Value) -> (Value, Option<Value>) {
    match &body {
        Value::Object(map) if map.contains_key("options") || map.contains_key("filter") => {
            let options = map.get("options").cloned().unwrap_or(Value::Null);
            let filter = map.get("filter").cloned();
            (options, filter)
        }
        _ => (body, None),
    }
}

fn error_response(e: ServerError) -> Response {
    let status = StatusCode::from_u16(e.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(json!({ "code": e.code, "error": e.message }))).into_response()
}

async fn list_rooms(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    match RoomQuery::from_params(&params) {
        Ok(query) => match state.mm.query_rooms(None, cap_limit(query, 200)) {
            Ok(result) => Json(result).into_response(),
            Err(e) => error_response(e),
        },
        Err(message) => bad_request(message),
    }
}

async fn list_rooms_by_name(
    State(state): State<AppState>,
    Path(room_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    match RoomQuery::from_params(&params) {
        Ok(query) => match state.mm.query_rooms(Some(&room_name), cap_limit(query, 200)) {
            Ok(result) => Json(result).into_response(),
            Err(e) => error_response(e),
        },
        Err(message) => bad_request(message),
    }
}

/// Clamp the public listing's page size so a single query can't dump the
/// whole room table.
fn cap_limit(mut query: RoomQuery, max: usize) -> RoomQuery {
    query.limit = Some(query.limit.unwrap_or(max).min(max));
    query
}

fn bad_request(message: String) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "code": 400, "error": message }))).into_response()
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(|s| s.to_string())
}

// ----------------------------------------------------------------------
// WebSocket transport
// ----------------------------------------------------------------------

#[derive(Deserialize)]
struct WsParams {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "reconnectionToken")]
    reconnection_token: Option<String>,
}

async fn ws_handler(
    State(state): State<AppState>,
    Path(room_id): Path<String>,
    Query(params): Query<WsParams>,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(handle) = state.mm.room_handle(&room_id) else {
        return error_response(ServerError::room_not_found(&room_id));
    };
    let mut ws = ws;
    if let Some(size) = state.ws_read_buffer_size {
        ws = ws.read_buffer_size(size);
    }
    if let Some(size) = state.ws_write_buffer_size {
        ws = ws.write_buffer_size(size).max_write_buffer_size(size.saturating_mul(4));
    }
    ws.on_upgrade(move |socket| handle_socket(socket, handle, params))
}

async fn handle_socket(socket: WebSocket, room: RoomHandle, params: WsParams) {
    use futures::{SinkExt, StreamExt};

    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Outbound>();
    let client = Client::new(params.session_id.clone(), outbound_tx);

    // Ask the room to accept this connection before pumping frames.
    let (respond, rx) = oneshot::channel();
    if room
        .tx
        .send(RoomEvent::Connect {
            client: client.clone(),
            reconnection_token: params.reconnection_token.clone(),
            respond,
        })
        .is_err()
    {
        return;
    }

    let (mut sink, mut stream) = socket.split();

    // writer task
    let writer = tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            match msg {
                Outbound::Bytes(bytes) => {
                    if sink.send(WsMessage::Binary(bytes)).await.is_err() {
                        break;
                    }
                }
                Outbound::Close(code, reason) => {
                    let _ = sink
                        .send(WsMessage::Close(Some(CloseFrame {
                            code,
                            reason: reason.into(),
                        })))
                        .await;
                    break;
                }
            }
        }
    });

    match tokio::time::timeout(std::time::Duration::from_secs(10), rx).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => {
            // room rejected the join; the error frame + close were queued already
            tracing::debug!("join rejected: {e}");
            let _ = writer.await;
            return;
        }
        _ => {
            client.error(codes::MATCHMAKE_UNHANDLED, "join timed out");
            client.leave(Some(close_codes::WITH_ERROR), Some("join timed out"));
            let _ = writer.await;
            return;
        }
    }

    // reader loop
    let mut close_code = close_codes::ABNORMAL_CLOSURE;
    while let Some(frame) = stream.next().await {
        match frame {
            Ok(WsMessage::Binary(bytes)) => match protocol::decode_client_message(&bytes) {
                Some(ClientMessage::Leave) => {
                    close_code = close_codes::CONSENTED;
                    let _ = room.tx.send(RoomEvent::Message {
                        client: client.clone(),
                        msg: ClientMessage::Leave,
                    });
                    break;
                }
                Some(msg) => {
                    let _ = room.tx.send(RoomEvent::Message {
                        client: client.clone(),
                        msg,
                    });
                }
                None => {
                    client.error(codes::INVALID_PAYLOAD, "could not decode message");
                }
            },
            Ok(WsMessage::Close(frame)) => {
                close_code = frame.map(|f| f.code).unwrap_or(close_codes::NORMAL_CLOSURE);
                break;
            }
            Ok(_) => {} // ping/pong/text: tungstenite answers pings itself
            Err(_) => break,
        }
    }

    let _ = room.tx.send(RoomEvent::Disconnected {
        client: client.clone(),
        code: close_code,
    });

    // drop our handle; when the room drops its senders the writer ends
    drop(client);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), &mut { writer }).await;
}

