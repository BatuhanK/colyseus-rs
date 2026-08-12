//! The server: HTTP matchmaking API + WebSocket transport (axum).
//!
//! HTTP API:
//! - `POST /matchmake/{method}/{roomName}` — `method` is one of
//!   `joinOrCreate`, `create`, `join`, `joinById`, `reconnect`
//!   (`joinById`/`reconnect` take a room id in place of the room name).
//!   Body: JSON client options. Response: a seat reservation.
//! - `GET /rooms` / `GET /rooms/{roomName}` — room listing queries.
//!
//! WebSocket:
//! - `GET /ws/{roomId}?sessionId=...&reconnectionToken=...` — binary msgpack
//!   frames as described in [`crate::protocol`].

use std::collections::HashMap;
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
use crate::error::{close_codes, codes, Result, ServerError};
use crate::matchmaker::{AuthContext, MatchMaker, RegisteredHandler};
use crate::presence::Presence;
use crate::protocol::{self, ClientMessage};
use crate::room::Room;

/// The game server. Register room types, then [`Server::listen`].
pub struct Server {
    handlers: HashMap<String, RegisteredHandler>,
    presence: Option<Arc<dyn Presence>>,
    public_address: Option<String>,
    extra_router: Option<Router>,
    cors: bool,
    greet: bool,
    ws_read_buffer_size: Option<usize>,
    ws_write_buffer_size: Option<usize>,
    /// `Some` = admin panel enabled (inner Option = bearer token).
    admin: Option<Option<String>>,
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
            public_address: None,
            extra_router: None,
            cors: true,
            greet: true,
            ws_read_buffer_size: None,
            ws_write_buffer_size: None,
            admin: None,
        }
    }

    /// Override the default in-process presence.
    pub fn presence(mut self, presence: Arc<dyn Presence>) -> Self {
        self.presence = Some(presence);
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
    pub fn admin_panel(mut self, token: Option<String>) -> Self {
        self.admin = Some(token);
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
        let mm = MatchMaker::new(self.presence, self.public_address);
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
        if let Some(token) = self.admin {
            if token.is_none() {
                tracing::warn!("admin panel enabled WITHOUT a token — anyone can access /admin");
            }
            app = app.merge(crate::admin::router(mm.clone(), token));
        }

        (app, mm)
    }

    /// Bind, serve, and handle graceful shutdown (Ctrl-C / SIGTERM).
    pub async fn listen(self, addr: &str) -> Result<()> {
        self.greet_banner(addr);
        let (app, mm) = self.build();

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

    let options = body.map(|Json(v)| v).unwrap_or(Value::Null);
    let auth = AuthContext {
        token: bearer_token(&headers),
        headers: headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.to_string(), v.to_string())))
            .collect(),
        ip: header(&headers, "x-forwarded-for")
            .or_else(|| header(&headers, "x-real-ip")),
    };

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

    let result = match method.as_str() {
        "joinOrCreate" => mm.join_or_create(&room_name, options, auth).await,
        "create" => mm.create(&room_name, options, auth).await,
        "join" => mm.join(&room_name, options, auth).await,
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
        Ok(reservation) => Json(reservation).into_response(),
        Err(e) => error_response(e),
    }
}

fn error_response(e: ServerError) -> Response {
    let status = StatusCode::from_u16(e.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(json!({ "code": e.code, "error": e.message }))).into_response()
}

async fn list_rooms(State(state): State<AppState>) -> Response {
    Json(state.mm.query(None, Default::default())).into_response()
}

async fn list_rooms_by_name(
    State(state): State<AppState>,
    Path(room_name): Path<String>,
) -> Response {
    Json(state.mm.query(Some(&room_name), Default::default())).into_response()
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

