//! Built-in admin panel (the `@colyseus/monitor` counterpart).
//!
//! Enable it on the server:
//!
//! ```ignore
//! Server::new().admin_panel(Some("secret-token".into())) // token optional
//! ```
//!
//! Then open `http://<host>:<port>/admin`. The panel is a single self-contained
//! page served by the game server itself; the JSON API lives under
//! `/admin/api/*` and is guarded by the bearer token when one is set.
//!
//! | endpoint | what it does |
//! | --- | --- |
//! | `GET /admin/api/overview` | process stats (room/connection counts) |
//! | `GET /admin/api/rooms` `{filters, sort, limit, offset}` | query rooms (operators, pagination) |
//! | `GET /admin/api/rooms/stats` | per-room-type open/waiting/full counts |
//! | `GET /admin/api/rooms/{id}` | inspect a room (state, clients, seats) |
//! | `POST /admin/api/rooms/{id}/lock` / `unlock` | toggle matchmaking lock |
//! | `POST /admin/api/rooms/{id}/kick` `{sessionId}` | force-disconnect a client |
//! | `POST /admin/api/rooms/{id}/message` `{sessionId?, type, data}` | message one client or broadcast |
//! | `POST /admin/api/rooms/{id}/dispose` | dispose the room (close 4000) |

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use sysinfo::System;
use tokio::sync::oneshot;

use crate::actor::{RoomEvent, RoomInspection};
use crate::admin_rpc::{AdminContext, RoomRpcHandler, RpcFn};
use crate::driver::{RoomListing, RoomQuery};
use crate::error::{close_codes, codes, ServerError};
use crate::matchmaker::MatchMaker;
use crate::protocol::MessageType;

/// The React admin panel, built to a single self-contained html file
/// (see `crates/colyseus/admin-ui/` — `npm run build` regenerates it).
const PANEL_HTML: &str = include_str!("../../admin-ui/dist/index.html");

#[derive(Clone)]
struct AdminState {
    mm: MatchMaker,
    token: Option<String>,
    started_at: Instant,
    sys: std::sync::Arc<Mutex<System>>,
    pid: u32,
    /// Serve the `/admin` HTML panel (false = API-only, e.g. SDK deployments).
    panel_html: bool,
    /// Custom admin RPCs registered via `Server::admin_rpc`.
    rpcs: HashMap<String, RpcFn>,
    /// Custom room RPCs registered via `Server::room_rpc`.
    room_rpcs: HashMap<String, RoomRpcHandler>,
}

/// Build the admin router (panel + JSON API + custom RPCs).
pub(crate) fn router(
    mm: MatchMaker,
    token: Option<String>,
    panel_html: bool,
    rpcs: Vec<(String, RpcFn)>,
    room_rpcs: Vec<(String, RoomRpcHandler)>,
) -> Router {
    let state = AdminState {
        mm,
        token,
        started_at: Instant::now(),
        sys: std::sync::Arc::new(Mutex::new(System::new())),
        pid: std::process::id(),
        panel_html,
        rpcs: rpcs.into_iter().collect(),
        room_rpcs: room_rpcs.into_iter().collect(),
    };

    Router::new()
        .route("/admin", get(panel))
        .route("/admin/api/overview", get(overview))
        .route("/admin/api/rooms", get(list_rooms_query))
        .route("/admin/api/rooms/stats", get(room_stats))
        .route("/admin/api/rooms/{room_id}", get(inspect_room))
        .route("/admin/api/rooms/{room_id}/lock", post(lock_room))
        .route("/admin/api/rooms/{room_id}/unlock", post(unlock_room))
        .route("/admin/api/rooms/{room_id}/kick", post(kick_client))
        .route("/admin/api/rooms/{room_id}/message", post(send_message))
        .route("/admin/api/rooms/{room_id}/dispose", post(dispose_room))
        .route("/admin/api/rooms/{room_id}/state", post(edit_state))
        .route("/admin/api/rooms/{room_id}/events", get(room_events))
        .route("/admin/api/rooms/{room_id}/rpc/{name}", post(call_room_rpc))
        .route("/admin/api/rpc/{name}", post(call_rpc))
        .with_state(state)
}

fn authorize(state: &AdminState, headers: &HeaderMap) -> Response {
    if let Some(expected) = &state.token {
        let ok = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|t| t == expected);
        if !ok {
            return (StatusCode::UNAUTHORIZED, "invalid admin token").into_response();
        }
    }
    StatusCode::OK.into_response()
}

/// The panel page itself is public — the JS overlay asks for the token and
/// the API enforces it. When the panel is disabled (`admin_token` only), the
/// route is a 404.
async fn panel(State(state): State<AdminState>) -> Response {
    if !state.panel_html {
        return StatusCode::NOT_FOUND.into_response();
    }
    Html(PANEL_HTML).into_response()
}

async fn overview(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }

    let listings = state.mm.query(None, Default::default());
    let connections: u32 = listings.iter().map(|l| l.clients).sum();

    let rss_bytes = {
        let mut sys = state.sys.lock().unwrap();
        let pid = sysinfo::Pid::from_u32(state.pid);
        sys.refresh_process(pid);
        sys.process(pid).map(|p| p.memory()).unwrap_or(0)
    };

    Json(json!({
        "processId": state.mm.process_id(),
        "pid": state.pid,
        "uptimeMillis": state.started_at.elapsed().as_millis() as u64,
        "rssBytes": rss_bytes,
        "rooms": listings.len(),
        "connections": connections,
    }))
    .into_response()
}

/// Filtered, paged room listing — the SDK endpoint behind `AdminClient.listRooms`.
/// Filters/sorts are validated against the room type's `filter_by` whitelist.
async fn list_rooms_query(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }
    match RoomQuery::from_params(&params) {
        Ok(query) => {
            let query = cap_limit(query, 1000);
            match state.mm.query_rooms(None, query) {
                Ok(result) => Json(result).into_response(),
                Err(e) => (StatusCode::from_u16(e.code).unwrap_or(StatusCode::BAD_REQUEST),
                    Json(json!({ "code": e.code, "error": e.message })))
                    .into_response(),
            }
        }
        Err(message) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "code": 400, "error": message })),
        )
            .into_response(),
    }
}

/// Per-room-type status counts (open / waiting / full / locked / private).
async fn room_stats(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }
    Json(state.mm.room_stats(params.get("name").map(String::as_str))).into_response()
}

fn cap_limit(mut query: RoomQuery, max: usize) -> RoomQuery {
    query.limit = Some(query.limit.unwrap_or(max).min(max));
    query
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct InspectResponse {
    #[serde(flatten)]
    inspection: RoomInspection,
    listing: RoomListing,
}

async fn inspect_room(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }

    let Some(listing) = state.mm.driver().get(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    let Some(handle) = state.mm.room_handle(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };

    let (tx, rx) = oneshot::channel();
    if handle.tx.send(RoomEvent::Inspect { respond: tx }).is_err() {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    }
    match tokio::time::timeout(std::time::Duration::from_secs(3), rx).await {
        Ok(Ok(inspection)) => Json(InspectResponse { inspection, listing }).into_response(),
        _ => (StatusCode::GATEWAY_TIMEOUT, "room did not respond").into_response(),
    }
}

async fn set_locked(state: &AdminState, room_id: &str, locked: bool) -> Response {
    let Some(handle) = state.mm.room_handle(room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    let _ = handle.tx.send(RoomEvent::SetLocked(locked));
    StatusCode::NO_CONTENT.into_response()
}

async fn lock_room(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }
    set_locked(&state, &room_id, true).await
}

async fn unlock_room(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }
    set_locked(&state, &room_id, false).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KickBody {
    session_id: String,
}

async fn kick_client(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    Json(body): Json<KickBody>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }
    let Some(handle) = state.mm.room_handle(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    let _ = handle.tx.send(RoomEvent::Kick {
        session_id: body.session_id,
    });
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageBody {
    session_id: Option<String>,
    #[serde(rename = "type")]
    msg_type: Value,
    data: Value,
}

async fn send_message(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    Json(body): Json<MessageBody>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }
    let Some(msg_type) = (match &body.msg_type {
        Value::String(s) => Some(MessageType::Str(s.clone())),
        Value::Number(n) => n.as_i64().map(MessageType::Num),
        _ => None,
    }) else {
        return (StatusCode::BAD_REQUEST, "type must be a string or number").into_response();
    };
    let Some(handle) = state.mm.room_handle(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    let _ = handle.tx.send(RoomEvent::AdminMessage {
        session_id: body.session_id,
        msg_type,
        payload: body.data,
    });
    StatusCode::NO_CONTENT.into_response()
}

async fn dispose_room(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }
    let Some(handle) = state.mm.room_handle(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    let _ = handle.tx.send(RoomEvent::Dispose {
        code: close_codes::CONSENTED,
    });
    StatusCode::NO_CONTENT.into_response()
}

// ----------------------------------------------------------------------
// State editing
// ----------------------------------------------------------------------

#[derive(Deserialize)]
struct StateEditBody {
    /// JSON-pointer-ish path, e.g. "/players/abc123/score"
    path: String,
    /// "set" | "remove"
    op: String,
    value: Option<Value>,
}

async fn edit_state(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(room_id): Path<String>,
    Json(body): Json<StateEditBody>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }

    let edit = match body.op.as_str() {
        "set" => crate::state::StateEdit::Set(body.value.unwrap_or(Value::Null)),
        "remove" => crate::state::StateEdit::Remove,
        other => {
            return (StatusCode::BAD_REQUEST, format!("unknown op \"{other}\"")).into_response()
        }
    };

    let path: Vec<String> = body
        .path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect();

    let Some(handle) = state.mm.room_handle(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };

    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(RoomEvent::EditState {
            path,
            edit,
            respond: tx,
        })
        .is_err()
    {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    }

    match tokio::time::timeout(std::time::Duration::from_secs(3), rx).await {
        Ok(Ok(Ok(()))) => StatusCode::NO_CONTENT.into_response(),
        Ok(Ok(Err(msg))) => (StatusCode::BAD_REQUEST, msg).into_response(),
        _ => (StatusCode::GATEWAY_TIMEOUT, "room did not respond").into_response(),
    }
}

// ----------------------------------------------------------------------
// Custom admin RPCs
// ----------------------------------------------------------------------

/// `POST /admin/api/rpc/{name}` — dispatch a custom admin RPC registered via
/// `Server::admin_rpc`. The JSON body becomes the RPC's `Params`; the RPC's
/// serialized `Response` is returned directly (errors use `{ code, error }`).
async fn call_rpc(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Option<Json<Value>>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }

    let params = body.map(|Json(v)| v).unwrap_or(Value::Null);
    let Some(rpc) = state.rpcs.get(&name) else {
        return (StatusCode::NOT_FOUND, format!("unknown admin rpc \"{name}\""))
            .into_response();
    };

    match rpc(AdminContext::new(state.mm.clone()), params).await {
        Ok(value) => Json(value).into_response(),
        Err(e) => rpc_error_response(e),
    }
}

fn rpc_error_response(e: ServerError) -> Response {
    let status = if e.code == codes::INVALID_PAYLOAD {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::from_u16(e.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    };
    (status, Json(json!({ "code": e.code, "error": e.message }))).into_response()
}

/// `POST /admin/api/rooms/{roomId}/rpc/{name}` — dispatch a room-based admin
/// RPC registered via `Server::room_rpc`. The RPC runs on the room actor with
/// typed `&mut MyRoom` access and returns its serialized response.
async fn call_room_rpc(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path((room_id, name)): Path<(String, String)>,
    body: Option<Json<Value>>,
) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }

    let Some(handle) = state.mm.room_handle(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    let Some(rpc) = state.room_rpcs.get(&name) else {
        return (StatusCode::NOT_FOUND, format!("unknown room rpc \"{name}\""))
            .into_response();
    };

    let params = body.map(|Json(v)| v).unwrap_or(Value::Null);
    match rpc(handle, params).await {
        Ok(value) => Json(value).into_response(),
        Err(e) => rpc_error_response(e),
    }
}

#[derive(Deserialize)]
struct EventsParams {
    /// Browsers can't set headers on WebSocket connections, so the panel
    /// passes the admin token as a query parameter here.
    token: Option<String>,
}

async fn room_events(
    State(state): State<AdminState>,
    Path(room_id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<EventsParams>,
    ws: WebSocketUpgrade,
) -> Response {
    if let Some(expected) = &state.token {
        if params.token.as_deref() != Some(expected.as_str()) {
            return (StatusCode::UNAUTHORIZED, "invalid admin token").into_response();
        }
    }
    let Some(handle) = state.mm.room_handle(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    let rx = handle.tap.subscribe();
    ws.on_upgrade(move |socket| stream_events(socket, rx))
}

async fn stream_events(
    socket: axum::extract::ws::WebSocket,
    mut rx: tokio::sync::broadcast::Receiver<crate::room::RoomEventLog>,
) {
    use futures::{SinkExt, StreamExt};
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(log) => {
                        let text = serde_json::to_string(&log).unwrap_or_default();
                        if sink.send(axum::extract::ws::Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            frame = stream.next() => {
                match frame {
                    Some(Ok(axum::extract::ws::Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}
