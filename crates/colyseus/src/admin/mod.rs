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
//! | `GET /admin/api/rooms/{id}/events` | live room traffic stream (WebSocket) |
//! | `GET /admin/api/schema` | capability catalog: room types, RPCs, filter fields |

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::{json, Value};
use sysinfo::System;
use tokio::sync::oneshot;

use crate::actor::{RoomEvent, RoomInspection};
use crate::admin_rpc::{AdminContext, AdminRpcRegistration, RoomRpcHandler};
use crate::driver::{RoomListing, RoomQuery};
use crate::error::{close_codes, codes, ServerError};
use crate::matchmaker::MatchMaker;
use crate::protocol::MessageType;

/// The React admin panel, built to a single self-contained html file
/// (see `crates/colyseus/admin-ui/` — `npm run build` regenerates it).
const PANEL_HTML: &str = include_str!("../../admin-ui/dist/index.html");

/// How long a successful admin RPC response is replayed for a duplicate
/// `Idempotency-Key` (same policy as the matchmaker's seat reservations).
const RPC_IDEMPOTENCY_TTL: Duration = Duration::from_secs(30);
/// Upper bound of cached RPC responses (oldest are evicted).
const RPC_IDEMPOTENCY_CAP: usize = 1024;

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
    rpcs: HashMap<String, AdminRpcRegistration>,
    /// Custom room RPCs registered via `Server::room_rpc`.
    room_rpcs: HashMap<String, RoomRpcHandler>,
    /// Successful RPC responses cached by `Idempotency-Key` for replay
    /// (`{rpc_name}:{key}` → `(cached at, body)`; errors are never cached).
    rpc_idempotency: Arc<DashMap<String, (Instant, Value)>>,
}

/// Build the admin router (panel + JSON API + custom RPCs).
pub(crate) fn router(
    mm: MatchMaker,
    token: Option<String>,
    panel_html: bool,
    rpcs: Vec<AdminRpcRegistration>,
    room_rpcs: Vec<(String, RoomRpcHandler)>,
) -> Router {
    let state = AdminState {
        mm,
        token,
        started_at: Instant::now(),
        sys: std::sync::Arc::new(Mutex::new(System::new())),
        pid: std::process::id(),
        panel_html,
        rpcs: rpcs.into_iter().map(|rpc| (rpc.name.clone(), rpc)).collect(),
        room_rpcs: room_rpcs.into_iter().collect(),
        rpc_idempotency: Arc::new(DashMap::new()),
    };

    Router::new()
        .route("/admin", get(panel))
        .route("/admin/api/overview", get(overview))
        .route("/admin/api/schema", get(schema))
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

/// `GET /admin/api/schema` — machine-readable capability catalog: the
/// registered room types with their matchmaking knobs, the custom admin RPCs
/// (with their Rust param/response type names), and the core filterable
/// listing fields. Lets SDKs and external backends discover capabilities
/// instead of hardcoding them.
async fn schema(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let auth = authorize(&state, &headers);
    if auth.status() != StatusCode::OK {
        return auth;
    }

    let mut room_types: Vec<Value> = state.mm.handlers().iter().map(|h| h.schema()).collect();
    room_types.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

    let mut rpcs: Vec<Value> = state
        .rpcs
        .values()
        .map(|rpc| {
            json!({
                "name": rpc.name,
                "params": rpc.params_type,
                "response": rpc.response_type,
            })
        })
        .collect();
    rpcs.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

    Json(json!({
        "roomTypes": room_types,
        "adminRpcs": rpcs,
        "coreFilterFields": MatchMaker::CORE_FILTER_FIELDS,
    }))
    .into_response()
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
///
/// An `Idempotency-Key` header caches a successful response for ~30s and
/// replays it for duplicate keys (errors are never cached) — handlers must be
/// side-effect-safe under replay within that window.
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

    let Some(rpc) = state.rpcs.get(&name) else {
        return (StatusCode::NOT_FOUND, format!("unknown admin rpc \"{name}\""))
            .into_response();
    };

    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .filter(|k| !k.is_empty())
        .map(|k| format!("{name}:{k}"));

    // replay the cached response for a duplicate Idempotency-Key
    if let Some(key) = &idempotency_key {
        if let Some(cached) = rpc_idempotency_get(&state, key) {
            return Json(cached).into_response();
        }
    }

    let params = body.map(|Json(v)| v).unwrap_or(Value::Null);
    match (rpc.handler)(AdminContext::new(state.mm.clone()), params).await {
        Ok(value) => {
            if let Some(key) = idempotency_key {
                rpc_idempotency_put(&state, key, value.clone());
            }
            Json(value).into_response()
        }
        Err(e) => rpc_error_response(e),
    }
}

/// Replay a cached RPC response for a duplicate `Idempotency-Key`.
fn rpc_idempotency_get(state: &AdminState, key: &str) -> Option<Value> {
    let entry = state.rpc_idempotency.get(key)?;
    if entry.0.elapsed() > RPC_IDEMPOTENCY_TTL {
        let key = entry.key().clone();
        drop(entry);
        state.rpc_idempotency.remove(&key);
        return None;
    }
    Some(entry.1.clone())
}

/// Cache a successful RPC response for `Idempotency-Key` replays. Expired
/// entries are swept on insert; the map is bounded (oldest evicted).
fn rpc_idempotency_put(state: &AdminState, key: String, body: Value) {
    state
        .rpc_idempotency
        .retain(|_, (at, _)| at.elapsed() <= RPC_IDEMPOTENCY_TTL);
    while state.rpc_idempotency.len() >= RPC_IDEMPOTENCY_CAP {
        let oldest = state
            .rpc_idempotency
            .iter()
            .max_by_key(|e| e.0.elapsed())
            .map(|e| e.key().clone());
        match oldest {
            Some(k) => {
                state.rpc_idempotency.remove(&k);
            }
            None => break,
        }
    }
    state.rpc_idempotency.insert(key, (Instant::now(), body));
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
    /// Deprecated fallback: browsers can't set headers on WebSocket
    /// connections, so the panel passes the admin token as a query
    /// parameter. Prefer the `bearer.<token>` subprotocol (it doesn't leak
    /// into logs).
    token: Option<String>,
}

/// The admin token offered as a WebSocket subprotocol
/// (`Sec-WebSocket-Protocol: bearer.<token>`). Returns the full offered
/// protocol string (echoed back on success, per RFC 6455) and the token.
fn subprotocol_token(headers: &HeaderMap) -> Option<(String, String)> {
    headers
        .get_all(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|p| p.len() > "bearer.".len())
        .find_map(|p| p.strip_prefix("bearer.").map(|t| (p.to_string(), t.to_string())))
}

async fn room_events(
    State(state): State<AdminState>,
    Path(room_id): Path<String>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<EventsParams>,
    ws: WebSocketUpgrade,
) -> Response {
    let subprotocol = subprotocol_token(&headers);
    if let Some(expected) = &state.token {
        let ok = subprotocol
            .as_ref()
            .is_some_and(|(_, token)| token == expected)
            // deprecated fallback: `?token=` query param
            || params.token.as_deref() == Some(expected.as_str());
        if !ok {
            return (StatusCode::UNAUTHORIZED, "invalid admin token").into_response();
        }
    }
    let Some(handle) = state.mm.room_handle(&room_id) else {
        return (StatusCode::NOT_FOUND, "room not found").into_response();
    };
    let rx = handle.tap.subscribe();
    let mut ws = ws;
    if let Some((protocol, _)) = subprotocol {
        // the client offered a subprotocol — the handshake must echo it
        ws = ws.protocols([protocol]);
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subprotocol_token_parses_bearer_protocols() {
        let headers = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(axum::http::header::SEC_WEBSOCKET_PROTOCOL, value.parse().unwrap());
            headers
        };

        // single bearer protocol
        assert_eq!(
            subprotocol_token(&headers("bearer.s3cr3t")),
            Some(("bearer.s3cr3t".to_string(), "s3cr3t".to_string()))
        );

        // comma-joined with other protocols; whitespace tolerated
        assert_eq!(
            subprotocol_token(&headers("other, bearer.tok")),
            Some(("bearer.tok".to_string(), "tok".to_string()))
        );

        // no bearer protocol, an empty token, or no header at all
        assert_eq!(subprotocol_token(&headers("other")), None);
        assert_eq!(subprotocol_token(&headers("bearer.")), None);
        assert_eq!(subprotocol_token(&HeaderMap::new()), None);
    }
}
