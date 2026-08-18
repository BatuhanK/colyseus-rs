//! End-to-end tests: real HTTP matchmaking + real WebSocket connections.

use std::collections::HashMap;
use std::time::Duration;

use colyseus::serde_json::{json, Value};
use colyseus::{async_trait, AdminContext, AdminRpc, Client, Result, Room, RoomContext, RoomRpc, RoomSnapshot, Server};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------
// Test rooms
// ---------------------------------------------------------------------

struct ChatRoom;

#[derive(Deserialize)]
struct ChatMsg {
    text: String,
}

#[async_trait]
impl Room for ChatRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.set_max_clients(Some(2));
        ctx.on_message(
            "chat",
            |_room: &mut ChatRoom, ctx, client, msg: ChatMsg| {
                Box::pin(async move {
                    ctx.broadcast(
                        "chat",
                        &json!({ "from": client.session_id(), "text": msg.text }),
                    );
                    Ok(())
                })
            },
        );
        Ok(())
    }
}

/// Internal room type: only creatable server-side.
struct InternalRoom;

#[async_trait]
impl Room for InternalRoom {}

/// Sends messages to the client during on_join (before join completes).
struct GreeterRoom;

#[async_trait]
impl Room for GreeterRoom {
    async fn on_join(
        &mut self,
        _ctx: &mut RoomContext,
        client: Client,
        _options: Value,
        _auth: Option<Value>,
    ) -> Result<()> {
        client.send("history", &json!([{"text": "old-1"}, {"text": "old-2"}]));
        client.send("welcome", &json!({"hi": true}));
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct Counter {
    count: i64,
}

// room with hidden per-player information (view filter showcase)
#[derive(Serialize, Deserialize)]
struct Secrets {
    round: u64,
    secrets: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct SecretView {
    round: u64,
    my_secret: Option<String>,
    players: usize,
}

struct SecretRoom;

#[async_trait]
impl Room for SecretRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.set_state(Secrets {
            round: 0,
            secrets: HashMap::new(),
        });
        ctx.set_patch_rate(Some(Duration::from_millis(20)));
        ctx.set_view_filter(|state: &Secrets, client: &Client| SecretView {
            round: state.round,
            my_secret: state.secrets.get(client.session_id()).cloned(),
            players: state.secrets.len(),
        });

        ctx.on_message("bump", |_room: &mut SecretRoom, ctx, _client, _msg: Value| {
            Box::pin(async move {
                ctx.state_mut::<Secrets>().unwrap().round += 1;
                Ok(())
            })
        });
        Ok(())
    }

    async fn on_join(
        &mut self,
        ctx: &mut RoomContext,
        client: Client,
        _options: Value,
        _auth: Option<Value>,
    ) -> Result<()> {
        ctx.state_mut::<Secrets>().unwrap().secrets.insert(
            client.session_id().to_string(),
            format!("secret-of-{}", client.session_id()),
        );
        Ok(())
    }
}

/// Room that requires a bearer token ("letmein") in on_auth.
struct AuthRoom;

#[async_trait]
impl Room for AuthRoom {
    async fn on_auth(
        &mut self,
        _ctx: &mut RoomContext,
        _options: &Value,
        auth: &colyseus::AuthContext,
    ) -> Result<Option<Value>> {
        match auth.token.as_deref() {
            Some("letmein") => Ok(Some(json!({ "name": "authed-user" }))),
            _ => Err(colyseus::ServerError::new(
                colyseus::codes::AUTH_FAILED,
                "missing bearer token",
            )),
        }
    }

    async fn on_join(
        &mut self,
        _ctx: &mut RoomContext,
        client: Client,
        _options: Value,
        auth: Option<Value>,
    ) -> Result<()> {
        client.send("welcome", &json!({ "auth": auth }));
        Ok(())
    }
}

struct StateRoom;

#[derive(Deserialize)]
struct Increment {
    by: i64,
}

#[async_trait]
impl Room for StateRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.set_state(Counter { count: 0 });
        ctx.set_patch_rate(Some(Duration::from_millis(20)));
        ctx.on_message(
            "increment",
            |_room: &mut StateRoom, ctx, _client, msg: Increment| {
                Box::pin(async move {
                    ctx.state_mut::<Counter>().unwrap().count += msg.by;
                    Ok(())
                })
            },
        );
        Ok(())
    }

    async fn on_drop(&mut self, ctx: &mut RoomContext, client: Client, _code: u16) {
        ctx.allow_reconnection(&client, Some(Duration::from_secs(30)));
    }

    async fn on_reconnect(&mut self, ctx: &mut RoomContext, client: Client) {
        client.send("reconnected", &json!({ "ok": true }));
        let _ = ctx;
    }
}

/// Room with both public state and server-only internal state, both persisted.
#[derive(Serialize, Deserialize)]
struct PersistentInternal {
    secret_value: i64,
}

struct PersistentRoom;

#[async_trait]
impl Room for PersistentRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.set_state(Counter { count: 0 });
        ctx.set_internal(PersistentInternal { secret_value: 1 });
        ctx.set_patch_rate(Some(Duration::from_millis(20)));
        ctx.on_message(
            "increment",
            |_room: &mut PersistentRoom, ctx, _client, msg: Increment| {
                Box::pin(async move {
                    ctx.state_mut::<Counter>().unwrap().count += msg.by;
                    ctx.internal_mut::<PersistentInternal>().unwrap().secret_value += msg.by;
                    Ok(())
                })
            },
        );
        ctx.on_message("report", |_room: &mut PersistentRoom, ctx, _client, _msg: Value| {
            Box::pin(async move {
                let secret = ctx.internal::<PersistentInternal>().unwrap().secret_value;
                ctx.broadcast("report", &json!({ "secret": secret }));
                Ok(())
            })
        });
        Ok(())
    }

    async fn on_restore(&mut self, ctx: &mut RoomContext, snapshot: &RoomSnapshot) -> Result<()> {
        ctx.restore_state::<Counter>(snapshot)?;
        ctx.restore_internal::<PersistentInternal>(snapshot)?;
        Ok(())
    }

    async fn on_reconnect(&mut self, ctx: &mut RoomContext, client: Client) {
        client.send("reconnected", &json!({ "ok": true }));
        let _ = ctx;
    }
}

// ---------------------------------------------------------------------
// Test client helpers
// ---------------------------------------------------------------------

struct TestServer {
    base: String,
    _handle: tokio::task::JoinHandle<()>,
}

async fn start_server() -> TestServer {
    let mut server = Server::new().admin_panel(None);
    server.define("chat", || ChatRoom).filter_by(&["mode"]);
    server.define("state", || StateRoom);
    server.define("secret", || SecretRoom);
    server.define("auth", || AuthRoom);
    server.define("internal", || InternalRoom).internal();
    server.define("greeter", || GreeterRoom);

    let (app, mm) = server.build();
    // server-side creation of an internal room (no seat, no auth)
    mm.create_room("internal", json!({})).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    TestServer {
        base: format!("http://{addr}"),
        _handle: handle,
    }
}

async fn matchmake(base: &str, method: &str, room: &str, options: Value) -> Value {
    matchmake_authed(base, method, room, options, None).await
}

async fn matchmake_authed(
    base: &str,
    method: &str,
    room: &str,
    options: Value,
    bearer_token: Option<&str>,
) -> Value {
    let mut req = reqwest::Client::new()
        .post(format!("{base}/matchmake/{method}/{room}"))
        .json(&options);
    if let Some(token) = bearer_token {
        req = req.bearer_auth(token);
    }
    req.send().await.unwrap().json::<Value>().await.unwrap()
}

struct WsClient {
    write: futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        Message,
    >,
    read: futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    >,
}

impl WsClient {
    async fn connect(base: &str, reservation: &Value) -> Self {
        Self::connect_with_token(base, reservation, None).await
    }

    async fn connect_with_token(base: &str, reservation: &Value, token: Option<&str>) -> Self {
        let room_id = reservation["room"]["roomId"].as_str().unwrap();
        let session_id = reservation["sessionId"].as_str().unwrap();
        let mut url = format!(
            "{}/ws/{room_id}?sessionId={session_id}",
            base.replace("http://", "ws://")
        );
        if let Some(token) = token {
            url.push_str(&format!("&reconnectionToken={token}"));
        }
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (write, read) = ws.split();
        WsClient { write, read }
    }

    async fn send(&mut self, msg_type: &str, payload: Value) {
        let bytes = rmp_serde::to_vec(&json!([13, msg_type, payload])).unwrap();
        self.write.send(Message::Binary(bytes.into())).await.unwrap();
    }

    async fn send_bytes(&mut self, bytes: Vec<u8>) {
        self.write.send(Message::Binary(bytes.into())).await.unwrap();
    }

    /// Receive the next binary frame, decoded as msgpack.
    async fn recv(&mut self) -> Value {
        let frame = tokio::time::timeout(Duration::from_secs(5), self.read.next())
            .await
            .expect("timed out waiting for frame")
            .expect("stream ended")
            .expect("ws error");
        let Message::Binary(bytes) = frame else {
            panic!("expected binary frame, got {frame:?}");
        };
        rmp_serde::from_slice::<Value>(&bytes).expect("invalid msgpack")
    }

    async fn close(self) {
        drop(self);
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[tokio::test]
async fn join_chat_and_broadcast() {
    let server = start_server().await;

    // two clients join (or create) the same room type; they share maxClients=2
    let r1 = matchmake(&server.base, "joinOrCreate", "chat", json!({})).await;
    let r2 = matchmake(&server.base, "joinOrCreate", "chat", json!({})).await;
    assert_eq!(r1["room"]["roomId"], r2["room"]["roomId"]);

    let mut c1 = WsClient::connect(&server.base, &r1).await;
    let mut c2 = WsClient::connect(&server.base, &r2).await;

    // join handshakes: [10, token, serializerId]
    let h1 = c1.recv().await;
    assert_eq!(h1[0], 10);
    assert_eq!(h1[2], "none"); // chat room has no state
    let h2 = c2.recv().await;
    assert_eq!(h2[0], 10);

    // c1 sends a chat message; both receive the broadcast
    c1.send("chat", json!({ "text": "hello" })).await;
    for c in [&mut c1, &mut c2] {
        let msg = c.recv().await;
        assert_eq!(msg[0], 13);
        assert_eq!(msg[1], "chat");
        assert_eq!(msg[2]["text"], "hello");
    }

    // maxClients=2 → third joinOrCreate creates a *new* room
    let r3 = matchmake(&server.base, "joinOrCreate", "chat", json!({})).await;
    assert_ne!(r1["room"]["roomId"], r3["room"]["roomId"]);
}

#[tokio::test]
async fn state_sync_full_and_patch() {
    let server = start_server().await;

    let r = matchmake(&server.base, "joinOrCreate", "state", json!({})).await;
    let mut c = WsClient::connect(&server.base, &r).await;

    let handshake = c.recv().await;
    assert_eq!(handshake[0], 10);
    assert_eq!(handshake[2], "json-patch");

    // full state: [14, {count: 0}]
    let full = c.recv().await;
    assert_eq!(full[0], 14);
    assert_eq!(full[1]["count"], 0);

    // mutate and expect a JSON-Patch: [15, [{op: replace, path: /count, value: 5}]]
    c.send("increment", json!({ "by": 5 })).await;
    let patch = c.recv().await;
    assert_eq!(patch[0], 15);
    let ops = patch[1].as_array().unwrap();
    assert!(
        ops.iter()
            .any(|op| op["path"] == "/count" && op["value"] == 5),
        "unexpected patch: {ops:?}"
    );
}

#[tokio::test]
async fn reconnect_after_drop() {
    let server = start_server().await;

    let r = matchmake(&server.base, "joinOrCreate", "state", json!({})).await;
    let mut c = WsClient::connect(&server.base, &r).await;
    let handshake = c.recv().await;
    let token = handshake[1].as_str().unwrap().to_string();
    let _full = c.recv().await;

    // bump the counter, then drop the connection (no close frame = abnormal)
    c.send("increment", json!({ "by": 3 })).await;
    let _patch = c.recv().await;
    c.close().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // reconnect via matchmaking with the reconnection token
    let room_id = r["room"]["roomId"].as_str().unwrap();
    let r2 = matchmake(
        &server.base,
        "reconnect",
        room_id,
        json!({ "reconnectionToken": token }),
    )
    .await;
    assert_eq!(r2["sessionId"], r["sessionId"]);

    let mut c2 = WsClient::connect_with_token(&server.base, &r2, Some(&token)).await;
    let h2 = c2.recv().await;
    assert_eq!(h2[0], 10);
    // fresh full state includes the earlier increment
    let full2 = c2.recv().await;
    assert_eq!(full2[1]["count"], 3);
    // on_reconnect fired
    let note = c2.recv().await;
    assert_eq!(note[1], "reconnected");
}

#[tokio::test]
async fn filter_by_routes_to_matching_rooms() {
    let server = start_server().await;

    let r1 = matchmake(&server.base, "joinOrCreate", "chat", json!({ "mode": "ranked" })).await;
    let r2 = matchmake(&server.base, "joinOrCreate", "chat", json!({ "mode": "casual" })).await;
    let r3 = matchmake(&server.base, "joinOrCreate", "chat", json!({ "mode": "ranked" })).await;

    assert_ne!(r1["room"]["roomId"], r2["room"]["roomId"]);
    assert_eq!(r1["room"]["roomId"], r3["room"]["roomId"]);

    // listings visible via the HTTP API, filter field flattened in
    let rooms: Vec<Value> = reqwest::Client::new()
        .get(format!("{}/rooms/chat", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rooms.len(), 2);
    assert!(rooms.iter().any(|r| r["mode"] == "ranked"));
}

#[tokio::test]
async fn binary_messages_and_ping() {
    let server = start_server().await;
    let r = matchmake(&server.base, "joinOrCreate", "chat", json!({})).await;
    let mut c = WsClient::connect(&server.base, &r).await;
    let _ = c.recv().await; // handshake

    // app-level ping → pong (single byte 18)
    c.send_bytes(vec![18]).await;
    let frame = tokio::time::timeout(Duration::from_secs(2), c.read.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Binary(bytes) = frame else {
        panic!("expected binary");
    };
    assert_eq!(bytes.as_ref(), &[18]);
}

#[tokio::test]
async fn join_by_id_passes_auth_context() {
    let server = start_server().await;

    // create a room (authed)
    let created =
        matchmake_authed(&server.base, "create", "auth", json!({}), Some("letmein")).await;
    let room_id = created["room"]["roomId"].as_str().unwrap();

    // joinById without a token → 525
    let resp = reqwest::Client::new()
        .post(format!("{}/matchmake/joinById/{room_id}", server.base))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 525);

    // joinById with the token → seat reserved, auth data reaches on_join
    let res =
        matchmake_authed(&server.base, "joinById", room_id, json!({}), Some("letmein")).await;
    assert!(res["sessionId"].is_string(), "unexpected: {res}");

    let mut c = WsClient::connect(&server.base, &res).await;
    let _handshake = c.recv().await;
    let welcome = c.recv().await;
    assert_eq!(welcome[1], "welcome");
    assert_eq!(welcome[2]["auth"]["name"], "authed-user");
}

#[tokio::test]
async fn messages_sent_during_on_join_are_delivered() {
    let server = start_server().await;
    let r = matchmake(&server.base, "joinOrCreate", "greeter", json!({})).await;
    let mut c = WsClient::connect(&server.base, &r).await;

    let handshake = c.recv().await;
    assert_eq!(handshake[0], 10);

    // both messages queued during on_join must arrive right after the handshake
    let m1 = c.recv().await;
    assert_eq!(m1[0], 13);
    assert_eq!(m1[1], "history");
    assert_eq!(m1[2][0]["text"], "old-1");
    let m2 = c.recv().await;
    assert_eq!(m2[1], "welcome");
}

#[tokio::test]
async fn admin_state_edit_broadcasts_patch() {
    let server = start_server().await;

    let r = matchmake(&server.base, "joinOrCreate", "state", json!({})).await;
    let room_id = r["room"]["roomId"].as_str().unwrap();
    let mut c = WsClient::connect(&server.base, &r).await;
    let _ = c.recv().await; // handshake
    let _ = c.recv().await; // full state

    // valid edit: number into number field
    let resp = reqwest::Client::new()
        .post(format!("{}/admin/api/rooms/{room_id}/state", server.base))
        .json(&json!({ "path": "/count", "op": "set", "value": 42 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 204);

    // the edit reaches the connected client as a live patch
    let patch = c.recv().await;
    assert_eq!(patch[0], 15);
    assert!(patch[1].as_array().unwrap().iter().any(|op| op["path"] == "/count" && op["value"] == 42));

    // type-invalid edit is rejected (string into i64 field)
    let resp = reqwest::Client::new()
        .post(format!("{}/admin/api/rooms/{room_id}/state", server.base))
        .json(&json!({ "path": "/count", "op": "set", "value": "not a number" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
}

#[tokio::test]
async fn internal_rooms_reject_client_creation() {
    let server = start_server().await;

    // clients cannot create internal rooms…
    for method in ["create", "joinOrCreate"] {
        let resp = reqwest::Client::new()
            .post(format!("{}/matchmake/{method}/internal", server.base))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 520, "{method} should be rejected");
    }

    // …but the server-created instance is joinable
    let res = matchmake(&server.base, "join", "internal", json!({})).await;
    assert!(res["sessionId"].is_string(), "unexpected: {res}");
}

#[tokio::test]
async fn invalid_room_name_rejected() {
    let server = start_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/matchmake/joinOrCreate/nope", server.base))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 520);
    let body: HashMap<String, Value> = resp.json().await.unwrap();
    assert_eq!(body["code"], 520);
}

#[tokio::test]
async fn view_filter_sends_per_client_state() {
    let server = start_server().await;

    let r1 = matchmake(&server.base, "joinOrCreate", "secret", json!({})).await;
    let r2 = matchmake(&server.base, "joinOrCreate", "secret", json!({})).await;
    assert_eq!(r1["room"]["roomId"], r2["room"]["roomId"]);
    let sid1 = r1["sessionId"].as_str().unwrap().to_string();
    let sid2 = r2["sessionId"].as_str().unwrap().to_string();

    // second client's join triggers a state change; client 1 may receive a
    // patch about `players` before/after we read — drain until full states seen
    let mut c1 = WsClient::connect(&server.base, &r1).await;
    let mut c2 = WsClient::connect(&server.base, &r2).await;

    // full states: each client only sees their own secret
    let h1 = c1.recv().await;
    assert_eq!(h1[0], 10);
    let full1 = c1.recv().await;
    assert_eq!(full1[0], 14);
    assert_eq!(full1[1]["my_secret"], format!("secret-of-{sid1}"));
    assert!(full1[1].get("secrets").is_none(), "leaked full state: {full1}");

    let h2 = c2.recv().await;
    assert_eq!(h2[0], 10);
    let full2 = c2.recv().await;
    assert_eq!(full2[1]["my_secret"], format!("secret-of-{sid2}"));
    assert_eq!(full2[1]["players"], 2);

    // c1 bumps the round → both get patches, neither leaks the other's secret
    c1.send("bump", json!({})).await;
    let p1 = c1.recv().await;
    assert_eq!(p1[0], 15);
    let ops1 = p1[1].as_array().unwrap();
    assert!(ops1.iter().all(|op| {
        let path = op["path"].as_str().unwrap_or("");
        path == "/round" || path == "/players"
    }), "unexpected ops for c1: {ops1:?}");

    let p2 = c2.recv().await;
    assert_eq!(p2[0], 15);
    let ops2 = p2[1].as_array().unwrap();
    assert!(
        ops2.iter().any(|op| op["path"] == "/round" && op["value"] == 1),
        "unexpected ops for c2: {ops2:?}"
    );
    let serialized = Value::Array(ops2.clone()).to_string();
    assert!(!serialized.contains(&sid1), "client 2 received client 1's data");
}

#[tokio::test]
async fn persistence_restores_state_and_internal_across_restart() {
    use colyseus::snapshot::{FileSnapshotStore, PersistenceConfig};

    let dir = std::env::temp_dir().join(format!("colyseus-persist-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = PersistenceConfig::new(FileSnapshotStore::new(dir.clone()))
        .auto_save_interval(Duration::from_millis(10));

    let room_id;

    // ---- server A: create, mutate, shutdown (snapshot is written) ----
    {
        let mut server = Server::new().persistence(cfg.clone());
        server.define("persist", || PersistentRoom);
        let (app, mm) = server.build();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let base = format!("http://{addr}");

        let r = matchmake(&base, "joinOrCreate", "persist", json!({})).await;
        room_id = r["room"]["roomId"].as_str().unwrap().to_string();
        let mut c = WsClient::connect(&base, &r).await;
        let _handshake = c.recv().await;
        let _full = c.recv().await;

        c.send("increment", json!({ "by": 7 })).await;
        let _patch = c.recv().await;
        tokio::time::sleep(Duration::from_millis(200)).await; // let the auto-save fire

        mm.shutdown().await; // disposes rooms and flushes the snapshot writer
        handle.abort();
    }

    // ---- server B: restores the room before accepting traffic ----
    {
        let mut server = Server::new().persistence(cfg);
        server.define("persist", || PersistentRoom);
        let (app, mm) = server.build();

        let restored = mm.restore_all().await;
        assert_eq!(restored, vec![room_id.clone()], "room should be restored on boot");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let base = format!("http://{addr}");

        // the restored room is reachable by its original id
        let r = matchmake(&base, "joinById", &room_id, json!({})).await;
        assert_eq!(r["room"]["roomId"], room_id);
        let mut c = WsClient::connect(&base, &r).await;
        let _handshake = c.recv().await;

        // public state survived
        let full = c.recv().await;
        assert_eq!(full[0], 14);
        assert_eq!(full[1]["count"], 7, "public state should be restored");

        // server-only internal state survived (verified via a broadcast)
        c.send("report", json!({})).await;
        let report = c.recv().await;
        assert_eq!(report[1], "report");
        assert_eq!(report[2]["secret"], 8, "internal state should be restored");

        mm.shutdown().await;
        handle.abort();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn persistence_preserves_reconnection_token_across_restart() {
    use colyseus::snapshot::{FileSnapshotStore, PersistenceConfig};

    let dir = std::env::temp_dir().join(format!("colyseus-persist-rc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = PersistenceConfig::new(FileSnapshotStore::new(dir.clone()))
        .auto_save_interval(Duration::from_millis(10));

    let (room_id, session_id, token);

    // ---- server A: connect, mutate, graceful shutdown ----
    {
        let mut server = Server::new().persistence(cfg.clone());
        server.define("persist", || PersistentRoom);
        let (app, mm) = server.build();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let base = format!("http://{addr}");

        let r = matchmake(&base, "joinOrCreate", "persist", json!({})).await;
        room_id = r["room"]["roomId"].as_str().unwrap().to_string();
        session_id = r["sessionId"].as_str().unwrap().to_string();
        let mut c = WsClient::connect(&base, &r).await;
        let handshake = c.recv().await;
        token = handshake[1].as_str().unwrap().to_string();
        let _full = c.recv().await;
        c.send("increment", json!({ "by": 5 })).await;
        let _patch = c.recv().await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        mm.shutdown().await;
        handle.abort();
    }

    // ---- server B: restore, then reconnect with the original token ----
    {
        let mut server = Server::new().persistence(cfg);
        server.define("persist", || PersistentRoom);
        let (app, mm) = server.build();
        assert_eq!(mm.restore_all().await, vec![room_id.clone()]);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let base = format!("http://{addr}");

        // the persisted token is still valid after the restart
        let r2 = matchmake(
            &base,
            "reconnect",
            &room_id,
            json!({ "reconnectionToken": token }),
        )
        .await;
        assert_eq!(r2["sessionId"], session_id, "same seat should be re-seated");

        let mut c2 = WsClient::connect_with_token(&base, &r2, Some(&token)).await;
        let handshake2 = c2.recv().await;
        assert_eq!(handshake2[0], 10);
        let full2 = c2.recv().await;
        assert_eq!(full2[1]["count"], 5, "state preserved across restart + reconnect");
        let note = c2.recv().await;
        assert_eq!(note[1], "reconnected");

        mm.shutdown().await;
        handle.abort();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------
// Admin RPCs
// ---------------------------------------------------------------------

#[derive(Deserialize)]
struct SumRpc {
    a: i64,
    b: i64,
}

#[derive(Serialize)]
struct SumRpcResult {
    sum: i64,
}

#[async_trait]
impl AdminRpc for SumRpc {
    type Params = SumRpc;
    type Response = SumRpcResult;
    async fn call(p: Self::Params, _ctx: AdminContext) -> Result<Self::Response> {
        Ok(SumRpcResult { sum: p.a + p.b })
    }
}

#[derive(Deserialize)]
struct CreateStateRpc {
    mode: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateStateRpcResult {
    room_id: String,
}

#[async_trait]
impl AdminRpc for CreateStateRpc {
    type Params = CreateStateRpc;
    type Response = CreateStateRpcResult;
    async fn call(p: Self::Params, ctx: AdminContext) -> Result<Self::Response> {
        let listing = ctx.create_room("state", json!({ "mode": p.mode })).await?;
        Ok(CreateStateRpcResult {
            room_id: listing.room_id,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResetCountRpc {
    room_id: String,
}

#[derive(Serialize)]
struct ResetCountRpcResult {
    found: bool,
}

#[async_trait]
impl AdminRpc for ResetCountRpc {
    type Params = ResetCountRpc;
    type Response = ResetCountRpcResult;
    async fn call(p: Self::Params, ctx: AdminContext) -> Result<Self::Response> {
        let found = ctx.command_room::<StateRoom, _>(&p.room_id, |_room, ctx| {
            Box::pin(async move {
                if let Some(state) = ctx.state_mut::<Counter>() {
                    state.count = 0;
                }
            })
        });
        Ok(ResetCountRpcResult { found })
    }
}

// Room-based RPC: runs on the room actor, returns a response.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdjustCountRpc {
    delta: i64,
}

#[derive(Serialize, Deserialize)]
struct CountResult {
    count: i64,
}

#[async_trait]
impl RoomRpc<StateRoom> for AdjustCountRpc {
    type Params = AdjustCountRpc;
    type Response = CountResult;

    async fn call(_room: &mut StateRoom, ctx: &mut RoomContext, p: Self::Params) -> Result<CountResult> {
        let count = {
            let state = ctx.state_mut::<Counter>().unwrap();
            state.count += p.delta;
            state.count
        };
        Ok(CountResult { count })
    }
}

async fn start_admin_rpc_server() -> (String, tokio::task::JoinHandle<()>) {
    let mut server = Server::new();
    server.define("state", || StateRoom);
    let server = server
        .admin_token(Some("backend-secret".to_string()))
        .admin_rpc::<SumRpc>("sum")
        .admin_rpc::<CreateStateRpc>("createState")
        .admin_rpc::<ResetCountRpc>("resetCount")
        .room_rpc::<StateRoom, AdjustCountRpc>("adjustCount");
    let (app, _mm) = server.build();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn admin_rpc_auth_and_dispatch() {
    let (base, handle) = start_admin_rpc_server().await;

    // no token → 401
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/sum"))
        .json(&json!({ "a": 1, "b": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);

    // wrong token → 401
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/sum"))
        .bearer_auth("wrong")
        .json(&json!({ "a": 1, "b": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);

    // correct token + valid params → 200 { sum: 3 }
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/sum"))
        .bearer_auth("backend-secret")
        .json(&json!({ "a": 1, "b": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.json::<Value>().await.unwrap()["sum"], 3);

    // invalid params → 400 with a code
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/sum"))
        .bearer_auth("backend-secret")
        .json(&json!({ "a": "x", "b": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let body = resp.json::<Value>().await.unwrap();
    assert_eq!(body["code"], colyseus::codes::INVALID_PAYLOAD);

    // unknown rpc → 404
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/nope"))
        .bearer_auth("backend-secret")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    handle.abort();
}

#[tokio::test]
async fn admin_rpc_can_manage_rooms() {
    let (base, handle) = start_admin_rpc_server().await;

    // create a room via RPC
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/createState"))
        .bearer_auth("backend-secret")
        .json(&json!({ "mode": "ranked" }))
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    assert_eq!(status, 200);
    let room_id = resp.json::<Value>().await.unwrap()["roomId"]
        .as_str()
        .unwrap()
        .to_string();

    // reset its count via typed command_room
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/resetCount"))
        .bearer_auth("backend-secret")
        .json(&json!({ "roomId": room_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.json::<Value>().await.unwrap()["found"], true);

    // unknown room → found=false (not an error)
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/resetCount"))
        .bearer_auth("backend-secret")
        .json(&json!({ "roomId": "nope" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap()["found"], false);

    handle.abort();
}

#[tokio::test]
async fn room_rpc_runs_on_room_actor() {
    let (base, handle) = start_admin_rpc_server().await;

    // create a room via RPC
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rpc/createState"))
        .bearer_auth("backend-secret")
        .json(&json!({ "mode": "ranked" }))
        .send()
        .await
        .unwrap();
    let room_id = resp.json::<Value>().await.unwrap()["roomId"]
        .as_str()
        .unwrap()
        .to_string();

    // room RPC mutates typed state on the room actor and returns a response
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rooms/{room_id}/rpc/adjustCount"))
        .bearer_auth("backend-secret")
        .json(&json!({ "delta": 5 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.json::<Value>().await.unwrap()["count"], 5);

    // second call sees the mutated state (proves it ran on the room actor)
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rooms/{room_id}/rpc/adjustCount"))
        .bearer_auth("backend-secret")
        .json(&json!({ "delta": 10 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap()["count"], 15);

    // auth: no token → 401
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rooms/{room_id}/rpc/adjustCount"))
        .json(&json!({ "delta": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);

    // unknown room rpc → 404
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rooms/{room_id}/rpc/nope"))
        .bearer_auth("backend-secret")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    // unknown room → 404
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rooms/nope/rpc/adjustCount"))
        .bearer_auth("backend-secret")
        .json(&json!({ "delta": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);

    // invalid params → 400
    let resp = reqwest::Client::new()
        .post(format!("{base}/admin/api/rooms/{room_id}/rpc/adjustCount"))
        .bearer_auth("backend-secret")
        .json(&json!({ "delta": "x" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);

    handle.abort();
}
