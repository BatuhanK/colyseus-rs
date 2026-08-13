//! Server-side client connection handle.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::error::{close_codes, codes};
use crate::protocol::{self, MessageType};
use crate::utils::generate_id;

/// Connection state of a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    Joining,
    Joined,
    Reconnecting,
    Reconnected,
    Leaving,
    Closed,
}

/// Outbound frame towards a client's WebSocket writer task.
pub(crate) enum Outbound {
    Bytes(Bytes),
    Close(u16, String),
}

/// Options for `send` / `broadcast`.
#[derive(Debug, Clone, Default)]
pub struct SendOptions {
    /// Defer delivery until right after the next state patch broadcast.
    pub after_next_patch: bool,
}

/// A message deferred until after the next patch broadcast.
pub(crate) struct AfterPatchItem {
    pub target: AfterPatchTarget,
    pub bytes: Vec<u8>,
}

pub(crate) enum AfterPatchTarget {
    Client(String),
    Broadcast { except: Option<String> },
}

pub(crate) type AfterPatchQueue = Arc<Mutex<Vec<AfterPatchItem>>>;

pub(crate) struct ClientInner {
    /// Unique per physical connection (survives nothing — a new WebSocket
    /// means a new connection id, even for the same session).
    pub connection_id: String,
    pub session_id: String,
    pub tx: mpsc::UnboundedSender<Outbound>,
    pub state: RwLock<ClientState>,
    pub auth: RwLock<Option<Value>>,
    pub user_data: RwLock<Option<Value>>,
    pub reconnection_token: RwLock<String>,
    /// Messages sent while not yet fully joined; flushed on join.
    pub pending: Mutex<Vec<Vec<u8>>>,
    /// Shared with the room, used for `after_next_patch` delivery.
    pub after_patch: RwLock<Option<AfterPatchQueue>>,
    /// Room traffic tap (admin panel).
    pub tap: RwLock<Option<crate::room::EventTap>>,
    /// Rate limiting window: (window_start, count).
    pub rate: Mutex<(Instant, u32)>,
}

/// A handle to a connected client.
///
/// Cheaply cloneable (Arc-backed) and uses interior mutability, so it can be
/// held and used from any room handler without borrow conflicts.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

impl Client {
    pub(crate) fn new(session_id: String, tx: mpsc::UnboundedSender<Outbound>) -> Self {
        Client {
            inner: Arc::new(ClientInner {
                connection_id: generate_id(),
                session_id,
                tx,
                state: RwLock::new(ClientState::Joining),
                auth: RwLock::new(None),
                user_data: RwLock::new(None),
                reconnection_token: RwLock::new(String::new()),
                pending: Mutex::new(Vec::new()),
                after_patch: RwLock::new(None),
                tap: RwLock::new(None),
                rate: Mutex::new((Instant::now(), 0)),
            }),
        }
    }

    /// A client handle with no live transport.
    ///
    /// Used when restoring persisted reconnection entries: the handle carries
    /// the session id / auth / user data so a reconnecting client can be
    /// re-seated, but any outbound send is dropped silently (the receiver is
    /// dropped immediately).
    pub(crate) fn orphan(session_id: &str) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        Client::new(session_id.to_string(), tx)
    }

    pub fn session_id(&self) -> &str {
        &self.inner.session_id
    }

    pub fn state(&self) -> ClientState {
        *self.inner.state.read()
    }

    pub fn is_joined(&self) -> bool {
        matches!(self.state(), ClientState::Joined | ClientState::Reconnected)
    }

    /// Auth data produced by `Room::on_auth`.
    pub fn auth(&self) -> Option<Value> {
        self.inner.auth.read().clone()
    }

    /// Arbitrary per-connection data. Never synchronized anywhere.
    pub fn user_data(&self) -> Option<Value> {
        self.inner.user_data.read().clone()
    }

    pub fn set_user_data(&self, value: Value) {
        *self.inner.user_data.write() = Some(value);
    }

    /// The token the client uses to reconnect after `allow_reconnection`.
    pub fn reconnection_token(&self) -> String {
        self.inner.reconnection_token.read().clone()
    }

    /// Send a typed message (`[13, type, payload]`, msgpack).
    pub fn send<T: Serialize>(&self, msg_type: impl Into<MessageType>, message: &T) {
        self.send_with_options(msg_type, message, SendOptions::default());
    }

    pub fn send_with_options<T: Serialize>(
        &self,
        msg_type: impl Into<MessageType>,
        message: &T,
        options: SendOptions,
    ) {
        let msg_type = msg_type.into();
        if let Some(tap) = self.inner.tap.read().as_ref() {
            if tap.receiver_count() > 0 {
                let _ = tap.send(
                    crate::room::RoomEventLog::new("out", "send")
                        .client(&self.inner.session_id)
                        .msg_type(&msg_type)
                        .payload(serde_json::to_value(message).unwrap_or(Value::Null)),
                );
            }
        }
        let bytes = protocol::room_data(&msg_type, message);
        self.deliver(bytes, options);
    }

    /// Send a typed binary message (`[17, type, bytes]`).
    pub fn send_bytes(&self, msg_type: impl Into<MessageType>, payload: &[u8]) {
        let msg_type = msg_type.into();
        if let Some(tap) = self.inner.tap.read().as_ref() {
            if tap.receiver_count() > 0 {
                let _ = tap.send(
                    crate::room::RoomEventLog::new("out", "send")
                        .client(&self.inner.session_id)
                        .msg_type(&msg_type)
                        .bytes(payload.len()),
                );
            }
        }
        let bytes = protocol::room_data_bytes(&msg_type, payload);
        self.deliver(bytes, SendOptions::default());
    }

    /// Send a protocol-level error message to this client.
    pub fn error(&self, code: u16, message: &str) {
        self.raw_now(protocol::error(code, message));
    }

    /// Close this client's connection.
    pub fn leave(&self, code: Option<u16>, reason: Option<&str>) {
        self.set_state(ClientState::Leaving);
        let _ = self.inner.tx.send(Outbound::Close(
            code.unwrap_or(close_codes::NORMAL_CLOSURE),
            reason.unwrap_or_default().to_string(),
        ));
    }

    fn deliver(&self, bytes: Vec<u8>, options: SendOptions) {
        if options.after_next_patch {
            if let Some(queue) = &*self.inner.after_patch.read() {
                queue.lock().push(AfterPatchItem {
                    target: AfterPatchTarget::Client(self.session_id().to_string()),
                    bytes,
                });
                return;
            }
        }
        match self.state() {
            ClientState::Joined | ClientState::Reconnected => self.raw_now(bytes),
            ClientState::Joining | ClientState::Reconnecting => {
                self.inner.pending.lock().push(bytes);
            }
            _ => {}
        }
    }

    /// Enqueue raw bytes for immediate delivery (if the transport is alive).
    pub(crate) fn raw_now(&self, bytes: Vec<u8>) {
        let _ = self.inner.tx.send(Outbound::Bytes(Bytes::from(bytes)));
    }

    pub(crate) fn flush_pending(&self) {
        let pending = std::mem::take(&mut *self.inner.pending.lock());
        for bytes in pending {
            self.raw_now(bytes);
        }
    }

    /// Send a close frame without touching the client state.
    /// (Used for reconnecting clients whose socket is being recycled.)
    pub(crate) fn close_transport(&self, code: u16, reason: &str) {
        let _ = self.inner.tx.send(Outbound::Close(code, reason.to_string()));
    }

    pub(crate) fn set_state(&self, state: ClientState) {
        *self.inner.state.write() = state;
    }

    pub(crate) fn set_auth(&self, auth: Option<Value>) {
        *self.inner.auth.write() = auth;
    }

    pub(crate) fn set_reconnection_token(&self, token: String) {
        *self.inner.reconnection_token.write() = token;
    }

    pub(crate) fn attach_after_patch(&self, queue: AfterPatchQueue) {
        *self.inner.after_patch.write() = Some(queue);
    }

    pub(crate) fn attach_tap(&self, tap: crate::room::EventTap) {
        *self.inner.tap.write() = Some(tap);
    }

    /// Sliding-window message rate check. Returns false when over the limit.
    pub(crate) fn check_rate(&self, max_per_second: Option<u32>) -> bool {
        let Some(max) = max_per_second else { return true };
        let mut rate = self.inner.rate.lock();
        let now = Instant::now();
        if now.duration_since(rate.0).as_secs() >= 1 {
            *rate = (now, 0);
        }
        rate.1 += 1;
        rate.1 <= max
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("session_id", &self.inner.session_id)
            .field("state", &self.state())
            .finish()
    }
}

/// Send an "invalid payload" error for a failed message deserialization.
pub(crate) fn invalid_payload(client: &Client, err: impl std::fmt::Display) {
    client.error(codes::INVALID_PAYLOAD, &format!("invalid payload: {err}"));
}
