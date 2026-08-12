//! The global "mainpage" chat room.
//!
//! - Defined as `.internal()` and created exactly once at server startup via
//!   `MatchMaker::create_room`, so clients can't create it through the
//!   matchmaking API (they can only `join` it).
//! - `auto_dispose = false` — the room lives forever.
//!
//! Data model note: the message history is **room-internal state**, not
//! synchronized state. A feed/log doesn't belong in state sync — joining
//! clients get the last 50 messages pushed once (`history` message), then
//! only new messages arrive as broadcasts. No serialize+diff per message.

use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use colyseus::serde_json::Value;
use colyseus::{async_trait, AuthContext, Client, Result, Room, RoomContext, RoomSender};
use serde::{Deserialize, Serialize};

use crate::auth;
use crate::state::now_millis;

const MAX_MESSAGES: usize = 50;
const MAX_CLIENTS: u32 = 500;

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ChatMessage {
    pub from: String,
    pub text: String,
    pub at: u64,
    /// "chat" | "system"
    pub kind: String,
    /// set for system messages announcing a new game room
    pub room_id: Option<String>,
}

/// Shared slot for the chat room's sender, so server-side code (e.g. the
/// lobby-event subscriber in main.rs) can inject system messages.
#[derive(Clone, Default)]
pub struct ChatHandle(Arc<RwLock<Option<RoomSender>>>);

impl ChatHandle {
    pub fn set(&self, sender: RoomSender) {
        *self.0.write().unwrap() = Some(sender);
    }

    pub fn get(&self) -> Option<RoomSender> {
        self.0.read().unwrap().clone()
    }
}

pub struct ChatRoom {
    handle: ChatHandle,
    /// internal only — never synchronized
    messages: VecDeque<ChatMessage>,
}

impl ChatRoom {
    pub fn new(handle: ChatHandle) -> Self {
        ChatRoom {
            handle,
            messages: VecDeque::new(),
        }
    }

    fn push(&mut self, msg: ChatMessage) {
        self.messages.push_back(msg);
        while self.messages.len() > MAX_MESSAGES {
            self.messages.pop_front();
        }
    }

    /// Server-injected announcement (new game rooms, etc).
    pub fn system_message(&mut self, ctx: &mut RoomContext, text: String, room_id: Option<String>) {
        let msg = ChatMessage {
            from: "system".into(),
            text,
            at: now_millis(),
            kind: "system".into(),
            room_id,
        };
        self.push(msg.clone());
        ctx.broadcast("chat", &msg);
    }
}

#[derive(Deserialize)]
struct ChatMsg {
    text: String,
}

#[async_trait]
impl Room for ChatRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.auto_dispose = false;
        ctx.set_max_clients(Some(MAX_CLIENTS));
        self.handle.set(ctx.sender());

        ctx.on_message("chat", |room: &mut Self, ctx, client, msg: ChatMsg| {
            Box::pin(async move {
                let text: String = msg.text.trim().chars().take(300).collect();
                if text.is_empty() {
                    return Ok(());
                }
                let from = client
                    .auth()
                    .and_then(|a| a["name"].as_str().map(String::from))
                    .unwrap_or_else(|| "???".into());
                let msg = ChatMessage {
                    from,
                    text,
                    at: now_millis(),
                    kind: "chat".into(),
                    room_id: None,
                };
                room.push(msg.clone());
                ctx.broadcast("chat", &msg);
                Ok(())
            })
        });

        tracing::info!("global chat room created (roomId: {})", ctx.room_id());
        Ok(())
    }

    async fn on_auth(
        &mut self,
        _ctx: &mut RoomContext,
        _options: &Value,
        auth_ctx: &AuthContext,
    ) -> Result<Option<Value>> {
        auth::authenticate(auth_ctx).map(Some)
    }

    async fn on_join(
        &mut self,
        _ctx: &mut RoomContext,
        client: Client,
        _options: Value,
        _auth: Option<Value>,
    ) -> Result<()> {
        // push the recent history once; from here on it's live broadcasts
        let history: Vec<&ChatMessage> = self.messages.iter().collect();
        client.send("history", &history);
        Ok(())
    }
}
