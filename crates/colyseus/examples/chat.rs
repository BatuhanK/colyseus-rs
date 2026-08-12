//! A minimal chat room: join, send messages, broadcast to everyone.
//!
//! Try it:
//! ```sh
//! cargo run --example chat
//! ```
//! Then connect with the TypeScript client (clients/ts) or any msgpack-capable
//! WebSocket client (see README for the protocol).

use colyseus::serde_json::{json, Value};
use colyseus::{async_trait, Client, Result, Room, RoomContext, Server};
use serde::Deserialize;

struct ChatRoom;

#[derive(Deserialize)]
struct ChatMessage {
    text: String,
}

#[async_trait]
impl Room for ChatRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.set_max_clients(Some(64));

        ctx.on_message(
            "chat",
            |_room: &mut ChatRoom, ctx, client, msg: ChatMessage| {
                Box::pin(async move {
                    ctx.broadcast(
                        "chat",
                        &json!({
                            "from": client.session_id(),
                            "text": msg.text,
                        }),
                    );
                    Ok(())
                })
            },
        );

        Ok(())
    }

    async fn on_join(
        &mut self,
        ctx: &mut RoomContext,
        client: Client,
        _options: Value,
        _auth: Option<Value>,
    ) -> Result<()> {
        tracing::info!("{} joined chat", client.session_id());
        ctx.broadcast("system", &json!({ "text": format!("{} joined", client.session_id()) }));
        client.send("system", &json!({ "text": "welcome!" }));
        Ok(())
    }

    async fn on_leave(&mut self, ctx: &mut RoomContext, client: Client, _code: u16) {
        ctx.broadcast("system", &json!({ "text": format!("{} left", client.session_id()) }));
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut server = Server::new();
    server.define("chat", || ChatRoom);
    server.listen("0.0.0.0:2567").await.unwrap();
}
