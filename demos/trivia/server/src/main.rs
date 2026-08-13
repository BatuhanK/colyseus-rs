//! 🧠 trivia arena — game backend on colyseus-rs.
//!
//! All game logic lives in `commands/` (Command Pattern); `room.rs` is a thin
//! `Room` impl that translates lifecycle hooks and client messages into
//! commands. Long-running LLM calls happen in background tasks and re-enter
//! the room through `RoomSender` as `QuestionReady` commands.
//!
//! `chat.rs` holds the global mainpage chat room: created once here at
//! startup (clients can't create it), and every new trivia room announces
//! itself there via the lobby-event subscriber below.

mod auth;
mod chat;
mod commands;
mod llm;
mod results;
mod room;
mod state;

use std::sync::Arc;

use colyseus::serde_json::json;
use colyseus::{FileSnapshotStore, MatchmakerEvent, PersistenceConfig, Server};
use llm::LlmClient;
use results::ResultsPublisher;
use room::TriviaRoom;

use chat::{ChatHandle, ChatRoom};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let llm = Arc::new(LlmClient::from_env());
    let results = ResultsPublisher::from_env().await.map(Arc::new);
    if results.is_none() {
        tracing::warn!("REDIS_URL unreachable — results will only be logged");
    }

    let mut server = Server::new()
        .ws_buffer_sizes(4 * 1024, 8 * 1024)
        .public_address("localhost:2568")
        .admin_panel(Some(
            std::env::var("ADMIN_TOKEN").unwrap_or_else(|_| "admin123".into()),
        ))
        .persistence(PersistenceConfig::new(FileSnapshotStore::new("./snapshots")));

    server
        .define("trivia", {
            let llm = llm.clone();
            let results = results.clone();
            move || TriviaRoom::new(llm.clone(), results.clone())
        })
        .filter_by(&["difficulty", "category"])
        .sort_by(&[("clients", 1)]);

    // the global chat room — internal: only creatable server-side, and
    // non-persistent: recreated fresh at every startup (never snapshotted)
    let chat_handle = ChatHandle::default();
    server.define("chat", {
        let chat_handle = chat_handle.clone();
        move || ChatRoom::new(chat_handle.clone())
    }).internal().persistent(false);

    // build the router manually (instead of server.listen) so we can create
    // the bootstrap room and subscribe to lobby events before serving
    let (app, mm) = server.build();

    // restore persisted rooms before accepting traffic
    let restored = mm.restore_all().await;
    tracing::info!("restored {} room(s) from snapshots", restored.len());

    mm.create_room("chat", json!({}))
        .await
        .expect("failed to create the global chat room");

    // announce every new trivia room in the global chat
    tokio::spawn({
        let mut events = mm.subscribe();
        let chat_handle = chat_handle.clone();
        let driver = mm.driver();
        async move {
            while let Ok(event) = events.recv().await {
                let MatchmakerEvent::RoomCreated(listing) = event else {
                    continue;
                };
                if listing.name != "trivia" {
                    continue;
                }
                let Some(sender) = chat_handle.get() else {
                    continue;
                };
                let category = listing
                    .extra
                    .get("category")
                    .and_then(|v| v.as_str())
                    .unwrap_or("genel")
                    .to_string();
                let difficulty = listing
                    .extra
                    .get("difficulty")
                    .and_then(|v| v.as_str())
                    .unwrap_or("easy")
                    .to_string();
                let room_id = listing.room_id.clone();
                let driver = driver.clone();
                let sender = sender.clone();
                tokio::spawn(async move {
                    // skip ghost rooms from double-submits / instant disconnects
                    // (they get created and auto-disposed within milliseconds)
                    tokio::time::sleep(std::time::Duration::from_millis(750)).await;
                    if driver.get(&room_id).is_none() {
                        return;
                    }
                    sender.send(move |room: &mut ChatRoom, ctx| {
                        let room_id = room_id.clone();
                        Box::pin(async move {
                            room.system_message(
                                ctx,
                                format!("🎮 new game: {category} ({difficulty})"),
                                Some(room_id),
                            );
                        })
                    });
                });
            }
        }
    });

    let addr = "0.0.0.0:2568";
    println!("⚔️  colyseus-rs — listening on ws://{addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down: disposing all rooms...");
            mm.shutdown().await;
        })
        .await
        .expect("server error");
}
