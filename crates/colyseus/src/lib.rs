//! # colyseus-rs
//!
//! A multiplayer game server framework for Rust, inspired by
//! [Colyseus](https://colyseus.io/). Rooms, matchmaking, seat reservations,
//! state synchronization (JSON-Patch over MessagePack), reconnection, timers
//! and a game loop — built on tokio, with each room running as its own actor
//! task.
//!
//! ```ignore
//! use colyseus::{async_trait, Room, RoomContext, Server, Result};
//! use serde_json::Value;
//!
//! struct MyRoom;
//!
//! #[async_trait]
//! impl Room for MyRoom {
//!     async fn on_create(&mut self, ctx: &mut RoomContext, options: Value) -> Result<()> {
//!         ctx.set_max_clients(Some(4));
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut server = Server::new();
//!     server.define("my_room", || MyRoom);
//!     server.listen("0.0.0.0:2567").await.unwrap();
//! }
//! ```

pub mod protocol;
pub mod presence;
pub mod driver;
pub mod snapshot;

mod actor;
mod admin;
mod admin_rpc;
mod client;
mod command;
mod diff;
mod error;
mod matchmaker;
mod room;
mod server;
mod state;
mod utils;

pub use async_trait::async_trait;
pub use serde_json;

pub use actor::{ClientInspection, RoomInspection, RoomSender};
pub use admin_rpc::{AdminContext, AdminRpc, RoomRpc};
pub use client::{Client, ClientState, SendOptions};
pub use command::{Command, Dispatchable, Dispatcher};
pub use driver::{Conditions, LocalDriver, RoomListing, SortOptions};
pub use error::{close_codes, codes, Result, ServerError};
pub use matchmaker::{AuthContext, MatchMaker, MatchmakerEvent, RegisteredHandler, SeatReservation};
pub use presence::{LocalPresence, Presence};
pub use protocol::MessageType;
pub use room::{BoxFuture, BroadcastOptions, Room, RoomContext};
pub use server::Server;
pub use snapshot::{FileSnapshotStore, PersistenceConfig, RoomSnapshot, SnapshotStore};
pub use state::StateEdit;
pub use utils::Clock;
