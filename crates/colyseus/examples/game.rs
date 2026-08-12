//! A state-synced game room: players move around a 2D map.
//!
//! Demonstrates:
//! - `set_state` + automatic JSON-Patch broadcasts (default 50ms patch rate)
//! - `set_simulation_interval` game loop (`on_tick`)
//! - typed message handlers
//! - `allow_reconnection` for dropped clients
//!
//! ```sh
//! cargo run --example game
//! ```

use std::collections::HashMap;
use std::time::Duration;

use colyseus::serde_json::Value;
use colyseus::{async_trait, Client, Result, Room, RoomContext, Server};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------
// State: any Serialize struct works. Diffed and patched automatically.
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct Player {
    x: f64,
    y: f64,
    vx: f64,
    vy: f64,
}

#[derive(Serialize, Deserialize)]
struct GameState {
    players: HashMap<String, Player>,
}

// ---------------------------------------------------------------------

struct GameRoom;

#[derive(Deserialize)]
struct MoveInput {
    vx: f64,
    vy: f64,
}

#[async_trait]
impl Room for GameRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.set_state(GameState {
            players: HashMap::new(),
        });
        ctx.set_max_clients(Some(16));
        ctx.set_simulation_interval(Some(Duration::from_millis(1000 / 30))); // 30fps
        ctx.set_patch_rate(Some(Duration::from_millis(50))); // 20 updates/sec

        ctx.on_message(
            "move",
            |room: &mut GameRoom, ctx, client, input: MoveInput| {
                Box::pin(async move {
                    room.set_velocity(ctx, client.session_id(), input.vx, input.vy);
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
        ctx.state_mut::<GameState>().unwrap().players.insert(
            client.session_id().to_string(),
            Player {
                x: 0.0,
                y: 0.0,
                vx: 0.0,
                vy: 0.0,
            },
        );
        Ok(())
    }

    async fn on_tick(&mut self, ctx: &mut RoomContext, delta: f64) {
        let state = ctx.state_mut::<GameState>().unwrap();
        for player in state.players.values_mut() {
            player.x += player.vx * delta;
            player.y += player.vy * delta;
        }
        // patches are broadcast automatically at the patch rate
    }

    async fn on_drop(&mut self, ctx: &mut RoomContext, client: Client, _code: u16) {
        // keep the player around for 10 seconds in case they come back
        ctx.allow_reconnection(&client, Some(Duration::from_secs(10)));
    }

    async fn on_reconnect(&mut self, _ctx: &mut RoomContext, client: Client) {
        tracing::info!("{} reconnected", client.session_id());
    }

    async fn on_leave(&mut self, ctx: &mut RoomContext, client: Client, _code: u16) {
        ctx.state_mut::<GameState>()
            .unwrap()
            .players
            .remove(client.session_id());
    }
}

impl GameRoom {
    fn set_velocity(&mut self, ctx: &mut RoomContext, session_id: &str, vx: f64, vy: f64) {
        if let Some(p) = ctx
            .state_mut::<GameState>()
            .unwrap()
            .players
            .get_mut(session_id)
        {
            p.vx = vx.clamp(-100.0, 100.0);
            p.vy = vy.clamp(-100.0, 100.0);
        }
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
    server
        .define("game", || GameRoom)
        .filter_by(&["mode"])
        .sort_by(&[("clients", 1)]); // prefer the emptiest room
    server.listen("0.0.0.0:2567").await.unwrap();
}
