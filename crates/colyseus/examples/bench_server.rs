//! Standalone bench server (measure this process's RSS/CPU while
//! `bench` clients hammer it from another process).
//!
//! ```sh
//! cargo run --release --example bench_server -- 0.0.0.0:2568
//! BENCH_CLIENT_ONLY=127.0.0.1:2568 ./target/release/examples/bench 1000 50 2 20
//! ```

use std::collections::HashMap;
use std::time::Duration;

use colyseus::serde_json::Value;
use colyseus::{async_trait, Result, Room, RoomContext, Server};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct Player {
    x: f64,
    y: f64,
    vx: f64,
    vy: f64,
    hp: i32,
    name: String,
}

#[derive(Serialize, Deserialize)]
struct BenchState {
    players: HashMap<String, Player>,
    tick: u64,
}

struct BenchRoom;

#[async_trait]
impl Room for BenchRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, options: Value) -> Result<()> {
        let room_size = options["room_size"].as_u64().unwrap_or(20) as u32;
        ctx.set_max_clients(Some(room_size));

        if options["state"].as_bool().unwrap_or(false) {
            ctx.set_state(BenchState {
                players: HashMap::new(),
                tick: 0,
            });
            ctx.set_simulation_interval(Some(Duration::from_millis(1000 / 30)));
            ctx.set_patch_rate(Some(Duration::from_millis(50)));
        }

        ctx.on_message(
            "ping",
            |_room: &mut BenchRoom, ctx, _client, msg: Value| {
                Box::pin(async move {
                    ctx.broadcast("msg", &msg);
                    Ok(())
                })
            },
        );
        Ok(())
    }

    async fn on_join(
        &mut self,
        ctx: &mut RoomContext,
        client: colyseus::Client,
        _options: Value,
        _auth: Option<Value>,
    ) -> Result<()> {
        if let Some(state) = ctx.state_mut::<BenchState>() {
            state.players.insert(
                client.session_id().to_string(),
                Player {
                    x: 0.0,
                    y: 0.0,
                    vx: 1.0,
                    vy: 0.5,
                    hp: 100,
                    name: format!("player-{}", client.session_id()),
                },
            );
        }
        Ok(())
    }

    async fn on_tick(&mut self, ctx: &mut RoomContext, delta: f64) {
        let Some(state) = ctx.state_mut::<BenchState>() else {
            return;
        };
        state.tick += 1;
        for p in state.players.values_mut() {
            p.x += p.vx * delta;
            p.y += p.vy * delta;
        }
    }

    async fn on_leave(&mut self, ctx: &mut RoomContext, client: colyseus::Client, _code: u16) {
        if let Some(state) = ctx.state_mut::<BenchState>() {
            state.players.remove(client.session_id());
        }
    }
}

#[tokio::main]
async fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:2568".to_string());
    let mut server = Server::new().ws_buffer_sizes(16 * 1024, 32 * 1024);
    server
        .define("bench", || BenchRoom)
        .filter_by(&["g", "state", "room_size"]);
    println!("pid={}", std::process::id());
    server.listen(&addr).await.unwrap();
}
