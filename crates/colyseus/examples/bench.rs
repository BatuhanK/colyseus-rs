//! Load benchmark: spawns a server + N local WebSocket clients and measures
//! message throughput. Watch RSS/CPU externally (`ps -o rss,%cpu -p <pid>`).
//!
//! ```sh
//! cargo run --release --example bench -- <clients> <room_size> <msg_per_sec_per_client> <duration_s> [state]
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use colyseus::serde_json::{json, Value};
use colyseus::{async_trait, Result, Room, RoomContext, Server};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------

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
            // realistic per-player state sync room, 20 Hz patches
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

// ---------------------------------------------------------------------

static RECEIVED: AtomicU64 = AtomicU64::new(0);
static SENT: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n_clients: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(500);
    let room_size: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(50);
    let msg_per_sec: f64 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(1.0);
    let duration_s: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(15);
    let with_state = args.get(5).map(|s| s == "state").unwrap_or(false);

    // BENCH_CLIENT_ONLY=host:port → don't spawn a server, connect to an external one
    let client_only = std::env::var("BENCH_CLIENT_ONLY").ok();

    let addr: std::net::SocketAddr = if let Some(addr) = &client_only {
        addr.parse().unwrap()
    } else {
        let mut server = Server::new().disable_greet();
        server
            .define("bench", || BenchRoom)
            .filter_by(&["g", "state", "room_size"]);
        let (app, _mm) = server.build();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    };

    let http = reqwest::Client::new();
    let mut joins = Vec::new();
    let connect_started = std::time::Instant::now();

    for i in 0..n_clients {
        let base = format!("http://{addr}");
        let http = http.clone();
        joins.push(tokio::spawn(async move {
            let res: Value = http
                .post(format!("{base}/matchmake/joinOrCreate/bench"))
                .json(&json!({
                    "g": i / room_size,
                    "state": with_state,
                    "room_size": room_size,
                }))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let room_id = res["room"]["roomId"].as_str().unwrap();
            let session_id = res["sessionId"].as_str().unwrap();
            let url = format!("ws://{addr}/ws/{room_id}?sessionId={session_id}");
            let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
            (ws, res["room"]["roomId"].as_str().unwrap().to_string())
        }));
        // mild pacing to avoid thundering herd on the listen backlog
        if i % 100 == 99 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let mut sockets = Vec::new();
    for j in joins {
        sockets.push(j.await.unwrap());
    }
    println!(
        "connected {} clients in {:?} ({} rooms)",
        n_clients,
        connect_started.elapsed(),
        n_clients.div_ceil(room_size)
    );

    let send_interval = Duration::from_secs_f64(1.0 / msg_per_sec);
    let mut tasks = Vec::new();
    for (ws, _room) in sockets {
        let (mut write, mut read) = ws.split();
        tasks.push(tokio::spawn(async move {
            while let Some(Ok(_)) = read.next().await {
                RECEIVED.fetch_add(1, Ordering::Relaxed);
            }
        }));
        tasks.push(tokio::spawn(async move {
            let payload = rmp_serde::to_vec(&json!([13, "ping", {"n": 1}])).unwrap();
            loop {
                tokio::time::sleep(send_interval).await;
                if write
                    .send(Message::Binary(payload.clone().into()))
                    .await
                    .is_err()
                {
                    break;
                }
                SENT.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let start = std::time::Instant::now();
    let mut last_rx = 0u64;
    let mut last_tx = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let elapsed = start.elapsed().as_secs();
        let rx = RECEIVED.load(Ordering::Relaxed);
        let tx = SENT.load(Ordering::Relaxed);
        println!(
            "t={:>3}s  in: {:>8}/s  out: {:>9}/s   (totals: in={tx}, out={rx})",
            elapsed,
            (tx - last_tx) / 5,
            (rx - last_rx) / 5,
        );
        last_rx = rx;
        last_tx = tx;
        if elapsed >= duration_s {
            break;
        }
    }

    // keep process alive briefly so external RSS/CPU sampling stays valid
    println!("done. pid={}", std::process::id());
    std::process::exit(0);
}

// silence unused-import warning for Arc in some configs
#[allow(dead_code)]
fn _u(_: Arc<()>) {}
