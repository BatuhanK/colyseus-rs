//! Tic-tac-toe + chat backend on colyseus-rs.
//!
//! - Auth: HS256 JWT (shared `GAME_SECRET` with the web app), verified in
//!   `on_auth` from the `Authorization: Bearer` header of the matchmaking call.
//! - Two players per room (X and O), chat via broadcasts, game state via
//!   automatic state sync. Reconnection supported (60s).

use std::collections::HashMap;
use std::time::Duration;

use colyseus::serde_json::{json, Value};
use colyseus::{
    async_trait, codes, AuthContext, Client, Result, Room, RoomContext, Server, ServerError,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,   // user id
    name: String,  // display name
    #[allow(dead_code)]
    exp: usize,
}

fn game_secret() -> String {
    std::env::var("GAME_SECRET").unwrap_or_else(|_| "dev-secret-change-me".to_string())
}

fn verify_token(token: &str) -> Option<Claims> {
    jsonwebtoken::decode::<Claims>(
        token,
        &DecodingKey::from_secret(game_secret().as_bytes()),
        &Validation::new(Algorithm::HS256),
    )
    .ok()
    .map(|data| data.claims)
}

// ---------------------------------------------------------------------
// State
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct PlayerInfo {
    symbol: String,
    name: String,
}

#[derive(Serialize, Deserialize)]
struct TttState {
    /// 9 cells: "" | "X" | "O"
    board: Vec<String>,
    /// sessionId → player
    players: HashMap<String, PlayerInfo>,
    /// "X" | "O"
    turn: String,
    /// "waiting" | "playing" | "finished"
    status: String,
    /// "X" | "O" | "draw" | None
    winner: Option<String>,
}

impl TttState {
    fn new() -> Self {
        TttState {
            board: vec![String::new(); 9],
            players: HashMap::new(),
            turn: "X".into(),
            status: "waiting".into(),
            winner: None,
        }
    }

    fn reset(&mut self) {
        self.board = vec![String::new(); 9];
        self.turn = "X".into();
        self.winner = None;
        self.status = if self.players.len() == 2 {
            "playing".into()
        } else {
            "waiting".into()
        };
    }
}

const WIN_LINES: [[usize; 3]; 8] = [
    [0, 1, 2],
    [3, 4, 5],
    [6, 7, 8],
    [0, 3, 6],
    [1, 4, 7],
    [2, 5, 8],
    [0, 4, 8],
    [2, 4, 6],
];

fn check_winner(board: &[String]) -> Option<String> {
    for line in WIN_LINES {
        let [a, b, c] = line;
        if !board[a].is_empty() && board[a] == board[b] && board[b] == board[c] {
            return Some(board[a].clone());
        }
    }
    if board.iter().all(|c| !c.is_empty()) {
        return Some("draw".into());
    }
    None
}

// ---------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------

#[derive(Deserialize)]
struct MoveMsg {
    cell: usize,
}

#[derive(Deserialize)]
struct ChatMsg {
    text: String,
}

// ---------------------------------------------------------------------
// Room
// ---------------------------------------------------------------------

struct TicTacToeRoom;

#[async_trait]
impl Room for TicTacToeRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, _options: Value) -> Result<()> {
        ctx.set_state(TttState::new());
        ctx.set_max_clients(Some(2));
        ctx.set_patch_rate(Some(Duration::from_millis(50)));

        ctx.on_message("move", |_room: &mut Self, ctx, client, msg: MoveMsg| {
            Box::pin(async move {
                let state = ctx.state_mut::<TttState>().unwrap();

                if state.status != "playing" {
                    return Err(ServerError::new(codes::APPLICATION_ERROR, "game is not in progress"));
                }
                let Some(player) = state.players.get(client.session_id()) else {
                    return Err(ServerError::new(codes::APPLICATION_ERROR, "you are not a player"));
                };
                let symbol = player.symbol.clone();
                if symbol != state.turn {
                    return Err(ServerError::new(codes::APPLICATION_ERROR, "not your turn"));
                }
                if msg.cell >= 9 || !state.board[msg.cell].is_empty() {
                    return Err(ServerError::new(codes::APPLICATION_ERROR, "invalid cell"));
                }

                state.board[msg.cell] = symbol.clone();

                if let Some(winner) = check_winner(&state.board) {
                    state.status = "finished".into();
                    state.winner = Some(winner.clone());
                    let name = state.players[client.session_id()].name.clone();
                    ctx.broadcast(
                        "system",
                        &json!({ "text": if winner == "draw" { "draw!".to_string() } else { format!("{name} wins!") } }),
                    );
                } else {
                    state.turn = if symbol == "X" { "O".into() } else { "X".into() };
                }
                Ok(())
            })
        });

        ctx.on_message("chat", |_room: &mut Self, ctx, client, msg: ChatMsg| {
            Box::pin(async move {
                let name = ctx
                    .state::<TttState>()
                    .and_then(|s| s.players.get(client.session_id()).map(|p| p.name.clone()))
                    .unwrap_or_else(|| client.session_id().to_string());
                ctx.broadcast(
                    "chat",
                    &json!({ "from": name, "text": msg.text.chars().take(500).collect::<String>() }),
                );
                Ok(())
            })
        });

        ctx.on_message("rematch", |_room: &mut Self, ctx, _client, _msg: Value| {
            Box::pin(async move {
                let state = ctx.state_mut::<TttState>().unwrap();
                if state.status == "finished" {
                    state.reset();
                    ctx.broadcast("system", &json!({ "text": "rematch!" }));
                }
                Ok(())
            })
        });

        Ok(())
    }

    async fn on_auth(
        &mut self,
        _ctx: &mut RoomContext,
        _options: &Value,
        auth: &AuthContext,
    ) -> Result<Option<Value>> {
        let token = auth
            .token
            .as_deref()
            .ok_or_else(|| ServerError::new(codes::AUTH_FAILED, "missing bearer token"))?;
        let claims = verify_token(token)
            .ok_or_else(|| ServerError::new(codes::AUTH_FAILED, "invalid or expired token"))?;
        Ok(Some(json!({ "userId": claims.sub, "name": claims.name })))
    }

    async fn on_join(
        &mut self,
        ctx: &mut RoomContext,
        client: Client,
        _options: Value,
        auth: Option<Value>,
    ) -> Result<()> {
        let name = auth
            .as_ref()
            .and_then(|a| a["name"].as_str())
            .unwrap_or("anonymous")
            .to_string();
        let user_id = auth
            .as_ref()
            .and_then(|a| a["userId"].as_str())
            .unwrap_or("")
            .to_string();

        // session takeover: same account already seated under a stale session
        // (page reload after the reconnection window, second tab, …).
        // players are matched via the stale client's auth userId.
        let stale_sid = if user_id.is_empty() {
            None
        } else {
            ctx.clients()
                .iter()
                .find(|c| {
                    c.session_id() != client.session_id()
                        && c.auth()
                            .and_then(|a| a["userId"].as_str().map(String::from))
                            .as_deref()
                            == Some(user_id.as_str())
                })
                .map(|c| c.session_id().to_string())
        };
        if let Some(old_sid) = stale_sid {
            if let Some(old_client) = ctx.get_client(&old_sid) {
                old_client.leave(Some(codes::MATCHMAKE_EXPIRED), Some("session moved"));
            }
            ctx.remove_client(&old_sid);
            let state = ctx.state_mut::<TttState>().unwrap();
            if let Some(info) = state.players.remove(&old_sid) {
                state.players.insert(client.session_id().to_string(), info);
            }
            ctx.broadcast("system", &json!({ "text": format!("{name} rejoined") }));
            return Ok(());
        }

        let state = ctx.state_mut::<TttState>().unwrap();
        let has_x = state.players.values().any(|p| p.symbol == "X");
        let symbol = if has_x { "O" } else { "X" };
        state.players.insert(
            client.session_id().to_string(),
            PlayerInfo {
                symbol: symbol.to_string(),
                name: name.clone(),
            },
        );
        if state.players.len() == 2 {
            state.status = "playing".into();
        }

        ctx.broadcast("system", &json!({ "text": format!("{name} joined as {symbol}") }));
        Ok(())
    }

    async fn on_drop(&mut self, ctx: &mut RoomContext, client: Client, _code: u16) {
        ctx.allow_reconnection(&client, Some(Duration::from_secs(60)));
        ctx.broadcast("system", &json!({ "text": "opponent disconnected…" }));
    }

    async fn on_reconnect(&mut self, ctx: &mut RoomContext, _client: Client) {
        ctx.broadcast("system", &json!({ "text": "opponent reconnected" }));
    }

    async fn on_leave(&mut self, ctx: &mut RoomContext, client: Client, _code: u16) {
        let state = ctx.state_mut::<TttState>().unwrap();
        let name = state
            .players
            .get(client.session_id())
            .map(|p| p.name.clone())
            .unwrap_or_default();
        state.players.remove(client.session_id());
        state.reset(); // back to waiting; the remaining player keeps their seat
        ctx.broadcast("system", &json!({ "text": format!("{name} left the game") }));
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut server = Server::new()
        .ws_buffer_sizes(16 * 1024, 32 * 1024)
        .public_address("localhost:2567");
    server.define("tictactoe", || TicTacToeRoom);
    server.listen("0.0.0.0:2567").await.unwrap();
}
