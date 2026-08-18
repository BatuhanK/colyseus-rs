//! Admin SDK + custom RPCs demo.
//!
//! Run it:
//! ```sh
//! cargo run --example admin_rpc
//! ```
//! Then call from any HTTP client / the TypeScript admin SDK
//! (`clients/ts/src/admin.ts`):
//!
//! ```sh
//! # create a room
//! curl -X POST localhost:2567/admin/api/rpc/createGame \
//!   -H 'authorization: Bearer backend-secret' \
//!   -H 'content-type: application/json' \
//!   -d '{"mode":"ranked"}'
//!
//! # shout into it (needs a roomId from the previous call)
//! curl -X POST localhost:2567/admin/api/rpc/shout \
//!   -H 'authorization: Bearer backend-secret' \
//!   -H 'content-type: application/json' \
//!   -d '{"roomId":"<roomId>","text":"hello"}'
//!
//! # reset its score (typed room access)
//! curl -X POST localhost:2567/admin/api/rpc/resetScore \
//!   -H 'authorization: Bearer backend-secret' \
//!   -H 'content-type: application/json' \
//!   -d '{"roomId":"<roomId>"}'
//!
//! # room-based RPC (runs on the room actor, returns a response)
//! curl -X POST localhost:2567/admin/api/rooms/<roomId>/rpc/getScore \
//!   -H 'authorization: Bearer backend-secret' \
//!   -H 'content-type: application/json' \
//!   -d '{"player":"p1"}'
//! ```

use colyseus::serde_json::{json, Value};
use colyseus::{
    async_trait, AdminContext, AdminRpc, Client, Result, Room, RoomContext, RoomRpc, Server,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default)]
struct GameState {
    score: i64,
}

struct GameRoom;

#[async_trait]
impl Room for GameRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, options: Value) -> Result<()> {
        ctx.set_state(GameState::default());
        ctx.set_max_clients(Some(8));
        ctx.set_metadata(json!({ "mode": options.get("mode") }));
        Ok(())
    }

    async fn on_join(
        &mut self,
        ctx: &mut RoomContext,
        client: Client,
        _options: Value,
        _auth: Option<Value>,
    ) -> Result<()> {
        ctx.broadcast("system", &json!({ "text": format!("{} joined", client.session_id()) }));
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Custom admin RPCs (callable from your backend with a bearer token)
// ---------------------------------------------------------------------

/// `createGame` — server-side room creation.
#[derive(Deserialize)]
struct CreateGame {
    mode: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateGameResult {
    room_id: String,
}

#[async_trait]
impl AdminRpc for CreateGame {
    type Params = CreateGame;
    type Response = CreateGameResult;

    async fn call(params: Self::Params, ctx: AdminContext) -> Result<Self::Response> {
        let outcome = ctx
            .create_room("game", json!({ "mode": params.mode }))
            .await?;
        Ok(CreateGameResult {
            room_id: outcome.listing.room_id,
        })
    }
}

/// `shout` — broadcast a message into a room.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Shout {
    room_id: String,
    text: String,
}

#[derive(Serialize)]
struct ShoutResult {
    delivered: bool,
}

#[async_trait]
impl AdminRpc for Shout {
    type Params = Shout;
    type Response = ShoutResult;

    async fn call(params: Self::Params, ctx: AdminContext) -> Result<Self::Response> {
        let delivered = ctx.send_message(
            &params.room_id,
            None,
            "system",
            json!({ "text": params.text }),
        );
        Ok(ShoutResult { delivered })
    }
}

/// `resetScore` — typed access into the room actor (mutate `&mut GameRoom`).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResetScore {
    room_id: String,
}

#[derive(Serialize)]
struct ResetScoreResult {
    found: bool,
}

#[async_trait]
impl AdminRpc for ResetScore {
    type Params = ResetScore;
    type Response = ResetScoreResult;

    async fn call(params: Self::Params, ctx: AdminContext) -> Result<Self::Response> {
        let found = ctx.command_room::<GameRoom, _>(&params.room_id, |_room, ctx| {
            Box::pin(async move {
                if let Some(state) = ctx.state_mut::<GameState>() {
                    state.score = 0;
                }
            })
        });
        Ok(ResetScoreResult { found })
    }
}

// ---------------------------------------------------------------------
// Room-based RPC: runs on the room actor, returns a response.
// ---------------------------------------------------------------------

/// `getScore` — request/response into the room actor (reads typed state).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetScore {
    player: String,
}

#[derive(Serialize, Deserialize)]
struct Score {
    player: String,
    points: i64,
}

#[async_trait]
impl RoomRpc<GameRoom> for GetScore {
    type Params = GetScore;
    type Response = Score;

    async fn call(room: &mut GameRoom, ctx: &mut RoomContext, p: Self::Params) -> Result<Score> {
        let _ = room;
        let points = ctx.state::<GameState>().map(|s| s.score).unwrap_or(0);
        Ok(Score {
            player: p.player,
            points,
        })
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
    server.define("game", || GameRoom);

    // API only (no /admin panel UI), guarded by a token, with custom RPCs.
    let server = server
        .admin_token(Some("backend-secret".to_string()))
        .admin_rpc::<CreateGame>("createGame")
        .admin_rpc::<Shout>("shout")
        .admin_rpc::<ResetScore>("resetScore")
        .room_rpc::<GameRoom, GetScore>("getScore");

    server.listen("0.0.0.0:2567").await.unwrap();
}
