//! The trivia room: a thin `Room` impl that delegates all game logic to
//! commands (see `commands/`). Server-only secrets live here, never in state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use colyseus::serde_json::{json, Value};
use colyseus::{
    async_trait, codes, AuthContext, Client, Dispatchable, Dispatcher, Result, Room, RoomContext,
    ServerError,
};
use serde::Deserialize;

use crate::auth;
use crate::commands;
use crate::llm::{LlmClient, Question};
use crate::results::ResultsPublisher;
use crate::state::{TriviaState, MAX_PLAYERS, MAX_SPECTATORS};

pub struct TriviaRoom {
    dispatcher: Dispatcher<TriviaRoom>,

    // services
    pub llm: Arc<LlmClient>,
    pub results: Option<Arc<ResultsPublisher>>,

    // room settings
    pub password: Option<String>,

    // server-only game data (never synchronized to clients):
    pub correct_index: Option<usize>,
    pub round_answers: HashMap<String, usize>,
    pub pending_questions: HashMap<u32, Question>,
    pub batch_in_flight: bool,
    pub needed_round: Option<u32>,
    pub round_timer: Option<u64>,
    pub break_timer: Option<u64>,
}

impl TriviaRoom {
    pub fn new(llm: Arc<LlmClient>, results: Option<Arc<ResultsPublisher>>) -> Self {
        TriviaRoom {
            dispatcher: Dispatcher::new(),
            llm,
            results,
            password: None,
            correct_index: None,
            round_answers: HashMap::new(),
            pending_questions: HashMap::new(),
            batch_in_flight: false,
            needed_round: None,
            round_timer: None,
            break_timer: None,
        }
    }
}

impl Dispatchable for TriviaRoom {
    fn dispatcher_mut(&mut self) -> &mut Dispatcher<Self> {
        &mut self.dispatcher
    }
}

#[derive(Deserialize)]
struct AnswerMsg {
    choice: usize,
}

#[derive(Deserialize)]
struct ChatMsg {
    text: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header};

    fn jwt() -> String {
        #[derive(serde::Serialize)]
        struct C {
            sub: String,
            name: String,
            exp: usize,
        }
        jsonwebtoken::encode(
            &Header::default(),
            &C {
                sub: "u1".into(),
                name: "tester".into(),
                exp: 4_000_000_000,
            },
            &EncodingKey::from_secret("dev-secret-change-me".as_bytes()),
        )
        .unwrap()
    }

    fn authed(token: &str) -> AuthContext {
        AuthContext {
            token: Some(token.to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn password_gate() {
        let mut room = TriviaRoom::new(Arc::new(LlmClient::from_env()), None);
        let mut ctx = RoomContext::default();
        room.on_create(
            &mut ctx,
            json!({ "difficulty": "easy", "category": "x", "password": "s3cret" }),
        )
        .await
        .unwrap();

        // no password
        let err = room
            .on_auth(&mut ctx, &json!({}), &authed(&jwt()))
            .await
            .unwrap_err();
        assert_eq!(err.message, "password required");

        // wrong password
        let err = room
            .on_auth(&mut ctx, &json!({ "password": "nope" }), &authed(&jwt()))
            .await
            .unwrap_err();
        assert_eq!(err.message, "wrong password");

        // correct password + valid token
        let auth = room
            .on_auth(&mut ctx, &json!({ "password": "s3cret" }), &authed(&jwt()))
            .await
            .unwrap();
        assert_eq!(auth.unwrap()["name"], "tester");

        // hasPassword leaks into listing metadata but the password never does
        assert_eq!(ctx.metadata().unwrap()["hasPassword"], true);
        assert!(!ctx.metadata().unwrap().to_string().contains("s3cret"));
    }

    #[tokio::test]
    async fn no_password_means_open_room() {
        let mut room = TriviaRoom::new(Arc::new(LlmClient::from_env()), None);
        let mut ctx = RoomContext::default();
        room.on_create(&mut ctx, json!({ "difficulty": "easy" }))
            .await
            .unwrap();
        assert!(room
            .on_auth(&mut ctx, &json!({}), &authed(&jwt()))
            .await
            .is_ok());
    }
}

#[async_trait]
impl Room for TriviaRoom {
    async fn on_create(&mut self, ctx: &mut RoomContext, options: Value) -> Result<()> {
        let difficulty = options["difficulty"].as_str().unwrap_or("easy");
        if !["easy", "medium", "hard"].contains(&difficulty) {
            return Err(ServerError::new(
                codes::MATCHMAKE_INVALID_CRITERIA,
                "difficulty must be easy, medium or hard",
            ));
        }
        let category = options["category"]
            .as_str()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && s.len() <= 40)
            .unwrap_or("genel")
            .to_string();

        ctx.set_state(TriviaState::new(difficulty, &category));
        ctx.set_max_clients(Some((MAX_PLAYERS + MAX_SPECTATORS) as u32));
        ctx.set_patch_rate(Some(Duration::from_millis(50)));

        // optional room password — stored room-internally, never in state
        self.password = options["password"]
            .as_str()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && s.len() <= 64)
            .map(String::from);
        ctx.state_mut::<TriviaState>().unwrap().has_password = self.password.is_some();
        ctx.set_metadata(json!({
            "phase": "lobby",
            "round": 0,
            "players": 0,
            "spectators": 0,
            "hasPassword": self.password.is_some(),
        }));

        // every client message just becomes a command
        ctx.on_message("ready", |room: &mut Self, ctx, client, _msg: Value| {
            Box::pin(async move {
                room.dispatch(ctx, commands::ToggleReady {
                    session_id: client.session_id().to_string(),
                })
                .await;
                Ok(())
            })
        });

        ctx.on_message("start", |room: &mut Self, ctx, client, _msg: Value| {
            Box::pin(async move {
                room.dispatch(ctx, commands::StartGame { client }).await;
                Ok(())
            })
        });

        ctx.on_message("answer", |room: &mut Self, ctx, client, msg: AnswerMsg| {
            Box::pin(async move {
                room.dispatch(ctx, commands::Answer {
                    session_id: client.session_id().to_string(),
                    choice: msg.choice,
                })
                .await;
                Ok(())
            })
        });

        ctx.on_message("chat", |room: &mut Self, ctx, client, msg: ChatMsg| {
            Box::pin(async move {
                room.dispatch(ctx, commands::Chat {
                    session_id: client.session_id().to_string(),
                    text: msg.text,
                })
                .await;
                Ok(())
            })
        });

        ctx.on_message("restart", |room: &mut Self, ctx, client, _msg: Value| {
            Box::pin(async move {
                room.dispatch(ctx, commands::Restart {
                    session_id: client.session_id().to_string(),
                })
                .await;
                Ok(())
            })
        });

        Ok(())
    }

    async fn on_auth(
        &mut self,
        _ctx: &mut RoomContext,
        options: &Value,
        auth_ctx: &AuthContext,
    ) -> Result<Option<Value>> {
        // room password gate (before identity verification is even relevant)
        if let Some(expected) = &self.password {
            match options["password"].as_str() {
                None => {
                    return Err(ServerError::new(codes::AUTH_FAILED, "password required"));
                }
                Some(given) if given != expected => {
                    return Err(ServerError::new(codes::AUTH_FAILED, "wrong password"));
                }
                _ => {}
            }
        }
        auth::authenticate(auth_ctx).map(Some)
    }

    async fn on_join(
        &mut self,
        ctx: &mut RoomContext,
        client: Client,
        options: Value,
        auth_data: Option<Value>,
    ) -> Result<()> {
        self.dispatch(ctx, commands::Join {
            session_id: client.session_id().to_string(),
            name: auth::auth_name(&auth_data),
            user_id: auth::auth_user_id(&auth_data),
            wants_player: options["role"].as_str().unwrap_or("player") != "spectator",
        })
        .await;

        // join may fail (room full) — surface it as a join error
        let state = ctx.state::<TriviaState>().unwrap();
        let joined = state.players.contains_key(client.session_id())
            || state.spectators.contains_key(client.session_id());
        if joined {
            Ok(())
        } else {
            Err(ServerError::new(codes::MATCHMAKE_EXPIRED, "room is full"))
        }
    }

    async fn on_drop(&mut self, ctx: &mut RoomContext, client: Client, _code: u16) {
        let phase = ctx
            .state::<TriviaState>()
            .map(|s| s.phase.clone())
            .unwrap_or_default();

        if phase == "lobby" {
            // No reconnection grace in the lobby: the leave flow removes the
            // player immediately (owner is promoted if needed) so the
            // remaining players can ready-up and start.
            return;
        }

        // mid-game: hold the seat open for a reconnect
        ctx.allow_reconnection(&client, Some(Duration::from_secs(60)));
        ctx.broadcast("system", &json!({ "text": "a player disconnected…" }));
    }

    async fn on_reconnect(&mut self, ctx: &mut RoomContext, _client: Client) {
        ctx.broadcast("system", &json!({ "text": "a player reconnected" }));
    }

    async fn on_leave(&mut self, ctx: &mut RoomContext, client: Client, _code: u16) {
        self.dispatch(ctx, commands::Leave {
            session_id: client.session_id().to_string(),
        })
        .await;
    }

    async fn on_dispose(&mut self, _ctx: &mut RoomContext) {
        self.dispatcher_mut().stop();
    }
}
