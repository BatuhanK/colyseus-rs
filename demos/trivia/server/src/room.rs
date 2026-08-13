//! The trivia room: a thin `Room` impl that delegates all game logic to
//! commands (see `commands/`). Server-only secrets live here, never in state.

use std::sync::Arc;
use std::time::Duration;

use colyseus::serde_json::{json, Value};
use colyseus::{
    async_trait, codes, AuthContext, Client, Dispatchable, Dispatcher, Result, Room, RoomContext,
    RoomSnapshot, ServerError,
};
use serde::Deserialize;

use crate::auth;
use crate::commands::{self, Advance, EndRound, GenerateQuestions};
use crate::llm::LlmClient;
use crate::results::ResultsPublisher;
use crate::state::{
    now_millis, TriviaInternal, TriviaState, MAX_PLAYERS, MAX_SPECTATORS, REVEAL_BREAK, ROUND_TIME,
};

pub struct TriviaRoom {
    dispatcher: Dispatcher<TriviaRoom>,

    // services (rebuilt by the factory / `on_restore`)
    pub llm: Arc<LlmClient>,
    pub results: Option<Arc<ResultsPublisher>>,

    // transient (timer ids don't survive restarts; re-armed in `on_restore`)
    pub round_timer: Option<u64>,
    pub break_timer: Option<u64>,
}

impl TriviaRoom {
    pub fn new(llm: Arc<LlmClient>, results: Option<Arc<ResultsPublisher>>) -> Self {
        TriviaRoom {
            dispatcher: Dispatcher::new(),
            llm,
            results,
            round_timer: None,
            break_timer: None,
        }
    }

    pub(crate) fn internal(ctx: &RoomContext) -> &TriviaInternal {
        ctx.internal::<TriviaInternal>().expect("internal state is set")
    }

    pub(crate) fn internal_mut(ctx: &mut RoomContext) -> &mut TriviaInternal {
        ctx.internal_mut::<TriviaInternal>().expect("internal state is set")
    }

    /// Resume in-flight work after a restore: the background LLM task died with
    /// the process, so clear its in-flight flag and either re-trigger question
    /// generation ("generating") or re-arm the phase timer ("question" /
    /// "reveal") from the persisted wall-clock deadline.
    async fn resume_after_restore(&mut self, ctx: &mut RoomContext) {
        // any background LLM task died with the process — clear the stale flag
        TriviaRoom::internal_mut(ctx).batch_in_flight = false;

        let (phase, ends_at) = {
            let state = ctx.state::<TriviaState>().unwrap();
            (state.phase.clone(), state.phase_ends_at)
        };
        let now = now_millis();
        match phase.as_str() {
            "generating" => {
                // the batch was being produced when we died — restart it
                if TriviaRoom::internal(ctx).needed_round.is_none() {
                    TriviaRoom::internal_mut(ctx).needed_round = Some(1);
                }
                self.dispatch(ctx, GenerateQuestions).await;
            }
            "question" => {
                let remaining = ends_at
                    .map(|t| t.saturating_sub(now))
                    .unwrap_or(ROUND_TIME.as_millis() as u64);
                self.round_timer = Some(ctx.set_timeout(
                    Duration::from_millis(remaining),
                    |room: &mut TriviaRoom, ctx| {
                        Box::pin(async move { room.dispatch(ctx, EndRound).await })
                    },
                ));
            }
            "reveal" => {
                let remaining = ends_at
                    .map(|t| t.saturating_sub(now))
                    .unwrap_or(REVEAL_BREAK.as_millis() as u64);
                self.break_timer = Some(ctx.set_timeout(
                    Duration::from_millis(remaining),
                    |room: &mut TriviaRoom, ctx| {
                        Box::pin(async move { room.dispatch(ctx, Advance).await })
                    },
                ));
            }
            _ => {}
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
        ctx.set_internal(TriviaInternal::default());
        ctx.set_max_clients(Some((MAX_PLAYERS + MAX_SPECTATORS) as u32));
        ctx.set_patch_rate(Some(Duration::from_millis(50)));

        // optional room password — stored in private internal state
        let password = options["password"]
            .as_str()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && s.len() <= 64)
            .map(String::from);
        let has_password = password.is_some();
        TriviaRoom::internal_mut(ctx).password = password;
        ctx.state_mut::<TriviaState>().unwrap().has_password = has_password;
        ctx.set_metadata(json!({
            "phase": "lobby",
            "round": 0,
            "players": 0,
            "spectators": 0,
            "hasPassword": has_password,
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

    async fn on_restore(&mut self, ctx: &mut RoomContext, snapshot: &RoomSnapshot) -> Result<()> {
        ctx.restore_state::<TriviaState>(snapshot)?;
        ctx.restore_internal::<TriviaInternal>(snapshot)?;
        // services (llm/results) are already rebuilt by the factory; resume
        // any in-flight generation or re-arm the phase timer.
        self.resume_after_restore(ctx).await;
        Ok(())
    }

    async fn on_auth(
        &mut self,
        ctx: &mut RoomContext,
        options: &Value,
        auth_ctx: &AuthContext,
    ) -> Result<Option<Value>> {
        // room password gate (before identity verification is even relevant)
        if let Some(expected) = TriviaRoom::internal(ctx).password.clone() {
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
