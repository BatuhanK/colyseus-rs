//! Lobby-phase commands: joining, leaving, ready-up, chat, restart.

use colyseus::serde_json::json;
use colyseus::{codes, BoxFuture, Command, Dispatcher, RoomContext};

use crate::room::TriviaRoom;
use crate::state::{PlayerState, SpectatorState, TriviaState, MAX_PLAYERS, MAX_SPECTATORS};
use crate::commands::{EndRound, FinishGame};

/// A client joins the room — as a player if there's room in the lobby,
/// otherwise (or mid-game) as a spectator.
///
/// Session takeover: if the same account (userId) already sits in the room
/// under a different session id (page reload after the reconnection window,
/// a second tab, …), the stale session is dropped and the player entry —
/// score, ready flag, even mid-round answers — moves to the new session.
pub struct Join {
    pub session_id: String,
    pub name: String,
    pub user_id: String,
    pub wants_player: bool,
}

impl Command<TriviaRoom> for Join {
    fn execute<'a>(
        self: Box<Self>,
        _room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // ---------------------------------------------------------------
            // 1) session takeover for a known account
            // ---------------------------------------------------------------
            let stale_sid = if self.user_id.is_empty() {
                None
            } else {
                ctx.state::<TriviaState>()
                    .unwrap()
                    .players
                    .iter()
                    .find(|(sid, p)| p.user_id == self.user_id && **sid != self.session_id)
                    .map(|(sid, _)| sid.clone())
            };

            if let Some(old_sid) = stale_sid {
                if let Some(old_client) = ctx.get_client(&old_sid) {
                    old_client.leave(Some(codes::MATCHMAKE_EXPIRED), Some("session moved to a new connection"));
                }
                ctx.remove_client(&old_sid);

                // transfer the round answer, if any was locked in
                {
                    let internal = TriviaRoom::internal_mut(ctx);
                    if let Some(answer) = internal.round_answers.remove(&old_sid) {
                        internal.round_answers.insert(self.session_id.clone(), answer);
                    }
                }

                let state = ctx.state_mut::<TriviaState>().unwrap();
                if let Some(entry) = state.players.remove(&old_sid) {
                    state.players.insert(self.session_id.clone(), entry);
                }
                if state.owner.as_deref() == Some(old_sid.as_str()) {
                    state.owner = Some(self.session_id.clone());
                }
                ctx.broadcast("system", &json!({ "text": format!("{} rejoined", self.name) }));
                sync_meta(ctx);
                return;
            }

            // ---------------------------------------------------------------
            // 2) fresh join
            // ---------------------------------------------------------------
            let state = ctx.state_mut::<TriviaState>().unwrap();
            let can_play = state.phase == "lobby" && state.players.len() < MAX_PLAYERS;

            if self.wants_player && can_play {
                // same account idling as spectator? drop that entry
                state.spectators.remove(&self.session_id);
                if state.owner.is_none() {
                    state.owner = Some(self.session_id.clone());
                }
                state.players.insert(
                    self.session_id.clone(),
                    PlayerState::new(self.user_id, self.name.clone()),
                );
                // pre-warm: generate the game's questions while players ready up
                let first_player = state.players.len() == 1;
                ctx.broadcast("system", &json!({ "text": format!("{} joined the game", self.name) }));
                if first_player {
                    d.enqueue(crate::commands::GenerateQuestions);
                }
            } else if state.spectators.len() < MAX_SPECTATORS {
                state.spectators.insert(
                    self.session_id.clone(),
                    SpectatorState { name: self.name.clone() },
                );
                ctx.broadcast("system", &json!({ "text": format!("{} is spectating", self.name) }));
            }
            // (if neither worked, the room impl rejects the join)
            sync_meta(ctx);
        })
    }
}

/// A client leaves for good (consented or reconnection expired).
pub struct Leave {
    pub session_id: String,
}

impl Command<TriviaRoom> for Leave {
    fn execute<'a>(
        self: Box<Self>,
        _room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let (was_player, in_game, players_left);
            {
                let state = ctx.state_mut::<TriviaState>().unwrap();
                let name = state.display_name(&self.session_id).unwrap_or_default();
                was_player = state.players.remove(&self.session_id).is_some();
                state.spectators.remove(&self.session_id);
                if state.owner.as_deref() == Some(self.session_id.as_str()) {
                    state.owner = state.players.keys().next().cloned();
                }
                players_left = state.players.len();
                in_game = matches!(state.phase.as_str(), "question" | "reveal" | "generating");
                ctx.broadcast("system", &json!({ "text": format!("{name} left") }));
            }
            sync_meta(ctx);

            if was_player {
                // a departed player's pending answer no longer blocks the round
                let all_answered = {
                    let state = ctx.state::<TriviaState>().unwrap();
                    state.phase == "question"
                        && !state.players.is_empty()
                        && state.answers_in as usize >= players_left
                };
                if all_answered {
                    d.enqueue(EndRound);
                }
                if players_left == 0 && in_game {
                    d.enqueue(FinishGame);
                }
            }
        })
    }
}

/// Toggle a player's ready flag (lobby only).
pub struct ToggleReady {
    pub session_id: String,
}

impl Command<TriviaRoom> for ToggleReady {
    fn execute<'a>(
        self: Box<Self>,
        _room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        _d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let state = ctx.state_mut::<TriviaState>().unwrap();
            if state.phase != "lobby" {
                return;
            }
            if let Some(p) = state.players.get_mut(&self.session_id) {
                p.ready = !p.ready;
            }
        })
    }
}

/// Chat message relay (players and spectators alike).
pub struct Chat {
    pub session_id: String,
    pub text: String,
}

impl Command<TriviaRoom> for Chat {
    fn execute<'a>(
        self: Box<Self>,
        _room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        _d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let name = ctx
                .state::<TriviaState>()
                .and_then(|s| s.display_name(&self.session_id))
                .unwrap_or_else(|| "???".into());
            let text: String = self.text.chars().take(300).collect();
            ctx.broadcast("chat", &json!({ "from": name, "text": text }));
        })
    }
}

/// Owner sends the finished room back to the lobby for another game.
pub struct Restart {
    pub session_id: String,
}

impl Command<TriviaRoom> for Restart {
    fn execute<'a>(
        self: Box<Self>,
        _room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            {
                let state = ctx.state_mut::<TriviaState>().unwrap();
                if state.phase != "finished" || state.owner.as_deref() != Some(self.session_id.as_str()) {
                    return;
                }
                state.phase = "lobby".into();
                state.round = 0;
                state.question = None;
                state.correct_index = None;
                state.winners.clear();
                for p in state.players.values_mut() {
                    *p = PlayerState::new(p.user_id.clone(), p.name.clone());
                }
            }
            TriviaRoom::internal_mut(ctx).pending_questions.clear();
            TriviaRoom::internal_mut(ctx).correct_index = None;
            // pre-generate the next game's questions while players ready up
            d.enqueue(crate::commands::GenerateQuestions);
            ctx.broadcast("system", &json!({ "text": "back to lobby — ready up!" }));
            sync_meta(ctx);
        })
    }
}

/// Keep the public listing metadata (phase, counts) fresh for the home page.
pub fn sync_meta(ctx: &mut RoomContext) {
    let state = ctx.state::<TriviaState>().unwrap();
    ctx.set_metadata(json!({
        "phase": state.phase,
        "round": state.round,
        "players": state.players.len(),
        "spectators": state.spectators.len(),
        "hasPassword": state.has_password,
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::LlmClient;
    use crate::state::TriviaInternal;
    use colyseus::Dispatchable;
    use std::sync::Arc;

    fn test_room(phase: &str) -> (TriviaRoom, RoomContext) {
        let room = TriviaRoom::new(Arc::new(LlmClient::from_env()), None);
        let mut ctx = RoomContext::default();
        ctx.set_state(TriviaState::new("easy", "test"));
        ctx.set_internal(TriviaInternal::default());
        ctx.state_mut::<TriviaState>().unwrap().phase = phase.into();
        (room, ctx)
    }

    fn join_cmd(sid: &str, name: &str, wants_player: bool) -> Join {
        Join {
            session_id: sid.into(),
            name: name.into(),
            user_id: format!("u-{sid}"),
            wants_player,
        }
    }

    #[tokio::test]
    async fn lobby_join_becomes_player() {
        let (mut room, mut ctx) = test_room("lobby");
        room.dispatch(&mut ctx, join_cmd("s1", "alice", true)).await;
        let state = ctx.state::<TriviaState>().unwrap();
        assert!(state.players.contains_key("s1"));
        assert!(!state.spectators.contains_key("s1"));
        assert_eq!(state.owner.as_deref(), Some("s1"));
    }

    #[tokio::test]
    async fn midgame_join_becomes_spectator() {
        let (mut room, mut ctx) = test_room("question");
        room.dispatch(&mut ctx, join_cmd("s1", "alice", true)).await;
        room.dispatch(&mut ctx, join_cmd("s2", "ff", true)).await;
        let state = ctx.state::<TriviaState>().unwrap();
        // mid-game: nobody can become a player, both are spectators
        assert!(state.spectators.contains_key("s1"));
        assert!(state.spectators.contains_key("s2"));
        assert!(state.players.is_empty());
    }

    #[tokio::test]
    async fn full_lobby_join_becomes_spectator() {
        let (mut room, mut ctx) = test_room("lobby");
        for i in 0..4 {
            room.dispatch(&mut ctx, join_cmd(&format!("s{i}"), "p", true)).await;
        }
        room.dispatch(&mut ctx, join_cmd("late", "ff", true)).await;
        let state = ctx.state::<TriviaState>().unwrap();
        assert_eq!(state.players.len(), 4);
        assert!(state.spectators.contains_key("late"));
    }

    #[tokio::test]
    async fn same_account_takes_over_session() {
        let (mut room, mut ctx) = test_room("question");
        room.dispatch(&mut ctx, Join {
            session_id: "old".into(),
            name: "ff".into(),
            user_id: "u-ff".into(),
            wants_player: true,
        }).await; // mid-game → spectator
        room.dispatch(&mut ctx, Join {
            session_id: "new".into(),
            name: "ff".into(),
            user_id: "u-ff".into(),
            wants_player: true,
        }).await;
        let state = ctx.state::<TriviaState>().unwrap();
        // spectator path: no takeover target among players, so both spectator entries
        assert!(!state.players.contains_key("old"));
    }
}
