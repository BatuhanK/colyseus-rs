//! Game-flow commands: the round state machine.
//!
//! ```text
//! (first lobby join) ──► GenerateQuestions  [pre-warm, background]
//! StartGame ──► StartRound(1) — instantly if questions are ready
//! StartRound ──(timer or all-answered)──► EndRound ──(5s break)──► Advance
//! Advance ──► StartRound(n+1) … or FinishGame after round 10
//! ```
//!
//! Questions are generated as ONE batch (all rounds, single LLM context →
//! no intra-game repeats), typically before the game even starts.

use colyseus::serde_json::json;
use colyseus::{codes, BoxFuture, Client, Command, Dispatchable, Dispatcher, RoomContext};

use crate::commands::lobby::sync_meta;
use crate::llm::Question;
use crate::room::TriviaRoom;
use crate::state::{
    now_millis, points_for, QuestionPublic, TriviaState, REVEAL_BREAK, ROUND_TIME, TOTAL_ROUNDS,
};

/// Owner starts the game (everyone must be ready).
pub struct StartGame {
    pub client: Client,
}

impl Command<TriviaRoom> for StartGame {
    fn execute<'a>(
        self: Box<Self>,
        _room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let state = ctx.state::<TriviaState>().unwrap();
            let err = if state.phase != "lobby" {
                Some("game already started")
            } else if state.owner.as_deref() != Some(self.client.session_id()) {
                Some("only the room owner can start")
            } else if state.players.is_empty() {
                Some("no players")
            } else if state.players.values().any(|p| !p.ready) {
                Some("everyone must be ready")
            } else {
                None
            };
            if let Some(err) = err {
                self.client.error(codes::APPLICATION_ERROR, err);
                return;
            }

            d.enqueue(StartRound { round: 1 });
        })
    }
}

/// A player locks in an answer. Ends the round early when everyone's in.
pub struct Answer {
    pub session_id: String,
    pub choice: usize,
}

impl Command<TriviaRoom> for Answer {
    fn execute<'a>(
        self: Box<Self>,
        room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            {
                let state = ctx.state_mut::<TriviaState>().unwrap();
                if state.phase != "question" || self.choice >= 4 {
                    return;
                }
                let Some(p) = state.players.get_mut(&self.session_id) else {
                    return; // spectators can't answer
                };
                if p.answered {
                    return;
                }
                p.answered = true;
                state.answers_in += 1;
                room.round_answers.insert(self.session_id, self.choice);
            }

            let all_answered = {
                let state = ctx.state::<TriviaState>().unwrap();
                !state.players.is_empty() && state.answers_in as usize >= state.players.len()
            };
            if all_answered {
                d.enqueue(EndRound);
            }
        })
    }
}

/// Generate the whole game's questions in one background batch (idempotent).
pub struct GenerateQuestions;

impl Command<TriviaRoom> for GenerateQuestions {
    fn execute<'a>(
        self: Box<Self>,
        room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        _d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if room.batch_in_flight || room.pending_questions.len() >= TOTAL_ROUNDS as usize {
                return;
            }
            room.batch_in_flight = true;

            let (difficulty, category) = {
                let state = ctx.state::<TriviaState>().unwrap();
                (state.difficulty.clone(), state.category.clone())
            };
            let llm = room.llm.clone();
            let sender = ctx.sender();

            tokio::spawn(async move {
                let questions = llm
                    .generate_batch_or_mock(&difficulty, &category, TOTAL_ROUNDS)
                    .await;
                sender.send(move |room: &mut TriviaRoom, ctx| {
                    Box::pin(async move {
                        room.dispatch(ctx, QuestionsReady { questions }).await;
                    })
                });
            });
        })
    }
}

/// The question batch arrived from the LLM task (via `RoomSender`).
pub struct QuestionsReady {
    pub questions: Vec<Question>,
}

impl Command<TriviaRoom> for QuestionsReady {
    fn execute<'a>(
        self: Box<Self>,
        room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            room.batch_in_flight = false;
            room.llm.record_asked(self.questions.iter().map(|q| q.text.clone()));
            for (i, q) in self.questions.into_iter().enumerate() {
                room.pending_questions.insert(i as u32 + 1, q);
            }

            // if the game is waiting on round 1, kick it off now
            if room.needed_round == Some(1)
                && ctx.state::<TriviaState>().unwrap().phase == "generating"
            {
                room.needed_round = None;
                d.enqueue(StartRound { round: 1 });
            }
        })
    }
}

/// Begin a round: take the prefetched question (falling back to a mock if
/// somehow missing), publish it (answer stays server-side), arm the timer.
pub struct StartRound {
    pub round: u32,
}

impl Command<TriviaRoom> for StartRound {
    fn execute<'a>(
        self: Box<Self>,
        room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let question = match room.pending_questions.remove(&self.round) {
                Some(q) => q,
                None => {
                    // batch not ready yet — wait in the generating phase
                    if !room.batch_in_flight {
                        d.enqueue(GenerateQuestions);
                    }
                    let state = ctx.state_mut::<TriviaState>().unwrap();
                    state.phase = "generating".into();
                    state.phase_ends_at = None;
                    room.needed_round = Some(self.round);
                    sync_meta(ctx);
                    return;
                }
            };

            room.correct_index = Some(question.answer_index);
            room.round_answers.clear();

            {
                let state = ctx.state_mut::<TriviaState>().unwrap();
                state.phase = "question".into();
                state.round = self.round;
                state.question = Some(QuestionPublic {
                    text: question.text,
                    choices: question.choices,
                });
                state.correct_index = None;
                state.answers_in = 0;
                state.phase_ends_at = Some(now_millis() + ROUND_TIME.as_millis() as u64);
                for p in state.players.values_mut() {
                    p.answered = false;
                    p.choice = None;
                }
            }

            room.round_timer = Some(ctx.set_timeout(ROUND_TIME, |room: &mut TriviaRoom, ctx| {
                Box::pin(async move { room.dispatch(ctx, EndRound).await })
            }));
            sync_meta(ctx);
        })
    }
}

/// End the round: reveal the answer, score it, start the break timer.
pub struct EndRound;

impl Command<TriviaRoom> for EndRound {
    fn execute<'a>(
        self: Box<Self>,
        room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        _d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if ctx.state::<TriviaState>().unwrap().phase != "question" {
                return; // already ended (everyone answered earlier)
            }
            if let Some(t) = room.round_timer.take() {
                ctx.clear_timeout(t);
            }

            let points = points_for(&ctx.state::<TriviaState>().unwrap().difficulty);
            let correct = room.correct_index;
            let answers = std::mem::take(&mut room.round_answers);

            {
                let state = ctx.state_mut::<TriviaState>().unwrap();
                state.phase = "reveal".into();
                state.correct_index = correct;
                state.phase_ends_at = Some(now_millis() + REVEAL_BREAK.as_millis() as u64);
                for (sid, choice) in answers {
                    if let Some(p) = state.players.get_mut(&sid) {
                        p.choice = Some(choice);
                        if Some(choice) == correct {
                            p.score += points;
                            p.correct_count += 1;
                        }
                    }
                }
            }

            room.break_timer = Some(ctx.set_timeout(REVEAL_BREAK, |room: &mut TriviaRoom, ctx| {
                Box::pin(async move { room.dispatch(ctx, Advance).await })
            }));
        })
    }
}

/// Advance after the reveal break: next round, or finish after round 10.
pub struct Advance;

impl Command<TriviaRoom> for Advance {
    fn execute<'a>(
        self: Box<Self>,
        _room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let round = ctx.state::<TriviaState>().unwrap().round;
            if round >= TOTAL_ROUNDS {
                d.enqueue(FinishGame);
            } else {
                d.enqueue(StartRound { round: round + 1 });
            }
        })
    }
}

/// Finish the game: compute winners, publish results to Redis.
pub struct FinishGame;

impl Command<TriviaRoom> for FinishGame {
    fn execute<'a>(
        self: Box<Self>,
        room: &'a mut TriviaRoom,
        ctx: &'a mut RoomContext,
        _d: &'a mut Dispatcher<TriviaRoom>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let payload;
            let room_id = ctx.room_id().to_string();
            {
                let state = ctx.state_mut::<TriviaState>().unwrap();
                if state.phase == "finished" {
                    return;
                }
                state.phase = "finished".into();
                state.question = None;
                state.phase_ends_at = None;

                let best = state.players.values().map(|p| p.score).max().unwrap_or(0);
                state.winners = state
                    .players
                    .values()
                    .filter(|p| p.score == best)
                    .map(|p| p.name.clone())
                    .collect();

                payload = json!({
                    "type": "game_finished",
                    "roomId": room_id,
                    "difficulty": state.difficulty,
                    "category": state.category,
                    "endedAt": now_millis(),
                    "players": state.players.values().map(|p| json!({
                        "userId": p.user_id,
                        "name": p.name,
                        "score": p.score,
                        "correct": p.correct_count,
                    })).collect::<Vec<_>>(),
                });
            }

            if let Some(results) = &room.results {
                let results = results.clone();
                tokio::spawn(async move { results.publish(payload).await });
            } else {
                tracing::info!("redis not configured; final results: {payload}");
            }
            sync_meta(ctx);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::LlmClient;
    use crate::state::PlayerState;
    use colyseus::Dispatchable;
    use std::sync::Arc;

    fn test_room() -> (TriviaRoom, RoomContext) {
        let room = TriviaRoom::new(Arc::new(LlmClient::from_env()), None);
        let mut ctx = RoomContext::default();
        ctx.set_state(TriviaState::new("medium", "test"));
        (room, ctx)
    }

    /// EndRound scores correct answers and reveals everyone's picks.
    #[tokio::test]
    async fn end_round_scores_answers() {
        let (mut room, mut ctx) = test_room();

        {
            let state = ctx.state_mut::<TriviaState>().unwrap();
            state.phase = "question".into();
            state.round = 3;
            state.players.insert("a".into(), PlayerState::new("ua".into(), "alice".into()));
            state.players.insert("b".into(), PlayerState::new("ub".into(), "bob".into()));
        }
        room.correct_index = Some(2);
        room.round_answers.insert("a".into(), 2); // correct
        room.round_answers.insert("b".into(), 0); // wrong

        room.dispatch(&mut ctx, EndRound).await;

        let state = ctx.state::<TriviaState>().unwrap();
        assert_eq!(state.phase, "reveal");
        assert_eq!(state.correct_index, Some(2));
        assert_eq!(state.players["a"].score, 20); // medium = 20p
        assert_eq!(state.players["a"].correct_count, 1);
        assert_eq!(state.players["b"].score, 0);
        assert_eq!(state.players["b"].choice, Some(0));
    }

    /// FinishGame publishes winners and locks the phase.
    #[tokio::test]
    async fn finish_game_computes_winners() {
        let (mut room, mut ctx) = test_room();
        {
            let state = ctx.state_mut::<TriviaState>().unwrap();
            let mut alice = PlayerState::new("ua".into(), "alice".into());
            alice.score = 100;
            let mut bob = PlayerState::new("ub".into(), "bob".into());
            bob.score = 60;
            state.players.insert("a".into(), alice);
            state.players.insert("b".into(), bob);
        }

        room.dispatch(&mut ctx, FinishGame).await;

        let state = ctx.state::<TriviaState>().unwrap();
        assert_eq!(state.phase, "finished");
        assert_eq!(state.winners, vec!["alice"]);
    }

    /// StartRound consumes the prefetched question; missing → generating.
    #[tokio::test]
    async fn start_round_uses_pending_question() {
        let (mut room, mut ctx) = test_room();
        room.pending_questions.insert(
            1,
            Question {
                text: "q?".into(),
                choices: vec!["a".into(), "b".into(), "c".into(), "d".into()],
                answer_index: 2,
            },
        );

        room.dispatch(&mut ctx, StartRound { round: 1 }).await;

        let state = ctx.state::<TriviaState>().unwrap();
        assert_eq!(state.phase, "question");
        assert_eq!(state.question.as_ref().unwrap().text, "q?");
        assert_eq!(state.correct_index, None); // hidden until reveal
        assert_eq!(room.correct_index, Some(2));

        // round 2 has nothing prefetched → generating + batch requested
        room.dispatch(&mut ctx, StartRound { round: 2 }).await;
        let state = ctx.state::<TriviaState>().unwrap();
        assert_eq!(state.phase, "generating");
        assert_eq!(room.needed_round, Some(2));
    }

    /// QuestionsReady fills the pending map and kicks a waiting game off.
    #[tokio::test]
    async fn questions_ready_starts_waiting_game() {
        let (mut room, mut ctx) = test_room();
        {
            let state = ctx.state_mut::<TriviaState>().unwrap();
            state.phase = "generating".into();
        }
        room.needed_round = Some(1);

        let questions: Vec<Question> = (0..TOTAL_ROUNDS)
            .map(|i| Question {
                text: format!("q{i}?"),
                choices: vec!["a".into(), "b".into(), "c".into(), "d".into()],
                answer_index: 0,
            })
            .collect();

        room.dispatch(&mut ctx, QuestionsReady { questions }).await;

        let state = ctx.state::<TriviaState>().unwrap();
        assert_eq!(state.phase, "question");
        assert_eq!(state.round, 1);
        assert_eq!(room.pending_questions.len(), (TOTAL_ROUNDS - 1) as usize);
    }
}
