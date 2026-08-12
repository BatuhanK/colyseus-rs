//! Synchronized room state — everything here is visible to clients.
//! (The correct answer is *not* here; it lives on the room struct until the
//! reveal phase.)

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub const TOTAL_ROUNDS: u32 = 10;
pub const ROUND_TIME: std::time::Duration = std::time::Duration::from_secs(20);
pub const REVEAL_BREAK: std::time::Duration = std::time::Duration::from_secs(5);
pub const MAX_PLAYERS: usize = 4;
pub const MAX_SPECTATORS: usize = 10;
/// How many rounds ahead we generate questions for, in parallel.
pub const PREFETCH_DEPTH: u32 = 5;

pub fn points_for(difficulty: &str) -> i64 {
    match difficulty {
        "easy" => 10,
        "medium" => 20,
        "hard" => 30,
        _ => 10,
    }
}

pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlayerState {
    pub user_id: String,
    pub name: String,
    pub ready: bool,
    pub score: i64,
    pub correct_count: u32,
    pub answered: bool,
    /// revealed at round end (what they picked)
    pub choice: Option<usize>,
}

impl PlayerState {
    pub fn new(user_id: String, name: String) -> Self {
        PlayerState {
            user_id,
            name,
            ready: false,
            score: 0,
            correct_count: 0,
            answered: false,
            choice: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SpectatorState {
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct QuestionPublic {
    pub text: String,
    pub choices: Vec<String>,
}

/// Game phases: "lobby" | "generating" | "question" | "reveal" | "finished"
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriviaState {
    pub difficulty: String,
    pub category: String,
    pub phase: String,
    pub round: u32,
    pub total_rounds: u32,
    pub owner: Option<String>,
    pub players: HashMap<String, PlayerState>,
    pub spectators: HashMap<String, SpectatorState>,
    pub question: Option<QuestionPublic>,
    /// only set during "reveal"
    pub correct_index: Option<usize>,
    /// ms epoch deadline for the current phase (question countdown)
    pub phase_ends_at: Option<u64>,
    pub answers_in: u32,
    pub winners: Vec<String>,
    /// whether the room requires a password (the password itself is never exposed)
    pub has_password: bool,
}

impl TriviaState {
    pub fn new(difficulty: &str, category: &str) -> Self {
        TriviaState {
            difficulty: difficulty.into(),
            category: category.into(),
            phase: "lobby".into(),
            round: 0,
            total_rounds: TOTAL_ROUNDS,
            owner: None,
            players: HashMap::new(),
            spectators: HashMap::new(),
            question: None,
            correct_index: None,
            phase_ends_at: None,
            answers_in: 0,
            winners: Vec::new(),
            has_password: false,
        }
    }

    pub fn display_name(&self, session_id: &str) -> Option<String> {
        self.players
            .get(session_id)
            .map(|p| p.name.clone())
            .or_else(|| self.spectators.get(session_id).map(|s| s.name.clone()))
    }
}
