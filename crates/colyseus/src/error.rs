//! Error types and well-known codes.

use std::fmt;

/// Matchmaking / application error codes (also used as HTTP status codes).
pub mod codes {
    pub const MATCHMAKE_NO_HANDLER: u16 = 520;
    pub const MATCHMAKE_INVALID_CRITERIA: u16 = 521;
    pub const MATCHMAKE_INVALID_ROOM_ID: u16 = 522;
    pub const MATCHMAKE_UNHANDLED: u16 = 523;
    pub const MATCHMAKE_EXPIRED: u16 = 524;
    pub const AUTH_FAILED: u16 = 525;
    pub const APPLICATION_ERROR: u16 = 526;

    pub const INVALID_PAYLOAD: u16 = 4217;
}

/// WebSocket close codes.
pub mod close_codes {
    pub const NORMAL_CLOSURE: u16 = 1000;
    pub const GOING_AWAY: u16 = 1001;
    pub const ABNORMAL_CLOSURE: u16 = 1006;

    /// The client explicitly consented to leave / room disposed.
    pub const CONSENTED: u16 = 4000;
    pub const SERVER_SHUTDOWN: u16 = 4001;
    pub const WITH_ERROR: u16 = 4002;
    pub const FAILED_TO_RECONNECT: u16 = 4003;

    pub const MAY_TRY_RECONNECT: u16 = 4010;
}

/// The single error type used across the framework.
#[derive(Debug, Clone)]
pub struct ServerError {
    pub code: u16,
    pub message: String,
}

impl ServerError {
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        ServerError {
            code,
            message: message.into(),
        }
    }

    pub fn no_handler(room_name: &str) -> Self {
        Self::new(
            codes::MATCHMAKE_NO_HANDLER,
            format!("provided room name \"{room_name}\" is not defined"),
        )
    }

    pub fn room_not_found(room_id: &str) -> Self {
        Self::new(
            codes::MATCHMAKE_INVALID_ROOM_ID,
            format!("room \"{room_id}\" not found"),
        )
    }

    pub fn seat_expired() -> Self {
        Self::new(codes::MATCHMAKE_EXPIRED, "seat reservation expired")
    }

    /// Was this error caused by a failed seat reservation (full/locked room)?
    /// The matchmaker retries `join_or_create` only for these.
    pub(crate) fn is_seat_reservation_failure(&self) -> bool {
        self.code == codes::MATCHMAKE_EXPIRED
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for ServerError {}

impl From<serde_json::Error> for ServerError {
    fn from(e: serde_json::Error) -> Self {
        ServerError::new(codes::INVALID_PAYLOAD, e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ServerError>;
