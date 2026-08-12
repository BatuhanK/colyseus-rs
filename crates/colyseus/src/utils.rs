//! Small shared utilities.

use std::time::{Duration, Instant};

/// Generate a URL-safe unique id (session ids, room ids, reconnection tokens).
pub fn generate_id() -> String {
    nanoid::nanoid!(16)
}

/// A monotonic clock for rooms, similar to Colyseus' `Clock`.
///
/// The clock is ticked by the framework: on every simulation tick when a
/// simulation interval is set, or on every patch broadcast otherwise.
#[derive(Debug, Clone)]
pub struct Clock {
    start: Instant,
    elapsed: Duration,
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock {
    pub fn new() -> Self {
        Clock {
            start: Instant::now(),
            elapsed: Duration::ZERO,
        }
    }

    /// Update `elapsed` from the start instant. Called by the framework.
    pub fn tick(&mut self) {
        self.elapsed = self.start.elapsed();
    }

    /// Elapsed time since the clock was created (as of the last tick).
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn elapsed_millis(&self) -> u64 {
        self.elapsed.as_millis() as u64
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.elapsed.as_secs_f64()
    }
}
