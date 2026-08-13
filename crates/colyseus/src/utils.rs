//! Small shared utilities.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Generate a URL-safe unique id (session ids, room ids, reconnection tokens).
pub fn generate_id() -> String {
    nanoid::nanoid!(16)
}

/// Current wall-clock time in milliseconds since the Unix epoch.
pub fn now_wallclock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Convert a monotonic `Instant` deadline to an absolute wall-clock ms value.
/// Used when persisting reconnection / seat expiries so they survive restarts.
pub fn instant_to_wallclock_ms(t: Instant) -> u64 {
    let now = Instant::now();
    let sys_now = now_wallclock_ms();
    if t <= now {
        sys_now
    } else {
        sys_now + (t - now).as_millis() as u64
    }
}

/// Convert an absolute wall-clock ms value back into a monotonic `Instant`.
pub fn wallclock_ms_to_instant(ms: u64) -> Instant {
    let sys_now = now_wallclock_ms();
    let delta = ms.saturating_sub(sys_now);
    Instant::now() + Duration::from_millis(delta)
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

    /// Reconstruct a clock with a previously elapsed duration (restore).
    pub fn restore(elapsed: Duration) -> Self {
        Clock {
            start: Instant::now(),
            elapsed,
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
