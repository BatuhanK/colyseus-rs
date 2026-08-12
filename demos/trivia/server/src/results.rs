//! Game-results delivery to the web backend's leaderboard.
//!
//! Uses a Redis **list as a queue** (`trivia:results`): the game server
//! `RPUSH`es results, the web backend `BLPOP`s them. Unlike PUBLISH this is
//! durable — if the web backend is down or restarting, results wait in the
//! queue and are drained when it comes back.
//!
//! (For at-least-once processing semantics under consumer crashes, upgrade
//! to Redis Streams + consumer groups; a list is enough here.)

use colyseus::serde_json::Value;

pub const RESULTS_QUEUE: &str = "trivia:results";
/// Cap the backlog so a dead consumer can't grow memory unboundedly.
const MAX_QUEUED: isize = 1000;

#[derive(Clone)]
pub struct ResultsPublisher {
    conn: redis::aio::MultiplexedConnection,
}

impl ResultsPublisher {
    pub async fn from_env() -> Option<Self> {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let client = redis::Client::open(url).ok()?;
        let conn = client.get_multiplexed_async_connection().await.ok()?;
        Some(ResultsPublisher { conn })
    }

    pub async fn publish(&self, payload: Value) {
        let mut conn = self.conn.clone();
        let message = payload.to_string();
        let res: redis::RedisResult<()> = redis::pipe()
            .cmd("RPUSH")
            .arg(RESULTS_QUEUE)
            .arg(&message)
            .cmd("LTRIM")
            .arg(RESULTS_QUEUE)
            .arg(-MAX_QUEUED)
            .arg(-1)
            .ignore()
            .query_async(&mut conn)
            .await;
        match res {
            Ok(()) => tracing::info!("queued game results to redis ({RESULTS_QUEUE})"),
            Err(e) => tracing::error!("failed to queue results to redis: {e}"),
        }
    }
}
