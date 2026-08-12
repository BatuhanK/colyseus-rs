//! Presence: pub/sub and key-value storage scoped to this process.
//!
//! The [`Presence`] trait mirrors Colyseus' presence abstraction. The default
//! [`LocalPresence`] is purely in-memory; a Redis-backed implementation can be
//! dropped in for multi-process deployments without changing room code.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

#[async_trait]
pub trait Presence: Send + Sync {
    async fn publish(&self, topic: &str, message: Value);
    async fn subscribe(&self, topic: &str) -> BoxStream<'static, Value>;

    async fn set(&self, key: &str, value: Value);
    async fn get(&self, key: &str) -> Option<Value>;
    async fn del(&self, key: &str);

    async fn hset(&self, key: &str, field: &str, value: Value);
    async fn hget(&self, key: &str, field: &str) -> Option<Value>;
    async fn hdel(&self, key: &str, field: &str);
    async fn hgetall(&self, key: &str) -> HashMap<String, Value>;
}

/// In-process presence backed by `tokio::broadcast` + `DashMap`.
#[derive(Default)]
pub struct LocalPresence {
    topics: DashMap<String, broadcast::Sender<Value>>,
    kv: DashMap<String, Value>,
    hashes: DashMap<String, HashMap<String, Value>>,
}

impl LocalPresence {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl Presence for LocalPresence {
    async fn publish(&self, topic: &str, message: Value) {
        if let Some(tx) = self.topics.get(topic) {
            let _ = tx.send(message);
        }
    }

    async fn subscribe(&self, topic: &str) -> BoxStream<'static, Value> {
        let rx = self
            .topics
            .entry(topic.to_string())
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe();
        Box::pin(BroadcastStream::new(rx).filter_map(|r| async move { r.ok() }))
    }

    async fn set(&self, key: &str, value: Value) {
        self.kv.insert(key.to_string(), value);
    }

    async fn get(&self, key: &str) -> Option<Value> {
        self.kv.get(key).map(|v| v.clone())
    }

    async fn del(&self, key: &str) {
        self.kv.remove(key);
    }

    async fn hset(&self, key: &str, field: &str, value: Value) {
        self.hashes
            .entry(key.to_string())
            .or_default()
            .insert(field.to_string(), value);
    }

    async fn hget(&self, key: &str, field: &str) -> Option<Value> {
        self.hashes.get(key).and_then(|h| h.get(field).cloned())
    }

    async fn hdel(&self, key: &str, field: &str) {
        if let Some(mut h) = self.hashes.get_mut(key) {
            h.remove(field);
        }
    }

    async fn hgetall(&self, key: &str) -> HashMap<String, Value> {
        self.hashes.get(key).map(|h| h.clone()).unwrap_or_default()
    }
}
