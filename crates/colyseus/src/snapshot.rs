//! Room persistence: snapshots, the snapshot store, and background writing.
//!
//! A room's serializable footprint — its public [`crate::RoomContext::set_state`]
//! state, its private [`crate::RoomContext::set_internal`] state, plus the
//! framework fields needed to rebuild a listing (metadata, lock, seats,
//! reconnections, …) — is captured as a [`RoomSnapshot`] and stored through a
//! [`SnapshotStore`]. The default [`FileSnapshotStore`] writes one
//! length-checked, checksummed MessagePack file per room, atomically.
//!
//! Writes are handed to a single background writer thread (one per process)
//! so the room actor never blocks on disk I/O and per-room writes stay
//! ordered.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Envelope magic bytes: "CLRS" (colyseus-rs snapshot).
const MAGIC: [u8; 4] = *b"CLRS";
/// Current on-disk envelope format version.
const ENVELOPE_VERSION: u32 = 1;

/// A persisted reconnection entry. A client holding `token` may rejoin
/// `session_id` after a restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedReconnection {
    pub session_id: String,
    pub token: String,
    /// Wall-clock ms epoch. `None` = manual (never expires on its own).
    pub expires_at_ms: Option<u64>,
    pub auth: Option<Value>,
    pub user_data: Option<Value>,
}

/// A persisted, unconsumed seat reservation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSeat {
    pub session_id: String,
    pub options: Value,
    pub auth: Option<Value>,
    /// Wall-clock ms epoch.
    pub expires_at_ms: u64,
}

/// The complete serializable footprint of a room.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomSnapshot {
    /// Schema version of the *room type* that produced this snapshot.
    /// Used to trigger [`crate::Room::on_migrate`] on restore.
    pub schema_version: u32,
    pub room_id: String,
    pub room_name: String,
    /// Process id that wrote the snapshot (informational only).
    pub process_id: String,
    /// Wall-clock ms epoch the room was originally created.
    pub created_at: u64,
    /// Wall-clock ms epoch this snapshot was written.
    pub saved_at: u64,
    /// Restored into [`crate::utils::Clock`].
    pub clock_elapsed_ms: u64,
    /// Original creation options (re-passed to `on_restore` / `on_create`).
    pub options: Value,
    /// Public, client-visible state (`Value::Null` when the room has none).
    pub state: Value,
    /// Private, server-only state (`Value::Null` when the room has none).
    pub internal: Value,
    pub metadata: Option<Value>,
    pub locked: bool,
    pub is_private: bool,
    pub max_clients: Option<u32>,
    pub filter_extra: Map<String, Value>,
    pub reconnections: Vec<PersistedReconnection>,
    pub seats: Vec<PersistedSeat>,
}

impl RoomSnapshot {
    /// Deserialize the public state into the room's concrete state type.
    pub fn state<T: DeserializeOwned>(&self) -> Result<T, String> {
        serde_json::from_value(self.state.clone()).map_err(|e| e.to_string())
    }

    /// Deserialize the private state into the room's concrete internal type.
    pub fn internal<T: DeserializeOwned>(&self) -> Result<T, String> {
        serde_json::from_value(self.internal.clone()).map_err(|e| e.to_string())
    }
}

/// Compute a checksum over the serialized snapshot payload.
fn crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

/// Serialize a snapshot into the on-disk envelope
/// (`magic | version | checksum | msgpack payload`).
pub(crate) fn encode_snapshot(snapshot: &RoomSnapshot) -> Vec<u8> {
    let payload = rmp_serde::to_vec(snapshot).unwrap_or_default();
    let checksum = crc32(&payload);
    let mut out = Vec::with_capacity(12 + payload.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&ENVELOPE_VERSION.to_be_bytes());
    out.extend_from_slice(&checksum.to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Parse a snapshot envelope, validating magic + checksum + version.
pub(crate) fn decode_snapshot(bytes: &[u8]) -> Result<RoomSnapshot, String> {
    if bytes.len() < 12 {
        return Err("snapshot too short".into());
    }
    if bytes[0..4] != MAGIC {
        return Err("bad snapshot magic".into());
    }
    let version = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version > ENVELOPE_VERSION {
        return Err(format!("unsupported snapshot envelope version {version}"));
    }
    let expected = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let payload = &bytes[12..];
    if crc32(payload) != expected {
        return Err("snapshot checksum mismatch".into());
    }
    rmp_serde::from_slice(payload).map_err(|e| e.to_string())
}

/// Storage backend for room snapshots.
pub trait SnapshotStore: Send + Sync + 'static {
    /// Atomically persist the serialized snapshot for `room_id`.
    fn save(&self, room_id: &str, bytes: &[u8]) -> Result<(), String>;
    /// Load the serialized snapshot, if any.
    fn load(&self, room_id: &str) -> Option<Vec<u8>>;
    /// Delete a snapshot (missing file is not an error).
    fn delete(&self, room_id: &str) -> Result<(), String>;
    /// All persisted room ids.
    fn list_room_ids(&self) -> Vec<String>;
    /// Move a corrupt snapshot out of the way. Defaults to deleting it.
    fn quarantine(&self, room_id: &str, _reason: &str) {
        let _ = self.delete(room_id);
    }
}

/// Sanitize a room id into a safe file name component.
fn sanitize(room_id: &str) -> String {
    room_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// File-backed [`SnapshotStore`]. One `<room_id>.snap` file per room, written
/// atomically (temp file + fsync + rename).
#[derive(Clone)]
pub struct FileSnapshotStore {
    dir: PathBuf,
}

impl FileSnapshotStore {
    /// Create (if needed) and use `dir` as the snapshot directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let _ = std::fs::create_dir_all(&dir);
        FileSnapshotStore { dir }
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    fn file_path(&self, room_id: &str) -> PathBuf {
        self.dir.join(format!("{}.snap", sanitize(room_id)))
    }
}

impl SnapshotStore for FileSnapshotStore {
    fn save(&self, room_id: &str, bytes: &[u8]) -> Result<(), String> {
        let path = self.file_path(room_id);
        let tmp = self.dir.join(format!(".{}.tmp", sanitize(room_id)));
        std::fs::write(&tmp, bytes).map_err(|e| e.to_string())?;
        if let Ok(f) = std::fs::File::open(&tmp) {
            let _ = f.sync_all();
        }
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
    }

    fn load(&self, room_id: &str) -> Option<Vec<u8>> {
        std::fs::read(self.file_path(room_id)).ok()
    }

    fn delete(&self, room_id: &str) -> Result<(), String> {
        match std::fs::remove_file(self.file_path(room_id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn list_room_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(stem) = name.strip_suffix(".snap") {
                    ids.push(stem.to_string());
                }
            }
        }
        ids
    }

    fn quarantine(&self, room_id: &str, reason: &str) {
        let path = self.file_path(room_id);
        if !path.exists() {
            return;
        }
        let ts = crate::utils::now_wallclock_ms();
        let target = self.dir.join(format!(
            "{}.corrupt-{ts}",
            sanitize(room_id)
        ));
        if let Err(e) = std::fs::rename(&path, &target) {
            tracing::warn!("failed to quarantine corrupt snapshot {room_id}: {e}");
            return;
        }
        tracing::warn!("quarantined corrupt snapshot {room_id} -> {} ({reason})", target.display());
    }
}

// ----------------------------------------------------------------------
// Background writer
// ----------------------------------------------------------------------

enum SnapshotOp {
    Save { room_id: String, bytes: Vec<u8> },
    Delete { room_id: String },
    Flush(std::sync::mpsc::Sender<()>),
}

/// A cloneable handle that funnels snapshot writes onto one background thread,
/// preserving per-room ordering and keeping disk I/O off the room actor.
#[derive(Clone)]
pub(crate) struct SnapshotWriter {
    tx: std::sync::mpsc::Sender<SnapshotOp>,
}

impl SnapshotWriter {
    pub fn spawn(store: Arc<dyn SnapshotStore>) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<SnapshotOp>();
        std::thread::Builder::new()
            .name("colyseus-snapshot-writer".into())
            .spawn(move || {
                while let Ok(op) = rx.recv() {
                    match op {
                        SnapshotOp::Save { room_id, bytes } => {
                            if let Err(e) = store.save(&room_id, &bytes) {
                                tracing::error!("snapshot save for {room_id} failed: {e}");
                            }
                        }
                        SnapshotOp::Delete { room_id } => {
                            if let Err(e) = store.delete(&room_id) {
                                tracing::error!("snapshot delete for {room_id} failed: {e}");
                            }
                        }
                        SnapshotOp::Flush(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
            })
            .expect("failed to spawn snapshot writer");
        SnapshotWriter { tx }
    }

    pub fn save(&self, room_id: &str, bytes: Vec<u8>) {
        let _ = self.tx.send(SnapshotOp::Save {
            room_id: room_id.to_string(),
            bytes,
        });
    }

    pub fn delete(&self, room_id: &str) {
        let _ = self.tx.send(SnapshotOp::Delete {
            room_id: room_id.to_string(),
        });
    }

    /// Block until all previously queued writes have hit disk.
    pub fn flush(&self) {
        let (ack, rx) = std::sync::mpsc::channel();
        if self.tx.send(SnapshotOp::Flush(ack)).is_ok() {
            let _ = rx.recv_timeout(Duration::from_secs(10));
        }
    }
}

// ----------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------

/// Persistence configuration for a server / room type.
#[derive(Clone)]
pub struct PersistenceConfig {
    /// Where snapshots are stored.
    pub store: Arc<dyn SnapshotStore>,
    /// Minimum time between automatic snapshot writes. Default 1s.
    pub auto_save_interval: Duration,
    /// Write a final snapshot when a room is disposed. Default `true`.
    pub save_on_dispose: bool,
    /// Delete the snapshot when a room is disposed (overrides `save_on_dispose`).
    /// Default `false` — rooms remain resumable across restarts.
    pub delete_on_dispose: bool,
}

impl PersistenceConfig {
    pub fn new(store: impl SnapshotStore) -> Self {
        PersistenceConfig {
            store: Arc::new(store),
            auto_save_interval: Duration::from_secs(1),
            save_on_dispose: true,
            delete_on_dispose: false,
        }
    }

    pub fn auto_save_interval(mut self, interval: Duration) -> Self {
        self.auto_save_interval = interval;
        self
    }

    pub fn save_on_dispose(mut self, save: bool) -> Self {
        self.save_on_dispose = save;
        self
    }

    pub fn delete_on_dispose(mut self, delete: bool) -> Self {
        self.delete_on_dispose = delete;
        self
    }
}

/// What a room actor needs to write snapshots: the config + the writer.
#[derive(Clone)]
pub(crate) struct PersistenceHandle {
    pub config: PersistenceConfig,
    pub writer: SnapshotWriter,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let mut filter_extra = Map::new();
        filter_extra.insert("difficulty".into(), serde_json::json!("easy"));
        filter_extra.insert("category".into(), serde_json::json!("x"));

        let snap = RoomSnapshot {
            schema_version: 1,
            room_id: "r1".into(),
            room_name: "game".into(),
            process_id: "p1".into(),
            created_at: 100,
            saved_at: 200,
            clock_elapsed_ms: 5,
            options: Value::Null,
            state: serde_json::json!({"count": 3}),
            internal: serde_json::json!({"secret": "x"}),
            metadata: Some(serde_json::json!({"phase": "lobby"})),
            locked: false,
            is_private: false,
            max_clients: Some(8),
            filter_extra,
            reconnections: vec![PersistedReconnection {
                session_id: "s1".into(),
                token: "tok".into(),
                expires_at_ms: None,
                auth: Some(serde_json::json!({"userId": "u1"})),
                user_data: None,
            }],
            seats: vec![PersistedSeat {
                session_id: "s2".into(),
                options: Value::Null,
                auth: None,
                expires_at_ms: 42,
            }],
        };
        let bytes = encode_snapshot(&snap);
        let decoded = decode_snapshot(&bytes).unwrap();
        assert_eq!(decoded.room_id, "r1");
        assert_eq!(decoded.state["count"], 3);
        assert_eq!(decoded.internal["secret"], "x");
        assert_eq!(decoded.filter_extra["difficulty"], "easy");
        assert_eq!(decoded.reconnections.len(), 1);
        assert_eq!(decoded.reconnections[0].token, "tok");
        assert_eq!(decoded.seats.len(), 1);
        assert_eq!(decoded.seats[0].expires_at_ms, 42);
    }

    #[test]
    fn envelope_detects_corruption() {
        let snap = RoomSnapshot {
            schema_version: 1,
            room_id: "r1".into(),
            room_name: "game".into(),
            process_id: "p1".into(),
            created_at: 0,
            saved_at: 0,
            clock_elapsed_ms: 0,
            options: Value::Null,
            state: Value::Null,
            internal: Value::Null,
            metadata: None,
            locked: false,
            is_private: false,
            max_clients: None,
            filter_extra: Map::new(),
            reconnections: vec![],
            seats: vec![],
        };
        let mut bytes = encode_snapshot(&snap);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(decode_snapshot(&bytes).is_err());
    }

    #[test]
    fn file_store_roundtrip_and_delete() {
        let dir = std::env::temp_dir().join(format!("colyseus-snap-test-{}", std::process::id()));
        let store = FileSnapshotStore::new(&dir);
        store.save("room-1", b"hello").unwrap();
        assert_eq!(store.load("room-1"), Some(b"hello".to_vec()));
        assert_eq!(store.list_room_ids(), vec!["room-1".to_string()]);
        store.delete("room-1").unwrap();
        assert!(store.load("room-1").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
