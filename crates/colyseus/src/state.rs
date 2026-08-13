//! Room state synchronization.
//!
//! Any `serde::Serialize` struct can be used as room state. The framework keeps
//! a JSON snapshot of the last broadcasted state; on every patch tick the
//! current state is serialized and diffed against the snapshot, producing an
//! RFC 6902 JSON-Patch document which is broadcast to all joined clients.
//!
//! This replaces Colyseus' `@colyseus/schema` with a serde-native approach:
//! no macros or decorators are required on the state struct beyond
//! `#[derive(Serialize)]`.

use std::any::Any;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

/// A state edit applied through the admin panel.
#[derive(Debug, Clone)]
pub enum StateEdit {
    Set(Value),
    Remove,
}

/// Serializer id sent in the join handshake when a state is present.
pub const JSON_PATCH_SERIALIZER_ID: &str = "json-patch";
/// Serializer id when the room has no state.
pub const NONE_SERIALIZER_ID: &str = "none";

/// Type-erased serializable state.
pub(crate) trait ErasedState: Send {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn to_value(&self) -> Value;
    /// Apply an admin edit via a validated serialize→edit→deserialize
    /// round-trip. Type mismatches are rejected by deserialization.
    fn apply_edit(&mut self, path: &[String], edit: &StateEdit) -> Result<(), String>;
}

impl<T> ErasedState for T
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    fn apply_edit(&mut self, path: &[String], edit: &StateEdit) -> Result<(), String> {
        let mut value = serde_json::to_value(&*self).map_err(|e| e.to_string())?;
        edit_at_path(&mut value, path, edit)?;
        *self = serde_json::from_value(value).map_err(|e: serde_json::Error| e.to_string())?;
        Ok(())
    }
}

/// Set or remove a value at a JSON-pointer-ish path (segments; numeric
/// segments index into arrays).
fn edit_at_path(root: &mut Value, path: &[String], edit: &StateEdit) -> Result<(), String> {
    if path.is_empty() {
        return match edit {
            StateEdit::Set(v) => {
                *root = v.clone();
                Ok(())
            }
            StateEdit::Remove => Err("cannot remove the state root".into()),
        };
    }

    let mut cur = &mut *root;
    for seg in &path[..path.len() - 1] {
        cur = match cur {
            Value::Object(map) => map.get_mut(seg).ok_or(format!("no such path: {}", seg))?,
            Value::Array(arr) => {
                let i: usize = seg.parse().map_err(|_| format!("bad array index: {seg}"))?;
                arr.get_mut(i).ok_or(format!("array index out of range: {i}"))?
            }
            _ => return Err(format!("cannot descend into {seg}")),
        };
    }

    let last = &path[path.len() - 1];
    match cur {
        Value::Object(map) => match edit {
            StateEdit::Set(v) => {
                map.insert(last.clone(), v.clone());
                Ok(())
            }
            StateEdit::Remove => {
                map.remove(last).ok_or(format!("no such key: {last}"))?;
                Ok(())
            }
        },
        Value::Array(arr) => {
            let i: usize = last.parse().map_err(|_| format!("bad array index: {last}"))?;
            match edit {
                StateEdit::Set(v) => {
                    if i >= arr.len() {
                        return Err(format!("array index out of range: {i}"));
                    }
                    arr[i] = v.clone();
                    Ok(())
                }
                StateEdit::Remove => {
                    if i >= arr.len() {
                        return Err(format!("array index out of range: {i}"));
                    }
                    arr.remove(i);
                    Ok(())
                }
            }
        }
        _ => Err("path parent is not a container".into()),
    }
}

/// Holds the room's private (server-only) serializable state.
///
/// Like [`StateSlot`] but never broadcast to clients and carries no diff
/// snapshot: it exists purely so server-side game data can survive restarts.
pub(crate) struct InternalSlot {
    typed: Box<dyn ErasedState>,
}

impl InternalSlot {
    pub fn new<S: Serialize + DeserializeOwned + Send + 'static>(state: S) -> Self {
        InternalSlot {
            typed: Box::new(state),
        }
    }

    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.typed.as_any().downcast_ref::<T>()
    }

    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.typed.as_any_mut().downcast_mut::<T>()
    }

    /// The current full value (serialized into snapshots).
    pub fn full(&self) -> Value {
        self.typed.to_value()
    }
}

/// Holds the room's state plus the snapshot of the last broadcast.
pub(crate) struct StateSlot {
    typed: Box<dyn ErasedState>,
    snapshot: Value,
}

impl StateSlot {
    pub fn new<S: Serialize + DeserializeOwned + Send + 'static>(state: S) -> Self {
        let snapshot = serde_json::to_value(&state).unwrap_or(Value::Null);
        StateSlot {
            typed: Box::new(state),
            snapshot,
        }
    }

    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.typed.as_any().downcast_ref::<T>()
    }

    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.typed.as_any_mut().downcast_mut::<T>()
    }

    /// Type-erased shared access (for view filters).
    pub fn as_any(&self) -> &dyn Any {
        self.typed.as_any()
    }

    /// The current full state (sent to joining clients).
    pub fn full(&self) -> Value {
        self.typed.to_value()
    }

    /// Admin edit: mutate the state at `path`. The next patch broadcast
    /// delivers the change to clients automatically.
    pub fn apply_edit(&mut self, path: &[String], edit: &StateEdit) -> Result<(), String> {
        self.typed.apply_edit(path, edit)
    }

    /// Compute a JSON-Patch against the last snapshot. Returns `None` when
    /// nothing changed. Updates the snapshot on change.
    pub fn diff(&mut self) -> Option<Value> {
        let current = self.typed.to_value();
        if current == self.snapshot {
            return None;
        }
        let ops = crate::diff::diff(&self.snapshot, &current);
        self.snapshot = current;
        if ops.is_empty() {
            return None;
        }
        Some(Value::Array(ops))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Serialize, serde::Deserialize)]
    struct Pos {
        x: i32,
        y: i32,
    }

    #[test]
    fn diff_produces_patch_only_on_change() {
        let mut slot = StateSlot::new(Pos { x: 0, y: 0 });
        assert!(slot.diff().is_none());

        slot.get_mut::<Pos>().unwrap().x = 5;
        let patch = slot.diff().expect("expected a patch");
        let ops = patch.as_array().unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0]["op"], "replace");
        assert_eq!(ops[0]["path"], "/x");
        assert_eq!(ops[0]["value"], json!(5));

        assert!(slot.diff().is_none());
    }

    #[test]
    fn full_state() {
        let slot = StateSlot::new(Pos { x: 1, y: 2 });
        assert_eq!(slot.full(), json!({"x": 1, "y": 2}));
    }
}
