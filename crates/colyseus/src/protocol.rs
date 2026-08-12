//! Wire protocol.
//!
//! All WebSocket frames are binary MessagePack. A frame is a msgpack array whose
//! first element is a protocol code ([`code`]). The only exception is
//! [`code::PING`], which is a single raw byte.
//!
//! Server → client:
//! - `[10, reconnectionToken, serializerId]` — join handshake
//! - `[11, code, message]` — error
//! - `[13, type, payload]` — room message
//! - `[14, state]` — full room state
//! - `[15, patch]` — JSON-Patch (RFC 6902) array applied to the previous state
//! - `[17, type, bytes]` — room message with binary payload
//! - `0x12` (single byte) — ping/pong
//!
//! Client → server:
//! - `[12]` — consented leave
//! - `[13, type, payload]` — room message
//! - `[17, type, bytes]` — room message with binary payload
//! - `0x12` (single byte) — ping

use serde::Serialize;
use serde_json::Value;

/// Protocol codes. Codes below 128 encode as a single msgpack byte.
pub mod code {
    pub const JOIN_ROOM: u8 = 10;
    pub const ERROR: u8 = 11;
    pub const LEAVE_ROOM: u8 = 12;
    pub const ROOM_DATA: u8 = 13;
    pub const ROOM_STATE: u8 = 14;
    pub const ROOM_STATE_PATCH: u8 = 15;
    pub const ROOM_DATA_BYTES: u8 = 17;
    pub const PING: u8 = 18;
}

/// Identifier of a message. Strings and numbers are both supported, like in Colyseus.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MessageType {
    Num(i64),
    Str(String),
}

impl MessageType {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            MessageType::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_num(&self) -> Option<i64> {
        match self {
            MessageType::Num(n) => Some(*n),
            _ => None,
        }
    }

    pub(crate) fn to_value(&self) -> Value {
        match self {
            MessageType::Num(n) => Value::from(*n),
            MessageType::Str(s) => Value::from(s.clone()),
        }
    }

    pub(crate) fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::String(s) => Some(MessageType::Str(s.clone())),
            Value::Number(n) => n.as_i64().map(MessageType::Num),
            _ => None,
        }
    }
}

impl From<&str> for MessageType {
    fn from(s: &str) -> Self {
        MessageType::Str(s.to_string())
    }
}
impl From<String> for MessageType {
    fn from(s: String) -> Self {
        MessageType::Str(s)
    }
}
impl From<i64> for MessageType {
    fn from(n: i64) -> Self {
        MessageType::Num(n)
    }
}
impl From<i32> for MessageType {
    fn from(n: i32) -> Self {
        MessageType::Num(n as i64)
    }
}
impl From<u32> for MessageType {
    fn from(n: u32) -> Self {
        MessageType::Num(n as i64)
    }
}
impl From<u8> for MessageType {
    fn from(n: u8) -> Self {
        MessageType::Num(n as i64)
    }
}

/// A message received from a client.
#[derive(Debug)]
pub enum ClientMessage {
    Leave,
    Data(MessageType, Value),
    DataBytes(MessageType, Vec<u8>),
    Ping,
}

fn encode_value(v: &Value) -> Vec<u8> {
    rmp_serde::to_vec(v).unwrap_or_default()
}

/// `[10, reconnectionToken, serializerId, handshake?]`
pub fn join_room(reconnection_token: &str, serializer_id: &str, handshake: Option<Value>) -> Vec<u8> {
    let mut arr = vec![
        Value::from(code::JOIN_ROOM),
        Value::from(reconnection_token),
        Value::from(serializer_id),
    ];
    if let Some(handshake) = handshake {
        arr.push(handshake);
    }
    encode_value(&Value::Array(arr))
}

/// `[11, code, message]`
pub fn error(error_code: u16, message: &str) -> Vec<u8> {
    encode_value(&Value::Array(vec![
        Value::from(code::ERROR),
        Value::from(error_code),
        Value::from(message),
    ]))
}

/// `[14, state]`
pub fn room_state(state: &Value) -> Vec<u8> {
    rmp_serde::to_vec(&(code::ROOM_STATE, state)).unwrap_or_default()
}

/// `[15, patch]` where `patch` is a JSON array of RFC 6902 operations.
pub fn room_state_patch(patch: &Value) -> Vec<u8> {
    rmp_serde::to_vec(&(code::ROOM_STATE_PATCH, patch)).unwrap_or_default()
}

/// `[13, type, payload]`
///
/// NB: uses `to_vec_named` so struct payloads serialize as msgpack *maps*
/// (`{field: value}`), not positional arrays — clients always see objects.
pub fn room_data<T: Serialize>(msg_type: &MessageType, payload: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(&(code::ROOM_DATA, msg_type.to_value(), payload)).unwrap_or_default()
}

/// `[17, type, bytes]`
pub fn room_data_bytes(msg_type: &MessageType, bytes: &[u8]) -> Vec<u8> {
    rmp_serde::to_vec(&(
        code::ROOM_DATA_BYTES,
        msg_type.to_value(),
        serde_bytes::ByteBuf::from(bytes.to_vec()),
    ))
    .unwrap_or_default()
}

/// `[12]` — client → server consented leave.
pub fn leave() -> Vec<u8> {
    encode_value(&Value::Array(vec![Value::from(code::LEAVE_ROOM)]))
}

/// Single-byte ping/pong frame.
pub fn ping() -> Vec<u8> {
    vec![code::PING]
}

/// Decode a frame received from a client.
pub fn decode_client_message(bytes: &[u8]) -> Option<ClientMessage> {
    if bytes.len() == 1 && bytes[0] == code::PING {
        return Some(ClientMessage::Ping);
    }

    match rmp_serde::from_slice::<Value>(bytes) {
        Ok(Value::Array(arr)) => {
            let c = arr.first()?.as_u64()? as u8;
            match c {
                code::LEAVE_ROOM => Some(ClientMessage::Leave),
                code::ROOM_DATA => {
                    let t = MessageType::from_value(arr.get(1)?)?;
                    let payload = arr.get(2).cloned().unwrap_or(Value::Null);
                    Some(ClientMessage::Data(t, payload))
                }
                _ => None,
            }
        }
        Ok(_) => None,
        Err(_) => {
            // binary payloads cannot decode into serde_json::Value; try the bytes form
            let (c, t, payload): (u8, Value, serde_bytes::ByteBuf) =
                rmp_serde::from_slice(bytes).ok()?;
            if c != code::ROOM_DATA_BYTES {
                return None;
            }
            Some(ClientMessage::DataBytes(
                MessageType::from_value(&t)?,
                payload.into_vec(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_room_data() {
        let bytes = room_data(&"chat".into(), &json!({"text": "hello"}));
        match decode_client_message(&bytes) {
            Some(ClientMessage::Data(t, payload)) => {
                assert_eq!(t.as_str(), Some("chat"));
                assert_eq!(payload["text"], "hello");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn roundtrip_bytes() {
        let bytes = room_data_bytes(&7.into(), &[1, 2, 3, 255]);
        match decode_client_message(&bytes) {
            Some(ClientMessage::DataBytes(t, payload)) => {
                assert_eq!(t.as_num(), Some(7));
                assert_eq!(payload, vec![1, 2, 3, 255]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_ping_and_leave() {
        assert!(matches!(decode_client_message(&ping()), Some(ClientMessage::Ping)));
        assert!(matches!(decode_client_message(&leave()), Some(ClientMessage::Leave)));
    }

    #[test]
    fn struct_payloads_encode_as_maps() {
        #[derive(serde::Serialize)]
        struct Msg {
            from: String,
            text: String,
        }
        let bytes = room_data(&"chat".into(), &Msg {
            from: "a".into(),
            text: "hi".into(),
        });
        match decode_client_message(&bytes) {
            Some(ClientMessage::Data(_, payload)) => {
                assert_eq!(payload["from"], "a");
                assert_eq!(payload["text"], "hi");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn encode_join_room() {
        let bytes = join_room("tok", "json-patch", None);
        let v: Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(v[0], 10);
        assert_eq!(v[1], "tok");
        assert_eq!(v[2], "json-patch");
    }
}
