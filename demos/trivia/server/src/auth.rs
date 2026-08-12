//! Auth: HS256 JWTs issued by the T3 backend, verified in `Room::on_auth`.

use colyseus::serde_json::{json, Value};
use colyseus::{codes, AuthContext, Result, ServerError};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String, // user id
    name: String,
    #[allow(dead_code)]
    exp: usize,
}

fn game_secret() -> String {
    std::env::var("GAME_SECRET").unwrap_or_else(|_| "dev-secret-change-me".to_string())
}

/// Verify the bearer token from a matchmaking request and turn it into the
/// client's auth payload (`{ userId, name }`).
pub fn authenticate(auth: &AuthContext) -> Result<Value> {
    let token = auth
        .token
        .as_deref()
        .ok_or_else(|| ServerError::new(codes::AUTH_FAILED, "missing bearer token"))?;

    let claims = jsonwebtoken::decode::<Claims>(
        token,
        &DecodingKey::from_secret(game_secret().as_bytes()),
        &Validation::new(Algorithm::HS256),
    )
    .map_err(|_| ServerError::new(codes::AUTH_FAILED, "invalid or expired token"))?
    .claims;

    Ok(json!({ "userId": claims.sub, "name": claims.name }))
}

pub fn auth_name(auth: &Option<Value>) -> String {
    auth.as_ref()
        .and_then(|a| a["name"].as_str())
        .unwrap_or("anon")
        .to_string()
}

pub fn auth_user_id(auth: &Option<Value>) -> String {
    auth.as_ref()
        .and_then(|a| a["userId"].as_str())
        .unwrap_or("")
        .to_string()
}
