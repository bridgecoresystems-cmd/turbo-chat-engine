use anyhow::{anyhow, Result};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,  // user_id
    pub role: String, // "teacher" | "student"
    pub exp: usize,
}

pub fn verify(token: &str, secret: &str) -> Result<Claims> {
    let key = DecodingKey::from_secret(secret.as_bytes());
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    decode::<Claims>(token, &key, &validation)
        .map(|d| d.claims)
        .map_err(|e| anyhow!("invalid token: {e}"))
}

/// Extracts `token` value from a URL query string.
pub fn token_from_query(query: Option<&str>) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == "token").then(|| v.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_token(secret: &str, sub: &str, exp_offset_secs: i64) -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let claims = Claims {
            sub: sub.to_string(),
            role: "teacher".to_string(),
            exp: (now + exp_offset_secs) as usize,
        };
        encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_bytes())).unwrap()
    }

    #[test]
    fn verify_valid_token() {
        let token = make_token("secret", "user_1", 3600);
        let claims = verify(&token, "secret").unwrap();
        assert_eq!(claims.sub, "user_1");
        assert_eq!(claims.role, "teacher");
    }

    #[test]
    fn verify_wrong_secret_fails() {
        let token = make_token("secret", "user_1", 3600);
        assert!(verify(&token, "wrong-secret").is_err());
    }

    #[test]
    fn verify_expired_token_fails() {
        // Use -3600 because jsonwebtoken has a default 60s leeway
        let token = make_token("secret", "user_1", -3600);
        assert!(verify(&token, "secret").is_err());
    }

    #[test]
    fn verify_garbage_fails() {
        assert!(verify("not.a.token", "secret").is_err());
    }

    #[test]
    fn token_from_query_single_param() {
        assert_eq!(token_from_query(Some("token=abc123")), Some("abc123".to_string()));
    }

    #[test]
    fn token_from_query_among_params() {
        assert_eq!(token_from_query(Some("foo=bar&token=xyz&baz=1")), Some("xyz".to_string()));
    }

    #[test]
    fn token_from_query_missing() {
        assert_eq!(token_from_query(Some("foo=bar")), None);
        assert_eq!(token_from_query(None), None);
    }
}
