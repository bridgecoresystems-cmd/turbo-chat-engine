use anyhow::{anyhow, Result};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Deserialize)]
struct ServiceAccount {
    project_id: String,
    private_key: String,
    client_email: String,
}

#[derive(Serialize)]
struct JwtClaims {
    iss: String,
    scope: String,
    aud: String,
    exp: u64,
    iat: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

struct CachedToken {
    token: String,
    expires_at: u64,
}

pub struct FcmClient {
    project_id: String,
    client_email: String,
    private_key: EncodingKey,
    http: reqwest::Client,
    cached_token: Mutex<Option<CachedToken>>,
}

impl FcmClient {
    pub fn from_json(json: &str) -> Result<Self> {
        let sa: ServiceAccount = serde_json::from_str(json)?;
        let key = EncodingKey::from_rsa_pem(sa.private_key.as_bytes())?;
        Ok(Self {
            project_id: sa.project_id,
            client_email: sa.client_email,
            private_key: key,
            http: reqwest::Client::new(),
            cached_token: Mutex::new(None),
        })
    }

    async fn access_token(&self) -> Result<String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        {
            let cached = self.cached_token.lock().await;
            if let Some(ref t) = *cached {
                if t.expires_at > now + 60 {
                    return Ok(t.token.clone());
                }
            }
        }

        let claims = JwtClaims {
            iss: self.client_email.clone(),
            scope: "https://www.googleapis.com/auth/firebase.messaging".to_string(),
            aud: "https://oauth2.googleapis.com/token".to_string(),
            exp: now + 3600,
            iat: now,
        };

        let jwt = encode(&Header::new(Algorithm::RS256), &claims, &self.private_key)?;

        let resp = self
            .http
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ])
            .send()
            .await?;

        if !resp.status().is_success() {
            let err = resp.text().await.unwrap_or_default();
            return Err(anyhow!("OAuth2 error: {err}"));
        }

        let token_resp: TokenResponse = resp.json().await?;
        let token = token_resp.access_token.clone();

        let mut cached = self.cached_token.lock().await;
        *cached = Some(CachedToken {
            token: token_resp.access_token,
            expires_at: now + token_resp.expires_in,
        });

        Ok(token)
    }

    pub async fn send(
        &self,
        fcm_token: &str,
        title: &str,
        body: &str,
        room_id: &str,
        sender_id: &str,
    ) -> Result<()> {
        let token = self.access_token().await?;
        let url = format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            self.project_id
        );

        let payload = serde_json::json!({
            "message": {
                "token": fcm_token,
                "notification": { "title": title, "body": body },
                "data": { "room_id": room_id, "sender_id": sender_id },
                "android": { "priority": "high" },
                "apns": { "headers": { "apns-priority": "10" } }
            }
        });

        let resp = self.http.post(&url).bearer_auth(&token).json(&payload).send().await?;

        if resp.status().is_success() {
            info!("FCM sent to {}...{}", &fcm_token[..8], &fcm_token[fcm_token.len().saturating_sub(4)..]);
        } else {
            let err = resp.text().await.unwrap_or_default();
            error!("FCM error: {err}");
        }

        Ok(())
    }
}
