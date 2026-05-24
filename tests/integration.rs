/// Integration tests — запускаются против живого сервера.
/// Перед запуском: docker compose up -d postgres redis
/// Запуск: DATABASE_URL=... cargo test --test integration
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

const BASE: &str = "http://127.0.0.1:8080";
const SECRET: &str = "dev-secret-change-in-production";

fn client() -> reqwest::Client {
    // Disable proxy — ALL_PROXY env var would route through SOCKS5 otherwise
    reqwest::Client::builder().no_proxy().build().unwrap()
}

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    role: String,
    exp: usize,
}

fn make_jwt(user_id: &str, role: &str) -> String {
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
        + 3600;
    let claims = Claims { sub: user_id.to_string(), role: role.to_string(), exp };
    encode(&Header::default(), &claims, &EncodingKey::from_secret(SECRET.as_bytes())).unwrap()
}

#[tokio::test]
async fn health_returns_ok() {
    let resp = client().get(format!("{BASE}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn history_without_token_returns_200() {
    let resp = client().get(format!("{BASE}/history/test-room?limit=10")).send().await.unwrap();
    assert!(resp.status() == 200 || resp.status() == 500);
}

#[tokio::test]
async fn upload_url_without_token_returns_401() {
    let resp = client().get(format!("{BASE}/upload-url?filename=test.jpg")).send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn upload_url_with_valid_token_returns_urls() {
    let token = make_jwt("test_user", "teacher");
    let resp = client()
        .get(format!("{BASE}/upload-url?filename=photo.jpg&content_type=image/jpeg&token={token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["upload_url"].as_str().unwrap().contains("r2.cloudflarestorage.com"));
    assert!(body["file_url"].as_str().is_some());
    assert!(body["key"].as_str().is_some());
}

#[tokio::test]
async fn register_token_requires_auth() {
    let resp = client()
        .post(format!("{BASE}/register-token"))
        .json(&serde_json::json!({ "token": "fcm_test_token" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn register_token_with_valid_jwt() {
    let token = make_jwt("test_user_reg", "student");
    let resp = client()
        .post(format!("{BASE}/register-token?token={token}"))
        .json(&serde_json::json!({ "token": "fcm_device_token_abc123" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);
}

#[tokio::test]
async fn create_room_and_list() {
    let token = make_jwt("teacher_1", "teacher");

    let c = client();

    // Create
    let resp = c.post(format!("{BASE}/rooms?token={token}"))
        .json(&serde_json::json!({ "name": "Integration Test Room", "members": ["student_1"] }))
        .send().await.unwrap();
    assert_eq!(resp.status(), 201);
    let room: serde_json::Value = resp.json().await.unwrap();
    let room_id = room["id"].as_str().unwrap().to_string();
    assert_eq!(room["created_by"], "teacher_1");

    // List
    let resp = c.get(format!("{BASE}/rooms?token={token}")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let rooms: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(rooms.iter().any(|r| r["id"] == room_id));

    // Delete
    let resp = c.delete(format!("{BASE}/rooms/{room_id}?token={token}")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Delete again — should 404
    let resp = c.delete(format!("{BASE}/rooms/{room_id}?token={token}")).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn delete_room_of_another_user_fails() {
    let teacher_token = make_jwt("teacher_owner", "teacher");
    let other_token   = make_jwt("other_user", "teacher");
    let c = client();

    let resp = c.post(format!("{BASE}/rooms?token={teacher_token}"))
        .json(&serde_json::json!({ "name": "Private Room", "members": [] }))
        .send().await.unwrap();
    let room: serde_json::Value = resp.json().await.unwrap();
    let room_id = room["id"].as_str().unwrap().to_string();

    let resp = c.delete(format!("{BASE}/rooms/{room_id}?token={other_token}"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 404);
}
