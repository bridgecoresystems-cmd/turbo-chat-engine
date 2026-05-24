use anyhow::Result;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{Builder, Region},
    presigning::PresigningConfig,
    Client,
};
use std::time::Duration;

pub struct R2Storage {
    client: Client,
    bucket: String,
    pub_base: String,
}

impl R2Storage {
    pub fn new(account_id: &str, access_key: &str, secret_key: &str, bucket: &str, pub_base: &str) -> Self {
        let creds = Credentials::from_keys(access_key, secret_key, None);
        let config = Builder::new()
            .endpoint_url(format!("https://{account_id}.r2.cloudflarestorage.com"))
            .credentials_provider(creds)
            .region(Region::new("auto"))
            .behavior_version_latest()
            .build();
        Self {
            client: Client::from_conf(config),
            bucket: bucket.to_string(),
            pub_base: pub_base.trim_end_matches('/').to_string(),
        }
    }

    /// Presigned PUT URL — клиент загружает файл напрямую в R2 (5 минут).
    pub async fn presigned_put(&self, key: &str, content_type: &str) -> Result<String> {
        let cfg = PresigningConfig::expires_in(Duration::from_secs(300))?;
        let req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .presigned(cfg)
            .await?;
        Ok(req.uri().to_string())
    }

    /// Публичный URL файла после загрузки.
    pub fn public_url(&self, key: &str) -> String {
        format!("{}/{}", self.pub_base, key)
    }
}
