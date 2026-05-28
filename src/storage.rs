use anyhow::Result;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{Builder, Region},
    Client,
};

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

    /// Upload bytes directly to R2 (no presigning needed).
    pub async fn put_object(&self, key: &str, content_type: &str, data: bytes::Bytes) -> Result<String> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(aws_sdk_s3::primitives::ByteStream::from(data))
            .send()
            .await?;
        Ok(self.public_url(key))
    }

    /// Публичный URL файла после загрузки.
    pub fn public_url(&self, key: &str) -> String {
        format!("{}/{}", self.pub_base, key)
    }
}
