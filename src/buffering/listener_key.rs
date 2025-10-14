#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ListenerKey {
    dataset: Option<String>,
    bucket: String,
    key_prefix: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
}

impl ListenerKey {
    pub const fn new(
        dataset: Option<String>,
        bucket: String,
        key_prefix: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> Self {
        Self {
            dataset,
            bucket,
            key_prefix,
            region,
            access_key_id,
            secret_access_key,
        }
    }

    pub fn get_bucket(&self) -> &str {
        &self.bucket
    }

    pub fn get_dataset(&self) -> Option<&str> {
        self.dataset.as_deref()
    }

    pub fn get_key_prefix(&self) -> &str {
        &self.key_prefix
    }

    pub fn get_region(&self) -> &str {
        &self.region
    }

    pub fn get_access_key_id(&self) -> &str {
        &self.access_key_id
    }

    pub fn get_secret_access_key(&self) -> &str {
        &self.secret_access_key
    }
}
