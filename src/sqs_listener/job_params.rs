use serde::Deserialize;
use url::Url;

#[derive(Deserialize, Clone, Debug)]
pub struct JobParams {
    region: String,
    bucket: String,
    key_prefix: String,
    sqs_url: Url,
    dataset: Option<String>,
}

impl JobParams {
    pub fn get_region(&self) -> &str {
        &self.region
    }

    pub fn get_bucket(&self) -> &str {
        &self.bucket
    }

    pub fn get_key_prefix(&self) -> &str {
        &self.key_prefix
    }

    pub fn get_sqs_url(&self) -> &str {
        self.sqs_url.as_str()
    }

    pub fn get_dataset(&self) -> Option<&str> {
        self.dataset.as_deref()
    }
}
