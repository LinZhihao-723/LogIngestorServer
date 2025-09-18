use serde::Deserialize;

/// Parameters for a scanner job, specifying the S3 region, bucket, and key prefix.
#[derive(Deserialize, Clone)]
pub struct JobParams {
    region: String,
    bucket: String,
    key_prefix: String,
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
}
