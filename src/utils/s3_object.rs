#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct S3Object {
    bucket: String,
    key: String,
    size: usize,
}

impl S3Object {
    pub const fn new(bucket: String, key: String, size: usize) -> Self {
        Self { bucket, key, size }
    }

    pub fn _get_bucket(&self) -> &str {
        &self.bucket
    }

    pub fn get_key(&self) -> &str {
        &self.key
    }

    pub const fn get_size(&self) -> usize {
        self.size
    }
}
