#[derive(Debug, Clone)]
pub struct ScannedObject {
    bucket: String,
    key: String,
    size: usize,
}

impl ScannedObject {
    pub const fn new(bucket: String, key: String, size: usize) -> Self {
        Self { bucket, key, size }
    }

    pub fn get_bucket(&self) -> &str {
        &self.bucket
    }

    pub fn get_key(&self) -> &str {
        &self.key
    }

    pub const fn get_size(&self) -> usize {
        self.size
    }
}
