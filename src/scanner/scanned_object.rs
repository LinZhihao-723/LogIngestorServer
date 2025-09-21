#[derive(Debug)]
pub struct ScannedObject {
    bucket: String,
    key: String,
    size: usize,
}

impl ScannedObject {
    pub fn new(bucket: String, key: String, size: usize) -> Self {
        Self { bucket, key, size }
    }

    pub fn get_bucket(&self) -> &str {
        &self.bucket
    }

    pub fn get_key(&self) -> &str {
        &self.key
    }

    pub fn get_size(&self) -> usize {
        self.size
    }
}
