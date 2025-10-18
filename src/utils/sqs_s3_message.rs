use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct S3Event {
    #[serde(rename = "Records")]
    pub records: Vec<S3Record>,
}

#[derive(Debug, Deserialize)]
pub struct S3Record {
    pub s3: S3Entity,
    #[serde(rename = "eventName")]
    pub event_name: String,
}

#[derive(Debug, Deserialize)]
pub struct S3Entity {
    pub bucket: S3Bucket,
    pub object: S3Object,
}

#[derive(Debug, Deserialize)]
pub struct S3Bucket {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct S3Object {
    pub key: String,
    pub size: u64,
}
