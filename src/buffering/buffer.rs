use anyhow::Result;

use super::ListenerKey;
use crate::{
    compression::{
        config::{AwsAuthentication, AwsCredentials, Input, JobConfig, Output},
        submit_compression_job,
    },
    utils::S3Object,
};

pub struct Buffer {
    tag: String,
    buffered_objects: Vec<S3Object>,
    listener_key: ListenerKey,
    total_buffered_size: usize,
    size_threshold: usize,
}

impl Buffer {
    pub fn new(listener_key: ListenerKey, size_threshold: usize) -> Self {
        let buffer_tag = format!(
            "{}-{}-{}",
            listener_key.get_dataset().unwrap_or("default"),
            listener_key.get_bucket(),
            listener_key.get_access_key_id()
        );
        Self {
            tag: buffer_tag,
            buffered_objects: Vec::new(),
            listener_key,
            total_buffered_size: 0,
            size_threshold,
        }
    }

    pub async fn add_object(&mut self, object: S3Object) -> Result<()> {
        self.total_buffered_size += object.get_size();
        self.buffered_objects.push(object);
        if self.total_buffered_size < self.size_threshold {
            return Ok(());
        }
        self.flush().await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        if self.buffered_objects.is_empty() {
            log::info!("[{}] Buffer is empty, nothing to flush.", self.tag.as_str());
            return Ok(());
        }
        log::info!(
            "[{}] Flushing {} objects with total size {} bytes.",
            self.tag.as_str(),
            self.buffered_objects.len(),
            self.total_buffered_size
        );

        let mut keys = Vec::new();
        for obj in &self.buffered_objects {
            log::info!("Submitting object with key: {:?}", obj.get_key());
            keys.push(obj.get_key().to_owned());
        }

        let job_config = JobConfig {
            input: Input {
                aws_authentication: AwsAuthentication::Credentials {
                    credentials: AwsCredentials {
                        access_key_id: self.listener_key.get_access_key_id().to_string(),
                        secret_access_key: self.listener_key.get_secret_access_key().to_string(),
                    },
                },
                bucket: self.listener_key.get_bucket().to_string(),
                dataset: self
                    .listener_key
                    .get_dataset()
                    .unwrap_or("default")
                    .to_string(),
                key_prefix: self.listener_key.get_key_prefix().to_string(),
                region_code: self.listener_key.get_region().to_string(),
                keys: Some(keys),
            },
            output: Output {
                compression_level: 3,
                target_archive_size: 268_435_456,
                target_dictionaries_size: 33_554_432,
                target_encoded_file_size: 268_435_456,
                target_segment_size: 268_435_456,
            },
        };

        match submit_compression_job(job_config).await {
            Ok(compression_job_id) => {
                log::info!(
                    "[{}] Submitted compression job. Job ID: {}.",
                    self.tag.as_str(),
                    compression_job_id
                );
            }
            Err(e) => {
                log::error!(
                    "[{}] Failed to submit compression job for object. Error: {}",
                    self.tag.as_str(),
                    e
                );
            }
        }

        log::info!(
            "[{}] Flushing objects:\n{}",
            self.tag.as_str(),
            self.buffered_objects
                .iter()
                .map(|obj| format!("{obj:?}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        self.clear();
        Ok(())
    }

    fn clear(&mut self) {
        self.buffered_objects.clear();
        self.total_buffered_size = 0;
    }
}
