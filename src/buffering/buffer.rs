use super::{Listener, ListenerKey};
use crate::scanner::ScannedObject;
use anyhow::Result;

pub struct Buffer {
    buffer_tag: String,
    buffered_objects: Vec<ScannedObject>,
    listener_key: ListenerKey,
    total_buffered_size: usize,
    size_threshold: usize,
}

impl Buffer {
    pub fn new(listener_key: ListenerKey, size_threshold: usize) -> Self {
        let buffer_tag = format!(
            "{}-{}",
            listener_key.get_dataset().unwrap_or("all"),
            listener_key.get_access_key_id()
        );
        Self {
            buffer_tag,
            buffered_objects: Vec::new(),
            listener_key,
            total_buffered_size: 0,
            size_threshold,
        }
    }

    pub async fn add_object(&mut self, object: ScannedObject) -> Result<()> {
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
            log::info!(
                "[{}] Buffer is empty, nothing to flush.",
                self.buffer_tag.as_str()
            );
            return Ok(());
        }
        log::info!(
            "[{}] Flushing {} objects with total size {} bytes.",
            self.buffer_tag.as_str(),
            self.buffered_objects.len(),
            self.total_buffered_size
        );
        log::info!(
            "[{}] Flushing objects:\n{}",
            self.buffer_tag.as_str(),
            self.buffered_objects
                .iter()
                .map(|obj| format!("{:?}", obj))
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
