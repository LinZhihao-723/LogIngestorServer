use crate::scanner::ScannedObject;
use anyhow::Result;

pub struct Buffer {
    objects: Vec<ScannedObject>,
    total_size: usize,
    size_threshold: usize,
}

impl Buffer {
    pub fn new(size_threshold: usize) -> Self {
        Self {
            objects: Vec::new(),
            total_size: 0,
            size_threshold,
        }
    }

    pub async fn add_object(&mut self, object: ScannedObject) -> Result<()> {
        self.total_size += object.get_size();
        self.objects.push(object);
        if self.total_size < self.size_threshold {
            return Ok(());
        }
        self.flush().await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        if self.objects.is_empty() {
            log::info!("Buffer is empty, nothing to flush.");
            return Ok(());
        }
        log::info!(
            "Flushing {} objects with total size {} bytes.",
            self.objects.len(),
            self.total_size
        );
        log::info!(
            "Flushing objects:\n{}",
            self.objects
                .iter()
                .map(|obj| format!("{:?}", obj))
                .collect::<Vec<_>>()
                .join("\n")
        );
        self.clear();
        Ok(())
    }

    fn clear(&mut self) {
        self.objects.clear();
        self.total_size = 0;
    }
}
