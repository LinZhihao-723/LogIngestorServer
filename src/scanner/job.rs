use anyhow::Result;
use aws_sdk_s3::Client;
use tokio::{sync::mpsc::Sender, task::JoinHandle, time::sleep};

use super::ScannedObject;
use crate::scanner::JobParams;

pub struct Job {
    id: uuid::Uuid,
    handle: JoinHandle<()>,
}

async fn list_bucket_with_prefix(
    client: &Client,
    bucket: &str,
    prefix: &str,
    start_after: Option<&str>,
) -> Result<(Vec<ScannedObject>, bool)> {
    let mut scanned_objects = Vec::new();

    let resp_handler = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .set_start_after(start_after.map(std::string::ToString::to_string))
        .send();

    let resp = match resp_handler.await {
        Ok(output) => output,
        Err(e) => {
            log::error!("Error listing objects in bucket {bucket}: {e:?}");
            return Err(anyhow::anyhow!(e));
        }
    };

    if let Some(contents) = resp.contents {
        for object in contents {
            if let (Some(key), Some(size)) = (object.key, object.size) {
                if key.ends_with('/') {
                    // Skip directory-like entries
                    log::warn!("Skipping directory-like entry: {key}");
                    continue;
                }
                scanned_objects.push(ScannedObject::new(
                    bucket.to_string(),
                    key,
                    usize::try_from(size)?,
                ));
            }
        }
    }

    match resp.is_truncated {
        Some(is_truncated) => {
            if is_truncated {
                log::info!(
                    "Response is truncated. Followings keys will be handled in the next request."
                );
            }
            Ok((scanned_objects, is_truncated))
        }
        None => Ok((scanned_objects, false)),
    }
}

async fn execute(client: Client, params: JobParams, sender: Sender<ScannedObject>) -> Result<()> {
    let mut start_after: Option<String> = None;
    loop {
        let (scanned_objects, is_truncated) = list_bucket_with_prefix(
            &client,
            params.get_bucket(),
            params.get_key_prefix(),
            start_after.as_deref(),
        )
        .await?;
        if scanned_objects.is_empty() {
            log::info!("No objects found with prefix: {}.", params.get_key_prefix());
        } else {
            log::info!(
                "Found {} objects with prefix: {}",
                scanned_objects.len(),
                params.get_key_prefix()
            );
        }
        for scanned_object in scanned_objects {
            log::info!("Found file: {scanned_object:?}");
            start_after = Some(scanned_object.get_key().to_owned());
            sender.send(scanned_object).await?;
        }
        log::info!("Last ingested key: {start_after:?}");
        if is_truncated {
            // Don't sleep. Restart the next iteration immediately to handle more keys.
            // TODO:
            // Use continuation token instead of start_after for better performance and lower cost.
            continue;
        }
        sleep(std::time::Duration::from_secs(30)).await;
    }
}

impl Job {
    pub fn spawn(client: Client, params: JobParams, sender: Sender<ScannedObject>) -> Self {
        let handle = tokio::spawn(async move {
            if let Err(e) = execute(client, params, sender).await {
                log::error!("Job execution failed: {e:?}");
            }
        });
        Self {
            id: uuid::Uuid::new_v4(),
            handle,
        }
    }

    pub fn cancel(&self) {
        self.handle.abort();
    }

    pub const fn get_id(&self) -> uuid::Uuid {
        self.id
    }
}
