use crate::scanner::JobParams;
use crate::scanner::utils::create_s3_client;
use actix_web::mime::Params;
use aws_sdk_s3::Client;
use secrecy::SecretString;
use tokio::{task::JoinHandle, time::sleep};

use super::ScannedObject;

pub struct Job {
    params: JobParams,
    id: uuid::Uuid,
    handle: JoinHandle<()>,
}

async fn list_bucket_with_prefix(
    client: &Client,
    bucket: &str,
    prefix: &str,
    start_after: &Option<String>,
) -> Result<(Vec<ScannedObject>, bool), aws_sdk_s3::Error> {
    let mut scanned_objects = Vec::new();

    let resp_handler = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .set_start_after(start_after.clone())
        .send();

    let resp = resp_handler.await?;

    if let Some(contents) = resp.contents {
        for object in contents {
            if let (Some(key), Some(size)) = (object.key, object.size) {
                scanned_objects.push(ScannedObject::new(bucket.to_string(), key, size as usize));
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

async fn execute(client: Client, params: JobParams) -> Result<(), aws_sdk_s3::Error> {
    let mut start_after: Option<String> = None;
    loop {
        let (scanned_objects, is_truncated) = list_bucket_with_prefix(
            &client,
            params.get_bucket(),
            params.get_key_prefix(),
            &start_after,
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
        for scanned_object in &scanned_objects {
            log::info!("Found file: {:?}", scanned_object);
            start_after = Some(scanned_object.get_key().to_owned());
        }
        log::info!("Last ingested key: {:?}", start_after);
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
    pub fn new(client: Client, params: JobParams) -> Self {
        let execution_param = params.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = execute(client, execution_param).await {
                log::error!("Job execution failed: {}", e);
            }
        });
        Self {
            params,
            id: uuid::Uuid::new_v4(),
            handle,
        }
    }

    pub fn get_id(&self) -> uuid::Uuid {
        self.id
    }
}
