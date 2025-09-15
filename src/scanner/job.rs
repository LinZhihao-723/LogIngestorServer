use crate::scanner::JobParams;
use crate::scanner::utils::create_s3_client;
use actix_web::mime::Params;
use aws_sdk_s3::Client;
use secrecy::SecretString;
use tokio::{task::JoinHandle, time::sleep};

pub struct Job {
    params: JobParams,
    id: uuid::Uuid,
    handle: JoinHandle<()>,
}

async fn list_bucket_with_prefix(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<(Vec<String>, bool), aws_sdk_s3::Error> {
    let mut keys = Vec::new();

    let resp = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await?;

    if let Some(contents) = resp.contents {
        for object in contents {
            match object.key {
                Some(key) => keys.push(key),
                None => continue,
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
            Ok((keys, is_truncated))
        }
        None => Ok((keys, false)),
    }
}

async fn execute(client: Client, params: JobParams) -> Result<(), aws_sdk_s3::Error> {
    let mut key_prefix = params.get_key_prefix().to_owned();
    loop {
        let (keys, is_truncated) =
            list_bucket_with_prefix(&client, params.get_bucket(), &key_prefix).await?;
        if keys.is_empty() {
            log::info!("No objects found with prefix: {}", key_prefix);
        } else {
            key_prefix = keys.last().unwrap().to_owned();
            for key in &keys {
                log::info!("Found object key: {}", key);
            }
        }
        if is_truncated {
            // Don't sleep. Restart next iteration immediately to handle more keys.
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
