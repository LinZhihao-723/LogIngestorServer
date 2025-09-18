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

#[derive(Debug)]
struct ParsedFile {
    pub bucket: String,
    pub key: String,
    pub size: usize,
}

async fn list_bucket_with_prefix(
    client: &Client,
    bucket: &str,
    prefix: &str,
    start_after: &Option<String>,
) -> Result<(Vec<ParsedFile>, bool), aws_sdk_s3::Error> {
    let mut keys = Vec::new();

    let mut resp_handler = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .set_start_after(start_after.clone())
        .send();

    let resp = resp_handler.await?;

    if let Some(contents) = resp.contents {
        for object in contents {
            if let (Some(key), Some(size)) = (object.key, object.size) {
                keys.push(ParsedFile::new(bucket.to_string(), key, size as usize));
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
    let mut start_after: Option<String> = None;
    loop {
        let (parsed_files, is_truncated) = list_bucket_with_prefix(
            &client,
            params.get_bucket(),
            params.get_key_prefix(),
            &start_after,
        )
        .await?;
        if parsed_files.is_empty() {
            log::info!("No objects found with prefix: {}.", params.get_key_prefix());
        } else {
            log::info!(
                "Found {} objects with prefix: {}",
                parsed_files.len(),
                params.get_key_prefix()
            );
        }
        for parsed_file in &parsed_files {
            log::info!("Found file: {:?}", parsed_file);
            start_after = Some(parsed_file.key.clone());
        }
        log::info!("Last ingested key: {:?}", start_after);
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

impl ParsedFile {
    pub fn new(bucket: String, key: String, size: usize) -> Self {
        Self { bucket, key, size }
    }
}
