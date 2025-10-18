use anyhow::Result;
use aws_sdk_sqs::Client;
use tokio::{sync::mpsc::Sender, task::JoinHandle};

use crate::{
    sqs_listener::JobParams,
    utils::{S3Event, S3Object},
};

pub struct Job {
    id: uuid::Uuid,
    handle: JoinHandle<()>,
}

impl Job {
    pub fn spawn(client: Client, params: JobParams, sender: Sender<S3Object>) -> Self {
        let handle = tokio::spawn(async move {
            if let Err(e) = listen_to_sqs_queue(client, params, sender).await {
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

async fn listen_to_sqs_queue(
    client: Client,
    job: JobParams,
    sender: Sender<S3Object>,
) -> Result<()> {
    loop {
        // TODO: Add adaptive visibility timeout handling:
        // If there are too many irrelevant messages, increase the visibility timeout to reduce
        // duplicated handling for these messages.
        let resp = client
            .receive_message()
            .queue_url(job.get_sqs_url())
            .max_number_of_messages(10)
            .wait_time_seconds(10)
            .send()
            .await?;

        let Some(messages) = resp.messages else {
            log::info!("No messages received from SQS queue.");
            continue;
        };

        for msg in messages {
            if msg.body.is_none() {
                log::warn!("Received SQS message with empty body. Skipping.");
                continue;
            }

            let event: S3Event = match serde_json::from_str(msg.body().unwrap()) {
                Ok(deserialized) => deserialized,
                Err(e) => {
                    log::error!(
                        "Failed to deserialize SQS message body as a S3 Event: {e:?}. Skipping."
                    );
                    continue;
                }
            };

            let mut object_found = false;
            for record in event.records {
                if !record.event_name.starts_with("ObjectCreated:") {
                    continue;
                }

                let bucket_name = record.s3.bucket.name.as_str();
                if job.get_bucket() != bucket_name {
                    continue;
                }

                let object_key = record.s3.object.key.as_str();
                if object_key.ends_with('/') || !object_key.starts_with(job.get_key_prefix()) {
                    continue;
                }

                let s3_object = S3Object::new(
                    bucket_name.to_string(),
                    object_key.to_string(),
                    usize::try_from(record.s3.object.size)?,
                );

                log::info!("Found S3 object from SQS message: {s3_object:?}");
                sender.send(s3_object).await?;
                object_found = true;
            }

            if !object_found {
                log::info!("No relevant S3 objects found in SQS message.");
                continue;
            }
            if let Some(receipt) = msg.receipt_handle() {
                match client
                    .delete_message()
                    .queue_url(job.get_sqs_url())
                    .receipt_handle(receipt)
                    .send()
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("Failed to delete SQS message: {e:?}");
                    }
                }
            }
        }
    }
}
