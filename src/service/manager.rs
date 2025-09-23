use std::time::Duration;

use actix_web_httpauth::extractors::basic::BasicAuth;
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use secrecy::{ExposeSecret, SecretString};
use uuid::Uuid;

use crate::{
    buffering::{Listener, ListenerKey},
    scanner::{Job, JobParams, create_s3_client},
};

pub struct ScannerServiceManager {
    job_table: DashMap<Uuid, Job>,
    listener_table: DashMap<ListenerKey, Listener>,
    listener_channel_size: usize,
    listener_channel_timeout: Duration,
    buffer_size: usize,
}

impl ScannerServiceManager {
    pub fn new(
        listener_channel_size: usize,
        listener_channel_timeout: Duration,
        buffer_size: usize,
    ) -> Self {
        Self {
            job_table: DashMap::new(),
            listener_table: DashMap::new(),
            listener_channel_size,
            listener_channel_timeout,
            buffer_size,
        }
    }

    pub async fn create_job(&self, auth: BasicAuth, job_params: JobParams) -> Result<Uuid> {
        log::info!("Received job creation request {job_params:?}.");
        let access_key_id = auth.user_id().to_owned();
        let secret_access_key = SecretString::from(auth.password().unwrap_or("").to_owned());

        let listener_key = ListenerKey::new(
            job_params
                .get_dataset()
                .map(std::string::ToString::to_string),
            job_params.get_region().to_string(),
            access_key_id.clone(),
            secret_access_key.expose_secret().clone(),
        );

        let client =
            create_s3_client(&access_key_id, &secret_access_key, job_params.get_region()).await;
        let job = Job::spawn(
            client,
            job_params.clone(),
            self.listener_table
                .entry(listener_key.clone())
                .or_insert_with(|| {
                    log::info!("Creating a new listener.");
                    Listener::spawn(
                        listener_key,
                        self.listener_channel_timeout,
                        self.listener_channel_size,
                        self.buffer_size,
                    )
                })
                .get_new_sender(),
        );

        let id = job.get_id();
        self.job_table.insert(id, job);
        Ok(id)
    }

    #[allow(clippy::unused_async)]
    pub async fn delete_job(&self, job_id: &str) -> Result<()> {
        let Ok(id) = Uuid::parse_str(job_id.to_string().as_str()) else {
            let error_msg = format!("Invalid job_id format: {job_id}.");
            log::warn!("{}", error_msg.as_str());
            return Err(anyhow!(error_msg));
        };

        if let Some((_, job)) = self.job_table.remove(&id) {
            job.cancel();
            log::info!("Job {job_id} cancelled and removed.");
            Ok(())
        } else {
            let error_msg = format!("Job {job_id} not found for deletion.");
            log::warn!("{}", error_msg.as_str());
            Err(anyhow!(error_msg))
        }
    }
}
