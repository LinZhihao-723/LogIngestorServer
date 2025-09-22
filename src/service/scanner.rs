use crate::buffering::{Buffer, Listener};
use crate::scanner::{Job, JobParams, create_s3_client};
use actix_web::{HttpResponse, Responder, get, web};
use actix_web_httpauth::extractors::basic::BasicAuth;
use dashmap::DashMap;
use secrecy::SecretString;
use serde::Deserialize;
use uuid::Uuid;

pub struct JobTable {
    jobs: DashMap<Uuid, Job>,

    // TODO: Create a listener table
    listener: Listener,
}

impl JobTable {
    pub fn new() -> Self {
        let buffer = Buffer::new(1024 * 1024 * 10);
        Self {
            jobs: DashMap::new(),
            listener: Listener::spawn(buffer, std::time::Duration::from_secs(60), 100),
        }
    }
}

#[get("/create")]
pub async fn create_scanner_job(
    job_table: web::Data<JobTable>,
    auth: BasicAuth,
    query: web::Query<JobParams>,
) -> impl Responder {
    log::info!(
        "Received job creation request for bucket: {}, prefix: {} in region: {}.",
        query.get_bucket(),
        query.get_key_prefix(),
        query.get_region()
    );
    let access_key_id = auth.user_id().to_owned();
    let secret_access_key = SecretString::from(auth.password().unwrap_or("").to_owned());

    let client = create_s3_client(&access_key_id, &secret_access_key, query.get_region()).await;
    let job = Job::spawn(
        client,
        query.into_inner().clone(),
        job_table.listener.get_new_sender(),
    );

    let id = job.get_id();
    job_table.jobs.insert(id, job);

    HttpResponse::Ok().body(id.to_string())
}

#[derive(Deserialize)]
struct JobIdQuery {
    job_id: String,
}

#[get("/delete")]
pub async fn delete_scanner_job(
    job_table: web::Data<JobTable>,
    query: web::Query<JobIdQuery>,
) -> impl Responder {
    let job_id = match Uuid::parse_str(&query.job_id) {
        Ok(id) => id,
        Err(_) => {
            log::warn!("Invalid job_id format: {}", query.job_id);
            return HttpResponse::BadRequest().body("Invalid job_id format");
        }
    };

    match job_table.jobs.remove(&job_id) {
        Some((_, job)) => {
            job.cancel();
            log::info!("Job {} cancelled and removed.", job_id);
            HttpResponse::Ok().body(format!("Job {} deleted", job_id))
        }
        None => {
            log::warn!("Job {} not found for deletion.", job_id);
            HttpResponse::NotFound().body("Job not found")
        }
    }
}
