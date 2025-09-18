mod scanner;

use actix_web::{App, HttpResponse, HttpServer, Responder, get, web};
use actix_web_httpauth::extractors::basic::BasicAuth;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use dashmap::DashMap;
use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger, Naming};
use scanner::{Job, JobParams, create_s3_client};
use uuid::Uuid;

struct JobTable {
    jobs: DashMap<Uuid, Job>,
}

impl JobTable {
    fn new() -> Self {
        Self {
            jobs: DashMap::new(),
        }
    }
}

#[get("/create")]
async fn create_scanner_job(
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
    let job = Job::new(client, query.into_inner().clone());

    let id = job.get_id();
    job_table.jobs.insert(id, job);

    HttpResponse::Ok().body(id.to_string())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // TODO: Handle logger initialization errors
    Logger::try_with_env_or_str("info")
        .unwrap()
        .log_to_file(
            FileSpec::default()
                .directory("logs")
                .basename("server")
                .suffix("log"),
        )
        .duplicate_to_stdout(Duplicate::All)
        .rotate(
            Criterion::Size(10_000_000),
            Naming::Numbers,
            Cleanup::KeepLogFiles(7),
        )
        .use_utc() // optional: timestamps in UTC; remove for local time
        .format(flexi_logger::detailed_format)
        .start()
        .expect("failed to initialize logging");

    log::info!("Starting server.");

    let job_table = web::Data::new(JobTable::new());

    // IMPORTANT: serve behind HTTPS (TLS termination at a proxy),
    // or configure Rustls on HttpServer (not shown here to keep it short).
    HttpServer::new(move || {
        App::new()
            .app_data(job_table.clone())
            .service(create_scanner_job)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
