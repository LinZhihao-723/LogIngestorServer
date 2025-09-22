mod buffering;
mod scanner;
mod service;

use actix_web::{App, HttpResponse, HttpServer, Responder, get, web};
use actix_web_httpauth::extractors::basic::BasicAuth;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use buffering::{Buffer, Listener};
use dashmap::DashMap;
use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger, Naming};
use scanner::{Job, JobParams, create_s3_client};
use uuid::Uuid;

use service::scanner::{JobTable, create_scanner_job, delete_scanner_job};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // TODO: Handle logger initialization errors
    // TODO: Make the logger async friendly: consider using https://docs.rs/tracing/latest/tracing/
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

    // TODO: serve behind HTTPS (TLS termination at a proxy) or configure Rustls on HttpServer.
    HttpServer::new(move || {
        App::new()
            .app_data(job_table.clone())
            .service(create_scanner_job)
            .service(delete_scanner_job)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
