mod scanner;

use actix_web::{App, HttpResponse, HttpServer, Responder, get, web};
use actix_web_httpauth::extractors::basic::BasicAuth;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use flexi_logger::{Cleanup, Criterion, Duplicate, FileSpec, Logger, Naming};

#[derive(Deserialize)]
struct S3Params {
    region: String,
    bucket: String,
    key_prefix: String,
}

#[get("/create")]
async fn s3_op(auth: BasicAuth, query: web::Query<S3Params>) -> impl Responder {
    let access_key_id = auth.user_id().to_owned();
    let secret_access_key = SecretString::from(auth.password().unwrap_or("").to_owned());

    HttpResponse::Ok().finish()
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

    // IMPORTANT: serve behind HTTPS (TLS termination at a proxy),
    // or configure Rustls on HttpServer (not shown here to keep it short).
    HttpServer::new(|| App::new().service(s3_op))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
